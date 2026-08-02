# Ultra Bot v6.0 — FIX #32/#33: Live %/speed/ETA progress engine extended to
# compress, merge, scene-detect, split & trim (previously download/upload only)
import os
import sys
import re
import json
import glob
import time
import math
import base64
import asyncio
import logging
import traceback
from collections import deque
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import FloodWait, MessageNotModified
from pyrogram.errors import RPCError as ServerError  # FIX #24 — ServerError removed in latest pyrogram

try:
    from groq import AsyncGroq
except ImportError:
    AsyncGroq = None

# ══════════════════════════════════════════════════════════════
#  LOGGING
# ══════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("UltraBot")


# ══════════════════════════════════════════════════════════════
#  FIX #12 + #19 — ENV VALIDATION
#  - Clear error if missing (not cryptic TypeError)
#  - ValueError if API_ID is not a number
# ══════════════════════════════════════════════════════════════
def _require_env(key: str) -> str:
    val = os.getenv(key, "").strip()
    if not val:
        log.critical(f"❌ Environment variable '{key}' is not set. Bot cannot start.")
        sys.exit(1)
    return val

def _require_int_env(key: str) -> int:
    raw = _require_env(key)
    try:
        return int(raw)
    except ValueError:
        log.critical(f"❌ '{key}' must be an integer, got: '{raw}'")
        sys.exit(1)

API_ID    = _require_int_env("API_ID")
API_HASH  = _require_env("API_HASH")
BOT_TOKEN = _require_env("BOT_TOKEN")

# ══════════════════════════════════════════════════════════════
#  OPTIONAL — AI FEATURES (Groq)
#  Not required to run the bot. If GROQ_API_KEY isn't set, or the
#  groq package isn't installed, all AI features are silently
#  skipped and the bot behaves exactly as before.
#
#  FIX #26 — llama-3.3-70b-versatile is deprecated by Groq
#  (shutdown 2026-08-16). Default updated to openai/gpt-oss-120b,
#  Groq's current recommended replacement. Vision default updated
#  to qwen/qwen3.6-27b — the only vision-capable model Groq
#  currently hosts (meta-llama/llama-4-scout, used in older docs/
#  examples, was shut down 2026-07-17). Override either via env
#  var if Groq's lineup changes again — check console.groq.com/docs/models.
# ══════════════════════════════════════════════════════════════
GROQ_API_KEY      = os.getenv("GROQ_API_KEY", "").strip()
GROQ_MODEL        = os.getenv("GROQ_MODEL", "openai/gpt-oss-120b").strip()
GROQ_VISION_MODEL = os.getenv("GROQ_VISION_MODEL", "qwen/qwen3.6-27b").strip()
WHISPER_MODEL     = os.getenv("GROQ_WHISPER_MODEL", "whisper-large-v3-turbo").strip()
AI_ENABLED        = bool(GROQ_API_KEY and AsyncGroq is not None)
_groq_client       = AsyncGroq(api_key=GROQ_API_KEY) if AI_ENABLED else None
AI_TIMEOUT         = 8   # seconds — captions never stall the upload pipeline
AI_VISION_TIMEOUT  = 20  # seconds — vision calls are slower

if GROQ_API_KEY and AsyncGroq is None:
    log.warning("⚠️ GROQ_API_KEY set but 'groq' package not installed — AI features disabled. "
                "Add `groq` to requirements.txt to enable.")

# ══════════════════════════════════════════════════════════════
#  OPTIONAL — ADMIN FEATURES
#  ADMIN_IDS: comma-separated Telegram user IDs, e.g. "123456,987654"
#  Leave unset to disable /stats and /broadcast for everyone.
# ══════════════════════════════════════════════════════════════
ADMIN_IDS: set[int] = set()
for _part in os.getenv("ADMIN_IDS", "").split(","):
    _part = _part.strip()
    if _part.isdigit():
        ADMIN_IDS.add(int(_part))

def _is_admin(uid: int) -> bool:
    return uid in ADMIN_IDS


# ══════════════════════════════════════════════════════════════
#  CONSTANTS & DIRS
# ══════════════════════════════════════════════════════════════
DOWNLOAD_DIR        = "downloads"
THUMB_DIR           = "thumbs"
MAX_FILE_WARN       = 1.8 * 1024 ** 3  # 1.8 GB — warn near TG limit
FFMPEG_CUT_TIMEOUT  = 600              # 10 min per segment
FFPROBE_TIMEOUT     = 30              # 30 s for duration probe
THUMB_TIMEOUT       = 15              # 15 s for thumbnail
MIN_PART_BYTES      = 1024            # FIX #21 — reject parts smaller than 1 KB
# NOTE: download idle/total timeouts removed per user request —
# downloads now run for as long as they take, with no timeout of any kind.
FFMPEG_MERGE_TIMEOUT    = 900          # 15 min for merge
FFMPEG_COMPRESS_TIMEOUT = 1800         # 30 min for re-encode/compress
FFMPEG_AUDIO_TIMEOUT    = 300          # 5 min for audio extraction
SCENE_DETECT_TIMEOUT    = 600          # 10 min — full decode pass, same budget as FFMPEG_CUT_TIMEOUT
MAX_MERGE_VIDEOS        = 20           # cap merge queue size
COMPRESS_PRESETS        = {"low": 28, "medium": 23, "high": 18}  # CRF values (lower = better quality)
WHISPER_MAX_BYTES       = 24 * 1024 * 1024   # stay under Groq's 25MB free-tier cap, with margin
WHISPER_CHUNK_SECONDS   = 1200                # 20 min per transcription chunk if audio is too big
TRANSCRIBE_TIMEOUT      = 120                 # per-chunk Whisper API timeout
HIGHLIGHT_MIN_SEC       = 20                  # floor — LLM picks are clamped/filtered around this
HIGHLIGHT_MAX_SEC       = 70                  # ceiling
HIGHLIGHT_MIN_COUNT     = 5
HIGHLIGHT_MAX_COUNT     = 10

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(THUMB_DIR,    exist_ok=True)


# ══════════════════════════════════════════════════════════════
#  FIX #13 + #22 — Stale file cleanup on startup
#  Added .wmv and .3gp patterns that were missing in v2
# ══════════════════════════════════════════════════════════════
def _cleanup_stale_files() -> None:
    patterns = [
        f"{DOWNLOAD_DIR}/video_*.mp4",  f"{DOWNLOAD_DIR}/video_*.mkv",
        f"{DOWNLOAD_DIR}/video_*.avi",  f"{DOWNLOAD_DIR}/video_*.mov",
        f"{DOWNLOAD_DIR}/video_*.webm", f"{DOWNLOAD_DIR}/video_*.wmv",
        f"{DOWNLOAD_DIR}/video_*.3gp",  f"{DOWNLOAD_DIR}/part_*.mp4",
        f"{DOWNLOAD_DIR}/merge_*.*",    f"{DOWNLOAD_DIR}/merged_*.mp4",
        f"{DOWNLOAD_DIR}/compressed_*.mp4", f"{DOWNLOAD_DIR}/trim_*.mp4",
        f"{DOWNLOAD_DIR}/audio_*.mp3",  f"{DOWNLOAD_DIR}/scene_*.mp4",
        f"{DOWNLOAD_DIR}/concat_*.txt",
        f"{THUMB_DIR}/thumb_*.jpg",     f"{THUMB_DIR}/describe_*.jpg",
    ]
    removed = 0
    for pat in patterns:
        for f in glob.glob(pat):
            try:
                os.remove(f)
                removed += 1
            except Exception:
                pass
    if removed:
        log.info(f"🧹 Cleaned {removed} stale file(s) from previous session.")

_cleanup_stale_files()


# ══════════════════════════════════════════════════════════════
#  CLIENT
# ══════════════════════════════════════════════════════════════
app = Client(
    "ultra-bot",
    api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN,
    in_memory=True,
    sleep_threshold=300,
    ipv6=False,
)


# ══════════════════════════════════════════════════════════════
#  PER-USER STATE
# ══════════════════════════════════════════════════════════════
user_files:  dict[int, str]           = {}
user_locks:  dict[int, asyncio.Lock]  = {}
user_cancel: dict[int, asyncio.Event] = {}
user_status: dict[int, dict]          = {}
user_merge_queue: dict[int, list[str]] = {}   # uid -> list of queued file paths for /merge

# ══════════════════════════════════════════════════════════════
#  STATS (in-memory — resets on restart, same as all other state here)
# ══════════════════════════════════════════════════════════════
_stats_users_seen: set[int] = set()
_stats_videos_processed     = 0
_stats_parts_created        = 0
_stats_start_time           = time.time()

def _track_video_processed() -> None:
    global _stats_videos_processed
    _stats_videos_processed += 1

def _track_part_created() -> None:
    global _stats_parts_created
    _stats_parts_created += 1

# FIX #23 — Prune throttled: track last prune time, don't prune on every call
_MAX_USERS        = 2000
_last_prune_time  = 0.0
_PRUNE_INTERVAL   = 300  # prune at most once every 5 minutes

def _prune_state() -> None:
    """
    Drop oldest inactive users when state grows too large.
    FIX #17: Never prune a user whose lock is currently held (task running).
    FIX #23: Throttled — runs at most once per 5 minutes.
    """
    global _last_prune_time
    now = time.time()
    if len(user_locks) < _MAX_USERS:
        return
    if now - _last_prune_time < _PRUNE_INTERVAL:
        return
    _last_prune_time = now

    # FIX #17 — exclude users with active locks from pruning
    active = {uid for uid, lk in user_locks.items() if lk.locked()}
    inactive = [uid for uid in list(user_locks.keys()) if uid not in active]
    # Drop oldest half of inactive users
    to_remove = set(inactive[:len(inactive) // 2])

    for d in (user_locks, user_cancel, user_status):
        for k in list(to_remove):
            d.pop(k, None)
    for k in list(to_remove):
        path = user_files.pop(k, None)
        if path:
            try: os.remove(path)
            except: pass
        queue = user_merge_queue.pop(k, None)
        if queue:
            for p in queue:
                try:
                    if os.path.exists(p): os.remove(p)
                except: pass

    log.info(f"🧹 Pruned {len(to_remove)} inactive user states.")


def _get_lock(uid: int) -> asyncio.Lock:
    _prune_state()
    if uid not in user_locks:
        user_locks[uid] = asyncio.Lock()
    return user_locks[uid]

def _get_cancel(uid: int) -> asyncio.Event:
    if uid not in user_cancel:
        user_cancel[uid] = asyncio.Event()
    return user_cancel[uid]

def _set_status(uid: int, task: str, detail: str = "") -> None:
    user_status[uid] = {"task": task, "detail": detail, "since": time.time()}

def _clear_status(uid: int) -> None:
    user_status.pop(uid, None)

def _uid(message) -> int | None:
    """Return user id, or None for channel posts / anonymous senders."""
    uid = message.from_user.id if message.from_user else None
    if uid is not None:
        _stats_users_seen.add(uid)
    return uid


# ══════════════════════════════════════════════════════════════
#  DEDUP  (FIX #16 — correct eviction order)
#
#  v2 BUG:  eviction checked _seen_deque[0] AFTER append.
#           When deque (maxlen=500) is full, append auto-pops
#           the OLDEST item from the deque. So _seen_deque[0]
#           after append is the 2nd-oldest — wrong key discarded
#           from _seen_set → set could grow unbounded.
#
#  v3 FIX:  check len BEFORE append. If deque is full, grab
#           the item that WILL be evicted (index 0), remove it
#           from the set, THEN append the new key.
# ══════════════════════════════════════════════════════════════
_seen_set:       set               = set()
_seen_deque:     deque             = deque(maxlen=500)
_seen_lock_obj:  asyncio.Lock | None = None

def _get_seen_lock() -> asyncio.Lock:
    global _seen_lock_obj
    if _seen_lock_obj is None:
        _seen_lock_obj = asyncio.Lock()
    return _seen_lock_obj

async def _dedup(message) -> bool:
    """Returns True if this exact message was already processed."""
    key = (message.chat.id, message.id)
    async with _get_seen_lock():
        if key in _seen_set:
            return True
        # FIX #16 — evict the item that WILL be auto-popped BEFORE appending
        if len(_seen_deque) == _seen_deque.maxlen:
            _seen_set.discard(_seen_deque[0])  # this is the one about to leave
        _seen_set.add(key)
        _seen_deque.append(key)
        return False


# ══════════════════════════════════════════════════════════════
#  PROGRESS ENGINE
#  FIX #34 — /cancel didn't actually stop an in-flight download/upload.
#  Root cause: progress() only checked the cancel Event to skip editing
#  the status message; the underlying message.download()/reply_video()
#  call from Pyrogram kept running regardless, since nothing ever told
#  it to stop. Result: user taps /cancel -> "Stopping at next
#  checkpoint..." -> nothing happens (no checkpoint exists inside a
#  single download) -> lock stays held -> /clear also refuses ("finish
#  /cancel first") -> user is stuck for up to the 30 min hard timeout.
#  FIX: raise _UserCancelled from inside progress(); Pyrogram calls this
#  callback synchronously from within its own chunk-read loop, so a
#  raised exception propagates straight out of .download()/.reply_video()
#  and aborts the transfer immediately instead of only skipping the UI.
# ══════════════════════════════════════════════════════════════
class _UserCancelled(Exception):
    pass

THROTTLE  = 0.5
EMA_ALPHA = 0.35
SPINNER   = ["⠋","⠙","⠹","⠸","⠼","⠴","⠦","⠧","⠇","⠏"]

_last_edit: dict[int, float] = {}
_ema_speed: dict[int, float] = {}
_spin_idx:  dict[int, int]   = {}
_shown_pct: dict[int, float] = {}

def _reset(uid: int) -> None:
    for d in (_last_edit, _ema_speed, _spin_idx, _shown_pct):
        d.pop(uid, None)

def _sz(b: float) -> str:
    b = max(0.0, b)  # guard negative
    for u in ("B","KB","MB","GB"):
        if b < 1024: return f"{b:.1f} {u}"
        b /= 1024
    return f"{b:.1f} TB"

def _eta(s: float) -> str:
    s = max(0.0, s)
    if s >= 3600: return f"{int(s//3600)}h {int(s%3600//60)}m"
    if s >= 60:   return f"{int(s//60)}m {int(s%60)}s"
    return f"{int(s)}s"

def _bar(pct: float, w: int = 20) -> str:
    pct = max(0.0, min(100.0, pct))
    n = int(pct / 100 * w)
    return "[" + "█" * n + "░" * (w - n) + "]"

def _parse_time(s: str) -> float | None:
    """Parse 'HH:MM:SS', 'MM:SS', or plain seconds into float seconds. None if invalid."""
    s = s.strip()
    try:
        if ":" in s:
            pieces = [float(p) for p in s.split(":")]
            if len(pieces) == 3:
                h, m, sec = pieces
                return h * 3600 + m * 60 + sec
            if len(pieces) == 2:
                m, sec = pieces
                return m * 60 + sec
            return None
        return float(s)
    except (ValueError, TypeError):
        return None

def _badge(pct: float) -> str:
    return ("🏁" if pct>=100 else "🔥" if pct>=80 else
            "⚡" if pct>=60 else "🚀" if pct>=40 else
            "💫" if pct>=20 else "🌀")

def _count_up(uid: int, real: float, step: float = 1.8) -> float:
    prev  = _shown_pct.get(uid, 0.0)
    shown = min(real, prev + step) if real > prev else real
    _shown_pct[uid] = shown
    return shown

async def _safe_edit(msg, text: str, reply_markup=None) -> None:
    try:
        await msg.edit(text, reply_markup=reply_markup)
    except FloodWait as e:
        await asyncio.sleep(e.value + 0.5)
        try: await msg.edit(text, reply_markup=reply_markup)
        except: pass
    except MessageNotModified:
        pass
    except Exception:
        pass

async def progress(current, total, msg, start, uid: int = 0,
                   mode: str = "📥 Download") -> None:
    if _get_cancel(uid).is_set():
        raise _UserCancelled()
    if not isinstance(total, (int, float)) or total <= 0: return
    now     = time.time()
    elapsed = max(now - start, 0.001)
    # FIX — liveness signal (used by the idle watchdog) must update on
    # EVERY progress() call, not only when we actually edit the message.
    # Previously this line ran only after the throttle check below, so
    # on large (2GB+) files any callback that landed inside the 0.5s
    # UI-throttle window never refreshed _last_edit — bytes were still
    # flowing but the watchdog saw a stale timestamp and eventually
    # misfired a false "stalled" abort. Splitting the two concerns
    # fixes it: liveness updates unconditionally, UI-edit stays throttled.
    prev_edit = _last_edit.get(uid, 0.0)
    _last_edit[uid] = now
    if now - prev_edit < THROTTLE and prev_edit != 0.0:
        return
    raw   = current / elapsed
    ema   = EMA_ALPHA * raw + (1 - EMA_ALPHA) * _ema_speed.get(uid, raw)
    _ema_speed[uid] = ema
    eta_s = (total - current) / ema if ema > 0 else 0
    real  = current * 100 / total
    shown = _count_up(uid, real)
    spin  = SPINNER[_spin_idx.get(uid, 0) % len(SPINNER)]
    _spin_idx[uid] = _spin_idx.get(uid, 0) + 1
    kb    = ema / 1024
    tier  = "🟢 Fast" if kb >= 1024 else ("🟡 Good" if kb >= 256 else "🔴 Slow")
    await _safe_edit(msg,
        f"{spin} **{mode}**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{_bar(shown)}\n"
        f"  {_badge(shown)} **{shown:.1f}%** ·  {_sz(current)} / {_sz(total)}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"  🚄 **Speed** : `{_sz(ema)}/s`  {tier}\n"
        f"  ⏱ **ETA** : `{_eta(eta_s)}`\n"
        f"  ⏳ **Elapsed** : `{_eta(elapsed)}`\n"
        f"  ❌ /cancel to stop"
    )

async def upload_progress(current, total, msg, start, uid: int = 0) -> None:
    await progress(current, total, msg, start, uid=uid, mode="⬆️ Upload")


# ══════════════════════════════════════════════════════════════
#  FIX #35 — IDLE WATCHDOG for downloads
#  The OLD 30-min asyncio.wait_for() only caught a stall once the WHOLE
#  operation had run 30 min — a connection that dies 2 min in still sat
#  there frozen for the remaining ~28 min with nothing watching it, which
#  is exactly the "same %, same elapsed time, for 50+ minutes" freeze
#  reported (and stale files/stuck locks piled up the same way).
#
#  UPDATE (user request) — both the idle-stall check and the absolute
#  30-min cap have been removed entirely. Downloads now run for as long
#  as they take, with no timeout of any kind. Note: a genuinely dead
#  connection will now hang forever (holding the user's lock) since
#  nothing aborts it anymore — this is the tradeoff of "unlimited time".
# ══════════════════════════════════════════════════════════════
class _DownloadStalled(Exception):
    """No longer raised anywhere — kept only so any stray references don't break."""
    pass

async def _await_with_watchdog(coro, uid: int):
    # No timeout logic — just run the download to completion, however long it takes.
    return await coro


# ══════════════════════════════════════════════════════════════
#  FFMPEG HELPERS
#  FIX #18 — proc.wait() after every proc.kill() to reap zombies
# ══════════════════════════════════════════════════════════════
async def _kill_proc(proc) -> None:
    """Kill a subprocess and wait for it to avoid zombie processes."""
    try:
        proc.kill()
    except ProcessLookupError:
        pass  # already dead
    try:
        await asyncio.wait_for(proc.wait(), timeout=5)
    except asyncio.TimeoutError:
        pass  # best effort

async def ffmpeg_cut(inp: str, out: str, ss: float, t: float) -> bool:
    """Cut a video segment. Returns True on success."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-ss", str(ss), "-i", inp,
            "-t", str(t), "-c", "copy", "-avoid_negative_ts", "make_zero", out,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            _, stderr_data = await asyncio.wait_for(
                proc.communicate(), timeout=FFMPEG_CUT_TIMEOUT
            )
        except asyncio.TimeoutError:
            await _kill_proc(proc)  # FIX #18
            log.error(f"ffmpeg_cut timed out ({FFMPEG_CUT_TIMEOUT}s): {inp}")
            return False
        if proc.returncode != 0:
            log.error(f"ffmpeg_cut rc={proc.returncode}: {stderr_data.decode()[-400:]}")
        return proc.returncode == 0
    except Exception as e:
        log.error(f"ffmpeg_cut exception: {e}")
        return False

async def make_thumb(video: str, ss: float, out: str) -> str | None:
    """Generate thumbnail image. Returns path or None."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-ss", str(ss), "-i", video,
            "-vframes", "1", "-q:v", "2", "-vf", "scale=320:-1", out,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            await asyncio.wait_for(proc.wait(), timeout=THUMB_TIMEOUT)
        except asyncio.TimeoutError:
            await _kill_proc(proc)  # FIX #18
            return None
        return out if os.path.exists(out) else None
    except Exception:
        return None

async def get_duration(file: str) -> float | None:
    """Get video duration in seconds. Returns None on failure."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1", file,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            out, _ = await asyncio.wait_for(
                proc.communicate(), timeout=FFPROBE_TIMEOUT
            )
        except asyncio.TimeoutError:
            await _kill_proc(proc)  # FIX #18
            log.error(f"ffprobe timed out: {file}")
            return None
        raw = out.decode().strip()
        return float(raw) if raw and raw != "N/A" else None
    except Exception:
        return None


# ══════════════════════════════════════════════════════════════
#  ADVANCED FFMPEG OPS — merge, compress, extract audio, scene detect
# ══════════════════════════════════════════════════════════════
async def _emit_ffmpeg_progress(uid: int, status_msg, label: str, snap: dict[str, str],
                                t0: float, total_duration: float) -> None:
    """Shared renderer for ffmpeg `-progress pipe:1` snapshots — same visual
    language (bar/badge/spinner/speed/ETA) as the download/upload `progress()`
    engine, so every long-running operation in the bot looks and feels the
    same. Used by compress, merge, and scene-detection.

    FIX #32 — reads `out_time_us` (ffmpeg's newer, unambiguously-named field)
    rather than the legacy `out_time_ms` — verified empirically that both
    carry the identical microsecond value, but `_us` doesn't need a comment
    explaining a naming quirk. Rate is computed from OUR OWN elapsed-time /
    video-seconds-processed ratio (EMA-smoothed like the download/upload
    speed) rather than trusting ffmpeg's own `speed=` field, which can read
    `N/A` or lag right after a seek/keyframe.
    """
    now = time.time()
    if now - _last_edit.get(uid, 0.0) < THROTTLE and _last_edit.get(uid, 0.0) != 0.0:
        return
    _last_edit[uid] = now
    elapsed = max(now - t0, 0.001)

    out_us = snap.get("out_time_us", "")
    try:
        out_sec = max(0.0, float(out_us) / 1_000_000) if out_us and out_us != "N/A" else 0.0
    except ValueError:
        out_sec = 0.0

    raw_rate = out_sec / elapsed
    ema = EMA_ALPHA * raw_rate + (1 - EMA_ALPHA) * _ema_speed.get(uid, raw_rate)
    _ema_speed[uid] = ema

    pct   = min(100.0, out_sec * 100 / total_duration) if total_duration > 0 else 0.0
    shown = _count_up(uid, pct)
    spin  = SPINNER[_spin_idx.get(uid, 0) % len(SPINNER)]
    _spin_idx[uid] = _spin_idx.get(uid, 0) + 1

    eta_s   = (total_duration - out_sec) / ema if ema > 0 else 0.0
    eta_txt = _eta(eta_s) if eta_s < 359999 else "calculating…"  # guard absurd values

    await _safe_edit(status_msg,
        f"{spin} **{label}**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{_bar(shown)}\n"
        f"  {_badge(shown)} **{shown:.1f}%**  ·  `{_eta(out_sec)}` / `{_eta(total_duration)}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"  🚄 **Speed** : `{ema:.2f}x realtime`\n"
        f"  ⏱ **ETA** : `{eta_txt}`  ·  ⏳ `{_eta(elapsed)}`\n"
        f"  ❌ /cancel to stop"
    )


async def _run_ffmpeg_polled(args: list[str], uid: int, status_msg, label: str,
                             timeout: int, total_duration: float | None = None) -> bool:
    """Run an ffmpeg command, updating status_msg live, honoring /cancel.
    Returns True iff ffmpeg exits with code 0.

    FIX #32 — when `total_duration` (seconds) is known, streams the full
    bar/%/speed/ETA UI via `-progress pipe:1` (see _emit_ffmpeg_progress),
    correctly batching the several key=value lines ffmpeg emits per snapshot
    and only rendering once the `progress=` marker line closes that batch
    (rather than reacting to each field the instant it's seen, which can
    pair a fresh position with stale size/speed fields from the previous
    snapshot). When duration isn't known, or if progress lines stop arriving
    for a few seconds (e.g. a build without -progress support, or a stalled
    pipe), falls back to the original elapsed-time + spinner display — same
    safety net as before, never blocks either way.

    FIX #28 — ffmpeg writes continuous stats to stderr by default; if nobody
    drains that pipe, the OS buffer (~64KB) fills up and ffmpeg blocks on
    write(), hanging forever even though we're "polling" via proc.wait().
    -loglevel error -nostats silences the routine spew, and a background
    task drains stderr as a belt-and-suspenders fix, keeping only the tail
    for error logging.
    """
    use_progress = bool(total_duration and total_duration > 0)
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-loglevel", "error", "-nostats",
            *(["-progress", "pipe:1"] if use_progress else []),
            *args,
            stdout=asyncio.subprocess.PIPE if use_progress else asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
    except Exception as e:
        log.error(f"{label} failed to start: {e}")
        return False

    stderr_chunks: list[bytes] = []

    async def _drain_stderr():
        try:
            while True:
                chunk = await proc.stderr.read(4096)
                if not chunk:
                    break
                stderr_chunks.append(chunk)
                if sum(len(c) for c in stderr_chunks) > 8192:
                    stderr_chunks[:] = [b"".join(stderr_chunks)[-4096:]]
        except Exception:
            pass

    _reset(uid)
    t0 = time.time()
    spin_i = 0
    last_progress_seen = t0
    drain_task = asyncio.create_task(_drain_stderr())
    progress_task = None

    if use_progress:
        async def _read_stdout_progress():
            nonlocal last_progress_seen
            buf = b""
            snap: dict[str, str] = {}
            try:
                while True:
                    chunk = await proc.stdout.read(4096)
                    if not chunk:
                        break
                    buf += chunk
                    while b"\n" in buf:
                        line, buf = buf.split(b"\n", 1)
                        s = line.decode(errors="ignore").strip()
                        if "=" not in s:
                            continue
                        k, _, v = s.partition("=")
                        snap[k] = v
                        if k == "progress":
                            last_progress_seen = time.time()
                            await _emit_ffmpeg_progress(uid, status_msg, label, snap, t0, total_duration)
                            snap = {}
            except Exception:
                pass
        progress_task = asyncio.create_task(_read_stdout_progress())

    try:
        while True:
            if _get_cancel(uid).is_set():
                await _kill_proc(proc)
                return False
            if time.time() - t0 > timeout:
                await _kill_proc(proc)
                log.error(f"{label} timed out ({timeout}s)")
                return False
            try:
                await asyncio.wait_for(proc.wait(), timeout=2)
                break  # process exited
            except asyncio.TimeoutError:
                # Fallback spinner — only if real progress lines aren't
                # actually coming through (no duration, or a stale/stuck feed)
                if not use_progress or (time.time() - last_progress_seen > 4):
                    spin = SPINNER[spin_i % len(SPINNER)]
                    spin_i += 1
                    await _safe_edit(status_msg,
                        f"{spin} **{label}**\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"  ⏳ Elapsed: `{_eta(time.time()-t0)}`\n"
                        f"  ❌ /cancel to stop"
                    )
    finally:
        drain_task.cancel()
        if progress_task:
            progress_task.cancel()
        for t in (drain_task, progress_task):
            if t is None:
                continue
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass

    if proc.returncode != 0:
        tail = b"".join(stderr_chunks).decode(errors="ignore")[-400:]
        log.error(f"{label} rc={proc.returncode}: {tail}")
    return proc.returncode == 0


async def ffmpeg_merge(file_list: list[str], out: str, uid: int, status_msg,
                       timeout: int = FFMPEG_MERGE_TIMEOUT) -> bool:
    """Merge videos in order using ffmpeg's concat demuxer. Falls back to
    a re-encoding concat if stream-copy fails (common with mismatched codecs).

    FIX #32 — total duration (summed across all inputs, probed in PARALLEL so
    one slow ffprobe can't serialize the rest) is computed internally and fed
    to _run_ffmpeg_polled, so both the stream-copy attempt and the re-encode
    fallback show a real %/speed/ETA bar instead of just an elapsed spinner.
    If any single probe fails, we fall back to the spinner entirely rather
    than show a percentage computed from incomplete data.
    """
    concat_path = f"{DOWNLOAD_DIR}/concat_{uid}.txt"
    try:
        durations = await asyncio.gather(*(get_duration(fp) for fp in file_list))
        total_duration = sum(durations) if all(durations) else None

        with open(concat_path, "w") as f:
            for fp in file_list:
                safe = os.path.abspath(fp).replace("'", "'\\''")
                f.write(f"file '{safe}'\n")

        ok = await _run_ffmpeg_polled(
            ["-f", "concat", "-safe", "0", "-i", concat_path, "-c", "copy", out],
            uid, status_msg, f"Merging {len(file_list)} videos…", timeout,
            total_duration=total_duration,
        )
        if not ok and not _get_cancel(uid).is_set():
            log.info("Stream-copy merge failed — retrying with re-encode fallback.")
            if os.path.exists(out):
                try: os.remove(out)
                except: pass
            ok = await _run_ffmpeg_polled(
                ["-f", "concat", "-safe", "0", "-i", concat_path,
                 "-c:v", "libx264", "-preset", "veryfast", "-c:a", "aac", out],
                uid, status_msg, f"Merging {len(file_list)} videos (re-encode)…", timeout,
                total_duration=total_duration,
            )
        return ok
    except Exception as e:
        log.error(f"ffmpeg_merge exception: {e}")
        return False
    finally:
        try: os.remove(concat_path)
        except: pass


async def ffmpeg_compress(inp: str, out: str, crf: int, uid: int, status_msg,
                          timeout: int = FFMPEG_COMPRESS_TIMEOUT) -> bool:
    """Re-encode at given CRF to shrink file size.

    FIX #32 — duration is probed internally to feed _run_ffmpeg_polled's live
    %/speed/ETA bar; if the probe fails for any reason this just falls back
    to the elapsed-time spinner, so a probe failure never blocks compression.
    """
    dur = await get_duration(inp)
    return await _run_ffmpeg_polled(
        ["-i", inp, "-c:v", "libx264", "-crf", str(crf), "-preset", "veryfast",
         "-c:a", "aac", "-b:a", "128k", out],
        uid, status_msg, "Compressing…", timeout, total_duration=dur,
    )


async def ffmpeg_extract_audio(inp: str, out: str,
                               timeout: int = FFMPEG_AUDIO_TIMEOUT) -> bool:
    """Extract audio track as MP3. No progress UI — usually fast."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-i", inp, "-vn", "-acodec", "libmp3lame", "-q:a", "2", out,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            _, stderr_data = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        except asyncio.TimeoutError:
            await _kill_proc(proc)
            log.error(f"ffmpeg_extract_audio timed out ({timeout}s): {inp}")
            return False
        if proc.returncode != 0:
            log.error(f"ffmpeg_extract_audio rc={proc.returncode}: {stderr_data.decode(errors='ignore')[-400:]}")
        return proc.returncode == 0
    except Exception as e:
        log.error(f"ffmpeg_extract_audio exception: {e}")
        return False


async def detect_scenes(file: str, uid: int, status_msg, total_duration: float,
                        threshold: float = 10.0, max_cuts: int = 99) -> list[float]:
    """Detect scene-change timestamps using ffmpeg's `scdet` filter.

    FIX #29 — originally used the classic `select='gt(scene,X)'` heuristic
    with threshold 0.4 (the commonly cited default). Testing this against
    real cut points revealed it can score genuine hard cuts as ~0 on some
    content (its metric leans on texture/edge complexity, not raw pixel
    difference), so it can silently find nothing on legitimate cuts.
    `scdet` is ffmpeg's purpose-built, modern replacement — testing showed
    a real cut scoring ~26 against ~0.0–0.4 for ordinary in-scene motion,
    a much cleaner separation. We log every frame's score in one pass
    (threshold=0 so nothing is filtered at the ffmpeg level), then apply
    our own threshold in Python — with an adaptive step-down if nothing
    clears it, since natural score distributions vary a lot by content
    type (animation vs. live-action vs. screen recordings, etc.).

    FIX #32 — this used to run silently for up to SCENE_DETECT_TIMEOUT
    (10 min) with zero feedback beyond the initial "Scanning…" message. Now
    streams the same live bar/%/speed/ETA UI as compress/merge via
    `-progress pipe:1` on stdout, while stderr — needed in FULL, every
    frame's scdet score/time debug line, not just a tail — is drained
    concurrently on its own task instead of one blocking communicate() call.
    A size cap still guards memory on very long/high-framerate videos (past
    the cap, scoring data is silently dropped rather than crashing — a very
    long scan may only find cuts in the portion that fit).
    """
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-i", file,
            "-filter:v", "scdet=threshold=0",
            "-f", "null", "-",
            "-loglevel", "debug",
            "-progress", "pipe:1", "-nostats",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except Exception as e:
        log.error(f"detect_scenes failed to start: {e}")
        return []

    MAX_STDERR_BYTES = 24 * 1024 * 1024  # safety cap — debug logging on very long videos
    stderr_chunks: list[bytes] = []
    stderr_total = 0

    async def _drain_stderr_full():
        nonlocal stderr_total
        try:
            while True:
                chunk = await proc.stderr.read(65536)
                if not chunk:
                    break
                stderr_total += len(chunk)
                if stderr_total <= MAX_STDERR_BYTES:
                    stderr_chunks.append(chunk)
        except Exception:
            pass

    async def _read_stdout_progress():
        buf = b""
        snap: dict[str, str] = {}
        try:
            while True:
                chunk = await proc.stdout.read(4096)
                if not chunk:
                    break
                buf += chunk
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    s = line.decode(errors="ignore").strip()
                    if "=" not in s:
                        continue
                    k, _, v = s.partition("=")
                    snap[k] = v
                    if k == "progress":
                        await _emit_ffmpeg_progress(uid, status_msg, "🔍 Scanning for scenes…",
                                                    snap, t0, total_duration)
                        snap = {}
        except Exception:
            pass

    _reset(uid)
    t0 = time.time()
    drain_task = asyncio.create_task(_drain_stderr_full())
    progress_task = asyncio.create_task(_read_stdout_progress())

    try:
        while True:
            if _get_cancel(uid).is_set():
                await _kill_proc(proc)
                return []
            if time.time() - t0 > SCENE_DETECT_TIMEOUT:
                await _kill_proc(proc)
                log.error(f"detect_scenes timed out: {file}")
                return []
            try:
                await asyncio.wait_for(proc.wait(), timeout=1)
                break
            except asyncio.TimeoutError:
                continue
    finally:
        drain_task.cancel()
        progress_task.cancel()
        for t in (drain_task, progress_task):
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass

    try:
        text = b"".join(stderr_chunks).decode(errors="ignore")
        pairs = re.findall(
            r"lavfi\.scd\.score:\s*([\d.]+),\s*lavfi\.scd\.time:\s*([\d.]+)", text
        )
        if not pairs:
            log.warning(f"detect_scenes: no scdet output parsed for {file}")
            return []

        scored = [(float(s), float(t)) for s, t in pairs]

        cuts: list[float] = []
        for th in (threshold, threshold / 2, threshold / 5, threshold / 10):
            cuts = sorted(t for s, t in scored if s >= th)
            if cuts:
                break

        if len(cuts) > max_cuts:
            step = math.ceil(len(cuts) / max_cuts)
            cuts = cuts[::step]
        return cuts
    except Exception as e:
        log.error(f"detect_scenes exception: {e}")
        return []


# ══════════════════════════════════════════════════════════════
#  AI HIGHLIGHT EXTRACTION — /highlights
#  Content-aware alternative to /splitscene. Instead of cutting at every
#  visual shot change (which can produce dozens of tiny, meaningless
#  clips on heavily-edited content like TV/movies), this transcribes the
#  dialogue and asks the LLM to pick a handful of genuinely "best" /
#  shareable moments — the way a person skimming a transcript would.
# ══════════════════════════════════════════════════════════════
async def _cut_audio(inp: str, out: str, start: float, dur: float,
                     timeout: int = FFMPEG_CUT_TIMEOUT) -> bool:
    """Stream-copy cut of an audio-only file (used for chunking before
    transcription) — deliberately separate from ffmpeg_cut (video-oriented)
    to avoid any cross-contamination between the two code paths."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-ss", str(start), "-i", inp, "-t", str(dur), "-c", "copy", out,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            await asyncio.wait_for(proc.communicate(), timeout=timeout)
        except asyncio.TimeoutError:
            await _kill_proc(proc)
            return False
        return proc.returncode == 0
    except Exception as e:
        log.error(f"_cut_audio exception: {e}")
        return False


async def _extract_speech_audio(inp: str, out: str,
                                timeout: int = FFMPEG_AUDIO_TIMEOUT) -> bool:
    """Extract a small, speech-optimized mono track (16kHz, low bitrate) —
    this is for transcription accuracy/size, not listening quality, so it
    stays well under Groq's per-file size limit for as long as possible."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-i", inp, "-vn", "-ar", "16000", "-ac", "1",
            "-c:a", "libmp3lame", "-b:a", "48k", out,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            _, stderr_data = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        except asyncio.TimeoutError:
            await _kill_proc(proc)
            return False
        if proc.returncode != 0:
            log.error(f"_extract_speech_audio rc={proc.returncode}: {stderr_data.decode(errors='ignore')[-300:]}")
        return proc.returncode == 0
    except Exception as e:
        log.error(f"_extract_speech_audio exception: {e}")
        return False


async def _transcribe_chunk(path: str) -> list[dict]:
    """Transcribe one audio file (must already be under Groq's size limit)
    via Whisper. Returns [{start, end, text}, ...] with LOCAL timestamps
    (relative to the start of this specific file) — [] on any failure."""
    if not AI_ENABLED:
        return []
    try:
        with open(path, "rb") as f:
            data = f.read()
        resp = await asyncio.wait_for(
            _groq_client.audio.transcriptions.create(
                file=(os.path.basename(path), data),
                model=WHISPER_MODEL,
                response_format="verbose_json",
                timestamp_granularities=["segment"],
            ),
            timeout=TRANSCRIBE_TIMEOUT,
        )
        segments = getattr(resp, "segments", None)
        if segments is None and isinstance(resp, dict):
            segments = resp.get("segments", [])
        out = []
        for seg in (segments or []):
            try:
                if isinstance(seg, dict):
                    out.append({"start": float(seg["start"]), "end": float(seg["end"]),
                               "text": str(seg["text"])})
                else:
                    out.append({"start": float(seg.start), "end": float(seg.end),
                               "text": str(seg.text)})
            except (KeyError, AttributeError, ValueError, TypeError):
                continue
        return out
    except asyncio.TimeoutError:
        log.warning(f"Whisper transcription timed out: {path}")
        return []
    except Exception as e:
        log.warning(f"Whisper transcription failed: {e}")
        return []


async def _transcribe_video(file: str, dur: float, uid: int, status_msg) -> list[dict]:
    """Extract speech audio, chunk it if it's too big for a single Whisper
    call, transcribe each piece, and merge into one list of {start, end,
    text} segments with GLOBAL timestamps (chunk-offset corrected)."""
    audio_path = f"{DOWNLOAD_DIR}/speech_{uid}.mp3"
    ok = await _extract_speech_audio(file, audio_path)
    if not ok or not os.path.exists(audio_path):
        return []

    size = os.path.getsize(audio_path)
    if size <= WHISPER_MAX_BYTES:
        segments = await _transcribe_chunk(audio_path)
        try: os.remove(audio_path)
        except: pass
        return segments

    # Too big for one call — chunk and offset timestamps
    all_segments: list[dict] = []
    n_chunks = math.ceil(dur / WHISPER_CHUNK_SECONDS)
    for i in range(n_chunks):
        if _get_cancel(uid).is_set():
            break
        start = i * WHISPER_CHUNK_SECONDS
        seg_len = min(WHISPER_CHUNK_SECONDS, dur - start)
        if seg_len <= 0:
            break
        chunk_path = f"{DOWNLOAD_DIR}/speech_{uid}_c{i}.mp3"
        await _safe_edit(status_msg, f"🎙 **Transcribing…** chunk {i+1}/{n_chunks}")
        cok = await _cut_audio(audio_path, chunk_path, start, seg_len)
        if cok and os.path.exists(chunk_path):
            segs = await _transcribe_chunk(chunk_path)
            for s in segs:
                all_segments.append({"start": s["start"] + start, "end": s["end"] + start,
                                     "text": s["text"]})
            try: os.remove(chunk_path)
            except: pass
    try: os.remove(audio_path)
    except: pass
    return all_segments


async def _select_highlights(segments: list[dict], duration: float) -> list[dict]:
    """Ask the LLM to pick the 5-10 best highlight-worthy moments from a
    timestamped transcript. Returns [{start, end, reason}, ...], validated,
    clamped to sane durations, de-overlapped, and ranked-order preserved.
    Empty list on any failure — caller shows a graceful fallback message."""
    if not AI_ENABLED or not segments:
        return []

    lines = []
    for s in segments:
        m, sec = divmod(int(s["start"]), 60)
        lines.append(f"[{m:02d}:{sec:02d}] {s['text'].strip()}")
    transcript = "\n".join(lines)
    if len(transcript) > 60000:  # guard against blowing the LLM's context window
        transcript = transcript[:60000] + "\n...[transcript truncated]"

    prompt = (
        f"You are selecting the best highlight clips from a {duration/60:.0f}-minute "
        f"video for social media, aiming for maximum shareable/viral potential.\n\n"
        f"Timestamped transcript:\n{transcript}\n\n"
        f"Select {HIGHLIGHT_MIN_COUNT} to {HIGHLIGHT_MAX_COUNT} of the BEST moments as "
        f"standalone clips. Rules:\n"
        f"- Each clip must be {HIGHLIGHT_MIN_SEC}-{HIGHLIGHT_MAX_SEC} seconds long\n"
        f"- Pick moments with strong drama, emotion, comedy, conflict, a punchline, "
        f"a twist, or a cliffhanger — not random ordinary dialogue\n"
        f"- Prefer self-contained moments that make sense without extra context\n"
        f"- Clips must not overlap\n"
        f"- Order by how good/viral-worthy each is, best first\n\n"
        f'Respond ONLY with JSON in this exact shape: '
        f'{{"highlights": [{{"start": 123.4, "end": 165.0, "reason": "short reason, max 10 words"}}, ...]}}'
    )
    try:
        resp = await asyncio.wait_for(
            _groq_client.chat.completions.create(
                model=GROQ_MODEL,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=1200,
                temperature=0.4,
                response_format={"type": "json_object"},
            ),
            timeout=60,
        )
        data = json.loads(resp.choices[0].message.content)
        items = data.get("highlights", []) if isinstance(data, dict) else []
    except asyncio.TimeoutError:
        log.warning("_select_highlights timed out.")
        return []
    except Exception as e:
        log.warning(f"_select_highlights LLM call/parse failed: {e}")
        return []

    picked = []
    for it in items:
        try:
            start = float(it["start"])
            end = float(it["end"])
            reason = str(it.get("reason", "")).strip()[:80]
        except (KeyError, TypeError, ValueError):
            continue
        if start < 0 or end <= start or end > duration + 1:
            continue
        if end - start > HIGHLIGHT_MAX_SEC:           # never exceed the user's stated ceiling
            end = start + HIGHLIGHT_MAX_SEC
        if end - start < HIGHLIGHT_MIN_SEC * 0.5:     # way too short to be a real pick — discard
            continue
        picked.append({"start": start, "end": min(end, duration), "reason": reason or "Highlight moment"})

    # Remove overlaps (keep earlier/higher-ranked picks), preserve original rank order
    picked_by_time = sorted(range(len(picked)), key=lambda i: picked[i]["start"])
    keep_idx = set()
    last_end = -1.0
    for i in picked_by_time:
        if picked[i]["start"] >= last_end - 1:
            keep_idx.add(i)
            last_end = picked[i]["end"]
    final = [picked[i] for i in range(len(picked)) if i in keep_idx]
    return final[:HIGHLIGHT_MAX_COUNT]


# ══════════════════════════════════════════════════════════════
#  AI CAPTION + DESCRIPTION (optional, Groq) — never blocks/crashes
# ══════════════════════════════════════════════════════════════
async def _ai_caption(orig_filename: str, num: int, total: int) -> str | None:
    """Return a short punchy caption for this part, or None on any failure/timeout."""
    if not AI_ENABLED:
        return None
    try:
        prompt = (
            f"Write ONE short, punchy Telegram caption (max 12 words, 1 emoji max) "
            f"for part {num} of {total} of a video file named '{orig_filename}'. "
            f"No hashtags, no quotes, just the caption text."
        )
        resp = await asyncio.wait_for(
            _groq_client.chat.completions.create(
                model=GROQ_MODEL,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=40,
                temperature=0.8,
            ),
            timeout=AI_TIMEOUT,
        )
        text = resp.choices[0].message.content.strip().strip('"')
        return text if text else None
    except asyncio.TimeoutError:
        log.warning(f"AI caption timed out (part {num}/{total}) — using fallback.")
        return None
    except Exception as e:
        log.warning(f"AI caption failed (part {num}/{total}): {e} — using fallback.")
        return None


async def _ai_describe(thumb_path: str) -> str | None:
    """Generate a short AI description of a video from one representative frame.
    Returns None on any failure — caller should show a graceful fallback message."""
    if not AI_ENABLED:
        return None
    try:
        with open(thumb_path, "rb") as f:
            img_b64 = base64.b64encode(f.read()).decode()
        resp = await asyncio.wait_for(
            _groq_client.chat.completions.create(
                model=GROQ_VISION_MODEL,
                messages=[{
                    "role": "user",
                    "content": [
                        {"type": "text", "text": (
                            "Describe what's likely happening in this video in one "
                            "short sentence (max 15 words), based on this frame."
                        )},
                        {"type": "image_url",
                         "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}},
                    ],
                }],
                max_tokens=60,
            ),
            timeout=AI_VISION_TIMEOUT,
        )
        text = resp.choices[0].message.content.strip()
        return text if text else None
    except asyncio.TimeoutError:
        log.warning("AI describe timed out.")
        return None
    except Exception as e:
        log.warning(f"AI describe failed: {e} — vision model may be unavailable; "
                    f"check GROQ_VISION_MODEL against console.groq.com/docs/vision")
        return None


async def _ai_caption_from_frame(thumb_path: str, num: int, total: int) -> str | None:
    """FIX #30 — per-part captions now analyze that part's OWN thumbnail frame
    via the vision model, instead of only guessing from the filename. Gives a
    genuinely content-aware caption per part (e.g. reacts to what's actually
    on screen at that timestamp) rather than a generic "Part N" filler line.
    Returns None on any failure/timeout — caller falls back to the older,
    filename-only _ai_caption(), and if that also fails, to a plain caption."""
    if not AI_ENABLED:
        return None
    try:
        with open(thumb_path, "rb") as f:
            img_b64 = base64.b64encode(f.read()).decode()
        resp = await asyncio.wait_for(
            _groq_client.chat.completions.create(
                model=GROQ_VISION_MODEL,
                messages=[{
                    "role": "user",
                    "content": [
                        {"type": "text", "text": (
                            f"This is a frame from part {num} of {total} of a video. "
                            f"Write ONE short, punchy Telegram caption (max 12 words, "
                            f"1 emoji max) based on what's actually shown in this frame. "
                            f"No hashtags, no quotes, just the caption text."
                        )},
                        {"type": "image_url",
                         "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}},
                    ],
                }],
                max_tokens=40,
                temperature=0.8,
            ),
            timeout=AI_VISION_TIMEOUT,
        )
        text = resp.choices[0].message.content.strip().strip('"')
        return text if text else None
    except asyncio.TimeoutError:
        log.warning(f"AI frame-caption timed out (part {num}/{total}).")
        return None
    except Exception as e:
        log.warning(f"AI frame-caption failed (part {num}/{total}): {e} — falling back.")
        return None


# ══════════════════════════════════════════════════════════════
#  INLINE KEYBOARDS — button-based UX (tap instead of type)
# ══════════════════════════════════════════════════════════════
def _quick_split_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✂️ 2 Parts", callback_data="qs:2"),
         InlineKeyboardButton("✂️ 3 Parts", callback_data="qs:3"),
         InlineKeyboardButton("✂️ 5 Parts", callback_data="qs:5")],
        [InlineKeyboardButton("🔥 Best Clips (AI)", callback_data="qhighlights"),
         InlineKeyboardButton("🎬 Scene Split", callback_data="qscene")],
        [InlineKeyboardButton("🤖 Describe", callback_data="qdesc")],
    ])

def _merge_kb(count: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"✅ Done — Merge {count} Videos", callback_data="mdone")],
        [InlineKeyboardButton("❌ Cancel Merge", callback_data="mcancel")],
    ])


# ══════════════════════════════════════════════════════════════
#  SPLIT UI
# ══════════════════════════════════════════════════════════════
async def _split_update(msg, done: int, total: int, start_time: float,
                        label: str, note: str = "") -> None:
    pct = (done * 100 // total) if total > 0 else 0
    note_line = f"\n  ┗ _{note}_" if note else ""
    elapsed = time.time() - start_time
    if done > 0:
        avg = elapsed / done
        time_line = (f"  🚄 **Speed** : `{avg:.1f}s/part`\n"
                    f"  ⏱ **ETA** : `{_eta(avg * (total - done))}`  ·  ⏳ `{_eta(elapsed)}`\n")
    else:
        time_line = f"  ⏳ **Elapsed** : `{_eta(elapsed)}`\n"
    await _safe_edit(msg,
        f"✂️ **{label}**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{_bar(pct, 16)} **{pct}%**\n"
        f"  Part **{done}** / **{total}** done{note_line}\n"
        f"{time_line}"
        f"  ❌ /cancel to stop"
    )


# ══════════════════════════════════════════════════════════════
#  UPLOAD PART
# ══════════════════════════════════════════════════════════════
async def _upload_part(message, path: str, num: int, total: int,
                       uid: int, thumb_time: float,
                       custom_caption: str | None = None) -> bool:
    if _get_cancel(uid).is_set(): return False

    # FIX #21 — reject 0-byte or tiny part files before even trying to upload
    part_size = os.path.getsize(path) if os.path.exists(path) else 0
    if part_size < MIN_PART_BYTES:
        log.error(f"Part {num} is too small ({part_size} bytes) — skipping upload.")
        return False

    try:
        status = await message.reply(
            f"⬆️ **Upload** — part {num}/{total}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"[░░░░░░░░░░░░░░░░░░░░]\n"
            f"  🌀 **0%** ·  starting…"
        )
    except Exception as e:
        log.error(f"Could not send upload status for part {num}: {e}")
        return False

    _reset(uid)
    t0 = time.time()
    thumb_path = f"{THUMB_DIR}/thumb_{uid}_{num}.jpg"
    thumb = await make_thumb(path, thumb_time, thumb_path)
    uploaded = False

    # AI caption — best effort, always falls back gracefully:
    #   1) vision analysis of THIS part's actual thumbnail frame (best — content-aware)
    #   2) filename-based text guess (if vision unavailable/failed)
    #   3) plain "Part N / total" caption (if AI is off entirely)
    ai_text = None
    if thumb and os.path.exists(thumb_path):
        ai_text = await _ai_caption_from_frame(thumb_path, num, total)
    if not ai_text:
        orig_name = os.path.basename(user_files.get(uid, "")) or "video"
        ai_text = await _ai_caption(orig_name, num, total)
    base_caption = custom_caption or f"🎬 **Part {num} / {total}**"
    caption = f"{base_caption}\n_{ai_text}_" if ai_text else base_caption

    # FIX #25 — distinguish "definitely sent" vs "safe to retry" network errors.
    # v3 BUG: ANY non-FloodWait exception (including plain connection drops /
    # timeouts that happen mid-upload — most common on the LAST, often largest
    # part after a long-running task) was treated as fatal and the whole split
    # stopped right there with no retry. That's the "ruk jata hai last mein" bug.
    # FIX: retry a bounded number of times on transient/network-looking errors,
    # only give up for real (no retry) on errors that indicate TG already has it.
    MAX_UPLOAD_ATTEMPTS = 3
    NO_RETRY_MARKERS = ("FILE_PARTS_INVALID", "MEDIA_EMPTY", "FILE_ID_INVALID")

    for attempt in range(MAX_UPLOAD_ATTEMPTS):
        if _get_cancel(uid).is_set():
            await _safe_edit(status, "🚫 Upload cancelled.")
            break
        try:
            await message.reply_video(
                path,
                caption=caption,
                thumb=thumb,
                progress=upload_progress,
                progress_args=(status, t0, uid),
            )
            uploaded = True
            _track_part_created()
            break  # ✅ NEVER retry after success — prevents double upload

        except _UserCancelled:
            await _safe_edit(status, "🚫 Upload cancelled.")
            break

        except FloodWait as e:
            # Safe to retry: FloodWait means Telegram rejected before storing
            wait = e.value + 2
            await _safe_edit(status,
                f"⏳ **Flood wait** — part {num}/{total}\n"
                f"  Resuming in `{wait}s`…"
            )
            await asyncio.sleep(wait)

        except Exception as e:
            err_str = str(e)
            is_final_attempt = attempt == MAX_UPLOAD_ATTEMPTS - 1
            looks_fatal = any(marker in err_str for marker in NO_RETRY_MARKERS)

            if looks_fatal or is_final_attempt:
                log.error(f"Upload part {num} FAILED (attempt {attempt+1}/{MAX_UPLOAD_ATTEMPTS}): {e}")
                await _safe_edit(status, f"❌ Upload failed part {num}: `{e}`")
                break

            backoff = 3 * (attempt + 1)
            log.error(f"Upload part {num} attempt {attempt+1} failed, retrying in {backoff}s: {e}")
            await _safe_edit(status,
                f"⚠️ **Retry** — part {num}/{total} (attempt {attempt+2}/{MAX_UPLOAD_ATTEMPTS})\n"
                f"  Reason: `{err_str[:80]}`\n"
                f"  Resuming in `{backoff}s`…"
            )
            await asyncio.sleep(backoff)

    _reset(uid)
    if thumb and os.path.exists(thumb_path):
        try: os.remove(thumb_path)
        except: pass
    try: await status.delete()
    except: pass
    return uploaded


# ══════════════════════════════════════════════════════════════
#  CORE SPLIT ENGINE
#  FIX #20 — duration re-fetched inside lock to avoid mismatch
#            when user sends a new video during get_duration call
#  FIX #27 — extracted into _run_split_segments() so /splitscene
#            (scene-based cuts) can reuse the exact same tested
#            upload/cancel/error-handling path instead of a copy.
# ══════════════════════════════════════════════════════════════
async def _run_split_segments(message, uid: int, segments: list[tuple[float, float]],
                              label: str, caption_fn=None) -> None:
    """
    segments: list of (start_seconds, duration_seconds) — one per output part.
    caption_fn(part_num, total) -> str | None — custom caption per part, or
                                   None to use the default "Part N / total".
    """
    file = user_files.get(uid)
    if not file or not os.path.exists(file):
        await message.reply("❌ File mil nahi rahi. Dobara video bhejo!")
        return

    parts = len(segments)
    cancel = _get_cancel(uid)
    cancel.clear()
    t0 = time.time()
    msg = await message.reply(
        f"✂️ **{label}**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{_bar(0, 16)} **0%**\n"
        f"  ❌ /cancel to stop"
    )
    try:
        for i, (ss, seg) in enumerate(segments):
            if cancel.is_set():
                await _safe_edit(msg,
                    f"🚫 **Cancelled!**\n  Stopped after **{i}** / **{parts}** parts.")
                _clear_status(uid)
                return
            _set_status(uid, "Splitting", f"part {i+1}/{parts}")
            await _split_update(msg, i, parts, t0, label, f"cutting {i+1}/{parts}…")
            out = f"{DOWNLOAD_DIR}/part_{uid}_{i+1}.mp4"
            ok  = await ffmpeg_cut(file, out, ss, seg)
            if not ok or not os.path.exists(out):
                await _safe_edit(msg, f"❌ ffmpeg failed on part {i+1}. Check logs.")
                _clear_status(uid)
                return
            _set_status(uid, "Uploading", f"part {i+1}/{parts}")
            caption = caption_fn(i + 1, parts) if caption_fn else None
            uploaded = await _upload_part(message, out, i+1, parts, uid, ss + seg/2,
                                          custom_caption=caption)
            try:
                if os.path.exists(out): os.remove(out)
            except: pass
            if not uploaded:
                await _safe_edit(msg,
                    f"🚫 **Stopped!**\n  Stopped after **{i+1}** / **{parts}** parts.")
                _clear_status(uid)
                return

        # All parts done
        try:
            if os.path.exists(file): os.remove(file)
        except: pass
        user_files.pop(uid, None)
        _clear_status(uid)
        total_elapsed = time.time() - t0
        avg_part = total_elapsed / parts if parts else 0
        await _safe_edit(msg,
            f"🏁 **All {parts} parts done!**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{_bar(100, 16)} **100%**\n"
            f"  ⏱ Total: `{_eta(total_elapsed)}`  ·  🚄 avg `{avg_part:.1f}s`/part\n"
            f"  ✅ Complete — send next video!"
        )
        log.info(f"Split done uid={uid} parts={parts}")

    except Exception as e:
        log.error(f"_run_split_segments error uid={uid}: {traceback.format_exc()}")
        _clear_status(uid)
        await _safe_edit(msg, f"❌ Error: `{e}`")


async def _do_split(message, uid: int, parts: int,
                    seg_override: float | None = None,
                    label: str = "") -> None:
    """
    seg_override: if given, use as segment length (splitmin/splitsize).
                  If None, compute as dur/parts (split N).
    """
    # Re-validate + re-fetch duration INSIDE the lock (FIX #20)
    file = user_files.get(uid)
    if not file or not os.path.exists(file):
        await message.reply("❌ File mil nahi rahi. Dobara video bhejo!")
        return

    dur = await get_duration(file)
    if not dur:
        await message.reply("❌ Video duration nahi mila (inside lock).")
        return

    seg = seg_override if seg_override is not None else dur / parts
    segments = [(i * seg, seg) for i in range(parts)]
    _track_video_processed()
    await _run_split_segments(message, uid, segments, label)


# ══════════════════════════════════════════════════════════════
#  COMMAND LIST — defined before receive handler (FIX #3)
# ══════════════════════════════════════════════════════════════
COMMAND_LIST = [
    "start", "help", "split", "splitmin", "splitsize",
    "info",  "status", "cancel", "clear", "aistatus",
    "trim", "extractaudio", "compress", "splitscene", "highlights", "describe",
    "mergestart", "mergedone", "mergecancel",
    "stats", "broadcast",
]


# ══════════════════════════════════════════════════════════════
#  COMMANDS
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.command("start"), group=1)
async def cmd_start(client, message):
    if await _dedup(message): return
    name = getattr(message.from_user, "first_name", "User") or "User"
    await message.reply(
        f"⚡ **ULTRA BOT v6** — ready!\n\n"
        f"👋 Hey **{name}**!\n\n"
        f"📤 Send any **video**, then:\n"
        f"  • `/split 3`       — N equal parts\n"
        f"  • `/splitmin 2`    — chunk every N minutes\n"
        f"  • `/splitsize 500` — chunk every N MB\n"
        f"  • `/splitscene`    — split at every shot/cut (many small clips)\n"
        f"  • `/highlights`    — 🔥 AI picks the 5-10 BEST moments (30-60s each)\n\n"
        f"🎛 **Video tools:**\n"
        f"  • `/trim 1:00 3:30` — extract a clip\n"
        f"  • `/compress medium` — shrink file size\n"
        f"  • `/extractaudio`   — get audio as MP3\n"
        f"  • `/mergestart`     — merge multiple videos\n\n"
        f"🤖 **AI:** `/describe` — what's in this video?\n\n"
        f"🛠 **Utils:** `/info` · `/status` · `/cancel` · `/clear`\n"
        f"  • `/help` — full help\n\n"
        f"✨ Multi-user · Async ffmpeg · Auto thumbnails\n"
        f"🔁 Retry-safe uploads · 🔄 FloodWait safe · No crash"
    )

@app.on_message(filters.command("help"), group=1)
async def cmd_help(client, message):
    if await _dedup(message): return
    admin_line = "\n  `/stats` · `/broadcast msg` → admin only\n" if _is_admin(_uid(message) or -1) else ""
    await message.reply(
        f"📖 **ULTRA BOT v6 — Help**\n\n"
        f"**Step 1:** Koi bhi video send karo\n"
        f"  _(MP4, MKV, AVI, MOV, WEBM, WMV, 3GP)_\n\n"
        f"**Step 2:** Splitting:\n"
        f"  `/split N`       → N equal parts  |  `/split 3`\n"
        f"  `/splitmin N`    → N min chunks   |  `/splitmin 5`\n"
        f"  `/splitsize N`   → N MB chunks    |  `/splitsize 500`\n"
        f"  `/splitscene`    → cuts at EVERY shot change (can be many tiny clips —\n"
        f"                     good for precise editing, not for highlights)\n"
        f"  `/highlights`    → 🔥 AI reads the dialogue and picks the 5-10 BEST,\n"
        f"                     most shareable moments (30-60s each) — this is\n"
        f"                     what you want for viral/highlight clips\n\n"
        f"**Video tools:**\n"
        f"  `/trim start end`   → clip a range  |  `/trim 1:00 3:30`\n"
        f"  `/compress level`   → low · medium · high\n"
        f"  `/extractaudio`     → save audio as MP3\n"
        f"  `/mergestart`       → start collecting videos to merge\n"
        f"  `/mergedone`        → merge the queued videos into one\n"
        f"  `/mergecancel`      → discard the merge queue\n\n"
        f"**AI:**\n"
        f"  `/highlights` → best-moments auto-clipper (see above)\n"
        f"  `/describe`   → AI description of the loaded video\n"
        f"  `/aistatus`   → check if AI features are active\n\n"
        f"**Utils:**\n"
        f"  `/info`   → loaded video ki details\n"
        f"  `/status` → kya chal raha hai\n"
        f"  `/cancel` → rok do\n"
        f"  `/clear`  → stuck state reset karo\n"
        f"{admin_line}"
    )

@app.on_message(filters.command("info"), group=1)
async def cmd_info(client, message):
    if await _dedup(message): return
    uid = _uid(message)
    if uid is None: return
    path = user_files.get(uid)
    if not path or not os.path.exists(path):
        return await message.reply("❌ Koi video load nahi hai.\nPehle video bhejo!")
    d  = await get_duration(path)
    sz = os.path.getsize(path)
    opts = ""
    if d:
        for n in [2, 3, 4, 5]:
            opts += f"  `/split {n}` → {n}×{_eta(d/n)}\n"
    await message.reply(
        f"📋 **File Info**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"  📁 `{os.path.basename(path)}`\n"
        f"  📦 `{_sz(sz)}`\n"
        f"  🎬 `{_eta(d) if d else 'unknown'}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"  💡 Split options:\n{opts}"
        f"  👉 `/split N` · `/splitmin N` · `/splitsize N`"
    )

@app.on_message(filters.command("status"), group=1)
async def cmd_status(client, message):
    if await _dedup(message): return
    uid = _uid(message)
    if uid is None: return
    info = user_status.get(uid)
    if not _get_lock(uid).locked() or not info:
        path = user_files.get(uid)
        if path and os.path.exists(path):
            return await message.reply(
                f"💤 **Idle — Ready**\n"
                f"  📁 `{os.path.basename(path)}`\n"
                f"  📦 `{_sz(os.path.getsize(path))}`\n"
                f"  👉 `/split N` · `/splitmin N`"
            )
        return await message.reply("💤 **Idle** — send a video!")
    elapsed = _eta(time.time() - info["since"])
    await message.reply(
        f"⚙️ **Running…**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"  📌 **Task** : `{info['task']}`\n"
        f"  📝 **Detail** : `{info['detail']}`\n"
        f"  ⏳ **Time** : `{elapsed}`\n\n"
        f"  ❌ /cancel to stop"
    )

@app.on_message(filters.command("aistatus"), group=1)
async def cmd_aistatus(client, message):
    if await _dedup(message): return
    if AI_ENABLED:
        await message.reply(
            f"🤖 **AI Captions: ON**\n"
            f"  Model: `{GROQ_MODEL}`\n"
            f"  Each part's thumbnail is analyzed → real content-aware caption.\n"
            f"  Falls back silently if AI is slow/unavailable."
        )
    else:
        reason = "GROQ_API_KEY not set" if not GROQ_API_KEY else "`groq` package not installed"
        await message.reply(
            f"🤖 **AI Captions: OFF**\n"
            f"  Reason: {reason}\n"
            f"  Set `GROQ_API_KEY` env var (and add `groq` to requirements.txt) to enable."
        )

@app.on_message(filters.command("cancel"), group=1)
async def cmd_cancel(client, message):
    if await _dedup(message): return
    uid = _uid(message)
    if uid is None: return
    if not _get_lock(uid).locked():
        return await message.reply("💤 Koi task nahi chal raha.")
    _get_cancel(uid).set()
    await message.reply("🚫 **Cancel requested!**\nStopping at next checkpoint…")

@app.on_message(filters.command("clear"), group=1)
async def cmd_clear(client, message):
    """Reset stuck user state without bot restart."""
    if await _dedup(message): return
    uid = _uid(message)
    if uid is None: return
    if _get_lock(uid).locked():
        return await message.reply(
            "⚠️ Task abhi chal raha hai.\n"
            "Pehle `/cancel` karo, phir `/clear`."
        )
    path = user_files.pop(uid, None)
    if path:
        try: os.remove(path)
        except: pass
    queue = user_merge_queue.pop(uid, None)
    if queue:
        for p in queue:
            try:
                if os.path.exists(p): os.remove(p)
            except: pass
    _get_cancel(uid).clear()
    _clear_status(uid)
    user_locks.pop(uid, None)
    await message.reply(
        "🗑️ **State cleared!**\n"
        "Sab kuch reset ho gaya.\n"
        "Naya video bhejo 👇"
    )


# ══════════════════════════════════════════════════════════════
#  RECEIVE VIDEO
#  group=-1 ensures this runs BEFORE command handlers (group=1)
#  ~filters.command(COMMAND_LIST) prevents overlap with commands
# ══════════════════════════════════════════════════════════════
@app.on_message(
    filters.incoming & (filters.video | filters.document)
    & ~filters.command(COMMAND_LIST),
    group=-1,
)
async def receive(client, message):
    if await _dedup(message): return
    uid = _uid(message)
    if uid is None: return

    lock = _get_lock(uid)
    if lock.locked():
        return await message.reply("⏳ Task running.\n👉 /status · /cancel")

    media = message.document or message.video
    if not media: return

    mime      = getattr(media, "mime_type", "") or ""
    file_size = getattr(media, "file_size", 0) or 0

    # Filter: only accept video-like MIME types
    if mime and not (mime.startswith("video/") or mime == "application/octet-stream"):
        return

    ext_map = {
        "video/x-matroska": "mkv", "video/mkv": "mkv",
        "video/avi":         "avi", "video/x-msvideo": "avi",
        "video/webm":       "webm", "video/quicktime": "mov",
        "video/x-ms-wmv":   "wmv", "video/3gpp": "3gp",
    }
    ext    = ext_map.get(mime, "mp4")
    sz_str = _sz(file_size) if file_size else "?"

    # ── MERGE-MODE branch: user ran /mergestart — collect instead of replacing ──
    if uid in user_merge_queue:
        if lock.locked():
            return  # race guard, same pattern as the normal flow below
        if len(user_merge_queue[uid]) >= MAX_MERGE_VIDEOS:
            return await message.reply(
                f"⚠️ Merge queue full ({MAX_MERGE_VIDEOS} max).\n👉 `/mergedone` ya `/mergecancel`"
            )
        async with lock:
            idx = len(user_merge_queue[uid]) + 1
            fname_m = f"{DOWNLOAD_DIR}/merge_{uid}_{idx}_{message.id}.{ext}"
            status = await message.reply(f"📥 **Downloading video #{idx} for merge…** {sz_str}")
            _reset(uid)  # fresh progress baseline — don't inherit stale speed/timing from a prior op
            t0 = time.time()
            try:
                path = await _await_with_watchdog(
                    message.download(
                        file_name=fname_m,
                        progress=progress,
                        progress_args=(status, t0, uid, f"📥 Merge #{idx}"),
                    ),
                    uid,
                )
            except _UserCancelled:
                try:
                    if os.path.exists(fname_m): os.remove(fname_m)
                except: pass
                await _safe_edit(status, "🚫 **Download cancelled.**")
                return
            except Exception as e:
                try:
                    if os.path.exists(fname_m): os.remove(fname_m)
                except: pass
                await _safe_edit(status, f"❌ Download failed: `{e}`")
                return
            if not path or not os.path.exists(path):
                await _safe_edit(status, "❌ File not saved — try again.")
                return
            user_merge_queue[uid].append(path)
            _track_video_processed()
            listing = "\n".join(
                f"  ✅ Video {i+1} — {_sz(os.path.getsize(p))}"
                for i, p in enumerate(user_merge_queue[uid]) if os.path.exists(p)
            )
            await _safe_edit(status,
                f"🔗 **Merge Queue ({idx} video{'s' if idx != 1 else ''})**\n"
                f"{listing}\n\n"
                f"👉 Aur bhejo, ya neeche tap karo:",
                reply_markup=_merge_kb(idx),
            )
        return

    fname  = f"{DOWNLOAD_DIR}/video_{uid}_{message.id}.{ext}"

    size_warn = ""
    if file_size and file_size > MAX_FILE_WARN:
        size_warn = f"\n  ⚠️ File `{sz_str}` — near Telegram 2 GB limit!"

    # FIX double message: acquire lock BEFORE sending status reply
    # so only one task ever starts the download for this user
    if lock.locked():
        return  # silently drop — first task already handling it
    async with lock:
        status = await message.reply(
            f"📥 **Downloading…** `{ext.upper()}` · {sz_str}{size_warn}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⏳ Starting…"
        )

        old_path = user_files.pop(uid, None)
        if old_path and os.path.exists(old_path):
            try: os.remove(old_path)
            except: pass

        _get_cancel(uid).clear()
        _reset(uid)
        _set_status(uid, "Downloading", sz_str)
        t0 = time.time()

        try:
            path = await _await_with_watchdog(
                message.download(
                    file_name=fname,
                    progress=progress,
                    progress_args=(status, t0, uid, "📥 Download"),
                ),
                uid,
            )
        except _UserCancelled:
            _clear_status(uid)
            try:
                if os.path.exists(fname): os.remove(fname)
            except: pass
            await _safe_edit(status, "🚫 **Download cancelled.**")
            return
        except Exception as e:
            _clear_status(uid)
            await _safe_edit(status, f"❌ Download failed: `{e}`")
            return

        if not path or not os.path.exists(path):
            _clear_status(uid)
            await _safe_edit(status, "❌ File not saved — try again.")
            return

        _reset(uid)
        _clear_status(uid)
        user_files[uid] = path
        _track_video_processed()
        await _safe_edit(status,
            f"✅ **Download complete!**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"  📁 `{os.path.basename(path)}`\n"
            f"  📦 {_sz(os.path.getsize(path))}  ·  ⏱ {_eta(time.time()-t0)}\n\n"
            f"👉 Neeche se ek tap karo, ya `/trim` · `/compress` · `/mergestart` type karo:",
            reply_markup=_quick_split_kb(),
        )


# ══════════════════════════════════════════════════════════════
#  SPLIT COMMANDS
#  FIX #20 — duration re-fetched inside _do_split (inside lock)
#            so the seg value is always consistent with the file
# ══════════════════════════════════════════════════════════════
def _is_ready(uid: int) -> bool:
    path = user_files.get(uid)
    return bool(path and os.path.exists(path))


# ══════════════════════════════════════════════════════════════
#  QUICK ACTIONS — shared logic for both /commands and button taps
#  FIX #31 — extracted so inline-keyboard buttons (tap "2 Parts", "AI
#  Scene Split", "Describe") can reuse the exact same validated path
#  as the text commands, instead of duplicating the lock/ready checks.
# ══════════════════════════════════════════════════════════════
async def _quick_split(message, uid: int, parts: int) -> None:
    lock = _get_lock(uid)
    if lock.locked():
        await message.reply("⏳ Already processing.\n👉 /status · /cancel")
        return
    if not _is_ready(uid):
        await message.reply("❌ Pehle video bhejo!")
        return
    async with lock:
        if not _is_ready(uid):
            await message.reply("❌ File mil nahi rahi. Dobara bhejo!")
            return
        await _do_split(message, uid, parts,
                        seg_override=None,
                        label=f"Splitting into {parts} equal parts…")


async def _quick_splitscene(message, uid: int) -> None:
    lock = _get_lock(uid)
    if lock.locked():
        await message.reply("⏳ Already processing.\n👉 /status · /cancel")
        return
    if not _is_ready(uid):
        await message.reply("❌ Pehle video bhejo!")
        return
    async with lock:
        if not _is_ready(uid):
            await message.reply("❌ File mil nahi rahi. Dobara bhejo!")
            return
        file = user_files[uid]
        dur = await get_duration(file)
        if not dur:
            await message.reply("❌ Video duration nahi mila.")
            return
        scan_msg = await message.reply("🔍 **Scanning for scene changes…**")
        cuts = await detect_scenes(file, uid, scan_msg, dur)
        if not cuts:
            await _safe_edit(scan_msg,
                "❌ Koi clear scene change nahi mila.\n👉 `/split N` try karo instead.")
            return
        bounds = sorted(set([0.0] + [round(c, 2) for c in cuts if 0 < c < dur] + [dur]))
        segments = [(bounds[i], bounds[i+1] - bounds[i])
                    for i in range(len(bounds) - 1) if bounds[i+1] - bounds[i] > 0.5]
        if len(segments) < 2:
            await _safe_edit(scan_msg,
                "❌ Sirf 1 scene mila — poora video ek jaisa hai.\n👉 `/split N` try karo.")
            return
        if len(segments) > 100:
            segments = segments[:100]
        await _safe_edit(scan_msg, f"🎬 **{len(segments)} scenes mile!** Splitting shuru ho raha hai…")
        _track_video_processed()
        await _run_split_segments(
            message, uid, segments, f"{len(segments)} scenes — AI split…",
            caption_fn=lambda i, t: f"🎬 **Scene {i} / {t}**",
        )


async def _quick_highlights(message, uid: int) -> None:
    """/highlights — content-aware alternative to /splitscene. Transcribes
    the dialogue and asks the LLM to pick 5-10 genuinely shareable moments
    (30-60s each), instead of cutting at every visual shot change."""
    lock = _get_lock(uid)
    if lock.locked():
        await message.reply("⏳ Already processing.\n👉 /status · /cancel")
        return
    if not _is_ready(uid):
        await message.reply("❌ Pehle video bhejo!")
        return
    if not AI_ENABLED:
        await message.reply("🤖 AI abhi off hai.\n👉 `/aistatus` check karo.")
        return

    async with lock:
        if not _is_ready(uid):
            await message.reply("❌ File mil nahi rahi. Dobara bhejo!")
            return
        file = user_files[uid]
        dur = await get_duration(file)
        if not dur:
            await message.reply("❌ Video duration nahi mila.")
            return

        _get_cancel(uid).clear()
        status = await message.reply(
            "🎙 **Audio nikal rahe hain aur samajh rahe hain…**\n"
            "  _(Pura dialogue analyze ho raha hai — thoda time lagega)_"
        )
        segments = await _transcribe_video(file, dur, uid, status)
        if _get_cancel(uid).is_set():
            await _safe_edit(status, "🚫 Cancelled.")
            return
        if not segments:
            await _safe_edit(status,
                "❌ Transcription fail ho gayi — audio na ho ya bahut lambi ho sakti hai.\n"
                "👉 `/splitscene` try karo instead."
            )
            return

        await _safe_edit(status,
            f"🧠 **Best clips choose kar rahe hain…** ({len(segments)} dialogue segments mile)")
        highlights = await _select_highlights(segments, dur)
        if not highlights:
            await _safe_edit(status,
                "❌ Koi strong highlight nahi mila.\n👉 `/splitscene` ya `/split N` try karo.")
            return

        await _safe_edit(status, f"🔥 **{len(highlights)} best clips mile!** Cutting shuru…")
        segs = [(h["start"], h["end"] - h["start"]) for h in highlights]
        reasons = [h["reason"] for h in highlights]
        _track_video_processed()
        await _run_split_segments(
            message, uid, segs, f"{len(highlights)} AI-picked highlights…",
            caption_fn=lambda i, t: f"🔥 **Highlight {i} / {t}**\n_{reasons[i-1]}_",
        )


async def _quick_describe(message, uid: int) -> None:
    lock = _get_lock(uid)
    if lock.locked():
        await message.reply("⏳ Already processing.\n👉 /status · /cancel")
        return
    if not _is_ready(uid):
        await message.reply("❌ Pehle video bhejo!")
        return
    if not AI_ENABLED:
        await message.reply("🤖 AI abhi off hai.\n👉 `/aistatus` check karo.")
        return
    async with lock:
        if not _is_ready(uid):
            await message.reply("❌ File mil nahi rahi. Dobara bhejo!")
            return
        file = user_files[uid]
        status = await message.reply("🔍 **Analyzing video frame…**")
        dur = await get_duration(file) or 0
        mid_point = dur / 2 if dur else 1.0
        thumb_path = f"{THUMB_DIR}/describe_{uid}.jpg"
        thumb = await make_thumb(file, mid_point, thumb_path)
        if not thumb:
            await _safe_edit(status, "❌ Thumbnail nahi ban paya.")
            return
        desc = await _ai_describe(thumb_path)
        try:
            if os.path.exists(thumb_path): os.remove(thumb_path)
        except: pass
        if not desc:
            await _safe_edit(status,
                "❌ AI description generate nahi ho paayi.\n"
                "  Vision model unavailable ho sakta hai — `GROQ_VISION_MODEL` env var check karo."
            )
            return
        await _safe_edit(status, f"🤖 **AI Description:**\n_{desc}_")

@app.on_message(filters.command("split"), group=1)
async def cmd_split(client, message):
    if await _dedup(message): return
    uid = _uid(message)
    if uid is None: return
    if not _is_ready(uid):
        return await message.reply("❌ Pehle video bhejo!")
    try:
        parts = int(message.command[1])
        assert 2 <= parts <= 100
    except:
        return await message.reply("❌ Usage: `/split 3`\n  Min 2, max 100.")
    await _quick_split(message, uid, parts)

@app.on_message(filters.command("splitmin"), group=1)
async def cmd_splitmin(client, message):
    if await _dedup(message): return
    uid = _uid(message)
    if uid is None: return
    lock = _get_lock(uid)
    if lock.locked():
        return await message.reply("⏳ Already processing.\n👉 /status · /cancel")
    if not _is_ready(uid):
        return await message.reply("❌ Pehle video bhejo!")
    try:
        mins = int(message.command[1])
        assert 1 <= mins <= 120
    except:
        return await message.reply("❌ Usage: `/splitmin 2`\n  Minutes 1-120.")

    # Quick pre-check (outside lock) — full check happens inside _do_split
    dur_pre = await get_duration(user_files[uid])
    if not dur_pre:
        return await message.reply("❌ Video duration nahi mila.")
    seg   = mins * 60
    parts = math.ceil(dur_pre / seg)
    if parts > 100:
        return await message.reply(f"❌ Too many parts ({parts}). Bada chunk lo.")
    if parts < 2:
        return await message.reply(f"❌ Video {mins} min se chhota hai!")

    async with lock:
        if not _is_ready(uid):
            return await message.reply("❌ File mil nahi rahi. Dobara bhejo!")
        await _do_split(message, uid, parts,
                        seg_override=seg,
                        label=f"{mins} min chunks → {parts} parts…")

@app.on_message(filters.command("splitsize"), group=1)
async def cmd_splitsize(client, message):
    if await _dedup(message): return
    uid = _uid(message)
    if uid is None: return
    lock = _get_lock(uid)
    if lock.locked():
        return await message.reply("⏳ Already processing.\n👉 /status · /cancel")
    if not _is_ready(uid):
        return await message.reply("❌ Pehle video bhejo!")
    try:
        mb = int(message.command[1])
        assert 10 <= mb <= 2000
    except:
        return await message.reply("❌ Usage: `/splitsize 500`\n  MB 10-2000.")

    file     = user_files[uid]
    total_mb = os.path.getsize(file) / 1048576
    dur_pre  = await get_duration(file)
    if not dur_pre:
        return await message.reply("❌ Video duration nahi mila.")
    parts = math.ceil(total_mb / mb)
    if parts > 100:
        return await message.reply(f"❌ Too many parts ({parts}).")
    if parts < 2:
        return await message.reply(f"❌ File already ≤{mb}MB. Split ki zaroorat nahi.")
    seg = dur_pre / parts

    async with lock:
        if not _is_ready(uid):
            return await message.reply("❌ File mil nahi rahi. Dobara bhejo!")
        await _do_split(message, uid, parts,
                        seg_override=seg,
                        label=f"{mb}MB chunks → {parts} parts…")


# ══════════════════════════════════════════════════════════════
#  AI SMART SPLIT — scene-change detection
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.command("splitscene"), group=1)
async def cmd_splitscene(client, message):
    if await _dedup(message): return
    uid = _uid(message)
    if uid is None: return
    await _quick_splitscene(message, uid)

@app.on_message(filters.command("highlights"), group=1)
async def cmd_highlights(client, message):
    if await _dedup(message): return
    uid = _uid(message)
    if uid is None: return
    await _quick_highlights(message, uid)


# ══════════════════════════════════════════════════════════════
#  VIDEO TOOLS — trim, compress, extract audio
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.command("trim"), group=1)
async def cmd_trim(client, message):
    if await _dedup(message): return
    uid = _uid(message)
    if uid is None: return
    lock = _get_lock(uid)
    if lock.locked():
        return await message.reply("⏳ Already processing.\n👉 /status · /cancel")
    if not _is_ready(uid):
        return await message.reply("❌ Pehle video bhejo!")
    if len(message.command) < 3:
        return await message.reply("❌ Usage: `/trim 1:00 3:30`\n  Ya seconds mein: `/trim 60 210`")

    start_s = _parse_time(message.command[1])
    end_s   = _parse_time(message.command[2])
    if start_s is None or end_s is None or end_s <= start_s:
        return await message.reply("❌ Invalid time range.\n  Format: `HH:MM:SS`, `MM:SS`, ya seconds.")

    async with lock:
        if not _is_ready(uid):
            return await message.reply("❌ File mil nahi rahi. Dobara bhejo!")
        file = user_files[uid]
        dur = await get_duration(file)
        if not dur:
            return await message.reply("❌ Video duration nahi mila.")
        if start_s >= dur:
            return await message.reply(f"❌ Start time video duration (`{_eta(dur)}`) se zyada/barabar hai.")

        end_s = min(end_s, dur)
        seg = end_s - start_s
        _get_cancel(uid).clear()
        _set_status(uid, "Trimming", f"{_eta(start_s)}–{_eta(end_s)}")
        msg = await message.reply(f"✂️ **Trimming** `{_eta(start_s)}` → `{_eta(end_s)}`…")

        out = f"{DOWNLOAD_DIR}/trim_{uid}_{message.id}.mp4"
        ok = await ffmpeg_cut(file, out, start_s, seg)
        if not ok or not os.path.exists(out):
            _clear_status(uid)
            return await _safe_edit(msg, "❌ Trim failed. Check logs.")

        _set_status(uid, "Uploading trim")
        uploaded = await _upload_part(
            message, out, 1, 1, uid, start_s + seg / 2,
            custom_caption="✂️ **Trimmed Clip**",
        )
        try:
            if os.path.exists(out): os.remove(out)
        except: pass
        _clear_status(uid)
        if uploaded:
            _track_video_processed()
            await _safe_edit(msg, "✅ **Trim complete & uploaded!**")
        else:
            await _safe_edit(msg, "❌ Trim upload failed.")


@app.on_message(filters.command("compress"), group=1)
async def cmd_compress(client, message):
    if await _dedup(message): return
    uid = _uid(message)
    if uid is None: return
    lock = _get_lock(uid)
    if lock.locked():
        return await message.reply("⏳ Already processing.\n👉 /status · /cancel")
    if not _is_ready(uid):
        return await message.reply("❌ Pehle video bhejo!")

    level = message.command[1].lower() if len(message.command) > 1 else "medium"
    if level not in COMPRESS_PRESETS:
        return await message.reply("❌ Usage: `/compress low` · `/compress medium` · `/compress high`")
    crf = COMPRESS_PRESETS[level]

    async with lock:
        if not _is_ready(uid):
            return await message.reply("❌ File mil nahi rahi. Dobara bhejo!")
        file = user_files[uid]
        orig_size = os.path.getsize(file)
        _get_cancel(uid).clear()
        _set_status(uid, "Compressing", level)
        msg = await message.reply(f"🗜 **Compressing** (`{level}`)…")

        t0 = time.time()
        out = f"{DOWNLOAD_DIR}/compressed_{uid}_{message.id}.mp4"
        ok = await ffmpeg_compress(file, out, crf, uid, msg)
        elapsed = time.time() - t0

        if _get_cancel(uid).is_set():
            _clear_status(uid)
            try:
                if os.path.exists(out): os.remove(out)
            except: pass
            return await _safe_edit(msg, "🚫 Compression cancelled.")

        if not ok or not os.path.exists(out):
            _clear_status(uid)
            try:
                if os.path.exists(out): os.remove(out)
            except: pass
            return await _safe_edit(msg, "❌ Compression failed. Check logs.")

        new_size = os.path.getsize(out)
        try: os.remove(file)
        except: pass
        user_files[uid] = out
        _clear_status(uid)
        saved_pct = (1 - new_size / orig_size) * 100 if orig_size else 0
        reaction = ("🤯 **Massive shrink!**" if saved_pct >= 60 else
                    "🔥 **Great compression!**" if saved_pct >= 35 else
                    "👍 **Nicely trimmed down!**" if saved_pct >= 10 else
                    "✅ **Done!** _(already fairly optimized)_")
        await _safe_edit(msg,
            f"{reaction}\n"
            f"  📦 `{_sz(orig_size)}` → `{_sz(new_size)}`  ({saved_pct:.0f}% smaller)\n"
            f"  ⏱ Took `{_eta(elapsed)}`\n"
            f"  👉 `/split N` ab is compressed file pe chalega."
        )


@app.on_message(filters.command("extractaudio"), group=1)
async def cmd_extract_audio(client, message):
    if await _dedup(message): return
    uid = _uid(message)
    if uid is None: return
    lock = _get_lock(uid)
    if lock.locked():
        return await message.reply("⏳ Already processing.\n👉 /status · /cancel")
    if not _is_ready(uid):
        return await message.reply("❌ Pehle video bhejo!")

    async with lock:
        if not _is_ready(uid):
            return await message.reply("❌ File mil nahi rahi. Dobara bhejo!")
        file = user_files[uid]
        _set_status(uid, "Extracting audio")
        msg = await message.reply("🎵 **Extracting audio…**")

        out = f"{DOWNLOAD_DIR}/audio_{uid}_{message.id}.mp3"
        ok = await ffmpeg_extract_audio(file, out)
        if not ok or not os.path.exists(out):
            _clear_status(uid)
            return await _safe_edit(msg, "❌ Audio extraction failed. Check logs.")

        try:
            await message.reply_audio(out, caption="🎵 **Extracted Audio**")
        except Exception as e:
            _clear_status(uid)
            try: os.remove(out)
            except: pass
            return await _safe_edit(msg, f"❌ Upload failed: `{e}`")

        try: os.remove(out)
        except: pass
        _clear_status(uid)
        await _safe_edit(msg, "✅ **Audio extracted & sent!**")


# ══════════════════════════════════════════════════════════════
#  MERGE — collect multiple videos, join into one
#  FIX #31 — core logic extracted into _merge_finish/_merge_abort so
#  the inline "✅ Done" / "❌ Cancel" buttons can trigger the exact
#  same path as /mergedone and /mergecancel, not a re-implementation.
# ══════════════════════════════════════════════════════════════
async def _merge_abort(uid: int, reply_target) -> bool:
    """Returns True if a queue was actually found & cancelled."""
    queue = user_merge_queue.pop(uid, None)
    if queue is None:
        await reply_target.reply("💤 Koi merge queue active nahi hai.")
        return False
    for p in queue:
        try:
            if os.path.exists(p): os.remove(p)
        except: pass
    await reply_target.reply(f"🚫 Merge queue cancelled. {len(queue)} video(s) discarded.")
    return True


async def _merge_finish(uid: int, reply_target) -> None:
    lock = _get_lock(uid)
    if lock.locked():
        await reply_target.reply("⏳ Already processing.\n👉 /status · /cancel")
        return
    queue = user_merge_queue.get(uid)
    if not queue:
        await reply_target.reply("❌ Merge queue khaali hai.\n👉 Pehle `/mergestart` karo, phir videos bhejo.")
        return
    if len(queue) < 2:
        await reply_target.reply(f"❌ Kam se kam 2 videos chahiye (abhi {len(queue)}).\n👉 Aur bhejo ya `/mergecancel`.")
        return

    async with lock:
        files = user_merge_queue.pop(uid, [])
        if len(files) < 2:
            for p in files:  # race guard — queue changed before lock acquired
                try:
                    if os.path.exists(p): os.remove(p)
                except: pass
            await reply_target.reply("❌ Merge queue khaali ho gaya. Dobara `/mergestart` karo.")
            return

        _get_cancel(uid).clear()
        _set_status(uid, "Merging", f"{len(files)} videos")
        msg = await reply_target.reply(f"🔗 **Merging {len(files)} videos…**")

        # Combined source duration, just for the celebration message below —
        # ffmpeg_merge now probes durations itself (in parallel) for its OWN
        # live progress bar, so this is a separate, cheap, parallel lookup.
        durations = await asyncio.gather(*(get_duration(p) for p in files))
        combined_dur = sum(durations) if all(durations) else 0.0

        t0 = time.time()
        out = f"{DOWNLOAD_DIR}/merged_{uid}_{int(time.time()*1000)}.mp4"
        ok = await ffmpeg_merge(files, out, uid, msg)
        elapsed = time.time() - t0

        for p in files:
            try:
                if os.path.exists(p): os.remove(p)
            except: pass

        if _get_cancel(uid).is_set():
            _clear_status(uid)
            try:
                if os.path.exists(out): os.remove(out)
            except: pass
            await _safe_edit(msg, "🚫 Merge cancelled.")
            return

        if not ok or not os.path.exists(out):
            _clear_status(uid)
            try:
                if os.path.exists(out): os.remove(out)
            except: pass
            await _safe_edit(msg, "❌ Merge failed. Videos ka codec/format bahut alag ho sakta hai.")
            return

        old = user_files.pop(uid, None)
        if old and os.path.exists(old):
            try: os.remove(old)
            except: pass
        user_files[uid] = out
        _clear_status(uid)
        _track_video_processed()
        await _safe_edit(msg,
            f"🎉 **{len(files)} videos merged into 1!**\n"
            f"  📦 `{_sz(os.path.getsize(out))}`"
            f"{f'  ·  🎞 `{_eta(combined_dur)}` total' if combined_dur else ''}\n"
            f"  ⏱ Took `{_eta(elapsed)}`\n"
            f"  👉 Neeche se quick split karo, ya `/split N` type karo:",
            reply_markup=_quick_split_kb(),
        )


@app.on_message(filters.command("mergestart"), group=1)
async def cmd_merge_start(client, message):
    if await _dedup(message): return
    uid = _uid(message)
    if uid is None: return
    lock = _get_lock(uid)
    if lock.locked():
        return await message.reply("⏳ Already processing.\n👉 /status · /cancel")
    if uid in user_merge_queue:
        return await message.reply(
            f"⚠️ Merge queue already active ({len(user_merge_queue[uid])} video(s)).\n"
            f"👉 `/mergedone` ya `/mergecancel`"
        )
    user_merge_queue[uid] = []
    await message.reply(
        "🔗 **Merge mode ON!**\n"
        f"Ab jitni videos merge karni hain bhejo (max {MAX_MERGE_VIDEOS}, order maintain hoga).\n"
        "Jab bhej chuko, neeche button se ✅ Done tap karo (ya `/mergedone` type karo)."
    )

@app.on_message(filters.command("mergecancel"), group=1)
async def cmd_merge_cancel(client, message):
    if await _dedup(message): return
    uid = _uid(message)
    if uid is None: return
    await _merge_abort(uid, message)

@app.on_message(filters.command("mergedone"), group=1)
async def cmd_merge_done(client, message):
    if await _dedup(message): return
    uid = _uid(message)
    if uid is None: return
    await _merge_finish(uid, message)


# ══════════════════════════════════════════════════════════════
#  AI DESCRIBE
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.command("describe"), group=1)
async def cmd_describe(client, message):
    if await _dedup(message): return
    uid = _uid(message)
    if uid is None: return
    await _quick_describe(message, uid)


# ══════════════════════════════════════════════════════════════
#  ADMIN — stats & broadcast (only for IDs in ADMIN_IDS env var)
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.command("stats"), group=1)
async def cmd_stats(client, message):
    if await _dedup(message): return
    uid = _uid(message)
    if uid is None: return
    if not _is_admin(uid):
        return await message.reply("🚫 Admin-only command.")

    uptime = _eta(time.time() - _stats_start_time)
    active_tasks  = sum(1 for lk in user_locks.values() if lk.locked())
    active_merges = sum(1 for q in user_merge_queue.values() if q)
    await message.reply(
        f"📊 **Bot Stats**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"  👥 Users seen: `{len(_stats_users_seen)}`\n"
        f"  📹 Videos processed: `{_stats_videos_processed}`\n"
        f"  ✂️ Parts created: `{_stats_parts_created}`\n"
        f"  ⚙️ Active tasks: `{active_tasks}`\n"
        f"  🔗 Active merge queues: `{active_merges}`\n"
        f"  🤖 AI: `{'ON — ' + GROQ_MODEL if AI_ENABLED else 'OFF'}`\n"
        f"  ⏳ Uptime: `{uptime}`"
    )

@app.on_message(filters.command("broadcast"), group=1)
async def cmd_broadcast(client, message):
    if await _dedup(message): return
    uid = _uid(message)
    if uid is None: return
    if not _is_admin(uid):
        return await message.reply("🚫 Admin-only command.")
    if len(message.command) < 2:
        return await message.reply("❌ Usage: `/broadcast Your message here`")

    text = message.text.split(None, 1)[1]
    targets = list(_stats_users_seen)
    sent = failed = 0
    status = await message.reply(f"📢 Broadcasting to {len(targets)} user(s)…")

    for target_uid in targets:
        try:
            await client.send_message(target_uid, f"📢 **Announcement**\n\n{text}")
            sent += 1
        except FloodWait as e:
            await asyncio.sleep(e.value + 1)
            try:
                await client.send_message(target_uid, f"📢 **Announcement**\n\n{text}")
                sent += 1
            except Exception:
                failed += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.05)  # gentle throttle

    await _safe_edit(status, f"📢 **Broadcast done!**\n  ✅ Sent: {sent}  ❌ Failed: {failed}")


# ══════════════════════════════════════════════════════════════
#  CALLBACK QUERIES — inline button taps
#  Every handler answers the callback immediately (required by
#  Telegram, dismisses the button's loading spinner) before doing
#  any real work, then routes to the exact same shared logic the
#  text commands use — no separate/duplicated code paths.
# ══════════════════════════════════════════════════════════════
@app.on_callback_query(filters.regex(r"^qs:\d+$"))
async def cb_quick_split(client, callback_query):
    await callback_query.answer()
    uid = callback_query.from_user.id if callback_query.from_user else None
    if uid is None: return
    _stats_users_seen.add(uid)
    parts = int(callback_query.data.split(":")[1])
    await _quick_split(callback_query.message, uid, parts)

@app.on_callback_query(filters.regex(r"^qscene$"))
async def cb_quick_scene(client, callback_query):
    await callback_query.answer()
    uid = callback_query.from_user.id if callback_query.from_user else None
    if uid is None: return
    _stats_users_seen.add(uid)
    await _quick_splitscene(callback_query.message, uid)

@app.on_callback_query(filters.regex(r"^qhighlights$"))
async def cb_quick_highlights(client, callback_query):
    await callback_query.answer()
    uid = callback_query.from_user.id if callback_query.from_user else None
    if uid is None: return
    _stats_users_seen.add(uid)
    await _quick_highlights(callback_query.message, uid)

@app.on_callback_query(filters.regex(r"^qdesc$"))
async def cb_quick_describe(client, callback_query):
    await callback_query.answer()
    uid = callback_query.from_user.id if callback_query.from_user else None
    if uid is None: return
    _stats_users_seen.add(uid)
    await _quick_describe(callback_query.message, uid)

@app.on_callback_query(filters.regex(r"^mdone$"))
async def cb_merge_done(client, callback_query):
    await callback_query.answer()
    uid = callback_query.from_user.id if callback_query.from_user else None
    if uid is None: return
    _stats_users_seen.add(uid)
    await _merge_finish(uid, callback_query.message)

@app.on_callback_query(filters.regex(r"^mcancel$"))
async def cb_merge_cancel(client, callback_query):
    await callback_query.answer()
    uid = callback_query.from_user.id if callback_query.from_user else None
    if uid is None: return
    await _merge_abort(uid, callback_query.message)


# ══════════════════════════════════════════════════════════════
def _start_dummy_webserver():
    """Render free 'Web Service' expects an open port. This bot is a
    background process (Telegram polling via Pyrogram), so it never binds
    a port on its own — Render's port scan times out and kills the deploy.
    This starts a tiny Flask server on $PORT just to satisfy that scan.
    (Not needed if you switch the Render service type to Background Worker.)"""
    from flask import Flask
    import threading

    web = Flask(__name__)

    @web.route("/")
    def home():
        return "Bot is running", 200

    def run():
        port = int(os.environ.get("PORT", 10000))
        web.run(host="0.0.0.0", port=port)

    threading.Thread(target=run, daemon=True).start()


if __name__ == "__main__":
    log.info("🚀 Ultra Bot v6 starting…")
    _start_dummy_webserver()
    app.run()
