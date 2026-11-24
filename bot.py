# Ultra Bot v10.0 — scene/highlight engine upgrade + FIX #32/#33: Live %/speed/ETA progress engine extended to
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
import shutil
import random
import sqlite3
import uuid
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
#  V10 — PERSISTENT JOB RECOVERY
#  Uses Postgres/Neon when DATABASE_URL is configured. Falls back to
#  SQLite for local development. Render Free needs DATABASE_URL/Neon
#  for restart-safe recovery because its local filesystem is ephemeral.
# ══════════════════════════════════════════════════════════════
RECOVERY_DB_URL = (os.getenv("DATABASE_URL", "") or os.getenv("NEON_DATABASE_URL", "")).strip()
RECOVERY_DB_PATH = os.getenv("RECOVERY_DB_PATH", "recovery_jobs.sqlite3")

try:
    import psycopg2
    _HAS_PG = True
except ImportError:
    psycopg2 = None
    _HAS_PG = False

_DB_LOCK = asyncio.Lock()

def _db_conn():
    if RECOVERY_DB_URL and _HAS_PG:
        return psycopg2.connect(RECOVERY_DB_URL, connect_timeout=10)
    conn = sqlite3.connect(RECOVERY_DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn

def _db_init_sync():
    conn = _db_conn()
    try:
        cur = conn.cursor()
        if RECOVERY_DB_URL and _HAS_PG:
            cur.execute("""CREATE TABLE IF NOT EXISTS video_jobs (
                job_id TEXT PRIMARY KEY, uid BIGINT NOT NULL, chat_id BIGINT NOT NULL,
                source_message_id BIGINT NOT NULL, kind TEXT NOT NULL, state TEXT NOT NULL,
                payload TEXT NOT NULL, source_path TEXT, attempts INTEGER NOT NULL DEFAULT 0,
                created_at DOUBLE PRECISION NOT NULL, updated_at DOUBLE PRECISION NOT NULL
            )""")
        else:
            cur.execute("""CREATE TABLE IF NOT EXISTS video_jobs (
                job_id TEXT PRIMARY KEY, uid INTEGER NOT NULL, chat_id INTEGER NOT NULL,
                source_message_id INTEGER NOT NULL, kind TEXT NOT NULL, state TEXT NOT NULL,
                payload TEXT NOT NULL, source_path TEXT, attempts INTEGER NOT NULL DEFAULT 0,
                created_at REAL NOT NULL, updated_at REAL NOT NULL
            )""")
        conn.commit()
    finally:
        conn.close()

def _db_exec_sync(sql, params=(), fetch=False, many=False):
    conn=_db_conn()
    try:
        if not (RECOVERY_DB_URL and _HAS_PG):
            sql=sql.replace("%s", "?")
        cur=conn.cursor()
        if many: cur.executemany(sql, params)
        else: cur.execute(sql, params)
        rows=cur.fetchall() if fetch else None
        conn.commit()
        return rows
    finally: conn.close()

def _db_exec_sqlite_safe(sql, params=(), fetch=False):
    # Convert PostgreSQL placeholders to SQLite placeholders; schema syntax used above is compatible.
    sql2=sql.replace("%s", "?")
    return _db_exec_sync(sql2, params, fetch=fetch)

async def _db_init():
    await asyncio.to_thread(_db_init_sync)

async def _job_create(uid, chat_id, source_message_id, kind, payload, source_path=None, job_id=None):
    job_id = job_id or uuid.uuid4().hex[:16]
    now=time.time()
    await asyncio.to_thread(_db_exec_sync,
        "INSERT INTO video_jobs(job_id,uid,chat_id,source_message_id,kind,state,payload,source_path,attempts,created_at,updated_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (job_id,uid,chat_id,source_message_id,kind,"active",json.dumps(payload),source_path,0,now,now))
    return job_id

async def _job_update(job_id, state=None, payload=None, source_path=None, attempts_inc=False):
    fields=[]; vals=[]
    if state is not None: fields += ["state=%s"]; vals += [state]
    if payload is not None: fields += ["payload=%s"]; vals += [json.dumps(payload)]
    if source_path is not None: fields += ["source_path=%s"]; vals += [source_path]
    if attempts_inc: fields += ["attempts=attempts+1"]
    fields += ["updated_at=%s"]; vals += [time.time(), job_id]
    await asyncio.to_thread(_db_exec_sync, "UPDATE video_jobs SET "+", ".join(fields)+" WHERE job_id=%s", tuple(vals))

async def _job_finish(job_id, state="done"):
    await _job_update(job_id, state=state)

async def _job_get_active():
    rows=await asyncio.to_thread(_db_exec_sync,
        "SELECT job_id,uid,chat_id,source_message_id,kind,state,payload,source_path,attempts FROM video_jobs WHERE state IN ('active','recovering','retry') ORDER BY created_at", (), True)
    out=[]
    for r in rows:
        out.append({"job_id":r[0],"uid":int(r[1]),"chat_id":int(r[2]),"source_message_id":int(r[3]),"kind":r[4],"state":r[5],"payload":json.loads(r[6] or "{}"),"source_path":r[7],"attempts":int(r[8] or 0)})
    return out

async def _job_delete(job_id):
    await asyncio.to_thread(_db_exec_sync, "DELETE FROM video_jobs WHERE job_id=%s", (job_id,))

async def _source_for(uid):
    rows=await asyncio.to_thread(_db_exec_sync,
        "SELECT job_id,chat_id,source_message_id,source_path FROM video_jobs WHERE uid=%s AND kind='source' AND state='ready' ORDER BY updated_at DESC LIMIT 1", (uid,), True)
    if not rows: return None
    r=rows[0]
    return {"job_id":r[0],"chat_id":int(r[1]),"source_message_id":int(r[2]),"source_path":r[3]}

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
SCENE_DETECT_TIMEOUT    = 600          # hard upper bound; scene scan is downscaled/low-FPS
SCENE_MIN_GAP_SEC       = float(os.getenv("SCENE_MIN_GAP_SEC", "4.5"))
SCENE_MAX_COUNT         = int(os.getenv("SCENE_MAX_COUNT", "40"))
SCENE_SCAN_FPS          = float(os.getenv("SCENE_SCAN_FPS", "5"))
SCENE_SCAN_WIDTH        = int(os.getenv("SCENE_SCAN_WIDTH", "480"))
MAX_MERGE_VIDEOS        = 20           # cap merge queue size
COMPRESS_PRESETS        = {"low": 28, "medium": 23, "high": 18}  # CRF values (lower = better quality)
WHISPER_MAX_BYTES       = 24 * 1024 * 1024   # stay under Groq's 25MB free-tier cap, with margin
WHISPER_CHUNK_SECONDS   = 1200                # 20 min per transcription chunk if audio is too big
TRANSCRIBE_TIMEOUT      = 120                 # per-chunk Whisper API timeout
HIGHLIGHT_MIN_SEC       = 20                  # floor — LLM picks are clamped/filtered around this
HIGHLIGHT_MAX_SEC       = 70                  # ceiling
HIGHLIGHT_MIN_COUNT     = 5
HIGHLIGHT_MAX_COUNT     = 10
HIGHLIGHT_CANDIDATE_MAX  = int(os.getenv("HIGHLIGHT_CANDIDATE_MAX", "24"))
HIGHLIGHT_VISION_CHECKS  = int(os.getenv("HIGHLIGHT_VISION_CHECKS", "6"))
HIGHLIGHT_USE_VISION     = os.getenv("HIGHLIGHT_USE_VISION", "1").lower() not in {"0", "false", "no"}

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

# V10: stale cleanup intentionally avoids protected job files; recovery decides what is safe.
_cleanup_stale_files()


# ══════════════════════════════════════════════════════════════
#  CLIENT
# ══════════════════════════════════════════════════════════════
app = Client(
    "ultra-bot",
    api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN,
    in_memory=True,
    sleep_threshold=60,
    ipv6=False,
    max_concurrent_transmissions=max(1, min(2, int(os.getenv("MAX_CONCURRENT_TRANSMISSIONS", "1")))),
)


# ══════════════════════════════════════════════════════════════
#  PER-USER STATE
# ══════════════════════════════════════════════════════════════
user_files:  dict[int, str]           = {}
user_sources: dict[int, dict]         = {}
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

THROTTLE  = 1.2
EMA_ALPHA = 0.35

FAST_UPLOAD_MODE = os.getenv("FAST_UPLOAD_MODE", "1").strip().lower() not in {"0", "false", "no"}
ENABLE_UPLOAD_THUMB = os.getenv("ENABLE_UPLOAD_THUMB", "0").strip().lower() in {"1", "true", "yes"}
MAX_INPUT_BYTES = int(os.getenv("MAX_INPUT_MB", "2000")) * 1024 * 1024
DISK_SAFETY_BYTES = 256 * 1024 * 1024
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
    """Keep the transmission callback lightweight; UI updates are throttled."""
    if _get_cancel(uid).is_set():
        try:
            app.stop_transmission()
        except Exception:
            pass
        raise _UserCancelled()

    if not isinstance(total, (int, float)) or total <= 0:
        return

    now = time.time()
    elapsed = max(now - start, 0.001)
    prev_edit = _last_edit.get(uid, 0.0)
    _last_edit[uid] = now  # liveness signal on every transferred chunk
    if now - prev_edit < THROTTLE and prev_edit != 0.0:
        return

    raw = current / elapsed
    ema = EMA_ALPHA * raw + (1 - EMA_ALPHA) * _ema_speed.get(uid, raw)
    _ema_speed[uid] = ema
    eta_s = (total - current) / ema if ema > 0 else 0
    pct = max(0.0, min(100.0, current * 100 / total))
    kb = ema / 1024
    tier = "🟢 Fast" if kb >= 1024 else ("🟡 Good" if kb >= 256 else "🔴 Slow")
    spin = SPINNER[_spin_idx.get(uid, 0) % len(SPINNER)]
    _spin_idx[uid] = _spin_idx.get(uid, 0) + 1

    await _safe_edit(msg,
        f"{spin} **{mode}**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{_bar(pct)}\n"
        f"  {_badge(pct)} **{pct:.1f}%** · {_sz(current)} / {_sz(total)}\n"
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
                        threshold: float = 8.0, max_cuts: int | None = None) -> list[float]:
    """Fast, conservative scene detector.

    The old implementation decoded every source frame and then accepted *any*
    threshold hit. On fast-cut TV/video that turns tiny score spikes into
    dozens of 0.5-3 second clips (e.g. 85 scenes from a 3-minute video).

    This version:
      1. scans a downscaled 480px stream at 5fps (much lighter on Render Free),
      2. collects scdet scores without retaining a giant debug log,
      3. uses an adaptive threshold with a hard floor,
      4. performs non-maximum suppression so nearby spikes become one cut,
      5. enforces a minimum scene length, and
      6. caps the total number of scenes according to video duration.

    Returned values are GLOBAL timestamps of accepted cuts.
    """
    if not total_duration or total_duration <= 1:
        return []

    min_gap = max(2.5, SCENE_MIN_GAP_SEC)
    duration_cap = max(2, int(math.ceil(total_duration / min_gap)))
    if max_cuts is None:
        max_cuts = max(1, min(SCENE_MAX_COUNT - 1, duration_cap - 1))
    max_cuts = max(1, min(int(max_cuts), SCENE_MAX_COUNT - 1))

    fps = max(2.0, min(8.0, SCENE_SCAN_FPS))
    width = max(240, min(720, SCENE_SCAN_WIDTH))

    try:
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-hide_banner", "-loglevel", "info", "-i", file,
            "-vf", f"fps={fps:g},scale={width}:-2:flags=fast_bilinear,scdet=threshold=0",
            "-an", "-f", "null", "-", "-progress", "pipe:1", "-nostats",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except Exception as e:
        log.error(f"detect_scenes failed to start: {e}")
        return []

    # Keep only the tiny amount of stderr needed for score parsing.  The old
    # code stored up to 24MB of debug output, which is unnecessary on a free
    # 512MB service and could itself become a memory pressure point.
    scores: list[tuple[float, float]] = []
    stderr_buf = b""
    progress_buf = b""
    last_ui = 0.0
    t0 = time.time()

    async def drain_stderr():
        nonlocal stderr_buf
        try:
            while True:
                chunk = await proc.stderr.read(65536)
                if not chunk:
                    break
                stderr_buf += chunk
                # Parse complete lines and immediately discard them.
                if len(stderr_buf) > 512 * 1024:
                    stderr_buf = stderr_buf[-256 * 1024:]
                lines = stderr_buf.split(b"\n")
                stderr_buf = lines.pop() if lines else b""
                for raw in lines:
                    line = raw.decode(errors="ignore")
                    m = re.search(r"lavfi\.scd\.score:\s*([0-9.]+),\s*lavfi\.scd\.time:\s*([0-9.]+)", line)
                    if m:
                        try:
                            scores.append((float(m.group(1)), float(m.group(2))))
                        except ValueError:
                            pass
        except asyncio.CancelledError:
            raise
        except Exception:
            pass

    async def drain_progress():
        nonlocal progress_buf, last_ui
        try:
            while True:
                chunk = await proc.stdout.read(4096)
                if not chunk:
                    break
                progress_buf += chunk
                while b"\n" in progress_buf:
                    line, progress_buf = progress_buf.split(b"\n", 1)
                    m = re.search(rb"out_time_us=(\d+)", line)
                    if not m:
                        continue
                    now = time.time()
                    if now - last_ui < 1.2:
                        continue
                    last_ui = now
                    done_sec = min(total_duration, int(m.group(1)) / 1_000_000)
                    pct = int(done_sec * 100 / total_duration)
                    await _safe_edit(status_msg,
                        f"🔍 **Scene scan**\n{_bar(pct, 16)} **{pct}%**\n"
                        f"  Scanning at `{fps:g}fps / {width}px`\n"
                        f"  ❌ /cancel to stop")
        except asyncio.CancelledError:
            raise
        except Exception:
            pass

    drain_task = asyncio.create_task(drain_stderr())
    progress_task = asyncio.create_task(drain_progress())
    try:
        while True:
            if _get_cancel(uid).is_set():
                await _kill_proc(proc)
                return []
            if time.time() - t0 > SCENE_DETECT_TIMEOUT:
                await _kill_proc(proc)
                log.error("detect_scenes timed out")
                return []
            try:
                await asyncio.wait_for(proc.wait(), timeout=1)
                break
            except asyncio.TimeoutError:
                continue
    finally:
        try:
            await asyncio.wait_for(drain_task, timeout=2)
        except Exception:
            drain_task.cancel()
        progress_task.cancel()
        try:
            await progress_task
        except BaseException:
            pass

    if proc.returncode != 0:
        log.error(f"detect_scenes ffmpeg rc={proc.returncode}")
        return []

    # Flush any final complete stderr line.
    tail = stderr_buf.decode(errors="ignore")
    for m in re.finditer(r"lavfi\.scd\.score:\s*([0-9.]+),\s*lavfi\.scd\.time:\s*([0-9.]+)", tail):
        try:
            scores.append((float(m.group(1)), float(m.group(2))))
        except ValueError:
            pass

    if not scores:
        log.warning("detect_scenes: no scdet scores found")
        return []

    # Ignore the initial frame and extremely late/end timestamps.
    scores = [(s, t) for s, t in scores if 0.15 < t < total_duration - 0.15 and math.isfinite(s)]
    if not scores:
        return []

    values = sorted(s for s, _ in scores)
    p90 = values[int(0.90 * (len(values) - 1))]
    adaptive = max(float(threshold), min(14.0, p90 * 1.5))
    # Hard cuts in scdet are commonly much larger than ordinary motion. Keep
    # a floor so a low-contrast source cannot explode into hundreds of cuts.
    adaptive = max(6.0, adaptive)

    raw = [(s, t) for s, t in scores if s >= adaptive]
    if not raw:
        raw = [(s, t) for s, t in scores if s >= max(4.0, adaptive * 0.65)]
    if not raw:
        return []

    # Non-maximum suppression: for each minimum-gap bucket keep the strongest
    # actual cut instead of accepting every nearby spike.
    raw.sort(key=lambda x: x[1])
    candidates: list[tuple[float, float]] = []
    cluster: list[tuple[float, float]] = []
    for item in raw:
        if not cluster or item[1] - cluster[-1][1] <= min_gap:
            cluster.append(item)
        else:
            candidates.append(max(cluster, key=lambda x: x[0]))
            cluster = [item]
    if cluster:
        candidates.append(max(cluster, key=lambda x: x[0]))

    # Enforce a true minimum distance between accepted boundaries.
    accepted: list[tuple[float, float]] = []
    for score, ts in sorted(candidates, key=lambda x: x[1]):
        if not accepted or ts - accepted[-1][1] >= min_gap:
            accepted.append((score, ts))
        elif score > accepted[-1][0]:
            accepted[-1] = (score, ts)

    # If still too many, retain the strongest cuts while preserving timeline order.
    if len(accepted) > max_cuts:
        strongest = sorted(accepted, key=lambda x: x[0], reverse=True)[:max_cuts]
        accepted = sorted(strongest, key=lambda x: x[1])

    cuts = [round(ts, 2) for _, ts in accepted]
    log.info("Scene detector: %d scores -> %d cuts (threshold=%.2f, min_gap=%.2fs)",
             len(scores), len(cuts), adaptive, min_gap)
    return cuts


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
    """Multi-stage text-AI highlight selector.

    Stage 1: Whisper supplies timestamped speech.
    Stage 2: the LLM ranks self-contained moments instead of blindly asking
             for arbitrary timestamps.
    Stage 3 (in _vision_rank_highlights): a vision model checks the selected
             frames and re-ranks them before the final cuts are made.
    """
    if not AI_ENABLED or not segments:
        return []

    # Compact transcript keeps API payload reasonable on long videos while
    # retaining timestamps and enough surrounding dialogue for setup/punchline.
    lines = []
    for s in segments:
        text = re.sub(r"\s+", " ", str(s.get("text", "")).strip())
        if not text:
            continue
        m, sec = divmod(int(max(0, s["start"])), 60)
        lines.append(f"[{m:02d}:{sec:02d}] {text[:500]}")
    transcript = "\n".join(lines)
    if len(transcript) > 50000:
        transcript = transcript[:50000] + "\n...[transcript truncated]"

    candidate_target = min(HIGHLIGHT_CANDIDATE_MAX, max(HIGHLIGHT_MAX_COUNT * 2, 12))
    prompt = (
        f"You are the first AI judge for a {duration/60:.1f}-minute video.\n"
        f"Find up to {candidate_target} candidate highlight moments from this timestamped transcript.\n\n"
        f"Rules:\n"
        f"- A moment should be self-contained and understandable.\n"
        f"- Prefer comedy, emotion, surprise, conflict, useful information, a punchline, reveal, or strong reaction.\n"
        f"- Include enough setup before the payoff.\n"
        f"- Target {HIGHLIGHT_MIN_SEC}-{HIGHLIGHT_MAX_SEC}s per final clip.\n"
        f"- Never invent timestamps outside the transcript/video.\n"
        f"- Candidates may overlap at this stage, but final clips must not overlap.\n"
        f"- score is 0-100 for highlight quality, not video quality.\n\n"
        f"TRANSCRIPT:\n{transcript}\n\n"
        f'Respond ONLY JSON: {{"highlights":[{{"start":123.4,"end":165.0,"score":92,"reason":"short reason"}}]}}'
    )
    try:
        resp = await asyncio.wait_for(
            _groq_client.chat.completions.create(
                model=GROQ_MODEL,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=1800,
                temperature=0.2,
                response_format={"type": "json_object"},
            ),
            timeout=90,
        )
        data = json.loads(resp.choices[0].message.content)
        items = data.get("highlights", []) if isinstance(data, dict) else []
    except asyncio.TimeoutError:
        log.warning("_select_highlights timed out")
        return []
    except Exception as e:
        log.warning(f"_select_highlights failed: {e}")
        return []

    picked: list[dict] = []
    for it in items:
        try:
            start = max(0.0, float(it["start"]))
            end = min(duration, float(it["end"]))
            score = max(0.0, min(100.0, float(it.get("score", 50))))
            reason = re.sub(r"\s+", " ", str(it.get("reason", "Highlight moment"))).strip()[:90]
        except (KeyError, TypeError, ValueError):
            continue

        # Expand very short AI picks to preserve setup + payoff, then clamp.
        if end - start < HIGHLIGHT_MIN_SEC:
            center = (start + end) / 2
            half = HIGHLIGHT_MIN_SEC / 2
            start = max(0.0, center - half)
            end = min(duration, start + HIGHLIGHT_MIN_SEC)
            if end - start < HIGHLIGHT_MIN_SEC:
                start = max(0.0, end - HIGHLIGHT_MIN_SEC)
        if end - start > HIGHLIGHT_MAX_SEC:
            end = start + HIGHLIGHT_MAX_SEC
        if end <= start or end - start < HIGHLIGHT_MIN_SEC * 0.75:
            continue
        picked.append({"start": start, "end": min(end, duration),
                       "score": score, "reason": reason or "Highlight moment"})

    # Deduplicate heavily overlapping model picks, retaining the stronger one.
    picked.sort(key=lambda x: x["score"], reverse=True)
    selected: list[dict] = []
    for item in picked:
        overlap = False
        for kept in selected:
            inter = max(0.0, min(item["end"], kept["end"]) - max(item["start"], kept["start"]))
            shorter = min(item["end"] - item["start"], kept["end"] - kept["start"])
            if shorter > 0 and inter / shorter >= 0.35:
                overlap = True
                break
        if not overlap:
            selected.append(item)
        if len(selected) >= HIGHLIGHT_MAX_COUNT:
            break

    return sorted(selected, key=lambda x: x["start"])


async def _vision_rank_highlights(video: str, uid: int, highlights: list[dict]) -> list[dict]:
    """Second AI judge: inspect representative frames and re-rank picks.

    Vision is intentionally limited to a handful of candidates so Render Free
    is not buried under dozens of FFmpeg/API calls. If vision fails, the text-AI
    ranking is retained unchanged.
    """
    if not AI_ENABLED or not HIGHLIGHT_USE_VISION or not highlights:
        return highlights

    ranked = list(highlights)
    checks = min(HIGHLIGHT_VISION_CHECKS, len(ranked))
    # Check strongest candidates first; the rest keep their text-AI score.
    ranked.sort(key=lambda h: h.get("score", 0), reverse=True)

    async def score_one(index: int, item: dict):
        thumb_path = f"{THUMB_DIR}/hv_{uid}_{index}.jpg"
        try:
            mid = (item["start"] + item["end"]) / 2
            thumb = await make_thumb(video, mid, thumb_path)
            if not thumb:
                return
            with open(thumb, "rb") as f:
                img_b64 = base64.b64encode(f.read()).decode()
            prompt = (
                "You are the second AI judge for a short-video highlight. "
                "Judge ONLY the visible frame: does it look like a meaningful, "
                "engaging moment rather than a blank/transition/boring frame? "
                "Return JSON only: {\"score\":0-100,\"reason\":\"max 8 words\"}."
            )
            resp = await asyncio.wait_for(
                _groq_client.chat.completions.create(
                    model=GROQ_VISION_MODEL,
                    messages=[{"role": "user", "content": [
                        {"type": "text", "text": prompt},
                        {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}},
                    ]}],
                    max_tokens=80,
                    temperature=0.1,
                    response_format={"type": "json_object"},
                ),
                timeout=AI_VISION_TIMEOUT,
            )
            data = json.loads(resp.choices[0].message.content)
            vscore = max(0.0, min(100.0, float(data.get("score", 50))))
            # Text AI remains the primary judge; vision breaks ties / removes
            # visually weak frames instead of overpowering transcript quality.
            item["score"] = item.get("score", 50) * 0.70 + vscore * 0.30
            vr = str(data.get("reason", "")).strip()
            if vr:
                item["vision_reason"] = vr[:60]
        except Exception as e:
            log.warning(f"highlight vision check failed: {e}")
        finally:
            try:
                if os.path.exists(thumb_path):
                    os.remove(thumb_path)
            except Exception:
                pass

    await asyncio.gather(*(score_one(i, ranked[i]) for i in range(checks)), return_exceptions=True)

    # Re-rank and remove overlaps again after the second AI score.
    ranked.sort(key=lambda h: h.get("score", 0), reverse=True)
    final: list[dict] = []
    for item in ranked:
        if any(
            max(0.0, min(item["end"], x["end"]) - max(item["start"], x["start"]))
            / max(0.1, min(item["end"] - item["start"], x["end"] - x["start"])) >= 0.35
            for x in final
        ):
            continue
        final.append(item)
        if len(final) >= HIGHLIGHT_MAX_COUNT:
            break
    return sorted(final, key=lambda h: h["start"])


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
def _main_menu_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✂️ Split & Scene", callback_data="menu:split"),
         InlineKeyboardButton("🤖 AI Highlights", callback_data="menu:ai")],
        [InlineKeyboardButton("🎬 Video Tools", callback_data="menu:tools"),
         InlineKeyboardButton("🔗 Merge", callback_data="menu:merge")],
        [InlineKeyboardButton("📊 Status / Info", callback_data="menu:status"),
         InlineKeyboardButton("📖 All Commands", callback_data="menu:help")],
        [InlineKeyboardButton("⚙️ How it works", callback_data="menu:how")],
    ])


def _quick_split_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✂️ 2 Parts", callback_data="qs:2"),
         InlineKeyboardButton("✂️ 3 Parts", callback_data="qs:3"),
         InlineKeyboardButton("✂️ 5 Parts", callback_data="qs:5")],
        [InlineKeyboardButton("🎬 Smart Scene", callback_data="qscene"),
         InlineKeyboardButton("🔥 AI Highlights", callback_data="qhighlights")],
        [InlineKeyboardButton("🛠 Tools", callback_data="menu:tools"),
         InlineKeyboardButton("🤖 Describe", callback_data="qdesc")],
        [InlineKeyboardButton("🔗 Merge", callback_data="menu:merge"),
         InlineKeyboardButton("📊 Status", callback_data="menu:status")],
        [InlineKeyboardButton("📖 Help", callback_data="menu:help")],
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
    thumb = None

    # Fast mode keeps the upload path free from FFmpeg thumbnail/AI work.
    # Enable those extras explicitly when desired.
    if ENABLE_UPLOAD_THUMB and not FAST_UPLOAD_MODE:
        thumb = await make_thumb(path, thumb_time, thumb_path)

    uploaded = False
    ai_text = None
    if not FAST_UPLOAD_MODE and thumb and os.path.exists(thumb_path):
        ai_text = await _ai_caption_from_frame(thumb_path, num, total)
    if not FAST_UPLOAD_MODE and not ai_text:
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
    MAX_UPLOAD_ATTEMPTS = 4
    NO_RETRY_MARKERS = ("FILE_PARTS_INVALID", "MEDIA_EMPTY", "FILE_ID_INVALID")

    for attempt in range(MAX_UPLOAD_ATTEMPTS):
        if _get_cancel(uid).is_set():
            await _safe_edit(status, "🚫 Upload cancelled.")
            break
        try:
            sent = await message.reply_video(
                path,
                caption=caption,
                thumb=thumb,
                supports_streaming=True,
                progress=upload_progress,
                progress_args=(status, t0, uid),
            )
            if sent is None:
                if _get_cancel(uid).is_set():
                    await _safe_edit(status, "🚫 Upload cancelled.")
                    break
                raise RuntimeError("Telegram transmission returned no message")
            uploaded = True
            _track_part_created()
            break

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

            backoff = min(20, (2 ** attempt) + random.uniform(0.25, 1.25))
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
                              label: str, caption_fn=None, job_id: str | None = None, start_index: int = 0) -> None:
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
    start_index = max(0, min(start_index, parts))
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
            if i < start_index:
                continue
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
            if uploaded and job_id:
                await _job_update(job_id, payload={"stage":"split","segments":segments,"next_index":i+1,"label":label})
            try:
                if os.path.exists(out): os.remove(out)
            except: pass
            if not uploaded:
                if job_id:
                    await _job_update(job_id, state="retry", payload={"stage":"split","segments":segments,"next_index":i,"label":label})
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
        if job_id:
            await _job_finish(job_id, "done")
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
        if job_id:
            await _job_update(job_id, state="retry", payload={"stage":"split","segments":segments,"next_index":start_index,"label":label})
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
    src = user_sources.get(uid) or await _source_for(uid)
    job_id = None
    if src:
        job_id = await _job_create(uid, message.chat.id, src["source_message_id"], "split", {"stage":"split","segments":segments,"next_index":0,"label":label}, file)
    await _run_split_segments(message, uid, segments, label, job_id=job_id)


# ══════════════════════════════════════════════════════════════
#  COMMAND LIST — defined before receive handler (FIX #3)
# ══════════════════════════════════════════════════════════════
COMMAND_LIST = [
    "start", "help", "split", "splitmin", "splitsize",
    "info",  "status", "cancel", "clear", "aistatus",
    "trim", "extractaudio", "compress", "splitscene", "highlights", "describe",
    "mergestart", "mergedone", "mergecancel",
    "stats", "broadcast", "retry", "resume",
]


# ══════════════════════════════════════════════════════════════
#  V10 RECOVERY COMMANDS
# ══════════════════════════════════════════════════════════════
async def _resume_job_record(job, message=None):
    uid=job["uid"]
    lock=_get_lock(uid)
    if lock.locked(): return False, "Already processing"
    async with lock:
        _get_cancel(uid).clear()
        src_msg=await app.get_messages(job["chat_id"], job["source_message_id"])
        if not src_msg or not (src_msg.video or src_msg.document):
            await _job_update(job["job_id"], state="retry")
            return False, "Original Telegram message is unavailable"
        path=job.get("source_path")
        if not path or not os.path.exists(path):
            ext="mp4"
            media=src_msg.video or src_msg.document
            mime=getattr(media,"mime_type","") or ""
            ext_map={"video/x-matroska":"mkv","video/mkv":"mkv","video/avi":"avi","video/x-msvideo":"avi","video/webm":"webm","video/quicktime":"mov"}
            ext=ext_map.get(mime,"mp4")
            path=f"{DOWNLOAD_DIR}/recovery_{uid}_{job['job_id']}.{ext}"
            status=message or src_msg
            st=await status.reply("♻️ **Recovering original video from Telegram…**")
            try:
                path=await _await_with_watchdog(src_msg.download(file_name=path, progress=progress, progress_args=(st,time.time(),uid,"♻️ Recover")),uid)
                await _safe_edit(st,"✅ Original recovered. Resuming job…")
            except Exception as e:
                await _job_update(job["job_id"], state="retry", attempts_inc=True)
                await _safe_edit(st,f"⚠️ Recovery retry needed: `{str(e)[:120]}`")
                return False, str(e)
            await _job_update(job["job_id"], source_path=path)
        user_files[uid]=path
        user_sources[uid]={"chat_id":job["chat_id"],"source_message_id":job["source_message_id"],"source_path":path}
        payload=job["payload"]
        kind=job["kind"]
        if kind in {"split","scene","highlights"}:
            segs=[tuple(x) for x in payload.get("segments",[])]
            if kind=="scene" and payload.get("stage")=="scan":
                dur=await get_duration(path); cuts=await detect_scenes(path,uid,await src_msg.reply("🔍 **Resuming scene scan…**"),dur)
                segs=_scene_segments_from_cuts(cuts,dur)
                payload.update({"stage":"split","cuts":cuts,"segments":segs,"next_index":0})
                await _job_update(job["job_id"],payload=payload)
            if kind=="highlights" and payload.get("stage") in {"transcribe","select","vision"}:
                dur=await get_duration(path)
                st=await src_msg.reply("♻️ **Resuming AI highlight analysis…**")
                if payload.get("stage")=="transcribe" or not payload.get("segments"):
                    segs=await _transcribe_video(path,dur,uid,st); payload["segments"]=segs; payload["stage"]="select"; await _job_update(job["job_id"],payload=payload)
                if payload.get("stage")=="select":
                    hs=await _select_highlights(payload.get("segments",[]),dur); payload["highlights"]=hs; payload["stage"]="vision" if HIGHLIGHT_USE_VISION else "split"; await _job_update(job["job_id"],payload=payload)
                if payload.get("stage")=="vision" and HIGHLIGHT_USE_VISION:
                    hs=await _vision_rank_highlights(path,uid,payload.get("highlights",[])); payload["highlights"]=hs; payload["stage"]="split"; await _job_update(job["job_id"],payload=payload)
                segs=[(h["start"],h["end"]-h["start"]) for h in payload.get("highlights",[])]
                payload["segments"]=segs; await _job_update(job["job_id"],payload=payload)
            label=payload.get("label",f"Recovered {kind} job…")
            await _run_split_segments(src_msg,uid,segs,label,job_id=job["job_id"],start_index=int(payload.get("next_index",0)))
            return True,"resumed"
    return False,"unsupported"

@app.on_message(filters.command("retry"), group=1)
async def cmd_retry(client,message):
    if await _dedup(message): return
    uid=_uid(message)
    jobs=[j for j in await _job_get_active() if j["uid"]==uid]
    if not jobs: return await message.reply("ℹ️ No recoverable job found.")
    ok,why=await _resume_job_record(jobs[-1],message)
    if not ok: await message.reply(f"⚠️ Recovery could not start: `{why}`")

@app.on_message(filters.command("resume"), group=1)
async def cmd_resume(client,message):
    return await cmd_retry(client,message)

# ══════════════════════════════════════════════════════════════
#  COMMANDS
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.command("start"), group=1)
async def cmd_start(client, message):
    if await _dedup(message): return
    name = getattr(message.from_user, "first_name", "User") or "User"
    await message.reply(
        f"⚡ **VIDEO SPLIT PRO v10**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👋 Welcome, **{name}**!\n\n"
        f"🎥 **Step 1 — Send your video**\n"
        f"📥 Wait for `Download complete`\n"
        f"🎛 **Step 2 — Choose an action below**\n\n"
        f"✨ Fast split • Smart scenes • Multi-AI highlights\n"
        f"🔁 Retry-safe uploads • Live progress • `/cancel` anytime\n\n"
        f"👇 **Choose what you want to do:**",
        reply_markup=_main_menu_kb(),
    )

@app.on_message(filters.command("help"), group=1)
async def cmd_help(client, message):
    if await _dedup(message): return
    await message.reply(
        "📖 **VIDEO SPLIT PRO v10 — COMMAND CENTER**\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "🎥 **1. Start**\n"
        "Send a video → wait for download → tap a button.\n\n"
        "✂️ **2. Split**\n"
        "`/split 3` → 3 equal parts\n"
        "`/splitmin 5` → every 5 minutes\n"
        "`/splitsize 500` → ~500 MB chunks\n"
        "`/splitscene` → smart scene-based clips\n\n"
        "🤖 **3. AI**\n"
        "`/highlights` → transcript + AI ranking + vision check\n"
        "`/describe` → AI description\n"
        "`/aistatus` → AI availability/model\n\n"
        "🎬 **4. Video Tools**\n"
        "`/trim 1:00 3:30` → cut a time range\n"
        "`/compress medium` → low/medium/high compression\n"
        "`/extractaudio` → MP3 audio\n\n"
        "🔗 **5. Merge**\n"
        "`/mergestart` → collect videos\n"
        "`/mergedone` → merge queue\n"
        "`/mergecancel` → clear queue\n\n"
        "🛠 **6. Control & Recovery**\n"
        "`/info` → file details + suggestions\n"
        "`/status` → current task/progress\n"
        "`/cancel` → stop current task\n"
        "`/clear` → reset idle/stuck state\n\n"
        "📊 **Admin**\n"
        "`/stats` → bot statistics\n"
        "`/broadcast text` → admin broadcast\n\n"
        "💡 **Tip:** Buttons are the easiest way — tap **📖 All Commands** or send `/start`.\n"
        "⚡ Designed for large videos and Render Free resource limits.",
        reply_markup=_main_menu_kb(),
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
#  LARGE-FILE PREFLIGHT
# ══════════════════════════════════════════════════════════════
def _preflight_large_file(file_size: int) -> tuple[bool, str]:
    """Fail early instead of letting a nearly-full filesystem look stuck."""
    if file_size and file_size > MAX_INPUT_BYTES:
        return False, f"File too large: `{_sz(file_size)}`. Limit is about `{_sz(MAX_INPUT_BYTES)}`."
    try:
        free = shutil.disk_usage(DOWNLOAD_DIR).free
    except Exception:
        return True, ""
    if file_size and free < file_size + DISK_SAFETY_BYTES:
        return False, (
            f"Server storage low. Need about `{_sz(file_size + DISK_SAFETY_BYTES)}` free, "
            f"but only `{_sz(free)}` is available."
        )
    return True, ""


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

    ok_preflight, preflight_reason = _preflight_large_file(file_size)
    if not ok_preflight:
        return await message.reply(
            f"❌ **Cannot start safely**\n{preflight_reason}\n\n"
            "Try a smaller file or free server storage first."
        )

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
        user_sources[uid] = {"chat_id": message.chat.id, "source_message_id": message.id, "source_path": path}
        # Persistent source record lets recovery re-download the original from Telegram after a Render restart.
        try:
            old = await _source_for(uid)
            if old: await _job_delete(old["job_id"])
            await _job_create(uid, message.chat.id, message.id, "source", {"stage":"ready"}, path, job_id=uuid.uuid4().hex[:16])
        except Exception as e:
            log.warning("Could not persist source checkpoint: %s", e)
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


def _scene_segments_from_cuts(cuts: list[float], duration: float) -> list[tuple[float, float]]:
    """Turn scene boundaries into sane segments and eliminate edge fragments."""
    if duration <= 0:
        return []
    min_len = max(2.5, SCENE_MIN_GAP_SEC)
    clean = sorted({round(float(c), 2) for c in cuts if 0.0 < float(c) < duration})

    # A cut very close to the beginning/end creates a useless tiny clip.
    clean = [c for c in clean if c >= min_len * 0.75 and duration - c >= min_len * 0.75]

    # Re-run spacing after edge cleanup.
    spaced: list[float] = []
    for c in clean:
        if not spaced or c - spaced[-1] >= min_len:
            spaced.append(c)

    bounds = [0.0] + spaced + [duration]
    segments: list[tuple[float, float]] = []
    for a, b in zip(bounds, bounds[1:]):
        d = b - a
        if d < min_len * 0.75 and segments:
            # Merge a tiny tail into the previous scene.
            prev_start, prev_dur = segments[-1]
            segments[-1] = (prev_start, b - prev_start)
        elif d >= min_len * 0.75:
            segments.append((a, d))

    # Safety fallback: never return an empty result for a valid video.
    if not segments and duration > 0:
        return [(0.0, duration)]
    return segments


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
        src = user_sources.get(uid) or await _source_for(uid)
        job_id = await _job_create(uid, message.chat.id, src["source_message_id"], "scene", {"stage":"scan","cuts":[],"segments":[],"next_index":0}, file) if src else None
        scan_msg = await message.reply("🔍 **Scanning for scene changes…**")
        cuts = await detect_scenes(file, uid, scan_msg, dur)
        if job_id:
            await _job_update(job_id, payload={"stage":"segments","cuts":cuts,"segments":[],"next_index":0})
        if not cuts:
            await _safe_edit(scan_msg,
                "❌ Koi clear scene change nahi mila.\n👉 `/split N` try karo instead.")
            return
        segments = _scene_segments_from_cuts(cuts, dur)
        if len(segments) < 2:
            await _safe_edit(scan_msg,
                "❌ Sirf 1 scene mila — poora video ek jaisa hai.\n👉 `/split N` try karo.")
            return
        # Absolute safety cap; normal duration-aware detection stays well below this.
        if len(segments) > SCENE_MAX_COUNT:
            segments = segments[:SCENE_MAX_COUNT]
        await _safe_edit(scan_msg, f"🎬 **{len(segments)} scenes mile!** Splitting shuru ho raha hai…")
        _track_video_processed()
        if job_id:
            await _job_update(job_id, payload={"stage":"split","cuts":cuts,"segments":segments,"next_index":0,"label":f"{len(segments)} scenes — smart scene split…"})
        await _run_split_segments(
            message, uid, segments, f"{len(segments)} scenes — smart scene split…",
            caption_fn=lambda i, t: f"🎬 **Scene {i} / {t}**", job_id=job_id
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
        src = user_sources.get(uid) or await _source_for(uid)
        job_id = await _job_create(uid, message.chat.id, src["source_message_id"], "highlights", {"stage":"transcribe","segments":[],"highlights":[],"next_index":0}, file) if src else None
        status = await message.reply(
            "🎙 **Audio nikal rahe hain aur samajh rahe hain…**\n"
            "  _(Pura dialogue analyze ho raha hai — thoda time lagega)_"
        )
        segments = await _transcribe_video(file, dur, uid, status)
        if job_id:
            await _job_update(job_id, payload={"stage":"select","segments":segments,"highlights":[],"next_index":0})
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
        if job_id:
            await _job_update(job_id, payload={"stage":"vision" if HIGHLIGHT_USE_VISION else "split","segments":segments,"highlights":highlights,"next_index":0})
        if highlights and HIGHLIGHT_USE_VISION and not _get_cancel(uid).is_set():
            await _safe_edit(status, f"👁️ **Second AI vision check…** ({min(HIGHLIGHT_VISION_CHECKS, len(highlights))} candidates)")
            highlights = await _vision_rank_highlights(file, uid, highlights)
        if not highlights:
            await _safe_edit(status,
                "❌ Koi strong highlight nahi mila.\n👉 `/splitscene` ya `/split N` try karo.")
            return

        await _safe_edit(status, f"🔥 **{len(highlights)} AI-ranked clips mile!** Cutting shuru…")
        if job_id:
            await _job_update(job_id, payload={"stage":"split","segments":[(h["start"], h["end"]-h["start"]) for h in highlights],"highlights":highlights,"next_index":0,"label":f"{len(highlights)} AI-picked highlights…"})
        segs = [(h["start"], h["end"] - h["start"]) for h in highlights]
        reasons = [h["reason"] for h in highlights]
        _track_video_processed()
        await _run_split_segments(
            message, uid, segs, f"{len(highlights)} AI-picked highlights…",
            caption_fn=lambda i, t: f"🔥 **Highlight {i} / {t}**\n_{reasons[i-1]}_",
            job_id=job_id,
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
async def _menu_text(kind: str):
    pages = {
        "split": ("✂️ **SPLIT & SCENE**", "`/split N` — equal parts\n`/splitmin N` — minute chunks\n`/splitsize N` — size chunks\n`/splitscene` — smart scenes\n\n🎯 Scene detection is conservative to avoid the old tiny-clip problem."),
        "ai": ("🤖 **AI CENTER**", "`/highlights` — multi-stage highlight selection\n`/describe` — describe video\n`/aistatus` — AI status\n\n🧠 Highlights use transcript analysis first, then a second vision check when enabled."),
        "tools": ("🎬 **VIDEO TOOLS**", "`/trim 1:00 3:30` — trim\n`/compress medium` — compress\n`/extractaudio` — MP3\n\n⚡ Processing shows live status. Use `/cancel` during long jobs."),
        "merge": ("🔗 **MERGE CENTER**", "1️⃣ `/mergestart`\n2️⃣ Send videos in order\n3️⃣ Tap **Done** or use `/mergedone`\n4️⃣ Use `/mergecancel` to discard\n\nMaximum queue: 20 videos."),
        "status": ("📊 **STATUS & CONTROL**", "`/info` — current video\n`/status` — current task\n`/cancel` — stop safely\n`/clear` — reset idle state\n\nIf something looks stuck: `/status` → `/cancel` → `/clear`."),
        "how": ("⚙️ **HOW IT WORKS**", "📥 Download → 💾 save locally → 🎬 FFmpeg/AI processing → 📤 Telegram upload\n\n📈 Progress is throttled to reduce Telegram API overhead.\n🔁 Uploads retry transient failures.\n🧹 Temporary output is cleaned after delivery.\n\n⚠️ Render Free CPU/RAM/network limits can still affect speed."),
        "help": ("📖 **ALL COMMANDS**", "`/start` `/help`\n`/split` `/splitmin` `/splitsize` `/splitscene`\n`/highlights` `/describe` `/aistatus`\n`/trim` `/compress` `/extractaudio`\n`/mergestart` `/mergedone` `/mergecancel`\n`/info` `/status` `/cancel` `/clear`\n`/retry` `/resume` — recovery\n`/stats` `/broadcast` (admin)"),
    }
    return pages.get(kind, pages["help"])


def _menu_kb(kind: str):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✂️ Split", callback_data="menu:split"), InlineKeyboardButton("🤖 AI", callback_data="menu:ai")],
        [InlineKeyboardButton("🎬 Tools", callback_data="menu:tools"), InlineKeyboardButton("🔗 Merge", callback_data="menu:merge")],
        [InlineKeyboardButton("📊 Status", callback_data="menu:status"), InlineKeyboardButton("📖 Commands", callback_data="menu:help")],
        [InlineKeyboardButton("🏠 Main Menu", callback_data="menu:home")],
    ])


@app.on_callback_query(filters.regex(r"^menu:(home|split|ai|tools|merge|status|how|help)$"))
async def cb_menu(client, callback_query):
    await callback_query.answer()
    kind = callback_query.data.split(":", 1)[1]
    if kind == "home":
        text = "⚡ **VIDEO SPLIT PRO v10**\n━━━━━━━━━━━━━━━━━━━━━━\n🎥 Send a video → choose an action.\n\n👇 Tap a category below:"
        kb = _main_menu_kb()
    else:
        title, body = await _menu_text(kind)
        text = f"{title}\n━━━━━━━━━━━━━━━━━━━━━━\n{body}"
        kb = _menu_kb(kind)
    try:
        await callback_query.message.edit_text(text, reply_markup=kb)
    except Exception as e:
        log.debug("menu edit failed: %s", e)


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


async def _startup_recovery():
    try:
        await _db_init()
        jobs=await _job_get_active()
        if not jobs:
            return
        log.warning("♻️ Found %d unfinished job(s); starting automatic recovery.",len(jobs))
        for job in jobs:
            try:
                await _job_update(job["job_id"],state="recovering",attempts_inc=True)
                ok,why=await _resume_job_record(job)
                if not ok:
                    log.warning("Recovery deferred for %s: %s",job["job_id"],why)
            except Exception:
                log.exception("Recovery failed for job %s",job["job_id"])
                await _job_update(job["job_id"],state="retry")
    except Exception:
        log.exception("Startup recovery initialization failed")

if __name__ == "__main__":
    log.info(f"🚀 Ultra Bot v10 starting… FAST_UPLOAD_MODE={FAST_UPLOAD_MODE} THROTTLE={THROTTLE}s")
    asyncio.get_event_loop().run_until_complete(_db_init())
    _start_dummy_webserver()
    async def _boot_recovery():
        await asyncio.sleep(3)
        await _startup_recovery()
    asyncio.get_event_loop().create_task(_boot_recovery())
    app.run()
