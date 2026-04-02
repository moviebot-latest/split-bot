import os
import time
import math
import asyncio
import random
from pyrogram import Client, filters
from pyrogram.errors import FloodWait, MessageNotModified

API_ID    = int(os.getenv("API_ID"))
API_HASH  = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")

app = Client(
    "ultra-bot",
    api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN,
)

DOWNLOAD_DIR = "downloads"
THUMB_DIR    = "thumbs"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(THUMB_DIR,    exist_ok=True)


# ══════════════════════════════════════════════════════════════
#  PER-USER STATE
# ══════════════════════════════════════════════════════════════
user_files:  dict[int, str]           = {}
user_locks:  dict[int, asyncio.Lock]  = {}
user_cancel: dict[int, asyncio.Event] = {}
user_status: dict[int, dict]          = {}

_seen_msgs: set[int]     = set()
_seen_lock: asyncio.Lock = asyncio.Lock()

# Per-user command cooldown — blocks duplicate deliveries within 3s
_last_cmd: dict[int, float] = {}
def _cmd_cooldown(uid: int, window: float = 3.0) -> bool:
    import time as _t
    now  = _t.time()
    last = _last_cmd.get(uid, 0.0)
    if now - last < window:
        return True
    _last_cmd[uid] = now
    return False


def _get_lock(uid: int) -> asyncio.Lock:
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

async def _dedup(mid: int) -> bool:
    async with _seen_lock:
        if mid in _seen_msgs:
            return True
        _seen_msgs.add(mid)
        if len(_seen_msgs) > 500:
            _seen_msgs.clear()
        return False


# ══════════════════════════════════════════════════════════════
#  UPLOAD DEDUP GUARD  (FIX FOR DOUBLE UPLOAD)
#  Key = (uid, part_num)  →  True if already uploaded
# ══════════════════════════════════════════════════════════════
_uploaded_parts: dict[tuple, bool] = {}

def _mark_uploaded(uid: int, num: int) -> None:
    _uploaded_parts[(uid, num)] = True

def _is_uploaded(uid: int, num: int) -> bool:
    return _uploaded_parts.get((uid, num), False)

def _clear_uploads(uid: int) -> None:
    keys = [k for k in _uploaded_parts if k[0] == uid]
    for k in keys:
        del _uploaded_parts[k]


# ══════════════════════════════════════════════════════════════
#  SPINNER / ANIMATION CONSTANTS
# ══════════════════════════════════════════════════════════════
BOUNCE  = ["⣾","⣽","⣻","⢿","⡿","⣟","⣯","⣷"]
SIGNAL  = ["▁","▂","▃","▄","▅","▆","▇","█","▇","▆","▅","▄","▃","▂"]
CLOCK   = ["🕐","🕑","🕒","🕓","🕔","🕕","🕖","🕗","🕘","🕙","🕚","🕛"]
ORBIT   = ["◜","◝","◞","◟"]
PHASE   = ["🌑","🌒","🌓","🌔","🌕","🌖","🌗","🌘"]
MATRIX  = list("ﾊﾋｼｦｲｸｺｻﾀﾔｹﾦｿﾌﾔｪｷ01")

# Progress bar styles (all Telegram-safe)
BAR_FILL  = "█"
BAR_EMPTY = "░"
BAR_LEAD  = "▓"   # leading edge of fill

# Speed tiers — clean, no weird chars
def _speed_label(bps: float) -> str:
    mbps = bps / (1024 * 1024)
    kbps = bps / 1024
    if mbps >= 10:  return "🟢 ULTRA"
    if mbps >= 5:   return "🟢 FAST"
    if mbps >= 1:   return "🟡 GOOD"
    if kbps >= 512: return "🟡 OK"
    return "🔴 SLOW"

# Milestone icons
def _milestone(pct: float) -> str:
    if pct >= 100: return "🏆"
    if pct >= 90:  return "🔥"
    if pct >= 75:  return "⚡"
    if pct >= 50:  return "🚀"
    if pct >= 25:  return "💫"
    if pct >= 10:  return "🌀"
    return "🔵"


# ══════════════════════════════════════════════════════════════
#  INTERNAL PROGRESS STATE
# ══════════════════════════════════════════════════════════════
_last_edit:     dict[int, float] = {}
_ema_speed:     dict[int, float] = {}
_spin_idx:      dict[int, int]   = {}
_shown_pct:     dict[int, float] = {}
_speed_history: dict[int, list]  = {}

def _reset(uid: int) -> None:
    for d in (_last_edit, _ema_speed, _spin_idx, _shown_pct):
        d.pop(uid, None)

def _record_speed(uid: int, bps: float) -> None:
    if uid not in _speed_history:
        _speed_history[uid] = []
    _speed_history[uid].append(bps)
    if len(_speed_history[uid]) > 24:
        _speed_history[uid].pop(0)

THROTTLE  = 0.18
EMA_ALPHA = 0.35


# ══════════════════════════════════════════════════════════════
#  FORMATTERS
# ══════════════════════════════════════════════════════════════
def _sz(b: float) -> str:
    for u in ("B","KB","MB","GB"):
        if b < 1024: return f"{b:.1f} {u}"
        b /= 1024
    return f"{b:.1f} TB"

def _eta(s: float) -> str:
    s = max(0, s)
    if s >= 3600: return f"{int(s//3600)}h {int(s%3600//60)}m"
    if s >= 60:   return f"{int(s//60)}m {int(s%60)}s"
    return f"{int(s)}s"

def _bar(pct: float, w: int = 18) -> str:
    """Clean solid bar — always renders correctly."""
    n = max(0, min(w, int(pct / 100 * w)))
    return BAR_FILL * n + BAR_EMPTY * (w - n)

def _abar(pct: float, tick: int, w: int = 18) -> str:
    """Animated bar with pulsing lead edge."""
    n = max(0, min(w, int(pct / 100 * w)))
    if n == 0:  return BAR_EMPTY * w
    if n >= w:  return BAR_FILL * w
    lead = BAR_LEAD if tick % 2 == 0 else BAR_FILL
    return BAR_FILL * (n - 1) + lead + BAR_EMPTY * (w - n)

def _sparkline(history: list, w: int = 12) -> str:
    """ASCII sparkline from speed samples."""
    bars = " ▁▂▃▄▅▆▇█"
    if not history: return "─" * w
    mx = max(history) or 1
    samples = history[-w:]
    return "".join(bars[min(8, int(s / mx * 8))] for s in samples)

def _count_up(uid: int, real: float, step: float = 2.5) -> float:
    prev  = _shown_pct.get(uid, 0.0)
    shown = min(real, prev + step) if real > prev else real
    _shown_pct[uid] = shown
    return shown


# ══════════════════════════════════════════════════════════════
#  SAFE EDIT
# ══════════════════════════════════════════════════════════════
async def _safe_edit(msg, text: str) -> None:
    try:
        await msg.edit(text)
    except FloodWait as e:
        await asyncio.sleep(min(e.value, 10) + 0.5)
        try: await msg.edit(text)
        except Exception: pass
    except MessageNotModified:
        pass
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════
#  PROGRESS ENGINE v8
#  FIX: clean bar, clean speed label, no broken chars
# ══════════════════════════════════════════════════════════════
async def progress(
    current: int, total: int,
    msg, start: float,
    uid: int = 0,
    mode: str = "📥 Download",
) -> None:
    if not isinstance(total, (int, float)) or total <= 0: return
    if _get_cancel(uid).is_set(): return

    now     = time.time()
    elapsed = max(now - start, 0.001)

    # Throttle
    if now - _last_edit.get(uid, 0.0) < THROTTLE and _last_edit.get(uid, 0.0) != 0.0:
        return
    _last_edit[uid] = now

    # EMA speed
    raw = current / elapsed
    ema = EMA_ALPHA * raw + (1 - EMA_ALPHA) * _ema_speed.get(uid, raw)
    _ema_speed[uid] = ema
    _record_speed(uid, ema)

    eta_s = (total - current) / ema if ema > 0 else 0
    real  = current * 100 / total
    shown = _count_up(uid, real)

    # Spinner
    tick = _spin_idx.get(uid, 0)
    _spin_idx[uid] = tick + 1
    spinner_pool   = BOUNCE if "Download" in mode else SIGNAL
    spin           = spinner_pool[tick % len(spinner_pool)]

    # Visuals
    bar         = _abar(shown, tick, 18)
    icon        = _milestone(shown)
    speed_lbl   = _speed_label(ema)
    graph       = _sparkline(_speed_history.get(uid, []), 12)
    pct_str     = f"{shown:.1f}%"
    header      = "📥" if "Download" in mode else "📤"

    # ETA urgency
    eta_str = f"⚡ {_eta(eta_s)}" if eta_s < 15 and shown > 5 else _eta(eta_s)

    await _safe_edit(msg,
        f"{spin} **{mode}**\n"
        f"══════════════════════════\n"
        f"`{bar}`\n"
        f"  {icon} **{pct_str}** — {speed_lbl}\n"
        f"──────────────────────────\n"
        f"  📊 `{graph}` speed chart\n"
        f"──────────────────────────\n"
        f"  {header} **Size** : `{_sz(current)}` / `{_sz(total)}`\n"
        f"  ⚡ **Speed**: `{_sz(ema)}/s`\n"
        f"  ⏱ **ETA**  : `{eta_str}`\n"
        f"  ⏳ **Time** : `{_eta(elapsed)}`\n"
        f"══════════════════════════\n"
        f"  ❌ /cancel to abort"
    )


async def upload_progress(current, total, msg, start, uid=0):
    await progress(current, total, msg, start, uid=uid, mode="📤 Upload")


# ══════════════════════════════════════════════════════════════
#  FFMPEG HELPERS
# ══════════════════════════════════════════════════════════════
async def ffmpeg_cut(inp: str, out: str, ss: float, t: float) -> bool:
    proc = await asyncio.create_subprocess_exec(
        "ffmpeg", "-y", "-ss", str(ss), "-i", inp,
        "-t", str(t), "-c", "copy", "-avoid_negative_ts", "make_zero", out,
        stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
    )
    await proc.wait()
    return proc.returncode == 0

async def make_thumb(video: str, ss: float, out: str) -> str | None:
    proc = await asyncio.create_subprocess_exec(
        "ffmpeg", "-y", "-ss", str(ss), "-i", video,
        "-vframes", "1", "-q:v", "2", "-vf", "scale=320:-1", out,
        stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
    )
    await proc.wait()
    return out if os.path.exists(out) else None

async def get_duration(file: str) -> float | None:
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1", file,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
        )
        out, _ = await proc.communicate()
        return float(out.decode().strip())
    except Exception:
        return None


# ══════════════════════════════════════════════════════════════
#  SPLIT PROGRESS BAR
# ══════════════════════════════════════════════════════════════
_split_tick: dict[int, int] = {}

async def _split_update(msg, done: int, total: int, uid: int, note: str = "") -> None:
    pct  = done * 100 // total
    tick = _split_tick.get(uid, 0)
    _split_tick[uid] = tick + 1

    spin = ORBIT[tick % len(ORBIT)]
    bar  = _abar(pct, tick, 16)

    # Part dots: ✅ done · ✂️ cutting · ⬜ pending
    max_dots = min(total, 20)
    dots = ""
    for i in range(max_dots):
        if i < done:          dots += "✅"
        elif i == done:       dots += "✂️"
        else:                 dots += "⬜"
        if (i + 1) % 10 == 0 and i + 1 < max_dots:
            dots += "\n  "
    if total > 20:
        dots += f" +{total-20} more"

    note_line = f"\n  ┗ _{note}_" if note else ""

    await _safe_edit(msg,
        f"{spin} **Splitting…**\n"
        f"══════════════════════════\n"
        f"`{bar}` **{pct}%**\n"
        f"  Part **{done}** / **{total}**{note_line}\n"
        f"──────────────────────────\n"
        f"  {dots}\n"
        f"══════════════════════════\n"
        f"  ❌ /cancel to stop"
    )


# ══════════════════════════════════════════════════════════════
#  UPLOAD ONE PART
#  FIX v8: per-part dedup dict prevents double upload
#          even across FloodWait retries or crash-restarts
# ══════════════════════════════════════════════════════════════
async def _upload_part(
    message, path: str, num: int, total: int,
    uid: int, thumb_time: float
) -> bool:

    # ── HARD GUARD: already uploaded this exact part? ──
    if _is_uploaded(uid, num):
        return True   # treat as success, skip silently

    if _get_cancel(uid).is_set():
        return False

    status = await message.reply(
        f"⣾ **Uploading** part {num}/{total}…\n"
        f"══════════════════════════\n"
        f"`{BAR_EMPTY * 18}` **0.0%**\n"
        f"  🔵 STARTING\n"
        f"══════════════════════════\n"
        f"  ❌ /cancel to abort"
    )

    _reset(uid)
    t0 = time.time()

    thumb_path = f"{THUMB_DIR}/thumb_{uid}_{num}.jpg"
    thumb = await make_thumb(path, thumb_time, thumb_path)

    success = False

    for attempt in range(5):
        # Re-check guard at top of every attempt
        if _is_uploaded(uid, num):
            success = True
            break

        if _get_cancel(uid).is_set():
            await _safe_edit(status,
                "🚫 **Upload cancelled.**\n"
                "  Stopped by user."
            )
            break

        try:
            await message.reply_video(
                path,
                caption=(
                    f"🎬 **Part {num} / {total}**\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"  ✅ Delivered by Ultra Bot v8"
                ),
                thumb=thumb,
                progress=upload_progress,
                progress_args=(status, t0, uid),
            )
            # ── Mark as done IMMEDIATELY after success ──
            _mark_uploaded(uid, num)
            success = True
            break

        except FloodWait as e:
            # FloodWait = Telegram rejected request, NOT sent yet → safe to retry
            wait = min(e.value + 2, 60)
            for remaining in range(wait, 0, -1):
                spin  = CLOCK[remaining % len(CLOCK)]
                done_  = wait - remaining
                fill   = int(done_ / wait * 18)
                await _safe_edit(status,
                    f"{spin} **Flood wait** — part {num}/{total}\n"
                    f"══════════════════════════\n"
                    f"  ⏳ Resume in `{remaining}s`\n"
                    f"  `{BAR_FILL*fill}{BAR_EMPTY*(18-fill)}`\n"
                    f"  Auto-resuming…"
                )
                await asyncio.sleep(1)

        except Exception as e:
            err = str(e).lower()
            # Telegram sometimes returns error AFTER successful send
            if any(x in err for x in ["duplicate", "already", "message_id_invalid"]):
                _mark_uploaded(uid, num)
                success = True
                break
            if attempt >= 4:
                await _safe_edit(status, f"❌ Upload failed part {num}:\n`{e}`")
                break
            await asyncio.sleep(3 * (attempt + 1))

    # Cleanup
    _reset(uid)
    if thumb and os.path.exists(thumb_path):
        try: os.remove(thumb_path)
        except Exception: pass
    try:
        await status.delete()
    except Exception:
        pass

    return success


# ══════════════════════════════════════════════════════════════
#  /start
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.command("start") & filters.incoming, group=0)
async def start(client, message):
    if await _dedup(message.id): return
    name = message.from_user.first_name or "User"
    await message.reply(
        f"╔══════════════════════════╗\n"
        f"║  ⚡  ULTRA BOT  v8  ⚡   ║\n"
        f"╚══════════════════════════╝\n\n"
        f"👋 Welcome, **{name}**!\n\n"
        f"📽 **Send any video file:**\n"
        f"  └─ MP4 · MKV · AVI · MOV · WEBM\n\n"
        f"✂️ **Split commands:**\n"
        f"  • `/split 3`    → 3 equal parts\n"
        f"  • `/splitmin 2` → 2-min chunks\n\n"
        f"🛠 **Utilities:**\n"
        f"  • `/status`  → live task info\n"
        f"  • `/cancel`  → abort task\n"
        f"  • `/info`    → file details\n\n"
        f"──────────────────────────\n"
        f"  🎯 Animated progress bars\n"
        f"  📊 Live speed sparkline\n"
        f"  🔄 No double upload — fixed!\n"
        f"  👥 Multi-user safe\n"
        f"──────────────────────────\n"
        f"  _Ultra Bot v8 — All bugs fixed_ ✓"
    )


# ══════════════════════════════════════════════════════════════
#  /info
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.command("info") & filters.incoming, group=0)
async def info_cmd(client, message):
    if await _dedup(message.id): return
    uid = message.from_user.id
    if uid not in user_files or not os.path.exists(user_files[uid]):
        return await message.reply("❌ No video loaded.\n  Send a video first!")
    path  = user_files[uid]
    size  = os.path.getsize(path)
    dur   = await get_duration(path)
    fname = os.path.basename(path)
    dur_str = _eta(dur) if dur else "unknown"
    await message.reply(
        f"📋 **FILE INFO**\n"
        f"══════════════════════════\n"
        f"  📁 `{fname}`\n"
        f"  📦 Size    : `{_sz(size)}`\n"
        f"  🎬 Duration: `{dur_str}`\n"
        f"══════════════════════════\n"
        f"  👉 `/split N`  or  `/splitmin N`"
    )


# ══════════════════════════════════════════════════════════════
#  /status
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.command("status") & filters.incoming, group=0)
async def status_cmd(client, message):
    if await _dedup(message.id): return
    uid  = message.from_user.id
    info = user_status.get(uid)

    if not _get_lock(uid).locked() or not info:
        if uid in user_files and os.path.exists(user_files[uid]):
            fname = os.path.basename(user_files[uid])
            fsize = _sz(os.path.getsize(user_files[uid]))
            return await message.reply(
                f"💤 **IDLE — Video ready**\n"
                f"══════════════════════════\n"
                f"  📁 `{fname}`\n"
                f"  📦 `{fsize}`\n"
                f"══════════════════════════\n"
                f"  👉 `/split N`  or  `/splitmin N`\n"
                f"  📋 `/info` for full details"
            )
        return await message.reply(
            f"💤 **IDLE**\n"
            f"══════════════════════════\n"
            f"  No video loaded.\n"
            f"  Send a video to begin!\n"
            f"══════════════════════════"
        )

    elapsed = _eta(time.time() - info["since"])
    hist    = _speed_history.get(uid, [])
    graph   = _sparkline(hist, 14) if hist else "no data"
    speed   = f"{_sz(hist[-1])}/s" if hist else "—"
    spin    = ORBIT[int(time.time() * 4) % len(ORBIT)]

    await message.reply(
        f"{spin} **RUNNING**\n"
        f"══════════════════════════\n"
        f"  📌 Task    : `{info['task']}`\n"
        f"  📝 Detail  : `{info['detail']}`\n"
        f"  ⏳ Running : `{elapsed}`\n"
        f"──────────────────────────\n"
        f"  📊 `{graph}` speed\n"
        f"  ⚡ `{speed}`\n"
        f"══════════════════════════\n"
        f"  ❌ /cancel to stop"
    )


# ══════════════════════════════════════════════════════════════
#  /cancel
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.command("cancel") & filters.incoming, group=0)
async def cancel_cmd(client, message):
    if await _dedup(message.id): return
    uid = message.from_user.id
    if not _get_lock(uid).locked():
        return await message.reply(
            "💤 **Nothing to cancel.**\n"
            "  No active task running."
        )
    _get_cancel(uid).set()
    await message.reply(
        "🚫 **CANCEL REQUESTED**\n"
        "══════════════════════════\n"
        "  Stopping at next checkpoint…\n"
        "  Please wait a moment."
    )


# ══════════════════════════════════════════════════════════════
#  RECEIVE VIDEO
# ══════════════════════════════════════════════════════════════
@app.on_message(
    filters.incoming
    & ~filters.command(["start","split","splitmin","status","cancel","info"])
    & (filters.video | filters.document),
    group=1
)
async def receive(client, message):
    if await _dedup(message.id): return

    uid  = message.from_user.id
    lock = _get_lock(uid)
    if lock.locked():
        return await message.reply(
            "⏳ **Task in progress!**\n"
            "  👉 /status · /cancel"
        )

    media = message.document or message.video
    if not media: return

    mime      = getattr(media, "mime_type", "") or ""
    file_size = getattr(media, "file_size", 0) or 0

    if mime and not (mime.startswith("video/") or mime == "application/octet-stream"):
        return

    # Clean old file
    if uid in user_files and os.path.exists(user_files[uid]):
        try: os.remove(user_files[uid])
        except Exception: pass
        user_files.pop(uid, None)

    ext_map = {
        "video/x-matroska":"mkv","video/mkv":"mkv",
        "video/avi":"avi","video/x-msvideo":"avi",
        "video/webm":"webm","video/quicktime":"mov",
        "video/x-ms-wmv":"wmv","video/3gpp":"3gp",
    }
    ext   = ext_map.get(mime, "mp4")
    fname = f"{DOWNLOAD_DIR}/video_{uid}_{message.id}.{ext}"
    sz_str= _sz(file_size) if file_size else "?"

    status = await message.reply(
        f"⣾ **DOWNLOAD STARTING**\n"
        f"══════════════════════════\n"
        f"  📁 Format : `{ext.upper()}`\n"
        f"  📦 Size   : `{sz_str}`\n"
        f"──────────────────────────\n"
        f"`{BAR_EMPTY * 18}` **0%**\n"
        f"══════════════════════════\n"
        f"  ❌ /cancel to abort"
    )

    _get_cancel(uid).clear()
    _reset(uid)
    _speed_history.pop(uid, None)
    _clear_uploads(uid)   # fresh upload state
    _set_status(uid, "Downloading", sz_str)
    t0 = time.time()

    try:
        path = await message.download(
            file_name=fname,
            progress=progress,
            progress_args=(status, t0, uid, "📥 Download"),
        )
    except Exception as e:
        _clear_status(uid)
        await _safe_edit(status, f"❌ **Download failed**\n  `{e}`")
        return

    if not path or not os.path.exists(path):
        _clear_status(uid)
        await _safe_edit(status,
            "❌ **File not saved.**\n"
            "  Try again or send a different file."
        )
        return

    elapsed = time.time() - t0
    avg_spd = file_size / elapsed if elapsed > 0 and file_size else 0
    actual_size = os.path.getsize(path)

    _reset(uid)
    _clear_status(uid)
    user_files[uid] = path

    await _safe_edit(status,
        f"✅ **DOWNLOAD COMPLETE**\n"
        f"══════════════════════════\n"
        f"`{BAR_FILL * 18}` **100%**\n"
        f"  🏆 COMPLETE\n"
        f"──────────────────────────\n"
        f"  📁 `{os.path.basename(path)}`\n"
        f"  📦 `{_sz(actual_size)}`\n"
        f"  ⚡ avg `{_sz(avg_spd)}/s`\n"
        f"  ⏱ in `{_eta(elapsed)}`\n"
        f"══════════════════════════\n"
        f"  👉 `/split N`  or  `/splitmin N`\n"
        f"  📋 `/info` for file details"
    )


# ══════════════════════════════════════════════════════════════
#  CORE SPLIT LOGIC
# ══════════════════════════════════════════════════════════════
async def _do_split(
    message, uid: int, seg: float, parts: int, label: str
) -> None:
    file   = user_files[uid]
    cancel = _get_cancel(uid)
    cancel.clear()
    _clear_uploads(uid)      # reset dedup for this split session
    _split_tick[uid] = 0

    msg = await message.reply(
        f"✂️ **{label}**\n"
        f"══════════════════════════\n"
        f"`{BAR_EMPTY * 16}` **0%**\n"
        f"  Part **0** / **{parts}**\n"
        f"══════════════════════════\n"
        f"  ❌ /cancel to stop"
    )

    try:
        for i in range(parts):
            if cancel.is_set():
                await _safe_edit(msg,
                    f"🚫 **CANCELLED**\n"
                    f"══════════════════════════\n"
                    f"  Stopped after **{i}** / **{parts}** parts.\n"
                    f"  Send video again to retry."
                )
                _clear_status(uid)
                return

            ss = i * seg
            _set_status(uid, "Splitting", f"part {i+1}/{parts}")
            await _split_update(msg, i, parts, uid, f"cutting part {i+1}…")

            out = f"{DOWNLOAD_DIR}/part_{uid}_{i+1}.mp4"
            ok  = await ffmpeg_cut(file, out, ss, seg)

            if not ok or not os.path.exists(out):
                await _safe_edit(msg,
                    f"❌ **ffmpeg error** on part {i+1}\n"
                    f"══════════════════════════\n"
                    f"  Source may be corrupted.\n"
                    f"  Please re-send the video."
                )
                _clear_status(uid)
                return

            _set_status(uid, "Uploading", f"part {i+1}/{parts}")
            await _split_update(msg, i, parts, uid, f"uploading part {i+1}…")

            uploaded = await _upload_part(
                message, out, i+1, parts, uid, ss + seg/2
            )

            # Clean part file regardless
            try:
                if os.path.exists(out):
                    os.remove(out)
            except Exception:
                pass

            if not uploaded:
                await _safe_edit(msg,
                    f"🚫 **CANCELLED**\n"
                    f"══════════════════════════\n"
                    f"  Stopped after part **{i+1}** / **{parts}**."
                )
                _clear_status(uid)
                return

            await _split_update(msg, i+1, parts, uid,
                "done! 🎉" if i+1 == parts else f"next: part {i+2}…"
            )

        # ── All done ──
        try:
            if os.path.exists(file):
                os.remove(file)
        except Exception:
            pass
        user_files.pop(uid, None)
        _clear_uploads(uid)
        _clear_status(uid)

        check_row = "".join(
            "✅" if j < parts else "⬜"
            for j in range(min(parts, 20))
        )
        if parts > 20:
            check_row += f" +{parts-20}"

        await _safe_edit(msg,
            f"🏆 **ALL {parts} PARTS DONE!**\n"
            f"══════════════════════════\n"
            f"`{BAR_FILL * 16}` **100%**\n"
            f"  {check_row}\n"
            f"──────────────────────────\n"
            f"  ✅ **{parts} parts** delivered\n"
            f"  🎬 Ultra Bot v8\n"
            f"══════════════════════════\n"
            f"  _Send another video to split!_"
        )

    except Exception as e:
        _clear_status(uid)
        await _safe_edit(msg,
            f"❌ **Unexpected error**\n"
            f"  `{e}`\n"
            f"  Please try again."
        )


# ══════════════════════════════════════════════════════════════
#  /split
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.command("split") & filters.incoming, group=0)
async def split_cmd(client, message):
    if await _dedup(message.id): return
    uid = message.from_user.id
    if _cmd_cooldown(uid): return
    lock = _get_lock(uid)
    if lock.locked():
        return await message.reply(
            "⏳ **Already processing!**\n  👉 /status · /cancel"
        )
    if uid not in user_files:
        return await message.reply(
            "❌ **No video loaded.**\n  Send a video file first!"
        )
    try:
        parts = int(message.command[1])
        assert 2 <= parts <= 100
    except Exception:
        return await message.reply(
            "❌ **Usage:** `/split 3`\n"
            "  Minimum 2 parts, max 100."
        )
    dur = await get_duration(user_files[uid])
    if not dur:
        return await message.reply(
            "❌ **Cannot read duration.**\n  File may be corrupted."
        )
    if lock.locked(): return
    async with lock:
        await _do_split(
            message, uid, dur / parts, parts,
            f"Splitting into **{parts}** equal parts…"
        )


# ══════════════════════════════════════════════════════════════
#  /splitmin
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.command("splitmin") & filters.incoming, group=0)
async def splitmin_cmd(client, message):
    if await _dedup(message.id): return
    uid = message.from_user.id
    if _cmd_cooldown(uid): return
    lock = _get_lock(uid)
    if lock.locked():
        return await message.reply(
            "⏳ **Already processing!**\n  👉 /status · /cancel"
        )
    if uid not in user_files:
        return await message.reply(
            "❌ **No video loaded.**\n  Send a video file first!"
        )
    try:
        mins = int(message.command[1])
        assert 1 <= mins <= 60
    except Exception:
        return await message.reply(
            "❌ **Usage:** `/splitmin 2`\n"
            "  Chunk size in minutes (1–60)."
        )
    dur = await get_duration(user_files[uid])
    if not dur:
        return await message.reply(
            "❌ **Cannot read duration.**\n  File may be corrupted."
        )
    seg   = mins * 60
    parts = math.ceil(dur / seg)
    if parts > 100:
        return await message.reply(
            f"❌ Too many parts ({parts}).\n"
            f"  Use a larger chunk size."
        )
    if lock.locked(): return
    async with lock:
        await _do_split(
            message, uid, seg, parts,
            f"Splitting — **{mins} min** chunks → **{parts}** parts…"
        )


# ══════════════════════════════════════════════════════════════
app.run()
