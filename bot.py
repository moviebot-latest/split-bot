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
        if len(_seen_msgs) > 300:
            _seen_msgs.clear()
        return False


# ══════════════════════════════════════════════════════════════
#  ANIMATION FRAMES & VISUAL CONSTANTS
# ══════════════════════════════════════════════════════════════

# Cinematic spinners
SPINNER_ORBIT    = ["◜","◝","◞","◟"]
SPINNER_PULSE    = ["◉","○","◉","○"]
SPINNER_MATRIX   = ["▓","▒","░"," ","░","▒","▓"]
SPINNER_RADAR    = ["◴","◷","◶","◵"]
SPINNER_BOUNCE   = ["⣾","⣽","⣻","⢿","⡿","⣟","⣯","⣷"]
SPINNER_WAVE     = ["〰","≈","∿","〰","≈"]
SPINNER_SIGNAL   = ["▁","▂","▃","▄","▅","▆","▇","█","▇","▆","▅","▄","▃","▂"]
SPINNER_CUBE     = ["▪","▫","▪","▫"]
SPINNER_PHASE    = ["🌑","🌒","🌓","🌔","🌕","🌖","🌗","🌘"]
SPINNER_CLOCK    = ["🕐","🕑","🕒","🕓","🕔","🕕","🕖","🕗","🕘","🕙","🕚","🕛"]
SPINNER_DOTS_V2  = ["⣀","⣄","⣤","⣦","⣶","⣷","⣿","⣷","⣶","⣦","⣤","⣄"]

# Download uses BOUNCE, Upload uses SIGNAL
DOWNLOAD_SPINNER = SPINNER_BOUNCE
UPLOAD_SPINNER   = SPINNER_SIGNAL

# Animated bar styles
BAR_STYLES = {
    "classic":  ("█", "░"),
    "smooth":   ("▰", "▱"),
    "gradient": ("▓", "░"),
    "block":    ("■", "□"),
    "sharp":    ("━", "─"),
    "circuit":  ("◼", "◻"),
    "wave":     ("≣", "≡"),
}

# Milestones
MILESTONES = {
    100: ("🏆", "COMPLETE"),
    90:  ("🔥", "BLAZING"),
    75:  ("⚡", "LIGHTNING"),
    50:  ("🚀", "CRUISING"),
    25:  ("💫", "RISING"),
    10:  ("🌀", "SPINNING"),
    0:   ("🔵", "STARTING"),
}

TIER_COLORS = {
    "🟢 ULTRA":  (10 * 1024 * 1024, "🟢 ██ ULTRA ██"),
    "🟩 FAST":   (5  * 1024 * 1024, "🟩 ▓▓ FAST  ▓▓"),
    "🟡 GOOD":   (1  * 1024 * 1024, "🟡 ▒▒ GOOD  ▒▒"),
    "🟠 OK":     (512 * 1024,       "🟠 ░░ OK    ░░"),
    "🔴 SLOW":   (0,                "🔴 ·· SLOW  ··"),
}

THROTTLE  = 0.15   # slightly faster refresh
EMA_ALPHA = 0.35


# ══════════════════════════════════════════════════════════════
#  INTERNAL PROGRESS STATE
# ══════════════════════════════════════════════════════════════
_last_edit:  dict[int, float] = {}
_ema_speed:  dict[int, float] = {}
_spin_idx:   dict[int, int]   = {}
_shown_pct:  dict[int, float] = {}
_frame_tick: dict[int, int]   = {}   # generic frame counter

def _reset(uid: int) -> None:
    for d in (_last_edit, _ema_speed, _spin_idx, _shown_pct, _frame_tick):
        d.pop(uid, None)


# ══════════════════════════════════════════════════════════════
#  UTILITY FORMATTERS
# ══════════════════════════════════════════════════════════════
def _sz(b: float) -> str:
    for u in ("B","KB","MB","GB"):
        if b < 1024: return f"{b:.1f} {u}"
        b /= 1024
    return f"{b:.1f} TB"

def _eta(s: float) -> str:
    s = max(0, s)
    if s >= 3600: return f"{int(s//3600)}h {int(s%3600//60)}m {int(s%60)}s"
    if s >= 60:   return f"{int(s//60)}m {int(s%60)}s"
    return f"{int(s)}s"

def _speed_tier(bps: float) -> str:
    for label, (threshold, display) in TIER_COLORS.items():
        if bps >= threshold:
            return display
    return TIER_COLORS["🔴 SLOW"][1]

def _milestone(pct: float) -> tuple[str, str]:
    for thresh, (icon, label) in sorted(MILESTONES.items(), reverse=True):
        if pct >= thresh:
            return icon, label
    return "🔵", "STARTING"

def _bar(pct: float, width: int = 18, style: str = "smooth") -> str:
    fill, empty = BAR_STYLES.get(style, BAR_STYLES["smooth"])
    n = int(min(pct, 100) / 100 * width)
    return fill * n + empty * (width - n)

def _animated_bar(pct: float, tick: int, width: int = 18) -> str:
    """Shimmer effect — leading edge pulses."""
    filled, empty = BAR_STYLES["smooth"]
    n = int(min(pct, 100) / 100 * width)
    if n == 0:
        return empty * width
    # Shimmer on leading character
    leader = "▸" if tick % 2 == 0 else "▹"
    if n >= width:
        return filled * width
    return filled * (n - 1) + leader + empty * (width - n)

def _count_up(uid: int, real: float, step: float = 2.0) -> float:
    prev  = _shown_pct.get(uid, 0.0)
    shown = min(real, prev + step) if real > prev else real
    _shown_pct[uid] = shown
    return shown

def _mini_graph(speeds: list[float], width: int = 10) -> str:
    """Tiny sparkline from recent speed samples."""
    bars = " ▁▂▃▄▅▆▇█"
    if not speeds: return "─" * width
    mx = max(speeds) or 1
    return "".join(bars[min(8, int(s / mx * 8))] for s in speeds[-width:])


# ══════════════════════════════════════════════════════════════
#  SAFE EDIT
# ══════════════════════════════════════════════════════════════
async def _safe_edit(msg, text: str) -> None:
    try:
        await msg.edit(text)
    except FloodWait as e:
        await asyncio.sleep(e.value + 0.5)
        try: await msg.edit(text)
        except Exception: pass
    except MessageNotModified:
        pass
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════
#  SPEED HISTORY (per user, rolling 20 samples)
# ══════════════════════════════════════════════════════════════
_speed_history: dict[int, list[float]] = {}

def _record_speed(uid: int, bps: float) -> None:
    if uid not in _speed_history:
        _speed_history[uid] = []
    _speed_history[uid].append(bps)
    if len(_speed_history[uid]) > 20:
        _speed_history[uid].pop(0)

def _get_speed_history(uid: int) -> list[float]:
    return _speed_history.get(uid, [])


# ══════════════════════════════════════════════════════════════
#  PROGRESS ENGINE v7  — Cinematic, animated, feature-rich
# ══════════════════════════════════════════════════════════════
async def progress(
    current: int, total: int,
    msg, start: float,
    uid: int = 0, mode: str = "📥 Download",
) -> None:
    if not isinstance(total, (int, float)) or total <= 0: return
    if _get_cancel(uid).is_set(): return

    now     = time.time()
    elapsed = max(now - start, 0.001)

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

    # Animation tick
    tick = _frame_tick.get(uid, 0)
    _frame_tick[uid] = tick + 1

    # Spinner selection
    spinner_frames = DOWNLOAD_SPINNER if "Download" in mode else UPLOAD_SPINNER
    spin = spinner_frames[tick % len(spinner_frames)]

    # Visual elements
    abar          = _animated_bar(shown, tick, 18)
    icon, tier_lbl = _milestone(shown)
    speed_tier    = _speed_tier(ema)
    graph         = _mini_graph(_get_speed_history(uid), 12)
    pct_int       = int(shown)
    pct_tenths    = int((shown - pct_int) * 10)

    # Percentage as digit-style display
    pct_display = f"{pct_int:>3}.{pct_tenths}%"

    # ETA with urgency cue
    if eta_s < 10 and shown > 10:
        eta_str = f"⚡`{_eta(eta_s)}`"
    else:
        eta_str = f"`{_eta(eta_s)}`"

    is_download = "Download" in mode
    header_char = "📥" if is_download else "📤"

    text = (
        f"{spin} **{mode}**\n"
        f"══════════════════════════\n"
        f"`{abar}`\n"
        f"  {icon} **{pct_display}** — {tier_lbl}\n"
        f"──────────────────────────\n"
        f"  📊 `{graph}` ← speed graph\n"
        f"──────────────────────────\n"
        f"  {header_char} **Size**    `{_sz(current)}` / `{_sz(total)}`\n"
        f"  ⚡ **Speed**   {speed_tier}\n"
        f"         `{_sz(ema)}/s`\n"
        f"  ⏱ **ETA**    {eta_str}\n"
        f"  ⏳ **Elapsed** `{_eta(elapsed)}`\n"
        f"══════════════════════════\n"
        f"  ❌ /cancel to abort"
    )
    await _safe_edit(msg, text)


async def upload_progress(current, total, msg, start, uid=0):
    await progress(current, total, msg, start, uid=uid, mode="📤 Upload")


# ══════════════════════════════════════════════════════════════
#  ASYNC FFMPEG + THUMBNAIL + DURATION
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
#  ANIMATED SPLIT PROGRESS
# ══════════════════════════════════════════════════════════════
_split_tick: dict[int, int] = {}

async def _split_update(msg, done: int, total: int, uid: int, note: str = "") -> None:
    pct  = done * 100 // total
    tick = _split_tick.get(uid, 0)
    _split_tick[uid] = tick + 1

    spin  = SPINNER_ORBIT[tick % len(SPINNER_ORBIT)]
    abar  = _animated_bar(pct, tick, 16)
    note_line = f"\n  ┗ _{note}_" if note else ""

    # Part indicators  e.g.  ✅ ✅ ✂️ ○ ○
    indicators = ""
    for i in range(total):
        if i < done:
            indicators += "✅"
        elif i == done:
            indicators += "✂️"
        else:
            indicators += "○"
        if (i + 1) % 10 == 0 and i + 1 < total:
            indicators += "\n  "

    await _safe_edit(msg,
        f"{spin} **Splitting…**\n"
        f"══════════════════════════\n"
        f"`{abar}` **{pct}%**\n"
        f"  Part **{done}** / **{total}** done{note_line}\n"
        f"──────────────────────────\n"
        f"  {indicators}\n"
        f"══════════════════════════\n"
        f"  ❌ /cancel to stop"
    )


# ══════════════════════════════════════════════════════════════
#  ANIMATED FFMPEG PROGRESS (optional)
# ══════════════════════════════════════════════════════════════
async def _ffmpeg_progress_watch(msg, uid: int, duration: float, part: int, total: int) -> None:
    """Animated 'cutting' display while ffmpeg runs (no real feedback — just cosmetic)."""
    frames = SPINNER_WAVE
    tick   = 0
    while True:
        if _get_cancel(uid).is_set():
            return
        spin = frames[tick % len(frames)]
        bar  = _animated_bar(min(99, tick * 3), tick, 14)
        await _safe_edit(msg,
            f"{spin} **Cutting part {part}/{total}…**\n"
            f"`{bar}` processing…\n"
            f"  🔪 ffmpeg at work…"
        )
        tick += 1
        await asyncio.sleep(0.4)


# ══════════════════════════════════════════════════════════════
#  UPLOAD ONE PART — FloodWait safe
# ══════════════════════════════════════════════════════════════
async def _upload_part(
    message, path: str, num: int, total: int,
    uid: int, thumb_time: float
) -> bool:
    if _get_cancel(uid).is_set():
        return False

    # Animated upload header
    status = await message.reply(
        f"⣾ **Upload** — part {num}/{total}\n"
        f"══════════════════════════\n"
        f"`░░░░░░░░░░░░░░░░░░` **0.0%**\n"
        f"  🔵 STARTING — 0 B / ?\n"
        f"══════════════════════════\n"
        f"  ❌ /cancel to abort"
    )
    _reset(uid)
    t0 = time.time()

    thumb_path = f"{THUMB_DIR}/thumb_{uid}_{num}.jpg"
    thumb = await make_thumb(path, thumb_time, thumb_path)

    uploaded = False
    for attempt in range(5):
        if _get_cancel(uid).is_set():
            await _safe_edit(status,
                "🚫 **Upload cancelled.**\n"
                "  Stopped by user request."
            )
            break
        try:
            await message.reply_video(
                path,
                caption=(
                    f"🎬 **Part {num} / {total}**\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"  ✅ Delivered by Ultra Bot v7"
                ),
                thumb=thumb,
                progress=upload_progress,
                progress_args=(status, t0, uid),
            )
            uploaded = True
            break
        except FloodWait as e:
            wait = e.value + 2
            # Countdown display
            for remaining in range(wait, 0, -1):
                spin = SPINNER_CLOCK[remaining % len(SPINNER_CLOCK)]
                await _safe_edit(status,
                    f"{spin} **Flood wait** — part {num}/{total}\n"
                    f"══════════════════════════\n"
                    f"  ⏳ Telegram says: wait `{remaining}s`\n"
                    f"  `{'█' * min(20, int((wait - remaining) / wait * 20))}{'░' * max(0, 20 - int((wait - remaining) / wait * 20))}`\n"
                    f"  Resuming automatically…"
                )
                await asyncio.sleep(1)
        except Exception as e:
            if attempt >= 4:
                await _safe_edit(status, f"❌ Upload failed part {num}: `{e}`")
                break
            await asyncio.sleep(3)

    _reset(uid)
    if thumb and os.path.exists(thumb_path):
        os.remove(thumb_path)
    try:
        await status.delete()
    except Exception:
        pass
    return uploaded


# ══════════════════════════════════════════════════════════════
#  /start — cinematic welcome card
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.command("start"), group=1)
async def start(client, message):
    if await _dedup(message.id): return
    name = message.from_user.first_name or "User"
    await message.reply(
        f"╔══════════════════════════╗\n"
        f"║  ⚡  ULTRA BOT  v7  ⚡   ║\n"
        f"╚══════════════════════════╝\n\n"
        f"👋 Welcome, **{name}**!\n\n"
        f"📽 **Send any video** to begin:\n"
        f"  └─ MP4 · MKV · AVI · MOV · WEBM…\n\n"
        f"✂️ **Split commands:**\n"
        f"  • `/split 3`    → 3 equal parts\n"
        f"  • `/splitmin 2` → 2-min chunks\n\n"
        f"🛠 **Utilities:**\n"
        f"  • `/status`     → live task view\n"
        f"  • `/cancel`     → abort task\n\n"
        f"──────────────────────────\n"
        f"  🎨 Animated progress bars\n"
        f"  📊 Live speed graph\n"
        f"  🔄 Flood-safe · No crash\n"
        f"  👥 Multi-user async\n"
        f"──────────────────────────\n"
        f"  _Ultra Bot v7 — Ready_"
    )


# ══════════════════════════════════════════════════════════════
#  /status — live dashboard
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.command("status"), group=1)
async def status_cmd(client, message):
    if await _dedup(message.id): return
    uid  = message.from_user.id
    info = user_status.get(uid)

    if not _get_lock(uid).locked() or not info:
        if uid in user_files and os.path.exists(user_files[uid]):
            fname = os.path.basename(user_files[uid])
            fsize = _sz(os.path.getsize(user_files[uid]))
            return await message.reply(
                f"💤 **IDLE — Ready to split**\n"
                f"══════════════════════════\n"
                f"  📁 `{fname}`\n"
                f"  📦 {fsize}\n"
                f"══════════════════════════\n"
                f"  👉 `/split N`  or  `/splitmin N`"
            )
        return await message.reply(
            f"💤 **IDLE**\n"
            f"══════════════════════════\n"
            f"  No video loaded.\n"
            f"  Send a video to start!\n"
            f"══════════════════════════"
        )

    elapsed = _eta(time.time() - info["since"])
    hist    = _get_speed_history(uid)
    graph   = _mini_graph(hist, 14) if hist else "no data"
    speed   = _sz(hist[-1]) + "/s" if hist else "—"

    spin = SPINNER_RADAR[int(time.time() * 3) % len(SPINNER_RADAR)]
    await message.reply(
        f"{spin} **RUNNING — Live Status**\n"
        f"══════════════════════════\n"
        f"  📌 **Task**     `{info['task']}`\n"
        f"  📝 **Detail**   `{info['detail']}`\n"
        f"  ⏳ **Running**  `{elapsed}`\n"
        f"──────────────────────────\n"
        f"  📊 `{graph}`\n"
        f"  ⚡ **Speed**    `{speed}`\n"
        f"══════════════════════════\n"
        f"  ❌ /cancel to stop"
    )


# ══════════════════════════════════════════════════════════════
#  /cancel
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.command("cancel"), group=1)
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
@app.on_message(filters.incoming & (filters.video | filters.document), group=-1)
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
        "video/x-matroska": "mkv", "video/mkv":       "mkv",
        "video/avi":        "avi", "video/x-msvideo":  "avi",
        "video/webm":      "webm", "video/quicktime":  "mov",
        "video/x-ms-wmv":  "wmv", "video/3gpp":        "3gp",
    }
    ext    = ext_map.get(mime, "mp4")
    fname  = f"{DOWNLOAD_DIR}/video_{uid}_{message.id}.{ext}"
    sz_str = _sz(file_size) if file_size else "?"

    status = await message.reply(
        f"⣾ **DOWNLOAD STARTING**\n"
        f"══════════════════════════\n"
        f"  📁 Format  : `{ext.upper()}`\n"
        f"  📦 Size    : `{sz_str}`\n"
        f"──────────────────────────\n"
        f"`░░░░░░░░░░░░░░░░░░` **0%**\n"
        f"══════════════════════════\n"
        f"  ❌ /cancel to abort"
    )

    _get_cancel(uid).clear()
    _reset(uid)
    _speed_history.pop(uid, None)
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
            "❌ **File not saved**\n"
            "  Try again or send a different file."
        )
        return

    elapsed = time.time() - t0
    avg_speed = file_size / elapsed if elapsed > 0 and file_size else 0
    _reset(uid)
    _clear_status(uid)
    user_files[uid] = path

    await _safe_edit(status,
        f"✅ **DOWNLOAD COMPLETE**\n"
        f"══════════════════════════\n"
        f"`▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰` **100%**\n"
        f"  🏆 COMPLETE\n"
        f"──────────────────────────\n"
        f"  📁 `{os.path.basename(path)}`\n"
        f"  📦 `{_sz(os.path.getsize(path))}`\n"
        f"  ⚡ avg `{_sz(avg_speed)}/s`\n"
        f"  ⏱ in `{_eta(elapsed)}`\n"
        f"══════════════════════════\n"
        f"  👉 `/split N`  or  `/splitmin N`"
    )


# ══════════════════════════════════════════════════════════════
#  CORE SPLIT LOGIC
# ══════════════════════════════════════════════════════════════
async def _do_split(message, uid: int, seg: float, parts: int, label: str) -> None:
    file   = user_files[uid]
    cancel = _get_cancel(uid)
    cancel.clear()
    _split_tick[uid] = 0

    msg = await message.reply(
        f"✂️ **{label}**\n"
        f"══════════════════════════\n"
        f"`░░░░░░░░░░░░░░░░` **0%**\n"
        f"  Part **0** / **{parts}** done\n"
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
                    f"  Run `/split` again to retry."
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
                    f"❌ **ffmpeg failed** on part {i+1}\n"
                    f"  Check the source file and try again."
                )
                _clear_status(uid)
                return

            await _split_update(msg, i, parts, uid, f"uploading part {i+1}…")
            _set_status(uid, "Uploading", f"part {i+1}/{parts}")
            uploaded = await _upload_part(message, out, i+1, parts, uid, ss + seg/2)
            os.remove(out)

            if not uploaded:
                await _safe_edit(msg,
                    f"🚫 **CANCELLED**\n"
                    f"══════════════════════════\n"
                    f"  Stopped after **{i+1}** / **{parts}** parts."
                )
                _clear_status(uid)
                return

            # Update split bar after successful upload
            await _split_update(msg, i+1, parts, uid,
                                 "done!" if i+1==parts else f"next: part {i+2}…")

        os.remove(file)
        user_files.pop(uid, None)
        _clear_status(uid)

        # Victory card
        duration_str = _eta(seg * parts)
        await _safe_edit(msg,
            f"🏆 **ALL {parts} PARTS DELIVERED!**\n"
            f"══════════════════════════\n"
            f"`▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰` **100%**\n"
            f"  {'✅' * min(parts, 20)}\n"
            f"──────────────────────────\n"
            f"  ✅ **{parts} parts** uploaded\n"
            f"  📽 Duration: `{duration_str}`\n"
            f"══════════════════════════\n"
            f"  _Send another video to split!_"
        )
    except Exception as e:
        _clear_status(uid)
        await _safe_edit(msg,
            f"❌ **Unexpected error**\n"
            f"══════════════════════════\n"
            f"  `{e}`\n"
            f"  Please try again."
        )


# ══════════════════════════════════════════════════════════════
#  /split
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.command("split"), group=1)
async def split(client, message):
    if await _dedup(message.id): return
    uid  = message.from_user.id
    lock = _get_lock(uid)
    if lock.locked():
        return await message.reply(
            "⏳ **Already processing!**\n"
            "  👉 /status · /cancel"
        )
    if uid not in user_files:
        return await message.reply(
            "❌ **No video loaded.**\n"
            "  Send a video file first!"
        )
    try:
        parts = int(message.command[1])
        assert parts >= 2
    except Exception:
        return await message.reply(
            "❌ **Usage:** `/split 3`\n"
            "  Minimum 2 parts."
        )
    dur = await get_duration(user_files[uid])
    if not dur:
        return await message.reply(
            "❌ **Could not read video duration.**\n"
            "  File may be corrupted."
        )
    async with lock:
        await _do_split(
            message, uid, dur / parts, parts,
            f"Splitting into **{parts}** equal parts…"
        )


# ══════════════════════════════════════════════════════════════
#  /splitmin
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.command("splitmin"), group=1)
async def splitmin(client, message):
    if await _dedup(message.id): return
    uid  = message.from_user.id
    lock = _get_lock(uid)
    if lock.locked():
        return await message.reply(
            "⏳ **Already processing!**\n"
            "  👉 /status · /cancel"
        )
    if uid not in user_files:
        return await message.reply(
            "❌ **No video loaded.**\n"
            "  Send a video file first!"
        )
    try:
        mins = int(message.command[1])
        assert mins >= 1
    except Exception:
        return await message.reply(
            "❌ **Usage:** `/splitmin 2`\n"
            "  Chunk size in minutes."
        )
    dur = await get_duration(user_files[uid])
    if not dur:
        return await message.reply(
            "❌ **Could not read video duration.**\n"
            "  File may be corrupted."
        )
    seg   = mins * 60
    parts = math.ceil(dur / seg)
    async with lock:
        await _do_split(
            message, uid, seg, parts,
            f"Splitting — **{mins}** min chunks → **{parts}** parts…"
        )


# ══════════════════════════════════════════════════════════════
app.run()
