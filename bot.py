import os
import time
import math
import asyncio
import subprocess
from pyrogram import Client, filters
from pyrogram.errors import FloodWait, MessageNotModified

API_ID    = int(os.getenv("API_ID"))
API_HASH  = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")

app = Client(
    "ultra-bot",
    api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN,
    # allow more concurrent workers for multi-user
    workers=8,
)

DOWNLOAD_DIR = "downloads"
THUMB_DIR    = "thumbs"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(THUMB_DIR,    exist_ok=True)

# per-user: stores video path
user_files: dict[int, str] = {}
# per-user: asyncio.Lock — true multi-user, each user has own lock
user_locks: dict[int, asyncio.Lock] = {}

def _get_lock(uid: int) -> asyncio.Lock:
    if uid not in user_locks:
        user_locks[uid] = asyncio.Lock()
    return user_locks[uid]


# ╔══════════════════════════════════════════════════════════════╗
#  ██  ULTRA PROGRESS ENGINE v4                                 ██
#  ██  0.2s · EMA · count-up · flood-safe · multi-user         ██
# ╚══════════════════════════════════════════════════════════════╝

THROTTLE  = 0.2
EMA_ALPHA = 0.35
BAR_WIDTH = 20
SPINNER   = ["⠋","⠙","⠹","⠸","⠼","⠴","⠦","⠧","⠇","⠏"]

_last_edit: dict[int, float] = {}
_ema_speed: dict[int, float] = {}
_spin_idx:  dict[int, int]   = {}
_shown_pct: dict[int, float] = {}

def _reset(uid: int) -> None:
    for d in (_last_edit, _ema_speed, _spin_idx, _shown_pct):
        d.pop(uid, None)

def _sz(b: float) -> str:
    for u in ("B","KB","MB","GB"):
        if b < 1024: return f"{b:.1f} {u}"
        b /= 1024
    return f"{b:.1f} TB"

def _eta(s: float) -> str:
    if s >= 3600: return f"{int(s//3600)}h {int(s%3600//60)}m"
    if s >= 60:   return f"{int(s//60)}m {int(s%60)}s"
    return f"{int(s)}s"

def _bar(pct: float) -> str:
    # ▰▱ are clearly visible on both dark & light Telegram themes
    n = int(pct / 100 * BAR_WIDTH)
    e = BAR_WIDTH - n - (1 if n < BAR_WIDTH else 0)
    return "▰" * n + ("◈" if n < BAR_WIDTH else "") + "▱" * e

def _badge(pct: float) -> str:
    return ("🏁" if pct>=100 else "🔥" if pct>=80 else
            "⚡" if pct>=60 else "🚀" if pct>=40 else
            "💫" if pct>=20 else "🌀")

def _count_up(uid: int, real: float, step: float = 1.8) -> float:
    prev  = _shown_pct.get(uid, 0.0)
    shown = min(real, prev + step) if real > prev else real
    _shown_pct[uid] = shown
    return shown


# ── Flood-safe edit ───────────────────────────────────────────
async def _safe_edit(message, text: str) -> None:
    """Edit message; on FloodWait sleep & retry once; ignore no-change errors."""
    try:
        await message.edit(text)
    except FloodWait as e:
        await asyncio.sleep(e.value + 0.5)
        try: await message.edit(text)
        except Exception: pass
    except MessageNotModified:
        pass
    except Exception:
        pass


# ── Main progress callback ────────────────────────────────────
async def progress(
    current: int, total: int,
    message, start: float,
    uid: int = 0, mode: str = "⬇️ Download",
) -> None:
    if not isinstance(total, (int, float)) or total <= 0:
        return
    now     = time.time()
    elapsed = max(now - start, 0.001)

    if now - _last_edit.get(uid, 0.0) < THROTTLE:
        return
    _last_edit[uid] = now

    raw = current / elapsed
    ema = EMA_ALPHA * raw + (1 - EMA_ALPHA) * _ema_speed.get(uid, raw)
    _ema_speed[uid] = ema
    eta_sec = (total - current) / ema if ema > 0 else 0

    real  = current * 100 / total
    shown = _count_up(uid, real)

    i    = _spin_idx.get(uid, 0)
    spin = SPINNER[i % len(SPINNER)]
    _spin_idx[uid] = i + 1

    kb   = ema / 1024
    tier = "🟢 Fast" if kb >= 1024 else ("🟡 Good" if kb >= 256 else "🔴 Slow")

    text = (
        f"{spin} **{mode}**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{_bar(shown)}\n"
        f"  {_badge(shown)} **{shown:.1f}%**  ·  {_sz(current)} / {_sz(total)}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"  🚄 **Speed**  : `{_sz(ema)}/s`  {tier}\n"
        f"  ⏱ **ETA**    : `{_eta(eta_sec)}`\n"
        f"  ⏳ **Elapsed** : `{_eta(elapsed)}`"
    )
    await _safe_edit(message, text)

async def upload_progress(current, total, message, start, uid=0):
    await progress(current, total, message, start, uid=uid, mode="⬆️ Upload")


# ══════════════════════════════════════════════════════════════
#  ASYNC FFMPEG  — non-blocking, bot never freezes
# ══════════════════════════════════════════════════════════════
async def ffmpeg_cut(input: str, output: str, ss: float, t: float) -> bool:
    """Run ffmpeg asynchronously. Returns True on success."""
    proc = await asyncio.create_subprocess_exec(
        "ffmpeg", "-y",
        "-ss",  str(ss),
        "-i",   input,
        "-t",   str(t),
        "-c",   "copy",
        "-avoid_negative_ts", "make_zero",
        output,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    await proc.wait()
    return proc.returncode == 0


# ══════════════════════════════════════════════════════════════
#  THUMBNAIL GENERATOR  — extract frame at midpoint of part
# ══════════════════════════════════════════════════════════════
async def make_thumb(video: str, ss: float, out: str) -> str | None:
    """Extract one JPEG frame at timestamp `ss`. Returns path or None."""
    proc = await asyncio.create_subprocess_exec(
        "ffmpeg", "-y",
        "-ss",    str(ss),
        "-i",     video,
        "-vframes","1",
        "-q:v",   "2",
        "-vf",    "scale=320:-1",
        out,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    await proc.wait()
    return out if os.path.exists(out) else None


# ══════════════════════════════════════════════════════════════
#  SPLIT PROGRESS BAR
# ══════════════════════════════════════════════════════════════
def _sbar(done: int, total: int, w: int = 16) -> str:
    f = int(done / total * w)
    return "▰" * f + "▱" * (w - f)

async def _split_update(msg, done: int, total: int, note: str = "") -> None:
    pct  = done * 100 // total
    note_line = f"\n  ┗ _{note}_" if note else ""
    await _safe_edit(
        msg,
        f"✂️ **Splitting…**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{_sbar(done, total)} **{pct}%**\n"
        f"  Part **{done}** / **{total}** done{note_line}"
    )


# ══════════════════════════════════════════════════════════════
#  VIDEO DURATION
# ══════════════════════════════════════════════════════════════
async def get_duration(file: str) -> float | None:
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe","-v","error",
            "-show_entries","format=duration",
            "-of","default=noprint_wrappers=1:nokey=1",
            file,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        out, _ = await proc.communicate()
        return float(out.decode().strip())
    except Exception:
        return None


# ══════════════════════════════════════════════════════════════
#  UPLOAD ONE PART  (with thumb + flood-safe retry)
# ══════════════════════════════════════════════════════════════
async def _upload_part(
    message, path: str, num: int, total: int,
    uid: int, thumb_time: float
) -> None:
    status = await message.reply(
        f"⠋ **⬆️ Upload**  — part {num}/{total}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"▱▱▱▱▱▱▱▱▱▱▱▱▱▱▱▱▱▱▱▱\n"
        f"  🌀 **0.0%**  ·  starting…"
    )
    _reset(uid)
    t0 = time.time()

    # Generate thumbnail at midpoint of this part
    thumb_path = f"{THUMB_DIR}/thumb_{uid}_{num}.jpg"
    thumb = await make_thumb(path, thumb_time, thumb_path)

    # Upload with flood-safe retry
    for attempt in range(3):
        try:
            await message.reply_video(
                path,
                caption=f"🎬 **Part {num} / {total}**",
                thumb=thumb,
                progress=upload_progress,
                progress_args=(status, t0, uid),
            )
            break
        except FloodWait as e:
            await asyncio.sleep(e.value + 1)
        except Exception as e:
            if attempt == 2:
                await message.reply(f"❌ Upload failed part {num}: `{e}`")

    _reset(uid)
    if thumb and os.path.exists(thumb_path):
        os.remove(thumb_path)
    try:
        await status.delete()
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════
#  /start
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.command("start"))
async def start(client, message):
    await message.reply(
        "⚡ **ULTRA BOT v4** — ready!\n\n"
        "📤 Send any **video**, then:\n"
        "  • `/split 3`     — N equal parts\n"
        "  • `/splitmin 2`  — chunk every N minutes\n\n"
        "✨ Multi-user · Async ffmpeg · Auto thumbnails\n"
        "🔄 Flood retry · 0.2s progress · EMA speed"
    )


# ══════════════════════════════════════════════════════════════
#  RECEIVE VIDEO  — multi-user, each user independent
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.video | filters.document)
async def receive(client, message):
    uid  = message.from_user.id
    lock = _get_lock(uid)

    if lock.locked():
        return await message.reply("⏳ Your previous task is still running.")

    status = await message.reply(
        "⠋ **⬇️ Download**\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "▱▱▱▱▱▱▱▱▱▱▱▱▱▱▱▱▱▱▱▱\n"
        "  🌀 **0.0%**  ·  starting…"
    )
    _reset(uid)
    t0 = time.time()

    path = await message.download(
        file_name=f"{DOWNLOAD_DIR}/video_{uid}_{message.id}.mp4",
        progress=progress,
        progress_args=(status, t0, uid, "⬇️ Download"),
    )

    _reset(uid)
    user_files[uid] = path
    await _safe_edit(status, "✅ **Download complete!**\n\n👉 `/split N`  or  `/splitmin N`")


# ══════════════════════════════════════════════════════════════
#  CORE SPLIT LOGIC  (shared by /split and /splitmin)
# ══════════════════════════════════════════════════════════════
async def _do_split(message, uid: int, seg: float, parts: int, label: str) -> None:
    file = user_files[uid]
    msg  = await message.reply(
        f"✂️ **{label}**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"▱▱▱▱▱▱▱▱▱▱▱▱▱▱▱▱ **0%**"
    )

    try:
        for i in range(parts):
            ss  = i * seg
            await _split_update(msg, i, parts, f"cutting {i+1}/{parts}…")

            out   = f"{DOWNLOAD_DIR}/part_{uid}_{i+1}.mp4"
            ok    = await ffmpeg_cut(file, out, ss, seg)

            if not ok or not os.path.exists(out):
                await _safe_edit(msg, f"❌ ffmpeg failed on part {i+1}")
                return

            # Thumbnail at midpoint of this part
            thumb_time = ss + seg / 2

            await _upload_part(message, out, i+1, parts, uid, thumb_time)
            os.remove(out)

        os.remove(file)
        user_files.pop(uid, None)
        await _safe_edit(
            msg,
            f"🏁 **All {parts} parts done!**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰ **100%**\n  ✅ Complete"
        )

    except Exception as e:
        await _safe_edit(msg, f"❌ Error: `{e}`")


# ══════════════════════════════════════════════════════════════
#  /split
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.command("split"))
async def split(client, message):
    uid  = message.from_user.id
    lock = _get_lock(uid)

    if lock.locked():
        return await message.reply("⏳ Already processing.")
    if uid not in user_files:
        return await message.reply("❌ Send a video first.")

    try:
        parts = int(message.command[1]); assert parts >= 2
    except Exception:
        return await message.reply("❌ Usage: `/split 3`")

    dur = await get_duration(user_files[uid])
    if not dur:
        return await message.reply("❌ Could not read video duration.")

    async with lock:
        await _do_split(message, uid, dur / parts, parts,
                        f"Splitting into {parts} equal parts…")


# ══════════════════════════════════════════════════════════════
#  /splitmin
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.command("splitmin"))
async def splitmin(client, message):
    uid  = message.from_user.id
    lock = _get_lock(uid)

    if lock.locked():
        return await message.reply("⏳ Already processing.")
    if uid not in user_files:
        return await message.reply("❌ Send a video first.")

    try:
        mins = int(message.command[1]); assert mins >= 1
    except Exception:
        return await message.reply("❌ Usage: `/splitmin 2`")

    dur = await get_duration(user_files[uid])
    if not dur:
        return await message.reply("❌ Could not read video duration.")

    seg   = mins * 60
    parts = math.ceil(dur / seg)

    async with lock:
        await _do_split(message, uid, seg, parts,
                        f"Splitting — {mins} min chunks ({parts} parts)…")


# ══════════════════════════════════════════════════════════════
app.run()
