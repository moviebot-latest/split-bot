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
    workers=8,
)

DOWNLOAD_DIR = "downloads"
THUMB_DIR    = "thumbs"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(THUMB_DIR,    exist_ok=True)

# ── Per-user state ────────────────────────────────────────────
user_files:  dict[int, str]            = {}  # saved video path
user_locks:  dict[int, asyncio.Lock]   = {}  # one lock per user
user_cancel: dict[int, asyncio.Event]  = {}  # cancel flag per user
user_status: dict[int, dict]           = {}  # current task info

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


# ╔══════════════════════════════════════════════════════════════╗
#  ██  ULTRA PROGRESS ENGINE v5                                 ██
#  ██  0.2s · EMA · count-up · cancel-aware · /status          ██
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
    n = int(pct / 100 * BAR_WIDTH)
    return "[" + "█" * n + "░" * (BAR_WIDTH - n) + "]"

def _badge(pct: float) -> str:
    return ("🏁" if pct>=100 else "🔥" if pct>=80 else
            "⚡" if pct>=60 else "🚀" if pct>=40 else
            "💫" if pct>=20 else "🌀")

def _count_up(uid: int, real: float, step: float = 1.8) -> float:
    prev  = _shown_pct.get(uid, 0.0)
    shown = min(real, prev + step) if real > prev else real
    _shown_pct[uid] = shown
    return shown

async def _safe_edit(message, text: str) -> None:
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

async def progress(
    current: int, total: int,
    message, start: float,
    uid: int = 0, mode: str = "📥 Download",
) -> None:
    if not isinstance(total, (int, float)) or total <= 0:
        return

    # ── Cancel check inside progress ─────────────────────────
    if _get_cancel(uid).is_set():
        return

    now     = time.time()
    elapsed = max(now - start, 0.001)

    if now - _last_edit.get(uid, 0.0) < THROTTLE and _last_edit.get(uid, 0.0) != 0.0:
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
        f"  🚄 **Speed**   : `{_sz(ema)}/s`  {tier}\n"
        f"  ⏱ **ETA**     : `{_eta(eta_sec)}`\n"
        f"  ⏳ **Elapsed** : `{_eta(elapsed)}`\n"
        f"  ❌ /cancel to stop"
    )
    await _safe_edit(message, text)

async def upload_progress(current, total, message, start, uid=0):
    await progress(current, total, message, start, uid=uid, mode="⬆️ Upload")


# ══════════════════════════════════════════════════════════════
#  ASYNC FFMPEG
# ══════════════════════════════════════════════════════════════
async def ffmpeg_cut(input: str, output: str, ss: float, t: float) -> bool:
    proc = await asyncio.create_subprocess_exec(
        "ffmpeg", "-y", "-ss", str(ss), "-i", input,
        "-t", str(t), "-c", "copy", "-avoid_negative_ts", "make_zero", output,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    await proc.wait()
    return proc.returncode == 0


# ══════════════════════════════════════════════════════════════
#  THUMBNAIL GENERATOR
# ══════════════════════════════════════════════════════════════
async def make_thumb(video: str, ss: float, out: str) -> str | None:
    proc = await asyncio.create_subprocess_exec(
        "ffmpeg", "-y", "-ss", str(ss), "-i", video,
        "-vframes", "1", "-q:v", "2", "-vf", "scale=320:-1", out,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    await proc.wait()
    return out if os.path.exists(out) else None


# ══════════════════════════════════════════════════════════════
#  VIDEO DURATION
# ══════════════════════════════════════════════════════════════
async def get_duration(file: str) -> float | None:
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1", file,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        out, _ = await proc.communicate()
        return float(out.decode().strip())
    except Exception:
        return None


# ══════════════════════════════════════════════════════════════
#  SPLIT BAR
# ══════════════════════════════════════════════════════════════
def _sbar(done: int, total: int, w: int = 16) -> str:
    n = int(done / total * w)
    return "[" + "█" * n + "░" * (w - n) + "]"

async def _split_update(msg, done: int, total: int, note: str = "") -> None:
    pct = done * 100 // total
    note_line = f"\n  ┗ _{note}_" if note else ""
    await _safe_edit(
        msg,
        f"✂️ **Splitting…**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{_sbar(done, total)} **{pct}%**\n"
        f"  Part **{done}** / **{total}** done{note_line}\n"
        f"  ❌ /cancel to stop"
    )


# ══════════════════════════════════════════════════════════════
#  UPLOAD ONE PART
# ══════════════════════════════════════════════════════════════
async def _upload_part(
    message, path: str, num: int, total: int,
    uid: int, thumb_time: float
) -> bool:
    """Returns False if cancelled."""
    if _get_cancel(uid).is_set():
        return False

    status = await message.reply(
        f"⬆️ **Upload**  — part {num}/{total}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"[░░░░░░░░░░░░░░░░░░░░]\n"
        f"  🌀 **0%**  ·  starting…"
    )
    _reset(uid)
    t0 = time.time()

    thumb_path = f"{THUMB_DIR}/thumb_{uid}_{num}.jpg"
    thumb = await make_thumb(path, thumb_time, thumb_path)

    for attempt in range(3):
        if _get_cancel(uid).is_set():
            await _safe_edit(status, "🚫 Upload cancelled.")
            return False
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
    return True


# ══════════════════════════════════════════════════════════════
#  /start
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.command("start"))
async def start(client, message):
    await message.reply(
        "⚡ **ULTRA BOT v5** — ready!\n\n"
        "📤 Send any **video**, then:\n"
        "  • `/split 3`     — N equal parts\n"
        "  • `/splitmin 2`  — chunk every N minutes\n\n"
        "🛠 Commands:\n"
        "  • `/status`  — see what's running\n"
        "  • `/cancel`  — stop current task\n\n"
        "✨ Multi-user · Async ffmpeg · Auto thumbnails\n"
        "🔄 Flood retry · 0.2s progress · EMA speed"
    )


# ══════════════════════════════════════════════════════════════
#  /status  — show current task info
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.command("status"), group=0)
async def status_cmd(client, message):
    uid  = message.from_user.id
    lock = _get_lock(uid)
    info = user_status.get(uid)

    if not lock.locked() or not info:
        # Check if video is ready but no task running
        if uid in user_files:
            fname = os.path.basename(user_files[uid])
            fsize = _sz(os.path.getsize(user_files[uid])) if os.path.exists(user_files[uid]) else "?"
            return await message.reply(
                f"💤 **Idle** — no task running\n\n"
                f"📁 Video ready: `{fname}`\n"
                f"📦 Size: `{fsize}`\n\n"
                f"👉 Use `/split N` or `/splitmin N`"
            )
        return await message.reply(
            "💤 **Idle** — no task running\n\n"
            "📤 Send a video to get started."
        )

    elapsed = _eta(time.time() - info["since"])
    await message.reply(
        f"⚙️ **Task running…**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"  📌 **Task**    : `{info['task']}`\n"
        f"  📝 **Detail**  : `{info['detail']}`\n"
        f"  ⏳ **Running** : `{elapsed}`\n\n"
        f"  ❌ Use /cancel to stop"
    )


# ══════════════════════════════════════════════════════════════
#  /cancel  — stop current task cleanly
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.command("cancel"), group=0)
async def cancel_cmd(client, message):
    uid  = message.from_user.id
    lock = _get_lock(uid)

    if not lock.locked():
        return await message.reply("💤 No task running to cancel.")

    # Set the cancel event — task will stop at next checkpoint
    _get_cancel(uid).set()
    await message.reply(
        "🚫 **Cancel requested!**\n\n"
        "Task will stop at the next checkpoint.\n"
        "Cleanup will happen automatically."
    )


# ══════════════════════════════════════════════════════════════
#  RECEIVE VIDEO — catches direct + forwarded + all formats
# ══════════════════════════════════════════════════════════════
_seen_msgs: set[int] = set()  # track message_id to prevent double processing

@app.on_message(filters.incoming & (filters.video | filters.document), group=-1)
async def receive(client, message):
    uid  = message.from_user.id

    # ── Deduplicate by message_id — foolproof double trigger fix
    if message.id in _seen_msgs:
        return
    _seen_msgs.add(message.id)
    # Keep set small — remove old entries after 100
    if len(_seen_msgs) > 100:
        _seen_msgs.clear()

    lock = _get_lock(uid)
    if lock.locked():
        return await message.reply("⏳ Task running.\n👉 /status · /cancel")

    # ── Pick correct media: document over video for MKV/large files
    media = message.document or message.video
    if not media:
        return

    mime      = getattr(media, "mime_type", "") or ""
    file_size = getattr(media, "file_size", 0) or 0

    VIDEO_MIMES = (
        "video/", "application/octet-stream",
    )
    if mime and not any(mime.startswith(v) for v in VIDEO_MIMES):
        return

    # ── Delete old file if exists ─────────────────────────────
    if uid in user_files and os.path.exists(user_files[uid]):
        try:
            os.remove(user_files[uid])
        except Exception:
            pass
        user_files.pop(uid, None)

    ext_map = {
        "video/x-matroska": "mkv", "video/mkv": "mkv",
        "video/avi": "avi",        "video/x-msvideo": "avi",
        "video/webm": "webm",      "video/quicktime": "mov",
        "video/x-ms-wmv": "wmv",   "video/3gpp": "3gp",
    }
    ext   = ext_map.get(mime, "mp4")
    fname = f"{DOWNLOAD_DIR}/video_{uid}_{message.id}.{ext}"
    sz_str = _sz(file_size) if file_size else "?"

    status = await message.reply(
        f"📥 **Downloading…** `{ext.upper()}` · {sz_str}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⏳ Starting…"
    )

    _get_cancel(uid).clear()
    _reset(uid)
    _set_status(uid, "Downloading", sz_str)
    t0 = time.time()

    try:
        path = await message.download(file_name=fname)
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
    actual_size = _sz(os.path.getsize(path))
    elapsed_str = _eta(time.time() - t0)
    await _safe_edit(
        status,
        f"✅ **Download complete!**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"  📁 `{os.path.basename(path)}`\n"
        f"  📦 {actual_size}  ·  ⏱ {elapsed_str}\n\n"
        f"👉 `/split N`  or  `/splitmin N`"
    )


# ══════════════════════════════════════════════════════════════
#  CORE SPLIT LOGIC
# ══════════════════════════════════════════════════════════════
async def _do_split(message, uid: int, seg: float, parts: int, label: str) -> None:
    file   = user_files[uid]
    cancel = _get_cancel(uid)
    cancel.clear()  # reset cancel flag at start

    msg = await message.reply(
        f"✂️ **{label}**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"[░░░░░░░░░░░░░░░░] **0%**\n"
        f"  ❌ /cancel to stop"
    )

    try:
        for i in range(parts):

            # ── Cancel checkpoint ─────────────────────────────
            if cancel.is_set():
                await _safe_edit(msg,
                    f"🚫 **Cancelled!**\n"
                    f"  Stopped after **{i}** / **{parts}** parts.\n"
                    f"  Partial files cleaned up."
                )
                _clear_status(uid)
                return

            ss = i * seg
            _set_status(uid, "Splitting", f"part {i+1}/{parts}")
            await _split_update(msg, i, parts, f"cutting {i+1}/{parts}…")

            out = f"{DOWNLOAD_DIR}/part_{uid}_{i+1}.mp4"
            ok  = await ffmpeg_cut(file, out, ss, seg)

            if not ok or not os.path.exists(out):
                await _safe_edit(msg, f"❌ ffmpeg failed on part {i+1}")
                _clear_status(uid)
                return

            _set_status(uid, "Uploading", f"part {i+1}/{parts}")
            thumb_time = ss + seg / 2
            uploaded = await _upload_part(message, out, i+1, parts, uid, thumb_time)
            os.remove(out)

            if not uploaded:
                # Was cancelled during upload
                await _safe_edit(msg,
                    f"🚫 **Cancelled!**\n"
                    f"  Stopped after **{i+1}** / **{parts}** parts."
                )
                _clear_status(uid)
                return

        # ── All done ──────────────────────────────────────────
        os.remove(file)
        user_files.pop(uid, None)
        _clear_status(uid)
        await _safe_edit(
            msg,
            f"🏁 **All {parts} parts done!**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"[████████████████] **100%**\n"
            f"  ✅ Complete"
        )

    except Exception as e:
        _clear_status(uid)
        await _safe_edit(msg, f"❌ Error: `{e}`")


# ══════════════════════════════════════════════════════════════
#  /split
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.command("split"))
async def split(client, message):
    uid  = message.from_user.id
    lock = _get_lock(uid)

    if lock.locked():
        return await message.reply("⏳ Already processing.\n👉 /status to check · /cancel to stop")
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
        return await message.reply("⏳ Already processing.\n👉 /status to check · /cancel to stop")
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
