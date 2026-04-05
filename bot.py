import os
import time
import math
import asyncio
import logging
import traceback
from pyrogram import Client, filters
from pyrogram.errors import FloodWait, MessageNotModified

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("UltraBot")

API_ID    = int(os.getenv("API_ID"))
API_HASH  = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")

app = Client(
    "ultra-bot",
    api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN,
    in_memory=True,
    sleep_threshold=300,
    ipv6=False,
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
#  PROGRESS ENGINE
# ══════════════════════════════════════════════════════════════
THROTTLE  = 0.2
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
    for u in ("B","KB","MB","GB"):
        if b < 1024: return f"{b:.1f} {u}"
        b /= 1024
    return f"{b:.1f} TB"

def _eta(s: float) -> str:
    if s >= 3600: return f"{int(s//3600)}h {int(s%3600//60)}m"
    if s >= 60:   return f"{int(s//60)}m {int(s%60)}s"
    return f"{int(s)}s"

def _bar(pct: float, w: int = 20) -> str:
    n = int(pct / 100 * w)
    return "[" + "█" * n + "░" * (w - n) + "]"

def _badge(pct: float) -> str:
    return ("🏁" if pct>=100 else "🔥" if pct>=80 else
            "⚡" if pct>=60 else "🚀" if pct>=40 else
            "💫" if pct>=20 else "🌀")

def _count_up(uid: int, real: float, step: float = 1.8) -> float:
    prev  = _shown_pct.get(uid, 0.0)
    shown = min(real, prev + step) if real > prev else real
    _shown_pct[uid] = shown
    return shown

async def _safe_edit(msg, text: str) -> None:
    try:
        await msg.edit(text)
    except FloodWait as e:
        await asyncio.sleep(e.value + 0.5)
        try: await msg.edit(text)
        except: pass
    except MessageNotModified:
        pass
    except Exception:
        pass

async def progress(current, total, msg, start, uid=0, mode="📥 Download"):
    if not isinstance(total, (int, float)) or total <= 0: return
    if _get_cancel(uid).is_set(): return
    now     = time.time()
    elapsed = max(now - start, 0.001)
    if now - _last_edit.get(uid, 0.0) < THROTTLE and _last_edit.get(uid, 0.0) != 0.0:
        return
    _last_edit[uid] = now
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
        f"  {_badge(shown)} **{shown:.1f}%**  ·  {_sz(current)} / {_sz(total)}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"  🚄 **Speed**   : `{_sz(ema)}/s`  {tier}\n"
        f"  ⏱ **ETA**     : `{_eta(eta_s)}`\n"
        f"  ⏳ **Elapsed** : `{_eta(elapsed)}`\n"
        f"  ❌ /cancel to stop"
    )

async def upload_progress(current, total, msg, start, uid=0):
    await progress(current, total, msg, start, uid=uid, mode="⬆️ Upload")


# ══════════════════════════════════════════════════════════════
#  FFMPEG HELPERS
# ══════════════════════════════════════════════════════════════
async def ffmpeg_cut(inp, out, ss, t) -> bool:
    proc = await asyncio.create_subprocess_exec(
        "ffmpeg", "-y", "-ss", str(ss), "-i", inp,
        "-t", str(t), "-c", "copy", "-avoid_negative_ts", "make_zero", out,
        stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
    )
    await proc.wait()
    return proc.returncode == 0

async def make_thumb(video, ss, out):
    proc = await asyncio.create_subprocess_exec(
        "ffmpeg", "-y", "-ss", str(ss), "-i", video,
        "-vframes", "1", "-q:v", "2", "-vf", "scale=320:-1", out,
        stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
    )
    await proc.wait()
    return out if os.path.exists(out) else None

async def get_duration(file) -> float | None:
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1", file,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
        )
        out, _ = await proc.communicate()
        return float(out.decode().strip())
    except:
        return None


# ══════════════════════════════════════════════════════════════
#  SPLIT UI + UPLOAD PART
# ══════════════════════════════════════════════════════════════
async def _split_update(msg, done, total, note=""):
    pct = done * 100 // total
    note_line = f"\n  ┗ _{note}_" if note else ""
    await _safe_edit(msg,
        f"✂️ **Splitting…**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{_bar(pct, 16)} **{pct}%**\n"
        f"  Part **{done}** / **{total}** done{note_line}\n"
        f"  ❌ /cancel to stop"
    )

async def _upload_part(message, path, num, total, uid, thumb_time) -> bool:
    if _get_cancel(uid).is_set(): return False
    status = await message.reply(
        f"⬆️ **Upload** — part {num}/{total}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"[░░░░░░░░░░░░░░░░░░░░]\n"
        f"  🌀 **0%**  ·  starting…"
    )
    _reset(uid)
    t0 = time.time()
    thumb_path = f"{THUMB_DIR}/thumb_{uid}_{num}.jpg"
    thumb = await make_thumb(path, thumb_time, thumb_path)
    uploaded = False
    for attempt in range(5):
        if _get_cancel(uid).is_set():
            await _safe_edit(status, "🚫 Upload cancelled.")
            break
        try:
            await message.reply_video(
                path,
                caption=f"🎬 **Part {num} / {total}**",
                thumb=thumb,
                progress=upload_progress,
                progress_args=(status, t0, uid),
            )
            uploaded = True
            break
        except FloodWait as e:
            wait = e.value + 2
            await _safe_edit(status,
                f"⏳ **Flood wait** — part {num}/{total}\n"
                f"  Resuming in `{wait}s`…"
            )
            await asyncio.sleep(wait)
        except Exception as e:
            if attempt >= 4:
                await _safe_edit(status, f"❌ Upload failed part {num}: `{e}`")
                break
            await asyncio.sleep(3)
    _reset(uid)
    if thumb and os.path.exists(thumb_path):
        try: os.remove(thumb_path)
        except: pass
    try: await status.delete()
    except: pass
    return uploaded


# ══════════════════════════════════════════════════════════════
#  CORE SPLIT ENGINE
# ══════════════════════════════════════════════════════════════
async def _do_split(message, uid, seg, parts, label):
    file   = user_files[uid]
    cancel = _get_cancel(uid)
    cancel.clear()
    msg = await message.reply(
        f"✂️ **{label}**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{_bar(0, 16)} **0%**\n"
        f"  ❌ /cancel to stop"
    )
    try:
        for i in range(parts):
            if cancel.is_set():
                await _safe_edit(msg, f"🚫 **Cancelled!**\n  Stopped after **{i}** / **{parts}** parts.")
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
            uploaded = await _upload_part(message, out, i+1, parts, uid, ss + seg/2)
            try:
                if os.path.exists(out): os.remove(out)
            except: pass
            if not uploaded:
                await _safe_edit(msg, f"🚫 **Cancelled!**\n  Stopped after **{i+1}** / **{parts}** parts.")
                _clear_status(uid)
                return
        try:
            if os.path.exists(file): os.remove(file)
        except: pass
        user_files.pop(uid, None)
        _clear_status(uid)
        await _safe_edit(msg,
            f"🏁 **All {parts} parts done!**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{_bar(100, 16)} **100%**\n"
            f"  ✅ Complete — send next video!"
        )
        log.info(f"Split done uid={uid} parts={parts}")
    except Exception as e:
        log.error(f"_do_split error: {traceback.format_exc()}")
        _clear_status(uid)
        await _safe_edit(msg, f"❌ Error: `{e}`")


# ══════════════════════════════════════════════════════════════
#  COMMANDS
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.command("start"), group=1)
async def cmd_start(client, message):
    if await _dedup(message.id): return
    name = getattr(message.from_user, "first_name", "User") or "User"
    await message.reply(
        f"⚡ **ULTRA BOT** — ready!\n\n"
        f"👋 Hey **{name}**!\n\n"
        f"📤 Send any **video**, then:\n"
        f"  • `/split 3`       — N equal parts\n"
        f"  • `/splitmin 2`    — chunk every N minutes\n"
        f"  • `/splitsize 500` — chunk every N MB\n\n"
        f"🛠 Other commands:\n"
        f"  • `/info`    — video details\n"
        f"  • `/status`  — current task\n"
        f"  • `/cancel`  — stop task\n"
        f"  • `/help`    — full help\n\n"
        f"✨ Multi-user · Async ffmpeg · Auto thumbnails\n"
        f"🔄 FloodWait safe · No crash · No double upload"
    )


@app.on_message(filters.command("help"), group=1)
async def cmd_help(client, message):
    if await _dedup(message.id): return
    await message.reply(
        f"📖 **ULTRA BOT — Help**\n\n"
        f"**Step 1:** Koi bhi video send karo\n"
        f"  _(MP4, MKV, AVI, MOV, WEBM)_\n\n"
        f"**Step 2:** Split command do:\n\n"
        f"  `/split N`\n"
        f"  → N equal parts\n"
        f"  → Example: `/split 3`\n\n"
        f"  `/splitmin N`\n"
        f"  → N minute ke chunks\n"
        f"  → Example: `/splitmin 5`\n\n"
        f"  `/splitsize N`\n"
        f"  → N MB ke chunks\n"
        f"  → Example: `/splitsize 500`\n\n"
        f"  `/info` → loaded video ki details\n"
        f"  `/status` → kya chal raha hai\n"
        f"  `/cancel` → rok do\n"
    )


@app.on_message(filters.command("info"), group=1)
async def cmd_info(client, message):
    if await _dedup(message.id): return
    uid = message.from_user.id
    if uid not in user_files or not os.path.exists(user_files[uid]):
        return await message.reply("❌ Koi video load nahi hai.\nPehle video bhejo!")
    p  = user_files[uid]
    d  = await get_duration(p)
    sz = os.path.getsize(p)
    opts = ""
    if d:
        for n in [2, 3, 4, 5]:
            opts += f"  `/split {n}` → {n}×{_eta(d/n)}\n"
    await message.reply(
        f"📋 **File Info**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"  📁 `{os.path.basename(p)}`\n"
        f"  📦 `{_sz(sz)}`\n"
        f"  🎬 `{_eta(d) if d else 'unknown'}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"  💡 Split options:\n{opts}"
        f"  👉 `/split N` · `/splitmin N` · `/splitsize N`"
    )


@app.on_message(filters.command("status"), group=1)
async def cmd_status(client, message):
    if await _dedup(message.id): return
    uid  = message.from_user.id
    info = user_status.get(uid)
    if not _get_lock(uid).locked() or not info:
        if uid in user_files and os.path.exists(user_files[uid]):
            p = user_files[uid]
            return await message.reply(
                f"💤 **Idle — Ready**\n"
                f"  📁 `{os.path.basename(p)}`\n"
                f"  📦 `{_sz(os.path.getsize(p))}`\n"
                f"  👉 `/split N` · `/splitmin N`"
            )
        return await message.reply("💤 **Idle** — send a video!")
    elapsed = _eta(time.time() - info["since"])
    await message.reply(
        f"⚙️ **Running…**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"  📌 **Task**   : `{info['task']}`\n"
        f"  📝 **Detail** : `{info['detail']}`\n"
        f"  ⏳ **Time**   : `{elapsed}`\n\n"
        f"  ❌ /cancel to stop"
    )


@app.on_message(filters.command("cancel"), group=1)
async def cmd_cancel(client, message):
    if await _dedup(message.id): return
    uid = message.from_user.id
    if not _get_lock(uid).locked():
        return await message.reply("💤 Koi task nahi chal raha.")
    _get_cancel(uid).set()
    await message.reply("🚫 **Cancel requested!**\nStopping at next checkpoint…")


# ══════════════════════════════════════════════════════════════
#  RECEIVE VIDEO
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.incoming & (filters.video | filters.document), group=-1)
async def receive(client, message):
    if await _dedup(message.id): return
    if not message.from_user: return

    uid  = message.from_user.id
    lock = _get_lock(uid)
    if lock.locked():
        return await message.reply("⏳ Task running.\n👉 /status · /cancel")

    media = message.document or message.video
    if not media: return

    mime      = getattr(media, "mime_type", "") or ""
    file_size = getattr(media, "file_size", 0) or 0

    if mime and not (mime.startswith("video/") or mime == "application/octet-stream"):
        return

    if uid in user_files and os.path.exists(user_files[uid]):
        try: os.remove(user_files[uid])
        except: pass
        user_files.pop(uid, None)

    ext_map = {
        "video/x-matroska":"mkv","video/mkv":"mkv",
        "video/avi":"avi","video/x-msvideo":"avi",
        "video/webm":"webm","video/quicktime":"mov",
        "video/x-ms-wmv":"wmv","video/3gpp":"3gp",
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
        path = await message.download(
            file_name=fname,
            progress=progress,
            progress_args=(status, t0, uid, "📥 Download"),
        )
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
    await _safe_edit(status,
        f"✅ **Download complete!**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"  📁 `{os.path.basename(path)}`\n"
        f"  📦 {_sz(os.path.getsize(path))}  ·  ⏱ {_eta(time.time()-t0)}\n\n"
        f"👉 `/split N`  ·  `/splitmin N`  ·  `/splitsize N`"
    )


# ══════════════════════════════════════════════════════════════
#  SPLIT COMMANDS
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.command("split"), group=1)
async def cmd_split(client, message):
    if await _dedup(message.id): return
    uid  = message.from_user.id
    lock = _get_lock(uid)
    if lock.locked():
        return await message.reply("⏳ Already processing.\n👉 /status · /cancel")
    if uid not in user_files:
        return await message.reply("❌ Pehle video bhejo!")
    try:
        parts = int(message.command[1]); assert 2 <= parts <= 100
    except:
        return await message.reply("❌ Usage: `/split 3`\n  Min 2, max 100.")
    dur = await get_duration(user_files[uid])
    if not dur:
        return await message.reply("❌ Video duration nahi mila.")
    async with lock:
        await _do_split(message, uid, dur/parts, parts, f"Splitting into {parts} equal parts…")


@app.on_message(filters.command("splitmin"), group=1)
async def cmd_splitmin(client, message):
    if await _dedup(message.id): return
    uid  = message.from_user.id
    lock = _get_lock(uid)
    if lock.locked():
        return await message.reply("⏳ Already processing.\n👉 /status · /cancel")
    if uid not in user_files:
        return await message.reply("❌ Pehle video bhejo!")
    try:
        mins = int(message.command[1]); assert 1 <= mins <= 120
    except:
        return await message.reply("❌ Usage: `/splitmin 2`\n  Minutes 1-120.")
    dur = await get_duration(user_files[uid])
    if not dur:
        return await message.reply("❌ Video duration nahi mila.")
    seg   = mins * 60
    parts = math.ceil(dur / seg)
    if parts > 100:
        return await message.reply(f"❌ Too many parts ({parts}). Bada chunk lo.")
    async with lock:
        await _do_split(message, uid, seg, parts, f"{mins} min chunks → {parts} parts…")


@app.on_message(filters.command("splitsize"), group=1)
async def cmd_splitsize(client, message):
    if await _dedup(message.id): return
    uid  = message.from_user.id
    lock = _get_lock(uid)
    if lock.locked():
        return await message.reply("⏳ Already processing.\n👉 /status · /cancel")
    if uid not in user_files:
        return await message.reply("❌ Pehle video bhejo!")
    try:
        mb = int(message.command[1]); assert 10 <= mb <= 2000
    except:
        return await message.reply("❌ Usage: `/splitsize 500`\n  MB 10-2000.")
    file     = user_files[uid]
    total_mb = os.path.getsize(file) / 1048576
    dur      = await get_duration(file)
    if not dur:
        return await message.reply("❌ Video duration nahi mila.")
    parts = math.ceil(total_mb / mb)
    if parts > 100:
        return await message.reply(f"❌ Too many parts ({parts}).")
    if parts < 2:
        return await message.reply(f"❌ File already ≤{mb}MB. Split ki zaroorat nahi.")
    seg = dur / parts
    async with lock:
        await _do_split(message, uid, seg, parts, f"{mb}MB chunks → {parts} parts…")


# ══════════════════════════════════════════════════════════════
app.run()
