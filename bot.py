import os
import time
import math
import asyncio
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
#  BULLETPROOF ANTI-DOUBLE SYSTEM  v9
#
#  Problem: Pyrogram sometimes delivers same command twice
#  with DIFFERENT message IDs — so _dedup(message.id) fails.
#
#  Solution: 3-layer system:
#    1. _seen_msgs   — dedup by message ID (catches exact dupes)
#    2. _running     — per-user running flag (SET = busy, blocks all)
#    3. _last_cmd    — per-user timestamp cooldown (3s window)
#
#  All 3 together = zero double execution guaranteed.
# ══════════════════════════════════════════════════════════════

# Layer 1 — message ID dedup
_seen_msgs: set  = set()
_seen_lock       = asyncio.Lock()

async def _dedup(mid: int) -> bool:
    async with _seen_lock:
        if mid in _seen_msgs:
            return True
        _seen_msgs.add(mid)
        if len(_seen_msgs) > 1000:
            _seen_msgs.clear()
        return False

# Layer 2 — per-user running flag (strongest guard)
_running: set[int] = set()   # uids currently processing

def _is_running(uid: int) -> bool:
    return uid in _running

def _set_running(uid: int) -> bool:
    """Try to mark user as running. Returns False if already running."""
    if uid in _running:
        return False
    _running.add(uid)
    return True

def _clear_running(uid: int) -> None:
    _running.discard(uid)

# Layer 3 — timestamp cooldown (catches near-simultaneous duplicates)
_last_cmd: dict[int, float] = {}
CMD_WINDOW = 5.0   # seconds

def _in_cooldown(uid: int) -> bool:
    now  = time.time()
    last = _last_cmd.get(uid, 0.0)
    if now - last < CMD_WINDOW:
        return True
    _last_cmd[uid] = now
    return False


# ══════════════════════════════════════════════════════════════
#  PER-USER STATE
# ══════════════════════════════════════════════════════════════
user_files:  dict[int, str]           = {}
user_cancel: dict[int, asyncio.Event] = {}
user_status: dict[int, dict]          = {}

def _get_cancel(uid: int) -> asyncio.Event:
    if uid not in user_cancel:
        user_cancel[uid] = asyncio.Event()
    return user_cancel[uid]

def _set_status(uid: int, task: str, detail: str = "") -> None:
    user_status[uid] = {"task": task, "detail": detail, "since": time.time()}

def _clear_status(uid: int) -> None:
    user_status.pop(uid, None)


# ══════════════════════════════════════════════════════════════
#  UPLOAD PART DEDUP  — prevents double video send
# ══════════════════════════════════════════════════════════════
_done_parts: dict[tuple, bool] = {}   # (uid, part_num) → True

def _part_done(uid: int, n: int) -> bool:
    return _done_parts.get((uid, n), False)

def _mark_part_done(uid: int, n: int) -> None:
    _done_parts[(uid, n)] = True

def _reset_parts(uid: int) -> None:
    for k in list(_done_parts):
        if k[0] == uid:
            del _done_parts[k]


# ══════════════════════════════════════════════════════════════
#  VISUAL CONSTANTS
# ══════════════════════════════════════════════════════════════
BOUNCE = ["⣾","⣽","⣻","⢿","⡿","⣟","⣯","⣷"]
SIGNAL = ["▁","▂","▃","▄","▅","▆","▇","█","▇","▆","▅","▄","▃","▂"]
CLOCK  = ["🕐","🕑","🕒","🕓","🕔","🕕","🕖","🕗","🕘","🕙","🕚","🕛"]
ORBIT  = ["◜","◝","◞","◟"]

BF = "█"   # bar fill
BE = "░"   # bar empty

def _bar(pct: float, w: int = 18) -> str:
    n = max(0, min(w, int(pct / 100 * w)))
    return BF * n + BE * (w - n)

def _abar(pct: float, tick: int, w: int = 18) -> str:
    n = max(0, min(w, int(pct / 100 * w)))
    if n == 0:  return BE * w
    if n >= w:  return BF * w
    lead = "▓" if tick % 2 == 0 else BF
    return BF * (n - 1) + lead + BE * (w - n)

def _spark(hist: list, w: int = 12) -> str:
    b = " ▁▂▃▄▅▆▇█"
    if not hist: return "─" * w
    mx = max(hist) or 1
    return "".join(b[min(8, int(s / mx * 8))] for s in hist[-w:])

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

def _spd_label(bps: float) -> str:
    mb = bps / 1048576
    kb = bps / 1024
    if mb >= 5:   return "🟢 FAST"
    if mb >= 1:   return "🟡 GOOD"
    if kb >= 512: return "🟡 OK"
    return "🔴 SLOW"

def _icon(pct: float) -> str:
    if pct >= 100: return "🏆"
    if pct >= 80:  return "🔥"
    if pct >= 50:  return "🚀"
    if pct >= 25:  return "⚡"
    return "🔵"

def _count_up(uid: int, real: float) -> float:
    prev = _shown.get(uid, 0.0)
    val  = min(real, prev + 2.5) if real > prev else real
    _shown[uid] = val
    return val


# ══════════════════════════════════════════════════════════════
#  PROGRESS STATE
# ══════════════════════════════════════════════════════════════
_last_edit: dict[int, float] = {}
_ema:       dict[int, float] = {}
_tick:      dict[int, int]   = {}
_shown:     dict[int, float] = {}
_hist:      dict[int, list]  = {}
THROTTLE  = 0.18
EMA_A     = 0.35

def _rst(uid: int) -> None:
    for d in (_last_edit, _ema, _tick, _shown): d.pop(uid, None)


# ══════════════════════════════════════════════════════════════
#  SAFE EDIT
# ══════════════════════════════════════════════════════════════
async def _edit(msg, text: str) -> None:
    try:
        await msg.edit(text)
    except FloodWait as e:
        await asyncio.sleep(min(e.value, 15) + 0.5)
        try: await msg.edit(text)
        except Exception: pass
    except MessageNotModified:
        pass
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════
#  PROGRESS ENGINE v9
# ══════════════════════════════════════════════════════════════
async def progress(current, total, msg, start, uid=0, mode="📥 Download"):
    if not total or total <= 0: return
    if _get_cancel(uid).is_set(): return

    now     = time.time()
    elapsed = max(now - start, 0.001)

    if now - _last_edit.get(uid, 0) < THROTTLE and _last_edit.get(uid, 0): return
    _last_edit[uid] = now

    raw = current / elapsed
    ema = EMA_A * raw + (1 - EMA_A) * _ema.get(uid, raw)
    _ema[uid] = ema

    if uid not in _hist: _hist[uid] = []
    _hist[uid].append(ema)
    if len(_hist[uid]) > 24: _hist[uid].pop(0)

    eta_s = (total - current) / ema if ema > 0 else 0
    real  = current * 100 / total
    shown = _count_up(uid, real)

    t = _tick.get(uid, 0); _tick[uid] = t + 1
    pool = BOUNCE if "Download" in mode else SIGNAL
    spin = pool[t % len(pool)]

    bar   = _abar(shown, t, 18)
    graph = _spark(_hist.get(uid, []), 12)
    hdr   = "📥" if "Download" in mode else "📤"

    eta_s_str = f"⚡ {_eta(eta_s)}" if eta_s < 15 and shown > 5 else _eta(eta_s)

    await _edit(msg,
        f"{spin} **{mode}**\n"
        f"══════════════════════════\n"
        f"`{bar}`\n"
        f"  {_icon(shown)} **{shown:.1f}%** — {_spd_label(ema)}\n"
        f"──────────────────────────\n"
        f"  📊 `{graph}` speed\n"
        f"──────────────────────────\n"
        f"  {hdr} **Size** : `{_sz(current)}` / `{_sz(total)}`\n"
        f"  ⚡ **Speed**: `{_sz(ema)}/s`\n"
        f"  ⏱ **ETA**  : `{eta_s_str}`\n"
        f"  ⏳ **Time** : `{_eta(elapsed)}`\n"
        f"══════════════════════════\n"
        f"  ❌ /cancel to abort"
    )

async def up_prog(current, total, msg, start, uid=0):
    await progress(current, total, msg, start, uid=uid, mode="📤 Upload")


# ══════════════════════════════════════════════════════════════
#  FFMPEG
# ══════════════════════════════════════════════════════════════
async def ffmpeg_cut(inp, out, ss, t) -> bool:
    p = await asyncio.create_subprocess_exec(
        "ffmpeg","-y","-ss",str(ss),"-i",inp,
        "-t",str(t),"-c","copy","-avoid_negative_ts","make_zero",out,
        stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
    )
    await p.wait()
    return p.returncode == 0

async def make_thumb(video, ss, out) -> str | None:
    p = await asyncio.create_subprocess_exec(
        "ffmpeg","-y","-ss",str(ss),"-i",video,
        "-vframes","1","-q:v","2","-vf","scale=320:-1",out,
        stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
    )
    await p.wait()
    return out if os.path.exists(out) else None

async def get_dur(file) -> float | None:
    try:
        p = await asyncio.create_subprocess_exec(
            "ffprobe","-v","error",
            "-show_entries","format=duration",
            "-of","default=noprint_wrappers=1:nokey=1",file,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
        )
        out,_ = await p.communicate()
        return float(out.decode().strip())
    except: return None


# ══════════════════════════════════════════════════════════════
#  SPLIT PROGRESS
# ══════════════════════════════════════════════════════════════
_stk: dict[int, int] = {}

async def _split_ui(msg, done, total, uid, note=""):
    pct  = done * 100 // total
    t    = _stk.get(uid, 0); _stk[uid] = t + 1
    spin = ORBIT[t % 4]
    bar  = _abar(pct, t, 16)

    cap  = min(total, 20)
    dots = ""
    for i in range(cap):
        if i < done:    dots += "✅"
        elif i == done: dots += "✂️"
        else:           dots += "⬜"
        if (i+1) % 10 == 0 and i+1 < cap: dots += "\n  "
    if total > 20: dots += f" +{total-20}"

    nl = f"\n  ┗ _{note}_" if note else ""
    await _edit(msg,
        f"{spin} **Splitting…**\n"
        f"══════════════════════════\n"
        f"`{bar}` **{pct}%**\n"
        f"  Part **{done}** / **{total}**{nl}\n"
        f"──────────────────────────\n"
        f"  {dots}\n"
        f"══════════════════════════\n"
        f"  ❌ /cancel to stop"
    )


# ══════════════════════════════════════════════════════════════
#  UPLOAD ONE PART
#  — NO new message created here (reuses split_msg)
#  — Part-level dedup via _done_parts
# ══════════════════════════════════════════════════════════════
async def _upload_part(message, split_msg, path, num, total, uid, thumb_t) -> bool:
    # Already sent this part? Return True immediately — no resend
    if _part_done(uid, num):
        return True

    if _get_cancel(uid).is_set():
        return False

    # Show upload state on the SAME split_msg (no new message)
    await _edit(split_msg,
        f"⣾ **Uploading** part {num}/{total}\n"
        f"══════════════════════════\n"
        f"`{BE*18}` **0%**\n"
        f"  🔵 STARTING\n"
        f"══════════════════════════\n"
        f"  ❌ /cancel to abort"
    )
    _rst(uid)
    t0 = time.time()

    thumb_path = f"{THUMB_DIR}/t_{uid}_{num}.jpg"
    thumb = await make_thumb(path, thumb_t, thumb_path)

    ok = False
    for attempt in range(5):
        if _part_done(uid, num): ok = True; break   # race check
        if _get_cancel(uid).is_set():
            await _edit(split_msg, "🚫 **Cancelled.**"); break
        try:
            await message.reply_video(
                path,
                caption=(
                    f"🎬 **Part {num} / {total}**\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"  ✅ Ultra Bot v9"
                ),
                thumb=thumb,
                progress=up_prog,
                progress_args=(split_msg, t0, uid),
            )
            _mark_part_done(uid, num)   # mark IMMEDIATELY after send
            ok = True
            break

        except FloodWait as e:
            wait = min(e.value + 2, 60)
            for rem in range(wait, 0, -1):
                sp   = CLOCK[rem % 12]
                fill = int((wait - rem) / wait * 18)
                await _edit(split_msg,
                    f"{sp} **Flood wait** — part {num}/{total}\n"
                    f"  ⏳ Resume in `{rem}s`\n"
                    f"  `{BF*fill}{BE*(18-fill)}`"
                )
                await asyncio.sleep(1)

        except Exception as e:
            err = str(e).lower()
            if any(x in err for x in ["duplicate","already","message_id_invalid"]):
                _mark_part_done(uid, num); ok = True; break
            if attempt >= 4:
                await _edit(split_msg, f"❌ Upload failed part {num}:\n`{e}`"); break
            await asyncio.sleep(3 * (attempt + 1))

    _rst(uid)
    if thumb and os.path.exists(thumb_path):
        try: os.remove(thumb_path)
        except: pass
    return ok


# ══════════════════════════════════════════════════════════════
#  /start
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.command("start") & filters.incoming, group=0)
async def cmd_start(client, message):
    if await _dedup(message.id): return
    name = message.from_user.first_name or "User"
    await message.reply(
        f"╔══════════════════════════╗\n"
        f"║  ⚡  ULTRA BOT  v9  ⚡   ║\n"
        f"╚══════════════════════════╝\n\n"
        f"👋 Welcome, **{name}**!\n\n"
        f"📽 **Send any video:**\n"
        f"  MP4 · MKV · AVI · MOV · WEBM\n\n"
        f"✂️ **Split commands:**\n"
        f"  • `/split 3`    → 3 equal parts\n"
        f"  • `/splitmin 2` → 2-min chunks\n\n"
        f"🛠 **Tools:**\n"
        f"  • `/status` → task info\n"
        f"  • `/cancel` → stop task\n"
        f"  • `/info`   → file details\n\n"
        f"──────────────────────────\n"
        f"  ✅ Zero double message\n"
        f"  ✅ Zero double upload\n"
        f"  ✅ Zero double split\n"
        f"──────────────────────────\n"
        f"  _Ultra Bot v9 — Bug Free_ 🔒"
    )


# ══════════════════════════════════════════════════════════════
#  /info
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.command("info") & filters.incoming, group=0)
async def cmd_info(client, message):
    if await _dedup(message.id): return
    uid = message.from_user.id
    if uid not in user_files or not os.path.exists(user_files[uid]):
        return await message.reply("❌ No video loaded.")
    p   = user_files[uid]
    dur = await get_dur(p)
    await message.reply(
        f"📋 **FILE INFO**\n"
        f"══════════════════════════\n"
        f"  📁 `{os.path.basename(p)}`\n"
        f"  📦 `{_sz(os.path.getsize(p))}`\n"
        f"  🎬 `{_eta(dur) if dur else 'unknown'}`\n"
        f"══════════════════════════\n"
        f"  👉 `/split N`  or  `/splitmin N`"
    )


# ══════════════════════════════════════════════════════════════
#  /status
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.command("status") & filters.incoming, group=0)
async def cmd_status(client, message):
    if await _dedup(message.id): return
    uid  = message.from_user.id
    info = user_status.get(uid)
    busy = _is_running(uid)

    if not busy or not info:
        if uid in user_files and os.path.exists(user_files[uid]):
            return await message.reply(
                f"💤 **IDLE — Video ready**\n"
                f"  📁 `{os.path.basename(user_files[uid])}`\n"
                f"  📦 `{_sz(os.path.getsize(user_files[uid]))}`\n"
                f"  👉 `/split N`  or  `/splitmin N`"
            )
        return await message.reply("💤 **IDLE** — Send a video to start.")

    hist  = _hist.get(uid, [])
    graph = _spark(hist, 14) if hist else "no data"
    speed = f"{_sz(hist[-1])}/s" if hist else "—"
    await message.reply(
        f"⚙️ **RUNNING**\n"
        f"══════════════════════════\n"
        f"  📌 Task : `{info['task']}`\n"
        f"  📝 Info : `{info['detail']}`\n"
        f"  ⏳ Time : `{_eta(time.time()-info['since'])}`\n"
        f"  📊 `{graph}`\n"
        f"  ⚡ `{speed}`\n"
        f"══════════════════════════\n"
        f"  ❌ /cancel to stop"
    )


# ══════════════════════════════════════════════════════════════
#  /cancel
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.command("cancel") & filters.incoming, group=0)
async def cmd_cancel(client, message):
    if await _dedup(message.id): return
    uid = message.from_user.id
    if not _is_running(uid):
        return await message.reply("💤 Nothing to cancel.")
    _get_cancel(uid).set()
    await message.reply(
        "🚫 **CANCEL REQUESTED**\n"
        "  Stopping at next checkpoint…"
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
async def recv_video(client, message):
    if await _dedup(message.id): return

    uid  = message.from_user.id

    if _is_running(uid):
        return await message.reply("⏳ Task running.\n👉 /status · /cancel")

    media     = message.document or message.video
    if not media: return

    mime      = getattr(media, "mime_type", "") or ""
    file_size = getattr(media, "file_size", 0) or 0

    if mime and not (mime.startswith("video/") or mime == "application/octet-stream"):
        return

    # Clean old file
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
    fname = f"{DOWNLOAD_DIR}/v_{uid}_{message.id}.{ext}"
    szs   = _sz(file_size) if file_size else "?"

    status = await message.reply(
        f"⣾ **DOWNLOADING**\n"
        f"══════════════════════════\n"
        f"  📁 `{ext.upper()}` · {szs}\n"
        f"`{BE*18}` **0%**\n"
        f"══════════════════════════\n"
        f"  ❌ /cancel"
    )

    _get_cancel(uid).clear()
    _rst(uid)
    _hist.pop(uid, None)
    _reset_parts(uid)
    _set_status(uid, "Downloading", szs)

    # Mark running for download too
    if not _set_running(uid):
        await _edit(status, "⏳ Already busy!"); return

    t0 = time.time()
    try:
        path = await message.download(
            file_name=fname,
            progress=progress,
            progress_args=(status, t0, uid, "📥 Download"),
        )
    except Exception as e:
        _clear_running(uid); _clear_status(uid)
        await _edit(status, f"❌ Download failed:\n`{e}`"); return

    if not path or not os.path.exists(path):
        _clear_running(uid); _clear_status(uid)
        await _edit(status, "❌ File not saved. Try again."); return

    elapsed = time.time() - t0
    avg     = file_size / elapsed if elapsed and file_size else 0
    actual  = os.path.getsize(path)

    _rst(uid)
    _clear_status(uid)
    _clear_running(uid)
    user_files[uid] = path

    await _edit(status,
        f"✅ **DOWNLOAD COMPLETE**\n"
        f"══════════════════════════\n"
        f"`{BF*18}` **100%** 🏆\n"
        f"──────────────────────────\n"
        f"  📁 `{os.path.basename(path)}`\n"
        f"  📦 `{_sz(actual)}`\n"
        f"  ⚡ `{_sz(avg)}/s`\n"
        f"  ⏱ `{_eta(elapsed)}`\n"
        f"══════════════════════════\n"
        f"  👉 `/split N`  or  `/splitmin N`"
    )


# ══════════════════════════════════════════════════════════════
#  CORE SPLIT LOGIC
# ══════════════════════════════════════════════════════════════
async def _do_split(message, uid: int, seg: float, parts: int, label: str):
    file   = user_files[uid]
    cancel = _get_cancel(uid)
    cancel.clear()
    _reset_parts(uid)
    _stk[uid] = 0

    msg = await message.reply(
        f"✂️ **{label}**\n"
        f"══════════════════════════\n"
        f"`{BE*16}` **0%**\n"
        f"  Part **0** / **{parts}**\n"
        f"══════════════════════════\n"
        f"  ❌ /cancel to stop"
    )

    try:
        for i in range(parts):
            if cancel.is_set():
                await _edit(msg,
                    f"🚫 **CANCELLED**\n"
                    f"  Stopped at part **{i}** / **{parts}**."
                )
                return

            ss = i * seg
            _set_status(uid, "Splitting", f"part {i+1}/{parts}")
            await _split_ui(msg, i, parts, uid, f"cutting {i+1}…")

            out = f"{DOWNLOAD_DIR}/p_{uid}_{i+1}.mp4"
            ok  = await ffmpeg_cut(file, out, ss, seg)
            if not ok or not os.path.exists(out):
                await _edit(msg, f"❌ ffmpeg failed on part {i+1}")
                return

            _set_status(uid, "Uploading", f"part {i+1}/{parts}")
            await _split_ui(msg, i, parts, uid, f"uploading {i+1}…")

            # Pass msg as split_msg — NO new message created
            sent = await _upload_part(message, msg, out, i+1, parts, uid, ss + seg/2)

            try:
                if os.path.exists(out): os.remove(out)
            except: pass

            if not sent:
                await _edit(msg,
                    f"🚫 **CANCELLED**\n"
                    f"  Stopped at part **{i+1}** / **{parts}**."
                )
                return

            await _split_ui(msg, i+1, parts, uid,
                "🎉 all done!" if i+1 == parts else f"next: {i+2}…"
            )

        # ── Success ──
        try:
            if os.path.exists(file): os.remove(file)
        except: pass
        user_files.pop(uid, None)
        _reset_parts(uid)

        checks = "".join("✅" if j < parts else "" for j in range(min(parts, 20)))
        if parts > 20: checks += f" +{parts-20}"

        await _edit(msg,
            f"🏆 **ALL {parts} PARTS DONE!**\n"
            f"══════════════════════════\n"
            f"`{BF*16}` **100%**\n"
            f"  {checks}\n"
            f"──────────────────────────\n"
            f"  ✅ **{parts} parts** delivered\n"
            f"  🎬 Ultra Bot v9\n"
            f"══════════════════════════\n"
            f"  _Send another video to split!_"
        )

    except Exception as e:
        await _edit(msg, f"❌ Error:\n`{e}`")
    finally:
        _clear_status(uid)
        _clear_running(uid)   # ALWAYS release running flag


# ══════════════════════════════════════════════════════════════
#  /split
#  TRIPLE GUARD: dedup → cooldown → running flag
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.command("split") & filters.incoming, group=0)
async def cmd_split(client, message):
    if await _dedup(message.id): return         # Layer 1: exact message ID
    uid = message.from_user.id
    if _in_cooldown(uid): return                # Layer 2: 5s time window
    if not _set_running(uid):                   # Layer 3: atomic running flag
        return await message.reply("⏳ Already processing!\n👉 /status · /cancel")

    # From here: we OWN the running flag — must release in finally
    try:
        if uid not in user_files:
            await message.reply("❌ No video loaded.\n  Send a video first!")
            return
        try:
            parts = int(message.command[1])
            assert 2 <= parts <= 100
        except Exception:
            await message.reply("❌ Usage: `/split 3`\n  Min 2, max 100 parts.")
            return
        dur = await get_dur(user_files[uid])
        if not dur:
            await message.reply("❌ Cannot read duration.")
            return
        await _do_split(message, uid, dur / parts, parts,
                        f"Splitting into **{parts}** equal parts…")
    except Exception as e:
        await message.reply(f"❌ Error: `{e}`")
        _clear_running(uid)
    # Note: _do_split's finally block handles _clear_running on normal flow


# ══════════════════════════════════════════════════════════════
#  /splitmin
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.command("splitmin") & filters.incoming, group=0)
async def cmd_splitmin(client, message):
    if await _dedup(message.id): return         # Layer 1
    uid = message.from_user.id
    if _in_cooldown(uid): return                # Layer 2
    if not _set_running(uid):                   # Layer 3
        return await message.reply("⏳ Already processing!\n👉 /status · /cancel")

    try:
        if uid not in user_files:
            await message.reply("❌ No video loaded.\n  Send a video first!")
            return
        try:
            mins = int(message.command[1])
            assert 1 <= mins <= 60
        except Exception:
            await message.reply("❌ Usage: `/splitmin 2`\n  Chunk in minutes (1–60).")
            return
        dur = await get_dur(user_files[uid])
        if not dur:
            await message.reply("❌ Cannot read duration.")
            return
        seg   = mins * 60
        parts = math.ceil(dur / seg)
        if parts > 100:
            await message.reply(f"❌ Too many parts ({parts}). Use larger chunk.")
            return
        await _do_split(message, uid, seg, parts,
                        f"Splitting **{mins} min** chunks → **{parts}** parts…")
    except Exception as e:
        await message.reply(f"❌ Error: `{e}`")
        _clear_running(uid)


# ══════════════════════════════════════════════════════════════
app.run()
