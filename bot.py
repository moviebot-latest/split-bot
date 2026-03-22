import os
import time
import math
import asyncio
import subprocess
from pyrogram import Client, filters

API_ID   = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")

app = Client("ultra-bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

user_files: dict[int, str] = {}
user_queue: set[int]       = set()

# ╔══════════════════════════════════════════════════════════════╗
#  ██  ULTRA PROGRESS ENGINE v3                                 ██
#  ██  0.2s throttle · EMA speed · count-up % · premium bar    ██
# ╚══════════════════════════════════════════════════════════════╝

THROTTLE  = 0.2     # seconds between Telegram message edits
EMA_ALPHA = 0.35    # EMA smoothing factor  (higher = more responsive)
BAR_WIDTH = 20      # bar character width

# Braille spinner — fast & minimal
SPINNER = ["⠋","⠙","⠹","⠸","⠼","⠴","⠦","⠧","⠇","⠏"]

# Per-user state dicts
_last_edit: dict[int, float] = {}
_ema_speed: dict[int, float] = {}
_spin_idx:  dict[int, int]   = {}
_shown_pct: dict[int, float] = {}   # for count-up animation


def _reset(uid: int) -> None:
    for d in (_last_edit, _ema_speed, _spin_idx, _shown_pct):
        d.pop(uid, None)


# ── Formatters ────────────────────────────────────────────────
def _sz(b: float) -> str:
    for u in ("B","KB","MB","GB"):
        if b < 1024: return f"{b:.1f} {u}"
        b /= 1024
    return f"{b:.1f} TB"

def _eta(s: float) -> str:
    if s >= 3600: return f"{int(s//3600)}h {int(s%3600//60)}m"
    if s >= 60:   return f"{int(s//60)}m {int(s%60)}s"
    return f"{int(s)}s"


# ── Bar: █████▓░░░░ (filled · frontier · empty) ──────────────
def _bar(pct: float) -> str:
    n = int(pct / 100 * BAR_WIDTH)
    f = BAR_WIDTH - n - (1 if n < BAR_WIDTH else 0)
    return "█" * n + ("▓" if n < BAR_WIDTH else "") + "░" * f


# ── Milestone badge ───────────────────────────────────────────
def _badge(pct: float) -> str:
    return ("🏁" if pct>=100 else "🔥" if pct>=80 else
            "⚡" if pct>=60 else "🚀" if pct>=40 else
            "💫" if pct>=20 else "🌀")


# ── Count-up: display % walks toward real % smoothly ─────────
def _count_up(uid: int, real: float, step: float = 1.8) -> float:
    prev = _shown_pct.get(uid, 0.0)
    shown = min(real, prev + step) if real > prev else real
    _shown_pct[uid] = shown
    return shown


# ══════════════════════════════════════════════════════════════
#  PROGRESS CALLBACK
# ══════════════════════════════════════════════════════════════
async def progress(
    current: int, total: int,
    message, start: float,
    uid: int = 0, mode: str = "⬇️ Download",
) -> None:
    if not isinstance(total, (int, float)) or total <= 0:
        return

    now     = time.time()
    elapsed = max(now - start, 0.001)

    # Throttle to THROTTLE seconds
    if now - _last_edit.get(uid, 0.0) < THROTTLE:
        return
    _last_edit[uid] = now

    # EMA speed — smooth but still reacts fast
    raw  = current / elapsed
    ema  = EMA_ALPHA * raw + (1 - EMA_ALPHA) * _ema_speed.get(uid, raw)
    _ema_speed[uid] = ema
    eta_sec = (total - current) / ema if ema > 0 else 0

    # Count-up percentage
    real  = current * 100 / total
    shown = _count_up(uid, real)

    # Spinner frame
    i = _spin_idx.get(uid, 0)
    spin = SPINNER[i % len(SPINNER)]
    _spin_idx[uid] = i + 1

    # Speed tier
    kb = ema / 1024
    tier = "🟢 Fast" if kb >= 1024 else ("🟡 Good" if kb >= 256 else "🔴 Slow")

    text = (
        f"{spin} **{mode}**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"`[{_bar(shown)}]`\n"
        f"  {_badge(shown)} **{shown:.1f}%**  ·  {_sz(current)} / {_sz(total)}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"  🚄 **Speed** : `{_sz(ema)}/s`  {tier}\n"
        f"  ⏱ **ETA**   : `{_eta(eta_sec)}`\n"
        f"  ⏳ **Elapsed**: `{_eta(elapsed)}`"
    )
    try:
        await message.edit(text)
    except Exception:
        pass


async def upload_progress(current, total, message, start, uid=0):
    await progress(current, total, message, start, uid=uid, mode="⬆️ Upload")


# ══════════════════════════════════════════════════════════════
#  SPLIT BAR  (part-by-part while ffmpeg processes)
# ══════════════════════════════════════════════════════════════
def _sbar(done: int, total: int, w: int = 16) -> str:
    f = int(done / total * w)
    return "▰" * f + "▱" * (w - f)

async def _split_update(msg, done: int, total: int, note: str = "") -> None:
    pct  = done * 100 // total
    note_line = f"\n  ┗ _{note}_" if note else ""
    await msg.edit(
        f"✂️ **Splitting…**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"`[{_sbar(done, total)}]` **{pct}%**\n"
        f"  Part **{done}** / **{total}** done{note_line}"
    )


# ══════════════════════════════════════════════════════════════
#  /start
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.command("start"))
async def start(client, message):
    await message.reply(
        "⚡ **ULTRA BOT v3** — ready!\n\n"
        "📤 Send any **video**, then:\n"
        "  • `/split 3`     — N equal parts\n"
        "  • `/splitmin 2`  — chunk every N minutes\n\n"
        "0.2s updates · EMA smooth speed · count-up % 🔥"
    )


# ══════════════════════════════════════════════════════════════
#  RECEIVE VIDEO
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.video | filters.document)
async def receive(client, message):
    uid = message.from_user.id
    if uid in user_queue:
        return await message.reply("⏳ Wait — previous task still running.")

    status = await message.reply(
        "⠋ **⬇️ Download**\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "`[░░░░░░░░░░░░░░░░░░░░]`\n"
        "  🌀 **0.0%**  ·  starting…"
    )
    _reset(uid)
    t0 = time.time()

    path = await message.download(
        file_name=f"{DOWNLOAD_DIR}/video_{message.id}.mp4",
        progress=progress,
        progress_args=(status, t0, uid, "⬇️ Download"),
    )

    _reset(uid)
    user_files[uid] = path
    await status.edit("✅ **Download complete!**\n\n👉 `/split N`  or  `/splitmin N`")


# ══════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════
def get_duration(file: str) -> float | None:
    try:
        return float(subprocess.check_output(
            ["ffprobe","-v","error","-show_entries","format=duration",
             "-of","default=noprint_wrappers=1:nokey=1", file],
            stderr=subprocess.DEVNULL,
        ).decode().strip())
    except Exception:
        return None

async def _upload_part(message, path: str, num: int, total: int, uid: int) -> None:
    status = await message.reply(
        f"⠋ **⬆️ Upload**  — part {num}/{total}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"`[░░░░░░░░░░░░░░░░░░░░]`\n"
        f"  🌀 **0.0%**  ·  starting…"
    )
    _reset(uid)
    t0 = time.time()
    await message.reply_video(
        path,
        caption=f"🎬 **Part {num} / {total}**",
        progress=upload_progress,
        progress_args=(status, t0, uid),
    )
    _reset(uid)
    await status.delete()


# ══════════════════════════════════════════════════════════════
#  /split
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.command("split"))
async def split(client, message):
    uid = message.from_user.id
    if uid in user_queue:  return await message.reply("⏳ Already processing.")
    if uid not in user_files: return await message.reply("❌ Send a video first.")

    try:
        parts = int(message.command[1]); assert parts >= 2
    except Exception:
        return await message.reply("❌ Usage: `/split 3`")

    user_queue.add(uid)
    file = user_files[uid]
    dur  = get_duration(file)

    if not dur:
        user_queue.discard(uid)
        return await message.reply("❌ Could not read video.")

    seg = dur / parts
    msg = await message.reply(
        f"✂️ **Splitting into {parts} parts…**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"`[▱▱▱▱▱▱▱▱▱▱▱▱▱▱▱▱]` **0%**"
    )

    try:
        for i in range(parts):
            await _split_update(msg, i, parts, f"cutting segment {i+1}…")
            out = f"{DOWNLOAD_DIR}/part_{i+1}.mp4"
            subprocess.run(
                ["ffmpeg","-y","-ss",str(i*seg),"-i",file,
                 "-t",str(seg),"-c","copy","-avoid_negative_ts","make_zero",out],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            await _upload_part(message, out, i+1, parts, uid)
            os.remove(out)

        os.remove(file); user_files.pop(uid, None)
        await msg.edit(
            f"🏁 **All {parts} parts done!**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"`[▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰]` **100%**\n  ✅ Complete"
        )
    except Exception as e:
        await msg.edit(f"❌ `{e}`")
    finally:
        user_queue.discard(uid)


# ══════════════════════════════════════════════════════════════
#  /splitmin
# ══════════════════════════════════════════════════════════════
@app.on_message(filters.command("splitmin"))
async def splitmin(client, message):
    uid = message.from_user.id
    if uid in user_queue:  return await message.reply("⏳ Already processing.")
    if uid not in user_files: return await message.reply("❌ Send a video first.")

    try:
        mins = int(message.command[1]); assert mins >= 1
    except Exception:
        return await message.reply("❌ Usage: `/splitmin 2`")

    user_queue.add(uid)
    file = user_files[uid]
    dur  = get_duration(file)

    if not dur:
        user_queue.discard(uid)
        return await message.reply("❌ Could not read video.")

    seg   = mins * 60
    parts = math.ceil(dur / seg)
    msg   = await message.reply(
        f"✂️ **Splitting — {mins} min chunks ({parts} parts)…**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"`[▱▱▱▱▱▱▱▱▱▱▱▱▱▱▱▱]` **0%**"
    )

    try:
        for i in range(parts):
            await _split_update(msg, i, parts, f"{i*mins}–{(i+1)*mins} min")
            out = f"{DOWNLOAD_DIR}/min_{i+1}.mp4"
            subprocess.run(
                ["ffmpeg","-y","-ss",str(i*seg),"-i",file,
                 "-t",str(seg),"-c","copy","-avoid_negative_ts","make_zero",out],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            await _upload_part(message, out, i+1, parts, uid)
            os.remove(out)

        os.remove(file); user_files.pop(uid, None)
        await msg.edit(
            f"🏁 **All {parts} parts done!**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"`[▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰]` **100%**\n  ✅ Complete"
        )
    except Exception as e:
        await msg.edit(f"❌ `{e}`")
    finally:
        user_queue.discard(uid)


# ══════════════════════════════════════════════════════════════
app.run()
