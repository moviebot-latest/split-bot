import os
import time
import math
import asyncio
import subprocess
from pyrogram import Client, filters

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")

app = Client("ultra-bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

user_files = {}
user_queue = set()


# ================= PROGRESS =================
async def progress(current, total, message, start):
    if not isinstance(total, (int, float)) or total == 0:
        return

    now = time.time()
    diff = now - start
    speed = current / diff if diff > 0 else 0
    eta = (total - current) / speed if speed > 0 else 0

    percent = current * 100 / total
    bar = "█" * int(percent // 5) + "░" * (20 - int(percent // 5))

    text = f"""
🚀 Progress
[{bar}] {percent:.1f}%

⚡ {speed/1024:.2f} KB/s
⏳ ETA: {int(eta)} sec
"""

    try:
        await message.edit(text)
    except:
        pass


# ================= START =================
@app.on_message(filters.command("start"))
async def start(client, message):
    await message.reply(
        "🔥 ULTRA BOT READY\n\n"
        "Send video → /split 2\n"
        "or /splitmin 1"
    )


# ================= RECEIVE =================
@app.on_message(filters.video | filters.document)
async def receive(client, message):
    if message.from_user.id in user_queue:
        return await message.reply("⏳ Wait previous task finish")

    status = await message.reply("📥 Downloading...")

    start = time.time()

    file_path = await message.download(
        file_name=f"{DOWNLOAD_DIR}/video_{message.id}.mp4",
        progress=progress,
        progress_args=(status, start)
    )

    user_files[message.from_user.id] = file_path

    await status.edit("✅ Download done!\n👉 Send /split 2")


# ================= DURATION =================
def get_duration(file):
    try:
        return float(subprocess.check_output([
            "ffprobe", "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            file
        ]).decode().strip())
    except:
        return None


# ================= SPLIT =================
@app.on_message(filters.command("split"))
async def split(client, message):
    uid = message.from_user.id

    if uid in user_queue:
        return await message.reply("⏳ Already processing")

    if uid not in user_files:
        return await message.reply("❌ Send video first")

    user_queue.add(uid)

    try:
        parts = int(message.command[1])
    except:
        user_queue.remove(uid)
        return await message.reply("❌ Use /split 2")

    file = user_files[uid]
    duration = get_duration(file)

    if not duration:
        user_queue.remove(uid)
        return await message.reply("❌ Video error")

    part_time = duration / parts
    msg = await message.reply("✂️ Splitting...")

    for i in range(parts):
        await msg.edit(f"✂️ Part {i+1}/{parts}")

        out = f"{DOWNLOAD_DIR}/part_{i}.mp4"

        subprocess.run([
            "ffmpeg", "-i", file,
            "-ss", str(i * part_time),
            "-t", str(part_time),
            "-c", "copy", out
        ])

        await message.reply_video(out)

        os.remove(out)

    os.remove(file)
    user_files.pop(uid)
    user_queue.remove(uid)

    await msg.edit("✅ Done!")


# ================= SPLIT MIN =================
@app.on_message(filters.command("splitmin"))
async def splitmin(client, message):
    uid = message.from_user.id

    if uid in user_queue:
        return await message.reply("⏳ Wait...")

    if uid not in user_files:
        return await message.reply("❌ Send video first")

    user_queue.add(uid)

    try:
        minutes = int(message.command[1])
    except:
        user_queue.remove(uid)
        return await message.reply("❌ Use /splitmin 1")

    file = user_files[uid]
    duration = get_duration(file)

    if not duration:
        user_queue.remove(uid)
        return await message.reply("❌ Error")

    part_time = minutes * 60
    parts = math.ceil(duration / part_time)

    msg = await message.reply("✂️ Splitting...")

    for i in range(parts):
        await msg.edit(f"✂️ Part {i+1}/{parts}")

        out = f"{DOWNLOAD_DIR}/min_{i}.mp4"

        subprocess.run([
            "ffmpeg", "-i", file,
            "-ss", str(i * part_time),
            "-t", str(part_time),
            "-c", "copy", out
        ])

        await message.reply_video(out)
        os.remove(out)

    os.remove(file)
    user_files.pop(uid)
    user_queue.remove(uid)

    await msg.edit("✅ Done!")


# ================= RUN =================
app.run()
