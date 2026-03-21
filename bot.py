import os
import time
import math
import asyncio
import subprocess
from pyrogram import Client, filters
from pyrogram.types import Message

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")

app = Client("split-bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

user_files = {}

# 🔥 Progress animation
async def progress_bar(message, current, total, start):
    now = time.time()
    diff = now - start
    if diff == 0:
        return

    percentage = current * 100 / total
    speed = current / diff
    eta = (total - current) / speed if speed > 0 else 0

    bar = "█" * int(percentage / 5) + "░" * (20 - int(percentage / 5))

    text = f"""
🚀 **Processing...**
[{bar}] {percentage:.2f}%

⚡ Speed: {speed/1024:.2f} KB/s
⏳ ETA: {int(eta)} sec
"""

    try:
        await message.edit(text)
    except:
        pass

# ✅ START COMMAND
@app.on_message(filters.command("start"))
async def start(client, message: Message):
    await message.reply_text(
        "👋 Send video\n\n"
        "Use:\n"
        "👉 /split 3 (parts)\n"
        "👉 /splitmin 2 (minutes)"
    )

# ✅ RECEIVE VIDEO
@app.on_message(filters.video)
async def video_handler(client, message: Message):
    msg = await message.reply("📥 Downloading video...")

    start = time.time()

    file_path = await message.download(
        progress=progress_bar,
        progress_args=(msg, start)
    )

    user_files[message.from_user.id] = file_path

    await msg.edit("✅ Video received!\nNow send /split 2")

# 🔥 SPLIT BY PARTS
@app.on_message(filters.command("split"))
async def split_handler(client, message: Message):
    user_id = message.from_user.id

    if user_id not in user_files:
        return await message.reply("❌ Send video first")

    parts = int(message.command[1])
    input_file = user_files[user_id]

    msg = await message.reply("✂️ Splitting video...")

    # duration
    cmd = [
        "ffprobe", "-v", "error", "-show_entries",
        "format=duration", "-of",
        "default=noprint_wrappers=1:nokey=1", input_file
    ]
    duration = float(subprocess.check_output(cmd).decode().strip())

    part_duration = duration / parts

    for i in range(parts):
        start_time = i * part_duration
        output = f"part_{i+1}.mp4"

        cmd = [
            "ffmpeg",
            "-i", input_file,
            "-ss", str(start_time),
            "-t", str(part_duration),
            "-c", "copy",
            output
        ]

        subprocess.run(cmd)

        await message.reply_video(output)
        os.remove(output)

    await msg.edit("✅ Done splitting!")

# 🔥 SPLIT BY MINUTES
@app.on_message(filters.command("splitmin"))
async def splitmin_handler(client, message: Message):
    user_id = message.from_user.id

    if user_id not in user_files:
        return await message.reply("❌ Send video first")

    minutes = int(message.command[1])
    input_file = user_files[user_id]

    msg = await message.reply("✂️ Splitting by minutes...")

    # duration
    cmd = [
        "ffprobe", "-v", "error", "-show_entries",
        "format=duration", "-of",
        "default=noprint_wrappers=1:nokey=1", input_file
    ]
    duration = float(subprocess.check_output(cmd).decode().strip())

    part_duration = minutes * 60
    parts = math.ceil(duration / part_duration)

    for i in range(parts):
        start_time = i * part_duration
        output = f"part_{i+1}.mp4"

        cmd = [
            "ffmpeg",
            "-i", input_file,
            "-ss", str(start_time),
            "-t", str(part_duration),
            "-c", "copy",
            output
        ]

        subprocess.run(cmd)

        await message.reply_video(output)
        os.remove(output)

    await msg.edit("✅ Done splitting!")

# 🚀 RUN
app.run()
