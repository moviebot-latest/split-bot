import os
import asyncio
import subprocess
from pyrogram import Client, filters
from pyrogram.types import Message

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")

bot = Client("split-bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

FFMPEG = "./ffmpeg"

# store user video
USER_VIDEO = {}

@bot.on_message(filters.command("start"))
async def start(_, msg: Message):
    await msg.reply("👋 Send video then use:\n\n/split 3 (parts)\n/splitmin 2 (minutes)")

@bot.on_message(filters.video)
async def save_video(_, msg: Message):
    file = await msg.download()
    USER_VIDEO[msg.from_user.id] = file
    await msg.reply("✅ Video received!\nNow send /split 3")

# SPLIT BY PARTS
@bot.on_message(filters.command("split"))
async def split_parts(_, msg: Message):
    user_id = msg.from_user.id

    if user_id not in USER_VIDEO:
        return await msg.reply("❌ Send video first")

    try:
        parts = int(msg.command[1])
    except:
        return await msg.reply("Usage: /split 3")

    file = USER_VIDEO[user_id]

    # get duration
    result = subprocess.run(
        [FFMPEG.replace("ffmpeg", "ffprobe"), "-i", file],
        stderr=subprocess.PIPE,
        text=True
    )

    import re
    duration = re.search(r"Duration: (\d+):(\d+):(\d+)", result.stderr)
    h, m, s = map(int, duration.groups())
    total_sec = h*3600 + m*60 + s

    part_duration = total_sec // parts

    await msg.reply(f"⚡ Splitting into {parts} parts...")

    for i in range(parts):
        start = i * part_duration
        out = f"part_{i}.mp4"

        cmd = [
            FFMPEG,
            "-ss", str(start),
            "-i", file,
            "-t", str(part_duration),
            "-c", "copy",
            out
        ]

        subprocess.run(cmd)

        await msg.reply_video(out, caption=f"Part {i+1}")
        os.remove(out)

# SPLIT BY MINUTES
@bot.on_message(filters.command("splitmin"))
async def split_minutes(_, msg: Message):
    user_id = msg.from_user.id

    if user_id not in USER_VIDEO:
        return await msg.reply("❌ Send video first")

    try:
        minutes = int(msg.command[1])
    except:
        return await msg.reply("Usage: /splitmin 2")

    file = USER_VIDEO[user_id]
    sec = minutes * 60

    await msg.reply(f"⚡ Splitting every {minutes} min...")

    i = 0
    start = 0

    while True:
        out = f"chunk_{i}.mp4"

        cmd = [
            FFMPEG,
            "-ss", str(start),
            "-i", file,
            "-t", str(sec),
            "-c", "copy",
            out
        ]

        subprocess.run(cmd)

        if not os.path.exists(out) or os.path.getsize(out) == 0:
            break

        await msg.reply_video(out, caption=f"Part {i+1}")
        os.remove(out)

        start += sec
        i += 1

bot.run()
