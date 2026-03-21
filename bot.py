import os
import math
import subprocess
from pyrogram import Client, filters

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")

app = Client("bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

user_files = {}

@app.on_message(filters.video)
async def video_handler(client, message):
    file = await message.download()
    user_files[message.chat.id] = file
    await message.reply("✅ Video received!\nNow send /split 3")

@app.on_message(filters.command("split"))
async def split_handler(client, message):
    try:
        parts = int(message.command[1])
    except:
        return await message.reply("❌ Use like: /split 3")

    if message.chat.id not in user_files:
        return await message.reply("❌ Send video first")

    input_file = user_files[message.chat.id]

    # get duration
    result = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries",
         "format=duration", "-of",
         "default=noprint_wrappers=1:nokey=1", input_file],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT
    )
    duration = float(result.stdout)

    part_duration = duration / parts

    for i in range(parts):
        start = i * part_duration
        output = f"part_{i}.mp4"

        cmd = [
            "ffmpeg",
            "-i", input_file,
            "-ss", str(start),
            "-t", str(part_duration),
            "-c", "copy",
            output
        ]

        subprocess.run(cmd)

        await message.reply_video(output, caption=f"Part {i+1}")
        os.remove(output)

app.run()
