import os
import subprocess
from pyrogram import Client, filters

API_ID = int(os.environ.get("API_ID"))
API_HASH = os.environ.get("API_HASH")
BOT_TOKEN = os.environ.get("BOT_TOKEN")

app = Client("video_split_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

USER_VIDEOS = {}

# START COMMAND
@app.on_message(filters.command("start"))
async def start(client, message):
    await message.reply_text("👋 Send a video then use /split 2 (example)")

# SAVE VIDEO
@app.on_message(filters.video)
async def save_video(client, message):
    msg = await message.reply("📥 Downloading video...")
    
    file_path = await message.download()
    USER_VIDEOS[message.chat.id] = file_path

    await msg.edit("✅ Video saved!\nNow send /split 2")

# SPLIT FUNCTION
def split_video(input_file, parts):
    duration_cmd = [
        "ffprobe", "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        input_file
    ]
    
    duration = float(subprocess.check_output(duration_cmd).decode().strip())
    part_duration = duration / parts

    output_files = []

    for i in range(parts):
        start = i * part_duration
        output = f"part_{i+1}.mp4"

        cmd = [
            "ffmpeg",
            "-i", input_file,
            "-ss", str(start),
            "-t", str(part_duration),
            "-c", "copy",
            output
        ]

        subprocess.run(cmd)
        output_files.append(output)

    return output_files

# SPLIT COMMAND
@app.on_message(filters.command("split"))
async def split_cmd(client, message):
    try:
        parts = int(message.command[1])
    except:
        return await message.reply("❌ Use like: /split 2")

    if message.chat.id not in USER_VIDEOS:
        return await message.reply("⚠️ First send a video")

    input_file = USER_VIDEOS[message.chat.id]

    msg = await message.reply("⚡ Splitting video...")

    try:
        files = split_video(input_file, parts)

        for i, file in enumerate(files):
            await message.reply_video(file, caption=f"Part {i+1}")
            os.remove(file)

        os.remove(input_file)
        USER_VIDEOS.pop(message.chat.id)

        await msg.delete()

    except Exception as e:
        await message.reply(f"❌ Error: {e}")

# RUN BOT
app.run()
