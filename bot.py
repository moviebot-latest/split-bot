import os
import time
import math
import asyncio
import subprocess
from pyrogram import Client, filters

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")

app = Client("split-bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

user_files = {}
busy = {}
last_update = 0


# ================= PROGRESS =================
async def progress_bar(current, total, message):
    global last_update

    if not isinstance(total, (int, float)) or total == 0:
        return

    now = time.time()
    if now - last_update < 2:
        return

    last_update = now
    percent = current * 100 / total
    bar = "█" * int(percent // 10) + "░" * (10 - int(percent // 10))

    try:
        await message.edit(f"📥 Downloading...\n[{bar}] {percent:.1f}%")
    except:
        pass


# ================= START =================
@app.on_message(filters.command("start"))
async def start(client, message):
    await message.reply(
        "👋 Send video\n\n"
        "👉 /split 2\n"
        "👉 /splitmin 5"
    )


# ================= RECEIVE =================
@app.on_message(filters.video | filters.document)
async def receive(client, message):
    status = await message.reply("📥 Starting download...")

    file_path = await message.download(
        file_name=f"{DOWNLOAD_DIR}/video_{message.id}.mp4",
        progress=progress_bar,
        progress_args=(status,)
    )

    user_files[message.from_user.id] = file_path
    await status.edit("✅ Download complete!\n👉 Send /split 2")


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


# ================= THUMBNAIL =================
def generate_thumbnail(video, output):
    cmd = [
        "ffmpeg",
        "-i", video,
        "-ss", "00:00:01",
        "-vframes", "1",
        output
    ]
    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


# ================= SAFE SEND =================
async def safe_send(client, chat_id, file, caption, thumb):
    for _ in range(3):
        try:
            await client.send_video(
                chat_id=chat_id,
                video=file,
                caption=caption,
                thumb=thumb
            )
            return
        except:
            await asyncio.sleep(2)


# ================= SPLIT =================
@app.on_message(filters.command("split"))
async def split(client, message):
    uid = message.from_user.id

    if busy.get(uid):
        return await message.reply("⏳ Already processing...")

    if uid not in user_files:
        return await message.reply("❌ Send video first")

    busy[uid] = True

    try:
        parts = int(message.command[1])
    except:
        busy[uid] = False
        return await message.reply("❌ Use /split 2")

    file = user_files[uid]
    duration = get_duration(file)

    if not duration:
        busy[uid] = False
        return await message.reply("❌ Video error")

    part_duration = math.ceil(duration / parts)

    msg = await message.reply("✂️ Splitting started...")

    start = 0

    for i in range(parts):
        if start >= duration:
            break

        await msg.edit(f"✂️ Processing {i+1}/{parts}")

        output = f"{DOWNLOAD_DIR}/part_{i}.mp4"

        cmd = [
            "ffmpeg", "-y",
            "-i", file,
            "-ss", str(start),
            "-t", str(part_duration),
            "-c", "copy",
            output
        ]

        subprocess.run(cmd)

        thumb = f"{DOWNLOAD_DIR}/thumb_{i}.jpg"
        generate_thumbnail(output, thumb)

        await safe_send(
            client,
            message.chat.id,
            output,
            f"📦 Part {i+1}/{parts}",
            thumb
        )

        os.remove(output)
        os.remove(thumb)

        start += part_duration
        await asyncio.sleep(1)

    os.remove(file)
    user_files.pop(uid)
    busy[uid] = False

    await msg.edit("✅ Done!")


# ================= RUN =================
app.run()
