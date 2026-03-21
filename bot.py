import os
import time
import math
import asyncio
import subprocess
from pyrogram import Client, filters

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")

app = Client("premium-bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

user_files = {}
busy = {}
last_update = 0

spinner = ["⏳", "🔄", "⚙️", "🚀"]

# ================= PREMIUM UI =================
async def premium_status(msg, current, total, start_time, title):
    now = time.time()
    diff = now - start_time

    percent = (current / total) * 100
    bar = "█" * int(percent // 5) + "░" * (20 - int(percent // 5))

    speed = current / diff if diff > 0 else 0
    eta = (total - current) / speed if speed > 0 else 0

    icon = spinner[current % len(spinner)]

    try:
        await msg.edit(
            f"{icon} {title}\n\n"
            f"[{bar}] {percent:.1f}%\n\n"
            f"📦 {current}/{total}\n"
            f"⚡ {speed/1024/1024:.2f} MB/s\n"
            f"⏱ ETA: {int(eta)} sec"
        )
    except:
        pass


# ================= DOWNLOAD PROGRESS =================
async def progress_bar(current, total, message, start):
    global last_update

    if not isinstance(total, (int, float)) or total == 0:
        return

    now = time.time()
    if now - last_update < 2:
        return

    last_update = now

    diff = now - start
    speed = current / diff if diff > 0 else 0
    eta = (total - current) / speed if speed > 0 else 0

    percent = current * 100 / total
    bar = "█" * int(percent // 10) + "░" * (10 - int(percent // 10))

    try:
        await message.edit(
            f"📥 Downloading...\n"
            f"[{bar}] {percent:.1f}%\n"
            f"⚡ {speed/1024/1024:.2f} MB/s\n"
            f"⏱ ETA: {int(eta)} sec"
        )
    except:
        pass


# ================= START =================
@app.on_message(filters.command("start"))
async def start(client, message):
    await message.reply(
        "🔥 Premium Split Bot\n\n"
        "/split 2\n"
        "/splitmin 5\n"
        "/splitminsmart 5"
    )


# ================= RECEIVE =================
@app.on_message(filters.video | filters.document)
async def receive(client, message):
    status = await message.reply("🚀 Starting download...")

    start = time.time()

    file_path = await message.download(
        file_name=f"{DOWNLOAD_DIR}/video_{message.id}.mp4",
        progress=progress_bar,
        progress_args=(status, start)
    )

    user_files[message.from_user.id] = file_path

    await status.edit("✅ Download complete!\n👉 Choose split command")


# ================= DURATION =================
def get_duration(file):
    try:
        return float(subprocess.check_output([
            "ffprobe","-v","error",
            "-show_entries","format=duration",
            "-of","default=noprint_wrappers=1:nokey=1",
            file
        ]).decode().strip())
    except:
        return None


# ================= THUMB =================
def generate_thumbnail(video, output, duration):
    sec = int(duration * 0.1)
    subprocess.run([
        "ffmpeg","-i",video,
        "-ss",str(sec),
        "-vframes","1",
        "-vf","scale=320:320",
        output
    ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


# ================= SAFE SEND =================
async def safe_send(client, chat_id, file, caption, thumb):
    for _ in range(3):
        try:
            await client.send_video(chat_id, file, caption=caption, thumb=thumb)
            return
        except:
            await asyncio.sleep(2)


# ================= COMMON SPLIT FUNCTION =================
async def process_split(client, message, parts, mode="equal"):
    uid = message.from_user.id

    if busy.get(uid):
        return await message.reply("⏳ Already processing")

    if uid not in user_files:
        return await message.reply("❌ Send video first")

    busy[uid] = True

    file = user_files[uid]
    duration = get_duration(file)

    if not duration:
        busy[uid] = False
        return await message.reply("❌ Video error")

    # mode logic
    if mode == "equal":
        part_duration = math.ceil(duration / parts)
    elif mode == "smart":
        base = parts * 60
        parts = max(1, round(duration / base))
        part_duration = int(duration / parts)
    else:
        part_duration = parts * 60
        parts = math.ceil(duration / part_duration)

    msg = await message.reply("🚀 Processing...")
    start_time = time.time()

    name = os.path.basename(file).replace(".mp4", "").replace("_", " ")

    start = 0

    for i in range(parts):
        if start >= duration:
            break

        await premium_status(msg, i+1, parts, start_time, "✂️ Splitting")

        output = f"{DOWNLOAD_DIR}/part_{i}.mp4"

        subprocess.run([
            "ffmpeg","-y","-i",file,
            "-ss",str(start),
            "-t",str(part_duration),
            "-c","copy",output
        ])

        thumb = f"{DOWNLOAD_DIR}/thumb_{i}.jpg"
        generate_thumbnail(output, thumb, part_duration)

        caption = f"🎬 {name}\n📦 Part {i+1}/{parts}"

        await safe_send(client, message.chat.id, output, caption, thumb)

        await asyncio.sleep(2)

        os.remove(output)
        os.remove(thumb)

        start += part_duration

    os.remove(file)
    user_files.pop(uid)
    busy[uid] = False

    await msg.edit("🎉 Done!")


# ================= COMMANDS =================
@app.on_message(filters.command("split"))
async def split(client, message):
    try:
        parts = int(message.command[1])
    except:
        return await message.reply("❌ Use /split 2")

    await process_split(client, message, parts, "equal")


@app.on_message(filters.command("splitmin"))
async def splitmin(client, message):
    try:
        minutes = int(message.command[1])
    except:
        return await message.reply("❌ Use /splitmin 5")

    await process_split(client, message, minutes, "time")


@app.on_message(filters.command("splitminsmart"))
async def splitminsmart(client, message):
    try:
        minutes = int(message.command[1])
    except:
        return await message.reply("❌ Use /splitminsmart 5")

    await process_split(client, message, minutes, "smart")


# ================= RUN =================
app.run()
