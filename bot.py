import os
import time
import math
import asyncio
import subprocess
from pyrogram import Client, filters

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")

app = Client("ultra-max-bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

user_files = {}
user_queue = set()
last_update = {}

spinner = ["⏳", "🔄", "⚙️", "🚀"]

# ================= DOWNLOAD PROGRESS =================
async def progress(current, total, message, start):
    uid = message.chat.id

    if not isinstance(total, (int, float)) or total == 0:
        return

    now = time.time()
    if uid in last_update and now - last_update[uid] < 1.5:
        return

    last_update[uid] = now

    diff = now - start
    speed = current / diff if diff > 0 else 0
    eta = (total - current) / speed if speed > 0 else 0

    percent = current * 100 / total
    bar = "█" * int(percent // 5) + "░" * (20 - int(percent // 5))

    try:
        await message.edit(
            f"🚀 Downloading...\n"
            f"[{bar}] {percent:.1f}%\n"
            f"⚡ {speed/1024:.2f} KB/s\n"
            f"⏱ ETA: {int(eta)} sec"
        )
    except:
        pass

    if current >= total:
        try:
            await message.edit("✅ Download Complete!\n🚀 Processing...")
        except:
            pass


# ================= SPLIT PROGRESS =================
async def split_progress(msg, i, parts, start_time):
    now = time.time()
    diff = now - start_time

    percent = (i / parts) * 100
    bar = "█" * int(percent // 5) + "░" * (20 - int(percent // 5))

    icon = spinner[i % len(spinner)]

    try:
        await msg.edit(
            f"{icon} ✂️ Splitting Video\n\n"
            f"[{bar}] {percent:.1f}%\n\n"
            f"📦 Part {i}/{parts}\n"
            f"⏱ Time: {int(diff)} sec"
        )
    except:
        pass


# ================= START =================
@app.on_message(filters.command("start"))
async def start(client, message):
    await message.reply(
        "🔥 ULTRA MAX BOT READY\n\n"
        "/split 2\n"
        "/splitmin 5"
    )


# ================= RECEIVE =================
@app.on_message(filters.video | filters.document)
async def receive(client, message):
    if message.from_user.id in user_queue:
        return await message.reply("⏳ Wait previous task")

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
            "ffprobe","-v","error",
            "-show_entries","format=duration",
            "-of","default=noprint_wrappers=1:nokey=1",
            file
        ]).decode().strip())
    except:
        return None


# ================= NAME =================
def clean_name(file):
    return os.path.basename(file).replace(".mp4","").replace("_"," ").replace("-"," ")


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
        return await message.reply("❌ Error")

    part_time = math.ceil(duration / parts)
    msg = await message.reply("🚀 Processing...")

    name = clean_name(file)
    start_time = time.time()

    for i in range(parts):
        if i * part_time >= duration:
            break

        await split_progress(msg, i+1, parts, start_time)

        out = f"{DOWNLOAD_DIR}/part_{i}.mp4"

        subprocess.run([
            "ffmpeg","-y",
            "-ss",str(i * part_time),
            "-i",file,
            "-t",str(part_time),
            "-c","copy",
            "-preset","ultrafast",
            "-threads","2",
            out
        ])

        thumb = f"{DOWNLOAD_DIR}/thumb_{i}.jpg"
        generate_thumbnail(out, thumb, part_time)

        await message.reply_video(
            out,
            caption=f"🎬 {name}\n📦 Part {i+1}/{parts}",
            thumb=thumb
        )

        await asyncio.sleep(1)

        os.remove(out)
        os.remove(thumb)

    os.remove(file)
    user_files.pop(uid)
    user_queue.remove(uid)

    await msg.edit("🎉 Done!")


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
        return await message.reply("❌ Use /splitmin 5")

    file = user_files[uid]
    duration = get_duration(file)

    part_time = minutes * 60
    parts = math.ceil(duration / part_time)

    msg = await message.reply("🚀 Processing...")
    name = clean_name(file)
    start_time = time.time()

    for i in range(parts):
        if i * part_time >= duration:
            break

        await split_progress(msg, i+1, parts, start_time)

        out = f"{DOWNLOAD_DIR}/min_{i}.mp4"

        subprocess.run([
            "ffmpeg","-y",
            "-ss",str(i * part_time),
            "-i",file,
            "-t",str(part_time),
            "-c","copy",
            "-preset","ultrafast",
            "-threads","2",
            out
        ])

        thumb = f"{DOWNLOAD_DIR}/thumb_{i}.jpg"
        generate_thumbnail(out, thumb, part_time)

        await message.reply_video(
            out,
            caption=f"🎬 {name}\n📦 Part {i+1}/{parts}",
            thumb=thumb
        )

        await asyncio.sleep(1)

        os.remove(out)
        os.remove(thumb)

    os.remove(file)
    user_files.pop(uid)
    user_queue.remove(uid)

    await msg.edit("🎉 Done!")


# ================= RUN =================
app.run()
