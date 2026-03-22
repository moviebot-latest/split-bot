import os
import time
import math
import asyncio
import subprocess
from pyrogram import Client, filters

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")

app = Client("ultra-final-bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

user_files = {}
busy_users = {}
cancel_users = set()
last_update = {}

spinner = ["⏳","🔄","⚙️","🚀"]

# ================= SAFE EDIT =================
async def safe_edit(msg, text):
    try:
        await msg.edit(text)
    except:
        pass


# ================= DOWNLOAD PROGRESS =================
async def progress(current, total, message, start):
    uid = message.chat.id
    now = time.time()

    if uid in last_update and now - last_update[uid] < 1.5:
        return

    last_update[uid] = now

    percent = (current / total) * 100 if total else 0
    bar = "█" * int(percent // 5) + "░" * (20 - int(percent // 5))

    diff = now - start
    speed = current / diff if diff > 0 else 0

    try:
        await message.edit(
            f"🚀 Downloading...\n"
            f"[{bar}] {percent:.1f}%\n"
            f"⚡ {speed/1024:.2f} KB/s"
        )
    except:
        pass

    if current >= total:
        await safe_edit(message, "✅ Download Complete!\n🚀 Processing...")


# ================= START =================
@app.on_message(filters.command("start"))
async def start(client, message):
    await message.reply("🔥 ULTRA FINAL BOT READY\n\n/send video → /split 2")


# ================= CANCEL =================
@app.on_message(filters.command("cancel"))
async def cancel(client, message):
    cancel_users.add(message.from_user.id)
    await message.reply("❌ Process cancelled")


# ================= RECEIVE =================
@app.on_message(filters.video | filters.document)
async def receive(client, message):
    uid = message.from_user.id

    if busy_users.get(uid):
        return await message.reply("⏳ Already processing")

    busy_users[uid] = True

    status = await message.reply("📥 Downloading...")
    start = time.time()

    file_path = await message.download(
        file_name=f"{DOWNLOAD_DIR}/video_{message.id}.mp4",
        progress=progress,
        progress_args=(status, start)
    )

    user_files[uid] = file_path
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

    if uid not in user_files:
        return await message.reply("❌ Send video first")

    try:
        parts = int(message.command[1])
    except:
        return await message.reply("❌ Use /split 2")

    file = user_files[uid]
    duration = get_duration(file)

    if not duration:
        return await message.reply("❌ Error")

    part_time = math.ceil(duration / parts)
    msg = await message.reply("🚀 Processing...")

    name = clean_name(file)
    start_time = time.time()

    for i in range(parts):
        if uid in cancel_users:
            cancel_users.remove(uid)
            break

        if i * part_time >= duration:
            break

        icon = spinner[i % 4]

        await safe_edit(
            msg,
            f"{icon} Splitting...\n📦 Part {i+1}/{parts}"
        )

        out = f"{DOWNLOAD_DIR}/part_{i}.mp4"

        try:
            await asyncio.to_thread(
                subprocess.run,
                [
                    "ffmpeg","-y",
                    "-ss",str(i * part_time),
                    "-i",file,
                    "-t",str(part_time),
                    "-c","copy",
                    "-preset","ultrafast",
                    "-threads","2",
                    out
                ]
            )
        except:
            await message.reply("❌ ffmpeg error")
            continue

        thumb = f"{DOWNLOAD_DIR}/thumb_{i}.jpg"
        await asyncio.to_thread(generate_thumbnail, out, thumb, part_time)

        try:
            await message.reply_video(
                out,
                caption=f"🎬 {name}\n📦 Part {i+1}/{parts}",
                thumb=thumb
            )
        except:
            pass

        await asyncio.sleep(0.5)

        os.remove(out)
        os.remove(thumb)

    os.remove(file)
    user_files.pop(uid, None)
    busy_users.pop(uid, None)

    await safe_edit(msg, "🎉 Done!\n📦 All parts sent")


# ================= RUN =================
app.run()
