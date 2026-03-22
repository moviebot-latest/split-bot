import os, time, math, asyncio, subprocess, gc
from pyrogram import Client, filters

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")

app = Client("god-bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

user_files = {}
task_queue = asyncio.Queue()
busy_users = {}
cancel_users = set()
cooldown = {}

USE_THUMB = False  # 🔥 Railway safe (True = heavy)

# ================= SAFE EDIT =================
async def safe_edit(msg, text):
    try:
        await msg.edit(text)
    except:
        pass

# ================= WORKER =================
async def worker():
    while True:
        func, args = await task_queue.get()
        try:
            await func(*args)
        except Exception as e:
            print("ERROR:", e)
        task_queue.task_done()

@app.on_message(filters.command("start"))
async def start(client, message):
    await message.reply("🔥 GOD MODE BOT READY\n/send video → /splitmin 4")

# ================= RECEIVE =================
@app.on_message(filters.video | filters.document)
async def receive(client, message):
    uid = message.from_user.id

    if uid in cooldown and time.time() - cooldown[uid] < 10:
        return await message.reply("⏳ Wait...")

    cooldown[uid] = time.time()

    msg = await message.reply("📥 Downloading...")
    file = await message.download(f"{DOWNLOAD_DIR}/{uid}.mp4")

    user_files[uid] = file
    await msg.edit("✅ Download done\n👉 /splitmin 4")

# ================= SPLITMIN ENTRY =================
@app.on_message(filters.command("splitmin"))
async def splitmin(client, message):
    uid = message.from_user.id

    if uid not in user_files:
        return await message.reply("❌ Send video first")

    await message.reply("⏳ Added to queue...")

    await task_queue.put((split_process, (client, message)))

# ================= MAIN SPLIT PROCESS =================
async def split_process(client, message):
    uid = message.from_user.id

    if busy_users.get(uid):
        return

    busy_users[uid] = True

    try:
        minutes = int(message.command[1])
    except:
        busy_users.pop(uid, None)
        return await message.reply("❌ Use /splitmin 4")

    file = user_files[uid]

    duration = float(subprocess.check_output([
        "ffprobe","-v","error",
        "-show_entries","format=duration",
        "-of","default=noprint_wrappers=1:nokey=1",
        file
    ]).decode().strip())

    part_time = minutes * 60

    # 🔥 SMART SPLIT
    if duration % part_time < 10:
        parts = int(duration // part_time)
    else:
        parts = math.ceil(duration / part_time)

    msg = await message.reply("🚀 Processing...")

    for i in range(parts):

        if uid in cancel_users:
            cancel_users.remove(uid)
            break

        start = i * part_time

        if start >= duration:
            break

        await safe_edit(msg, f"✂️ Part {i+1}/{parts}")

        out = f"{DOWNLOAD_DIR}/{uid}_{i}.mp4"

        await asyncio.to_thread(
            subprocess.run,
            [
                "ffmpeg","-y",
                "-ss",str(start),
                "-i",file,
                "-t",str(part_time),
                "-c","copy",
                "-preset","ultrafast",
                "-threads","1",
                out
            ]
        )

        try:
            await message.reply_video(
                out,
                caption=f"📦 Part {i+1}/{parts}"
            )
        except:
            pass

        await asyncio.sleep(0.5)

        os.remove(out)
        gc.collect()

    os.remove(file)
    user_files.pop(uid, None)
    busy_users.pop(uid, None)

    await safe_edit(msg, "🎉 Done!")

# ================= CANCEL =================
@app.on_message(filters.command("cancel"))
async def cancel(client, message):
    cancel_users.add(message.from_user.id)
    await message.reply("❌ Cancelled")

# ================= START WORKER =================
async def main():
    asyncio.create_task(worker())
    await app.start()
    print("🚀 BOT RUNNING")
    await asyncio.Event().wait()

asyncio.run(main())
