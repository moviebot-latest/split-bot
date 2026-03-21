import os, time, math, random, subprocess, threading, asyncio
from http.server import BaseHTTPRequestHandler, HTTPServer
from pyrogram import Client, filters
from pyrogram.types import Message

# ✅ FIX EVENT LOOP (IMPORTANT)
asyncio.set_event_loop(asyncio.new_event_loop())

# ---------------- CONFIG ----------------
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")

app = Client("split-bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

os.makedirs("downloads", exist_ok=True)
os.makedirs("output", exist_ok=True)

user_video = {}

# ---------------- UTIL ----------------

def get_duration(file):
    cmd = f'ffprobe -i "{file}" -show_entries format=duration -v quiet -of csv="p=0"'
    return float(subprocess.getoutput(cmd))

def get_size_mb(file):
    return os.path.getsize(file) / (1024*1024)

def generate_thumb(video, out):
    dur = get_duration(video)
    t = random.randint(1, max(2, int(dur)-1))
    cmd = f'ffmpeg -ss {t} -i "{video}" -frames:v 1 "{out}" -y'
    subprocess.run(cmd, shell=True)

# ---------------- VIDEO RECEIVE ----------------

@app.on_message(filters.video)
async def video_handler(client, message: Message):
    start = time.time()
    msg = await message.reply("📥 Downloading...")

    path = await message.download("downloads/")
    user_video[message.from_user.id] = path

    size = get_size_mb(path)
    speed = size / (time.time() - start)

    await msg.edit(
        f"✅ Video saved\n💾 {size:.2f} MB\n⚡ {speed:.2f} MB/s\n\n"
        f"/split 3\n/splitmin 3\n/splitmb 200"
    )

# ---------------- PROCESS ----------------

async def process_split(message, files):
    total = len(files)

    for i, f in enumerate(files):
        thumb = f + ".jpg"
        generate_thumb(f, thumb)

        await message.reply_video(
            f,
            thumb=thumb,
            caption=f"📦 Part {i+1}/{total}"
        )

        os.remove(f)
        if os.path.exists(thumb):
            os.remove(thumb)

# ---------------- SPLIT ----------------

@app.on_message(filters.command("split"))
async def split_cmd(client, message: Message):
    if message.from_user.id not in user_video:
        return await message.reply("❌ Send video first")

    parts = int(message.command[1])
    file = user_video[message.from_user.id]

    msg = await message.reply("✂️ Splitting...")

    duration = get_duration(file)
    part_time = duration / parts
    files = []

    for i in range(parts):
        out = f"output/part_{i+1}.mp4"
        cmd = f'ffmpeg -y -ss {i*part_time} -i "{file}" -t {part_time} -c copy "{out}"'
        subprocess.run(cmd, shell=True)

        files.append(out)
        await msg.edit(f"✂️ Splitting {i+1}/{parts}")

    await msg.edit("📤 Uploading...")
    await process_split(message, files)

    os.remove(file)
    del user_video[message.from_user.id]

    await msg.edit("✅ Done")

# ---------------- SPLIT MIN ----------------

@app.on_message(filters.command("splitmin"))
async def split_min(client, message: Message):
    if message.from_user.id not in user_video:
        return await message.reply("❌ Send video first")

    minutes = float(message.command[1])
    file = user_video[message.from_user.id]

    msg = await message.reply("✂️ Splitting...")

    duration = get_duration(file)
    part_time = minutes * 60
    parts = math.ceil(duration / part_time)

    files = []

    for i in range(parts):
        out = f"output/part_{i+1}.mp4"
        cmd = f'ffmpeg -y -ss {i*part_time} -i "{file}" -t {part_time} -c copy "{out}"'
        subprocess.run(cmd, shell=True)

        files.append(out)
        await msg.edit(f"✂️ Splitting {i+1}/{parts}")

    await msg.edit("📤 Uploading...")
    await process_split(message, files)

    os.remove(file)
    del user_video[message.from_user.id]

    await msg.edit("✅ Done")

# ---------------- SPLIT MB ----------------

@app.on_message(filters.command("splitmb"))
async def split_mb(client, message: Message):
    if message.from_user.id not in user_video:
        return await message.reply("❌ Send video first")

    mb = int(message.command[1])
    file = user_video[message.from_user.id]

    size = get_size_mb(file)
    parts = math.ceil(size / mb)

    message.command = ["split", str(parts)]
    await split_cmd(client, message)

# ---------------- WEB SERVER ----------------

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Bot is running")

def run_web():
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), Handler)
    server.serve_forever()

threading.Thread(target=run_web).start()

# ---------------- START ----------------
app.run()
