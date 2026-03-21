import os, time, math, asyncio, random, subprocess
from pyrogram import Client, filters
from pyrogram.types import Message

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")

app = Client("split-bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

os.makedirs("downloads", exist_ok=True)
os.makedirs("output", exist_ok=True)

user_video = {}

# -------------------- UTIL --------------------

def get_duration(file):
    cmd = f'ffprobe -i "{file}" -show_entries format=duration -v quiet -of csv="p=0"'
    return float(subprocess.getoutput(cmd))

def human_time(sec):
    return f"{int(sec)}s"

def get_size_mb(file):
    return os.path.getsize(file) / (1024*1024)

def generate_thumb(video, out):
    dur = get_duration(video)
    rand = random.randint(1, int(dur)-1)
    cmd = f'ffmpeg -ss {rand} -i "{video}" -frames:v 1 "{out}" -y'
    subprocess.run(cmd, shell=True)

# -------------------- ANIMATION --------------------

async def animate(msg, text):
    dots = ["", ".", "..", "..."]
    for _ in range(3):
        for d in dots:
            try:
                await msg.edit(text + d)
                await asyncio.sleep(1.5)
            except:
                pass

# -------------------- VIDEO RECEIVE --------------------

@app.on_message(filters.video)
async def video_handler(client, message: Message):
    start = time.time()
    msg = await message.reply("📥 Downloading...")

    path = await message.download("downloads/")
    user_video[message.from_user.id] = path

    size = get_size_mb(path)
    t = time.time() - start
    speed = size / t if t > 0 else 0

    await msg.edit(f"✅ Video saved\n💾 {size:.2f} MB\n⚡ {speed:.2f} MB/s\n\nUse:\n/split 3\n/splitmin 3\n/splitmb 200")

# -------------------- SPLIT CORE --------------------

async def process_split(message, files):
    total = len(files)

    for i, f in enumerate(files):
        thumb = f + ".jpg"
        generate_thumb(f, thumb)

        size = get_size_mb(f)

        await message.reply_video(
            f,
            thumb=thumb,
            caption=f"📦 Part {i+1}/{total}\n💾 {size:.2f} MB"
        )

        os.remove(f)
        if os.path.exists(thumb):
            os.remove(thumb)

# -------------------- SPLIT COMMAND --------------------

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
    start_time = time.time()

    for i in range(parts):
        out = f"output/part_{i+1}.mp4"
        cmd = f'ffmpeg -y -ss {i*part_time} -i "{file}" -t {part_time} -c copy "{out}"'
        subprocess.run(cmd, shell=True)

        files.append(out)
        await msg.edit(f"✂️ Splitting Part {i+1}/{parts}")

    await msg.edit("📤 Uploading...")
    await process_split(message, files)

    os.remove(file)
    del user_video[message.from_user.id]

    total_time = time.time() - start_time
    await msg.edit(f"✅ Done in {human_time(total_time)}")

# -------------------- SPLIT MIN --------------------

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

# -------------------- SPLIT MB --------------------

@app.on_message(filters.command("splitmb"))
async def split_mb(client, message: Message):
    if message.from_user.id not in user_video:
        return await message.reply("❌ Send video first")

    mb = int(message.command[1])
    file = user_video[message.from_user.id]

    msg = await message.reply("✂️ Calculating...")

    size = get_size_mb(file)
    parts = math.ceil(size / mb)

    await msg.edit(f"✂️ Splitting into {parts} parts...")
    await split_cmd(client, message)

# --------------------

app.run()
