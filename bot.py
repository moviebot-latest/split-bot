import asyncio, os, subprocess, math, time, random
from pyrogram import Client, filters
from aiohttp import web

# ===== ENV =====
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")

app = Client("split-bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# ===== WEB =====
async def handle(request):
    return web.Response(text="Bot Running 🚀")

async def start_web():
    app_web = web.Application()
    app_web.router.add_get("/", handle)
    port = int(os.environ.get("PORT", 8080))
    runner = web.AppRunner(app_web)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()

# ===== UTILS =====
def get_duration(file):
    return float(subprocess.getoutput(
        f'ffprobe -i "{file}" -show_entries format=duration -v quiet -of csv="p=0"'
    ))

def get_size(file):
    return os.path.getsize(file) / (1024*1024)

def gen_thumb(video, out):
    t = random.randint(1, max(2, int(get_duration(video)-1)))
    subprocess.run(f'ffmpeg -ss {t} -i "{video}" -frames:v 1 "{out}" -y', shell=True)

# ===== START =====
@app.on_message(filters.command("start"))
async def start_cmd(_, msg):
    await msg.reply_text(
        "🔥 ULTRA SPLIT BOT\n\n"
        "/split 3\n/splitmin 3\n/splitmb 100"
    )

# ===== CORE SPLIT =====
async def split_video(message, file_path, parts=None, minutes=None, size_mb=None):
    start_time = time.time()
    msg = await message.reply("📥 Processing...")

    duration = get_duration(file_path)
    size = get_size(file_path)

    if minutes:
        part_time = minutes * 60
        parts = math.ceil(duration / part_time)
    elif size_mb:
        parts = math.ceil(size / size_mb)
        part_time = duration / parts
    else:
        part_time = duration / parts

    files = []

    # ===== SPLIT =====
    for i in range(parts):
        await msg.edit(f"✂️ Splitting Part {i+1}/{parts}...")

        out = f"video_part_{i+1}.mp4"

        subprocess.run(
            f'ffmpeg -y -ss {i*part_time} -i "{file_path}" -t {part_time} -c copy "{out}"',
            shell=True
        )

        files.append(out)

    # ===== SORT (IMPORTANT) =====
    files.sort(key=lambda x: int(x.split("_")[-1].split(".")[0]))

    await msg.edit("📤 Uploading in order...")

    # ===== UPLOAD =====
    for i, f in enumerate(files):
        thumb = f + ".jpg"
        gen_thumb(f, thumb)

        caption = f"📦 Part {i+1}/{len(files)}"
        if i == len(files)-1:
            caption += "\n✅ Last Part"

        await message.reply_video(
            f,
            thumb=thumb,
            caption=caption
        )

        await asyncio.sleep(1)  # 🔥 ORDER FIX

        os.remove(f)
        if os.path.exists(thumb):
            os.remove(thumb)

    total_time = time.time() - start_time
    speed = size / total_time if total_time > 0 else 0

    os.remove(file_path)

    await msg.edit(
        f"✅ Done\n⏱ {int(total_time)}s\n⚡ {speed:.2f} MB/s"
    )

# ===== COMMANDS =====
@app.on_message(filters.command("split"))
async def split_cmd(_, msg):
    if not msg.reply_to_message:
        return await msg.reply("❌ Reply to video")

    parts = int(msg.command[1])
    file = await msg.reply_to_message.download()
    await split_video(msg, file, parts=parts)

@app.on_message(filters.command("splitmin"))
async def splitmin_cmd(_, msg):
    if not msg.reply_to_message:
        return await msg.reply("❌ Reply to video")

    minutes = float(msg.command[1])
    file = await msg.reply_to_message.download()
    await split_video(msg, file, minutes=minutes)

@app.on_message(filters.command("splitmb"))
async def splitmb_cmd(_, msg):
    if not msg.reply_to_message:
        return await msg.reply("❌ Reply to video")

    size_mb = int(msg.command[1])
    file = await msg.reply_to_message.download()
    await split_video(msg, file, size_mb=size_mb)

# ===== MAIN =====
async def main():
    await start_web()
    await app.start()
    print("🔥 Ultra Ordered Bot Started")

    while True:
        await asyncio.sleep(999)

if __name__ == "__main__":
    asyncio.run(main())
