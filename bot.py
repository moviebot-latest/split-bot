import os
import asyncio

# 🔥 FIX FOR PYTHON 3.14
asyncio.set_event_loop(asyncio.new_event_loop())

import math
import subprocess
from pyrogram import Client, filters, idle
from pyrogram.types import Message
from aiohttp import web

# ===== ENV =====
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")

# ===== BOT =====
app = Client("split-bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

os.makedirs("downloads", exist_ok=True)
os.makedirs("output", exist_ok=True)

user_video = {}

# ===== WEB SERVER =====
async def home(request):
    return web.Response(text="Bot Running 🔥")

async def start_web():
    port = int(os.environ.get("PORT", 10000))
    web_app = web.Application()
    web_app.router.add_get("/", home)
    runner = web.AppRunner(web_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()

# ===== START =====
@app.on_message(filters.command("start"))
async def start_cmd(_, msg: Message):
    await msg.reply_text("🔥 Send Video\nUse:\n/split 3\n/splitmin 3\n/splitmb 200")

# ===== SAVE VIDEO =====
@app.on_message(filters.video)
async def save_video(_, msg: Message):
    file = await msg.download("downloads/")
    user_video[msg.from_user.id] = file
    await msg.reply_text("✅ Video saved!\nNow send split command")

# ===== SPLIT =====
@app.on_message(filters.command("split"))
async def split_parts(_, msg: Message):
    if msg.from_user.id not in user_video:
        return await msg.reply_text("❌ Send video first")

    try:
        parts = int(msg.command[1])
    except:
        return await msg.reply_text("❌ Use like /split 3")

    file = user_video[msg.from_user.id]
    await msg.reply_text("✂️ Splitting...")

    duration = float(subprocess.getoutput(
        f'ffprobe -i "{file}" -show_entries format=duration -v quiet -of csv="p=0"'))

    part_time = duration / parts
    files = []

    for i in range(parts):
        out = f"output/part_{i+1}.mp4"
        subprocess.run(
            f'ffmpeg -y -ss {i*part_time} -i "{file}" -t {part_time} -c copy "{out}"',
            shell=True
        )
        files.append(out)

    await msg.reply_text("📤 Uploading...")

    for i, f in enumerate(files):
        await msg.reply_video(f, caption=f"📦 Part {i+1}/{len(files)}")
        os.remove(f)

    os.remove(file)
    del user_video[msg.from_user.id]

    await msg.reply_text("✅ Done")

# ===== MAIN =====
async def main():
    await start_web()
    await app.start()
    print("🔥 Bot Started")
    await idle()

if __name__ == "__main__":
    asyncio.run(main())
