from pyrogram import Client, filters
import os

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")

app = Client("split-bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# START COMMAND
@app.on_message(filters.command("start"))
async def start(client, message):
    await message.reply_text("🔥 Bot is working!\n\nSend video and use:\n/split 2")

# SAVE VIDEO FILE ID
user_video = {}

@app.on_message(filters.video)
async def video_handler(client, message):
    user_video[message.chat.id] = message.video.file_id
    await message.reply_text("✅ Video received!\nNow send /split <minutes>")

# SPLIT COMMAND
@app.on_message(filters.command("split"))
async def split_video(client, message):
    if message.chat.id not in user_video:
        await message.reply_text("❌ Send video first")
        return

    try:
        parts = int(message.command[1])
    except:
        await message.reply_text("Usage: /split 2")
        return

    await message.reply_text(f"⚡ Splitting into {parts} parts (demo)")

    # Demo send same video multiple times
    for i in range(parts):
        await client.send_video(
            message.chat.id,
            user_video[message.chat.id],
            caption=f"Part {i+1}"
        )

app.run()
