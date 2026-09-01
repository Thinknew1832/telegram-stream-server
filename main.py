import os
import math
from aiohttp import web
from pyrogram import Client, filters
from pyrogram.types import Message

# Environment Configurations
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
BIN_CHANNEL = int(os.getenv("BIN_CHANNEL", "0"))
PORT = int(os.getenv("PORT", "8080"))
BIND_ADDRESS = os.getenv("BIND_ADDRESS", "0.0.0.0")
FQDN = os.getenv("FQDN", f"http://localhost:{PORT}")

bot = Client(
    "StreamBot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

# Route 1: Health check
async def handle_ping(request):
    return web.Response(text="Telegram Stream Engine is Online!")

# Route 2: Stream by Channel Message ID with Range Header support
async def handle_stream(request):
    try:
        msg_id = int(request.match_info["msg_id"])
        msg: Message = await bot.get_messages(BIN_CHANNEL, msg_id)
        
        media = msg.video or msg.document or msg.audio
        if not media:
            return web.Response(status=404, text="Media not found.")

        file_size = media.file_size
        mime_type = media.mime_type or "video/mp4"
        
        range_header = request.headers.get("Range")
        if range_header:
            byte_range = range_header.replace("bytes=", "").split("-")
            from_byte = int(byte_range[0])
            to_byte = int(byte_range[1]) if byte_range[1] else file_size - 1
        else:
            from_byte = 0
            to_byte = file_size - 1

        length = to_byte - from_byte + 1
        chunk_size = 1024 * 1024  # 1MB per chunk

        response = web.StreamResponse(
            status=206 if range_header else 200,
            headers={
                "Content-Type": mime_type,
                "Content-Range": f"bytes {from_byte}-{to_byte}/{file_size}",
                "Accept-Ranges": "bytes",
                "Content-Length": str(length),
                "Access-Control-Allow-Origin": "*",
            }
        )
        await response.prepare(request)

        # Stream MTProto file chunks directly to browser/client
        offset = int(math.floor(from_byte / chunk_size))
        async for chunk in bot.stream_media(msg, offset=offset):
            await response.write(chunk)

        return response
    except Exception as e:
        return web.Response(status=500, text=f"Streaming Error: {str(e)}")

# Telegram Bot Handler: Auto-generate stream links on file upload
@bot.on_message(filters.private & (filters.document | filters.video | filters.audio))
async def auto_forward_and_link(client: Client, message: Message):
    forwarded = await message.forward(chat_id=BIN_CHANNEL)
    stream_url = f"{FQDN}/watch/{forwarded.id}"
    
    reply_text = (
        f"**File Processed Successfully!**\n\n"
        f"**File Name:** `{message.document.file_name if message.document else 'Video'}`\n"
        f"**Stream Link:** `{stream_url}`\n\n"
        f"[Click to Stream]({stream_url})"
    )
    await message.reply_text(reply_text, disable_web_page_preview=True)

# Application startup
async def init_app():
    app = web.Application()
    app.router.add_get("/", handle_ping)
    app.router.add_get("/watch/{msg_id}", handle_stream)
    return app

if __name__ == "__main__":
    import asyncio
    loop = asyncio.get_event_loop()
    
    async def run():
        await bot.start()
        app = await init_app()
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, BIND_ADDRESS, PORT)
        await site.start()
        print(f"Server running at http://{BIND_ADDRESS}:{PORT}")
        await asyncio.Event().wait()

    loop.run_until_complete(run())
