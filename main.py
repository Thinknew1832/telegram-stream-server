import os
import math
import sys
import json
import asyncio
from aiohttp import web
from pyrogram import Client, filters, enums
from pyrogram.types import Message

# Configurations
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "").strip()
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
BIN_CHANNEL = int(os.getenv("BIN_CHANNEL", "0"))
PORT = int(os.getenv("PORT", "8080"))
BIND_ADDRESS = os.getenv("BIND_ADDRESS", "0.0.0.0")
FQDN = os.getenv("FQDN", f"http://localhost:{PORT}").rstrip("/")

if not API_ID or not API_HASH or not BOT_TOKEN or not BIN_CHANNEL:
    print("[CRITICAL ERROR] Missing required environment variables.")
    sys.exit(1)

bot = Client(
    "StreamBot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    in_memory=True
)

LANG_MAP = {
    "jpn": "Japanese",
    "eng": "English",
    "hin": "Hindi",
    "tel": "Telugu",
    "tam": "Tamil",
    "mal": "Malayalam",
    "kan": "Kannada",
    "kor": "Korean",
    "spa": "Spanish",
    "fra": "French",
    "ger": "German",
    "und": "Default"
}

META_CACHE = {}

async def handle_ping(request):
    return web.Response(text="AnimeToon Stream Engine Online")

# Raw byte streaming directly out of Telegram
async def stream_telegram_media(msg: Message, request: web.Request):
    media = msg.video or msg.document or msg.audio
    if not media:
        return web.Response(status=404, text="Media not found.")

    file_size = media.file_size
    mime_type = media.mime_type or "video/mp4"

    range_header = request.headers.get("Range")
    if range_header:
        byte_range = range_header.replace("bytes=", "").split("-")
        from_byte = int(byte_range[0])
        to_byte = int(byte_range[1]) if len(byte_range) > 1 and byte_range[1] else file_size - 1
    else:
        from_byte = 0
        to_byte = file_size - 1

    content_length = to_byte - from_byte + 1
    chunk_size = 1024 * 1024

    headers = {
        "Content-Type": mime_type,
        "Content-Range": f"bytes {from_byte}-{to_byte}/{file_size}",
        "Accept-Ranges": "bytes",
        "Content-Length": str(content_length),
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Range, Content-Type",
    }

    if request.method == "HEAD":
        return web.Response(status=200, headers=headers)

    response = web.StreamResponse(status=206 if range_header else 200, headers=headers)
    await response.prepare(request)

    offset = int(math.floor(from_byte / chunk_size))
    bytes_sent = 0

    async for chunk in bot.stream_media(msg, offset=offset):
        if bytes_sent == 0 and (from_byte % chunk_size) != 0:
            chunk = chunk[(from_byte % chunk_size):]

        if bytes_sent + len(chunk) > content_length:
            chunk = chunk[:content_length - bytes_sent]

        await response.write(chunk)
        bytes_sent += len(chunk)

        if bytes_sent >= content_length:
            break

    return response

# Internal raw route for FFmpeg
async def handle_raw_stream(request):
    try:
        msg_id = int(request.match_info["msg_id"])
        msg: Message = await bot.get_messages(BIN_CHANNEL, msg_id)
        return await stream_telegram_media(msg, request)
    except Exception as e:
        return web.Response(status=500, text=str(e))

# Primary stream handler (MKV auto-remuxing to fragmented MP4)
async def handle_stream(request):
    try:
        msg_id = int(request.match_info["msg_id"])
        track_id = request.query.get("track", "0")
        start_time = request.query.get("ss", "0")

        msg: Message = await bot.get_messages(BIN_CHANNEL, msg_id)
        media = msg.video or msg.document or msg.audio
        if not media:
            return web.Response(status=404, text="Media not found.")

        file_name = (getattr(media, "file_name", "") or "").lower()
        mime_type = (media.mime_type or "").lower()

        # Direct streaming for native MP4s when no track/seek modifications are requested
        if mime_type == "video/mp4" and not file_name.endswith(".mkv") and track_id == "0" and start_time == "0":
            return await stream_telegram_media(msg, request)

        source_url = f"http://127.0.0.1:{PORT}/raw/{msg_id}"

        # Build FFmpeg command with seek support and browser-safe streaming
        cmd = ["ffmpeg"]
        if start_time != "0":
            cmd += ["-ss", str(start_time)]
        cmd += [
            "-i", source_url,
            "-map", "0:v:0",
            "-map", f"0:a:{track_id}?",
            "-c:v", "copy",
            "-c:a", "aac",
            "-b:a", "160k",
            "-movflags", "frag_keyframe+empty_moov+default_base_moof",
            "-f", "mp4",
            "pipe:1"
        ]

        response = web.StreamResponse(
            status=200,
            headers={
                "Content-Type": "video/mp4",
                "Access-Control-Allow-Origin": "*",
                "Cache-Control": "no-cache",
            }
        )
        await response.prepare(request)

        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL
        )

        try:
            while True:
                chunk = await process.stdout.read(64 * 1024)
                if not chunk:
                    break
                await response.write(chunk)
        except (asyncio.CancelledError, ConnectionResetError):
            pass
        finally:
            if process.returncode is None:
                try:
                    process.kill()
                    await process.wait()
                except Exception:
                    pass

        return response
    except Exception as e:
        return web.Response(status=500, text=f"Streaming Error: {str(e)}")

# Get Audio Tracks for UI Selector
async def handle_track_info(request):
    try:
        msg_id = int(request.match_info["msg_id"])
        if msg_id in META_CACHE:
            return web.json_response(META_CACHE[msg_id], headers={"Access-Control-Allow-Origin": "*"})

        source_url = f"http://127.0.0.1:{PORT}/raw/{msg_id}"
        cmd = [
            "ffprobe",
            "-v", "error",
            "-probesize", "5000000",
            "-analyzeduration", "3000000",
            "-show_entries", "stream=index,codec_type:stream_tags=language,title:format=duration",
            "-of", "json",
            source_url
        ]

        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await process.communicate()
        data = json.loads(stdout.decode())

        duration = float(data.get("format", {}).get("duration", 0))
        tracks = []
        audio_idx = 0

        for stream in data.get("streams", []):
            if stream.get("codec_type") == "audio":
                tags = stream.get("tags", {})
                raw_lang = tags.get("language", "und").lower()
                clean_lang = LANG_MAP.get(raw_lang, raw_lang.capitalize())
                title = tags.get("title", "")
                label = f"{title} ({clean_lang})" if title and "@" not in title else f"Track {audio_idx + 1} - {clean_lang}"
                tracks.append({"id": audio_idx, "title": label})
                audio_idx += 1

        if not tracks:
            tracks.append({"id": 0, "title": "Default Audio"})

        res = {"duration": duration, "tracks": tracks}
        META_CACHE[msg_id] = res
        return web.json_response(res, headers={"Access-Control-Allow-Origin": "*"})
    except Exception:
        return web.json_response({"duration": 0, "tracks": [{"id": 0, "title": "Default Audio"}]}, headers={"Access-Control-Allow-Origin": "*"})

# Telegram Thumbnail Server
async def handle_thumbnail(request):
    try:
        msg_id = int(request.match_info["msg_id"])
        msg: Message = await bot.get_messages(BIN_CHANNEL, msg_id)
        media = msg.video or msg.document

        if media and hasattr(media, "thumbs") and media.thumbs:
            thumb = media.thumbs[0]
            file_bytes = await bot.download_media(thumb.file_id, in_memory=True)
            return web.Response(
                body=file_bytes.getbuffer(),
                content_type="image/jpeg",
                headers={
                    "Cache-Control": "public, max-age=604800",
                    "Access-Control-Allow-Origin": "*"
                }
            )
    except Exception:
        pass

    fallback_svg = '<svg xmlns="http://www.w3.org/2000/svg" width="160" height="90" viewBox="0 0 160 90"><rect width="160" height="90" fill="#141414"/><text x="50%" y="50%" fill="#555" font-family="sans-serif" font-size="12" text-anchor="middle" dy=".3em">Episode</text></svg>'
    return web.Response(text=fallback_svg, content_type="image/svg+xml", headers={"Access-Control-Allow-Origin": "*"})

# Bot Assistant Handler
@bot.on_message(filters.private & (filters.document | filters.video | filters.audio))
async def bot_file_handler(client: Client, message: Message):
    try:
        forwarded = await message.forward(chat_id=BIN_CHANNEL)
        name = "Unknown File"
        if message.document and message.document.file_name:
            name = message.document.file_name
        elif message.video and message.video.file_name:
            name = message.video.file_name

        reply_text = (
            f"<b>File Processed Successfully!</b>\n\n"
            f"<b>File Name:</b> <code>{name}</code>\n"
            f"<b>Message ID (msg_id):</b> <code>{forwarded.id}</code>\n"
            f"<b>Direct Stream:</b> <code>{FQDN}/watch/{forwarded.id}</code>\n\n"
            f"<i>Put <code>{forwarded.id}</code> in Column K of your Google Sheet.</i>"
        )
        await message.reply_text(reply_text, parse_mode=enums.ParseMode.HTML)
    except Exception as e:
        await message.reply_text(f"Processing error: {str(e)}")

async def init_app():
    app = web.Application()
    app.router.add_get("/", handle_ping)
    app.router.add_get("/watch/{msg_id}", handle_stream)
    app.router.add_get("/raw/{msg_id}", handle_raw_stream)
    app.router.add_get("/thumb/{msg_id}", handle_thumbnail)
    app.router.add_get("/api/tracks/{msg_id}", handle_track_info)
    return app

if __name__ == "__main__":
    loop = asyncio.get_event_loop()

    async def run():
        await bot.start()
        app = await init_app()
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, BIND_ADDRESS, PORT)
        await site.start()
        print(f"Server is listening on port {PORT}")
        await asyncio.Event().wait()

    loop.run_until_complete(run())
