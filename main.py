import os
import math
import sys
import json
import asyncio
import urllib.request
from aiohttp import web
from pyrogram import Client, filters, enums
from pyrogram.types import Message

# Configurations
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "").strip()
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()

raw_channel = os.getenv("BIN_CHANNEL", "").strip()
try:
    BIN_CHANNEL = int(raw_channel)
except ValueError:
    BIN_CHANNEL = raw_channel

PORT = int(os.getenv("PORT", "8080"))
BIND_ADDRESS = os.getenv("BIND_ADDRESS", "0.0.0.0")
FQDN = os.getenv("FQDN", "https://telegram-stream-server-vglf.onrender.com").rstrip("/")

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
THUMB_CACHE = {}
DEMUX_LOCK = asyncio.Semaphore(3)

async def handle_ping(request):
    return web.Response(text="AnimeToon Stream Engine Online")

async def get_channel_message(msg_id: int) -> Message:
    try:
        return await bot.get_messages(BIN_CHANNEL, msg_id)
    except Exception:
        await bot.get_chat(BIN_CHANNEL)
        return await bot.get_messages(BIN_CHANNEL, msg_id)

# Standard Range-Supported Telegram Stream
async def stream_telegram_media(msg: Message, request: web.Request):
    media = msg.video or msg.document or msg.audio
    if not media:
        return web.Response(status=404, text="Media not found.")

    file_size = media.file_size
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
        "Content-Type": "video/mp4",
        "Accept-Ranges": "bytes",
        "Content-Range": f"bytes {from_byte}-{to_byte}/{file_size}",
        "Content-Length": str(content_length),
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Range, Content-Type",
        "Cache-Control": "public, max-age=86400",
        "Connection": "keep-alive",
    }

    if request.method == "HEAD":
        return web.Response(status=200, headers=headers)

    response = web.StreamResponse(status=206 if range_header else 200, headers=headers)
    await response.prepare(request)

    offset = int(math.floor(from_byte / chunk_size))
    bytes_sent = 0

    try:
        async for chunk in bot.stream_media(msg, offset=offset):
            if bytes_sent == 0 and (from_byte % chunk_size) != 0:
                chunk = chunk[(from_byte % chunk_size):]

            if bytes_sent + len(chunk) > content_length:
                chunk = chunk[:content_length - bytes_sent]

            await response.write(chunk)
            await response.drain()
            bytes_sent += len(chunk)

            if bytes_sent >= content_length:
                break
    except (asyncio.CancelledError, ConnectionResetError):
        pass

    return response

async def handle_raw_stream(request):
    try:
        msg_id = int(request.match_info["msg_id"])
        msg = await get_channel_message(msg_id)
        return await stream_telegram_media(msg, request)
    except Exception as e:
        return web.Response(status=500, text=str(e))

# High-Efficiency Stream Handler (Handles Seeking without Resetting + No Audio Lag)
async def handle_stream(request):
    try:
        msg_id = int(request.match_info["msg_id"])
        track_id = request.query.get("track", "0")
        start_time = request.query.get("ss", "0")

        msg = await get_channel_message(msg_id)
        media = msg.video or msg.document or msg.audio
        if not media:
            return web.Response(status=404, text="Media not found.")

        file_name = (getattr(media, "file_name", "") or "").lower()
        mime_type = (media.mime_type or "").lower()

        # Direct byte-range stream for native single-audio MP4 if not seeking via ss
        if track_id == "0" and start_time == "0" and mime_type == "video/mp4" and not file_name.endswith(".mkv"):
            return await stream_telegram_media(msg, request)

        source_url = f"http://127.0.0.1:{PORT}/raw/{msg_id}"

        # Fast input seek + audio-resync to eliminate audio delay/lag
        cmd = ["ffmpeg", "-threads", "1"]
        try:
            ss_float = float(start_time)
            if ss_float > 0:
                cmd += ["-ss", str(ss_float)]
        except ValueError:
            pass

        cmd += [
            "-reconnect", "1",
            "-reconnect_streamed", "1",
            "-reconnect_delay_max", "5",
            "-i", source_url,
            "-map", "0:v:0",
            "-map", f"0:a:{track_id}?",
            "-c:v", "copy",
            "-c:a", "aac",
            "-b:a", "128k",
            "-ac", "2",
            "-af", "aresample=async=1000",
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
                "Connection": "keep-alive",
            }
        )
        await response.prepare(request)

        async with DEMUX_LOCK:
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
                    await response.drain()
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

# Probes metadata to list audio tracks + total duration
async def handle_track_info(request):
    try:
        msg_id = int(request.match_info["msg_id"])
        if msg_id in META_CACHE:
            return web.json_response(META_CACHE[msg_id], headers={"Access-Control-Allow-Origin": "*", "Cache-Control": "public, max-age=86400"})

        source_url = f"http://127.0.0.1:{PORT}/raw/{msg_id}"
        cmd = [
            "ffprobe",
            "-v", "error",
            "-probesize", "10000000",
            "-analyzeduration", "5000000",
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
                label = f"{title} ({clean_lang})" if title and "@" not in title else f"Track {audio_idx + 1} ({clean_lang})"
                tracks.append({"id": audio_idx, "title": label})
                audio_idx += 1

        if not tracks:
            tracks.append({"id": 0, "title": "Default Audio"})

        res = {"duration": duration, "tracks": tracks}
        META_CACHE[msg_id] = res
        return web.json_response(res, headers={"Access-Control-Allow-Origin": "*", "Cache-Control": "public, max-age=86400"})
    except Exception:
        return web.json_response({"duration": 1440, "tracks": [{"id": 0, "title": "Default Audio"}]}, headers={"Access-Control-Allow-Origin": "*"})

# Episode Thumbnail Generator & Server
async def handle_thumbnail(request):
    try:
        msg_id = int(request.match_info["msg_id"])
        if msg_id in THUMB_CACHE:
            return web.Response(
                body=THUMB_CACHE[msg_id],
                content_type="image/jpeg",
                headers={"Cache-Control": "public, max-age=604800", "Access-Control-Allow-Origin": "*"}
            )

        msg = await get_channel_message(msg_id)
        media = msg.video or msg.document

        # 1. Try embedded Telegram thumbnail
        if media and hasattr(media, "thumbs") and media.thumbs:
            thumb = media.thumbs[0]
            file_bytes = await bot.download_media(thumb.file_id, in_memory=True)
            body = file_bytes.getbuffer().tobytes()
            THUMB_CACHE[msg_id] = body
            return web.Response(
                body=body,
                content_type="image/jpeg",
                headers={"Cache-Control": "public, max-age=604800", "Access-Control-Allow-Origin": "*"}
            )

        # 2. Extract thumbnail via FFmpeg if Telegram has no embedded preview
        source_url = f"http://127.0.0.1:{PORT}/raw/{msg_id}"
        cmd = [
            "ffmpeg",
            "-ss", "00:01:30",
            "-i", source_url,
            "-vframes", "1",
            "-vf", "scale=320:-1",
            "-f", "image2",
            "-q:v", "4",
            "pipe:1"
        ]
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL
        )
        stdout, _ = await process.communicate()
        if stdout and len(stdout) > 500:
            THUMB_CACHE[msg_id] = stdout
            return web.Response(
                body=stdout,
                content_type="image/jpeg",
                headers={"Cache-Control": "public, max-age=604800", "Access-Control-Allow-Origin": "*"}
            )
    except Exception:
        pass

    fallback_svg = '<svg xmlns="http://www.w3.org/2000/svg" width="160" height="90" viewBox="0 0 160 90"><rect width="160" height="90" fill="#181818"/><circle cx="80" cy="45" r="16" fill="#282828"/><polygon points="76,37 88,45 76,53" fill="#E50914"/></svg>'
    return web.Response(text=fallback_svg, content_type="image/svg+xml", headers={"Access-Control-Allow-Origin": "*", "Cache-Control": "public, max-age=86400"})

# Telegram Bot File Assistant
@bot.on_message(filters.private)
async def bot_file_handler(client: Client, message: Message):
    try:
        if message.text and message.text.startswith("/start"):
            await message.reply_text(
                "👋 <b>AnimeToon Bot is Online!</b>\n\nForward or upload any video/MKV file here to get your stream link.",
                parse_mode=enums.ParseMode.HTML
            )
            return

        media = message.video or message.document or message.audio
        if not media:
            return

        try:
            await client.get_chat(BIN_CHANNEL)
        except Exception:
            pass

        forwarded = await message.forward(chat_id=BIN_CHANNEL)
        name = getattr(media, "file_name", None) or "Anime_Episode.mkv"

        reply_text = (
            f"🎬 <b>File Processed Successfully!</b>\n\n"
            f"<b>File Name:</b> <code>{name}</code>\n"
            f"<b>Message ID (msg_id):</b> <code>{forwarded.id}</code>\n"
            f"<b>Direct Stream:</b> <code>{FQDN}/watch/{forwarded.id}</code>\n\n"
            f"<i>Paste <code>{forwarded.id}</code> into Column K (msg_id) of your Google Sheet.</i>"
        )
        await message.reply_text(reply_text, parse_mode=enums.ParseMode.HTML)
    except Exception as e:
        print(f"[BOT ERROR] {e}")
        await message.reply_text(f"⚠️ <b>Processing error:</b> <code>{str(e)}</code>", parse_mode=enums.ParseMode.HTML)

# Internal Self-Ping
async def keep_alive_worker():
    await asyncio.sleep(20)
    url = f"http://127.0.0.1:{PORT}/"
    while True:
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, lambda: urllib.request.urlopen(url, timeout=10))
            print("[KEEP-ALIVE] Ping successful.")
        except Exception as e:
            print(f"[KEEP-ALIVE WARNING] {e}")
        await asyncio.sleep(240)

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

        try:
            chat = await bot.get_chat(BIN_CHANNEL)
            print(f"[INIT] Channel cached: {chat.title} ({chat.id})")
        except Exception as e:
            print(f"[INIT ERROR] {e}")

        app = await init_app()
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, BIND_ADDRESS, PORT)
        await site.start()
        print(f"Server is listening on port {PORT}")

        asyncio.create_task(keep_alive_worker())
        await asyncio.Event().wait()

    loop.run_until_complete(run())
