import os
import math
import sys
import json
import asyncio
from aiohttp import web
from pyrogram import Client, filters, enums
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton

# Environment Configurations
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "").strip()
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
BIN_CHANNEL = int(os.getenv("BIN_CHANNEL", "0"))
PORT = int(os.getenv("PORT", "8080"))
BIND_ADDRESS = os.getenv("BIND_ADDRESS", "0.0.0.0")
FQDN = os.getenv("FQDN", f"http://localhost:{PORT}").rstrip("/")

if not API_ID or not API_HASH or not BOT_TOKEN or not BIN_CHANNEL:
    print("\n[CRITICAL ERROR] Missing mandatory environment variables!\n")
    sys.exit(1)

bot = Client(
    "StreamBot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    in_memory=True
)

async def handle_ping(request):
    return web.Response(text="Telegram Stream Engine is Online!")

# Direct Telegram Byte Stream (Default)
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
            to_byte = int(byte_range[1]) if len(byte_range) > 1 and byte_range[1] else file_size - 1
        else:
            from_byte = 0
            to_byte = file_size - 1

        content_length = to_byte - from_byte + 1
        chunk_size = 1024 * 1024  # 1MB buffer chunk

        headers = {
            "Content-Type": mime_type,
            "Content-Range": f"bytes {from_byte}-{to_byte}/{file_size}",
            "Accept-Ranges": "bytes",
            "Content-Length": str(content_length),
            "Access-Control-Allow-Origin": "*",
        }

        if request.method == "HEAD":
            return web.Response(status=200, headers=headers)

        response = web.StreamResponse(
            status=206 if range_header else 200,
            headers=headers
        )
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
    except Exception as e:
        return web.Response(status=500, text=f"Streaming Error: {str(e)}")

# API: Fast probe of audio streams
async def handle_track_info(request):
    try:
        msg_id = int(request.match_info["msg_id"])
        source_url = f"http://127.0.0.1:{PORT}/watch/{msg_id}"

        cmd = [
            "ffprobe",
            "-v", "error",
            "-probesize", "5000000",
            "-analyzeduration", "2000000",
            "-show_entries", "stream=index,codec_name,codec_type:stream_tags=language,title",
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

        tracks = []
        audio_idx = 0
        for stream in data.get("streams", []):
            if stream.get("codec_type") == "audio":
                tags = stream.get("tags", {})
                title = tags.get("title") or tags.get("language") or f"Audio Track {audio_idx + 1}"
                lang = tags.get("language", "und").upper()
                tracks.append({
                    "id": audio_idx,
                    "title": f"{title} [{lang}]" if lang != "UND" else title
                })
                audio_idx += 1

        return web.json_response({"tracks": tracks}, headers={"Access-Control-Allow-Origin": "*"})
    except Exception:
        return web.json_response({"tracks": []})

# High-Speed Remux Stream (Zero Transcoding Latency)
async def handle_remux_stream(request):
    try:
        msg_id = int(request.match_info["msg_id"])
        track_id = int(request.match_info["track_id"])
        source_url = f"http://127.0.0.1:{PORT}/watch/{msg_id}"
        seek_time = request.query.get("ss", "0")

        # Ultra-fast pipeline: Accurate fast seek + stream copy
        cmd = [
            "ffmpeg",
            "-ss", str(seek_time),
            "-i", source_url,
            "-map", "0:v:0",
            "-map", f"0:a:{track_id}",
            "-c:v", "copy",
            "-c:a", "copy",
            "-movflags", "frag_keyframe+empty_moov+default_base_moof+faststart",
            "-f", "mp4",
            "pipe:1"
        ]

        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL
        )

        response = web.StreamResponse(
            status=200,
            headers={
                "Content-Type": "video/mp4",
                "Access-Control-Allow-Origin": "*",
                "Cache-Control": "no-cache"
            }
        )
        await response.prepare(request)

        while True:
            chunk = await process.stdout.read(256 * 1024)
            if not chunk:
                break
            await response.write(chunk)

        await process.wait()
        return response
    except Exception as e:
        return web.Response(status=500, text=str(e))

# Route: Web Player
async def handle_player(request):
    msg_id = request.match_info["msg_id"]
    stream_url = f"{FQDN}/watch/{msg_id}"
    track_info_url = f"{FQDN}/api/tracks/{msg_id}"

    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Anime Stream Player</title>
        <script src="https://cdn.jsdelivr.net/npm/artplayer/dist/artplayer.js"></script>
        <style>
            * {{ box-sizing: border-box; margin: 0; padding: 0; }}
            body {{
                background-color: #080a10;
                color: #ffffff;
                display: flex;
                flex-direction: column;
                align-items: center;
                justify-content: center;
                min-height: 100vh;
                font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
                padding: 12px;
            }}
            .player-wrapper {{
                width: 100%;
                max-width: 1050px;
            }}
            .artplayer-app {{
                width: 100%;
                aspect-ratio: 16 / 9;
                border-radius: 10px;
                overflow: hidden;
                box-shadow: 0 10px 40px rgba(0,0,0,0.85);
            }}
            .status-bar {{
                margin-top: 10px;
                font-size: 13px;
                color: #38bdf8;
                text-align: right;
            }}
        </style>
    </head>
    <body>
        <div class="player-wrapper">
            <div class="artplayer-app"></div>
            <div class="status-bar" id="status-text">Loading stream info...</div>
        </div>

        <script>
            let currentTrack = 0;
            const originalUrl = '{stream_url}';

            const art = new Artplayer({{
                container: '.artplayer-app',
                url: originalUrl,
                volume: 0.8,
                isLive: false,
                autoplay: false,
                pip: true,
                screenshot: true,
                setting: true,
                playbackRate: true,
                aspectRatio: true,
                fullscreen: true,
                fullscreenWeb: true,
                playsInline: true,
                theme: '#38bdf8'
            }});

            fetch('{track_info_url}')
                .then(res => res.json())
                .then(data => {{
                    const tracks = data.tracks || [];
                    const statusText = document.getElementById('status-text');

                    if (tracks.length > 0) {{
                        statusText.innerText = `${{tracks.length}} Audio Track(s) Available`;

                        const selectorList = tracks.map((track, idx) => ({{
                            html: track.title,
                            value: track.id,
                            default: idx === 0
                        }}));

                        art.setting.add({{
                            width: 240,
                            html: 'Audio Track',
                            tooltip: tracks[0].title,
                            selector: selectorList,
                            onSelect: function (item) {{
                                if (item.value !== currentTrack) {{
                                    currentTrack = item.value;
                                    switchAudioStream(item.value);
                                }}
                                return item.html;
                            }}
                        }});
                    }} else {{
                        statusText.innerText = 'Single Audio Stream';
                    }}
                }});

            function switchAudioStream(trackId) {{
                const currentTime = Math.floor(art.currentTime);
                art.notice.show = 'Switching audio track...';

                if (trackId === 0) {{
                    art.switchUrl(originalUrl).then(() => {{
                        art.currentTime = currentTime;
                        art.play();
                    }});
                }} else {{
                    const remuxUrl = `{FQDN}/remux/{msg_id}/${{trackId}}?ss=${{currentTime}}`;
                    art.switchUrl(remuxUrl).then(() => {{
                        art.play();
                    }});
                }}
            }}
        </script>
    </body>
    </html>
    """
    return web.Response(text=html_content, content_type="text/html")

# Telegram Command: /start
@bot.on_message(filters.command("start") & filters.private)
async def start_handler(client: Client, message: Message):
    await message.reply_text(
        "<b>Anime Stream Engine is Online!</b>\n\n"
        "Send or forward any anime video/file to get an instant streaming player link with multi-audio support.",
        parse_mode=enums.ParseMode.HTML
    )

# Telegram Media Handler
@bot.on_message(filters.private & (filters.document | filters.video | filters.audio))
async def auto_forward_and_link(client: Client, message: Message):
    try:
        forwarded = await message.forward(chat_id=BIN_CHANNEL)
        file_name = "Anime Episode"
        if message.document and message.document.file_name:
            file_name = message.document.file_name
        elif message.video and message.video.file_name:
            file_name = message.video.file_name

        player_url = f"{FQDN}/player/{forwarded.id}"

        reply_text = (
            f"<b>File Processed Successfully!</b>\n\n"
            f"<b>Title:</b> <code>{file_name}</code>\n"
            f"<b>Web Player:</b> <code>{player_url}</code>"
        )

        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("Watch Online (Multi-Audio)", url=player_url)]
        ])

        await message.reply_text(
            reply_text,
            parse_mode=enums.ParseMode.HTML,
            reply_markup=keyboard,
            disable_web_page_preview=True
        )
    except Exception as e:
        await message.reply_text(f"Error processing file: {str(e)}")

# Web App Initialization
async def init_app():
    app = web.Application()
    app.router.add_get("/", handle_ping)
    app.router.add_get("/watch/{msg_id}", handle_stream)
    app.router.add_get("/api/tracks/{msg_id}", handle_track_info)
    app.router.add_get("/remux/{msg_id}/{track_id}", handle_remux_stream)
    app.router.add_get("/player/{msg_id}", handle_player)
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
        print(f"Streaming server live at http://{BIND_ADDRESS}:{PORT}")
        await asyncio.Event().wait()

    loop.run_until_complete(run())
