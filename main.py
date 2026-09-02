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
    return web.Response(text="Telegram Stream Engine is Online!")

# 1. Native High-Speed Video Stream (Full Byte-Range, Instant Seeking)
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
        chunk_size = 1024 * 1024  # 1MB chunks for smooth mobile buffering

        headers = {
            "Content-Type": mime_type,
            "Content-Range": f"bytes {from_byte}-{to_byte}/{file_size}",
            "Accept-Ranges": "bytes",
            "Content-Length": str(content_length),
            "Access-Control-Allow-Origin": "*",
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
    except Exception as e:
        return web.Response(status=500, text=f"Streaming Error: {str(e)}")

# 2. Extract Audio Stream Metadata
async def handle_track_info(request):
    msg_id = int(request.match_info["msg_id"])
    if msg_id in META_CACHE:
        return web.json_response(META_CACHE[msg_id], headers={"Access-Control-Allow-Origin": "*"})

    source_url = f"http://127.0.0.1:{PORT}/watch/{msg_id}"
    try:
        cmd = [
            "ffprobe",
            "-v", "error",
            "-probesize", "3000000",
            "-analyzeduration", "1500000",
            "-show_entries", "stream=index,codec_name,codec_type:stream_tags=language,title",
            "-of", "json",
            source_url
        ]
        process = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, _ = await process.communicate()
        data = json.loads(stdout.decode())

        tracks = []
        audio_idx = 0
        for stream in data.get("streams", []):
            if stream.get("codec_type") == "audio":
                tags = stream.get("tags", {})
                raw_lang = tags.get("language", "und").lower()
                clean_lang = LANG_MAP.get(raw_lang, raw_lang.capitalize())
                title = tags.get("title", "")
                if "@" in title or not title:
                    title = f"Track {audio_idx + 1}: {clean_lang}"
                else:
                    title = f"{title} ({clean_lang})"

                tracks.append({"id": audio_idx, "title": title})
                audio_idx += 1

        res = {"tracks": tracks}
        META_CACHE[msg_id] = res
        return web.json_response(res, headers={"Access-Control-Allow-Origin": "*"})
    except Exception:
        return web.json_response({"tracks": []})

# 3. Pure Audio Demux Stream (Near 0% CPU, Immediate Delivery)
async def handle_audio_stream(request):
    msg_id = int(request.match_info["msg_id"])
    track_id = int(request.match_info["track_id"])
    seek_time = request.query.get("ss", "0")
    source_url = f"http://127.0.0.1:{PORT}/watch/{msg_id}"

    cmd = [
        "ffmpeg",
        "-ss", str(seek_time),
        "-i", source_url,
        "-map", f"0:a:{track_id}",
        "-c:a", "aac",
        "-b:a", "160k",
        "-f", "adts",
        "pipe:1"
    ]

    response = web.StreamResponse(
        status=200,
        headers={
            "Content-Type": "audio/aac",
            "Access-Control-Allow-Origin": "*",
            "Cache-Control": "no-cache"
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

# 4. Embedded Web Player with Mobile Double-Tap & Instant Sync
async def handle_player(request):
    msg_id = request.match_info["msg_id"]
    stream_url = f"{FQDN}/watch/{msg_id}"
    track_info_url = f"{FQDN}/api/tracks/{msg_id}"

    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
        <title>Anime Stream Player</title>
        <script src="https://cdn.jsdelivr.net/npm/artplayer/dist/artplayer.js"></script>
        <style>
            * {{ box-sizing: border-box; margin: 0; padding: 0; -webkit-tap-highlight-color: transparent; }}
            body {{
                background-color: #07090e;
                color: #ffffff;
                display: flex;
                flex-direction: column;
                align-items: center;
                justify-content: center;
                min-height: 100vh;
                font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
                padding: 10px;
                overflow-x: hidden;
            }}
            .player-container {{
                width: 100%;
                max-width: 1080px;
                position: relative;
            }}
            .artplayer-app {{
                width: 100%;
                aspect-ratio: 16 / 9;
                border-radius: 12px;
                overflow: hidden;
                box-shadow: 0 12px 40px rgba(0,0,0,0.9);
                background: #000;
            }}
            .info-bar {{
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-top: 10px;
                font-size: 13px;
                color: #38bdf8;
                padding: 0 4px;
            }}
            /* Double tap visual ripples */
            .seek-feedback {{
                position: absolute;
                top: 0;
                bottom: 0;
                width: 40%;
                display: flex;
                align-items: center;
                justify-content: center;
                font-size: 18px;
                font-weight: bold;
                background: rgba(255,255,255,0.12);
                border-radius: 12px;
                opacity: 0;
                pointer-events: none;
                transition: opacity 0.25s ease;
                z-index: 99;
            }}
            .seek-left {{ left: 0; }}
            .seek-right {{ right: 0; }}
        </style>
    </head>
    <body>
        <div class="player-container">
            <div class="artplayer-app"></div>
            <div id="seek-left-feedback" class="seek-feedback seek-left">⏪ 10s</div>
            <div id="seek-right-feedback" class="seek-feedback seek-right">10s ⏩</div>
            <div class="info-bar">
                <span>Double-tap left/right to seek 10s</span>
                <span id="track-status">Checking audio tracks...</span>
            </div>
        </div>

        <script>
            let selectedAudioTrack = 0;
            const extAudio = new Audio();
            extAudio.preload = 'auto';

            const art = new Artplayer({{
                container: '.artplayer-app',
                url: '{stream_url}',
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
                theme: '#38bdf8',
                fastForward: true
            }});

            // Synchronize secondary audio engine with video
            art.on('play', () => {{
                if (selectedAudioTrack !== 0) extAudio.play();
            }});

            art.on('pause', () => {{
                if (selectedAudioTrack !== 0) extAudio.pause();
            }});

            art.on('seek', (time) => {{
                if (selectedAudioTrack !== 0) {{
                    syncSecondaryAudio(time);
                }}
            }});

            art.on('video:volumechange', () => {{
                if (selectedAudioTrack !== 0) {{
                    extAudio.volume = art.volume;
                    extAudio.muted = art.muted;
                }}
            }});

            // Mobile Double Tap for 10s Seek
            let lastTapTime = 0;
            let lastTapSide = null;
            const container = document.querySelector('.artplayer-app');

            container.addEventListener('touchend', (e) => {{
                const now = Date.now();
                const rect = container.getBoundingClientRect();
                const touchX = e.changedTouches[0].clientX - rect.left;
                const side = touchX < (rect.width / 2) ? 'left' : 'right';

                if (now - lastTapTime < 320 && side === lastTapSide) {{
                    e.preventDefault();
                    if (side === 'left') {{
                        art.currentTime = Math.max(0, art.currentTime - 10);
                        showFeedback('seek-left-feedback');
                    }} else {{
                        art.currentTime = Math.min(art.duration, art.currentTime + 10);
                        showFeedback('seek-right-feedback');
                    }}
                    lastTapTime = 0;
                }} else {{
                    lastTapTime = now;
                    lastTapSide = side;
                }}
            }});

            function showFeedback(id) {{
                const el = document.getElementById(id);
                el.style.opacity = '1';
                setTimeout(() => {{ el.style.opacity = '0'; }}, 300);
            }}

            // Fetch and set up audio tracks
            fetch('{track_info_url}')
                .then(res => res.json())
                .then(data => {{
                    const tracks = data.tracks || [];
                    const statusText = document.getElementById('track-status');

                    if (tracks.length > 0) {{
                        statusText.innerText = `${{tracks.length}} Audio Track(s) Ready`;

                        const selectorList = tracks.map((track, idx) => ({{
                            html: track.title,
                            value: track.id,
                            default: idx === 0
                        }}));

                        art.setting.add({{
                            width: 260,
                            html: 'Audio Track',
                            tooltip: tracks[0].title,
                            selector: selectorList,
                            onSelect: function (item) {{
                                if (item.value !== selectedAudioTrack) {{
                                    selectedAudioTrack = item.value;
                                    handleAudioTrackChange(item.value);
                                }}
                                return item.html;
                            }}
                        }});
                    }} else {{
                        statusText.innerText = 'Direct Audio Active';
                    }}
                }})
                .catch(() => {{
                    document.getElementById('track-status').innerText = 'Audio Ready';
                }});

            function handleAudioTrackChange(trackId) {{
                if (trackId === 0) {{
                    // Switch back to embedded audio
                    extAudio.pause();
                    extAudio.src = '';
                    art.template.$video.muted = false;
                    art.notice.show = 'Switched to Track 1';
                }} else {{
                    // Mute internal audio and start lightweight synced stream
                    art.template.$video.muted = true;
                    syncSecondaryAudio(art.currentTime);
                    art.notice.show = `Track ${{trackId + 1}} Active`;
                }}
            }}

            function syncSecondaryAudio(targetTime) {{
                const sec = Math.floor(targetTime);
                extAudio.src = `{FQDN}/audio/{msg_id}/${{selectedAudioTrack}}?ss=${{sec}}`;
                extAudio.volume = art.volume;
                extAudio.muted = art.muted;
                if (art.playing) {{
                    extAudio.play().catch(() => {{}});
                }}
            }}
        </script>
    </body>
    </html>
    """
    return web.Response(text=html_content, content_type="text/html")

# Telegram Bot Handlers
@bot.on_message(filters.command("start") & filters.private)
async def start_handler(client: Client, message: Message):
    await message.reply_text(
        "<b>Anime Stream Engine is Online!</b>\n\n"
        "Send or forward any anime video/file to get an instant streaming player link with multi-audio support.",
        parse_mode=enums.ParseMode.HTML
    )

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
    app.router.add_get("/audio/{msg_id}/{track_id}", handle_audio_stream)
    app.router.add_get("/player/{msg_id}", handle_player)
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
        print(f"Server live at http://{BIND_ADDRESS}:{PORT}")
        await asyncio.Event().wait()

    loop.run_until_complete(run())
