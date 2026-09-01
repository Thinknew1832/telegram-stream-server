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

# Basic credentials validation
if not API_ID or not API_HASH or not BOT_TOKEN or not BIN_CHANNEL:
    print("\n[CRITICAL ERROR] Missing mandatory environment variables!")
    print("Please configure API_ID, API_HASH, BOT_TOKEN, and BIN_CHANNEL in your cloud dashboard.\n")
    sys.exit(1)

# In-memory session prevents SQLite locks on cloud containers
bot = Client(
    "StreamBot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    in_memory=True
)

# Route 1: Health Check
async def handle_ping(request):
    return web.Response(text="Telegram Stream Engine is Online!")

# Route 2: Direct Byte-Range Streaming (MKV/MP4/Audio)
async def handle_stream(request):
    try:
        msg_id = int(request.match_info["msg_id"])
        msg: Message = await bot.get_messages(BIN_CHANNEL, msg_id)

        media = msg.video or msg.document or msg.audio
        if not media:
            return web.Response(status=404, text="Media not found.")

        file_size = media.file_size
        mime_type = media.mime_type or "video/mp4"

        # Check for HTTP Range Header
        range_header = request.headers.get("Range")
        if range_header:
            byte_range = range_header.replace("bytes=", "").split("-")
            from_byte = int(byte_range[0])
            to_byte = int(byte_range[1]) if len(byte_range) > 1 and byte_range[1] else file_size - 1
        else:
            from_byte = 0
            to_byte = file_size - 1

        content_length = to_byte - from_byte + 1
        chunk_size = 1024 * 1024  # 1MB chunk size

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

        # Pyrogram streaming offset calculation
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

# Helper: Probe Audio Tracks with ffprobe
async def get_audio_streams(stream_url):
    try:
        cmd = [
            "ffprobe",
            "-v", "error",
            "-show_entries", "stream=index,codec_name,codec_type:stream_tags=language,title",
            "-of", "json",
            stream_url
        ]
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await process.communicate()
        data = json.loads(stdout.decode())
        return [s for s in data.get("streams", []) if s.get("codec_type") == "audio"]
    except Exception:
        return []

# Route 3: Dynamic HLS Master Playlist (.m3u8)
async def handle_master_m3u8(request):
    try:
        msg_id = int(request.match_info["msg_id"])
        internal_url = f"http://127.0.0.1:{PORT}/watch/{msg_id}"
        
        audio_tracks = await get_audio_streams(internal_url)
        
        # Build HLS Master Playlist
        playlist = "#EXTM3U\n#EXT-X-VERSION:3\n"
        
        if len(audio_tracks) > 1:
            for i, track in enumerate(audio_tracks):
                lang = track.get("tags", {}).get("language", f"und_{i+1}")
                title = track.get("tags", {}).get("title", f"Audio Track {i+1} ({lang.upper()})")
                is_default = "YES" if i == 0 else "NO"
                
                playlist += (
                    f'#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio",NAME="{title}",'
                    f'DEFAULT={is_default},AUTOSELECT=YES,LANGUAGE="{lang}",'
                    f'URI="{FQDN}/watch/{msg_id}"\n'
                )
            playlist += (
                f'#EXT-X-STREAM-INF:BANDWIDTH=4000000,AUDIO="audio"\n'
                f'{FQDN}/watch/{msg_id}\n'
            )
        else:
            # Single audio track fallback
            playlist += (
                f'#EXT-X-STREAM-INF:BANDWIDTH=4000000\n'
                f'{FQDN}/watch/{msg_id}\n'
            )

        return web.Response(
            text=playlist,
            content_type="application/vnd.apple.mpegurl",
            headers={"Access-Control-Allow-Origin": "*"}
        )
    except Exception as e:
        return web.Response(status=500, text=f"Manifest Error: {str(e)}")

# Route 4: Embedded Modern Anime Web Player (Artplayer + Hls.js)
async def handle_player(request):
    msg_id = request.match_info["msg_id"]
    stream_url = f"{FQDN}/watch/{msg_id}"
    hls_url = f"{FQDN}/hls/{msg_id}/master.m3u8"
    
    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Anime Stream Player</title>
        <script src="https://cdn.jsdelivr.net/npm/hls.js@latest"></script>
        <script src="https://cdn.jsdelivr.net/npm/artplayer/dist/artplayer.js"></script>
        <style>
            * {{
                box-sizing: border-box;
                margin: 0;
                padding: 0;
            }}
            body {{
                background-color: #0b0d14;
                color: #ffffff;
                display: flex;
                flex-direction: column;
                align-items: center;
                justify-content: center;
                min-height: 100vh;
                font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
                padding: 16px;
            }}
            .player-wrapper {{
                width: 100%;
                max-width: 1080px;
            }}
            .artplayer-app {{
                width: 100%;
                aspect-ratio: 16 / 9;
                border-radius: 12px;
                overflow: hidden;
                box-shadow: 0 10px 40px rgba(0, 0, 0, 0.8);
            }}
            .player-actions {{
                margin-top: 14px;
                display: flex;
                justify-content: space-between;
                align-items: center;
                font-size: 13px;
                color: #94a3b8;
            }}
            .btn-group {{
                display: flex;
                gap: 10px;
            }}
            .btn {{
                background: #1e293b;
                color: #f1f5f9;
                text-decoration: none;
                padding: 8px 14px;
                border-radius: 6px;
                font-weight: 500;
                transition: background 0.2s ease;
                border: 1px solid #334155;
            }}
            .btn:hover {{
                background: #3b82f6;
                border-color: #3b82f6;
            }}
        </style>
    </head>
    <body>
        <div class="player-wrapper">
            <div class="artplayer-app"></div>
            <div class="player-actions">
                <span>Direct Audio Track Switcher Enabled</span>
                <div class="btn-group">
                    <a href="vlc://{stream_url.replace('https://', '').replace('http://', '')}" class="btn">Open in VLC</a>
                    <a href="{stream_url}" download class="btn">Direct Download</a>
                </div>
            </div>
        </div>

        <script>
            var art = new Artplayer({{
                container: '.artplayer-app',
                url: '{stream_url}',
                volume: 0.8,
                isLive: false,
                muted: false,
                autoplay: false,
                pip: true,
                autoSize: true,
                autoMini: true,
                screenshot: true,
                setting: true,
                loop: false,
                flip: true,
                playbackRate: true,
                aspectRatio: true,
                fullscreen: true,
                fullscreenWeb: true,
                miniProgressBar: true,
                mutex: true,
                backdrop: true,
                playsInline: true,
                autoPlayback: true,
                airplay: true,
                theme: '#3b82f6',
                icons: {{
                    state: '<svg width="60" height="60" viewBox="0 0 48 48" fill="#fff"><path d="M16 10v28l22-14z"/></svg>',
                }},
                customType: {{
                    m3u8: function (video, url, art) {{
                        if (Hls.isSupported()) {{
                            const hls = new Hls();
                            hls.loadSource(url);
                            hls.attachMedia(video);
                            art.hls = hls;

                            hls.on(Hls.Events.MANIFEST_PARSED, function () {{
                                if (hls.audioTracks && hls.audioTracks.length > 1) {{
                                    const selectorList = hls.audioTracks.map((track, index) => ({{
                                        html: track.name || `Audio Track ${{index + 1}}`,
                                        value: index,
                                        default: index === hls.audioTrack
                                    }}));

                                    art.setting.add({{
                                        width: 200,
                                        html: 'Audio Track',
                                        tooltip: 'Select Language',
                                        selector: selectorList,
                                        onSelect: function (item) {{
                                            hls.audioTrack = item.value;
                                            return item.html;
                                        }}
                                    }});
                                }}
                            }});
                        }} else if (video.canPlayType('application/vnd.apple.mpegurl')) {{
                            video.src = url;
                        }}
                    }}
                }}
            }});

            // Native browser audio track detection fallback
            art.on('ready', () => {{
                const video = art.template.$video;
                if (video.audioTracks && video.audioTracks.length > 1) {{
                    const audioOptions = [];
                    for (let i = 0; i < video.audioTracks.length; i++) {{
                        const track = video.audioTracks[i];
                        audioOptions.push({{
                            html: track.label || track.language || `Track ${{i + 1}}`,
                            value: i,
                            default: track.enabled
                        }});
                    }}

                    art.setting.add({{
                        width: 200,
                        html: 'Audio Track',
                        tooltip: 'Select Language',
                        selector: audioOptions,
                        onSelect: function (item) {{
                            for (let i = 0; i < video.audioTracks.length; i++) {{
                                video.audioTracks[i].enabled = (i === item.value);
                            }}
                            return item.html;
                        }}
                    }});
                }}
            }});
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

# Telegram Media Handler: Auto-forward and generate stream links
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
        stream_url = f"{FQDN}/watch/{forwarded.id}"

        reply_text = (
            f"<b>File Processed Successfully!</b>\n\n"
            f"<b>Title:</b> <code>{file_name}</code>\n"
            f"<b>Web Player:</b> <code>{player_url}</code>\n"
            f"<b>Direct Stream:</b> <code>{stream_url}</code>"
        )

        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("Watch in Web Player (Multi-Audio)", url=player_url)],
            [InlineKeyboardButton("Direct Stream Link", url=stream_url)]
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
    app.router.add_get("/hls/{msg_id}/master.m3u8", handle_master_m3u8)
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
