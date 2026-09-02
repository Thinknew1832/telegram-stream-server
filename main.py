import os
import math
import sys
import json
import re
import asyncio
import aiohttp
from aiohttp import web
from pyrogram import Client, filters, enums
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from motor.motor_asyncio import AsyncIOMotorClient
import PTN

# Environment Configurations
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "").strip()
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
BIN_CHANNEL = int(os.getenv("BIN_CHANNEL", "0"))
PORT = int(os.getenv("PORT", "8080"))
BIND_ADDRESS = os.getenv("BIND_ADDRESS", "0.0.0.0")
FQDN = os.getenv("FQDN", f"http://localhost:{PORT}").rstrip("/")
MONGO_URI = os.getenv("MONGO_URI", "").strip()

if not API_ID or not API_HASH or not BOT_TOKEN or not BIN_CHANNEL:
    print("\n[CRITICAL ERROR] Missing mandatory environment variables!\n")
    sys.exit(1)

# Database Setup
db_client = AsyncIOMotorClient(MONGO_URI) if MONGO_URI else None
db = db_client["animetoon_db"] if db_client else None
anime_col = db["anime"] if db is not None else None

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
DEMUX_LOCK = asyncio.Semaphore(1)

def slugify(text):
    return re.sub(r'[\W_]+', '-', text.lower()).strip('-')

# Fetch Anime Metadata from Jikan API (MyAnimeList free database)
async def fetch_anime_metadata(title):
    try:
        clean_title = re.sub(r"[\[\(].*?[\]\)]", "", title).strip()
        url = f"https://api.jikan.moe/v4/anime?q={clean_title}&limit=1"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    results = data.get("data", [])
                    if results:
                        item = results[0]
                        return {
                            "canonical_title": item.get("title_english") or item.get("title"),
                            "poster": item.get("images", {}).get("webp", {}).get("large_image_url") or "",
                            "banner": item.get("images", {}).get("jpg", {}).get("large_image_url") or "",
                            "synopsis": item.get("synopsis") or "No synopsis available.",
                            "rating": str(item.get("score") or "7.5"),
                            "genres": ", ".join([g.get("name") for g in item.get("genres", [])]),
                            "year": str(item.get("year") or "2024"),
                            "status": item.get("status") or "Ongoing"
                        }
    except Exception as e:
        print(f"Metadata fetch error: {e}")
    return None

async def handle_ping(request):
    return web.Response(text="Telegram Stream Engine is Online!")

# Direct High-Speed Byte-Range Streaming
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
        chunk_size = 1024 * 1024

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

# Track Metadata Inspector
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
            "-show_entries", "stream=index,codec_name,codec_type:stream_tags=language,title:format=duration",
            "-of", "json",
            source_url
        ]
        process = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
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

                if "@" in title or not title:
                    title = f"Track {audio_idx + 1}: {clean_lang}"
                else:
                    title = f"{title} [{clean_lang}]"

                tracks.append({"id": audio_idx, "title": title})
                audio_idx += 1

        res = {"duration": duration, "tracks": tracks}
        META_CACHE[msg_id] = res
        return web.json_response(res, headers={"Access-Control-Allow-Origin": "*"})
    except Exception:
        return web.json_response({"duration": 0, "tracks": []})

# Dynamic Remux Endpoint
async def handle_remux_stream(request):
    msg_id = int(request.match_info["msg_id"])
    track_id = int(request.match_info["track_id"])
    seek_time = request.query.get("ss", "0")
    source_url = f"http://127.0.0.1:{PORT}/watch/{msg_id}"

    cmd = [
        "ffmpeg",
        "-ss", str(seek_time),
        "-i", source_url,
        "-map", "0:v:0",
        "-map", f"0:a:{track_id}",
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
            "Cache-Control": "no-cache"
        }
    )
    await response.prepare(request)

    async with DEMUX_LOCK:
        process = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
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

# Web Player Interface
async def handle_player(request):
    msg_id = request.match_info["msg_id"]
    default_stream_url = f"{FQDN}/watch/{msg_id}"
    track_info_url = f"{FQDN}/api/tracks/{msg_id}"

    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
        <title>Anime Player</title>
        <script src="https://cdn.jsdelivr.net/npm/artplayer/dist/artplayer.js"></script>
        <style>
            * {{ box-sizing: border-box; margin: 0; padding: 0; -webkit-tap-highlight-color: transparent; user-select: none; }}
            body {{ background-color: #06070a; color: #ffffff; display: flex; flex-direction: column; align-items: center; justify-content: center; min-height: 100vh; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; overflow-x: hidden; }}
            .player-container {{ width: 100%; max-width: 1100px; position: relative; }}
            .artplayer-app {{ width: 100%; aspect-ratio: 16 / 9; border-radius: 10px; overflow: hidden; box-shadow: 0 10px 30px rgba(0,0,0,0.85); background: #000; }}
            .status-text {{ margin-top: 8px; font-size: 12px; color: #38bdf8; text-align: right; padding-right: 4px; }}
            .gesture-pill {{ position: absolute; top: 50%; transform: translateY(-50%); padding: 8px 16px; background: rgba(0, 0, 0, 0.75); border: 1px solid rgba(255, 255, 255, 0.2); border-radius: 20px; font-size: 14px; font-weight: 600; color: #fff; opacity: 0; pointer-events: none; transition: opacity 0.2s ease; z-index: 99; }}
            .gesture-left {{ left: 12%; }}
            .gesture-right {{ right: 12%; }}
            .gesture-center {{ left: 50%; transform: translate(-50%, -50%); }}
        </style>
    </head>
    <body>
        <div class="player-container">
            <div class="artplayer-app"></div>
            <div id="seek-back" class="gesture-pill gesture-left">-10s</div>
            <div id="play-pause-pill" class="gesture-pill gesture-center">Pause</div>
            <div id="seek-fwd" class="gesture-pill gesture-right">+10s</div>
            <div class="status-text" id="status-indicator">Connecting engine...</div>
        </div>

        <script>
            let currentTrack = 0;
            let fullVideoDuration = 0;
            const originalUrl = '{default_stream_url}';

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

            art.on('video:loadedmetadata', () => {{
                if (fullVideoDuration > 0) {{
                    try {{
                        Object.defineProperty(art.template.$video, 'duration', {{
                            configurable: true,
                            get: () => fullVideoDuration
                        }});
                    }} catch (e) {{}}
                }}
            }});

            fetch('{track_info_url}')
                .then(res => res.json())
                .then(data => {{
                    const tracks = data.tracks || [];
                    fullVideoDuration = data.duration || 0;
                    const statusIndicator = document.getElementById('status-indicator');

                    if (tracks.length > 0) {{
                        statusIndicator.innerText = `${{tracks.length}} Audio Track(s) Ready`;

                        const selectorList = tracks.map((track, idx) => ({{
                            html: track.title,
                            value: track.id,
                            default: idx === 0
                        }}));

                        art.setting.add({{
                            width: 250,
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
                        statusIndicator.innerText = 'Audio Ready';
                    }}
                }});

            function switchAudioStream(trackId) {{
                const resumePoint = Math.floor(art.currentTime);
                art.notice.show = 'Switching audio...';

                if (trackId === 0) {{
                    art.switchUrl(originalUrl).then(() => {{
                        art.currentTime = resumePoint;
                        art.play();
                    }});
                }} else {{
                    const remuxUrl = `{FQDN}/remux/{msg_id}/${{trackId}}?ss=${{resumePoint}}`;
                    art.switchUrl(remuxUrl).then(() => {{
                        art.play();
                    }});
                }}
            }}

            art.on('fullscreen', (state) => {{
                if (state) {{
                    if (screen.orientation && screen.orientation.lock) {{
                        screen.orientation.lock('landscape').catch(() => {{}});
                    }}
                }} else {{
                    if (screen.orientation && screen.orientation.unlock) {{
                        screen.orientation.unlock().catch(() => {{}});
                    }}
                }}
            }});

            let lastTapTime = 0;
            let lastTapZone = null;
            const playerBox = document.querySelector('.artplayer-app');

            playerBox.addEventListener('touchend', (e) => {{
                const now = Date.now();
                const rect = playerBox.getBoundingClientRect();
                const x = e.changedTouches[0].clientX - rect.left;
                const width = rect.width;

                let zone = 'center';
                if (x < width * 0.35) {{
                    zone = 'left';
                }} else if (x > width * 0.65) {{
                    zone = 'right';
                }}

                if (now - lastTapTime < 300 && zone === lastTapZone) {{
                    e.preventDefault();

                    if (zone === 'left') {{
                        art.currentTime = Math.max(0, art.currentTime - 10);
                        flashPill('seek-back');
                    }} else if (zone === 'right') {{
                        art.currentTime = Math.min(art.duration, art.currentTime + 10);
                        flashPill('seek-fwd');
                    }} else {{
                        if (art.playing) {{
                            art.pause();
                            document.getElementById('play-pause-pill').innerText = 'Pause';
                        }} else {{
                            art.play();
                            document.getElementById('play-pause-pill').innerText = 'Play';
                        }}
                        flashPill('play-pause-pill');
                    }}

                    lastTapTime = 0;
                }} else {{
                    lastTapTime = now;
                    lastTapZone = zone;
                }}
            }});

            function flashPill(id) {{
                const el = document.getElementById(id);
                el.style.opacity = '1';
                setTimeout(() => {{ el.style.opacity = '0'; }}, 350);
            }}
        </script>
    </body>
    </html>
    """
    return web.Response(text=html_content, content_type="text/html")

# Auto-Indexing Handler (Telegram Trigger)
@bot.on_message(filters.private & (filters.document | filters.video | filters.audio))
async def auto_forward_and_index(client: Client, message: Message):
    try:
        forwarded = await message.forward(chat_id=BIN_CHANNEL)

        file_name = "Unknown Anime"
        if message.document and message.document.file_name:
            file_name = message.document.file_name
        elif message.video and message.video.file_name:
            file_name = message.video.file_name
        elif message.caption:
            file_name = message.caption.split("\n")[0]

        parsed = PTN.parse(file_name)
        raw_title = parsed.get("title") or file_name.rsplit(".", 1)[0]
        season_num = parsed.get("season", 1)
        episode_num = parsed.get("episode", 1)

        if not parsed.get("episode"):
            ep_match = re.search(r"[Ee](\d{1,4})|\b(\d{1,4})\b", file_name)
            if ep_match:
                episode_num = int(ep_match.group(1) or ep_match.group(2))

        anime_slug = slugify(raw_title)

        if anime_col is not None:
            existing = await anime_col.find_one({"_id": anime_slug})
            if not existing:
                meta = await fetch_anime_metadata(raw_title)
                title = meta["canonical_title"] if meta else raw_title
                anime_doc = {
                    "_id": anime_slug,
                    "title": title,
                    "poster": meta["poster"] if meta else "https://via.placeholder.com/300x450",
                    "banner": meta["banner"] if meta else "https://via.placeholder.com/1280x720",
                    "genres": meta["genres"] if meta else "Anime",
                    "status": meta["status"] if meta else "Ongoing",
                    "description": meta["synopsis"] if meta else "No description available.",
                    "rating": meta["rating"] if meta else "7.5",
                    "year": meta["year"] if meta else "2024",
                    "languages": "Multi-Audio",
                    "episodes": []
                }
                await anime_col.insert_one(anime_doc)
            else:
                anime_doc = existing

            ep_entry = {
                "season": season_num,
                "ep": episode_num,
                "title": f"Episode {episode_num}",
                "msg_id": forwarded.id,
                "file_name": file_name
            }

            await anime_col.update_one(
                {"_id": anime_slug},
                {"$pull": {"episodes": {"season": season_num, "ep": episode_num}}}
            )
            await anime_col.update_one(
                {"_id": anime_slug},
                {"$push": {"episodes": {"$each": [ep_entry], "$sort": {"season": 1, "ep": 1}}}}
            )

        player_url = f"{FQDN}/player/{forwarded.id}"
        anime_page_url = f"{FQDN}/anime/{anime_slug}"

        reply_text = (
            f"<b>Episode Auto-Indexed!</b>\n\n"
            f"<b>Anime:</b> <code>{raw_title}</code>\n"
            f"<b>Episode:</b> Season {season_num} Episode {episode_num}\n"
            f"<b>Web Player:</b> <code>{player_url}</code>\n"
            f"<b>Anime Page:</b> <code>{anime_page_url}</code>"
        )

        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("Watch Episode", url=player_url)],
            [InlineKeyboardButton("View Anime Series", url=anime_page_url)]
        ])

        await message.reply_text(reply_text, parse_mode=enums.ParseMode.HTML, reply_markup=keyboard, disable_web_page_preview=True)
    except Exception as e:
        await message.reply_text(f"Auto-index failed: {str(e)}")

# Dynamic Home Page
async def handle_home(request):
    if anime_col is None:
        return web.Response(text="Database connection initializing...", content_type="text/html")

    anime_list = await anime_col.find().sort("year", -1).to_list(length=100)

    if not anime_list:
        return web.Response(text="<div style='font-family:sans-serif;padding:30px;text-align:center;'><h2>AnimeToon is Live!</h2><p>Upload or forward your anime video files to your Telegram bot to auto-populate the catalog.</p></div>", content_type="text/html")

    hero = anime_list[0]
    cards_html = ""
    for item in anime_list:
        cards_html += f"""
        <a href="/anime/{item['_id']}" class="anime-card">
            <div class="poster-box">
                <img src="{item.get('poster')}" alt="{item.get('title')}" loading="lazy" />
                <span class="rating-badge">★ {item.get('rating', '7.0')}</span>
                <span class="multi-badge">Multi</span>
            </div>
            <h3 class="card-title">{item.get('title')}</h3>
            <div class="card-meta">
                <span>{item.get('year', '2024')}</span>
                <span class="type-pill">TV</span>
            </div>
        </a>
        """

    html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>AnimeToon</title>
        <style>
            * {{ box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; text-decoration: none; }}
            body {{ background-color: #ffffff; color: #111; padding-bottom: 40px; }}
            header {{ display: flex; justify-content: space-between; align-items: center; padding: 14px 18px; }}
            .brand {{ font-size: 24px; font-weight: 800; color: #ff2a74; }}
            .hero-container {{ margin: 10px 16px; position: relative; border-radius: 18px; overflow: hidden; box-shadow: 0 10px 25px rgba(0,0,0,0.12); }}
            .hero-img {{ width: 100%; aspect-ratio: 16/9; object-fit: cover; display: block; }}
            .hero-overlay {{ position: absolute; inset: 0; background: linear-gradient(180deg, transparent 40%, rgba(0,0,0,0.85) 100%); display: flex; flex-direction: column; justify-content: flex-end; padding: 16px; }}
            .hero-title {{ color: #fff; font-size: 20px; font-weight: 700; margin-bottom: 10px; }}
            .watch-btn {{ background: #ff2a74; color: #fff; padding: 8px 18px; border-radius: 20px; font-size: 14px; font-weight: 600; display: inline-flex; align-items: center; gap: 6px; width: fit-content; }}
            .section-header {{ display: flex; justify-content: space-between; align-items: center; padding: 22px 18px 12px; }}
            .section-title {{ font-size: 17px; font-weight: 800; text-transform: uppercase; border-left: 4px solid #ff2a74; padding-left: 8px; }}
            .anime-grid {{ display: grid; grid-template-columns: repeat(2, 1fr); gap: 14px; padding: 0 16px; }}
            .anime-card {{ display: flex; flex-direction: column; }}
            .poster-box {{ position: relative; border-radius: 14px; overflow: hidden; aspect-ratio: 1/1.4; background: #eee; }}
            .poster-box img {{ width: 100%; height: 100%; object-fit: cover; }}
            .rating-badge {{ position: absolute; top: 8px; left: 8px; background: rgba(0,0,0,0.7); color: #ffd700; font-size: 11px; padding: 3px 6px; border-radius: 6px; font-weight: 700; }}
            .multi-badge {{ position: absolute; bottom: 8px; right: 8px; background: #ff2a74; color: #fff; font-size: 10px; font-weight: 700; padding: 2px 7px; border-radius: 8px; }}
            .card-title {{ font-size: 14px; font-weight: 700; color: #111; margin-top: 8px; display: -webkit-box; -webkit-line-clamp: 1; -webkit-box-orient: vertical; overflow: hidden; }}
            .card-meta {{ display: flex; justify-content: space-between; align-items: center; margin-top: 4px; font-size: 12px; color: #666; }}
            .type-pill {{ background: #f0f2f5; padding: 2px 6px; border-radius: 4px; font-size: 10px; font-weight: 700; }}
        </style>
    </head>
    <body>
        <header>
            <div class="brand">AnimeToon</div>
        </header>

        <div class="hero-container">
            <img src="{hero.get('banner')}" class="hero-img" alt="{hero.get('title')}" />
            <div class="hero-overlay">
                <div class="hero-title">{hero.get('title')}</div>
                <a href="/anime/{hero['_id']}" class="watch-btn">▶ Watch Now</a>
            </div>
        </div>

        <div class="section-header">
            <div class="section-title">Recently Added</div>
        </div>

        <div class="anime-grid">
            {cards_html}
        </div>
    </body>
    </html>
    """
    return web.Response(text=html, content_type="text/html")

# Dynamic Anime Details & Episode Selector
async def handle_anime_detail(request):
    if anime_col is None:
        return web.Response(status=500, text="Database Unavailable")

    anime_id = request.match_info["anime_id"]
    anime = await anime_col.find_one({"_id": anime_id})

    if not anime:
        return web.Response(status=404, text="Anime Not Found")

    episodes = anime.get("episodes", [])
    episodes_html = ""
    for ep in episodes:
        episodes_html += f"""
        <a href="/player/{ep['msg_id']}" class="ep-card">
            <div class="ep-thumb">
                <img src="{anime.get('poster')}" alt="Ep {ep['ep']}" />
            </div>
            <div class="ep-details">
                <span class="ep-num">S{ep.get('season', 1):02d} E{ep['ep']:02d}</span>
                <span class="ep-name">{ep.get('title', f"Episode {ep['ep']}")}</span>
            </div>
            <div class="ep-play-btn">▶</div>
        </a>
        """

    html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>{anime['title']} - AnimeToon</title>
        <style>
            * {{ box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; text-decoration: none; }}
            body {{ background-color: #f7f9fc; color: #111; padding-bottom: 40px; }}
            header {{ display: flex; justify-content: space-between; align-items: center; padding: 14px 18px; background: #fff; }}
            .brand {{ font-size: 24px; font-weight: 800; color: #ff2a74; }}
            .banner-box {{ margin: 12px 16px; border-radius: 18px; overflow: hidden; box-shadow: 0 8px 24px rgba(0,0,0,0.12); }}
            .banner-box img {{ width: 100%; aspect-ratio: 16/10; object-fit: cover; display: block; }}
            .meta-section {{ padding: 0 18px; margin-top: 12px; }}
            .genres {{ font-size: 12px; font-weight: 600; color: #ff2a74; }}
            .title {{ font-size: 20px; font-weight: 800; margin: 6px 0; }}
            .desc {{ font-size: 13px; color: #444; line-height: 1.45; margin: 8px 0; }}
            .specs {{ display: flex; align-items: center; gap: 10px; font-size: 13px; color: #333; font-weight: 600; }}
            .season-dropdown {{ margin: 18px 16px 12px; padding: 10px 14px; background: #eef2f6; border-radius: 10px; font-weight: 700; font-size: 14px; }}
            .episodes-list {{ display: flex; flex-direction: column; gap: 12px; padding: 0 16px; }}
            .ep-card {{ display: flex; align-items: center; background: #ffffff; padding: 10px 14px; border-radius: 16px; box-shadow: 0 2px 8px rgba(0,0,0,0.04); border: 1px solid #edf0f5; }}
            .ep-thumb {{ width: 90px; height: 60px; border-radius: 10px; overflow: hidden; flex-shrink: 0; background: #eee; }}
            .ep-thumb img {{ width: 100%; height: 100%; object-fit: cover; }}
            .ep-details {{ margin-left: 14px; flex-grow: 1; }}
            .ep-num {{ font-size: 11px; font-weight: 700; color: #666; text-transform: uppercase; }}
            .ep-name {{ font-size: 13px; font-weight: 700; color: #111; margin-top: 2px; }}
            .ep-play-btn {{ color: #ff2a74; font-size: 14px; margin-left: 8px; }}
        </style>
    </head>
    <body>
        <header>
            <a href="/" class="brand">AnimeToon</a>
        </header>

        <div class="banner-box">
            <img src="{anime.get('banner')}" alt="{anime['title']}" />
        </div>

        <div class="meta-section">
            <div class="genres">{anime.get('genres')}</div>
            <h1 class="title">{anime['title']}</h1>
            <p class="desc">{anime.get('description')}</p>
            <div class="specs">
                <span>★ {anime.get('rating', '7.5')}</span>
                <span>•</span>
                <span>{len(episodes)} Episodes</span>
            </div>
        </div>

        <div class="season-dropdown">EPISODES</div>

        <div class="episodes-list">
            {episodes_html if episodes_html else "<p style='padding:16px;'>No episodes added yet.</p>"}
        </div>
    </body>
    </html>
    """
    return web.Response(text=html, content_type="text/html")

# App Initialization
async def init_app():
    app = web.Application()
    app.router.add_get("/", handle_home)
    app.router.add_get("/anime/{anime_id}", handle_anime_detail)
    app.router.add_get("/watch/{msg_id}", handle_stream)
    app.router.add_get("/api/tracks/{msg_id}", handle_track_info)
    app.router.add_get("/remux/{msg_id}/{track_id}", handle_remux_stream)
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
        print(f"AnimeToon server live at http://{BIND_ADDRESS}:{PORT}")
        await asyncio.Event().wait()

    loop.run_until_complete(run())
