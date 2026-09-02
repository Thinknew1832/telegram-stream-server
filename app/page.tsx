'use client';

import React, { useState, useEffect, useRef } from 'react';

// Environment variables configured on Vercel
const STREAM_SERVER = process.env.NEXT_PUBLIC_STREAM_SERVER || "https://telegram-stream-server-vglf.onrender.com";
const SHEET_CSV_URL = process.env.NEXT_PUBLIC_SHEET_CSV_URL || "";

interface Episode {
  season: number;
  episode: number;
  title: string;
  msg_id: string;
}

interface Anime {
  id: string;
  title: string;
  banner: string;
  poster: string;
  genres: string;
  rating: string;
  year: string;
  episodes: Episode[];
}

export default function AnimeToonApp() {
  const [catalog, setCatalog] = useState<Anime[]>([]);
  const [activeAnime, setActiveAnime] = useState<Anime | null>(null);
  const [activeEpisode, setActiveEpisode] = useState<Episode | null>(null);
  const [searchQuery, setSearchQuery] = useState("");
  const [showSearch, setShowSearch] = useState(false);
  const [loading, setLoading] = useState(true);

  const playerRef = useRef<HTMLDivElement>(null);
  const artInstance = useRef<any>(null);

  // 1. Fetch and parse the live Excel/Google Sheets CSV
  useEffect(() => {
    async function loadCatalog() {
      if (!SHEET_CSV_URL) {
        setLoading(false);
        return;
      }
      try {
        const res = await fetch(SHEET_CSV_URL, { cache: 'no-store' });
        const text = await res.text();
        const rows = text.split('\n').map(r => r.trim()).filter(Boolean);
        const headers = rows[0].split(',').map(h => h.trim().toLowerCase());

        const animeMap: { [key: string]: Anime } = {};

        for (let i = 1; i < rows.length; i++) {
          // Handle standard CSV commas safely
          const values = rows[i].split(/,(?=(?:(?:[^"]*"){2})*[^"]*$)/).map(v => v.replace(/^"|"$/g, '').trim());
          const row: any = {};
          headers.forEach((h, idx) => row[h] = values[idx] || '');

          const id = row.anime_id;
          if (!id) continue;

          if (!animeMap[id]) {
            animeMap[id] = {
              id,
              title: row.title || 'Untitled Anime',
              banner: row.banner || '',
              poster: row.poster || '',
              genres: row.genres || 'Action',
              rating: row.rating || '7.5',
              year: row.year || '2024',
              episodes: []
            };
          }

          if (row.msg_id) {
            animeMap[id].episodes.push({
              season: parseInt(row.season) || 1,
              episode: parseInt(row.episode) || 1,
              title: row.ep_title || `Episode ${row.episode}`,
              msg_id: row.msg_id
            });
          }
        }

        const list = Object.values(animeMap);
        setCatalog(list);
        if (list.length > 0) {
          setActiveAnime(list[0]);
          if (list[0].episodes.length > 0) {
            setActiveEpisode(list[0].episodes[0]);
          }
        }
      } catch (err) {
        console.error("Failed to load catalog:", err);
      } finally {
        setLoading(false);
      }
    }
    loadCatalog();
  }, []);

  // 2. Initialize Player with Audio Track Switcher and Controls
  useEffect(() => {
    if (!activeEpisode || !playerRef.current || typeof window === 'undefined' || !(window as any).Artplayer) {
      return;
    }

    if (artInstance.current) {
      artInstance.current.destroy(false);
    }

    const defaultUrl = `${STREAM_SERVER}/watch/${activeEpisode.msg_id}`;
    let currentTrack = 0;

    const art = new (window as any).Artplayer({
      container: playerRef.current,
      url: defaultUrl,
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
      theme: '#ec4899',
    });

    // Fetch dynamic multi-audio tracks
    fetch(`${STREAM_SERVER}/api/tracks/${activeEpisode.msg_id}`)
      .then(res => res.json())
      .then(data => {
        const tracks = data.tracks || [];
        if (tracks.length > 1) {
          const selectorList = tracks.map((t: any, idx: number) => ({
            html: t.title,
            value: t.id,
            default: idx === 0
          }));

          art.setting.add({
            width: 250,
            html: 'Audio Track',
            tooltip: tracks[0].title,
            selector: selectorList,
            onSelect: function (item: any) {
              if (item.value !== currentTrack) {
                currentTrack = item.value;
                const resumePoint = Math.floor(art.currentTime);
                art.notice.show = 'Switching Audio...';
                if (item.value === 0) {
                  art.switchUrl(defaultUrl).then(() => {
                    art.currentTime = resumePoint;
                    art.play();
                  });
                } else {
                  const remux = `${STREAM_SERVER}/remux/${activeEpisode.msg_id}/${item.value}?ss=${resumePoint}`;
                  art.switchUrl(remux).then(() => art.play());
                }
              }
              return item.html;
            }
          });
        }
      })
      .catch(() => {});

    // Orientation lock for mobile landscape
    art.on('fullscreen', (state: boolean) => {
      if (state && screen.orientation && (screen.orientation as any).lock) {
        (screen.orientation as any).lock('landscape').catch(() => {});
      } else if (!state && screen.orientation && screen.orientation.unlock) {
        screen.orientation.unlock().catch(() => {});
      }
    });

    artInstance.current = art;

    return () => {
      if (artInstance.current) {
        artInstance.current.destroy(false);
      }
    };
  }, [activeEpisode]);

  const filteredCatalog = catalog.filter(a =>
    a.title.toLowerCase().includes(searchQuery.toLowerCase()) ||
    a.genres.toLowerCase().includes(searchQuery.toLowerCase())
  );

  return (
    <div className="max-w-md md:max-w-4xl mx-auto px-4 py-4 min-h-screen">
      {/* Header */}
      <header className="flex items-center justify-between pb-3 border-b border-slate-800">
        <span className="text-2xl font-black tracking-tight text-pink-500">AnimeToon</span>
        <button
          onClick={() => setShowSearch(!showSearch)}
          className="p-2 text-slate-400 hover:text-white"
          aria-label="Toggle Search"
        >
          🔍
        </button>
      </header>

      {/* Expandable Search */}
      {showSearch && (
        <div className="mt-3">
          <input
            type="text"
            placeholder="Search anime title or genre..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="w-full bg-slate-900 border border-slate-800 rounded-xl px-4 py-2 text-sm text-white focus:outline-none focus:border-pink-500"
          />
        </div>
      )}

      {loading ? (
        <div className="flex items-center justify-center h-64 text-slate-500 text-sm">
          Loading catalog from spreadsheet...
        </div>
      ) : (
        <main className="mt-4 space-y-6">
          {/* Active Video Player Screen */}
          {activeEpisode && (
            <section className="space-y-3">
              <div className="aspect-video w-full rounded-2xl overflow-hidden shadow-2xl bg-black border border-slate-800">
                <div ref={playerRef} className="w-full h-full" />
              </div>

              <div className="flex items-center justify-between">
                <div>
                  <h2 className="text-lg font-bold text-slate-100">{activeAnime?.title}</h2>
                  <p className="text-xs text-slate-400">
                    S{String(activeEpisode.season).padStart(2, '0')} E{String(activeEpisode.episode).padStart(2, '0')} - {activeEpisode.title}
                  </p>
                </div>
                <span className="text-xs font-semibold px-2 py-1 bg-pink-500/20 text-pink-400 rounded-md border border-pink-500/30">
                  ★ {activeAnime?.rating}
                </span>
              </div>
            </section>
          )}

          {/* Episode Drawer */}
          {activeAnime && activeAnime.episodes.length > 0 && (
            <section className="space-y-2">
              <h3 className="text-xs font-bold uppercase tracking-wider text-slate-400">Episodes</h3>
              <div className="space-y-2 max-h-56 overflow-y-auto pr-1">
                {activeAnime.episodes.map((ep) => {
                  const isCurrent = activeEpisode?.msg_id === ep.msg_id;
                  return (
                    <button
                      key={ep.msg_id}
                      onClick={() => setActiveEpisode(ep)}
                      className={`w-full flex items-center gap-3 p-2.5 rounded-xl border text-left transition-all ${
                        isCurrent
                          ? 'bg-pink-500/10 border-pink-500/50 text-pink-300'
                          : 'bg-slate-900/60 border-slate-800/80 text-slate-300 hover:bg-slate-900'
                      }`}
                    >
                      <div className="w-16 h-10 rounded-lg overflow-hidden bg-slate-800 flex-shrink-0">
                        <img
                          src={`${STREAM_SERVER}/thumb/${ep.msg_id}`}
                          alt={`Ep ${ep.episode}`}
                          className="w-full h-full object-cover"
                          loading="lazy"
                        />
                      </div>
                      <div className="flex-1 min-w-0">
                        <span className="text-[10px] font-bold uppercase text-slate-500 block">
                          S{String(ep.season).padStart(2, '0')} E{String(ep.episode).padStart(2, '0')}
                        </span>
                        <p className="text-xs font-semibold truncate">{ep.title}</p>
                      </div>
                      <span className="text-sm text-pink-500">▶</span>
                    </button>
                  );
                })}
              </div>
            </section>
          )}

          {/* Anime Grid */}
          <section className="space-y-3 pt-2">
            <h3 className="text-xs font-bold uppercase tracking-wider text-slate-400">All Anime Series</h3>
            <div className="grid grid-cols-2 sm:grid-cols-3 gap-3">
              {filteredCatalog.map((anime) => (
                <div
                  key={anime.id}
                  onClick={() => {
                    setActiveAnime(anime);
                    if (anime.episodes.length > 0) setActiveEpisode(anime.episodes[0]);
                    window.scrollTo({ top: 0, behavior: 'smooth' });
                  }}
                  className="cursor-pointer group flex flex-col space-y-1.5"
                >
                  <div className="aspect-[3/4] w-full rounded-xl overflow-hidden bg-slate-900 border border-slate-800 relative shadow-md">
                    <img
                      src={anime.poster}
                      alt={anime.title}
                      className="w-full h-full object-cover group-hover:scale-105 transition duration-300"
                      loading="lazy"
                    />
                    <span className="absolute top-2 left-2 text-[10px] font-bold px-1.5 py-0.5 rounded bg-black/60 backdrop-blur text-amber-300">
                      ★ {anime.rating}
                    </span>
                  </div>
                  <h4 className="text-xs font-semibold text-slate-200 truncate group-hover:text-pink-400">
                    {anime.title}
                  </h4>
                  <p className="text-[10px] text-slate-500">{anime.year} • {anime.episodes.length} Eps</p>
                </div>
              ))}
            </div>
          </section>
        </main>
      )}
    </div>
  );
}
