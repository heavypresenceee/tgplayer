"""
MusicWeb Server — FastAPI + yt-dlp hybrid streaming
SoundCloud first, YouTube Music fallback
"""
import asyncio, logging, os, json, re
from typing import Optional
import aiohttp, aiosqlite
from dotenv import load_dotenv
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

load_dotenv()
LASTFM_API_KEY = os.getenv("LASTFM_API_KEY", "")
SERVER_PORT    = int(os.getenv("PORT", "8000"))
DB_PATH        = os.getenv("DATABASE_PATH", "musicweb.db")

FILTER_TAGS = [
    "indian","hindi","bollywood","punjabi","tamil","telugu","bhojpuri",
    "kannada","malayalam","marathi","gujarati","bangla","nepali","pakistani",
    "kollywood","tollywood","desi","bhangra"
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

app = FastAPI(title="MusicWeb API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# In-memory session cache: user_id -> {artists: {name: count}, tracks_started: int}
session_cache: dict[str, dict] = {}

# ── DB ─────────────────────────────────────────────────────────────────────────

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS player_state (
                user_id    TEXT PRIMARY KEY,
                track_json TEXT,
                queue_json TEXT DEFAULT '[]',
                progress   INTEGER DEFAULT 0,
                is_playing INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS liked_tracks (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id    TEXT,
                track_json TEXT,
                added_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS playlists (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id    TEXT,
                name       TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS playlist_tracks (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                playlist_id INTEGER,
                track_json  TEXT,
                added_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS listen_history (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     TEXT,
                track_json  TEXT,
                listened_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS ban_list (
                id      INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT,
                term    TEXT,
                UNIQUE(user_id, term)
            );
        """)
        await db.commit()


def get_uid(request: Request) -> str:
    return request.headers.get("X-Web-User-Id", "anonymous")


async def db_get_state(uid: str) -> dict:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM player_state WHERE user_id=?", (uid,)) as c:
            row = await c.fetchone()
    if not row: return {}
    s = dict(row)
    if s.get("track_json"): s["track"] = json.loads(s["track_json"])
    if s.get("queue_json"): s["queue"] = json.loads(s["queue_json"])
    return s


async def db_save_state(uid, track, queue, progress=0, is_playing=False):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO player_state (user_id,track_json,queue_json,progress,is_playing)
            VALUES (?,?,?,?,?)
            ON CONFLICT(user_id) DO UPDATE SET
                track_json=excluded.track_json, queue_json=excluded.queue_json,
                progress=excluded.progress, is_playing=excluded.is_playing
        """, (uid, json.dumps(track,ensure_ascii=False),
              json.dumps(queue,ensure_ascii=False), progress, int(is_playing)))
        await db.commit()


async def db_add_history(uid, track):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO listen_history (user_id,track_json) VALUES (?,?)",
            (uid, json.dumps(track, ensure_ascii=False))
        )
        # Keep only last 50
        await db.execute("""
            DELETE FROM listen_history WHERE user_id=? AND id NOT IN (
                SELECT id FROM listen_history WHERE user_id=? ORDER BY listened_at DESC LIMIT 50
            )""", (uid, uid))
        await db.commit()


async def db_get_history(uid) -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT track_json FROM listen_history WHERE user_id=? ORDER BY listened_at DESC LIMIT 50",
            (uid,)
        ) as c:
            return [json.loads(r[0]) for r in await c.fetchall()]


async def db_toggle_like(uid, track) -> bool:
    tj = json.dumps(track, ensure_ascii=False, sort_keys=True)
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT id FROM liked_tracks WHERE user_id=? AND track_json=?", (uid, tj)
        ) as c:
            row = await c.fetchone()
        if row:
            await db.execute("DELETE FROM liked_tracks WHERE id=?", (row[0],))
            liked = False
        else:
            await db.execute(
                "INSERT INTO liked_tracks (user_id,track_json) VALUES (?,?)", (uid, tj)
            )
            liked = True
        await db.commit()
    return liked


async def db_is_liked(uid, track) -> bool:
    tj = json.dumps(track, ensure_ascii=False, sort_keys=True)
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT 1 FROM liked_tracks WHERE user_id=? AND track_json=?", (uid, tj)
        ) as c:
            return await c.fetchone() is not None


async def db_get_liked(uid) -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT track_json FROM liked_tracks WHERE user_id=? ORDER BY added_at DESC",
            (uid,)
        ) as c:
            return [json.loads(r[0]) for r in await c.fetchall()]


async def db_get_playlists(uid) -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM playlists WHERE user_id=? ORDER BY created_at DESC", (uid,)
        ) as c:
            pls = [dict(r) for r in await c.fetchall()]
        for pl in pls:
            async with db.execute(
                "SELECT track_json FROM playlist_tracks WHERE playlist_id=?", (pl["id"],)
            ) as c:
                pl["tracks"] = [json.loads(r[0]) for r in await c.fetchall()]
    return pls


async def db_create_playlist(uid, name) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute(
            "INSERT INTO playlists (user_id,name) VALUES (?,?)", (uid, name)
        )
        await db.commit()
        return c.lastrowid


async def db_add_to_playlist(pl_id, track):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO playlist_tracks (playlist_id,track_json) VALUES (?,?)",
            (pl_id, json.dumps(track, ensure_ascii=False))
        )
        await db.commit()


async def db_get_ban(uid) -> list[str]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT term FROM ban_list WHERE user_id='global' OR user_id=?", (uid,)
        ) as c:
            return [r[0].lower() for r in await c.fetchall()]


async def db_add_ban(uid, term):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO ban_list (user_id,term) VALUES (?,?)",
            (uid, term.lower())
        )
        await db.commit()


def is_banned(track, ban_list) -> bool:
    text = f"{track.get('artist','')} {track.get('title','')}".lower()
    return any(w in text for w in ban_list)


# ── Music sources ──────────────────────────────────────────────────────────────

def sec_fmt(s: int) -> str:
    m, sec = divmod(int(s), 60)
    return f"{m}:{sec:02d}"


async def deezer_search(query: str, limit: int = 10) -> list[dict]:
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                "https://api.deezer.com/search",
                params={"q": query, "limit": limit},
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                data = await r.json()
        return [{
            "title":        i.get("title", "?"),
            "artist":       i.get("artist", {}).get("name", "?"),
            "duration":     i.get("duration", 0),
            "duration_str": sec_fmt(i.get("duration", 0)),
            "cover_url":    i.get("album", {}).get("cover_big") or i.get("album", {}).get("cover_medium", ""),
            "source":       "Deezer",
            "source_id":    str(i.get("id", "")),
            "link":         i.get("link", ""),
            "preview_url":  i.get("preview", ""),
        } for i in data.get("data", [])]
    except Exception as e:
        log.error(f"Deezer search: {e}")
        return []


async def deezer_artist_top(artist: str, limit: int = 20) -> list[dict]:
    try:
        async with aiohttp.ClientSession() as s:
            # Search artist
            async with s.get(
                "https://api.deezer.com/search/artist",
                params={"q": artist, "limit": 1},
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                data = await r.json()
            artists = data.get("data", [])
            if not artists:
                return await deezer_search(artist, limit)
            artist_id = artists[0]["id"]
            # Get top tracks
            async with s.get(
                f"https://api.deezer.com/artist/{artist_id}/top",
                params={"limit": limit},
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                data = await r.json()
        return [{
            "title":        i.get("title", "?"),
            "artist":       i.get("artist", {}).get("name", artist),
            "duration":     i.get("duration", 0),
            "duration_str": sec_fmt(i.get("duration", 0)),
            "cover_url":    i.get("album", {}).get("cover_big") or i.get("album", {}).get("cover_medium", ""),
            "source":       "Deezer",
            "source_id":    str(i.get("id", "")),
            "link":         i.get("link", ""),
            "preview_url":  i.get("preview", ""),
        } for i in data.get("data", [])]
    except Exception as e:
        log.error(f"Deezer artist top: {e}")
        return []


async def deezer_chart(limit: int = 20) -> list[dict]:
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                "https://api.deezer.com/chart/0/tracks",
                params={"limit": limit},
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                data = await r.json()
        return [{
            "title":        i.get("title", "?"),
            "artist":       i.get("artist", {}).get("name", "?"),
            "duration":     i.get("duration", 0),
            "duration_str": sec_fmt(i.get("duration", 0)),
            "cover_url":    i.get("album", {}).get("cover_big", ""),
            "source":       "Deezer",
            "source_id":    str(i.get("id", "")),
            "link":         i.get("link", ""),
            "preview_url":  i.get("preview", ""),
        } for i in data.get("data", [])]
    except Exception as e:
        log.error(f"Deezer chart: {e}")
        return []


async def lastfm_similar(artist: str, title: str, limit: int = 20) -> list[dict]:
    if not LASTFM_API_KEY:
        return await deezer_artist_top(artist, limit)
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                "http://ws.audioscrobbler.com/2.0/",
                params={
                    "method": "track.getSimilar", "artist": artist, "track": title,
                    "api_key": LASTFM_API_KEY, "format": "json",
                    "limit": limit, "autocorrect": 1,
                },
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                data = await r.json()
        tracks = []
        for i in data.get("similartracks", {}).get("track", []):
            tags = [t.get("name","").lower() for t in i.get("toptags",{}).get("tag",[])]
            if any(ft in tags for ft in FILTER_TAGS):
                continue
            tracks.append({
                "title":        i.get("name", "?"),
                "artist":       i.get("artist", {}).get("name", "?"),
                "duration":     int(i.get("duration", 0)),
                "duration_str": sec_fmt(int(i.get("duration", 0))),
                "cover_url":    next(
                    (img["#text"] for img in i.get("image",[]) if img.get("size")=="extralarge"), ""
                ),
                "source":       "Last.fm",
                "source_id":    i.get("mbid",""),
                "link":         i.get("url",""),
                "preview_url":  "",
            })
        # Fill missing covers from Deezer
        for t in tracks:
            if not t["cover_url"]:
                dz = await deezer_search(f"{t['artist']} {t['title']}", 1)
                if dz:
                    t["cover_url"]  = dz[0]["cover_url"]
                    t["preview_url"] = dz[0]["preview_url"]
        return tracks
    except Exception as e:
        log.error(f"Last.fm similar: {e}")
        return await deezer_artist_top(artist, limit)


async def lastfm_artist_top(artist: str, limit: int = 20) -> list[dict]:
    if not LASTFM_API_KEY:
        return await deezer_artist_top(artist, limit)
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                "http://ws.audioscrobbler.com/2.0/",
                params={
                    "method": "artist.getTopTracks", "artist": artist,
                    "api_key": LASTFM_API_KEY, "format": "json", "limit": limit,
                },
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                data = await r.json()
        tracks = []
        for i in data.get("toptracks", {}).get("track", []):
            tracks.append({
                "title":        i.get("name", "?"),
                "artist":       artist,
                "duration":     int(i.get("duration", 0)),
                "duration_str": sec_fmt(int(i.get("duration", 0))),
                "cover_url":    next(
                    (img["#text"] for img in i.get("image",[]) if img.get("size")=="extralarge"), ""
                ),
                "source":       "Last.fm",
                "source_id":    i.get("mbid",""),
                "link":         i.get("url",""),
                "preview_url":  "",
            })
        # Fill covers
        for t in tracks:
            if not t["cover_url"]:
                dz = await deezer_search(f"{t['artist']} {t['title']}", 1)
                if dz:
                    t["cover_url"]   = dz[0]["cover_url"]
                    t["preview_url"] = dz[0]["preview_url"]
        return tracks
    except Exception as e:
        log.error(f"Last.fm artist top: {e}")
        return await deezer_artist_top(artist, limit)


async def yt_search(query: str, limit: int = 10) -> list[dict]:
    """Search YouTube Music via yt-dlp."""
    cmd = [
        "yt-dlp", f"ytsearch{limit}:{query}",
        "--dump-json", "--no-playlist", "--quiet", "--no-warnings",
        "--flat-playlist",
    ]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=20)
        tracks = []
        for line in stdout.decode().strip().split("\n"):
            if not line: continue
            try:
                d = json.loads(line)
                dur = d.get("duration") or 0
                tracks.append({
                    "title":        d.get("title", "?"),
                    "artist":       d.get("uploader", d.get("channel", "?")),
                    "duration":     int(dur),
                    "duration_str": sec_fmt(int(dur)),
                    "cover_url":    d.get("thumbnail", ""),
                    "source":       "YouTube",
                    "source_id":    d.get("id", ""),
                    "link":         d.get("webpage_url", ""),
                    "preview_url":  "",
                })
            except: continue
        return tracks
    except Exception as e:
        log.error(f"yt search: {e}")
        return []


# ── Hybrid streaming ───────────────────────────────────────────────────────────

async def get_stream_url(artist: str, title: str, platform: str = "auto") -> tuple[Optional[str], str]:
    """
    Returns (stream_url, actual_platform).
    Tries SoundCloud first, falls back to YouTube Music.
    """
    sc_query  = f"scsearch1:{artist} - {title}"
    yt_query  = f"ytsearch1:{artist} - {title}"

    async def try_source(query: str) -> Optional[str]:
        cmd = [
            "yt-dlp", query,
            "-f", "bestaudio",
            "--get-url",
            "--no-playlist",
            "--quiet",
            "--no-warnings",
        ]
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=30)
            url = stdout.decode().strip().split("\n")[0]
            return url if url.startswith("http") else None
        except Exception as e:
            log.warning(f"Stream attempt failed for {query}: {e}")
            return None

    if platform == "sc":
        url = await try_source(sc_query)
        return (url, "SC") if url else (None, "SC")

    if platform == "yt":
        url = await try_source(yt_query)
        return (url, "YT") if url else (None, "YT")

    # Auto: SC first, then YT
    url = await try_source(sc_query)
    if url:
        return url, "SC"
    log.info(f"SC failed, trying YT for: {artist} - {title}")
    url = await try_source(yt_query)
    return (url, "YT") if url else (None, "YT")


# ── Wave / recommendations ─────────────────────────────────────────────────────

def get_session(uid: str) -> dict:
    if uid not in session_cache:
        session_cache[uid] = {"artists": {}, "tracks_started": 0}
    return session_cache[uid]


async def get_wave_track(uid: str, current_artist: str, current_title: str) -> Optional[dict]:
    ban = await db_get_ban(uid)
    sess = get_session(uid)

    # Build candidate list from session artists + current track
    candidates = await lastfm_similar(current_artist, current_title, 30)

    # Also mix in top tracks from session artists
    for artist_name, count in sorted(sess["artists"].items(), key=lambda x: -x[1])[:3]:
        more = await lastfm_artist_top(artist_name, 10)
        candidates.extend(more)

    # Deduplicate
    seen = set()
    unique = []
    for t in candidates:
        k = f"{t['artist'].lower()}|||{t['title'].lower()}"
        if k not in seen:
            seen.add(k)
            unique.append(t)

    history = await db_get_history(uid)
    history_keys = {f"{t.get('artist','').lower()}|||{t.get('title','').lower()}" for t in history}

    for t in unique:
        if is_banned(t, ban): continue
        k = f"{t['artist'].lower()}|||{t['title'].lower()}"
        if k in history_keys: continue
        # Filter indian music by artist name heuristic
        if any(ft in t.get("artist","").lower() for ft in FILTER_TAGS): continue
        return t

    # Final fallback — Deezer chart
    chart = await deezer_chart(20)
    for t in chart:
        if not is_banned(t, ban):
            k = f"{t['artist'].lower()}|||{t['title'].lower()}"
            if k not in history_keys:
                return t
    return None


async def get_for_you(uid: str) -> dict:
    """Generate 'For You' page data based on session + liked tracks."""
    sess = get_session(uid)
    liked = await db_get_liked(uid)
    ban   = await db_get_ban(uid)

    # Get session artists
    session_artists = sorted(sess["artists"].items(), key=lambda x: -x[1])

    # Artist mixes (horizontal scroll cards)
    mixes = []
    for artist_name, count in session_artists[:5]:
        mixes.append({
            "name":   f"Микс {artist_name}",
            "artist": artist_name,
            "count":  count,
        })
    # Add liked artists
    liked_artists = {}
    for t in liked:
        a = t.get("artist","")
        liked_artists[a] = liked_artists.get(a,0) + 1
    for artist_name in sorted(liked_artists, key=lambda x: -liked_artists[x])[:3]:
        if not any(m["artist"] == artist_name for m in mixes):
            mixes.append({"name": f"Микс {artist_name}", "artist": artist_name, "count": liked_artists[artist_name]})

    # Recommendation track list (30 tracks)
    rec_tracks = []
    # From session artists
    for artist_name, _ in session_artists[:3]:
        more = await lastfm_artist_top(artist_name, 10)
        rec_tracks.extend(more)
    # From liked
    if liked:
        import random
        sample = random.sample(liked, min(3, len(liked)))
        for t in sample:
            more = await lastfm_similar(t["artist"], t["title"], 5)
            rec_tracks.extend(more)
    # Fallback chart
    if len(rec_tracks) < 10:
        rec_tracks.extend(await deezer_chart(20))

    # Deduplicate + filter
    seen = set()
    filtered = []
    for t in rec_tracks:
        k = f"{t['artist'].lower()}|||{t['title'].lower()}"
        if k in seen: continue
        if is_banned(t, ban): continue
        if any(ft in t.get("artist","").lower() for ft in FILTER_TAGS): continue
        seen.add(k)
        filtered.append(t)
        if len(filtered) >= 30: break

    return {"mixes": mixes, "tracks": filtered}


# ── FastAPI routes ──────────────────────────────────────────────────────────────

@app.get("/")
async def root():
    return {"status": "ok", "service": "MusicWeb API"}


@app.get("/api/track")
async def api_get_track(request: Request):
    uid   = get_uid(request)
    state = await db_get_state(uid)
    if not state or not state.get("track"):
        tracks = await deezer_chart(20)
        if tracks:
            await db_save_state(uid, tracks[0], tracks[1:], is_playing=False)
            liked = await db_is_liked(uid, tracks[0])
            return JSONResponse({**tracks[0], "queue": tracks[1:8],
                                  "progress": 0, "is_playing": False, "liked": liked})
        return JSONResponse({"error": "no tracks"}, status_code=500)
    track = state.get("track", {})
    queue = state.get("queue", [])
    liked = await db_is_liked(uid, track)
    return JSONResponse({**track, "queue": queue[:8],
                          "progress": state.get("progress", 0),
                          "is_playing": bool(state.get("is_playing")),
                          "liked": liked})


@app.get("/api/stream")
async def api_stream(request: Request, artist: str = "", title: str = "", platform: str = "auto"):
    """Hybrid streaming: SoundCloud -> YouTube Music fallback."""
    uid = get_uid(request)
    if not artist or not title:
        return JSONResponse({"error": "artist and title required"}, status_code=400)

    stream_url, actual_platform = await get_stream_url(artist, title, platform)

    if not stream_url:
        return JSONResponse({"error": "stream not found"}, status_code=404)

    log.info(f"Streaming [{actual_platform}]: {artist} - {title}")

    async def audio_gen():
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    stream_url,
                    timeout=aiohttp.ClientTimeout(total=600)
                ) as resp:
                    async for chunk in resp.content.iter_chunked(16384):
                        yield chunk
        except Exception as e:
            log.error(f"Stream error: {e}")

    return StreamingResponse(
        audio_gen(),
        media_type="audio/mpeg",
        headers={
            "X-Stream-Platform": actual_platform,
            "Accept-Ranges":     "bytes",
            "Cache-Control":     "no-cache",
            "Access-Control-Allow-Origin": "*",
        }
    )


@app.get("/api/stream/info")
async def api_stream_info(request: Request, artist: str = "", title: str = ""):
    """Get stream platform without streaming — for UI indicator."""
    _, plat = await get_stream_url(artist, title, "auto")
    return JSONResponse({"platform": plat})


@app.get("/api/search")
async def api_search(request: Request, q: str = "", source: str = "deezer", limit: int = 10):
    uid = get_uid(request)
    if not q.strip():
        return JSONResponse({"tracks": []})
    if source == "youtube":
        tracks = await yt_search(q, limit)
    elif source == "soundcloud":
        # yt-dlp SC search
        tracks = await yt_search(f"scsearch{limit}:{q}", limit)
        for t in tracks:
            t["source"] = "SoundCloud"
    else:
        tracks = await deezer_search(q, limit)
    return JSONResponse({"tracks": tracks})


@app.get("/api/artist/{artist_name}")
async def api_artist(artist_name: str, request: Request):
    """Get artist top tracks."""
    tracks = await lastfm_artist_top(artist_name, 20)
    if not tracks:
        tracks = await deezer_artist_top(artist_name, 20)
    return JSONResponse({"artist": artist_name, "tracks": tracks})


@app.get("/api/liked")
async def api_liked(request: Request):
    uid = get_uid(request)
    return JSONResponse({"tracks": await db_get_liked(uid)})


@app.get("/api/history")
async def api_history(request: Request):
    uid = get_uid(request)
    return JSONResponse({"tracks": await db_get_history(uid)})


@app.get("/api/playlists")
async def api_playlists(request: Request):
    uid = get_uid(request)
    return JSONResponse({"playlists": await db_get_playlists(uid)})


@app.get("/api/for-you")
async def api_for_you(request: Request):
    uid = get_uid(request)
    data = await get_for_you(uid)
    return JSONResponse(data)


@app.post("/api/action")
async def api_action(request: Request):
    uid  = get_uid(request)
    body = await request.json()
    action = body.get("action", "")
    state  = await db_get_state(uid)
    track  = state.get("track", {})
    queue  = state.get("queue", [])
    prog   = state.get("progress", 0)
    resp   = {"ok": True}

    if action == "play":
        await db_save_state(uid, track, queue, prog, True)
        # Track history + session
        if track:
            await db_add_history(uid, track)
            sess = get_session(uid)
            artist = track.get("artist","")
            if artist:
                sess["artists"][artist] = sess["artists"].get(artist, 0) + 1

    elif action == "pause":
        await db_save_state(uid, track, queue, prog, False)

    elif action == "seek":
        pos = int(body.get("position", 0))
        await db_save_state(uid, track, queue, pos, True)

    elif action == "progress_update":
        pos = int(body.get("position", 0))
        await db_save_state(uid, track, queue, pos, True)

    elif action == "session_listen":
        # Called when user listens >60% of a track
        t = body.get("track") or track
        if t:
            sess = get_session(uid)
            artist = t.get("artist","")
            if artist:
                sess["artists"][artist] = sess["artists"].get(artist, 0) + 3  # weighted

    elif action == "next":
        if queue:
            new_track = queue[0]
            new_queue = queue[1:]
            if len(new_queue) < 3:
                ban  = await db_get_ban(uid)
                more = await deezer_search(new_track["artist"], 5)
                hist = await db_get_history(uid)
                hist_keys = {f"{t.get('artist','').lower()}|||{t.get('title','').lower()}" for t in hist}
                for t in more:
                    k = f"{t['artist'].lower()}|||{t['title'].lower()}"
                    if not is_banned(t, ban) and k not in hist_keys:
                        new_queue.append(t)
            await db_save_state(uid, new_track, new_queue, 0, True)
            liked = await db_is_liked(uid, new_track)
            resp["track"] = {**new_track, "queue": new_queue[:8], "progress": 0,
                              "is_playing": True, "liked": liked}

    elif action == "prev":
        await db_save_state(uid, track, queue, 0, True)
        liked = await db_is_liked(uid, track)
        resp["track"] = {**track, "queue": queue[:8], "progress": 0,
                          "is_playing": True, "liked": liked}

    elif action == "wave":
        nt = await get_wave_track(uid, track.get("artist",""), track.get("title",""))
        if nt:
            more = await lastfm_similar(nt["artist"], nt["title"], 8)
            ban  = await db_get_ban(uid)
            nq   = [t for t in more if not is_banned(t, ban)][:8]
            await db_save_state(uid, nt, nq, 0, True)
            liked = await db_is_liked(uid, nt)
            resp["track"]   = {**nt, "queue": nq[:8], "progress": 0, "is_playing": True, "liked": liked}
            resp["message"] = f"🌊 {nt['artist']} — {nt['title']}"
        else:
            resp["message"] = "Не нашёл похожих треков 😕"

    elif action == "like":
        t     = body.get("track") or track
        liked = await db_toggle_like(uid, t)
        resp["liked"]   = liked
        resp["message"] = "❤️ Добавлено в избранное" if liked else "💔 Удалено из избранного"

    elif action == "ban":
        artist = (body.get("track") or track).get("artist","")
        if artist:
            await db_add_ban(uid, artist)
            resp["message"] = f"🚫 {artist} забанен"
            if queue:
                nt = queue[0]; nq = queue[1:]
                await db_save_state(uid, nt, nq, 0, True)
                liked = await db_is_liked(uid, nt)
                resp["track"] = {**nt, "queue": nq[:8], "progress": 0, "is_playing": True, "liked": liked}

    elif action == "play_track":
        nt = body.get("track")
        if nt:
            await db_save_state(uid, nt, queue, 0, True)
            await db_add_history(uid, nt)
            sess = get_session(uid)
            if nt.get("artist"):
                sess["artists"][nt["artist"]] = sess["artists"].get(nt["artist"],0) + 1
            liked = await db_is_liked(uid, nt)
            resp["track"] = {**nt, "queue": queue[:8], "progress": 0, "is_playing": True, "liked": liked}

    elif action == "jump":
        idx = int(body.get("index", 0))
        if 0 < idx <= len(queue):
            nt = queue[idx-1]; nq = queue[idx:]
            await db_save_state(uid, nt, nq, 0, True)
            liked = await db_is_liked(uid, nt)
            resp["track"] = {**nt, "queue": nq[:8], "progress": 0, "is_playing": True, "liked": liked}

    elif action == "create_playlist":
        name  = body.get("name", "Новый плейлист")
        pl_id = await db_create_playlist(uid, name)
        resp["playlist_id"] = pl_id
        resp["message"]     = f"✅ Плейлист «{name}» создан"

    elif action == "add_to_playlist":
        pl_id = body.get("playlist_id")
        t     = body.get("track") or track
        if pl_id:
            await db_add_to_playlist(pl_id, t)
            resp["message"] = "✅ Добавлено в плейлист"

    return JSONResponse(resp)


# ── Run ─────────────────────────────────────────────────────────────────────────

async def main():
    await init_db()
    cfg = uvicorn.Config(app, host="0.0.0.0", port=SERVER_PORT, log_level="info")
    await uvicorn.Server(cfg).serve()

if __name__ == "__main__":
    asyncio.run(main())
