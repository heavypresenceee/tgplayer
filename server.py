"""
MusicBot Server v2 — FastAPI + aiogram + yt-dlp streaming
"""
import asyncio, logging, os, json, tempfile, re, subprocess
from typing import Optional
import aiohttp
import aiosqlite
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

load_dotenv()

BOT_TOKEN      = os.getenv("BOT_TOKEN", "")
LASTFM_API_KEY = os.getenv("LASTFM_API_KEY", "")
WEBAPP_URL     = os.getenv("WEBAPP_URL", "")
SERVER_PORT    = int(os.getenv("PORT", "8000"))
DB_PATH        = os.getenv("DATABASE_PATH", "musicapp.db")

# Языки/теги для фильтрации нежелательной музыки
FILTER_TAGS = ["indian", "hindi", "bollywood", "punjabi", "tamil", "telugu",
                "bhojpuri", "kannada", "malayalam", "marathi", "gujarati"]

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="MusicBot API v2")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

bot = Bot(token=BOT_TOKEN)
dp  = Dispatcher(storage=MemoryStorage())

# ── DB ────────────────────────────────────────────────────────────────────────

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS player_state (
                user_id    INTEGER PRIMARY KEY,
                track_json TEXT,
                queue_json TEXT,
                progress   INTEGER DEFAULT 0,
                is_playing INTEGER DEFAULT 0,
                wave_mode  INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS liked_tracks (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id    INTEGER,
                track_json TEXT,
                added_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, track_json)
            );
            CREATE TABLE IF NOT EXISTS playlists (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id    INTEGER,
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
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id    INTEGER,
                track_key  TEXT,
                listened_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS ban_list (
                id      INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                term    TEXT,
                UNIQUE(user_id, term)
            );
        """)
        # Дефолтные стоп-слова
        for word in ["платина", "young trappa"]:
            await db.execute("INSERT OR IGNORE INTO ban_list (user_id, term) VALUES (0, ?)", (word,))
        await db.commit()


async def get_state(user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM player_state WHERE user_id=?", (user_id,)) as c:
            row = await c.fetchone()
    if not row: return {}
    s = dict(row)
    if s.get("track_json"): s["track"] = json.loads(s["track_json"])
    if s.get("queue_json"): s["queue"] = json.loads(s["queue_json"])
    return s


async def save_state(user_id, track, queue, progress=0, is_playing=False, wave_mode=False):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO player_state (user_id,track_json,queue_json,progress,is_playing,wave_mode)
            VALUES (?,?,?,?,?,?)
            ON CONFLICT(user_id) DO UPDATE SET
                track_json=excluded.track_json, queue_json=excluded.queue_json,
                progress=excluded.progress, is_playing=excluded.is_playing, wave_mode=excluded.wave_mode
        """, (user_id, json.dumps(track), json.dumps(queue), progress, int(is_playing), int(wave_mode)))
        await db.commit()


async def add_history(user_id, track):
    key = f"{track.get('artist','').lower()}|||{track.get('title','').lower()}"
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT INTO listen_history (user_id,track_key) VALUES (?,?)", (user_id, key))
        await db.execute("""DELETE FROM listen_history WHERE user_id=? AND id NOT IN (
            SELECT id FROM listen_history WHERE user_id=? ORDER BY listened_at DESC LIMIT 300)""", (user_id, user_id))
        await db.commit()


async def in_history(user_id, track):
    key = f"{track.get('artist','').lower()}|||{track.get('title','').lower()}"
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT 1 FROM listen_history WHERE user_id=? AND track_key=? LIMIT 1", (user_id, key)) as c:
            return await c.fetchone() is not None


async def get_ban(user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT term FROM ban_list WHERE user_id=0 OR user_id=?", (user_id,)) as c:
            return [r[0].lower() for r in await c.fetchall()]


async def add_ban(user_id, term):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR IGNORE INTO ban_list (user_id,term) VALUES (?,?)", (user_id, term.lower()))
        await db.commit()


def is_banned(track, ban_list):
    text = f"{track.get('artist','')} {track.get('title','')}".lower()
    return any(w in text for w in ban_list)


async def get_liked(user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT track_json FROM liked_tracks WHERE user_id=? ORDER BY added_at DESC", (user_id,)) as c:
            return [json.loads(r[0]) for r in await c.fetchall()]


async def toggle_like(user_id, track):
    tj = json.dumps(track, ensure_ascii=False, sort_keys=True)
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT 1 FROM liked_tracks WHERE user_id=? AND track_json=?", (user_id, tj)) as c:
            exists = await c.fetchone()
        if exists:
            await db.execute("DELETE FROM liked_tracks WHERE user_id=? AND track_json=?", (user_id, tj))
            liked = False
        else:
            await db.execute("INSERT OR IGNORE INTO liked_tracks (user_id,track_json) VALUES (?,?)", (user_id, tj))
            liked = True
        await db.commit()
    return liked


async def is_liked(user_id, track):
    tj = json.dumps(track, ensure_ascii=False, sort_keys=True)
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT 1 FROM liked_tracks WHERE user_id=? AND track_json=?", (user_id, tj)) as c:
            return await c.fetchone() is not None


async def get_playlists(user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM playlists WHERE user_id=? ORDER BY created_at DESC", (user_id,)) as c:
            pls = [dict(r) for r in await c.fetchall()]
        for pl in pls:
            async with db.execute("SELECT track_json FROM playlist_tracks WHERE playlist_id=?", (pl["id"],)) as c:
                pl["tracks"] = [json.loads(r[0]) for r in await c.fetchall()]
    return pls


async def create_playlist(user_id, name):
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("INSERT INTO playlists (user_id,name) VALUES (?,?)", (user_id, name))
        await db.commit()
        return c.lastrowid


async def add_to_playlist(playlist_id, track):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT INTO playlist_tracks (playlist_id,track_json) VALUES (?,?)",
                         (playlist_id, json.dumps(track, ensure_ascii=False)))
        await db.commit()

# ── Music sources ─────────────────────────────────────────────────────────────

def sec_fmt(s):
    m, sec = divmod(int(s), 60)
    return f"{m}:{sec:02d}"


async def deezer_search(query, limit=10):
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get("https://api.deezer.com/search", params={"q": query, "limit": limit},
                             timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await r.json()
        return [{
            "title": i.get("title","?"), "artist": i.get("artist",{}).get("name","?"),
            "duration": i.get("duration",0), "duration_str": sec_fmt(i.get("duration",0)),
            "cover_url": i.get("album",{}).get("cover_big") or i.get("album",{}).get("cover_medium"),
            "source": "Deezer", "source_id": str(i.get("id","")),
            "link": i.get("link",""), "preview_url": i.get("preview",""),
        } for i in data.get("data",[])]
    except Exception as e:
        logger.error(f"Deezer: {e}")
        return []


async def deezer_chart(limit=15):
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get("https://api.deezer.com/chart/0/tracks", params={"limit": limit},
                             timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await r.json()
        return [{
            "title": i.get("title","?"), "artist": i.get("artist",{}).get("name","?"),
            "duration": i.get("duration",0), "duration_str": sec_fmt(i.get("duration",0)),
            "cover_url": i.get("album",{}).get("cover_big"),
            "source": "Deezer", "source_id": str(i.get("id","")),
            "link": i.get("link",""), "preview_url": i.get("preview",""),
        } for i in data.get("data",[])]
    except Exception as e:
        logger.error(f"Deezer chart: {e}")
        return []


async def lastfm_similar(artist, title, limit=30):
    if not LASTFM_API_KEY:
        return await deezer_search(artist, limit)
    try:
        params = {"method":"track.getSimilar","artist":artist,"track":title,
                  "api_key":LASTFM_API_KEY,"format":"json","limit":limit,"autocorrect":1}
        async with aiohttp.ClientSession() as s:
            async with s.get("http://ws.audioscrobbler.com/2.0/", params=params,
                             timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await r.json()
        tracks = []
        for i in data.get("similartracks",{}).get("track",[]):
            t = {
                "title": i.get("name","?"), "artist": i.get("artist",{}).get("name","?"),
                "duration": int(i.get("duration",0)), "duration_str": sec_fmt(int(i.get("duration",0))),
                "cover_url": next((img["#text"] for img in i.get("image",[]) if img.get("size")=="extralarge"), None),
                "source": "Last.fm", "source_id": i.get("mbid",""),
                "link": i.get("url",""), "preview_url": "",
            }
            # Фильтруем индийскую музыку по тегам
            tags_raw = i.get("toptags",{}).get("tag",[])
            tags = [tg.get("name","").lower() for tg in tags_raw]
            if any(ft in tags for ft in FILTER_TAGS):
                continue
            tracks.append(t)
        return tracks
    except Exception as e:
        logger.error(f"Last.fm: {e}")
        return []


async def get_yt_stream_url(artist, title, sc_link=None):
    """Получить прямую ссылку на аудиопоток через yt-dlp."""
    source = sc_link if sc_link and "soundcloud.com" in sc_link else f"ytsearch1:{artist} - {title}"
    cmd = ["yt-dlp", source, "-f", "bestaudio", "--get-url", "--no-playlist", "--quiet", "--no-warnings"]
    try:
        proc = await asyncio.create_subprocess_exec(*cmd,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=30)
        url = stdout.decode().strip().split("\n")[0]
        return url if url.startswith("http") else None
    except Exception as e:
        logger.error(f"yt-dlp stream: {e}")
        return None


async def get_wave_track(user_id, artist, title):
    ban = await get_ban(user_id)
    candidates = await lastfm_similar(artist, title, 40)
    for t in candidates:
        if is_banned(t, ban): continue
        if await in_history(user_id, t): continue
        return t
    # Фолбек — Deezer поиск по артисту
    results = await deezer_search(artist, 10)
    for t in results:
        if not is_banned(t, ban) and not await in_history(user_id, t):
            return t
    return None

# ── FastAPI routes ────────────────────────────────────────────────────────────

@app.get("/")
async def root():
    return {"status": "ok"}


@app.get("/api/track")
async def api_get_track(user_id: int = 0):
    state = await get_state(user_id)
    if not state or not state.get("track"):
        tracks = await deezer_chart(15)
        if tracks:
            await save_state(user_id, tracks[0], tracks[1:], is_playing=False)
            liked = await is_liked(user_id, tracks[0])
            return JSONResponse({**tracks[0], "queue": tracks[1:4], "progress": 0,
                                  "is_playing": False, "liked": liked})
        return JSONResponse({"error": "no tracks"}, status_code=500)
    track = state.get("track", {})
    queue = state.get("queue", [])
    liked = await is_liked(user_id, track)
    return JSONResponse({**track, "queue": queue[:3], "progress": state.get("progress", 0),
                          "is_playing": bool(state.get("is_playing")), "liked": liked})


@app.get("/api/stream")
async def api_stream(user_id: int = 0, title: str = "", artist: str = "", sc_link: str = ""):
    """Стриминг аудио через yt-dlp — главный эндпоинт для воспроизведения."""
    stream_url = await get_yt_stream_url(artist, title, sc_link or None)
    if not stream_url:
        return JSONResponse({"error": "stream not found"}, status_code=404)

    # Обновляем состояние — трек играет
    state = await get_state(user_id)
    if state.get("track"):
        await save_state(user_id, state["track"], state.get("queue", []),
                         state.get("progress", 0), is_playing=True)

    # Проксируем аудиопоток через наш сервер чтобы обойти CORS
    async def audio_generator():
        async with aiohttp.ClientSession() as session:
            async with session.get(stream_url, timeout=aiohttp.ClientTimeout(total=300)) as resp:
                async for chunk in resp.content.iter_chunked(8192):
                    yield chunk

    return StreamingResponse(
        audio_generator(),
        media_type="audio/mpeg",
        headers={
            "Accept-Ranges": "bytes",
            "Cache-Control": "no-cache",
            "Access-Control-Allow-Origin": "*",
        }
    )


@app.get("/api/search")
async def api_search(q: str = "", user_id: int = 0):
    if not q.strip():
        return JSONResponse({"tracks": []})
    tracks = await deezer_search(q, 10)
    return JSONResponse({"tracks": tracks})


@app.get("/api/liked")
async def api_liked(user_id: int = 0):
    tracks = await get_liked(user_id)
    return JSONResponse({"tracks": tracks})


@app.get("/api/playlists")
async def api_playlists(user_id: int = 0):
    pls = await get_playlists(user_id)
    return JSONResponse({"playlists": pls})


@app.post("/api/action")
async def api_action(request: Request):
    body     = await request.json()
    user_id  = body.get("user_id", 0)
    action   = body.get("action", "")
    state    = await get_state(user_id)
    track    = state.get("track", {})
    queue    = state.get("queue", [])
    progress = state.get("progress", 0)
    resp     = {"ok": True}

    if action == "play":
        await save_state(user_id, track, queue, progress, True)

    elif action == "pause":
        await save_state(user_id, track, queue, progress, False)

    elif action == "seek":
        pos = int(body.get("position", 0))
        await save_state(user_id, track, queue, pos, True)

    elif action == "next":
        if queue:
            await add_history(user_id, track)
            new_track = queue[0]
            new_queue = queue[1:]
            if len(new_queue) < 2:
                more = await deezer_search(new_track["artist"], 5)
                ban  = await get_ban(user_id)
                for t in more:
                    if not is_banned(t, ban) and not await in_history(user_id, t):
                        new_queue.append(t)
            await save_state(user_id, new_track, new_queue, 0, True)
            liked = await is_liked(user_id, new_track)
            resp["track"] = {**new_track, "queue": new_queue[:3], "progress": 0,
                              "is_playing": True, "liked": liked}

    elif action == "prev":
        await save_state(user_id, track, queue, 0, True)
        liked = await is_liked(user_id, track)
        resp["track"] = {**track, "queue": queue[:3], "progress": 0, "is_playing": True, "liked": liked}

    elif action == "wave":
        artist = track.get("artist","")
        title  = track.get("title","")
        if artist:
            next_t = await get_wave_track(user_id, artist, title)
            if next_t:
                await add_history(user_id, track)
                more = await lastfm_similar(next_t["artist"], next_t["title"], 5)
                ban  = await get_ban(user_id)
                nq   = [t for t in more if not is_banned(t, ban)][:4]
                await save_state(user_id, next_t, nq, 0, True, True)
                liked = await is_liked(user_id, next_t)
                resp["track"]   = {**next_t, "queue": nq[:3], "progress": 0, "is_playing": True, "liked": liked}
                resp["message"] = f"🌊 {next_t['artist']}"
                try:
                    await bot.send_message(user_id,
                        f"🌊 <b>Волна:</b> {next_t['artist']} — {next_t['title']}", parse_mode="HTML")
                except: pass
            else:
                resp["message"] = "Не нашёл похожих треков 😕"

    elif action == "ban":
        artist = track.get("artist","")
        if artist:
            await add_ban(user_id, artist)
            resp["message"] = f"🚫 {artist} забанен"
            try:
                await bot.send_message(user_id, f"🚫 <b>{artist}</b> добавлен в чёрный список", parse_mode="HTML")
            except: pass
            if queue:
                nt = queue[0]; nq = queue[1:]
                await save_state(user_id, nt, nq, 0, True)
                liked = await is_liked(user_id, nt)
                resp["track"] = {**nt, "queue": nq[:3], "progress": 0, "is_playing": True, "liked": liked}

    elif action == "like":
        t = body.get("track") or track
        liked = await toggle_like(user_id, t)
        resp["liked"] = liked
        resp["message"] = "❤️ Добавлено" if liked else "💔 Удалено из любимых"

    elif action == "play_track":
        new_track = body.get("track")
        if new_track:
            await save_state(user_id, new_track, queue, 0, True)
            liked = await is_liked(user_id, new_track)
            resp["track"] = {**new_track, "queue": queue[:3], "progress": 0, "is_playing": True, "liked": liked}

    elif action == "add_to_playlist":
        pl_id = body.get("playlist_id")
        t     = body.get("track") or track
        if pl_id:
            await add_to_playlist(pl_id, t)
            resp["message"] = "✅ Добавлено в плейлист"

    elif action == "create_playlist":
        name = body.get("name","Новый плейлист")
        pl_id = await create_playlist(user_id, name)
        resp["playlist_id"] = pl_id
        resp["message"] = f"✅ Плейлист «{name}» создан"

    elif action == "jump":
        idx = int(body.get("index", 0))
        if 0 < idx <= len(queue):
            await add_history(user_id, track)
            nt = queue[idx-1]; nq = queue[idx:]
            await save_state(user_id, nt, nq, 0, True)
            liked = await is_liked(user_id, nt)
            resp["track"] = {**nt, "queue": nq[:3], "progress": 0, "is_playing": True, "liked": liked}

    return JSONResponse(resp)


# ── Bot handlers ──────────────────────────────────────────────────────────────

@dp.message(Command("start"))
async def cmd_start(msg: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🎵 Открыть плеер", web_app=WebAppInfo(url=WEBAPP_URL))
    ]]) if WEBAPP_URL else None
    await msg.answer(
        "🎵 <b>MusicBot</b>\n\n/ban [артист] — чёрный список\n/search [запрос] — поиск",
        parse_mode="HTML", reply_markup=kb)


@dp.message(Command("ban"))
async def cmd_ban(msg: Message):
    args = msg.text.split(maxsplit=1)
    if len(args) < 2:
        await msg.answer("Использование: /ban [артист]")
        return
    await add_ban(msg.from_user.id, args[1].strip())
    await msg.answer(f"🚫 «{args[1].strip()}» в бан-листе")


@dp.message(Command("search"))
async def cmd_search_bot(msg: Message):
    args = msg.text.split(maxsplit=1)
    if len(args) < 2:
        await msg.answer("Использование: /search [запрос]")
        return
    tracks = await deezer_search(args[1].strip(), 5)
    if not tracks:
        await msg.answer("Не найдено")
        return
    await save_state(msg.from_user.id, tracks[0], tracks[1:], is_playing=True)
    lines = [f"🎵 <b>Результаты:</b>\n"]
    for i, t in enumerate(tracks, 1):
        lines.append(f"{i}. {t['artist']} — {t['title']} ({t['duration_str']})")
    await msg.answer("\n".join(lines), parse_mode="HTML")


# ── Run ───────────────────────────────────────────────────────────────────────

async def run_bot():
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

async def run_server():
    cfg = uvicorn.Config(app, host="0.0.0.0", port=SERVER_PORT, log_level="warning")
    await uvicorn.Server(cfg).serve()

async def main():
    await init_db()
    await asyncio.gather(run_bot(), run_server())

if __name__ == "__main__":
    asyncio.run(main())
