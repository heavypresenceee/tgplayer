"""
MusicBot — главный файл сервера.
Запускает одновременно Telegram-бота (aiogram) и веб-сервер (FastAPI),
который отдаёт данные для Mini App плеера.
"""

import asyncio
import logging
import os
import json
import tempfile
import re
from typing import Optional

# ── Telegram / aiogram ────────────────────────────────────────────────────────
from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton,
    WebAppInfo, CallbackQuery,
)
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage

# ── FastAPI — веб-сервер для Mini App ─────────────────────────────────────────
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# ── Музыкальные источники ─────────────────────────────────────────────────────
import aiohttp

# ── База данных (простой JSON-файл, не нужен сервер) ─────────────────────────
import aiosqlite

from dotenv import load_dotenv
load_dotenv()  # Загружаем .env файл

# ─────────────────────────────────────────────────────────────────────────────
# НАСТРОЙКИ — заполни в файле .env
# ─────────────────────────────────────────────────────────────────────────────
BOT_TOKEN      = os.getenv("BOT_TOKEN", "")           # Токен от BotFather
LASTFM_API_KEY = os.getenv("LASTFM_API_KEY", "")      # Ключ Last.fm (опционально)
WEBAPP_URL     = os.getenv("WEBAPP_URL", "")           # URL твоего index.html (Netlify/Vercel)
SERVER_PORT    = int(os.getenv("PORT", "8000"))        # Порт сервера

# Стоп-слова — треки с этими словами в названии/артисте пропускаются автоматически
DEFAULT_STOP_WORDS = ["платина", "young trappa", "трофимов"]

# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ── Инициализация FastAPI ─────────────────────────────────────────────────────
app = FastAPI(title="MusicBot API")

# CORS — разрешаем запросы из браузера (нужно для Mini App)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Инициализация aiogram бота ────────────────────────────────────────────────
bot = Bot(token=BOT_TOKEN)
dp  = Dispatcher(storage=MemoryStorage())

# ─────────────────────────────────────────────────────────────────────────────
# БАЗА ДАННЫХ
# ─────────────────────────────────────────────────────────────────────────────

DB_PATH = os.getenv("DATABASE_PATH", "musicapp.db")

async def init_db():
    """Создать таблицы если не существуют."""
    async with aiosqlite.connect(DB_PATH) as db:
        # Состояние плеера каждого пользователя
        await db.execute("""
            CREATE TABLE IF NOT EXISTS player_state (
                user_id     INTEGER PRIMARY KEY,
                track_json  TEXT,      -- JSON с данными текущего трека
                queue_json  TEXT,      -- JSON со списком очереди
                progress    INTEGER DEFAULT 0,
                is_playing  INTEGER DEFAULT 0,
                wave_mode   INTEGER DEFAULT 0
            )
        """)
        # История прослушивания (чтобы не повторять треки в Волне)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS listen_history (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id   INTEGER,
                track_key TEXT,        -- "артист|||название"
                listened_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Чёрный список артистов
        await db.execute("""
            CREATE TABLE IF NOT EXISTS ban_list (
                id      INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                term    TEXT,          -- Имя артиста или стоп-слово
                UNIQUE(user_id, term)
            )
        """)
        # Добавляем стоп-слова по умолчанию (глобальные, user_id=0)
        for word in DEFAULT_STOP_WORDS:
            await db.execute(
                "INSERT OR IGNORE INTO ban_list (user_id, term) VALUES (0, ?)",
                (word.lower(),)
            )
        await db.commit()


async def get_player_state(user_id: int) -> dict:
    """Получить состояние плеера пользователя."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM player_state WHERE user_id = ?", (user_id,)) as cur:
            row = await cur.fetchone()
    if not row:
        return {}
    state = dict(row)
    if state.get("track_json"):
        state["track"] = json.loads(state["track_json"])
    if state.get("queue_json"):
        state["queue"] = json.loads(state["queue_json"])
    return state


async def save_player_state(user_id: int, track: dict, queue: list, progress: int = 0, is_playing: bool = False, wave_mode: bool = False):
    """Сохранить состояние плеера."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO player_state (user_id, track_json, queue_json, progress, is_playing, wave_mode)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                track_json = excluded.track_json,
                queue_json = excluded.queue_json,
                progress   = excluded.progress,
                is_playing = excluded.is_playing,
                wave_mode  = excluded.wave_mode
        """, (user_id, json.dumps(track), json.dumps(queue), progress, int(is_playing), int(wave_mode)))
        await db.commit()


async def add_to_history(user_id: int, track: dict):
    """Добавить трек в историю прослушивания."""
    key = f"{track.get('artist','').lower()}|||{track.get('title','').lower()}"
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO listen_history (user_id, track_key) VALUES (?, ?)",
            (user_id, key)
        )
        # Оставляем только последние 200 треков в истории
        await db.execute("""
            DELETE FROM listen_history WHERE user_id = ? AND id NOT IN (
                SELECT id FROM listen_history WHERE user_id = ? ORDER BY listened_at DESC LIMIT 200
            )
        """, (user_id, user_id))
        await db.commit()


async def is_in_history(user_id: int, track: dict) -> bool:
    """Проверить, слушал ли пользователь этот трек недавно."""
    key = f"{track.get('artist','').lower()}|||{track.get('title','').lower()}"
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT 1 FROM listen_history WHERE user_id = ? AND track_key = ? LIMIT 1",
            (user_id, key)
        ) as cur:
            return await cur.fetchone() is not None


async def get_ban_list(user_id: int) -> list[str]:
    """Получить все запрещённые слова (глобальные + личные пользователя)."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT term FROM ban_list WHERE user_id = 0 OR user_id = ?",
            (user_id,)
        ) as cur:
            rows = await cur.fetchall()
    return [r[0].lower() for r in rows]


async def add_to_ban(user_id: int, term: str):
    """Добавить в чёрный список."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO ban_list (user_id, term) VALUES (?, ?)",
            (user_id, term.lower())
        )
        await db.commit()


def is_banned(track: dict, ban_list: list[str]) -> bool:
    """Проверить трек на стоп-слова."""
    text = f"{track.get('artist','')} {track.get('title','')}".lower()
    return any(word in text for word in ban_list)


# ─────────────────────────────────────────────────────────────────────────────
# МУЗЫКАЛЬНЫЕ ИСТОЧНИКИ
# ─────────────────────────────────────────────────────────────────────────────

def _ms_to_sec(ms: int) -> int:
    return ms // 1000

def _sec_fmt(sec: int) -> str:
    m, s = divmod(sec, 60)
    return f"{m}:{s:02d}"


async def deezer_search(query: str, limit: int = 10) -> list[dict]:
    """Поиск треков на Deezer (без API-ключа)."""
    url = "https://api.deezer.com/search"
    params = {"q": query, "limit": limit}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                data = await resp.json()
        result = []
        for item in data.get("data", []):
            result.append({
                "title":       item.get("title", "Unknown"),
                "artist":      item.get("artist", {}).get("name", "Unknown"),
                "duration":    item.get("duration", 0),
                "duration_str": _sec_fmt(item.get("duration", 0)),
                "preview_url": item.get("preview"),
                "cover_url":   item.get("album", {}).get("cover_big") or item.get("album", {}).get("cover_medium"),
                "source":      "Deezer",
                "link":        item.get("link"),
            })
        return result
    except Exception as e:
        logger.error(f"Deezer search error: {e}")
        return []


async def lastfm_similar(artist: str, track_title: str, limit: int = 20) -> list[dict]:
    """Получить похожие треки через Last.fm (для Волны)."""
    if not LASTFM_API_KEY:
        # Если ключа нет — ищем похожих через Deezer
        results = await deezer_search(artist, limit=limit)
        return [t for t in results if t["artist"].lower() != artist.lower()]

    url = "http://ws.audioscrobbler.com/2.0/"
    params = {
        "method":   "track.getSimilar",
        "artist":   artist,
        "track":    track_title,
        "api_key":  LASTFM_API_KEY,
        "format":   "json",
        "limit":    limit,
        "autocorrect": 1,
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                data = await resp.json()

        tracks = []
        for item in data.get("similartracks", {}).get("track", []):
            tracks.append({
                "title":       item.get("name", "Unknown"),
                "artist":      item.get("artist", {}).get("name", "Unknown"),
                "duration":    int(item.get("duration", 0)),
                "duration_str": _sec_fmt(int(item.get("duration", 0))),
                "preview_url": None,
                "cover_url":   next(
                    (img["#text"] for img in item.get("image", []) if img.get("size") == "extralarge"),
                    None
                ),
                "source":      "Last.fm",
                "link":        item.get("url"),
            })
        # Дополняем обложки через Deezer если нет
        for t in tracks:
            if not t["cover_url"]:
                dz = await deezer_search(f"{t['artist']} {t['title']}", limit=1)
                if dz:
                    t["cover_url"]   = dz[0]["cover_url"]
                    t["preview_url"] = dz[0]["preview_url"]
        return tracks

    except Exception as e:
        logger.error(f"Last.fm similar error: {e}")
        return []


async def download_track(artist: str, title: str, sc_link: str = None) -> Optional[str]:
    """Скачать трек через yt-dlp (SC или YouTube). Возвращает путь к mp3."""
    tmp_dir  = tempfile.mkdtemp()
    out_tmpl = os.path.join(tmp_dir, "track.%(ext)s")

    source = sc_link if sc_link and "soundcloud.com" in sc_link else f"ytsearch1:{artist} - {title} audio"
    cmd = [
        "yt-dlp", source,
        "-x", "--audio-format", "mp3", "--audio-quality", "5",
        "-o", out_tmpl, "--no-playlist", "--quiet", "--max-filesize", "50m",
    ]
    try:
        proc = await asyncio.create_subprocess_exec(*cmd,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        await asyncio.wait_for(proc.communicate(), timeout=120)
        for f in os.listdir(tmp_dir):
            if f.endswith(".mp3"):
                return os.path.join(tmp_dir, f)
    except Exception as e:
        logger.error(f"yt-dlp error: {e}")
    return None


# ─────────────────────────────────────────────────────────────────────────────
# ВОЛНА — алгоритм рекомендаций
# ─────────────────────────────────────────────────────────────────────────────

async def get_wave_track(user_id: int, current_artist: str, current_title: str) -> Optional[dict]:
    """
    Найти следующий трек для Волны:
    1. Получаем похожие треки через Last.fm или Deezer
    2. Проверяем чёрный список
    3. Проверяем историю (не повторяем)
    4. Возвращаем первый подходящий трек
    """
    ban_list = await get_ban_list(user_id)
    candidates = await lastfm_similar(current_artist, current_title, limit=30)

    for track in candidates:
        # Пропускаем забаненных
        if is_banned(track, ban_list):
            logger.info(f"Wave: пропускаем забаненный трек {track['artist']} - {track['title']}")
            continue
        # Пропускаем уже прослушанные
        if await is_in_history(user_id, track):
            logger.info(f"Wave: пропускаем трек из истории {track['artist']} - {track['title']}")
            continue
        # Нашли подходящий!
        logger.info(f"Wave: следующий трек {track['artist']} - {track['title']}")
        return track

    # Если не нашли ничего — берём просто поиск по артисту
    results = await deezer_search(current_artist, limit=10)
    for track in results:
        if not is_banned(track, ban_list) and not await is_in_history(user_id, track):
            return track

    return None


# ─────────────────────────────────────────────────────────────────────────────
# FASTAPI ЭНДПОИНТЫ — API для Mini App
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/")
async def root():
    """Проверка что сервер работает."""
    return {"status": "ok", "message": "MusicBot API is running"}


@app.get("/api/track")
async def get_track(user_id: int = 0):
    """
    Отдать данные о текущем треке пользователю.
    Mini App вызывает этот эндпоинт при открытии.
    """
    state = await get_player_state(user_id)

    if not state or not state.get("track"):
        # Если плеер пустой — загружаем чарты Deezer
        tracks = await deezer_search("top hits 2024", limit=10)
        if tracks:
            current = tracks[0]
            queue   = tracks[1:4]
            await save_player_state(user_id, current, queue, is_playing=False)
            return JSONResponse({**current, "queue": queue, "progress": 0, "is_playing": False})
        return JSONResponse({"error": "Не удалось загрузить треки"}, status_code=500)

    track    = state.get("track", {})
    queue    = state.get("queue", [])
    progress = state.get("progress", 0)
    playing  = bool(state.get("is_playing", False))

    return JSONResponse({**track, "queue": queue[:3], "progress": progress, "is_playing": playing})


@app.post("/api/action")
async def handle_action(request: Request):
    """
    Обработать действие из Mini App:
    play, pause, next, prev, wave, ban, seek, jump
    """
    body    = await request.json()
    user_id = body.get("user_id", 0)
    action  = body.get("action", "")

    state   = await get_player_state(user_id)
    track   = state.get("track", {})
    queue   = state.get("queue", [])
    progress = state.get("progress", 0)

    response = {"ok": True}

    if action == "play":
        await save_player_state(user_id, track, queue, progress, is_playing=True)
        response["message"] = "▶️ Воспроизведение"

    elif action == "pause":
        await save_player_state(user_id, track, queue, progress, is_playing=False)
        response["message"] = "⏸ Пауза"

    elif action == "next":
        # Переключить на следующий трек в очереди
        if queue:
            await add_to_history(user_id, track)
            new_track = queue[0]
            new_queue = queue[1:]

            # Если очередь заканчивается — подгружаем ещё
            if len(new_queue) < 2:
                more = await deezer_search(f"{new_track['artist']}", limit=5)
                ban_list = await get_ban_list(user_id)
                for t in more:
                    if not is_banned(t, ban_list) and not await is_in_history(user_id, t):
                        new_queue.append(t)

            await save_player_state(user_id, new_track, new_queue, 0, is_playing=True)
            response["track"] = {**new_track, "queue": new_queue[:3], "progress": 0, "is_playing": True}
        else:
            response["message"] = "Очередь пуста"

    elif action == "prev":
        # Назад — просто сбросить прогресс (или реализовать историю)
        await save_player_state(user_id, track, queue, 0, is_playing=True)
        response["track"] = {**track, "queue": queue[:3], "progress": 0, "is_playing": True}

    elif action == "wave":
        # Запустить Волну — найти похожие треки
        artist = track.get("artist", "")
        title  = track.get("title", "")

        if not artist:
            response["message"] = "Сначала выбери трек"
        else:
            next_track = await get_wave_track(user_id, artist, title)
            if next_track:
                await add_to_history(user_id, track)

                # Генерируем очередь из похожих треков
                more = await lastfm_similar(next_track["artist"], next_track["title"], limit=5)
                ban_list = await get_ban_list(user_id)
                new_queue = [t for t in more if not is_banned(t, ban_list)][:4]

                await save_player_state(user_id, next_track, new_queue, 0, is_playing=True, wave_mode=True)
                response["track"]   = {**next_track, "queue": new_queue[:3], "progress": 0, "is_playing": True}
                response["message"] = f"🌊 Волна: {next_track['artist']}"

                # Уведомляем пользователя в Telegram
                try:
                    await bot.send_message(user_id, f"🌊 <b>Волна запущена!</b>\n\n🎵 {next_track['artist']} — {next_track['title']}", parse_mode="HTML")
                except Exception:
                    pass
            else:
                response["message"] = "😕 Не удалось найти похожие треки"

    elif action == "ban":
        # Добавить артиста в чёрный список
        artist = track.get("artist", "")
        if artist:
            await add_to_ban(user_id, artist)
            response["message"] = f"🚫 {artist} добавлен в бан-лист"
            # Уведомление в Telegram
            try:
                await bot.send_message(user_id, f"🚫 <b>{artist}</b> добавлен в чёрный список.\n\nТреки этого артиста больше не будут появляться в Волне.", parse_mode="HTML")
            except Exception:
                pass
            # Переключаем трек
            if queue:
                new_track = queue[0]
                new_queue = queue[1:]
                await save_player_state(user_id, new_track, new_queue, 0, is_playing=True)
                response["track"] = {**new_track, "queue": new_queue[:3], "progress": 0, "is_playing": True}
        else:
            response["message"] = "Нет текущего артиста"

    elif action == "seek":
        # Перемотка
        position = int(body.get("position", 0))
        await save_player_state(user_id, track, queue, position, is_playing=True)

    elif action == "jump":
        # Перейти к треку в очереди по индексу
        index = int(body.get("index", 0))
        if 0 < index <= len(queue):
            await add_to_history(user_id, track)
            new_track = queue[index - 1]
            new_queue = queue[index:]
            await save_player_state(user_id, new_track, new_queue, 0, is_playing=True)
            response["track"] = {**new_track, "queue": new_queue[:3], "progress": 0, "is_playing": True}

    return JSONResponse(response)


@app.get("/api/search")
async def api_search(q: str, user_id: int = 0):
    """Поиск треков — для будущего расширения Mini App."""
    tracks = await deezer_search(q, limit=10)
    return JSONResponse({"tracks": tracks})


# ─────────────────────────────────────────────────────────────────────────────
# TELEGRAM BOT HANDLERS
# ─────────────────────────────────────────────────────────────────────────────

@dp.message(Command("start"))
async def cmd_start(message: Message):
    """Команда /start — отправить кнопку для открытия Mini App плеера."""
    if not WEBAPP_URL:
        await message.answer(
            "⚠️ WEBAPP_URL не настроен. Задеплой index.html на Netlify и добавь ссылку в .env",
        )
        return

    keyboard = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text="🎵 Открыть плеер",
            web_app=WebAppInfo(url=WEBAPP_URL)  # Открывает Mini App
        )
    ]])

    await message.answer(
        "🎵 <b>MusicBot Player</b>\n\n"
        "Нажми кнопку ниже, чтобы открыть плеер!\n\n"
        "Доступные команды:\n"
        "/ban [артист] — добавить в чёрный список\n"
        "/wave — запустить Волну в текущем чате\n"
        "/search [запрос] — найти трек",
        parse_mode="HTML",
        reply_markup=keyboard,
    )


@dp.message(Command("ban"))
async def cmd_ban(message: Message):
    """Команда /ban [артист] — добавить артиста в чёрный список."""
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.answer("Использование: /ban [имя артиста]\nПример: /ban Платина")
        return

    term = args[1].strip()
    await add_to_ban(message.from_user.id, term)
    await message.answer(f"🚫 «<b>{term}</b>» добавлен в чёрный список.\n\nТреки с этим именем больше не будут появляться в Волне.", parse_mode="HTML")


@dp.message(Command("wave"))
async def cmd_wave(message: Message):
    """Команда /wave — запустить Волну по текущему треку."""
    state = await get_player_state(message.from_user.id)
    track = state.get("track")

    if not track:
        await message.answer("Сначала открой плеер и выбери трек!")
        return

    artist = track.get("artist", "")
    title  = track.get("title", "")

    msg = await message.answer(f"🌊 Ищу похожие треки на <b>{artist} — {title}</b>...", parse_mode="HTML")
    next_track = await get_wave_track(message.from_user.id, artist, title)

    if next_track:
        queue = await lastfm_similar(next_track["artist"], next_track["title"], limit=5)
        await save_player_state(message.from_user.id, next_track, queue, 0, is_playing=True, wave_mode=True)
        await msg.edit_text(
            f"🌊 <b>Волна запущена!</b>\n\n"
            f"🎵 <b>{next_track['artist']}</b> — <i>{next_track['title']}</i>\n"
            f"⏱ {next_track.get('duration_str', '?')}\n\n"
            f"Открой плеер, чтобы слушать!",
            parse_mode="HTML",
        )
    else:
        await msg.edit_text("😕 Не удалось найти похожие треки. Попробуй другой трек.")


@dp.message(Command("search"))
async def cmd_search(message: Message):
    """Команда /search [запрос] — найти трек и добавить в очередь."""
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.answer("Использование: /search [название трека]\nПример: /search Словеды")
        return

    query = args[1].strip()
    msg   = await message.answer(f"🔍 Ищу «{query}»...")
    tracks = await deezer_search(query, limit=5)

    if not tracks:
        await msg.edit_text("😕 Треки не найдены")
        return

    # Первый трек ставим на воспроизведение
    state = await get_player_state(message.from_user.id)
    current = state.get("track") or tracks[0]
    queue   = (state.get("queue") or []) + tracks[:3]

    await save_player_state(message.from_user.id, current, queue, is_playing=True)

    lines = [f"🎵 Найдено {len(tracks)} треков:\n"]
    for i, t in enumerate(tracks, 1):
        lines.append(f"{i}. <b>{t['artist']}</b> — <i>{t['title']}</i> ⏱{t['duration_str']}")

    await msg.edit_text("\n".join(lines) + "\n\nТреки добавлены в очередь плеера!", parse_mode="HTML")


# ─────────────────────────────────────────────────────────────────────────────
# ЗАПУСК — бот + сервер одновременно
# ─────────────────────────────────────────────────────────────────────────────

async def run_bot():
    """Запустить Telegram-бота."""
    logger.info("Запуск Telegram-бота...")
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())


async def run_server():
    """Запустить FastAPI сервер."""
    logger.info(f"Запуск веб-сервера на порту {SERVER_PORT}...")
    config = uvicorn.Config(app, host="0.0.0.0", port=SERVER_PORT, log_level="warning")
    server = uvicorn.Server(config)
    await server.serve()


async def main():
    """Запустить всё вместе."""
    await init_db()
    # Запускаем бота и сервер параллельно
    await asyncio.gather(run_bot(), run_server())


if __name__ == "__main__":
    asyncio.run(main())
