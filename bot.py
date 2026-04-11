import asyncio
import time
import os
import sqlite3
import json
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.types import (
    Message,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    InputMediaPhoto,
    InputMediaVideo,
    InputMediaDocument,
    InputMediaAudio,
)
from aiogram.utils.markdown import html_decoration as hd

TOKEN = os.getenv("TOKEN")

ADMINS = {8470365734, 7955144889}
CHANNEL_ID = -1003682143536

bot = Bot(token=TOKEN)
dp = Dispatcher()

reply_mode = {}       # admin_id -> user_id
ban_reason_mode = {}  # admin_id -> user_id
mute_reason_mode = {} # admin_id -> (user_id, seconds)

SPAM_DELAY = 10
FOREVER_SECONDS = 100 * 365 * 24 * 3600
MEDIA_GROUP_DELAY = 1.0

pending_media_groups = {}  # (chat_id, media_group_id) -> {"messages": [...], "task": asyncio.Task}


# ================= DATABASE =================

DB_PATH = os.getenv("DB_PATH", "/data/bot.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
db = sqlite3.connect(DB_PATH, check_same_thread=False)
db.execute("PRAGMA journal_mode=WAL")
db.execute("PRAGMA busy_timeout = 5000")
cursor = db.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS users(
    id INTEGER PRIMARY KEY,
    username TEXT,
    first_name TEXT,
    joined_at REAL DEFAULT 0
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS messages(
    user_id INTEGER PRIMARY KEY,
    count INTEGER DEFAULT 0,
    last_message_time REAL DEFAULT 0
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS daily_activity(
    date TEXT NOT NULL,
    user_id INTEGER NOT NULL,
    message_count INTEGER DEFAULT 1,
    PRIMARY KEY (date, user_id)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS banned_users(
    user_id INTEGER PRIMARY KEY,
    username TEXT,
    banned_at REAL,
    reason TEXT
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS muted_users(
    user_id INTEGER PRIMARY KEY,
    username TEXT,
    muted_until REAL,
    reason TEXT
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS publication_map(
    admin_chat_id INTEGER NOT NULL,
    admin_message_id INTEGER NOT NULL,
    source_chat_id INTEGER NOT NULL,
    source_message_id INTEGER NOT NULL,
    published INTEGER DEFAULT 0,
    channel_message_id INTEGER,
    deleted INTEGER DEFAULT 0,
    content_text TEXT,
    content_caption TEXT,
    PRIMARY KEY (admin_chat_id, admin_message_id)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS publication_status_map(
    admin_chat_id INTEGER NOT NULL,
    status_message_id INTEGER NOT NULL,
    source_chat_id INTEGER NOT NULL,
    source_message_id INTEGER NOT NULL,
    PRIMARY KEY (admin_chat_id, status_message_id)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS media_group_posts(
    source_chat_id INTEGER NOT NULL,
    first_source_message_id INTEGER NOT NULL,
    media_group_id TEXT NOT NULL,
    source_message_ids TEXT NOT NULL,
    media_items_json TEXT NOT NULL,
    channel_message_ids TEXT,
    PRIMARY KEY (source_chat_id, first_source_message_id)
)
""")

# Миграции для старых БД
for migration in [
    "ALTER TABLE users ADD COLUMN joined_at REAL DEFAULT 0",
    "ALTER TABLE banned_users ADD COLUMN reason TEXT",
    "ALTER TABLE muted_users ADD COLUMN reason TEXT",
    "ALTER TABLE daily_activity ADD COLUMN message_count INTEGER DEFAULT 1",
    "ALTER TABLE publication_map ADD COLUMN channel_message_id INTEGER",
    "ALTER TABLE publication_map ADD COLUMN deleted INTEGER DEFAULT 0",
    "ALTER TABLE publication_map ADD COLUMN content_text TEXT",
    "ALTER TABLE publication_map ADD COLUMN content_caption TEXT",
    "ALTER TABLE media_group_posts ADD COLUMN channel_message_ids TEXT",
]:
    try:
        cursor.execute(migration)
        db.commit()
    except Exception:
        pass

db.commit()


# ================= DB FUNCTIONS =================

def add_user(user_id, username, first_name):
    cursor.execute(
        """
        INSERT INTO users (id, username, first_name, joined_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            username = excluded.username,
            first_name = excluded.first_name
        """,
        (user_id, username, first_name, time.time())
    )
    cursor.execute(
        "INSERT OR IGNORE INTO messages (user_id, count, last_message_time) VALUES (?, 0, 0)",
        (user_id,)
    )
    db.commit()


def add_message(user_id):
    cursor.execute("UPDATE messages SET count = count + 1 WHERE user_id = ?", (user_id,))
    today = datetime.utcnow().strftime("%Y-%m-%d")
    cursor.execute(
        """
        INSERT INTO daily_activity (date, user_id, message_count)
        VALUES (?, ?, 1)
        ON CONFLICT(date, user_id) DO UPDATE SET
            message_count = COALESCE(message_count, 1) + 1
        """,
        (today, user_id)
    )
    db.commit()


def save_publication_copy(
    admin_chat_id: int,
    admin_message_id: int,
    source_chat_id: int,
    source_message_id: int,
    content_text: str | None = None,
    content_caption: str | None = None,
):
    existing = get_publication_entry(admin_chat_id, admin_message_id)
    published = existing[2] if existing else 0
    channel_message_id = existing[3] if existing else None
    deleted = existing[4] if existing else 0

    cursor.execute(
        """
        INSERT OR REPLACE INTO publication_map (
            admin_chat_id,
            admin_message_id,
            source_chat_id,
            source_message_id,
            published,
            channel_message_id,
            deleted,
            content_text,
            content_caption
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            admin_chat_id,
            admin_message_id,
            source_chat_id,
            source_message_id,
            published,
            channel_message_id,
            deleted,
            content_text,
            content_caption,
        )
    )
    db.commit()


def save_publication_status(admin_chat_id: int, status_message_id: int, source_chat_id: int, source_message_id: int):
    cursor.execute(
        """
        INSERT OR REPLACE INTO publication_status_map (
            admin_chat_id, status_message_id, source_chat_id, source_message_id
        ) VALUES (?, ?, ?, ?)
        """,
        (admin_chat_id, status_message_id, source_chat_id, source_message_id)
    )
    db.commit()


def get_publication_entry(admin_chat_id: int, admin_message_id: int):
    cursor.execute(
        """
        SELECT source_chat_id, source_message_id, published, channel_message_id, deleted, content_text, content_caption
        FROM publication_map
        WHERE admin_chat_id = ? AND admin_message_id = ?
        """,
        (admin_chat_id, admin_message_id)
    )
    return cursor.fetchone()


def get_publication_copies(source_chat_id: int, source_message_id: int):
    cursor.execute(
        """
        SELECT admin_chat_id, admin_message_id, published, channel_message_id, deleted, content_text, content_caption
        FROM publication_map
        WHERE source_chat_id = ? AND source_message_id = ?
        """,
        (source_chat_id, source_message_id)
    )
    return cursor.fetchall()


def get_publication_statuses(source_chat_id: int, source_message_id: int):
    cursor.execute(
        """
        SELECT admin_chat_id, status_message_id
        FROM publication_status_map
        WHERE source_chat_id = ? AND source_message_id = ?
        """,
        (source_chat_id, source_message_id)
    )
    return cursor.fetchall()


def mark_publication_done(source_chat_id: int, source_message_id: int, channel_message_id: int):
    cursor.execute(
        """
        UPDATE publication_map
        SET published = 1, channel_message_id = ?, deleted = 0
        WHERE source_chat_id = ? AND source_message_id = ?
        """,
        (channel_message_id, source_chat_id, source_message_id)
    )
    db.commit()


def mark_publication_deleted(source_chat_id: int, source_message_id: int):
    cursor.execute(
        """
        UPDATE publication_map
        SET deleted = 1
        WHERE source_chat_id = ? AND source_message_id = ?
        """,
        (source_chat_id, source_message_id)
    )
    db.commit()


def get_channel_message_id(source_chat_id: int, source_message_id: int):
    cursor.execute(
        """
        SELECT channel_message_id
        FROM publication_map
        WHERE source_chat_id = ? AND source_message_id = ? AND channel_message_id IS NOT NULL
        LIMIT 1
        """,
        (source_chat_id, source_message_id)
    )
    row = cursor.fetchone()
    return row[0] if row else None


def save_media_group_post(
    source_chat_id: int,
    first_source_message_id: int,
    media_group_id: str,
    source_message_ids: list[int],
    media_items: list[dict],
):
    cursor.execute(
        """
        INSERT OR REPLACE INTO media_group_posts (
            source_chat_id,
            first_source_message_id,
            media_group_id,
            source_message_ids,
            media_items_json,
            channel_message_ids
        ) VALUES (?, ?, ?, ?, ?, COALESCE((SELECT channel_message_ids FROM media_group_posts WHERE source_chat_id = ? AND first_source_message_id = ?), NULL))
        """,
        (
            source_chat_id,
            first_source_message_id,
            str(media_group_id),
            json.dumps(source_message_ids),
            json.dumps(media_items, ensure_ascii=False),
            source_chat_id,
            first_source_message_id,
        )
    )
    db.commit()


def get_media_group_post(source_chat_id: int, first_source_message_id: int):
    cursor.execute(
        """
        SELECT media_group_id, source_message_ids, media_items_json, channel_message_ids
        FROM media_group_posts
        WHERE source_chat_id = ? AND first_source_message_id = ?
        """,
        (source_chat_id, first_source_message_id)
    )
    row = cursor.fetchone()
    if not row:
        return None
    media_group_id, source_message_ids_json, media_items_json, channel_message_ids_json = row
    return {
        "media_group_id": media_group_id,
        "source_message_ids": json.loads(source_message_ids_json),
        "media_items": json.loads(media_items_json),
        "channel_message_ids": json.loads(channel_message_ids_json) if channel_message_ids_json else None,
    }


def set_media_group_channel_ids(source_chat_id: int, first_source_message_id: int, channel_message_ids: list[int]):
    cursor.execute(
        """
        UPDATE media_group_posts
        SET channel_message_ids = ?
        WHERE source_chat_id = ? AND first_source_message_id = ?
        """,
        (json.dumps(channel_message_ids), source_chat_id, first_source_message_id)
    )
    db.commit()


def build_media_group_items(messages: list[Message]):
    items = []
    caption_used = False
    for msg in sorted(messages, key=lambda m: m.message_id):
        caption = None
        if msg.caption and not caption_used:
            caption = msg.caption
            caption_used = True

        if msg.photo:
            items.append({"type": "photo", "file_id": msg.photo[-1].file_id, "caption": caption})
        elif msg.video:
            items.append({"type": "video", "file_id": msg.video.file_id, "caption": caption})
        elif msg.document:
            items.append({"type": "document", "file_id": msg.document.file_id, "caption": caption})
        elif msg.audio:
            items.append({"type": "audio", "file_id": msg.audio.file_id, "caption": caption})
    return items


def build_input_media(items: list[dict], escape_caption: bool = False):
    media = []
    for item in items:
        caption = item.get("caption")
        parse_mode = None
        if caption is not None and escape_caption:
            caption = hd.quote(caption)
            parse_mode = "HTML"

        media_type = item["type"]
        if media_type == "photo":
            media.append(InputMediaPhoto(media=item["file_id"], caption=caption, parse_mode=parse_mode))
        elif media_type == "video":
            media.append(InputMediaVideo(media=item["file_id"], caption=caption, parse_mode=parse_mode))
        elif media_type == "document":
            media.append(InputMediaDocument(media=item["file_id"], caption=caption, parse_mode=parse_mode))
        elif media_type == "audio":
            media.append(InputMediaAudio(media=item["file_id"], caption=caption, parse_mode=parse_mode))
    return media


async def forward_media_group(items: list[dict], target_id: int, escape_caption: bool = False):
    media = build_input_media(items, escape_caption=escape_caption)
    if not media:
        return []
    return await bot.send_media_group(chat_id=target_id, media=media)


def get_stats():
    cursor.execute("SELECT COUNT(*) FROM users")
    users = cursor.fetchone()[0]
    cursor.execute("SELECT SUM(count) FROM messages")
    msgs = cursor.fetchone()[0] or 0
    return users, msgs


def get_top():
    cursor.execute("""
        SELECT users.username, users.first_name, messages.user_id, messages.count
        FROM messages JOIN users ON users.id = messages.user_id
        ORDER BY messages.count DESC LIMIT 10
    """)
    return cursor.fetchall()


def get_activity_last_days(days: int = 7):
    result = []
    for i in range(days - 1, -1, -1):
        d = (datetime.utcnow() - timedelta(days=i)).strftime("%Y-%m-%d")
        cursor.execute("SELECT COUNT(*) FROM daily_activity WHERE date = ?", (d,))
        result.append((d, cursor.fetchone()[0]))
    return result

def get_user_activity_last_days(user_id: int, days: int = 7):
    result = []
    for i in range(days - 1, -1, -1):
        d = (datetime.utcnow() - timedelta(days=i)).strftime("%Y-%m-%d")
        cursor.execute(
            "SELECT COALESCE(message_count, 1) FROM daily_activity WHERE date = ? AND user_id = ?",
            (d, user_id)
        )
        row = cursor.fetchone()
        result.append((d, row[0] if row else 0))
    return result


def get_user_info(target: str):
    if target.isdigit():
        cursor.execute(
            "SELECT u.id, u.username, u.first_name, u.joined_at, COALESCE(m.count,0) "
            "FROM users u LEFT JOIN messages m ON m.user_id = u.id WHERE u.id = ?",
            (int(target),)
        )
    else:
        cursor.execute(
            "SELECT u.id, u.username, u.first_name, u.joined_at, COALESCE(m.count,0) "
            "FROM users u LEFT JOIN messages m ON m.user_id = u.id WHERE u.username = ?",
            (target,)
        )
    return cursor.fetchone()


# ===== БАН =====

def ban_user(user_id, username, reason=None):
    cursor.execute(
        "INSERT OR REPLACE INTO banned_users (user_id, username, banned_at, reason) VALUES (?, ?, ?, ?)",
        (user_id, username, time.time(), reason)
    )
    db.commit()


def unban_user(user_id):
    cursor.execute("DELETE FROM banned_users WHERE user_id = ?", (user_id,))
    db.commit()


def is_banned(user_id):
    cursor.execute("SELECT 1 FROM banned_users WHERE user_id = ?", (user_id,))
    return cursor.fetchone() is not None


def get_ban_list():
    cursor.execute("SELECT user_id, username, reason FROM banned_users")
    return cursor.fetchall()


def get_ban_reason(user_id):
    cursor.execute("SELECT reason FROM banned_users WHERE user_id = ?", (user_id,))
    row = cursor.fetchone()
    return row[0] if row else None


# ===== МУТ =====

def mute_user(user_id, username, seconds, reason=None):
    cursor.execute(
        "INSERT OR REPLACE INTO muted_users (user_id, username, muted_until, reason) VALUES (?, ?, ?, ?)",
        (user_id, username, time.time() + seconds, reason)
    )
    db.commit()


def unmute_user(user_id):
    cursor.execute("DELETE FROM muted_users WHERE user_id = ?", (user_id,))
    db.commit()


def is_muted(user_id):
    cursor.execute("SELECT muted_until FROM muted_users WHERE user_id = ?", (user_id,))
    row = cursor.fetchone()
    if row is None:
        return False
    if time.time() < row[0]:
        return True
    unmute_user(user_id)
    return False


def get_mute_info(user_id):
    cursor.execute("SELECT muted_until, reason FROM muted_users WHERE user_id = ?", (user_id,))
    return cursor.fetchone()


def get_mute_list():
    now = time.time()
    cursor.execute("SELECT user_id, username, muted_until, reason FROM muted_users WHERE muted_until > ?", (now,))
    return cursor.fetchall()


# ===== СПАМ =====

def is_spam(user_id: int) -> bool:
    cursor.execute("SELECT last_message_time FROM messages WHERE user_id = ?", (user_id,))
    row = cursor.fetchone()
    now = time.time()
    if row and now - row[0] < SPAM_DELAY:
        return True
    cursor.execute("UPDATE messages SET last_message_time = ? WHERE user_id = ?", (now, user_id))
    db.commit()
    return False


# ===== УТИЛИТЫ =====

def parse_mute_duration(arg: str):
    """Парсит '30m', '2h', '1d' -> секунды. Возвращает None если не распознано."""
    if not arg:
        return None
    units = {"m": 60, "h": 3600, "d": 86400}
    unit = arg[-1].lower()
    if unit in units:
        try:
            return int(arg[:-1]) * units[unit]
        except ValueError:
            return None
    try:
        return int(arg) * 60
    except ValueError:
        return None


def format_duration(seconds: int) -> str:
    if seconds >= FOREVER_SECONDS - 86400:
        return "навсегда"
    if seconds < 3600:
        return f"{seconds // 60} мин"
    if seconds < 86400:
        return f"{seconds // 3600} ч"
    return f"{seconds // 86400} д"


def get_user_display(user_id, username) -> str:
    return f"@{username}" if username else f"ID:{user_id}"


def get_user_count() -> int:
    cursor.execute("SELECT COUNT(*) FROM users")
    return cursor.fetchone()[0]


def build_activity_chart(data: list) -> str:
    if not data:
        return "Нет данных"
    max_val = max(v for _, v in data) or 1
    bar_width = 10
    lines = []
    for date_str, count in data:
        day = date_str[5:]  # MM-DD
        filled = round(count / max_val * bar_width)
        bar = "█" * filled + "░" * (bar_width - filled)
        lines.append(f"{day} {bar} {count}")
    return "\n".join(lines)


# ===== КОДИРОВАНИЕ chat_id в callback_data =====
# ВАЖНО: chat_id может быть отрицательным (каналы/группы), поэтому
# передаём его как abs(chat_id) и восстанавливаем с минусом по признаку.
# Используем разделитель ":" вместо "_" чтобы не ломать split("_").

def encode_pub_data(msg_id: int, chat_id: int) -> str:
    """Кодирует msg_id и chat_id в строку для callback_data."""
    return f"{msg_id}:{chat_id}"


def decode_pub_data(s: str):
    """Декодирует строку обратно в (msg_id, chat_id)."""
    msg_id_str, chat_id_str = s.split(":", 1)
    return int(msg_id_str), int(chat_id_str)


def strip_publication_status(text: str | None) -> str | None:
    if text is None:
        return None
    for suffix in ("\n\n✅ Опубликовано", "\n\n🗑 Удалено из канала"):
        if text.endswith(suffix):
            return text[:-len(suffix)]
    return text


async def mirror_admin_reply_to_other_admins(message: Message, target_user_id: int):
    admin_name = hd.quote(message.from_user.first_name or "Админ")
    info_text = f"↩️ <b>Ответ пользователю</b>\nАдмин: {admin_name}"

    for admin_id in ADMINS:
        if admin_id == message.from_user.id:
            continue
        try:
            if message.text:
                await bot.send_message(admin_id, f"{info_text}\n\n{hd.quote(message.text)}", parse_mode="HTML")
            elif message.photo:
                caption = info_text + (f"\n\n{hd.quote(message.caption)}" if message.caption else "")
                await bot.send_photo(admin_id, message.photo[-1].file_id, caption=caption, parse_mode="HTML")
            elif message.video:
                caption = info_text + (f"\n\n{hd.quote(message.caption)}" if message.caption else "")
                await bot.send_video(admin_id, message.video.file_id, caption=caption, parse_mode="HTML")
            elif message.audio:
                caption = info_text + (f"\n\n{hd.quote(message.caption)}" if message.caption else "")
                await bot.send_audio(admin_id, message.audio.file_id, caption=caption, parse_mode="HTML")
            elif message.document:
                caption = info_text + (f"\n\n{hd.quote(message.caption)}" if message.caption else "")
                await bot.send_document(admin_id, message.document.file_id, caption=caption, parse_mode="HTML")
            elif message.voice:
                caption = info_text + (f"\n\n{hd.quote(message.caption)}" if message.caption else "")
                await bot.send_voice(admin_id, message.voice.file_id, caption=caption, parse_mode="HTML")
            elif message.video_note:
                sent = await bot.send_video_note(admin_id, message.video_note.file_id)
                await bot.send_message(admin_id, info_text, parse_mode="HTML", reply_to_message_id=sent.message_id)
            elif message.sticker:
                sent = await bot.send_sticker(admin_id, message.sticker.file_id)
                await bot.send_message(admin_id, info_text, parse_mode="HTML", reply_to_message_id=sent.message_id)
            elif message.animation:
                caption = info_text + (f"\n\n{hd.quote(message.caption)}" if message.caption else "")
                await bot.send_animation(admin_id, message.animation.file_id, caption=caption, parse_mode="HTML")
            elif message.poll:
                sent = await bot.send_poll(
                    admin_id,
                    question=message.poll.question,
                    options=[option.text for option in message.poll.options],
                    is_anonymous=message.poll.is_anonymous,
                    type=message.poll.type,
                    allows_multiple_answers=message.poll.allows_multiple_answers,
                    correct_option_id=message.poll.correct_option_id,
                    explanation=message.poll.explanation,
                    open_period=message.poll.open_period,
                    close_date=message.poll.close_date,
                    is_closed=message.poll.is_closed,
                )
                await bot.send_message(admin_id, info_text, parse_mode="HTML", reply_to_message_id=sent.message_id)
            else:
                sent = await bot.copy_message(chat_id=admin_id, from_chat_id=message.chat.id, message_id=message.message_id)
                await bot.send_message(admin_id, info_text, parse_mode="HTML", reply_to_message_id=sent.message_id)
        except Exception as e:
            print(f"Ошибка mirror_admin_reply_to_other_admins: {e}")


def build_status_keyboard(source_chat_id: int, source_message_id: int, status_text: str = None, allow_delete: bool = False, show_status_button: bool = False) -> InlineKeyboardMarkup | None:
    rows = []
    if show_status_button and status_text:
        rows.append([InlineKeyboardButton(text=status_text, callback_data="noop")])
    if allow_delete:
        data = encode_pub_data(source_message_id, source_chat_id)
        rows.append([InlineKeyboardButton(text="🗑 Удалить из канала", callback_data=f"delpost:{data}")])
    return InlineKeyboardMarkup(inline_keyboard=rows) if rows else None


async def sync_admin_post_state(source_chat_id: int, source_message_id: int, status_text: str, allow_delete: bool):
    copies = get_publication_copies(source_chat_id, source_message_id)
    for admin_chat_id, admin_message_id, _, _, _, _, _ in copies:
        entry = get_publication_entry(admin_chat_id, admin_message_id)
        if not entry:
            continue
        _, _, _, _, _, content_text, content_caption = entry
        try:
            if content_text is not None:
                base_text = strip_publication_status(content_text) or ""
                new_text = f"{base_text}\n\n{status_text}" if base_text else status_text
                await bot.edit_message_text(
                    chat_id=admin_chat_id,
                    message_id=admin_message_id,
                    text=new_text,
                    reply_markup=build_status_keyboard(source_chat_id, source_message_id, allow_delete=allow_delete),
                    parse_mode=None,
                )
            elif content_caption is not None:
                base_caption = strip_publication_status(content_caption) or ""
                new_caption = f"{base_caption}\n\n{status_text}" if base_caption else status_text
                await bot.edit_message_caption(
                    chat_id=admin_chat_id,
                    message_id=admin_message_id,
                    caption=new_caption,
                    reply_markup=build_status_keyboard(source_chat_id, source_message_id, allow_delete=allow_delete),
                    parse_mode="HTML",
                )
            else:
                await bot.edit_message_reply_markup(
                    chat_id=admin_chat_id,
                    message_id=admin_message_id,
                    reply_markup=build_status_keyboard(
                        source_chat_id,
                        source_message_id,
                        status_text=status_text,
                        allow_delete=allow_delete,
                        show_status_button=True,
                    )
                )
        except Exception as e:
            print(f"Ошибка sync_admin_post_state: {e}")
# ================= KEYBOARDS =================

def mute_time_keyboard(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="10 мин",   callback_data=f"mutetime:{user_id}:600"),
                InlineKeyboardButton(text="30 мин",   callback_data=f"mutetime:{user_id}:1800"),
                InlineKeyboardButton(text="1 час",    callback_data=f"mutetime:{user_id}:3600"),
            ],
            [
                InlineKeyboardButton(text="3 часа",   callback_data=f"mutetime:{user_id}:10800"),
                InlineKeyboardButton(text="1 день",   callback_data=f"mutetime:{user_id}:86400"),
                InlineKeyboardButton(text="Навсегда", callback_data=f"mutetime:{user_id}:0"),
            ],
            [
                InlineKeyboardButton(text="❌ Отмена", callback_data=f"mutecancel:{user_id}"),
            ]
        ]
    )


def publish_confirm_keyboard(msg_id: int, chat_id: int) -> InlineKeyboardMarkup:
    data = encode_pub_data(msg_id, chat_id)
    return InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(text="✅ Да, опубликовать", callback_data=f"pubconfirm:{data}"),
            InlineKeyboardButton(text="❌ Отмена",           callback_data=f"pubcancel:{data}"),
        ]]
    )


def published_status_keyboard(source_chat_id: int, source_message_id: int) -> InlineKeyboardMarkup:
    data = encode_pub_data(source_message_id, source_chat_id)
    return InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(text="🗑 Удалить из канала", callback_data=f"delpost:{data}")
        ]]
    )


def user_action_keyboard(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✉️ Ответить", callback_data=f"do_reply:{user_id}"),
                InlineKeyboardButton(text="🚫 Бан",      callback_data=f"do_ban:{user_id}"),
            ],
            [
                InlineKeyboardButton(text="🔇 Мут",      callback_data=f"do_mute:{user_id}"),
                InlineKeyboardButton(text="✅ Разбан",    callback_data=f"do_unban:{user_id}"),
            ]
        ]
    )


def admin_main_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats"),
                InlineKeyboardButton(text="🏆 Топ",        callback_data="admin_top"),
            ],
            [
                InlineKeyboardButton(text="📈 График",      callback_data="admin_chart"),
                InlineKeyboardButton(text="🔍 Поиск юзера", callback_data="admin_search"),
            ],
            [
                InlineKeyboardButton(text="🚫 Бан-лист",    callback_data="admin_bans"),
                InlineKeyboardButton(text="🔇 Мут-лист",    callback_data="admin_mutes"),
            ]
        ]
    )


def back_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="admin_back")]]
    )


# ================= HELPERS =================

async def _do_ban(user_id: int, reason, reply_to: Message):
    cursor.execute("SELECT username FROM users WHERE id = ?", (user_id,))
    row = cursor.fetchone()
    username = row[0] if row else None
    ban_user(user_id, username, reason)
    r_text = f"\nПричина: {hd.quote(reason)}" if reason else ""
    await reply_to.answer(f"🚫 Пользователь забанен.{r_text}", parse_mode="HTML")
    try:
        notify = "🚫 Вы были <b>забанены</b>."
        if reason:
            notify += f"\nПричина: <i>{hd.quote(reason)}</i>"
        await bot.send_message(user_id, notify, parse_mode="HTML")
    except Exception:
        pass


async def _do_mute(user_id: int, seconds: int, reason, reply_to: Message):
    cursor.execute("SELECT username FROM users WHERE id = ?", (user_id,))
    row = cursor.fetchone()
    username = row[0] if row else None
    forever = seconds == 0
    actual_seconds = FOREVER_SECONDS if forever else seconds
    mute_user(user_id, username, actual_seconds, reason)
    label_time = "навсегда" if forever else format_duration(seconds)
    r_text = f"\nПричина: {hd.quote(reason)}" if reason else ""
    await reply_to.answer(f"🔇 Пользователь замучен на {label_time}.{r_text}", parse_mode="HTML")
    try:
        notify = f"🔇 Вы были <b>замучены</b> на <b>{label_time}</b>."
        if reason:
            notify += f"\nПричина: <i>{hd.quote(reason)}</i>"
        await bot.send_message(user_id, notify, parse_mode="HTML")
    except Exception:
        pass


# ================= CALLBACKS: ОТВЕТ =================
# FIX: переименованы в do_reply:, do_ban:, do_mute:, do_unban: — нет коллизий со startswith

@dp.callback_query(F.data.startswith("do_reply:"))
async def reply_button(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMINS:
        return
    user_id = int(callback.data.split(":")[1])
    reply_mode[callback.from_user.id] = user_id
    cancel_kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="❌ Отменить ответ", callback_data="reply_cancel")]]
    )
    await callback.message.answer("✏️ Напишите ответ пользователю:", reply_markup=cancel_kb)
    await callback.answer()


@dp.callback_query(F.data == "reply_cancel")
async def reply_cancel(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMINS:
        return
    reply_mode.pop(callback.from_user.id, None)
    await callback.message.edit_text("↩️ Ответ отменён.")
    await callback.answer()


@dp.callback_query(F.data == "noop")
async def noop_callback(callback: types.CallbackQuery):
    await callback.answer()


# ================= CALLBACKS: БАН =================

@dp.callback_query(F.data.startswith("do_ban:"))
async def ban_button(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMINS:
        return
    user_id = int(callback.data.split(":")[1])
    ban_reason_mode[callback.from_user.id] = user_id
    cancel_kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(
            text="❌ Забанить без причины", callback_data=f"banconfirm:{user_id}:"
        )]]
    )
    await callback.message.answer(
        "✍️ Напишите причину бана (или нажмите кнопку):",
        reply_markup=cancel_kb
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("banconfirm:"))
async def ban_confirm_no_reason(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMINS:
        return
    # формат: banconfirm:{user_id}:{reason}
    parts = callback.data.split(":", 2)
    user_id = int(parts[1])
    reason = parts[2] if len(parts) > 2 and parts[2] else None
    ban_reason_mode.pop(callback.from_user.id, None)
    await _do_ban(user_id, reason, callback.message)
    await callback.answer()


@dp.callback_query(F.data.startswith("do_unban:"))
async def unban_button(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMINS:
        return
    user_id = int(callback.data.split(":")[1])
    unban_user(user_id)
    await callback.answer("✅ Пользователь разбанен")
    try:
        await bot.send_message(user_id, "✅ Вы были <b>разбанены</b>. Можете снова писать.", parse_mode="HTML")
    except Exception:
        pass


# ================= CALLBACKS: МУТ =================

@dp.callback_query(F.data.startswith("do_mute:"))
async def mute_button(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMINS:
        return
    user_id = int(callback.data.split(":")[1])
    await callback.message.edit_reply_markup(reply_markup=mute_time_keyboard(user_id))
    await callback.answer("Выберите время мута")


@dp.callback_query(F.data.startswith("mutetime:"))
async def mute_time_selected(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMINS:
        return
    # формат: mutetime:{user_id}:{seconds}
    parts = callback.data.split(":")
    user_id = int(parts[1])
    seconds = int(parts[2])
    mute_reason_mode[callback.from_user.id] = (user_id, seconds)
    cancel_kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(
            text="❌ Замутить без причины",
            callback_data=f"muteconfirm:{user_id}:{seconds}:"
        )]]
    )
    # Сначала восстанавливаем кнопки карточки, потом отправляем запрос причины
    await callback.message.edit_reply_markup(reply_markup=user_action_keyboard(user_id))
    await callback.message.answer(
        "✍️ Напишите причину мута (или нажмите кнопку):",
        reply_markup=cancel_kb
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("muteconfirm:"))
async def mute_confirm_no_reason(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMINS:
        return
    # формат: muteconfirm:{user_id}:{seconds}:{reason}
    parts = callback.data.split(":", 3)
    user_id = int(parts[1])
    seconds = int(parts[2])
    reason = parts[3] if len(parts) > 3 and parts[3] else None
    mute_reason_mode.pop(callback.from_user.id, None)
    await _do_mute(user_id, seconds, reason, callback.message)
    await callback.answer()


@dp.callback_query(F.data.startswith("mutecancel:"))
async def mute_cancel(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMINS:
        return
    user_id = int(callback.data.split(":")[1])
    await callback.message.edit_reply_markup(reply_markup=user_action_keyboard(user_id))
    await callback.answer("Отменено")


# ================= CALLBACKS: ПУБЛИКАЦИЯ =================
# FIX: chat_id отрицательный — используем ":" как разделитель

@dp.callback_query(F.data.startswith("publish:"))
async def publish_button(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMINS:
        return
    # формат: publish:{msg_id}:{chat_id}
    raw = callback.data[len("publish:"):]
    msg_id, chat_id = decode_pub_data(raw)
    await callback.message.edit_reply_markup(reply_markup=publish_confirm_keyboard(msg_id, chat_id))
    await callback.answer("Подтвердите публикацию")


@dp.callback_query(F.data.startswith("pubconfirm:"))
async def publish_confirm(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMINS:
        return
    raw = callback.data[len("pubconfirm:"):]
    msg_id, chat_id = decode_pub_data(raw)
    entry = get_publication_entry(chat_id, msg_id)
    source_chat_id = chat_id
    source_message_id = msg_id
    if entry:
        source_chat_id, source_message_id, published, channel_message_id, deleted, _, _ = entry
        if published and channel_message_id and not deleted:
            await sync_admin_post_state(source_chat_id, source_message_id, "✅ Опубликовано", allow_delete=True)
            await callback.answer("Уже опубликовано", show_alert=True)
            return

    try:
        media_group_entry = get_media_group_post(source_chat_id, source_message_id)
        if media_group_entry:
            sent_to_channel = await forward_media_group(
                media_group_entry["media_items"],
                CHANNEL_ID,
                escape_caption=False,
            )
            channel_message_ids = [msg.message_id for msg in sent_to_channel]
            first_channel_message_id = channel_message_ids[0] if channel_message_ids else None
            if first_channel_message_id is None:
                raise RuntimeError("Не удалось опубликовать альбом")
            mark_publication_done(source_chat_id, source_message_id, first_channel_message_id)
            set_media_group_channel_ids(source_chat_id, source_message_id, channel_message_ids)
        else:
            sent_to_channel = await bot.copy_message(chat_id=CHANNEL_ID, from_chat_id=chat_id, message_id=msg_id)
            mark_publication_done(source_chat_id, source_message_id, sent_to_channel.message_id)

        await sync_admin_post_state(source_chat_id, source_message_id, "✅ Опубликовано", allow_delete=True)
        await callback.answer("✅ Опубликовано в канал!", show_alert=True)
    except Exception as e:
        await callback.answer(f"❌ Ошибка: {e}", show_alert=True)


@dp.callback_query(F.data.startswith("pubcancel:"))
async def publish_cancel(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMINS:
        return
    raw = callback.data[len("pubcancel:"):]
    msg_id, chat_id = decode_pub_data(raw)
    pub_kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(
            text="📢 Опубликовать в канал",
            callback_data=f"publish:{encode_pub_data(msg_id, chat_id)}"
        )]]
    )
    await callback.message.edit_reply_markup(reply_markup=pub_kb)
    await callback.answer("Отменено")


@dp.callback_query(F.data.startswith("delpost:"))
async def delete_channel_post(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMINS:
        return

    raw = callback.data[len("delpost:"):]
    source_message_id, source_chat_id = decode_pub_data(raw)

    copies = get_publication_copies(source_chat_id, source_message_id)
    if not copies:
        await callback.answer("Пост не найден", show_alert=True)
        return

    published = any(row[2] for row in copies)
    deleted = any(row[4] for row in copies)
    channel_message_id = get_channel_message_id(source_chat_id, source_message_id)

    if not published or not channel_message_id:
        await callback.answer("Пост ещё не опубликован", show_alert=True)
        return

    if deleted:
        await sync_admin_post_state(source_chat_id, source_message_id, "🗑 Удалено из канала", allow_delete=False)
        await callback.answer("Уже удалено", show_alert=True)
        return

    try:
        media_group_entry = get_media_group_post(source_chat_id, source_message_id)
        if media_group_entry and media_group_entry.get("channel_message_ids"):
            for mid in media_group_entry["channel_message_ids"]:
                try:
                    await bot.delete_message(chat_id=CHANNEL_ID, message_id=mid)
                except Exception as inner_e:
                    inner_err = str(inner_e).lower()
                    if "message to delete not found" not in inner_err and "message can't be deleted" not in inner_err and "message cant be deleted" not in inner_err:
                        raise
        else:
            await bot.delete_message(chat_id=CHANNEL_ID, message_id=channel_message_id)
    except Exception as e:
        err = str(e).lower()
        if "message to delete not found" not in err and "message can't be deleted" not in err and "message cant be deleted" not in err:
            await callback.answer(f"❌ Ошибка: {e}", show_alert=True)
            return

    mark_publication_deleted(source_chat_id, source_message_id)
    await sync_admin_post_state(source_chat_id, source_message_id, "🗑 Удалено из канала", allow_delete=False)
    await callback.answer("🗑 Пост удалён", show_alert=True)


# ================= ADMIN PANEL =================

@dp.message(Command("admin"))
async def admin_panel(message: Message):
    if message.from_user.id not in ADMINS:
        return
    await message.answer("⚙️ Админ панель", reply_markup=admin_main_keyboard())


@dp.callback_query(F.data == "admin_stats")
async def admin_stats(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMINS:
        return
    users, msgs = get_stats()
    text = f"📊 <b>Статистика</b>\n\n👤 Пользователей: {users}\n✉️ Сообщений: {msgs}"
    await callback.message.edit_text(text, reply_markup=back_keyboard(), parse_mode="HTML")
    await callback.answer()


@dp.callback_query(F.data == "admin_top")
async def admin_top(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMINS:
        return
    data = get_top()
    text = "🏆 <b>Топ пользователей:</b>\n\n"
    for i, (username, first_name, user_id, count) in enumerate(data, 1):
        name = f"@{username}" if username else (first_name or f"ID:{user_id}")
        text += f"{i}. {hd.quote(name)} — {count} сообщений\n"
    if not data:
        text += "Пока никого нет."
    await callback.message.edit_text(text, reply_markup=back_keyboard(), parse_mode="HTML")
    await callback.answer()


@dp.callback_query(F.data == "admin_chart")
async def admin_chart(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMINS:
        return
    data = get_activity_last_days(7)
    chart = build_activity_chart(data)
    total = sum(v for _, v in data)
    text = (
        f"📈 <b>Активность за 7 дней</b>\n"
        f"(уникальных юзеров в день)\n\n"
        f"<code>{chart}</code>\n\n"
        f"Итого за неделю: <b>{total}</b> чел."
    )
    await callback.message.edit_text(text, reply_markup=back_keyboard(), parse_mode="HTML")
    await callback.answer()


@dp.callback_query(F.data == "admin_search")
async def admin_search_prompt(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMINS:
        return
    await callback.message.edit_text(
        "🔍 <b>Поиск пользователя</b>\n\n"
        "Введите команду:\n"
        "<code>/user @username</code>\n"
        "<code>/user 123456789</code>",
        reply_markup=back_keyboard(),
        parse_mode="HTML"
    )
    await callback.answer()


@dp.callback_query(F.data == "admin_bans")
async def admin_bans(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMINS:
        return
    bans = get_ban_list()
    if not bans:
        text = "🚫 Бан-лист пуст."
    else:
        text = "🚫 <b>Бан-лист:</b>\n\n"
        for user_id, username, reason in bans:
            r = f" — <i>{hd.quote(reason)}</i>" if reason else ""
            text += f"• {get_user_display(user_id, username)}{r}\n"
    await callback.message.edit_text(text, reply_markup=back_keyboard(), parse_mode="HTML")
    await callback.answer()


@dp.callback_query(F.data == "admin_mutes")
async def admin_mutes(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMINS:
        return
    mutes = get_mute_list()
    if not mutes:
        text = "🔇 Мут-лист пуст."
    else:
        text = "🔇 <b>Пользователи в муте:</b>\n\n"
        for user_id, username, muted_until, reason in mutes:
            remaining = int(muted_until - time.time())
            r = f" — <i>{hd.quote(reason)}</i>" if reason else ""
            text += f"• {get_user_display(user_id, username)} — ещё {format_duration(remaining)}{r}\n"
    await callback.message.edit_text(text, reply_markup=back_keyboard(), parse_mode="HTML")
    await callback.answer()


@dp.callback_query(F.data == "admin_back")
async def admin_back(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMINS:
        return
    await callback.message.edit_text("⚙️ Админ панель", reply_markup=admin_main_keyboard())
    await callback.answer()


# ================= /user — ПОИСК =================

@dp.message(Command("user"))
async def cmd_user(message: Message):
    if message.from_user.id not in ADMINS:
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.answer("❌ Использование: /user @username или /user <user_id>")
    target = args[1].strip().lstrip("@")
    row = get_user_info(target)
    if not row:
        return await message.answer("❌ Пользователь не найден в базе.")

    user_id, username, first_name, joined_at, msg_count = row
    profile_link = f'<a href="tg://user?id={user_id}">{user_id}</a>'
    uname = f"@{username}" if username else "—"
    joined = datetime.utcfromtimestamp(joined_at).strftime("%d.%m.%Y %H:%M") if joined_at else "неизвестно"

    if is_banned(user_id):
        reason = get_ban_reason(user_id)
        status = "🚫 Забанен" + (f" (<i>{hd.quote(reason)}</i>)" if reason else "")
    else:
        mute_row = get_mute_info(user_id)
        if mute_row and time.time() < mute_row[0]:
            remaining = format_duration(int(mute_row[0] - time.time()))
            r = mute_row[1]
            status = f"🔇 В муте ещё {remaining}" + (f" (<i>{hd.quote(r)}</i>)" if r else "")
        else:
            status = "✅ Активен"

    data = get_activity_last_days(7)
    chart = build_activity_chart(data)

    text = (
        f"🔍 <b>Профиль</b>\n\n"
        f"ID: {profile_link}\n"
        f"Username: {uname}\n"
        f"Имя: {hd.quote(first_name or '—')}\n"
        f"Зарегистрирован: {joined}\n"
        f"Сообщений: {msg_count}\n"
        f"Статус: {status}\n\n"
        f"📈 <b>Активность бота (7 дней):</b>\n"
        f"<code>{chart}</code>"
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🚫 Бан",      callback_data=f"do_ban:{user_id}"),
                InlineKeyboardButton(text="🔇 Мут",      callback_data=f"do_mute:{user_id}"),
            ],
            [
                InlineKeyboardButton(text="✅ Разбан",    callback_data=f"do_unban:{user_id}"),
                InlineKeyboardButton(text="✉️ Ответить",  callback_data=f"do_reply:{user_id}"),
            ]
        ]
    )
    await message.answer(text, parse_mode="HTML", reply_markup=kb)


# ================= START =================

@dp.message(Command("start"))
async def start(message: Message):
    uid = message.from_user.id
    if is_banned(uid):
        reason = get_ban_reason(uid)
        text = "🚫 Вы забанены."
        if reason:
            text += f"\nПричина: <i>{hd.quote(reason)}</i>"
        return await message.answer(text, parse_mode="HTML")

    mute_row = get_mute_info(uid)
    if mute_row and time.time() < mute_row[0]:
        remaining = format_duration(int(mute_row[0] - time.time()))
        reason = mute_row[1]
        text = f"🔇 Вы в муте. Осталось: <b>{remaining}</b>."
        if reason:
            text += f"\nПричина: <i>{hd.quote(reason)}</i>"
        return await message.answer(text, parse_mode="HTML")

    user_count = get_user_count()
    await message.answer(
        f"👋 Привет!\n\n"
        f"Отправь своё сообщение — мы ответим в ближайшее время.\n"
        f"Это полностью анонимно 🎭\n\n"
    )


# ================= ADMIN КОМАНДЫ =================

@dp.message(Command("ban"))
async def cmd_ban(message: Message):
    if message.from_user.id not in ADMINS:
        return
    args = message.text.split(maxsplit=2)
    if len(args) < 2:
        return await message.answer("❌ Использование: /ban @username [причина]")
    target = args[1].strip().lstrip("@")
    reason = args[2].strip() if len(args) > 2 else None
    if target.isdigit():
        cursor.execute("SELECT id, username FROM users WHERE id = ?", (int(target),))
    else:
        cursor.execute("SELECT id, username FROM users WHERE username = ?", (target,))
    row = cursor.fetchone()
    if not row:
        return await message.answer("❌ Пользователь не найден в базе.")
    user_id, username = row
    if user_id in ADMINS:
        return await message.answer("❌ Нельзя забанить администратора.")
    await _do_ban(user_id, reason, message)


@dp.message(Command("unban"))
async def cmd_unban(message: Message):
    if message.from_user.id not in ADMINS:
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.answer("❌ Использование: /unban @username или /unban <user_id>")
    target = args[1].strip().lstrip("@")
    if target.isdigit():
        cursor.execute("SELECT user_id, username FROM banned_users WHERE user_id = ?", (int(target),))
    else:
        cursor.execute("SELECT user_id, username FROM banned_users WHERE username = ?", (target,))
    row = cursor.fetchone()
    if not row:
        return await message.answer("❌ Пользователь не найден в бан-листе.")
    user_id, username = row
    unban_user(user_id)
    await message.answer(f"✅ {get_user_display(user_id, username)} разбанен.")
    try:
        await bot.send_message(user_id, "✅ Вы были <b>разбанены</b>. Можете снова писать.", parse_mode="HTML")
    except Exception:
        pass


@dp.message(Command("mute"))
async def cmd_mute(message: Message):
    if message.from_user.id not in ADMINS:
        return
    args = message.text.split(maxsplit=3)
    if len(args) < 2:
        return await message.answer(
            "❌ Использование: /mute @username [время] [причина]\n"
            "Примеры: /mute @user 30m спам | /mute @user 2h | /mute @user спам"
        )
    target = args[1].strip().lstrip("@")

    # FIX: если второй аргумент не является временем — считаем его причиной, мут на 1ч
    duration_arg = args[2].strip() if len(args) > 2 else None
    seconds = parse_mute_duration(duration_arg) if duration_arg else 3600
    if seconds is None:
        # duration_arg не распознан как время — это причина, мут на 1ч
        reason = " ".join(args[2:]).strip() if len(args) > 2 else None
        seconds = 3600
    else:
        reason = args[3].strip() if len(args) > 3 else None

    if target.isdigit():
        cursor.execute("SELECT id, username FROM users WHERE id = ?", (int(target),))
    else:
        cursor.execute("SELECT id, username FROM users WHERE username = ?", (target,))
    row = cursor.fetchone()
    if not row:
        return await message.answer("❌ Пользователь не найден в базе.")
    user_id, username = row
    if user_id in ADMINS:
        return await message.answer("❌ Нельзя замутить администратора.")
    await _do_mute(user_id, seconds, reason, message)


@dp.message(Command("unmute"))
async def cmd_unmute(message: Message):
    if message.from_user.id not in ADMINS:
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.answer("❌ Использование: /unmute @username или /unmute <user_id>")
    target = args[1].strip().lstrip("@")
    if target.isdigit():
        cursor.execute("SELECT user_id, username FROM muted_users WHERE user_id = ?", (int(target),))
    else:
        cursor.execute("SELECT user_id, username FROM muted_users WHERE username = ?", (target,))
    row = cursor.fetchone()
    if not row:
        return await message.answer("❌ Пользователь не найден в мут-листе.")
    user_id, username = row
    unmute_user(user_id)
    await message.answer(f"✅ {get_user_display(user_id, username)} размучен.")
    try:
        await bot.send_message(user_id, "✅ Ваш мут снят. Можете снова писать.", parse_mode="HTML")
    except Exception:
        pass


# ================= FORWARD HELPER =================

async def forward_content(message: Message, target_id: int, caption: str = None):
    try:
        if message.text:
            return await bot.send_message(target_id, message.text, parse_mode=None)
        elif message.photo:
            return await bot.send_photo(target_id, message.photo[-1].file_id, caption=caption, parse_mode="HTML")
        elif message.video:
            return await bot.send_video(target_id, message.video.file_id, caption=caption, parse_mode="HTML")
        elif message.audio:
            return await bot.send_audio(target_id, message.audio.file_id, caption=caption, parse_mode="HTML")
        elif message.document:
            return await bot.send_document(target_id, message.document.file_id, caption=caption, parse_mode="HTML")
        elif message.voice:
            return await bot.send_voice(target_id, message.voice.file_id, caption=caption, parse_mode="HTML")
        elif message.video_note:
            return await bot.send_video_note(target_id, message.video_note.file_id)
        elif message.poll:
            return await bot.send_poll(
                target_id,
                question=message.poll.question,
                options=[option.text for option in message.poll.options],
                is_anonymous=message.poll.is_anonymous,
                type=message.poll.type,
                allows_multiple_answers=message.poll.allows_multiple_answers,
                correct_option_id=message.poll.correct_option_id,
                explanation=message.poll.explanation,
                open_period=message.poll.open_period,
                close_date=message.poll.close_date,
                is_closed=message.poll.is_closed,
            )
        elif message.sticker:
            return await bot.send_sticker(target_id, message.sticker.file_id)
        elif message.animation:
            return await bot.send_animation(target_id, message.animation.file_id, caption=caption, parse_mode="HTML")
        else:
            return await bot.copy_message(
                chat_id=target_id,
                from_chat_id=message.chat.id,
                message_id=message.message_id
            )
    except Exception as e:
        print(f"Ошибка forward_content: {e}")
        return None


async def process_user_media_group(messages: list[Message]):
    if not messages:
        return

    messages = sorted(messages, key=lambda m: m.message_id)
    first_message = messages[0]
    user = first_message.from_user

    add_user(user.id, user.username, user.first_name)
    add_message(user.id)

    profile_link = f'<a href="tg://user?id={user.id}">{user.id}</a>'
    username_line = f"@{user.username}" if user.username else "—"
    name_line = hd.quote(user.first_name or "—")

    user_card = (
        f"👤 <b>Новое сообщение</b>\n\n"
        f"ID: {profile_link}\n"
        f"Username: {username_line}\n"
        f"Имя: {name_line}"
    )

    keyboard = user_action_keyboard(user.id)
    media_items = build_media_group_items(messages)
    if not media_items:
        await first_message.answer("❌ Не удалось обработать альбом.")
        return

    first_source_message_id = messages[0].message_id
    content_caption = None
    for item in media_items:
        if item.get("caption"):
            content_caption = hd.quote(item["caption"])
            break

    save_media_group_post(
        source_chat_id=first_message.chat.id,
        first_source_message_id=first_source_message_id,
        media_group_id=str(first_message.media_group_id),
        source_message_ids=[msg.message_id for msg in messages],
        media_items=media_items,
    )

    for admin in ADMINS:
        try:
            sent_group = await forward_media_group(media_items, admin, escape_caption=True)
            if sent_group:
                first_sent = sent_group[0]
                save_publication_copy(
                    admin_chat_id=admin,
                    admin_message_id=first_sent.message_id,
                    source_chat_id=first_message.chat.id,
                    source_message_id=first_source_message_id,
                    content_text=None,
                    content_caption=content_caption,
                )
                try:
                    pub_data = encode_pub_data(first_sent.message_id, first_sent.chat.id)
                    pub_kb = InlineKeyboardMarkup(
                        inline_keyboard=[[InlineKeyboardButton(
                            text="📢 Опубликовать в канал",
                            callback_data=f"publish:{pub_data}"
                        )]]
                    )
                    await bot.edit_message_reply_markup(
                        chat_id=admin,
                        message_id=first_sent.message_id,
                        reply_markup=pub_kb
                    )
                except Exception as e:
                    print(f"Ошибка установки кнопки публикации альбома: {e}")

            await bot.send_message(admin, user_card, reply_markup=keyboard, parse_mode="HTML")
        except Exception as e:
            print(f"Ошибка отправки альбома админу: {e}")

    await first_message.answer("✅ Сообщение отправлено анонимно.")


async def flush_media_group(key):
    await asyncio.sleep(MEDIA_GROUP_DELAY)
    group = pending_media_groups.pop(key, None)
    if not group:
        return
    try:
        await process_user_media_group(group["messages"])
    except Exception as e:
        print(f"Ошибка обработки media_group: {e}")


async def queue_media_group_message(message: Message):
    key = (message.chat.id, str(message.media_group_id))
    group = pending_media_groups.get(key)
    if group is None:
        group = {"messages": [message]}
        group["task"] = asyncio.create_task(flush_media_group(key))
        pending_media_groups[key] = group
    else:
        group["messages"].append(message)


# ================= ALL MESSAGES =================

@dp.message()
async def all_messages(message: Message):
    user = message.from_user

    # ===== ADMIN: ввод причины бана =====
    if user.id in ADMINS and user.id in ban_reason_mode:
        target_id = ban_reason_mode.pop(user.id)
        reason = message.text.strip() if message.text else None
        await _do_ban(target_id, reason, message)
        return

    # ===== ADMIN: ввод причины мута =====
    if user.id in ADMINS and user.id in mute_reason_mode:
        target_id, seconds = mute_reason_mode.pop(user.id)
        reason = message.text.strip() if message.text else None
        await _do_mute(target_id, seconds, reason, message)
        return

    # ===== ADMIN: режим ответа =====
    if user.id in ADMINS:
        if user.id not in reply_mode:
            return
        target = reply_mode.pop(user.id)
        try:
            await forward_content(message, target, message.caption)
            await mirror_admin_reply_to_other_admins(message, target)
            await message.answer("✅ Ответ отправлен")
        except Exception as e:
            await message.answer(f"❌ Ошибка отправки: {e}")
        return

    # ===== USER =====
    if is_banned(user.id):
        reason = get_ban_reason(user.id)
        text = "🚫 Вы забанены."
        if reason:
            text += f"\nПричина: <i>{hd.quote(reason)}</i>"
        return await message.answer(text, parse_mode="HTML")

    mute_row = get_mute_info(user.id)
    if mute_row and time.time() < mute_row[0]:
        remaining = format_duration(int(mute_row[0] - time.time()))
        reason = mute_row[1]
        text = f"🔇 Вы в муте. Осталось: <b>{remaining}</b>."
        if reason:
            text += f"\nПричина: <i>{hd.quote(reason)}</i>"
        return await message.answer(text, parse_mode="HTML")
    elif mute_row:
        unmute_user(user.id)

    add_user(user.id, user.username, user.first_name)

    if message.media_group_id:
        key = (message.chat.id, str(message.media_group_id))
        if key not in pending_media_groups and is_spam(user.id):
            return await message.answer(f"⏱ Не спамь. Подожди {SPAM_DELAY} секунд.")
        await queue_media_group_message(message)
        return

    if is_spam(user.id):
        return await message.answer(f"⏱ Не спамь. Подожди {SPAM_DELAY} секунд.")

    add_message(user.id)

    profile_link = f'<a href="tg://user?id={user.id}">{user.id}</a>'
    username_line = f"@{user.username}" if user.username else "—"
    name_line = hd.quote(user.first_name or "—")

    user_card = (
        f"👤 <b>Новое сообщение</b>\n\n"
        f"ID: {profile_link}\n"
        f"Username: {username_line}\n"
        f"Имя: {name_line}"
    )

    keyboard = user_action_keyboard(user.id)

    content_caption = hd.quote(message.caption) if message.caption else None

    for admin in ADMINS:
        try:
            sent = await forward_content(message, admin, content_caption)
            if sent:
                save_publication_copy(
                    admin_chat_id=admin,
                    admin_message_id=sent.message_id,
                    source_chat_id=message.chat.id,
                    source_message_id=message.message_id,
                    content_text=message.text if message.text else None,
                    content_caption=content_caption,
                )
                try:
                    pub_data = encode_pub_data(sent.message_id, sent.chat.id)
                    pub_kb = InlineKeyboardMarkup(
                        inline_keyboard=[[InlineKeyboardButton(
                            text="📢 Опубликовать в канал",
                            callback_data=f"publish:{pub_data}"
                        )]]
                    )
                    await bot.edit_message_reply_markup(
                        chat_id=admin,
                        message_id=sent.message_id,
                        reply_markup=pub_kb
                    )
                except Exception as e:
                    print(f"Ошибка установки кнопки публикации: {e}")
            await bot.send_message(admin, user_card, reply_markup=keyboard, parse_mode="HTML")
        except Exception as e:
            print(f"Ошибка отправки админу: {e}")

    await message.answer("✅ Сообщение отправлено анонимно.")


# ================= RUN =================

async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
