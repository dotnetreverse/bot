import asyncio
import time
import os
import sqlite3
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
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

# Миграции для старых БД
for migration in [
    "ALTER TABLE users ADD COLUMN joined_at REAL DEFAULT 0",
    "ALTER TABLE banned_users ADD COLUMN reason TEXT",
    "ALTER TABLE muted_users ADD COLUMN reason TEXT",
    "ALTER TABLE daily_activity ADD COLUMN message_count INTEGER DEFAULT 1",
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
    label = get_user_display(user_id, username)
    r_text = f"\nПричина: {hd.quote(reason)}" if reason else ""
    await reply_to.answer(f"🚫 {label} забанен.{r_text}", parse_mode="HTML")
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
    label = get_user_display(user_id, username)
    r_text = f"\nПричина: {hd.quote(reason)}" if reason else ""
    await reply_to.answer(f"🔇 {label} замучен на {label_time}.{r_text}", parse_mode="HTML")
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
    try:
        await bot.copy_message(chat_id=CHANNEL_ID, from_chat_id=chat_id, message_id=msg_id)
        await callback.answer("✅ Опубликовано в канал!", show_alert=True)
        await callback.message.edit_reply_markup(reply_markup=None)
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
            await bot.send_message(admin, user_card, reply_markup=keyboard, parse_mode="HTML")
        except Exception as e:
            print(f"Ошибка отправки админу: {e}")

    await message.answer("✅ Сообщение отправлено анонимно.")


# ================= RUN =================

async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
