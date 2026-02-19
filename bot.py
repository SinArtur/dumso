import asyncio
import logging
from datetime import datetime, timedelta

import pytz
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import BadRequest
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

from config import BOT_TOKEN, TIMEZONE, NAMAZ_NAMES, ADMIN_IDS, ADMIN_IDS
from database import Database
from parser import NamazParser
from scheduler import NotificationScheduler

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Глобальные объекты
db = Database()
parser = NamazParser()
scheduler = None

def format_schedule_message(schedule, date_label):
    """Форматирует сообщение с расписанием"""
    if not schedule:
        return f"❌ Расписание на {date_label} не найдено"
    
    message = f"📅 Расписание намазов на {date_label}:\n\n"
    for namaz_key, namaz_name in NAMAZ_NAMES.items():
        if namaz_key in schedule and schedule[namaz_key]:
            message += f"🕌 {namaz_name}: {schedule[namaz_key]}\n"
    
    return message

def get_main_keyboard():
    """Создает главную клавиатуру с кнопками"""
    keyboard = [
        [
            InlineKeyboardButton("📅 Сегодня", callback_data="today"),
            InlineKeyboardButton("📅 Завтра", callback_data="tomorrow")
        ],
        [
            InlineKeyboardButton("🔔 Подписаться", callback_data="subscribe"),
            InlineKeyboardButton("🔕 Отписаться", callback_data="unsubscribe")
        ],
        [
            InlineKeyboardButton("⏰ Настроить время", callback_data="set_time")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    user_id = update.effective_user.id
    
    # Создаем пользователя, если его нет
    await db.create_user(user_id)
    
    welcome_message = (
        "🕌 Ассаламу алейкум!\n\n"
        "Я бот для напоминаний о намазах в Саратове.\n\n"
        "Выберите действие:"
    )
    
    await update.message.reply_text(
        welcome_message,
        reply_markup=get_main_keyboard()
    )

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик нажатий на кнопки"""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    await db.create_user(user_id)
    
    if query.data == "today":
        try:
            schedule = parser.get_today_schedule()
            # Если расписание пустое, пытаемся получить из БД
            if not schedule:
                now = datetime.now(TIMEZONE)
                schedule = await db.get_schedule(now.day, now.month, now.year)
            message = format_schedule_message(schedule, "сегодня")
        except Exception as e:
            logger.error(f"Ошибка получения расписания на сегодня: {e}")
            # Пытаемся получить из БД
            try:
                now = datetime.now(TIMEZONE)
                schedule = await db.get_schedule(now.day, now.month, now.year)
                message = format_schedule_message(schedule, "сегодня")
            except Exception:
                message = "❌ Не удалось получить расписание. Попробуйте позже."
        try:
            await query.edit_message_text(message, reply_markup=get_main_keyboard())
        except BadRequest as e:
            # Игнорируем ситуацию, когда сообщение не изменилось
            if "Message is not modified" in str(e):
                pass  # Тихо игнорируем
            else:
                logger.error(f"Ошибка редактирования сообщения (today): {e}")
        except Exception as e:
            logger.error(f"Неожиданная ошибка редактирования сообщения (today): {e}")
    
    elif query.data == "tomorrow":
        try:
            schedule = parser.get_tomorrow_schedule()
            # Если расписание пустое, пытаемся получить из БД
            if not schedule:
                now = datetime.now(TIMEZONE)
                tomorrow_dt = now + timedelta(days=1)
                schedule = await db.get_schedule(tomorrow_dt.day, tomorrow_dt.month, tomorrow_dt.year)
            message = format_schedule_message(schedule, "завтра")
        except Exception as e:
            logger.error(f"Ошибка получения расписания на завтра: {e}")
            # Пытаемся получить из БД
            try:
                now = datetime.now(TIMEZONE)
                tomorrow_dt = now + timedelta(days=1)
                schedule = await db.get_schedule(tomorrow_dt.day, tomorrow_dt.month, tomorrow_dt.year)
                message = format_schedule_message(schedule, "завтра")
            except Exception:
                message = "❌ Не удалось получить расписание. Попробуйте позже."
        try:
            await query.edit_message_text(message, reply_markup=get_main_keyboard())
        except BadRequest as e:
            if "Message is not modified" in str(e):
                pass  # Тихо игнорируем
            else:
                logger.error(f"Ошибка редактирования сообщения (tomorrow): {e}")
        except Exception as e:
            logger.error(f"Неожиданная ошибка редактирования сообщения (tomorrow): {e}")
    
    elif query.data == "subscribe":
        await db.subscribe_user(user_id)
        try:
            await query.edit_message_text(
                "✅ Вы подписались на уведомления о намазах!",
                reply_markup=get_main_keyboard()
            )
        except BadRequest as e:
            if "Message is not modified" in str(e):
                pass  # Тихо игнорируем
            else:
                logger.error(f"Ошибка редактирования сообщения (subscribe): {e}")
        except Exception as e:
            logger.error(f"Неожиданная ошибка редактирования сообщения (subscribe): {e}")
    
    elif query.data == "unsubscribe":
        await db.unsubscribe_user(user_id)
        try:
            await query.edit_message_text(
                "❌ Вы отписались от уведомлений о намазах.",
                reply_markup=get_main_keyboard()
            )
        except BadRequest as e:
            if "Message is not modified" in str(e):
                pass  # Тихо игнорируем
            else:
                logger.error(f"Ошибка редактирования сообщения (unsubscribe): {e}")
        except Exception as e:
            logger.error(f"Неожиданная ошибка редактирования сообщения (unsubscribe): {e}")
    
    elif query.data == "set_time":
        # Создаем клавиатуру для выбора времени
        keyboard = [
            [
                InlineKeyboardButton("5 минут", callback_data="time_5"),
                InlineKeyboardButton("10 минут", callback_data="time_10"),
                InlineKeyboardButton("15 минут", callback_data="time_15")
            ],
            [
                InlineKeyboardButton("20 минут", callback_data="time_20"),
                InlineKeyboardButton("30 минут", callback_data="time_30")
            ],
            [
                InlineKeyboardButton("◀️ Назад", callback_data="back")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        try:
            await query.edit_message_text(
                "⏰ Выберите время напоминания до намаза:",
                reply_markup=reply_markup
            )
        except BadRequest as e:
            if "Message is not modified" in str(e):
                pass  # Тихо игнорируем
            else:
                logger.error(f"Ошибка редактирования сообщения (set_time): {e}")
        except Exception as e:
            logger.error(f"Неожиданная ошибка редактирования сообщения (set_time): {e}")
    
    elif query.data.startswith("time_"):
        offset = int(query.data.split("_")[1])
        await db.set_notification_offset(user_id, offset)
        try:
            await query.edit_message_text(
                f"✅ Время напоминания установлено: {offset} минут",
                reply_markup=get_main_keyboard()
            )
        except BadRequest as e:
            if "Message is not modified" in str(e):
                pass  # Тихо игнорируем
            else:
                logger.error(f"Ошибка редактирования сообщения (time_*): {e}")
        except Exception as e:
            logger.error(f"Неожиданная ошибка редактирования сообщения (time_*): {e}")
    
    elif query.data == "back":
        try:
            await query.edit_message_text(
                "Выберите действие:",
                reply_markup=get_main_keyboard()
            )
        except BadRequest as e:
            if "Message is not modified" in str(e):
                pass  # Тихо игнорируем
            else:
                logger.error(f"Ошибка редактирования сообщения (back): {e}")
        except Exception as e:
            logger.error(f"Неожиданная ошибка редактирования сообщения (back): {e}")

async def schedule_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /schedule"""
    try:
        schedule = parser.get_today_schedule()
        # Если расписание пустое, пытаемся получить из БД
        if not schedule:
            now = datetime.now(TIMEZONE)
            schedule = await db.get_schedule(now.day, now.month, now.year)
        message = format_schedule_message(schedule, "сегодня")
    except Exception as e:
        logger.error(f"Ошибка получения расписания: {e}")
        # Пытаемся получить из БД
        try:
            now = datetime.now(TIMEZONE)
            schedule = await db.get_schedule(now.day, now.month, now.year)
            message = format_schedule_message(schedule, "сегодня")
        except:
            message = "❌ Не удалось получить расписание. Попробуйте позже."
    await update.message.reply_text(message, reply_markup=get_main_keyboard())

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /status"""
    user_id = update.effective_user.id
    user = await db.get_user(user_id)
    
    if user:
        status = "подписан" if user['subscribed'] else "не подписан"
        offset = user.get('notification_offset', 10)
        message = (
            f"📊 Ваш статус:\n\n"
            f"Подписка: {status}\n"
            f"Время напоминания: {offset} минут"
        )
    else:
        message = "❌ Пользователь не найден"
    
    await update.message.reply_text(message, reply_markup=get_main_keyboard())

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /stats (только для администраторов)"""
    user_id = update.effective_user.id
    
    # Проверка на администратора
    if not ADMIN_IDS or user_id not in ADMIN_IDS:
        await update.message.reply_text("❌ У вас нет доступа к этой команде.")
        return
    
    try:
        stats = await db.get_statistics()
        
        message = (
            f"📊 **Статистика бота**\n\n"
            f"👥 Всего пользователей: {stats['total_users']}\n"
            f"🔔 Подписано на уведомления: {stats['subscribed_users']}\n"
            f"🔕 Не подписано: {stats['unsubscribed_users']}\n\n"
            f"📈 **Новые пользователи:**\n"
            f"   За 7 дней: {stats['new_users_week']}\n"
            f"   За 30 дней: {stats['new_users_month']}\n"
        )
        
        if stats['offset_distribution']:
            message += f"\n⏰ **Время напоминания:**\n"
            for offset in sorted(stats['offset_distribution'].keys()):
                count = stats['offset_distribution'][offset]
                message += f"   {offset} мин: {count} чел.\n"
        
        await update.message.reply_text(message, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Ошибка получения статистики: {e}")
        await update.message.reply_text("❌ Ошибка получения статистики.")

async def update_schedule_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /update_schedule (только для администраторов)"""
    user_id = update.effective_user.id
    
    # Проверка на администратора
    if not ADMIN_IDS or user_id not in ADMIN_IDS:
        await update.message.reply_text("❌ У вас нет доступа к этой команде.")
        return
    
    try:
        await update.message.reply_text("🔄 Обновляю расписание с сайта...")
        
        # Принудительно парсим сайт
        schedule = parser.parse_schedule(force_refresh=True)
        
        if not schedule or len(schedule) == 0:
            await update.message.reply_text("❌ Не удалось получить расписание с сайта.")
            return
        
        # Сохраняем в БД
        now = datetime.now(TIMEZONE)
        await db.save_schedule(schedule, now.month, now.year)
        
        # Обновляем расписание в планировщике, если он запущен
        if scheduler:
            await scheduler.update_schedule_daily()
        
        await update.message.reply_text(
            f"✅ Расписание успешно обновлено!\n"
            f"📅 Получено расписание на {len(schedule)} дней для {now.month}/{now.year}"
        )
        
    except Exception as e:
        logger.error(f"Ошибка обновления расписания: {e}")
        await update.message.reply_text(f"❌ Ошибка обновления расписания: {e}")

async def post_init(application: Application):
    """Инициализация после запуска бота"""
    global scheduler
    await db.init_db()
    scheduler = NotificationScheduler(application.bot, db)
    await scheduler.start()
    logger.info("Бот запущен и готов к работе")

async def post_shutdown(application: Application):
    """Очистка при остановке бота"""
    if scheduler:
        scheduler.stop()
    logger.info("Бот остановлен")

def main():
    """Основная функция запуска бота"""
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN не установлен! Проверьте файл .env")
        return
    
    # Создаем приложение
    application = Application.builder().token(BOT_TOKEN).post_init(post_init).post_shutdown(post_shutdown).build()
    
    # Регистрируем обработчики
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("schedule", schedule_command))
    application.add_handler(CommandHandler("status", status_command))
    application.add_handler(CommandHandler("stats", stats_command))
    application.add_handler(CommandHandler("update_schedule", update_schedule_command))
    application.add_handler(CallbackQueryHandler(button_handler))
    
    # Запускаем бота
    logger.info("Запуск бота...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()

