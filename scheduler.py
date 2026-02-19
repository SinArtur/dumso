from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime, timedelta
import pytz
import logging
from config import TIMEZONE, NOTIFICATION_OFFSET, NAMAZ_NAMES
from parser import NamazParser
from database import Database
import asyncio

logger = logging.getLogger(__name__)

class NotificationScheduler:
    def __init__(self, bot, db):
        self.bot = bot
        self.db = db
        self.scheduler = AsyncIOScheduler(timezone=TIMEZONE)
        self.parser = NamazParser()
        self.scheduled_jobs = {}
    
    async def start(self):
        """Запускает планировщик"""
        # Обновление расписания каждый день в 00:01
        self.scheduler.add_job(
            self.update_schedule_daily,
            CronTrigger(hour=0, minute=1),  # Каждый день в 00:01
            id='update_schedule'
        )
        
        # Проверка намазов каждую минуту
        self.scheduler.add_job(
            self.check_namaz_times,
            'interval',
            minutes=1,
            id='check_namaz'
        )
        
        # Очистка старых job_id каждый час
        self.scheduler.add_job(
            self.clear_old_jobs,
            'interval',
            hours=1,
            id='clear_old_jobs'
        )
        
        # Автоматическая очистка старых уведомлений каждый день в 03:00
        self.scheduler.add_job(
            self.cleanup_old_notifications,
            CronTrigger(hour=3, minute=0),
            id='cleanup_notifications'
        )
        
        # Первоначальное обновление расписания
        await self.update_schedule_daily()
        
        self.scheduler.start()
    
    async def update_schedule_daily(self):
        """Обновляет расписание ежедневно. При ошибке использует данные из БД"""
        now = datetime.now(TIMEZONE)
        try:
            # Пытаемся получить новое расписание с сайта
            schedule = self.parser.parse_schedule(force_refresh=True)
            
            # Проверяем, что расписание не пустое
            if not schedule or len(schedule) == 0:
                print(f"⚠️ Получено пустое расписание. Используем данные из БД.")
                # Пытаемся получить данные из БД для текущего дня
                today_schedule = await self.db.get_schedule(now.day, now.month, now.year)
                if today_schedule:
                    print(f"✅ Используем расписание из БД для {now.day}.{now.month}.{now.year}")
                else:
                    print(f"❌ Нет данных в БД для {now.day}.{now.month}.{now.year}")
                return
            
            # Сохраняем успешно полученное расписание
            await self.db.save_schedule(schedule, now.month, now.year)
            print(f"✅ Расписание успешно обновлено на {now.month}/{now.year} ({len(schedule)} дней)")
            
        except Exception as e:
            print(f"❌ Ошибка обновления расписания: {e}")
            print(f"📦 Используем существующие данные из БД")
            # Пытаемся получить данные из БД для текущего дня
            try:
                today_schedule = await self.db.get_schedule(now.day, now.month, now.year)
                if today_schedule:
                    print(f"✅ Данные из БД доступны для {now.day}.{now.month}.{now.year}")
                else:
                    print(f"⚠️ Нет данных в БД. Бот будет работать с ограниченным функционалом.")
            except Exception as db_error:
                print(f"❌ Ошибка при обращении к БД: {db_error}")
    
    async def check_namaz_times(self):
        """Проверяет время намазов и отправляет уведомления"""
        try:
            now = datetime.now(TIMEZONE)
            day = now.day
            month = now.month
            year = now.year
            
            schedule = await self.db.get_schedule(day, month, year)
            if not schedule:
                # Если нет в кэше, пытаемся парсить и сохранять
                try:
                    full_schedule = self.parser.parse_schedule(force_refresh=True)
                    if full_schedule and len(full_schedule) > 0:
                        await self.db.save_schedule(full_schedule, month, year)
                        schedule = full_schedule.get(day, {})
                        if schedule:
                            print(f"✅ Расписание получено с сайта и сохранено в БД для {day}.{month}.{year}")
                    else:
                        print(f"⚠️ Сайт вернул пустое расписание. Используем данные из БД если есть.")
                except Exception as parse_error:
                    print(f"⚠️ Ошибка парсинга при проверке намазов: {parse_error}. Используем данные из БД.")
                    # Если парсинг не удался, пытаемся получить данные из БД для других дней месяца
                    # или просто продолжаем без расписания для этого дня
            
            if not schedule:
                return
            
            subscribed_users = await self.db.get_subscribed_users()
            if not subscribed_users:
                return
            
            current_time = now.strftime('%H:%M')
            
            # Проверяем каждый намаз
            for namaz_key, namaz_name in NAMAZ_NAMES.items():
                if namaz_key not in schedule:
                    continue
                
                namaz_time_str = schedule[namaz_key]
                if not namaz_time_str:
                    continue
                
                # Парсим время намаза
                try:
                    namaz_hour, namaz_minute = map(int, namaz_time_str.split(':'))
                    # Создаем datetime для времени намаза СЕГОДНЯ в правильном часовом поясе
                    namaz_datetime = TIMEZONE.localize(
                        datetime(now.year, now.month, now.day, namaz_hour, namaz_minute, 0)
                    )
                    
                    # Вычисляем время уведомления
                    for user in subscribed_users:
                        offset = user.get('notification_offset', NOTIFICATION_OFFSET)
                        notification_time = namaz_datetime - timedelta(minutes=offset)
                        
                        # Проверяем, нужно ли отправить уведомление сейчас
                        # Уведомление должно быть отправлено в течение текущей минуты
                        time_diff = (notification_time - now).total_seconds()
                        
                        # Проверяем, что время уведомления уже наступило, но не прошло больше минуты
                        if -60 < time_diff <= 60:
                            job_id = f"{user['user_id']}_{namaz_key}_{day}_{month}_{year}"
                            
                            # Проверяем, не было ли уже отправлено уведомление
                            if job_id not in self.scheduled_jobs:
                                await self.send_notification(
                                    user['user_id'],
                                    namaz_name,
                                    namaz_time_str,
                                    offset
                                )
                                self.scheduled_jobs[job_id] = True
                                
                                # Удаляем из памяти через час
                                asyncio.create_task(self.clear_job_id(job_id, 3600))
                
                except ValueError as e:
                    print(f"Ошибка парсинга времени {namaz_time_str}: {e}")
                    continue
        
        except Exception as e:
            print(f"Ошибка проверки времени намазов: {e}")
    
    async def clear_job_id(self, job_id, delay):
        """Удаляет job_id из памяти через указанное время"""
        await asyncio.sleep(delay)
        self.scheduled_jobs.pop(job_id, None)
    
    async def clear_old_jobs(self):
        """Очищает старые job_id (вызывается периодически)"""
        # Очищаем job_id старше 24 часов (оставляем только сегодняшние)
        now = datetime.now(TIMEZONE)
        keys_to_remove = []
        for job_id in list(self.scheduled_jobs.keys()):
            # Формат job_id: user_id_namaz_key_day_month_year
            parts = job_id.split('_')
            if len(parts) >= 5:
                try:
                    job_day = int(parts[-3])
                    job_month = int(parts[-2])
                    job_year = int(parts[-1])
                    job_date = datetime(job_year, job_month, job_day, tzinfo=TIMEZONE)
                    if (now - job_date).days > 1:
                        keys_to_remove.append(job_id)
                except (ValueError, IndexError):
                    continue
        
        for key in keys_to_remove:
            self.scheduled_jobs.pop(key, None)
        
        if keys_to_remove:
            print(f"Очищено {len(keys_to_remove)} старых job_id")
    
    async def send_notification(self, user_id, namaz_name, namaz_time, offset):
        """Отправляет уведомление пользователю"""
        try:
            message = f"🕌 Через {offset} минут намаз {namaz_name} в {namaz_time}"
            sent_message = await self.bot.send_message(chat_id=user_id, text=message)
            # Сохраняем message_id уведомления в БД
            await self.db.save_message(sent_message.message_id, user_id, 'notification')
        except Exception as e:
            print(f"Ошибка отправки уведомления пользователю {user_id}: {e}")
    
    async def cleanup_old_notifications(self):
        """Удаляет старые уведомления (старше 2 дней)"""
        try:
            # Получаем список старых сообщений (старше 2 дней)
            old_messages = await self.db.get_old_messages(days=2)
            
            if not old_messages:
                print("Нет старых уведомлений для удаления")
                return
            
            deleted_count = 0
            failed_count = 0
            
            for message_id, user_id in old_messages:
                try:
                    await self.bot.delete_message(chat_id=user_id, message_id=message_id)
                    deleted_count += 1
                except Exception as e:
                    # Сообщение уже удалено или недоступно
                    failed_count += 1
                    logger.debug(f"Не удалось удалить сообщение {message_id} для пользователя {user_id}: {e}")
            
            # Удаляем из БД все сообщения (включая те, что не удалось удалить)
            await self.db.delete_messages(old_messages)
            
            print(f"✅ Автоочистка: удалено {deleted_count} уведомлений, не удалось {failed_count}")
            
        except Exception as e:
            print(f"❌ Ошибка автоочистки уведомлений: {e}")
    
    def stop(self):
        """Останавливает планировщик"""
        self.scheduler.shutdown()

