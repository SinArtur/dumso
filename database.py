import aiosqlite
import asyncio
from datetime import datetime

class Database:
    def __init__(self, db_path='namaz_bot.db'):
        self.db_path = db_path
    
    async def init_db(self):
        """Инициализирует базу данных"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    subscribed INTEGER DEFAULT 0,
                    notification_offset INTEGER DEFAULT 10,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            await db.execute('''
                CREATE TABLE IF NOT EXISTS schedule_cache (
                    month INTEGER,
                    year INTEGER,
                    day INTEGER,
                    fajr TEXT,
                    sunrise TEXT,
                    dhuhr TEXT,
                    asr TEXT,
                    maghrib TEXT,
                    isha TEXT,
                    PRIMARY KEY (year, month, day)
                )
            ''')
            await db.commit()
    
    async def get_user(self, user_id):
        """Получает информацию о пользователе"""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute('SELECT * FROM users WHERE user_id = ?', (user_id,)) as cursor:
                row = await cursor.fetchone()
                if row:
                    return dict(row)
                return None
    
    async def create_user(self, user_id):
        """Создает нового пользователя"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                'INSERT OR IGNORE INTO users (user_id, subscribed, notification_offset) VALUES (?, 0, 10)',
                (user_id,)
            )
            await db.commit()
    
    async def subscribe_user(self, user_id):
        """Подписывает пользователя на уведомления"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                'UPDATE users SET subscribed = 1 WHERE user_id = ?',
                (user_id,)
            )
            await db.commit()
    
    async def unsubscribe_user(self, user_id):
        """Отписывает пользователя от уведомлений"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                'UPDATE users SET subscribed = 0 WHERE user_id = ?',
                (user_id,)
            )
            await db.commit()
    
    async def set_notification_offset(self, user_id, offset):
        """Устанавливает время напоминания (в минутах)"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                'UPDATE users SET notification_offset = ? WHERE user_id = ?',
                (offset, user_id)
            )
            await db.commit()
    
    async def get_subscribed_users(self):
        """Получает список всех подписанных пользователей"""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute('SELECT * FROM users WHERE subscribed = 1') as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
    
    async def save_schedule(self, schedule, month, year):
        """Сохраняет расписание в кэш"""
        async with aiosqlite.connect(self.db_path) as db:
            for day, times in schedule.items():
                await db.execute('''
                    INSERT OR REPLACE INTO schedule_cache 
                    (year, month, day, fajr, sunrise, dhuhr, asr, maghrib, isha)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (year, month, day, times.get('fajr'), times.get('sunrise'),
                      times.get('dhuhr'), times.get('asr'), times.get('maghrib'), times.get('isha')))
            await db.commit()
    
    async def get_schedule(self, day, month, year):
        """Получает расписание из кэша"""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                'SELECT * FROM schedule_cache WHERE year = ? AND month = ? AND day = ?',
                (year, month, day)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return {
                        'fajr': row['fajr'],
                        'sunrise': row['sunrise'],
                        'dhuhr': row['dhuhr'],
                        'asr': row['asr'],
                        'maghrib': row['maghrib'],
                        'isha': row['isha']
                    }
                return None
    
    async def get_statistics(self):
        """Получает статистику пользователей"""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            
            # Общее количество пользователей
            async with db.execute('SELECT COUNT(*) as count FROM users') as cursor:
                total_users = (await cursor.fetchone())['count']
            
            # Подписанные пользователи
            async with db.execute('SELECT COUNT(*) as count FROM users WHERE subscribed = 1') as cursor:
                subscribed_users = (await cursor.fetchone())['count']
            
            # Новые пользователи за последние 7 дней
            async with db.execute('''
                SELECT COUNT(*) as count FROM users 
                WHERE created_at >= datetime('now', '-7 days')
            ''') as cursor:
                new_users_week = (await cursor.fetchone())['count']
            
            # Новые пользователи за последние 30 дней
            async with db.execute('''
                SELECT COUNT(*) as count FROM users 
                WHERE created_at >= datetime('now', '-30 days')
            ''') as cursor:
                new_users_month = (await cursor.fetchone())['count']
            
            # Распределение по времени напоминания
            async with db.execute('''
                SELECT notification_offset, COUNT(*) as count 
                FROM users 
                WHERE subscribed = 1 
                GROUP BY notification_offset
                ORDER BY notification_offset
            ''') as cursor:
                offset_distribution = {row['notification_offset']: row['count'] 
                                     for row in await cursor.fetchall()}
            
            return {
                'total_users': total_users,
                'subscribed_users': subscribed_users,
                'unsubscribed_users': total_users - subscribed_users,
                'new_users_week': new_users_week,
                'new_users_month': new_users_month,
                'offset_distribution': offset_distribution
            }

