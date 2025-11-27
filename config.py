import os
from dotenv import load_dotenv
import pytz

load_dotenv()

BOT_TOKEN = os.getenv('BOT_TOKEN')
NOTIFICATION_OFFSET = int(os.getenv('NOTIFICATION_OFFSET', 10))
TIMEZONE = pytz.timezone(os.getenv('TIMEZONE', 'Europe/Saratov'))

# Названия намазов на русском
NAMAZ_NAMES = {
    'fajr': 'Фаджр',
    'sunrise': 'Восход',
    'dhuhr': 'Зухр',
    'asr': 'Аср',
    'maghrib': 'Магриб',
    'isha': 'Иша'
}

# Порядок намазов в течение дня
NAMAZ_ORDER = ['fajr', 'sunrise', 'dhuhr', 'asr', 'maghrib', 'isha']

# ID администраторов (можно указать через переменную окружения ADMIN_IDS через запятую)
admin_ids_str = os.getenv('ADMIN_IDS', '')
if admin_ids_str:
    ADMIN_IDS = [int(uid.strip()) for uid in admin_ids_str.split(',') if uid.strip().isdigit()]
else:
    ADMIN_IDS = []  # Если не указано, список пустой

