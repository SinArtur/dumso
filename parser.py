import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from config import TIMEZONE

class NamazParser:
    def __init__(self):
        self.url = "https://dumso.ru/raspisanie"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        self._cache = None
        self._cache_month = None
    
    def parse_schedule(self, force_refresh=False):
        """Парсит расписание намазов на текущий месяц"""
        now = datetime.now(TIMEZONE)
        current_month = now.month
        
        # Используем кэш, если он актуален
        if not force_refresh and self._cache and self._cache_month == current_month:
            return self._cache
        
        try:
            response = requests.get(self.url, headers=self.headers, timeout=10)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')
            table = soup.find('table', class_='namaz_time')

            if not table:
                raise Exception("Таблица расписания не найдена")

            schedule = {}

            # Проверяем, это обычное месячное расписание или специальная таблица Рамадана
            header_row = table.find('tr')
            header_text = header_row.get_text(separator=' ', strip=True).lower() if header_row else ""
            is_ramadan_table = "расписание намазов на рамадан" in soup.get_text(separator=' ', strip=True).lower() or "рамадан" in header_text

            rows = table.find_all('tr')[1:]  # Пропускаем заголовок

            if is_ramadan_table:
                # Структура Рамадан-таблицы:
                # 0 - день Рамадана (1, 2, 3...)
                # 1 - день недели
                # 2 - дата по общему календарю (19, 20, 21...)
                # 3..8 - времена намазов

                # Попробуем определить базовый месяц из заголовка (например, "фев./март")
                base_month = current_month
                next_month = current_month % 12 + 1
                if header_row:
                    header_cols = header_row.find_all('td')
                    if len(header_cols) >= 3:
                        header_month_text = header_cols[2].get_text().lower()
                        if "фев" in header_month_text and "март" in header_month_text:
                            base_month = 2
                            next_month = 3

                month_for_row = base_month
                prev_day_greg = None

                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) < 9:
                        continue

                    day_str = cols[2].get_text().strip()
                    if not day_str.isdigit():
                        continue

                    day_greg = int(day_str)

                    # Если дата уменьшилась по сравнению с предыдущей строкой,
                    # считаем, что начался следующий месяц (переход с 28..29 на 1..)
                    if prev_day_greg is not None and day_greg < prev_day_greg:
                        month_for_row = (month_for_row % 12) + 1

                    prev_day_greg = day_greg

                    # Нас интересуют только строки для текущего месяца
                    if month_for_row != current_month:
                        continue

                    schedule[day_greg] = {
                        'fajr': self._format_time(cols[3].get_text().strip()),
                        'sunrise': self._format_time(cols[4].get_text().strip()),
                        'dhuhr': self._format_time(cols[5].get_text().strip()),
                        'asr': self._format_time(cols[6].get_text().strip()),
                        'maghrib': self._format_time(cols[7].get_text().strip()),
                        'isha': self._format_time(cols[8].get_text().strip())
                    }
            else:
                # Обычная месячная таблица (один месяц, без исламских дат)
                # Старое поведение: берем день из первой колонки
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) >= 9:
                        day = cols[0].get_text().strip()
                        if day.isdigit():
                            day_num = int(day)
                            schedule[day_num] = {
                                'fajr': self._format_time(cols[3].get_text().strip()),
                                'sunrise': self._format_time(cols[4].get_text().strip()),
                                'dhuhr': self._format_time(cols[5].get_text().strip()),
                                'asr': self._format_time(cols[6].get_text().strip()),
                                'maghrib': self._format_time(cols[7].get_text().strip()),
                                'isha': self._format_time(cols[8].get_text().strip())
                            }
            
            # Сохраняем в кэш
            self._cache = schedule
            self._cache_month = current_month
            
            return schedule
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Ошибка подключения к сайту: {e}")
            # Возвращаем кэш, если есть, даже если он устарел
            return self._cache if self._cache else {}
        except Exception as e:
            print(f"❌ Ошибка парсинга: {e}")
            # Возвращаем кэш, если есть, даже если он устарел
            return self._cache if self._cache else {}
    
    def _format_time(self, time_str):
        """Форматирует время из '6.53' в '06:53'"""
        time_str = time_str.replace(' ', '')
        if '.' in time_str:
            hours, minutes = time_str.split('.')
            return f"{int(hours):02d}:{int(minutes):02d}"
        return time_str
    
    def get_today_schedule(self):
        """Возвращает расписание на сегодня"""
        schedule = self.parse_schedule()
        today = datetime.now(TIMEZONE).day
        return schedule.get(today, {})
    
    def get_tomorrow_schedule(self):
        """Возвращает расписание на завтра"""
        schedule = self.parse_schedule()
        tomorrow_dt = datetime.now(TIMEZONE) + timedelta(days=1)
        tomorrow = tomorrow_dt.day
        return schedule.get(tomorrow, {})

