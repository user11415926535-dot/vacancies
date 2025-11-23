from __future__ import annotations
import os
import sys
import time
import json
import logging
import argparse
import requests
import asyncio
import html
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta, timezone
from collections import Counter
from telethon import TelegramClient
from telethon.sessions import StringSession

API_URL = "https://api.hh.ru/vacancies"
DEFAULT_OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "/data/it_vacancies_report.txt")

# Telegram configuration
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
SESSION_STRING = os.getenv("TELETHON_SESSION_STRING")
DEST_CHANNEL = os.getenv("DEST_CHANNEL")

# IT-специальности
IT_SPECIALIZATIONS = {
    "python": "Python разработчик",
    "java": "Java разработчик", 
    "frontend": "Frontend разработчик",
    "javascript": "JavaScript разработчик"
}

SEARCH_KEYWORDS = list(IT_SPECIALIZATIONS.keys())
SEARCH_TEXT = " OR ".join(SEARCH_KEYWORDS)

# Фильтры
MIN_SALARY = 80000
ONLY_WITH_SALARY = False

# Целевые города
TARGET_CITIES = ["Москва", "Санкт-Петербург", "Москва и Московская область", "Санкт-Петербург и область"]

# Сеть/таймауты/поведение
PER_PAGE = 50
CONNECT_TIMEOUT = 10
READ_TIMEOUT = 90
TIMEOUT = (CONNECT_TIMEOUT, READ_TIMEOUT)

MAX_PAGE_ATTEMPTS = 6
INITIAL_BACKOFF = 1.0
PAGE_PAUSE = 0.5

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("it_vacancies")

# Global Telegram client
tg_client = None

def check_telegram_env():
    """Проверка переменных окружения для Telegram"""
    missing_vars = []
    
    if not API_ID:
        missing_vars.append("API_ID")
    if not API_HASH:
        missing_vars.append("API_HASH")
    if not SESSION_STRING:
        missing_vars.append("TELETHON_SESSION_STRING")
    if not DEST_CHANNEL:
        missing_vars.append("DEST_CHANNEL")
    
    if missing_vars:
        print(f"❌ Отсутствуют переменные окружения: {', '.join(missing_vars)}")
        return False
    
    try:
        # Проверяем, что API_ID - число
        int(API_ID)
    except (ValueError, TypeError):
        print(f"❌ API_ID должно быть числом, получено: {API_ID}")
        return False
    
    return True

async def init_telegram():
    """Инициализация Telegram клиента"""
    global tg_client
    
    # Проверяем переменные окружения
    if not check_telegram_env():
        return False
    
    try:
        tg_client = TelegramClient(
            StringSession(SESSION_STRING),
            int(API_ID),  # Преобразуем в int
            API_HASH
        )
        
        await tg_client.start()
        print("✅ Telegram клиент успешно инициализирован")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка инициализации Telegram клиента: {e}")
        return False

async def send_to_telegram(report: str, vacancies_count: int):
    """Отправка отчета в Telegram канал"""
    if not tg_client or not DEST_CHANNEL:
        print("❌ Telegram клиент не инициализирован или канал не указан")
        return

    try:
        # Если отчет слишком длинный, разбиваем на части
        if len(report) > 4000:
            parts = []
            current_part = ""
            
            for line in report.split('\n'):
                if len(current_part + line + '\n') > 4000:
                    parts.append(current_part)
                    current_part = line + '\n'
                else:
                    current_part += line + '\n'
            
            if current_part:
                parts.append(current_part)
            
            # Отправляем первую часть
            await tg_client.send_message(DEST_CHANNEL, parts[0], parse_mode='md', link_preview=False)
            
            # Отправляем остальные части с задержкой
            for part in parts[1:]:
                await asyncio.sleep(1)
                await tg_client.send_message(DEST_CHANNEL, part, parse_mode='md', link_preview=False)
        else:
            # Отправляем весь отчет одним сообщением
            await tg_client.send_message(DEST_CHANNEL, report, parse_mode='md', link_preview=False)

        print(f"✅ Отчет отправлен в Telegram канал {DEST_CHANNEL}")
        print(f"📊 Вакансий в отчете: {vacancies_count}")

    except Exception as e:
        print(f"❌ Ошибка при отправке в Telegram: {e}")

def build_user_agent(contact: str) -> str:
    return f"it-vacancies/1.0 (contact: {contact})"

def validate_contact(contact: Optional[str]) -> bool:
    if not contact:
        return False
    low = contact.lower()
    if "example" in low or "test" in low or "your_email" in low:
        return False
    if "@" not in contact:
        return False
    return True

def make_session(user_agent: str) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": user_agent,
        "Accept": "application/json"
    })
    s.trust_env = True
    return s

def get_target_period() -> tuple[datetime, datetime]:
    """Определяет период с 20:00 вчера до 20:00 сегодня в UTC"""
    now_utc = datetime.now(timezone.utc)
    
    # Переводим в московское время (UTC+3) для расчета периода
    moscow_offset = timedelta(hours=3)
    now_moscow = now_utc + moscow_offset
    
    # Сегодня в 20:00 по Москве
    today_20_moscow = now_moscow.replace(hour=20, minute=0, second=0, microsecond=0)
    
    # Вчера в 20:00 по Москве
    yesterday_20_moscow = today_20_moscow - timedelta(days=1)
    
    # Переводим обратно в UTC для сравнения
    yesterday_20_utc = yesterday_20_moscow - moscow_offset
    today_20_utc = today_20_moscow - moscow_offset
    
    return yesterday_20_utc, today_20_utc

def parse_date(date_string: str) -> datetime:
    """Парсит дату из строки в datetime объект с временной зоной"""
    try:
        if date_string.endswith('Z'):
            date_string = date_string[:-1] + '+00:00'
        dt = datetime.fromisoformat(date_string)
        
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
            
        return dt
    except Exception as e:
        logger.warning(f"Failed to parse date {date_string}: {e}")
        return datetime.min.replace(tzinfo=timezone.utc)

def format_salary(salary_data: Dict[str, Any]) -> str:
    """Форматирует зарплату в красивый вид"""
    if not salary_data:
        return "💰 не указана"
    
    parts = []
    if salary_data.get('from'):
        parts.append(f"{salary_data['from']:,}".replace(',', ' '))
    if salary_data.get('to'):
        parts.append(f"{salary_data['to']:,}".replace(',', ' '))
    
    salary_str = " - ".join(parts)
    currency = salary_data.get('currency', 'RUR')
    
    currency_symbols = {
        'RUR': '₽',
        'RUB': '₽',
        'USD': '$',
        'EUR': '€'
    }
    
    symbol = currency_symbols.get(currency, currency)
    return f"💰 {salary_str} {symbol}"

def format_date(date_string: str) -> str:
    """Форматирует дату в читаемый вид (в московском времени)"""
    try:
        dt = parse_date(date_string)
        moscow_tz = timezone(timedelta(hours=3))
        dt_moscow = dt.astimezone(moscow_tz)
        return dt_moscow.strftime("📅 %d.%m.%Y %H:%M")
    except:
        return "📅 дата не указана"

def clean_html(text: str) -> str:
    """Очищает HTML теги из текста"""
    if not text:
        return ""
    import re
    return re.sub(r'<[^>]+>', '', text)

def detect_specialization(vacancy_name: str, snippet: Dict) -> str:
    """Определяет специализацию вакансии"""
    name_lower = vacancy_name.lower()
    snippet_lower = (snippet.get('requirement', '') + ' ' + snippet.get('responsibility', '')).lower()
    full_text = name_lower + ' ' + snippet_lower
    
    if 'python' in full_text:
        return "🐍 Python разработчик"
    elif 'java' in full_text and 'javascript' not in full_text and 'js' not in full_text:
        return "☕ Java разработчик"
    elif 'frontend' in full_text or 'react' in full_text or 'angular' in full_text or 'vue' in full_text:
        return "🎨 Frontend разработчик"
    elif 'javascript' in full_text or 'js' in full_text:
        return "🎨 Frontend разработчик"
    else:
        return "💻 IT-разработчик"

def is_target_city(city_name: str) -> bool:
    """Проверяет, является ли город целевым"""
    return any(target_city.lower() in city_name.lower() for target_city in TARGET_CITIES)

def filter_vacancies(vacancies: List[Dict], period_start: datetime, period_end: datetime) -> List[Dict]:
    """Фильтрует вакансии по периоду и городам"""
    filtered = []
    
    for vacancy in vacancies:
        try:
            # Фильтр по периоду
            published_at = parse_date(vacancy['published_at'])
            if not (period_start <= published_at <= period_end):
                continue
                
            # Фильтр по городу
            area_name = vacancy.get('area', {}).get('name', '')
            if not area_name or not is_target_city(area_name):
                continue
                
            filtered.append(vacancy)
            
        except Exception as e:
            logger.warning(f"Error filtering vacancy {vacancy.get('id', 'unknown')}: {e}")
            continue
    
    # Сортируем от новых к старым
    filtered.sort(key=lambda v: parse_date(v['published_at']), reverse=True)
    return filtered

def generate_beautiful_report(vacancies: List[Dict], total_found: int, period_start: datetime, period_end: datetime) -> str:
    """Генерирует красивый отчет для IT-вакансий"""
    
    # Конвертируем периоды в московское время для отображения
    moscow_tz = timezone(timedelta(hours=3))
    period_start_moscow = period_start.astimezone(moscow_tz)
    period_end_moscow = period_end.astimezone(moscow_tz)
    
    period_str = f"{period_start_moscow.strftime('%d.%m.%Y %H:%M')} - {period_end_moscow.strftime('%d.%m.%Y %H:%M')}"
    
    if not vacancies:
        return f"""❌ За указанный период ({period_str}) IT-вакансии не найдены.""".replace(',', ' ')
    
    # Собираем статистику
    cities = [v['area']['name'] for v in vacancies if v.get('area')]
    city_stats = Counter(cities)
    
    specializations = [detect_specialization(v['name'], v.get('snippet', {})) for v in vacancies]
    spec_stats = Counter(specializations)
    
    # Статистика по зарплатам
    salaries_with_info = [v for v in vacancies if v.get('salary')]
    
    # Формируем отчет
    report = []
    
    # Заголовок и статистика
    report.append("💻 **ОБЗОР IT-ВАКАНСИЙ**")
    report.append("══════════════════════════════")
    report.append("")
    
    report.append("📈 **СТАТИСТИКА:**")
    report.append(f"• Всего IT-вакансий найдено: **{total_found}**")
    report.append(f"• Подходит под фильтры: **{len(vacancies)}**")
    report.append(f"• Указана зарплата: **{len(salaries_with_info)}**")
    report.append(f"• Период: **{period_str}**")
    
    # Статистика по специализациям
    report.append(f"• Распределение: {', '.join(f'{spec.split()[-1]} ({count})' for spec, count in spec_stats.most_common())}")
    
    report.append("")
    report.append("📋 **ВАКАНСИИ:**")
    report.append("══════════════════════════════")
    report.append("")
    
    # Детали по каждой вакансии
    for i, vacancy in enumerate(vacancies, 1):
        specialization = detect_specialization(vacancy['name'], vacancy.get('snippet', {}))
        
        report.append(f"**{i}. {specialization}**")
        report.append(f"**{vacancy['name']}**")
        report.append("─" * 30)
        
        # Ссылка
        report.append(f"🔗 {vacancy['alternate_url']}")
        
        # Дата публикации
        date_str = format_date(vacancy['published_at'])
        vacancy_date = parse_date(vacancy['published_at'])
        
        # Определяем, насколько свежая вакансия
        now_utc = datetime.now(timezone.utc)
        time_diff = now_utc - vacancy_date
        
        if time_diff.total_seconds() <= 3600:  # До 1 часа
            date_str += " 🆕"
        elif time_diff.total_seconds() <= 21600:  # До 6 часов
            date_str += " 🔥"
        elif vacancy_date.date() == now_utc.date():  # Сегодня
            date_str += " ⭐"
        
        report.append(f"{date_str}")
        
        # Зарплата
        report.append(f"{format_salary(vacancy.get('salary'))}")
        
        # Работодатель и город
        report.append(f"🏢 **Компания:** {vacancy['employer']['name']}")
        report.append(f"📍 **Город:** {vacancy['area']['name']}")
        
        # Требования
        requirement = clean_html(vacancy.get('snippet', {}).get('requirement', ''))
        if requirement and len(requirement) > 5:
            if len(requirement) > 120:
                requirement = requirement[:120] + "..."
            report.append(f"📝 **Требования:** {requirement}")
        
        # Обязанности
        responsibility = clean_html(vacancy.get('snippet', {}).get('responsibility', ''))
        if responsibility and len(responsibility) > 5:
            if len(responsibility) > 120:
                responsibility = responsibility[:120] + "..."
            report.append(f"💼 **Обязанности:** {responsibility}")
        
        report.append("")
        report.append("══════════════════════════════")
        report.append("")
    
    # Итоговая статистика
    report.append("📊 **ИТОГИ ПОИСКА:**")
    report.append("─" * 25)
    
    # Статистика по датам
    if vacancies:
        newest_date = parse_date(vacancies[0]['published_at'])
        oldest_date = parse_date(vacancies[-1]['published_at'])
        newest_moscow = newest_date.astimezone(moscow_tz)
        oldest_moscow = oldest_date.astimezone(moscow_tz)
        
        # Форматируем даты правильно
        newest_str = newest_moscow.strftime('%d.%m.%Y %H:%M')
        oldest_str = oldest_moscow.strftime('%d.%m.%Y %H:%M')
        
        report.append(f"• Диапазон дат: {newest_str} - {oldest_str}")
        
        # Количество свежих вакансий
        now_utc = datetime.now(timezone.utc)
        today_count = sum(1 for v in vacancies if parse_date(v['published_at']).date() == now_utc.date())
        recent_count = sum(1 for v in vacancies if (now_utc - parse_date(v['published_at'])).total_seconds() <= 21600)
        
        if today_count > 0:
            report.append(f"• Опубликовано сегодня: **{today_count}**")
        if recent_count > 0:
            report.append(f"• За последние 6 часов: **{recent_count}**")
    
    report.append(f"• Города: {', '.join(f'{city} ({count})' for city, count in city_stats.most_common())}")
    report.append(f"• Специализации: {', '.join(f'{spec.split()[-1]} ({count})' for spec, count in spec_stats.most_common())}")
    
    report.append("")
    report.append(f"🕒 **Отчет обновлен:** {datetime.now().strftime('%d.%m.%Y в %H:%M')}")
    report.append("🔍 **Источник:** hh.ru")
    
    return "\n".join(report)

def fetch_page(session: requests.Session, page: int, per_page: int) -> Optional[Dict[str, Any]]:
    """Получает одну страницу вакансий"""
    params = {
        "text": SEARCH_TEXT,
        "per_page": per_page,
        "page": page,
        "search_field": "name",
        "order_by": "publication_time",
        "only_with_salary": "false"
    }
    
    backoff = INITIAL_BACKOFF
    for attempt in range(1, MAX_PAGE_ATTEMPTS + 1):
        try:
            logger.info(f"GET page={page} per_page={per_page} attempt={attempt}")
            r = session.get(API_URL, params=params, timeout=TIMEOUT)
            
            if r.status_code == 400:
                logger.error(f"400 Bad Request: {r.text[:512]}")
                params_simple = {"text": "python", "per_page": per_page, "page": page}
                r = session.get(API_URL, params=params_simple, timeout=TIMEOUT)
                r.raise_for_status()
                return {"json": r.json()}
                
            if r.status_code == 429:
                ra = r.headers.get("Retry-After")
                try:
                    wait = float(ra) if ra and ra.isdigit() else backoff
                except Exception:
                    wait = backoff
                logger.warning("429 Too Many Requests. Retry-After=%s. Waiting %.1fs", ra, wait)
                time.sleep(wait)
                backoff = min(backoff * 2, 60)
                continue
                
            r.raise_for_status()
            return {"json": r.json()}
            
        except requests.exceptions.RequestException as e:
            logger.warning("RequestException on page %s (attempt %s): %s", page, attempt, e)
            if attempt == MAX_PAGE_ATTEMPTS:
                logger.error("Failed to fetch page %s after %s attempts", page, MAX_PAGE_ATTEMPTS)
                return None
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)
    
    return None

async def collect_once(contact: str, limit: int, out_path: str) -> int:
    """Основная функция сбора вакансий"""
    user_agent = build_user_agent(contact)
    logger.info("Using User-Agent: %s", user_agent)
    logger.info("Target limit: %d vacancies", limit)
    
    session = make_session(user_agent)

    # Определяем период для фильтрации
    period_start, period_end = get_target_period()
    moscow_tz = timezone(timedelta(hours=3))
    period_start_moscow = period_start.astimezone(moscow_tz)
    period_end_moscow = period_end.astimezone(moscow_tz)
    
    logger.info(f"Filter period (Moscow): {period_start_moscow} - {period_end_moscow}")

    page = 0
    per_page = PER_PAGE
    all_vacancies = []
    total_found = 0

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)

    # Собираем вакансии
    while len(all_vacancies) < limit * 3 and page < 20:
        result = fetch_page(session, page, per_page)
        
        if result is None:
            logger.warning("Stopping collection due to repeated errors.")
            break
            
        if "error" in result:
            logger.error("Server returned error: %s", result.get("text", "unknown"))
            break
            
        data = result.get("json", {})
        
        if page == 0:
            total_found = data.get("found", 0)
            logger.info("Total IT vacancies found: %d", total_found)
        
        items = data.get("items", [])
        if not items:
            logger.info("No more items - stopping.")
            break

        for v in items:
            try:
                salary = v.get("salary")
                salary_repr = None
                if salary:
                    salary_repr = {
                        "from": salary.get("from"),
                        "to": salary.get("to"),
                        "currency": salary.get("currency"),
                        "gross": salary.get("gross")
                    }
                
                emp = v.get("employer") or {}
                area = v.get("area") or {}
                
                vacancy_info = {
                    "id": v.get("id"),
                    "name": v.get("name"),
                    "alternate_url": v.get("alternate_url"),
                    "published_at": v.get("published_at"),
                    "salary": salary_repr,
                    "employer": {"id": emp.get("id"), "name": emp.get("name")},
                    "area": {"id": area.get("id"), "name": area.get("name")},
                    "snippet": v.get("snippet") or {}
                }
                
                all_vacancies.append(vacancy_info)
                
            except Exception as e:
                logger.warning(f"Error processing vacancy: {e}")
                continue

        page += 1
        pages_total = data.get("pages", 0)
        if page >= pages_total:
            logger.info("Reached last page %d of %d", page, pages_total)
            break
            
        time.sleep(PAGE_PAUSE)

    logger.info("Collected %d vacancies before filtering", len(all_vacancies))

    # Фильтруем вакансии по периоду и городам
    filtered_vacancies = filter_vacancies(all_vacancies, period_start, period_end)
    
    # Берем только первые N после фильтрации
    final_vacancies = filtered_vacancies[:limit]
    
    # Генерируем красивый отчет
    report = generate_beautiful_report(final_vacancies, total_found, period_start, period_end)
    
    # Сохраняем отчет
    try:
        with open(out_path, "w", encoding="utf-8") as fout:
            fout.write(report)
        logger.info("Report saved to: %s", out_path)
    except Exception as e:
        logger.error("Failed to save report: %s", e)
        print("\n" + "="*60)
        print(report)
        print("="*60)
    
    # Отправляем отчет в Telegram
    telegram_ready = await init_telegram()
    if telegram_ready and final_vacancies:
        await send_to_telegram(report, len(final_vacancies))
    elif telegram_ready and not final_vacancies:
        await send_to_telegram(report, 0)
    
    # Закрытие Telegram клиента
    if tg_client:
        await tg_client.disconnect()
        print("🔚 Telegram клиент отключен")
    
    # Выводим статистику в консоль
    print("\n" + "="*60)
    print("🎯 IT-СТАТИСТИКА:")
    print(f"• Всего найдено на hh.ru: {total_found}")
    print(f"• Собрано вакансий: {len(all_vacancies)}")
    print(f"• После фильтрации: {len(filtered_vacancies)}")
    print(f"• В отчете: {len(final_vacancies)}")
    print(f"• Период: {period_start_moscow.strftime('%d.%m.%Y %H:%M')} - {period_end_moscow.strftime('%d.%m.%Y %H:%M')}")
    
    if final_vacancies:
        cities = [v['area']['name'] for v in final_vacancies]
        specs = [detect_specialization(v['name'], v.get('snippet', {})) for v in final_vacancies]
        city_count = Counter(cities)
        spec_count = Counter(specs)
        
        print(f"• Города: {', '.join(f'{city}({count})' for city, count in city_count.most_common(3))}")
        print(f"• Специализации: {', '.join(f'{spec}({count})' for spec, count in spec_count.most_common(3))}")
        
        # Информация о датах
        newest = parse_date(final_vacancies[0]['published_at'])
        oldest = parse_date(final_vacancies[-1]['published_at'])
        newest_moscow = newest.astimezone(moscow_tz)
        oldest_moscow = oldest.astimezone(moscow_tz)
        
        print(f"• Диапазон дат: {newest_moscow.strftime('%d.%m.%Y %H:%M')} - {oldest_moscow.strftime('%d.%m.%Y %H:%M')}")
    else:
        print("• ❌ Вакансий не найдено по заданным критериям")
    
    print(f"• 📄 Отчет: {out_path}")
    print("="*60)
    
    return len(final_vacancies)

async def main() -> None:
    parser = argparse.ArgumentParser(
        description="HH.ru — парсер IT-вакансий с фильтрацией по периоду и отправкой в Telegram",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  python3 it_vacancies.py --contact "your@email.com" --limit 15

Параметры поиска:
  • Период: с 20:00 вчера до 20:00 сегодня
  • Города: Москва, Санкт-Петербург
  • Отправка в Telegram: автоматически при наличии переменных окружения
        """
    )
    parser.add_argument("--contact", "-c", 
                       help="Контакт для User-Agent (email). Можно установить через HH_CONTACT env var.")
    parser.add_argument("--limit", "-n", type=int, default=15, 
                       help="Сколько вакансий показать в отчете (по умолчанию 15)")
    args = parser.parse_args()

    contact = args.contact or os.environ.get("HH_CONTACT")
    if not validate_contact(contact):
        logger.error('Требуется корректный контакт (email) для User-Agent. Укажите --contact "you@domain.tld" или установите HH_CONTACT.')
        sys.exit(1)

    limit = max(1, args.limit)
    out_path = os.environ.get("OUTPUT_PATH", DEFAULT_OUTPUT_PATH)

    # Запускаем сбор
    try:
        await collect_once(contact, limit, out_path)
    except Exception as e:
        logger.error("Fatal error: %s", e)
        sys.exit(1)

if __name__ == "__main__":

    asyncio.run(main())
