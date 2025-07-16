import requests
import logging
from datetime import datetime, timedelta
import asyncio
from concurrent.futures import ThreadPoolExecutor
import time
logger = logging.getLogger(__name__)

executor = ThreadPoolExecutor(max_workers=5)


async def fetch_report_async(api_token: str, date_from: datetime, date_to: datetime):
    """Асинхронная обертка для получения отчета"""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        executor, 
        fetch_report_detail_by_period, 
        api_token, date_from, date_to
    )

def calculate_period_intervals(start_date: datetime, end_date: datetime):
    """Разбивает период на интервалы по 7 дней (ограничение WB API)"""
    intervals = []
    current = start_date
    while current < end_date:
        next_date = current + timedelta(days=28)
        if next_date > end_date:
            next_date = end_date
        intervals.append((current, next_date))
        current = next_date + timedelta(days=1)
    return intervals

async def fetch_full_report(api_token: str, start_date: datetime, end_date: datetime):
    """Получение полного отчета за период с разбивкой на интервалы"""
    intervals = calculate_period_intervals(start_date, end_date)
    tasks = [fetch_report_async(api_token, start, end) for start, end in intervals]
    results = await asyncio.gather(*tasks)
    full_report = []
    for result in results:
        full_report.extend(result)
    return full_report


def fetch_report_detail_by_period(api_token: str, date_from: datetime, date_to: datetime, retries=3, delay=5):
    """Получение детального отчета по продажам за период с повторными попытками"""
    url = "https://statistics-api.wildberries.ru/api/v5/supplier/reportDetailByPeriod"
    headers = {"Authorization": api_token}
    params = {
        "dateFrom": date_from.strftime("%Y-%m-%d"),
        "dateTo": date_to.strftime("%Y-%m-%d")
    }
    
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            if response.status_code == 200:
                return response.json()
            
            # Обработка ошибки 429 (Too Many Requests)
            if response.status_code == 429:
                retry_after = int(response.headers.get('X-Ratelimit-Retry', 54))
                logger.warning(f"API limit exceeded. Retrying after {retry_after} seconds")
                time.sleep(retry_after)
                continue
                
            logger.warning(f"API error: {response.status_code}, attempt {attempt + 1}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error: {e}, attempt {attempt + 1}")
        except Exception as e:
            logger.error(f"Unknown error: {e}, attempt {attempt + 1}")
        
        time.sleep(delay)
    logger.info("Successfully got report")
    print("GOT REPORT")
    logger.error(f"Failed to fetch report after {retries} attempts")
    return []