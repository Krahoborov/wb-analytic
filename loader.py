# from tg_bot.services.wb_api import fetch_full_report
from calendar import week
import asyncio, datetime
from tg_bot.models import User, Shop, Order, engine, sessionmaker, CashedShopData
import requests
from threading import Thread as th
import logging
from datetime import date, datetime, timedelta
import asyncio
from datetime import time as timed
from concurrent.futures import ThreadPoolExecutor
import time

from tg_bot.models.DBSM import CashedShopData
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

def fetch_full_report(api_token: str):
    """Получение полного отчета за всё"""
    now = datetime.now()
    date_from = datetime(year=2024, month=1, day=29)
    date_to = datetime.now()
    full_report = []
    url = "https://statistics-api.wildberries.ru/api/v5/supplier/reportDetailByPeriod"
    headers = {"Authorization": api_token}
    all_done = False
    rrd = 0
    for i in range(1000):
        
        params = {
            "dateFrom": date_from.strftime("%Y-%m-%d"),
            "dateTo": date_to.strftime("%Y-%m-%d"),
            "rrdid": rrd
        }
        for attempt in range(3):
            try:
                response = requests.get(url, headers=headers, params=params, timeout=30)
                if response.status_code == 200:
                    if response.json() == []:
                        all_done = True
                        break
                    rrd = response.json()[-1]['rrd_id']
                    full_report += response.json()
                    break
                
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
        
        if all_done:
            break

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

async def get_reports():
    Session = sessionmaker(bind=engine)
    session = Session()
    shops = session.query(Shop).all()
    for shop in shops:
        print("SHOP ID", shop.id, "GET API TOKEN")
        full = fetch_full_report(shop.api_token)
        month_report = []
        week_report = []
        now = datetime.now()
        year_report = []
        for i in full:
            if datetime.strptime(i["sale_dt"][:10], "%Y-%m-%d") >= datetime(now.year, 1, 1, 0, 0):
                year_report.append(i)
            if datetime.strptime(i["sale_dt"][:10], "%Y-%m-%d") >= datetime(now.year, now.month, 1, 0, 0):
                month_report.append(i)
            star = datetime.today() - timedelta(days=datetime.today().isoweekday() % 7)
            current_start = datetime(star.year, star.month, star.day, 0, 0)
            if datetime.strptime(i["sale_dt"][:10], "%Y-%m-%d") >= current_start:
                week_report.append(i)
        cashed_shop = session.query(CashedShopData).filter_by(shop_id=shop.id).first()
        if cashed_shop is None:
            cashed_shop = CashedShopData(shop_id=shop.id)
        cashed_shop.cashed_week = week_report
        cashed_shop.cashed_all = full
        cashed_shop.cashed_month = month_report
        cashed_shop.cashed_year = year_report
        session.add(cashed_shop)
    session.commit()
    session.close()

def get_reports_none():
    Session = sessionmaker(bind=engine)
    session = Session()
    shops = session.query(Shop).all()
    for shop in shops:
        print("SHOP ID", shop.id, "GET API TOKEN")
        full = fetch_full_report(shop.api_token)
        month_report = []
        week_report = []
        now = datetime.now()
        year_report = []
        for i in full:
            if datetime.strptime(i["sale_dt"][:10], "%Y-%m-%d") >= datetime(now.year, 1, 1, 0, 0):
                year_report.append(i)
            if datetime.strptime(i["sale_dt"][:10], "%Y-%m-%d") >= datetime(now.year, now.month, 1, 0, 0):
                month_report.append(i)
            star = datetime.today() - timedelta(days=datetime.today().isoweekday() % 7)
            current_start = datetime(star.year, star.month, star.day, 0, 0)
            if datetime.strptime(i["sale_dt"][:10], "%Y-%m-%d") >= current_start:
                week_report.append(i)
        cashed_shop = session.query(CashedShopData).filter_by(shop_id=shop.id).first()
        if cashed_shop is None:
            cashed_shop = CashedShopData(shop_id=shop.id)
        cashed_shop.cashed_week = week_report
        cashed_shop.cashed_all = full
        cashed_shop.cashed_month = month_report
        cashed_shop.cashed_year = year_report
        session.add(cashed_shop)
    session.commit()
    session.close()


def get_reports_free():
    while True:
        get_reports_none()
        time.sleep(15)

def get_reports_free_1():
    while True:
        print("RUN GET REPORTS")
        asyncio.run(get_reports())
        time.sleep(10000)

    
if __name__ == "__main__":
    th(target=get_reports_free).start()
    time.sleep(10000)
    th(target=get_reports_free_1).start()