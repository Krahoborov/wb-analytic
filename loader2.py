from tg_bot.models import Shop, engine, Order, sessionmaker, CashedShopData, Penalty, Advertisement
import requests
import time
from threading import Thread as th
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


def get_all_penalties(shop: Shop):
    Session = sessionmaker()
    session = Session(bind=engine)
    session.query(Penalty).filter(Penalty.shop_id==shop.id).delete()
    session.commit()
    session.close()


    urls = [
        "https://seller-analytics-api.wildberries.ru/api/v1/analytics/antifraud-details",
        "https://seller-analytics-api.wildberries.ru/api/v1/analytics/incorrect-attachments",
        "https://seller-analytics-api.wildberries.ru/api/v1/analytics/storage-coefficient",
        "https://seller-analytics-api.wildberries.ru/api/v1/analytics/goods-labeling",
        "https://seller-analytics-api.wildberries.ru/api/v1/analytics/characteristics-change"
    ]
    response = requests.get(url=urls[0], headers={"Authorization": f"Bearer {shop.api_token}"})
    Session = sessionmaker()
    session = Session(bind=engine)
    # print(response.text, response.headers)
    for i in response.json()['details']:
        new_penalty = Penalty(
            shop_id=shop.id,
            nm_id=i["nmID"],
            sum=i["sum"],
            type="antifraud",
            date=datetime.strptime(i["dateFrom"], '%Y-%m-%d')
        )
        session.add(new_penalty)

    # response = requests.get(url=urls[1], params={"dateFrom": "2023-06-01", "dateTo": datetime.now().strftime("%Y-%m-%d") }, headers={"Authorization": f"Bearer {shop.api_token}"})
    # print(response.text)
    # for i in response.json()['report']:
    #     new_penalty = Penalty(
    #         shop_id=shop.id,
    #         nm_id=i["nmID"],
    #         sum=i["amount"],
    #         type="incorrect",
    #         date=datetime.strptime(i["date"], '%Y-%m-%d')
    #     )
    #     session.add(new_penalty)
    date_start = datetime.now() - timedelta(days=366)
    for i in range(13):
        date_end = date_start + timedelta(days=30)
        params = {
            "dateFrom": date_start.strftime("%Y-%m-%d"),
            "dateTo": date_end.strftime("%Y-%m-%d")
        }
        response = requests.get(url=urls[3], params=params, headers={"Authorization": f"Bearer {shop.api_token}"})
        # print(response.text)
        if response.status_code == 429:
            print(response.headers)
            print("sleep for", int(response.headers.get("X-Ratelimit-Retry", 1000)), "seconds")
            time.sleep(int(response.headers.get("X-Ratelimit-Retry", 1000)))
            continue
        for i in response.json()['report']:
            new_penalty = Penalty(
                shop_id=shop.id,
                nm_id=i["nmID"],
                sum=i["amount"],
                type="labeling",
                date=datetime.strptime(i["date"][:10], '%Y-%m-%d')
            )
            session.add(new_penalty)

        date_start = date_end
        time.sleep(25)

    date_start = datetime.now() - timedelta(days=366)
    for i in range(13):
        date_end = date_start + timedelta(days=30)
        params = {
            "dateFrom": date_start.strftime("%Y-%m-%d"),
            "dateTo": date_end.strftime("%Y-%m-%d")
        }
        response = requests.get(url=urls[4], params=params, headers={"Authorization": f"Bearer {shop.api_token}"})
        for i in response.json()['report']:
            new_penalty = Penalty(
                shop_id=shop.id,
                nm_id=i["nmID"],
                sum=i["amount"],
                type="labeling",
                date=datetime.strptime(i["date"][:10], '%Y-%m-%d')
            )
            session.add(new_penalty)
        date_start = date_end
        time.sleep(50)

    session.commit()
    session.close()
    print("DONE")


def sync_wb_advertisements(shop: Shop):
    Session = sessionmaker()
    db = Session(bind=engine)

    wb_api_key = shop.api_token
    shop_id = shop.id
    headers = {"Authorization": wb_api_key}

    # Удаляем старые данные за последний год
    try:
        one_year_ago = datetime.now() - timedelta(days=365)
        db.query(Advertisement).filter(
            Advertisement.shop_id == shop_id,
            Advertisement.date >= one_year_ago
        ).delete()
        db.commit()
    except Exception as e:
        db.rollback()
        logger.error(f"Ошибка при удалении старых данных: {str(e)}")
        return

    # Получаем список всех кампаний
    campaign_url = "https://advert-api.wildberries.ru/adv/v1/promotion/count"
    try:
        campaign_response = requests.get(campaign_url, headers=headers)
        if campaign_response.status_code == 429:
            time.sleep(1)
            campaign_response = requests.get(campaign_url, headers=headers)
        campaign_response.raise_for_status()
        campaign_data = campaign_response.json()
    except Exception as e:
        logger.error(f"Ошибка при получении списка кампаний: {str(e)}")
        return

    # Собираем ID всех кампаний
    campaign_ids = []
    for group in campaign_data.get("adverts", []):
        for campaign in group.get("advert_list", []):
            campaign_ids.append(campaign["advertId"])

    if not campaign_ids:
        logger.info("Не найдено кампаний для синхронизации")
        return

    details_url = "https://advert-api.wildberries.ru/adv/v1/promotion/adverts"
    campaign_articles = {}
    print(campaign_ids)
    for i in range(len(campaign_ids)//40+1):
        campaign_ids2 = campaign_ids[i*40:(i+1)*40]
        try:
            details_response = requests.post(details_url, headers=headers, json=campaign_ids2)
            if details_response.status_code == 429:
                time.sleep(1)
                details_response = requests.post(details_url, headers=headers, json=campaign_ids2)
            details_response.raise_for_status()

            if details_response.status_code == 200:
                details_data = details_response.json()
                for campaign in details_data:
                    # print(campaign)
                    advert_id = campaign["advertId"]
                    articles = []
                    if "nmCPM" in campaign:
                        articles += [i["nm"] for i in campaign['nmCPM']]
                    if "nms" in campaign:
                        articles.extend(campaign["nms"])
                    params = campaign.get("params", [])
                    if params == []:
                        params = [campaign.get("autoParams")] if campaign.get("autoParams", []) != [] else []
                    for param in params:
                        if "nms" in param:
                            articles.extend(param["nms"])

                            # print(param)
                        elif "subjectId" in param:
                            articles.append(param["subjectId"])
                        elif "setName" in param:
                            articles.append(f"set_{param['setName']}")
                    for param in campaign.get("unitedParams", []):
                        if "nms" in param:
                            # print(param)
                            articles.extend(param["nms"])
                        elif "subjectId" in param:
                            articles.append(param["subjectId"])
                        elif "setName" in param:
                            articles.append(f"set_{param['setName']}")
                    if articles == []:
                        print(campaign, "\n", articles, "\n")
                    campaign_articles[advert_id] = articles
        except Exception as e:
            logger.error(f"Ошибка при получении деталей кампаний: {str(e)}")
            return
        time.sleep(1)

    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=365)

    current_date = start_date
    while current_date <= end_date:
        interval_end = min(current_date + timedelta(days=31), end_date)
        from_str = current_date.strftime('%Y-%m-%d')
        to_str = interval_end.strftime('%Y-%m-%d')

        history_url = (
            f"https://advert-api.wildberries.ru/adv/v1/upd"
            f"?from={from_str}&to={to_str}"
        )

        try:
            history_response = requests.get(history_url, headers=headers)
            if history_response.status_code == 429:
                time.sleep(1)
                history_response = requests.get(history_url, headers=headers)
            history_response.raise_for_status()

            if history_response.status_code == 200:
                history_data = history_response.json()

                for record in history_data:
                    # print(record)
                    advert_id = record["advertId"]
                    amount = record["updSum"]

                    try:
                        record_date = datetime.fromisoformat(record["updTime"]).date()
                    except:
                        record_date = current_date + (interval_end - current_date) / 2

                    articles = campaign_articles.get(int(advert_id), [])
                    # print(campaign_articles)
                    print(articles)
                    if articles:
                        amount_per_article = amount / len(articles)
                        for article in articles:
                            ad = Advertisement(
                                shop_id=shop_id,
                                amount=amount_per_article,
                                date=record_date,
                                nmId=article,
                                advert_id=advert_id,
                                created_at=datetime.now()
                            )
                            db.add(ad)
                    else:
                        articles = ''
                        ad = Advertisement(
                            shop_id=shop_id,
                            amount=amount,
                            date=record_date,
                            nmId=None,
                            advert_id=advert_id,
                            created_at=datetime.now()
                        )
                        db.add(ad)

                db.commit()
                logger.info(f"Обработан период {from_str}-{to_str}: {len(history_data)} записей")
        except Exception as e:
            logger.error(f"Ошибка при обработке периода {from_str}-{to_str}: {str(e)}")

        current_date = interval_end + timedelta(days=1)
        time.sleep(1)

    logger.info(f"Синхронизация завершена для магазина {shop_id}")
    # print(campaign_articles)


def checker_penalties():
    while True:
        Session = sessionmaker()
        session = Session(bind=engine)
        shops = session.query(Shop).all()
        session.close()
        for shop in shops:
            th(target=get_all_penalties, args=(shop,)).start()
        time.sleep(3600)

def checker_advertisement():
    while True:
        Session = sessionmaker()
        session = Session(bind=engine)
        shops = session.query(Shop).all()
        session.close()
        for shop in shops:
            th(target=sync_wb_advertisements, args=(shop,)).start()
        time.sleep(3600)

if __name__ == "__main__":
    th(target=checker_advertisement).start()
    time.sleep(100)
    th(target=checker_penalties).start()


