from tg_bot.models import Shop, engine, sessionmaker, Order
import requests
import time
from datetime import datetime, timedelta
import requests


def get_orders(api_token, start_date):
    headers = {'Authorization': api_token}
    params = {'dateFrom': start_date.isoformat(), 'flag': 0}
    response = requests.get('https://statistics-api.wildberries.ru/api/v1/supplier/orders', headers=headers,
                            params=params)
    return response.json()


def get_buys(api_token, start_date):
    headers = {'Authorization': api_token}
    params = {'dateFrom': start_date.isoformat(), 'flag': 0}
    response = requests.get('https://statistics-api.wildberries.ru/api/v1/supplier/sales', headers=headers,
                            params=params)
    return response.json()


def save_order_data(session, order_data, account_id):
    try:
        print("SAVE ORDER FOR", account_id)
        order = Order(
            srid=order_data['srid'],
            date=datetime.fromisoformat(order_data['date']),
            lastChangeDate=datetime.fromisoformat(order_data['lastChangeDate']),
            warehouseName=order_data['warehouseName'],
            warehouseType=order_data['warehouseType'],
            countryName=order_data['countryName'],
            oblastOkrugName=order_data['oblastOkrugName'],
            regionName=order_data['regionName'],
            supplierArticle=order_data['supplierArticle'],
            nmId=order_data['nmId'],
            barcode=order_data['barcode'],
            category=order_data['category'],
            subject=order_data['subject'],
            brand=order_data['brand'],
            techSize=order_data['techSize'],
            incomeID=order_data['incomeID'],
            isSupply=order_data['isSupply'],
            isRealization=order_data['isRealization'],
            totalPrice=order_data['totalPrice'],
            discountPercent=order_data['discountPercent'],
            spp=order_data['spp'],
            finishedPrice=order_data['finishedPrice'],
            priceWithDisc=order_data['priceWithDisc'],
            isCancel=order_data['isCancel'] if 'isCancel' in order_data else False,
            cancelDate=datetime.fromisoformat(order_data['cancelDate']) if 'cancelDate' in order_data and order_data[
                'cancelDate'] != '0001-01-01T00:00:00' else None,
            orderType=order_data['orderType'] if 'orderType' in order_data else None,
            sticker=order_data['sticker'],
            gNumber=order_data['gNumber'],
            shop_id=int(account_id),
            forPay=order_data.get("forPay", 0)
        )
        session.merge(order)

        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Error saving order: {e}")


def process_buy(buy_data, account):
    session = sessionmaker(bind=engine)()
    order = session.query(Order).filter(Order.srid == buy_data['srid']).first()
    print("process_buy")
    if order is None or order.is_bouhght == False:
        save_order_data(session, buy_data, account.id)

        order = session.query(Order).filter(Order.srid == buy_data['srid']).first()
        order.is_bouhght = True
        session.add(order)
        session.commit()
    session.close()


def checker():
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        accounts = session.query(Shop).all()
        for account in accounts:
            print(account)
            account2 = session.query(Shop).filter(Shop.api_token == account.api_token).order_by(Shop.id).first()
            account = account2

            try:
                start_date = datetime.now() - timedelta(days=8)
                orders_data = get_orders(account.api_token, start_date)
                if orders_data == -1:
                    continue
                buys_data = get_buys(account.api_token, start_date)
                is_skip = session.query(Order).filter(Order.shop_id == account.id).first() == None
                try:
                    for order_data in orders_data:
                        save_order_data(session, order_data, account.id)
                except:
                    pass
                print("process buy")
                print(len(buys_data))
                for buy_data in buys_data:
                    try:
                        save_order_data(session, buy_data, account.id)
                        order = session.query(Order).filter(Order.srid == buy_data['srid']).first()
                        order.is_bouhght = True
                        session.add(order)
                        session.commit()
                    except Exception as E:
                        print(E, buy_data)
            except Exception as e:
                print(f"Error processing account {account.id}: {e}")
    finally:
        session.close()


if __name__ == "__main__":
    while True:
        checker()
        print("sleep")
        time.sleep(120)
