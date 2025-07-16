import asyncio
import logging

from aiogram import Bot, Dispatcher
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from tg_bot.config import load_config
from tg_bot.handlers import register_all_handlers
from tg_bot.handlers.subscription import ActivityMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler
logger = logging.getLogger(__name__)


async def cleanup_inactive_users():
    print("Запускаю очистку неактивных пользователей...")
    session = sessionmaker(bind=engine)()
    threshold = datetime.utcnow() - timedelta(days=30)

    inactive_users = session.query(User).filter(User.last_active < threshold).all()

    for user in inactive_users:
        user_shops = session.query(Shop).filter(Shop.user_id == user.id).all()
        for shop in user_shops:
            shop_id = shop.id
            session.query(Order).filter(Order.shop_id == shop_id).delete()
            session.query(ProductCost).filter(ProductCost.shop_id == shop_id).delete()
            session.query(OneTimeExpense).filter(OneTimeExpense.shop_id == shop_id).delete()
            session.query(Advertisement).filter(Advertisement.shop_id == shop_id).delete()
            session.query(Penalty).filter(Penalty.shop_id == shop_id).delete()
            session.delete(shop)
        session.delete(user)

    session.commit()
    session.close()
    print(f"Удалено пользователей: {len(inactive_users)}")

async def main():
    logging.basicConfig(
        level=logging.INFO,
        format=u'%(filename)s:%(lineno)d #%(levelname)-8s [%(asctime)s] - %(name)s - %(message)s'
    )
    config = load_config(".env")
    #token = '7356744583:AAHL7az8lnO9ZRqngd5YfVTtJa9zxAPQeMo'
    token = '7541007648:AAEUdZO949aHmH3qs62doFGr6bf6txrp5HU'
    bot = Bot(token=token, parse_mode="HTML")
    storage = MemoryStorage()
    dp = Dispatcher(bot, storage=storage)
    bot['config'] = config

    # Регистрируем middleware
    dp.middleware.setup(ActivityMiddleware())

    register_all_handlers(dp)

    # --- Здесь запускаем планировщик ---
    scheduler = AsyncIOScheduler()
    scheduler.add_job(cleanup_inactive_users, 'interval', days=1)
    scheduler.start()
    # ------------------------------------

    try:
        await dp.start_polling()
    finally:
        await dp.storage.close()
        await dp.storage.wait_closed()
        await bot.session.close()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.error("Bot stopped!")
