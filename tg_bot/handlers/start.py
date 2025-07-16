from aiogram import types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, InputFile, InputMediaPhoto
from aiogram.dispatcher import FSMContext
from tg_bot.keyboards.main_menu import main_menu_keyboard, menu_keyboard
from datetime import datetime

from tg_bot.models import User, Shop, engine, sessionmaker

async def start_command(message: types.Message, state: FSMContext):
    # await state.finish()
    
    welcome_text = (
        "👋 <b>Добро пожаловать в JustProfit!</b>\n\n"
        "Я помогу вам автоматизировать финансовый анализ вашего бизнеса на Wildberries:\n"
        "✅ Расчет рентабельности и ROI\n"
        "✅ Анализ сроков окупаемости\n"
        "✅ Персонализированные рекомендации\n"
        "✅ Симулятор сценариев \"А что если?\"\n\n"
        
        "<b>Как начать:</b>\n"
        "1. Добавьте ваш магазин WB через API-токен\n"
        "2. Настройте параметры бизнеса\n"
        "3. Получите первый финансовый отчет\n\n"
        
        "<i>Первые 14 дней - бесплатный период!</i>\n\n"
    )
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(InlineKeyboardButton("Добавить магазин", callback_data="add_shop"))
    async with state.proxy() as data:
        print(data)
        Session = sessionmaker()
        session = Session(bind=engine)
        user = session.query(User).filter(User.telegram_id == message.from_user.id).first()
        if user:
            user.last_active = datetime.utcnow()  # ← ДОБАВИТЬ
            session.commit()  # ← ДОБАВИТЬ
        if user:
            shop = session.query(Shop).filter(Shop.user_id==user.id).first()
        else:
            shop = None
        if shop:
            
            async with state.proxy() as data:
                data['shop'] = {
                    'id': shop.id,
                    'name': shop.name,
                    'api_token': shop.api_token
                }
            text = f"<b>Ваш кабинет:</b>\nМагазин: {shop.name}"
            text += "\n\nВыберите действие:"
            await message.answer_photo(photo=open("фото1.jpeg", "rb"), caption=text, reply_markup=main_menu_keyboard())
            
        else:
            user = session.query(User).filter(User.telegram_id == message.from_user.id).first()
            if user:
                shops = session.query(Shop).filter(Shop.user_id == user.id).all()
                if user and shops:
                    text = f"<b>Ваш кабинет:</b>\nМагазин: Не выбран"
                    text += "\n\nВыберите действие:"
                    await message.answer_photo(photo=open("фото1.jpeg", "rb"), caption=text, reply_markup=main_menu_keyboard())
                else:
                    text = welcome_text
                    await message.answer_photo(photo=open("фото1.jpeg", "rb"), caption=text, reply_markup=keyboard)
            else:
                text = welcome_text
                await message.answer_photo(photo=open("фото1.jpeg", "rb"), caption=text, reply_markup=keyboard)
        session.close()
            

async def start_query(call: types.CallbackQuery, state: FSMContext):
    await call.message.delete()
    # await state.finish()
    welcome_text = (
        "👋 <b>Добро пожаловать в JustProfit!</b>\n\n"
        "Я помогу вам автоматизировать финансовый анализ вашего бизнеса на Wildberries:\n"
        "✅ Расчет рентабельности и ROI\n"
        "✅ Анализ сроков окупаемости\n"
        "✅ Персонализированные рекомендации\n"
        "✅ Симулятор сценариев \"А что если?\"\n\n"
        
        "<b>Как начать:</b>\n"
        "1. Добавьте ваш магазин WB через API-токен\n"
        "2. Настройте параметры бизнеса\n"
        "3. Получите первый финансовый отчет\n\n"
        
        "<i>Первые 14 дней - бесплатный период!</i>\n\n"
    )
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(InlineKeyboardButton("Добавить магазин", callback_data="add_shop"))
    
    async with state.proxy() as data:
        if 'shop' in data:
            Session = sessionmaker()
            session = Session(bind=engine)
            shop = session.query(Shop).filter(Shop.id == data['shop']['id']).first()
            text = f"<b>Ваш кабинет:</b>\nМагазин: {shop.name if shop.name else 'Не указан'}"
            text += "\n\nВыберите действие:"
            await call.message.answer_photo(photo=open("фото1.jpeg", "rb"), caption=text, reply_markup=main_menu_keyboard())
        else:
            Session = sessionmaker()
            session = Session(bind=engine)
            user = session.query(User).filter(User.telegram_id == call.from_user.id).first()
            if user:
                shops = session.query(Shop).filter(Shop.user_id == user.id).all()
                if user and shops:
                    text = f"<b>Ваш кабинет:</b>\nМагазин: Не выбран"
                    text += "\n\nВыберите действие:"
                    await call.message.answer_photo(photo=open("фото1.jpeg", "rb"), caption=text, reply_markup=main_menu_keyboard())
                else:
                    text = welcome_text
                    await call.message.answer_photo(photo=open("фото1.jpeg", "rb"), caption=text, reply_markup=keyboard)
            else:
                text = welcome_text
                await call.message.answer_photo(photo=open("фото1.jpeg", "rb"), caption=text, reply_markup=keyboard)
            session.close()


async def main_menu(call: types.CallbackQuery, state: FSMContext):
    text = ("<b>Главное меню</b>\nВыберите действие:")
    Session = sessionmaker()
    session = Session(bind=engine)
    user = session.query(User).filter(User.telegram_id == call.from_user.id).first()
    shop = session.query(Shop).filter(Shop.user_id==user.id).first()
    if shop:
        
        async with state.proxy() as data:
            data['shop'] = {
                'id': shop.id,
                'name': shop.name,
                'api_token': shop.api_token
            }
    session.close()
    text = " <b>Это раздел финансов</b>\n\nЗдесь Вы можете узнать свои главные показатели по своему бизнесу.\n\n▫️ Чистая прибыль\n▫️ ROS — Рентабельность продаж, покажет, сколько Вы зарабатываете с одного рубля выручки\n▫️ Сроки окупаемости с учетом всех Ваших первоначальных затрат\n▫️ Рентабельность инвестиций покажет, насколько выгоден Ваш проект и как быстро он окупается\n▫️ Годовая доходность Вашего бизнеса покажет, насколько выгоден Ваш бизнес"
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("Чистая прибыль", callback_data="an_1"))
    #kb.add(InlineKeyboardButton("ROS(Рентабльность продаж)", callback_data="an_2"))
    kb.add(InlineKeyboardButton("Срок окупаемости", callback_data="an_3"))
    kb.add(InlineKeyboardButton("ROI(Рентабельность вложений)", callback_data="an_4"))
    kb.add(InlineKeyboardButton("Годовая доходность", callback_data="an_5"))
    kb.add(InlineKeyboardButton("Меню", callback_data="main_menu"))
    file = InputMediaPhoto(open("фото2.jpeg", "rb"), caption=text)
    await call.message.edit_media(file, reply_markup=kb)


def register_start_handlers(dp):
    dp.register_message_handler(start_command, commands=['start'], state="*")
    dp.register_callback_query_handler(start_query, text='main_menu', state="*")
    dp.register_callback_query_handler(main_menu, text='menu', state="*")