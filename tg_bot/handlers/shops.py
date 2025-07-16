import datetime
import logging
import asyncio
from aiogram import types
from aiogram.dispatcher import FSMContext
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from tg_bot.keyboards.shops_menu import shops_menu_keyboard, manage_shops_keyboard, shop_management_keyboard, back_to_shops_keyboard
from tg_bot.models import sessionmaker, engine
from tg_bot.models import User, TaxSystemSetting, TaxSystemType
from tg_bot.models import Shop
from tg_bot.handlers.start import start_command
from threading import Thread as th
import requests
from loader2 import get_all_penalties, sync_wb_advertisements
logger = logging.getLogger(__name__)

async def add_shop_callback(callback: types.CallbackQuery, state: FSMContext):
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(InlineKeyboardButton("🔙 Назад", callback_data="main_menu"))
    try:
        await callback.message.edit_text(
            "🔑 <b>Добавление магазина</b>\n\n"
            "1. Зайдите в личный кабинет WB\n"
            "2. Перейдите в раздел \"Настройки\" → \"Доступ к API\"\n"
            "3. Создайте новый токен с правами \"Только чтение\"\n"
            "4. Пришлите токен в этом сообщении\n\n",
            "<u>ВАЖНО: используйте новый токен и только для этого сервиса. Это позволит получить максимальную скорость генерации аналитических показателей</u>", parse_mode="HTML",
            reply_markup=keyboard
        )
    except:
        await callback.message.delete()
        await callback.message.answer(
            "🔑 <b>Добавление магазина</b>\n\n"
            "1. Зайдите в личный кабинет WB\n"
            "2. Перейдите в раздел \"Настройки\" → \"Доступ к API\"\n"
            "3. Создайте новый токен с правами \"Только чтение\"\n"
            "4. Пришлите токен в этом сообщении\n\n",
            reply_markup=keyboard
        )
    await state.set_state("waiting_for_api_token")





def get_seller_name(api_token: str):
    """Получение имени продавца по API-токену Wildberries"""
    try:
        url = "https://common-api.wildberries.ru/api/v1/seller-info"
        response = requests.get(url, headers={"Authorization": api_token}, timeout=10)
        if response.status_code == 200:
            return response.json().get("name", "Неизвестное имя")
        return None
    except Exception as e:
        logger.error(f"Ошибка при получении имени продавца: {e}")
        return None

async def shops_menu_callback(callback: types.CallbackQuery, state: FSMContext):
    """Меню управления магазинами"""
    session = sessionmaker()(bind=engine)
    try:
        user = session.query(User).filter(User.telegram_id == callback.from_user.id).first()
        
        text = "🏪 <b>Управление магазинами</b>"
        keyboard = shops_menu_keyboard()
        
        if user:
            shops = session.query(Shop).filter(Shop.user_id == user.id).all()
            if shops:
                text += "\n\nВаши магазины:"
                shop2 = {}
                shop2['id'] = -1
                async with state.proxy() as data:
                    try:
                        shop2['id'] = data['shop']['id']
                    except:
                        pass
                for shop in shops:
                    text += f"\n- {'✅ ' if shop.id == shop2['id'] else ''}{shop.name if shop.name else 'Без названия'} (ID: {shop.id})"
            else:
                text += "\n\n❌ У вас нет добавленных магазинов"
        else:
            text += "\n\n❌ Пользователь не найден"
        
        await callback.message.edit_text(text, reply_markup=keyboard)
    finally:
        session.close()


async def process_api_token(message: types.Message, state: FSMContext):
    """Обработка введенного API-токена"""
    api_token = message.text.strip()
    seller_name = get_seller_name(api_token)
    
    if not seller_name:
        await message.answer("❌ Неверный токен или ошибка подключения. Проверьте токен и попробуйте еще раз")
        return
    
    session = sessionmaker()(bind=engine)
    try:
        user = session.query(User).filter(User.telegram_id == message.from_user.id).first()
        
        if not user:
            days_14_after = datetime.datetime.now() + datetime.timedelta(days=14)
            user = User(telegram_id=message.from_user.id, subscription_start=datetime.datetime.now(), subscription_end=days_14_after, is_trial_used=True)

            session.add(user)
            session.commit()
            session.refresh(user)
        

        
        # Создаем новый магазин
        shop = Shop(api_token=api_token, name=seller_name, user_id=user.id)
        session.add(shop)
        session.commit()
        tax_setting = TaxSystemSetting(shop_id=shop.id, tax_system=TaxSystemType.NO_TAX)
        th(target=sync_wb_advertisements, args=(shop,)).start()
        th(target=get_all_penalties, args=(shop,)).start()
        session.add(tax_setting)
        session.commit()

        
        if len(user.shops) == 1:
            async with state.proxy() as data:
                data['shop'] = {
                    'id': shop.id,
                    'name': shop.name,
                    'api_token': shop.api_token
                }
            await message.answer(f"✅ Магазин <b>{seller_name}</b> добавлен! <u>‼️ Важно: необходимо подождать около 2-3х минут, чтобы я получил все данные по Вашему магазину и все функции бота работали корректно.</u>")
        else:
            await message.answer(f"✅ Магазин <b>{seller_name}</b> успешно добавлен! <u>‼️ Важно: необходимо подождать около 2-3х минут, чтобы я получил все данные по Вашему магазину и все функции бота работали корректно.</u>")
        await start_command(message, state)
    except Exception as e:
        logger.error(f"Ошибка при добавлении магазина: {e}")
        await message.answer("❌ Произошла ошибка при добавлении магазина")
    finally:
        session.close()
        await state.finish()
        # await show_shops_menu(message, state)

async def show_shops_menu(message: types.Message, state: FSMContext):
    """Показать меню магазинов с указанием выбранного"""
    session = sessionmaker()(bind=engine)
    try:
        user = session.query(User).filter(User.telegram_id == message.from_user.id).first()
        
        # Получаем текущий выбранный магазин
        current_shop = None
        async with state.proxy() as data:
            print("DATA", data)
            current_shop = data.get('shop', None)
        
        text = "🏪 <b>Выбор магазина</b>"
        
        if current_shop:
            text += f"\n\n✅ Текущий магазин: <b>{current_shop['name'] or current_shop['id']}</b>"
        
        keyboard = InlineKeyboardMarkup(row_width=1)
        
        if user and user.shops:
            text += "\n\nВаши магазины:"
            for shop in user.shops:
                prefix = "✅ " if current_shop and shop.id == current_shop['id'] else "🔹 "
                text += f"\n{prefix}{shop.name if shop.name else 'Без названия'} (ID: {shop.id})"
                
                btn_text = f"✅ Выбран" if current_shop and shop.id == current_shop['id'] else "Выбрать магазин"
                keyboard.add(InlineKeyboardButton(
                    f"{btn_text} - {shop.name or shop.id}", 
                    callback_data=f"select_shop_{shop.id}"
                ))
        else:
            text += "\n\n❌ У вас нет добавленных магазинов"
        
        # Кнопки управления
        keyboard.add(InlineKeyboardButton("➕ Добавить магазин", callback_data="add_shop"))
        keyboard.add(InlineKeyboardButton("⚙️ Управление магазинами", callback_data="manage_shops"))
        keyboard.add(InlineKeyboardButton("🔙 Назад", callback_data="main_menu"))
        
        await message.answer(text, reply_markup=keyboard)
    finally:
        session.close()

async def manage_shops_list_callback(callback: types.CallbackQuery, state: FSMContext):
    """Отображение списка магазинов для управления"""
    session = sessionmaker()(bind=engine)
    try:
        user = session.query(User).filter(User.telegram_id == callback.from_user.id).first()
        shops = session.query(Shop).filter(Shop.user_id == user.id).all()
        if not user or not shops:
            await callback.message.edit_text(
                "❌ У вас нет магазинов для управления",
                reply_markup=back_to_shops_keyboard()
            )
            return
        
        # Получаем текущий выбранный магазин
        current_shop = None
        async with state.proxy() as data:
            current_shop = data.get('shop', None)
        
        text = "🏪 <b>Выберите магазин для управления:</b>"
        keyboard = manage_shops_keyboard(shops, current_shop)
        await callback.message.edit_text(text, reply_markup=keyboard)
    finally:
        session.close()

async def manage_shop_callback(callback: types.CallbackQuery, state: FSMContext):
    """Управление конкретным магазином"""
    print(callback.data)
    shop_id = int(callback.data.split('_')[1])
    async with state.proxy() as data:
        print(data, data.get('shop', None))
    session = sessionmaker()(bind=engine)
    try:
        shop = session.query(Shop).filter(Shop.id == shop_id).first()
        
        if not shop:
            await callback.answer("❌ Магазин не найден", show_alert=True)
            return
        
        # Проверяем, выбран ли этот магазин
        is_selected = False
        async with state.proxy() as data:
            current_shop = data.get('shop', None)
            is_selected = current_shop and current_shop['id'] == shop.id
        
        text = (
            f"⚙️ <b>Управление магазином</b>\n\n"
            f"<b>Название:</b> {shop.name or 'Не указано'}\n"
            f"<b>ID магазина:</b> {shop.id}\n"
            f"<b>Статус:</b> {'✅ Выбран' if is_selected else '❌ Не выбран'}\n"
        )
        
        keyboard = shop_management_keyboard(shop_id, is_selected)
        await callback.message.edit_text(text, reply_markup=keyboard)
    finally:
        session.close()

async def change_api_callback(callback: types.CallbackQuery, state: FSMContext):
    """Изменение API-ключа магазина"""
    shop_id = int(callback.data.split('_')[2])
    await state.update_data(shop_id=shop_id)
    
    await callback.message.edit_text(
        " <b>Изменение API-ключа</b>\n\n"
        "1. Зайдите в личный кабинет WB\n"
        "2. Перейдите в раздел \"Настройки\" → \"Доступ к API\"\n"
        "3. Создайте новый токен с правами \"Только чтение\"\n"
        "4. Пришлите новый токен в этом сообщении\n\n"
        "<u>ВАЖНО: используйте новый токен и только для этого сервиса. Это позволит получить максимальную скорость генерации аналитических показателей</u>", parse_mode="HTML"
    )
    await state.set_state("waiting_for_new_api_token")

async def process_new_api_token(message: types.Message, state: FSMContext):
    """Обработка нового API-токена"""
    new_token = message.text.strip()
    seller_name = get_seller_name(new_token)
    
    if not seller_name:
        await message.answer("❌ Неверный токен или ошибка подключения. Проверьте токен и попробуйте еще раз")
        return
    
    user_data = await state.get_data()
    shop_id = user_data.get('shop_id')
    
    if not shop_id:
        await message.answer("❌ Ошибка: не указан магазин")
        await state.finish()
        return
    
    session = sessionmaker()(bind=engine)
    try:
        shop = session.query(Shop).filter(Shop.id == shop_id).first()
        
        if shop:
            shop.api_token = new_token
            shop.name = seller_name
            session.commit()
            await message.answer(f"✅ API-ключ для магазина <b>{seller_name}</b> успешно обновлен!")
            shop = session.query(Shop).filter(Shop.id == shop_id).first()
            th(target=sync_wb_advertisements, args=(shop,)).start()
            th(target=get_all_penalties, args=(shop,)).start()
        else:
            await message.answer("❌ Магазин не найден")
    except Exception as e:
        logger.error(f"Ошибка при обновлении API-ключа: {e}")
        await message.answer("❌ Произошла ошибка при обновлении API-ключа")
    finally:
        session.close()
        await state.finish()
        await start_command(message, state)

async def delete_shop_callback(callback: types.CallbackQuery):
    """Подтверждение удаления магазина"""
    shop_id = int(callback.data.split('_')[2])
    
    session = sessionmaker()(bind=engine)
    try:
        shop = session.query(Shop).filter(Shop.id == shop_id).first()
        
        if not shop:
            await callback.answer("❌ Магазин не найден", show_alert=True)
            return
        
        keyboard = InlineKeyboardMarkup(row_width=2)
        keyboard.add(
            InlineKeyboardButton("✅ Да, удалить", callback_data=f"confirm_delete_{shop_id}"),
            InlineKeyboardButton("❌ Отмена", callback_data=f"manage_{shop_id}")
        )
        
        await callback.message.edit_text(
            f"❌ Вы уверены, что хотите удалить магазин <b>{shop.name or 'Без названия'}</b>?\n"
            "Все данные этого магазина будут безвозвратно удалены!",
            reply_markup=keyboard
        )
    finally:
        session.close()

async def confirm_delete_callback(callback: types.CallbackQuery, state: FSMContext):
    """Окончательное удаление магазина"""
    shop_id = int(callback.data.split('_')[2])
    
    session = sessionmaker()(bind=engine)
    try:
        shop = session.query(Shop).filter(Shop.id == shop_id).first()
        
        if shop:
            shop_name = shop.name or f"Магазин {shop.id}"
            session.delete(shop)
            session.commit()
            await callback.message.edit_text(f"✅ Магазин <b>{shop_name}</b> успешно удален!")
            await callback.answer()
            
            # Возврат к списку магазинов через 2 секунды
            await asyncio.sleep(2)
            await show_shops_menu(callback.message, state)
        else:
            await callback.answer("❌ Магазин не найден", show_alert=True)
    finally:
        session.close()

async def select_shop_callback(callback: types.CallbackQuery, state: FSMContext):
    """Обработка выбора магазина"""
    shop_id = int(callback.data.split('_')[2])

    session = sessionmaker()(bind=engine)
    try:
        shop = session.query(Shop).filter(Shop.id == shop_id).first()
        if not shop:
            await callback.answer("❌ Магазин не найден", show_alert=True)
            return
        
        # Сохраняем выбранный магазин в state
        async with state.proxy() as data:
            data['shop'] = {
                'id': shop.id,
                'name': shop.name,
                'api_token': shop.api_token
            }
        
        await callback.answer(f"✅ Выбран магазин: {shop.name or shop.id}")
    finally:
        session.close()
    try:
        shop = session.query(Shop).filter(Shop.id == shop_id).first()
        
        if not shop:
            await callback.answer("❌ Магазин не найден", show_alert=True)
            return
        
        # Проверяем, выбран ли этот магазин
        is_selected = False
        async with state.proxy() as data:
            current_shop = data.get('shop', None)
            is_selected = current_shop and current_shop['id'] == shop.id
        
        text = (
            f"⚙️ <b>Управление магазином</b>\n\n"
            f"<b>Название:</b> {shop.name or 'Не указано'}\n"
            f"<b>ID магазина:</b> {shop.id}\n"
            f"<b>Статус:</b> {'✅ Выбран' if is_selected else '❌ Не выбран'}\n"
        )
        
        keyboard = shop_management_keyboard(shop_id, is_selected)
        await callback.message.edit_text(text, reply_markup=keyboard)
    finally:
        session.close()

async def unselect_shop_callback(callback: types.CallbackQuery, state: FSMContext):
    """Отмена выбора магазина"""
    shop_id = int(callback.data.split('_')[2])
    
    async with state.proxy() as data:
        current_shop = data.get('shop', None)
        if current_shop and current_shop['id'] == shop_id:
            print(data)
            del data['shop']
            await callback.answer("❌ Выбор магазина отменен")
        else:
            await callback.answer("⚠️ Этот магазин не был выбран")
    
    # Обновляем меню управления магазином
    # await manage_shop_callback(callback, state)
    session = sessionmaker()(bind=engine)
    try:
        shop = session.query(Shop).filter(Shop.id == shop_id).first()
        
        if not shop:
            await callback.answer("❌ Магазин не найден", show_alert=True)
            return
        
        # Проверяем, выбран ли этот магазин
        is_selected = False
        async with state.proxy() as data:
            current_shop = data.get('shop', None)
            is_selected = current_shop and current_shop['id'] == shop.id
        
        text = (
            f"⚙️ <b>Управление магазином</b>\n\n"
            f"<b>Название:</b> {shop.name or 'Не указано'}\n"
            f"<b>ID магазина:</b> {shop.id}\n"
            f"<b>Статус:</b> {'✅ Выбран' if is_selected else '❌ Не выбран'}\n"
        )
        
        keyboard = shop_management_keyboard(shop_id, is_selected)
        await callback.message.edit_text(text, reply_markup=keyboard)
    finally:
        session.close()

def register_shops_handlers(dp):
    dp.register_callback_query_handler(shops_menu_callback, text="shops_menu", state="*")
    dp.register_callback_query_handler(add_shop_callback, text="add_shop", state="*")
    dp.register_message_handler(process_api_token, state="waiting_for_api_token")
    
    # Обработчики для управления магазинами
    dp.register_callback_query_handler(manage_shops_list_callback, text="manage_shops", state="*")
    dp.register_callback_query_handler(manage_shop_callback, lambda c: c.data.startswith('manage_') and c.data != 'manage_shops', state="*")
    dp.register_callback_query_handler(change_api_callback, lambda c: c.data.startswith('change_api_'), state="*")
    dp.register_message_handler(process_new_api_token, state="waiting_for_new_api_token")
    dp.register_callback_query_handler(delete_shop_callback, lambda c: c.data.startswith('delete_shop_'), state="*")
    dp.register_callback_query_handler(confirm_delete_callback, lambda c: c.data.startswith('confirm_delete_'), state="*")
    dp.register_callback_query_handler(select_shop_callback, lambda c: c.data.startswith('select_shop_'), state="*")
    dp.register_callback_query_handler(unselect_shop_callback, lambda c: c.data.startswith('unselect_shop_'), state="*")
