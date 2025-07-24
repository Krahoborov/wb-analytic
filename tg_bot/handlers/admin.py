# tg_bot/handlers/admin.py
from aiogram import types
from aiogram.dispatcher import FSMContext
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from datetime import datetime, timedelta
from tg_bot.models import sessionmaker, engine, User
import logging
import asyncio


#ADMIN_IDS = [1924535035, 1441962095, 1275991975, 1275991975]
ADMIN_IDS = [1144068556, 877437439]

logger = logging.getLogger(__name__)

async def admin_command(message: types.Message):
    """Обработчик команды /admin"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("⛔️ У вас нет прав доступа к этой команде")
        return
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("📊 Аналитика", callback_data="admin_analytics"),
        InlineKeyboardButton("📢 Сделать рассылку", callback_data="admin_broadcast"),
        InlineKeyboardButton("🔙 Выход", callback_data="main_menu")
    )
    
    await message.answer("👑 <b>Админ-панель</b>", reply_markup=keyboard)

async def admin_analytics_callback(callback: types.CallbackQuery):
    """Аналитика для администратора"""
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("⛔️ Доступ запрещен", show_alert=True)
        return
    
    session = sessionmaker(bind=engine)()
    
    try:
        # Получаем текущую дату и даты для анализа
        now = datetime.now()
        week_ago = now - timedelta(days=7)
        month_ago = now - timedelta(days=30)
        current_month_start = datetime(now.year, now.month, 1)
        prev_month_start = (current_month_start - timedelta(days=1)).replace(day=1)
        
        # 1. Всего зарегистрированных профилей
        total_users = session.query(User).count()
        
        # 2. Активные подписки
        active_subscriptions = session.query(User).filter(
            User.subscription_end > now
        ).count()
        
        # 3. Конверсия
        conversion = (active_subscriptions / total_users * 100) if total_users > 0 else 0
        
        # 4. Новые пользователи
        new_users_week = session.query(User).filter(
            User.created_at >= week_ago
        ).count()
        
        new_users_month = session.query(User).filter(
            User.created_at >= month_ago
        ).count()
        
        # 5-7. Статистика по оплатам
        current_month_payments = session.query(User).filter(
            User.subscription_start >= current_month_start
        ).count()
        
        prev_month_payments = session.query(User).filter(
            User.subscription_start >= prev_month_start,
            User.subscription_start < current_month_start
        ).count()
        
        payment_diff = current_month_payments - prev_month_payments
        payment_diff_percent = (current_month_payments / prev_month_payments * 100) if prev_month_payments > 0 else 0
        
        # Форматируем статистику
        payment_diff_icon = "🟢" if payment_diff > 0 else "🔴"
        payment_percent_icon = "🟢" if payment_diff_percent > 100 else "🔴"
        
        text = (
            "📊 <b>Аналитика системы</b>\n\n"
            f"1️⃣ <b>Всего профилей:</b> {total_users}\n"
            f"2️⃣ <b>Активных подписок:</b> {active_subscriptions}\n"
            f"3️⃣ <b>Конверсия:</b> {conversion:.2f}%\n"
            f"4️⃣ <b>Новые пользователи:</b>\n"
            f"   - За неделю: {new_users_week}\n"
            f"   - За месяц: {new_users_month}\n\n"
            "💳 <b>Статистика оплат:</b>\n"
            f"5️⃣ Текущий месяц: {current_month_payments}\n"
            f"   Предыдущий месяц: {prev_month_payments}\n"
            f"6️⃣ Приток/отток: {payment_diff_icon} {payment_diff}\n"
            f"7️⃣ Изменение: {payment_percent_icon} {payment_diff_percent:.2f}%\n"
            f"\nПоследнее обновление: {now.strftime('%Y-%m-%d %H:%M:%S')}"
        )
        
        keyboard = InlineKeyboardMarkup()
        keyboard.add(InlineKeyboardButton("🔄 Обновить", callback_data="admin_analytics"))
        keyboard.add(InlineKeyboardButton("🔙 Назад", callback_data="admin_panel"))

        await callback.message.edit_text(text, reply_markup=keyboard)
    
    except Exception as e:
        logger.error(f"Ошибка в админ-аналитике: {e}")
        await callback.message.answer("❌ Произошла ошибка при получении статистики")
    finally:
        session.close()

async def admin_broadcast_callback(callback: types.CallbackQuery, state: FSMContext):
    """Инициализация рассылки"""
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("⛔️ Доступ запрещен", show_alert=True)
        return
    
    await callback.message.edit_text(
        "📢 <b>Рассылка сообщений</b>\n\n"
        "Пришлите сообщение для рассылки (текст, фото, видео или документ).\n"
        "Для отмены нажмите /cancel",
        reply_markup=InlineKeyboardMarkup().add(
            InlineKeyboardButton("🔙 Отмена", callback_data="admin_panel")
        )
    )
    await state.set_state("admin_broadcast")

async def process_broadcast_message(message: types.Message, state: FSMContext):
    """Обработка сообщения для рассылки"""
    if message.from_user.id not in ADMIN_IDS:
        await state.finish()
        return
    
    # Сохраняем сообщение для рассылки
    await state.update_data(
        broadcast_message=message.to_python(),
        broadcast_type=message.content_type
    )
    
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✅ Начать рассылку", callback_data="confirm_broadcast"),
        InlineKeyboardButton("❌ Отменить", callback_data="admin_panel")
    )
    
    await message.answer(
        "📤 <b>Сообщение готово к рассылке</b>\n\n"
        f"Получателей: {await get_total_users()}\n"
        "Начать рассылку?",
        reply_markup=keyboard
    )

async def confirm_broadcast_callback(callback: types.CallbackQuery, state: FSMContext):
    """Подтверждение и запуск рассылки"""
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("⛔️ Доступ запрещен", show_alert=True)
        return
    
    data = await state.get_data()
    await state.finish()
    
    if 'broadcast_message' not in data:
        await callback.message.answer("❌ Ошибка: сообщение не найдено")
        return
    
    total_users = await get_total_users()
    success = 0
    errors = 0
    
    # Отправляем статус
    status_message = await callback.message.answer(
        f"⏳ Начата рассылка для {total_users} пользователей...\n"
        f"✅ Успешно: {success}\n"
        f"❌ Ошибок: {errors}"
    )
    
    # Получаем всех пользователей
    session = sessionmaker(bind=engine)()
    users = session.query(User).all()
    session.close()
    
    # Рассылаем сообщение
    for user in users:
        try:
            msg_data = data['broadcast_message']
            # Создаем сообщение из сохраненных данных
            if data['broadcast_type'] == 'text':
                await callback.bot.send_message(
                    user.telegram_id,
                    msg_data['text'],
                    parse_mode='HTML'
                )
            elif data['broadcast_type'] == 'photo':
                await callback.bot.send_photo(
                    user.telegram_id,
                    photo=msg_data['photo'][-1]['file_id'],
                    caption=msg_data.get('caption', ''),
                    parse_mode='HTML'
                )
            elif data['broadcast_type'] == 'document':
                await callback.bot.send_document(
                    user.telegram_id,
                    document=msg_data['document']['file_id'],
                    caption=msg_data.get('caption', ''),
                    parse_mode='HTML'
                )
            elif data['broadcast_type'] == 'video':
                await callback.bot.send_video(
                    user.telegram_id,
                    video=msg_data['video']['file_id'],
                    caption=msg_data.get('caption', ''),
                    parse_mode='HTML'
                )
            
            success += 1
        except Exception as e:
            logger.error(f"Ошибка рассылки для {user.telegram_id}: {e}")
            errors += 1
        
        # Обновляем статус каждые 10 отправок
        if (success + errors) % 10 == 0:
            await status_message.edit_text(
                f"⏳ Рассылка для {total_users} пользователей...\n"
                f"✅ Успешно: {success}\n"
                f"❌ Ошибок: {errors}"
            )
        # Задержка чтобы не превысить лимиты Telegram
        await asyncio.sleep(0.1)
    
    # Финальный статус
    await status_message.edit_text(
        f"✅ Рассылка завершена!\n"
        f"Всего пользователей: {total_users}\n"
        f"✅ Успешно: {success}\n"
        f"❌ Не доставлено: {errors}"
    )
    
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("🔙 В админ-панель", callback_data="admin_panel"))
    await callback.message.answer("Рассылка завершена", reply_markup=keyboard)

async def admin_panel_callback(callback: types.CallbackQuery):
    """Возврат в админ-панель"""
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("⛔️ Доступ запрещен", show_alert=True)
        return
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("📊 Аналитика", callback_data="admin_analytics"),
        InlineKeyboardButton("📢 Сделать рассылку", callback_data="admin_broadcast"),
        InlineKeyboardButton("🔙 Выход", callback_data="main_menu")
    )
    
    await callback.message.edit_text("👑 <b>Админ-панель</b>", reply_markup=keyboard)

async def get_total_users():
    """Получение общего количества пользователей"""
    session = sessionmaker(bind=engine)()
    try:
        return session.query(User).count()
    finally:
        session.close()

def register_admin_handlers(dp):
    dp.register_message_handler(admin_command, commands=['admin'])
    dp.register_message_handler(admin_command, commands=['admin'], state="*")
    dp.register_callback_query_handler(admin_panel_callback, text="admin_panel")
    dp.register_callback_query_handler(admin_panel_callback, text="admin_panel", state="*")
    dp.register_callback_query_handler(admin_analytics_callback, text="admin_analytics")
    dp.register_callback_query_handler(admin_analytics_callback, text="admin_analytics", state="*")
    dp.register_callback_query_handler(admin_broadcast_callback, text="admin_broadcast", state="*")
    dp.register_message_handler(process_broadcast_message, content_types=types.ContentTypes.ANY, state="admin_broadcast")
    dp.register_callback_query_handler(confirm_broadcast_callback, text="confirm_broadcast", state="*")
