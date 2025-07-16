# subscription.py
import logging
import os
import hashlib
import random
import aiohttp
from datetime import datetime, timedelta
from aiogram import types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from tg_bot.models import sessionmaker, engine, User, Payment
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Boolean
from sqlalchemy.orm import relationship
from aiogram.dispatcher.filters import Text
from aiogram.dispatcher.filters.state import StatesGroup, State
from aiogram.dispatcher import FSMContext
# tg_bot/middlewares/activity_middleware.py

from aiogram.dispatcher.middlewares import BaseMiddleware
from tg_bot.models import sessionmaker, engine, User

class ActivityMiddleware(BaseMiddleware):
    async def on_pre_process_message(self, message: types.Message, data: dict):
        session = sessionmaker(bind=engine)()
        try:
            user_id = message.from_user.id
            user = session.query(User).filter(User.telegram_id == user_id).first()
            if user:
                print("ActivityMiddleware: on_pre_process_message")
                user.last_active = datetime.utcnow()
                session.commit()
        except Exception as e:
            print(f"[ActivityMiddleware] Error updating last_active: {e}")
        finally:
            session.close()

    async def on_pre_process_callback_query(self, callback_query: types.CallbackQuery, data: dict):
        session = sessionmaker(bind=engine)()
        try:
            user_id = callback_query.from_user.id
            user = session.query(User).filter(User.telegram_id == user_id).first()
            if user:
                print("ActivityMiddleware: on_pre_process_callback_query")
                user.last_active = datetime.utcnow()
                session.commit()
        except Exception as e:
            print(f"[ActivityMiddleware] Error updating last_active: {e}")
        finally:
            session.close()



logger = logging.getLogger(__name__)

# Настройки Tinkoff Kassa (должны быть в .env)
TINKOFF_TERMINAL_KEY = os.getenv("TINKOFF_TERMINAL_KEY")
TINKOFF_PASSWORD = os.getenv("TINKOFF_PASSWORD")
TINKOFF_API_URL = "https://securepay.tinkoff.ru/v2"

# Статусы платежей Tinkoff
STATUS_DESCRIPTION = {
    'NEW': 'Создан',
    'FORM_SHOWED': 'Форма оплаты открыта',
    'DEADLINE_EXPIRED': 'Просрочен',
    'CANCELED': 'Отменен',
    'PREAUTHORIZING': 'Проверка',
    'AUTHORIZING': 'Авторизация',
    'AUTHORIZED': 'Авторизован',
    'REJECTED': 'Отклонен',
    'CONFIRMED': 'Подтвержден',
    'REFUNDED': 'Возвращен'
}


# Генерация токена для Tinkoff API
def generate_token(data, password):
    params = []
    for key, value in data.items():
        if not isinstance(value, (dict, list)):
            params.append({key: value})
    params.append({"Password": password})
    sorted_params = sorted(params, key=lambda x: list(x.keys())[0])
    concatenated = ''.join(str(list(param.values())[0]) for param in sorted_params)
    return hashlib.sha256(concatenated.encode('utf-8')).hexdigest()


# Проверка активной подписки
def check_subscription(user):
    if not user:
        return False
    return user.subscription_end and user.subscription_end > datetime.now()


async def subscription_callback(callback: types.CallbackQuery):
    session = sessionmaker(bind=engine)()
    try:
        user = session.query(User).filter(User.telegram_id == callback.from_user.id).first()

        if not user:
            await callback.message.answer("❌ Пользователь не найден!")
            return

        if check_subscription(user):
            text = (
                f"🌟 <b>Ваша подписка активна!</b>\n\n"
                f"✅ Окончание подписки: {user.subscription_end.strftime('%d.%m.%Y')}\n"
                f"💎 Доступ ко всем функциям открыт"
            )
            keyboard = InlineKeyboardMarkup()
            keyboard.add(InlineKeyboardButton("🔙 Назад", callback_data="main_menu"))
        else:
            text = (
                "🔒 <b>Подписка ProfitBee Premium</b>\n\n"
                "💡 Получите полный доступ к возможностям бота:\n"
                "• Расчёт P&L и рентабельности\n"
                "• Персональные рекомендации\n"
                "• Расширенная аналитика\n"
                "• Симулятор сценариев\n\n"
                "💰 Стоимость: 990 руб./месяц\n"
                "🛒 Первые 14 дней - бесплатно!"
            )
            keyboard = InlineKeyboardMarkup(row_width=1)
            if not user.is_trial_used:
                keyboard.add(InlineKeyboardButton("🎁 Активировать пробный период", callback_data="activate_trial"))
            keyboard.add(
                InlineKeyboardButton("💳 Купить подписку", callback_data="buy_subscription"),
                InlineKeyboardButton("🔙 Назад", callback_data="main_menu")
            )

        await callback.message.answer(text, reply_markup=keyboard)

    except Exception as e:
        logger.error(f"Subscription error: {e}")
        await callback.message.answer("⚠️ Ошибка загрузки данных. Попробуйте позже.")
    finally:
        session.close()


async def activate_trial_callback(callback: types.CallbackQuery):
    session = sessionmaker(bind=engine)()
    try:
        user = session.query(User).filter(User.telegram_id == callback.from_user.id).first()

        if not user:
            await callback.answer("❌ Пользователь не найден!", show_alert=True)
            return

        if user.is_trial_used:
            await callback.answer("❌ Вы уже использовали пробный период", show_alert=True)
            return


        user.subscription_start = datetime.now()
        user.subscription_end = datetime.now() + timedelta(days=14)
        user.is_trial_used = True
        session.commit()

        text = (
            "🎉 <b>Пробный период активирован!</b>\n\n"
            f"Теперь у вас есть 14 дней бесплатного доступа ко всем функциям бота.\n"
            f"Окончание подписки: {user.subscription_end.strftime('%d.%m.%Y')}"
        )
        keyboard = InlineKeyboardMarkup()
        keyboard.add(InlineKeyboardButton("🚀 Начать использовать", callback_data="main_menu"))
        await callback.message.edit_text(text, reply_markup=keyboard)

    except Exception as e:
        logger.error(f"Trial activation error: {e}")
        await callback.answer("⚠️ Ошибка активации пробного периода", show_alert=True)
    finally:
        session.close()


async def buy_subscription_callback(callback: types.CallbackQuery):
    session = sessionmaker(bind=engine)()
    try:
        user = session.query(User).filter(User.telegram_id == callback.from_user.id).first()
        if not user:
            await callback.answer("❌ Пользователь не найден!", show_alert=True)
            return

        # Создаем платеж в Tinkoff
        amount = 99000  # 499 рублей в копейках
        order_id = str(random.randint(100000, 1000000000))
        description = "Подписка JustProfit Premium на 1 месяц"

        token_data = {
            "TerminalKey": TINKOFF_TERMINAL_KEY,
            "Amount": amount,
            "OrderId": order_id,
            "Description": description
        }
        token = generate_token(token_data, TINKOFF_PASSWORD)

        async with aiohttp.ClientSession() as http_session:
            response = await http_session.post(
                f"{TINKOFF_API_URL}/Init",
                json={
                    "TerminalKey": TINKOFF_TERMINAL_KEY,
                    "Amount": amount,
                    "OrderId": order_id,
                    "Description": description,
                    "Token": token
                },
                headers={"Content-Type": "application/json"}
            )
            data = await response.json()

        if not data.get('Success', False):
            error = data.get('Message', 'Неизвестная ошибка')
            await callback.answer(f"❌ Ошибка: {error}", show_alert=True)
            return

        # Сохраняем платеж в БД
        payment = Payment(
            account_id=user.id,
            amount=amount,
            payment_id=data['PaymentId'],
            status=data['Status']
        )
        session.add(payment)
        session.commit()

        # Формируем интерфейс оплаты
        keyboard = InlineKeyboardMarkup(row_width=1)
        keyboard.add(
            InlineKeyboardButton("💳 Оплатить онлайн", url=data['PaymentURL']),
            InlineKeyboardButton("🔄 Проверить оплату", callback_data=f"check_payment:{payment.id}"),
            InlineKeyboardButton("🔙 Назад", callback_data="subscription")
        )

        text = (
            '💳 <b>Оформление подписки</b>\n\n'
            'Для оплаты подписки нажмите кнопку "Оплатить онлайн".\n'
            'После успешной оплаты нажмите «Проверить оплату».\n\n'
            '💰 Стоимость: 499 руб./месяц'
        )
        await callback.message.edit_text(text, reply_markup=keyboard)

    except Exception as e:
        logger.error(f"Payment creation error: {e}")
        await callback.answer("⚠️ Ошибка создания платежа", show_alert=True)
    finally:
        session.close()


async def check_payment_callback(callback: types.CallbackQuery):
    payment_id = int(callback.data.split(':')[1])
    session = sessionmaker(bind=engine)()
    try:
        payment = session.query(Payment).get(payment_id)
        if not payment:
            await callback.answer("❌ Платеж не найден", show_alert=True)
            return

        # Проверяем статус в Tinkoff
        token_data = {
            "TerminalKey": TINKOFF_TERMINAL_KEY,
            "PaymentId": payment.payment_id
        }
        token = generate_token(token_data, TINKOFF_PASSWORD)

        async with aiohttp.ClientSession() as http_session:
            response = await http_session.post(
                f"{TINKOFF_API_URL}/GetState",
                json={
                    "TerminalKey": TINKOFF_TERMINAL_KEY,
                    "PaymentId": payment.payment_id,
                    "Token": token
                },
                headers={"Content-Type": "application/json"}
            )
            data = await response.json()

        if not data.get('Success', False):
            await callback.answer("❌ Ошибка проверки статуса", show_alert=True)
            return

        new_status = data['Status']
        payment.status = new_status
        session.commit()

        if new_status == 'CONFIRMED':
            user = session.query(User).filter(User.id == payment.account_id).first()
            # Обновляем подписку пользователя
            if user.subscription_end and user.subscription_end > datetime.now():
                user.subscription_end += timedelta(days=30)
            else:
                user.subscription_end = datetime.now() + timedelta(days=30)
            session.commit()

            text = (
                "🎉 <b>Подписка активирована!</b>\n\n"
                f"Теперь у вас есть полный доступ ко всем функциям бота.\n"
                f"Окончание подписки: {user.subscription_end.strftime('%d.%m.%Y')}"
            )
            keyboard = InlineKeyboardMarkup()
            keyboard.add(InlineKeyboardButton("🚀 Начать использовать", callback_data="main_menu"))
            await callback.message.edit_text(text, reply_markup=keyboard)
        else:
            status_text = STATUS_DESCRIPTION.get(new_status, new_status)
            await callback.answer(f"⌛ Статус платежа: {status_text}", show_alert=True)

    except Exception as e:
        logger.error(f"Payment check error: {e}")
        await callback.answer("❌ Ошибка проверки платежа", show_alert=True)
    finally:
        session.close()

#Донаты
class DonateStates(StatesGroup):
    waiting_for_amount = State()

async def donate_project_callback(callback: types.CallbackQuery):
    await callback.message.answer(
        "Бот бесплатный, но если хотите помочь с развитием — Спасибо! ❤️",
        parse_mode="Markdown"
    )
    await DonateStates.waiting_for_amount.set()

async def process_donation_amount(message: types.Message, state: FSMContext):
    session = sessionmaker(bind=engine)()
    try:
        amount_text = message.text.strip()
        if not amount_text.isdigit() or int(amount_text) < 1:
            await message.reply("❌ Пожалуйста, введите корректную сумму (целое число от 1 и выше).")
            return

        amount = int(amount_text) * 100  # копейки
        user = session.query(User).filter(User.telegram_id == message.from_user.id).first()
        if not user:
            await message.reply("❌ Пользователь не найден!")
            await state.finish()
            return

        order_id = str(random.randint(100000, 1000000000))
        description = f"Пожертвование от {user.telegram_id}"

        token_data = {
            "TerminalKey": TINKOFF_TERMINAL_KEY,
            "Amount": amount,
            "OrderId": order_id,
            "Description": description
        }
        token = generate_token(token_data, TINKOFF_PASSWORD)

        async with aiohttp.ClientSession() as http_session:
            response = await http_session.post(
                f"{TINKOFF_API_URL}/Init",
                json={
                    "TerminalKey": TINKOFF_TERMINAL_KEY,
                    "Amount": amount,
                    "OrderId": order_id,
                    "Description": description,
                    "Token": token
                },
                headers={"Content-Type": "application/json"}
            )
            data = await response.json()

        if not data.get('Success', False):
            error = data.get('Message', 'Неизвестная ошибка')
            await message.reply(f"❌ Ошибка: {error}")
            await state.finish()
            return

        payment = Payment(
            account_id=user.id,
            amount=amount,
            payment_id=data['PaymentId'],
            status=data['Status']
        )
        session.add(payment)
        session.commit()

        keyboard = InlineKeyboardMarkup(row_width=1)
        keyboard.add(
            InlineKeyboardButton("💳 Оплатить онлайн", url=data['PaymentURL']),
            InlineKeyboardButton("🔄 Проверить оплату", callback_data=f"check_payment:{payment.id}"),
            InlineKeyboardButton("🔙 Отмена", callback_data="main_menu")
        )

        await message.answer(
            f"Спасибо за желание поддержать проект! Пожалуйста, оплатите сумму {int(amount/100)} руб.",
            reply_markup=keyboard
        )
        await state.finish()

    except Exception as e:
        logger.error(f"Donation payment creation error: {e}")
        await message.reply("⚠️ Ошибка при создании платежа. Попробуйте позже.")
        await state.finish()
    finally:
        session.close()


def register_subscription_handlers(dp):
    dp.register_callback_query_handler(subscription_callback, text="subscription")
    dp.register_callback_query_handler(subscription_callback, text="subscription", state="*")
    dp.register_callback_query_handler(activate_trial_callback, text="activate_trial")
    dp.register_callback_query_handler(activate_trial_callback, text="activate_trial", state="*")
    dp.register_callback_query_handler(buy_subscription_callback, text="buy_subscription")
    dp.register_callback_query_handler(buy_subscription_callback, text="buy_subscription", state="*")
    dp.register_callback_query_handler(check_payment_callback, text="check_payment")
    dp.register_callback_query_handler(check_payment_callback, text="check_payment", state="*")
    dp.register_callback_query_handler(check_payment_callback, lambda c: c.data.startswith('check_payment:'))
    dp.register_callback_query_handler(donate_project_callback, text="donate_project")
    dp.register_message_handler(process_donation_amount, state=DonateStates.waiting_for_amount)