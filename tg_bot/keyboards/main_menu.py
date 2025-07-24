from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

def main_menu_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("Фин Отчёт", callback_data="an_1"),
        InlineKeyboardButton("отчёт по товарам .xl", callback_data="start_analytics_report"),
        InlineKeyboardButton("Настройка магазина", callback_data="settings"),
        InlineKeyboardButton("Подписка", callback_data="subscription"),
        InlineKeyboardButton("PNL", callback_data="pnl"),
        InlineKeyboardButton("Поддержать проект", callback_data="donate_project")
        #InlineKeyboardButton("Поддержка", callback_data="support")
    )
    return keyboard

def menu_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("Финансы", callback_data="finances"),
        # InlineKeyboardButton('Симулятор "А что если?"', callback_data="what_if_simulator"),
        InlineKeyboardButton("Выйти", callback_data="main_menu")
    )
    return keyboard
