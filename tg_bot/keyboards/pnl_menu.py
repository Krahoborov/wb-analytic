from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

def pnl_period_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("За неделю", callback_data="pnlperiod_week"),
        InlineKeyboardButton("За месяц", callback_data="pnlperiod_month"),
        InlineKeyboardButton("За год", callback_data="pnlperiod_year"),
        InlineKeyboardButton("Назад", callback_data="main_menu")
    )
    return keyboard