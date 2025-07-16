from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

def analytics_menu_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("📊 Оценка доходности", callback_data="profitability_estimation"),
        InlineKeyboardButton("🏆 Топ-5 товаров", callback_data="top5_products"),
        InlineKeyboardButton("🔮 Симулятор «А что если?»", callback_data="what_if_simulator"),
        InlineKeyboardButton("📋 Отчет по товарам .xl", callback_data="product_analytics"),
        InlineKeyboardButton("🔙 Назад", callback_data="main_menu")
    )
    return keyboard

def period_keyboard(type_data):
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("Эта неделя", callback_data=f"anperiod_week_{type_data}"),
        InlineKeyboardButton("Этот месяц", callback_data=f"anperiod_month_{type_data}"),
        InlineKeyboardButton("Этот год", callback_data=f"anperiod_year_{type_data}"),
        InlineKeyboardButton("Выбранный период", callback_data=f"custom_period_{type_data}"),
        InlineKeyboardButton("Назад", callback_data="main_menu")
    )
    return keyboard


def period_keyboard2(type_data):
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("Этот месяц", callback_data=f"anperiod_month_{type_data}"),
        InlineKeyboardButton("Этот год", callback_data=f"anperiod_year_{type_data}"),
        InlineKeyboardButton("Назад", callback_data="finances")
    )
    return keyboard