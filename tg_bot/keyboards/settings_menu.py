from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from tg_bot.models import TaxSystemType, RegularExpenseFrequency

def settings_menu_keyboard(id_shop):
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("🔑 Изменить API ключ", callback_data=f"change_api_{id_shop}"),
        InlineKeyboardButton("Налоговая система", callback_data="tax_settings"),
        InlineKeyboardButton("Себестоимость артикулов", callback_data="product_cost"),
        InlineKeyboardButton("Инвестиционные затраты", callback_data="one_time_expenses"),
        InlineKeyboardButton("Регулярные затраты", callback_data="regular_expenses"),
        InlineKeyboardButton("Ежедневные отчёты", callback_data="daily_reports"),
        InlineKeyboardButton("Назад", callback_data="main_menu")
    )
    return keyboard

def tax_system_keyboard(current_tax=None):
    keyboard = InlineKeyboardMarkup(row_width=1)
    
    # Для УСН 6%
    usn6_selected = "✅ " if current_tax == TaxSystemType.USN_6 else ""
    keyboard.add(InlineKeyboardButton(
        f"{usn6_selected}{TaxSystemType.USN_6.value}", 
        callback_data="tax_usn6"
    ))
    
    # Для "Без налога"
    notax_selected = "✅ " if current_tax == TaxSystemType.NO_TAX else ""
    keyboard.add(InlineKeyboardButton(
        f"{notax_selected}{TaxSystemType.NO_TAX.value}", 
        callback_data="tax_notax"
    ))
    
    # Для ввода произвольного процента налога
    custom_selected = "✅ " if current_tax == TaxSystemType.CUSTOM else ""
    keyboard.add(InlineKeyboardButton(
        f"{custom_selected} Введите процент налога", 
        callback_data="tax_custom"
    ))

    keyboard.add(InlineKeyboardButton("🔙 Назад", callback_data="back_to_settings"))
    return keyboard

def regular_expense_frequency_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=1)
    for freq in RegularExpenseFrequency:
        keyboard.add(InlineKeyboardButton(
            'Ежедневные' if freq.value == "daily" else "Еженедельные" if freq.value == "weekly" else "Ежемесячные",
            callback_data=f"frequency_{freq.value}"
        ))
    keyboard.add(InlineKeyboardButton("🔙 Назад", callback_data="back_to_settings"))
    return keyboard