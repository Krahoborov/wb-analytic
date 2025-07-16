from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

def shops_menu_keyboard():
    """Клавиатура основного меню магазинов"""
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("➕ Добавить магазин", callback_data="add_shop"),
        InlineKeyboardButton("⚙️ Управление магазинами", callback_data="manage_shops"),
        InlineKeyboardButton("🔙 Назад", callback_data="main_menu")
    )
    return keyboard

def manage_shops_keyboard(shops, current_shop=None):
    """Клавиатура для выбора магазина из списка с отметкой выбранного"""
    keyboard = InlineKeyboardMarkup(row_width=1)
    
    for shop in shops:
        # Добавляем галочку к выбранному магазину
        prefix = "✅ " if current_shop and shop.id == current_shop['id'] else ""
        btn_text = f"{prefix}{shop.name} (ID: {shop.id})" if shop.name else f"{prefix}Магазин {shop.id}"
        keyboard.add(InlineKeyboardButton(btn_text, callback_data=f"manage_{shop.id}"))
    
    keyboard.add(InlineKeyboardButton("🔙 Назад", callback_data="shops_menu"))
    return keyboard

def shop_management_keyboard(shop_id, is_selected=False):
    """Клавиатура для управления конкретным магазином с кнопкой выбора"""
    keyboard = InlineKeyboardMarkup(row_width=1)
    
    # Кнопка выбора/отмены выбора
    if is_selected:
        keyboard.add(InlineKeyboardButton("✅ Выбран (отменить выбор)", callback_data=f"unselect_shop_{shop_id}"))
    else:
        keyboard.add(InlineKeyboardButton("⭐ Выбрать магазин", callback_data=f"select_shop_{shop_id}"))
    
    keyboard.add(
        InlineKeyboardButton(" Изменить API ключ", callback_data=f"change_api_{shop_id}"),
        InlineKeyboardButton(" Удалить магазин", callback_data=f"delete_shop_{shop_id}"),
        InlineKeyboardButton(" К списку магазинов", callback_data="manage_shops")
    )
    return keyboard


def back_to_shops_keyboard():
    """Клавиатура для возврата к списку магазинов"""
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("🔙 К списку магазинов", callback_data="manage_shops"))
    return keyboard