from aiogram.dispatcher.filters.state import State, StatesGroup

class SettingsStates(StatesGroup):
    menu = State()
    tax_system = State()
    waiting_for_tax_percent = State()
    product_cost = State()
    regular_expenses = State()
    one_time_expenses = State()
    
    # Для загрузки файла себестоимости
    waiting_for_cost_file = State()
    
    # Для регулярных расходов
    waiting_for_regular_amount = State()
    waiting_for_regular_description = State()
    waiting_for_regular_frequency = State()
    
    # Для разовых расходов
    waiting_for_onetime_amount = State()
    waiting_for_onetime_description = State()
    waiting_for_onetime_date = State()