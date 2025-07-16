from aiogram.dispatcher.filters.state import State, StatesGroup

class AnalyticsStates(StatesGroup):
    waiting_for_article = State()
    waiting_for_price_and_cost = State()