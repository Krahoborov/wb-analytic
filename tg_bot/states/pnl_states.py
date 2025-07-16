from aiogram.dispatcher.filters.state import State, StatesGroup

class PNLStates(StatesGroup):
    waiting_for_period = State()