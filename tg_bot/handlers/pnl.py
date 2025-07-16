import logging
import asyncio
from datetime import datetime, timedelta
from aiogram import types
from aiogram.dispatcher import FSMContext
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from tg_bot.models import sessionmaker, engine
from tg_bot.models import Shop, CashedShopData, Advertisement, Penalty
from tg_bot.models import (
    TaxSystemType, TaxSystemSetting,
    ProductCost, RegularExpense, OneTimeExpense
)
from tg_bot.keyboards.pnl_menu import pnl_period_keyboard
from tg_bot.services.wb_api import fetch_full_report
from tg_bot.states.pnl_states import PNLStates
from dateutil.relativedelta import relativedelta

logger = logging.getLogger(__name__)

# Главное меню P&L
async def pnl_callback(callback: types.CallbackQuery, state: FSMContext):
    # Проверяем выбран ли магазин
    async with state.proxy() as data:
        if 'shop' not in data:
            await callback.answer("❌ Сначала выберите магазин", show_alert=True)
            return
    
    keyboard = pnl_period_keyboard()
    await callback.message.answer(
        "📊 <b>Расчёт прибыли и убытков (P&L)</b>\n\n"
        "Выберите период для расчета:",
        reply_markup=keyboard
    )
    await PNLStates.waiting_for_period.set()

# Расчет показателей на основе отчета
async def calculate_metrics_from_report(report_data, shop_id, start_date, end_date):
    session = sessionmaker()(bind=engine)
    try:
        # Основные показатели
        revenue = 0
        logistics = 0
        storage_fee = 0
        commission = 0
        cost_of_goods = 0
        
        # Собираем артикулы для расчета себестоимости
        articles = {}
        for item in report_data:
            # Выручка
            retail_price = item.get('retail_price_withdisc_rub', 0)
            quantity = item.get('quantity', 0)
            revenue += retail_price * quantity
            
            # Логистика
            logistics += item.get('delivery_rub', 0)
            
            # Хранение
            storage_fee += item.get('storage_fee', 0)
            
            # Комиссия WB
            # print(item.get('ppvz_sales_commission'), item.get('ppvz_vw'), item.get('ppvz_vw_nds'))
            commission += item.get('ppvz_sales_commission', 0) + item.get('ppvz_vw', 0) + item.get('ppvz_vw_nds', 0)
            
            # Собираем данные для себестоимости
            article = item.get('nm_id')
            if article:
                if article not in articles:
                    articles[article] = 0
                articles[article] += quantity
        
        # Себестоимость
        for article, quantity in articles.items():
            print(article, quantity)
            product_cost = session.query(ProductCost).filter(
                ProductCost.shop_id == shop_id,
                ProductCost.article == article
            ).first()
            if product_cost:
                cost_of_goods += product_cost.cost * quantity
        
        # Налоговая ставка
        tax_setting = session.query(TaxSystemSetting).filter(
            TaxSystemSetting.shop_id == shop_id
        ).first()
        tax_rate = 0.06 if tax_setting and tax_setting.tax_system == TaxSystemType.USN_6 else 0.0
        tax = revenue * tax_rate
        
        # Регулярные затраты за период
        regular_expenses = 0
        days_in_period = (end_date - start_date).days + 1
        for expense in session.query(RegularExpense).filter(RegularExpense.shop_id == shop_id):
            if expense.frequency == "daily":
                regular_expenses += expense.amount * days_in_period
            elif expense.frequency == "weekly":
                regular_expenses += expense.amount * (days_in_period / 7)
            elif expense.frequency == "monthly":
                regular_expenses += expense.amount * (days_in_period / 30)
        advert = sum(i.amount for i in session.query(Advertisement).filter(Advertisement.shop_id == shop_id).filter(Advertisement.date >= start_date).all())

        # Удержания
        stops = sum(i.sum for i in session.query(Penalty).filter(Penalty.date >= start_date).all())

        # Чистая прибыль
        net_profit = revenue - (commission + logistics + storage_fee + tax + cost_of_goods + regular_expenses + advert + stops)
        
        # Рентабельность
        profitability = (net_profit / revenue) * 100 if revenue > 0 else 0

        # Рекламные затраты


        # Разовые затраты (инвестиционные)
        one_time_expenses = session.query(OneTimeExpense).filter(OneTimeExpense.shop_id == shop_id).all()
        total_one_time = sum(expense.amount for expense in one_time_expenses)
        
        # Срок окупаемости
        payback_period = "не определен"
        if net_profit > 0 and total_one_time > 0:
            months = total_one_time / net_profit
            payback_period = f"{months:.1f} месяцев"
        
        # ROI
        roi = "не определен"
        if total_one_time > 0:
            roi_value = (net_profit / total_one_time) * 100
            roi = f"{roi_value:.1f}%"
            if roi_value > 100:
                roi += " ✅ Поздравляем, вы окупили вложения!"
        
        return {
            "revenue": revenue,
            "commission": commission,
            "logistics": logistics,
            "storage": storage_fee,
            "cost_of_goods": cost_of_goods,
            "tax": tax,
            "regular_expenses": regular_expenses,
            "net_profit": net_profit,
            "profitability": profitability,
            "payback_period": payback_period,
            "roi": roi,
            "advert": advert,
            "stops": stops
        }
    finally:
        session.close()

# Обработка выбора периода
async def select_pnl_period_callback(callback: types.CallbackQuery, state: FSMContext):
    period_type = callback.data.split('_')[1]  # day, week, month, year
    await callback.message.edit_text(text="Подождите около 10 секунд, пока произведем подсчёт данных... (иногда дольше, но не более 2х минут)")
    # Определяем периоды
    now = datetime.utcnow()
    if period_type == "week":
        current_start = now - timedelta(weeks=1)
        current_end = now
        previous_start = now - timedelta(weeks=2)
        previous_end = now - timedelta(weeks=1)
        period_name = "неделю"
    elif period_type == "month":
        current_start = now - relativedelta(months=1)
        current_end = now
        previous_start = now - relativedelta(months=2)
        previous_end = now - relativedelta(months=1)
        period_name = "месяц"
    else:  # year
        current_start = now - relativedelta(years=1)
        current_end = now
        previous_start = now - relativedelta(years=2)
        previous_end = now - relativedelta(years=1)
        period_name = "год"
    
    async with state.proxy() as data:
        shop_id = data['shop']['id']
        shop_name = data['shop']['name'] or f"Магазин {shop_id}"
        api_token = data['shop']['api_token']
    
    # Получаем отчет за текущий период
    current_report = {}
    session = sessionmaker(bind=engine)()
    cashed = session.query(CashedShopData).filter(CashedShopData.shop_id == shop_id).first()
    if period_type == "week":
        current_report = cashed.cashed_week
    elif period_type == "month":
        current_report = cashed.cashed_month
    else:  # year
        current_report = cashed.cashed_year
    if not current_report:
        await callback.answer("❌ Не удалось получить данные за текущий период, подождите около 1-2 минуты и попробуйте снова", show_alert=True)

        return


    previous_report = await fetch_full_report(api_token, previous_start, previous_end)
    await callback.message.edit_text(text="Осталось совсем чуть чуть ...")
    # Рассчитываем показатели
    current_metrics = await calculate_metrics_from_report(
        current_report, shop_id, current_start, current_end
    )
    
    previous_metrics = await calculate_metrics_from_report(
        previous_report or [], shop_id, previous_start, previous_end
    ) if previous_report else None
    
    # Рассчитываем динамику
    revenue_change = current_metrics["revenue"] - (previous_metrics["revenue"] if previous_metrics else 0)
    profit_change = current_metrics["net_profit"] - (previous_metrics["net_profit"] if previous_metrics else 0)
    
    revenue_indicator = "🟢▲" if revenue_change >= 0 else "🔴▼"
    profit_indicator = "🟢▲" if profit_change >= 0 else "🔴▼"
    
    # Форматируем отчет
    text = (
        f"📊 <b>Отчет P&L для {shop_name}</b>\n"
        f"Период: <b>за {period_name}</b>\n\n"
        
        "<u>Основные показатели:</u>\n"
        f"💰 Выручка: {current_metrics['revenue']:.2f} руб. "
        f"({revenue_indicator} {abs(revenue_change):.2f})\n"
        f"📦 Комиссии WB: {current_metrics['commission']:.2f} руб.\n"
        f"🚚 Логистика: {current_metrics['logistics']:.2f} руб.\n"
        f"🏭 Хранение: {current_metrics['storage']:.2f} руб.\n"
        f"🏷️ Себестоимость: {current_metrics['cost_of_goods']:.2f} руб.\n"
        f"🏛️ Налог: {current_metrics['tax']:.2f} руб.\n"
        f"💼 Регулярные затраты: {current_metrics['regular_expenses']:.2f} руб.\n"
        f"💵 Рекламные затраты: {current_metrics['advert']} руб.\n"
        f"💵 Прочие удержания: {current_metrics['stops']} руб.\n"
        f"💵 Чистая прибыль: {current_metrics['net_profit']:.2f} руб. "
        
        f"({profit_indicator} {abs(profit_change):.2f})\n\n"
        
        "<u>Аналитические показатели:</u>\n"
        f"📈 Рентабельность: {current_metrics['profitability']:.1f}%\n"
        f"⏳ Срок окупаемости: {current_metrics['payback_period']}\n"
        f"📊 ROI: {current_metrics['roi']}\n\n"
        
        "<i>Примечание: расчеты основаны на данных WB API</i>"
    )
    
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("🔙 Назад", callback_data="pnl"))
    
    await callback.message.edit_text(text, reply_markup=keyboard)
    # await state.finish()

def register_pnl_handlers(dp):
    dp.register_callback_query_handler(pnl_callback, text="pnl", state="*")
    dp.register_callback_query_handler(
        select_pnl_period_callback, 
        lambda c: c.data.startswith("pnlperiod_"), 
        state=PNLStates.waiting_for_period
    )