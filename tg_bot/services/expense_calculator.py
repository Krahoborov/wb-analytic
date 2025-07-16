from datetime import timedelta

def calculate_regular_expenses(expenses, days):
    """
    Рассчитывает регулярные расходы за указанное количество дней
    :param expenses: список регулярных расходов
    :param days: количество дней в периоде
    :return: сумма регулярных расходов за период
    """
    total = 0
    for expense in expenses:
        if expense.frequency == "daily":
            total += expense.amount * days
        elif expense.frequency == "weekly":
            total += expense.amount * (days / 7)
        elif expense.frequency == "monthly":
            total += expense.amount * (days / 30)
    return total