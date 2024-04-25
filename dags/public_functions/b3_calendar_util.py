from datetime import timedelta, datetime

from pandas_market_calendars import get_calendar
import pandas_market_calendars as mcal
import pytz

timezone = pytz.timezone("America/Sao_Paulo")

def get_b3_dates(execution_date):
    execution_date = datetime.strptime(execution_date, "%Y-%m-%d")

    b3_calendar = get_calendar("B3")

    is_b3_open = b3_calendar.valid_days(start_date=execution_date, end_date=execution_date).shape[0] > 0

    last_valid_date = find_last_valid_date(b3_calendar, execution_date)
    prev_prev_date = find_last_valid_date(b3_calendar, last_valid_date) if last_valid_date else None

    if last_valid_date is None:
        last_valid_date = "b3_is_closed"
    if prev_prev_date is None:
        prev_prev_date = "b3_is_closed"

    return {
        "d0": execution_date.strftime('%Y-%m-%d') if is_b3_open else "b3_is_closed",
        "d-1": last_valid_date.strftime('%Y-%m-%d'),
        "d-2": prev_prev_date.strftime('%Y-%m-%d')
    }


def find_last_valid_date(calendar, date):
    prev_date = date - timedelta(days=1)
    while prev_date >= date - timedelta(days=30):  # Check the last 30 days for a valid date
        if calendar.valid_days(start_date=prev_date, end_date=prev_date).size > 0:
            return prev_date
        prev_date -= timedelta(days=1)
    return None


def is_b3_open():
    try:
        now = datetime.now(timezone)
        b3_calendar = mcal.get_calendar("B3")
        execution_date = now.date()
        is_b3_open_check = (
            b3_calendar.valid_days(
                start_date=execution_date, end_date=execution_date
            ).size
            > 0
        )
        if is_b3_open_check:
            print(f"B3 is open today: {execution_date}")
            return "b3_is_open"
        else:
            print(f"B3 is closed today: {execution_date}")
            return "b3_is_closed"
    except Exception as e:
        print(f"Error while checking if B3 is open: {e}")
        return "b3_is_closed"