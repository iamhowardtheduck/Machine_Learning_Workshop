"""
business_calendar.py — Shared US Federal holiday + business cycle logic.
Imported by backfill_sdg.py, backfill_apm.py, and backfill_all.py.
"""

from datetime import date, timedelta

# ── US Federal Holidays (static observed dates 2025–2027) ────────────────────
# Observed date (Mon when holiday falls on weekend) is what matters for
# determining whether a workday is a holiday.

def _federal_holidays(year: int) -> set:
    """Return the set of observed US Federal holiday dates for a given year."""
    from datetime import date
    import calendar

    def nth_weekday(year, month, weekday, n):
        """nth occurrence of weekday (0=Mon) in month."""
        first = date(year, month, 1)
        delta = (weekday - first.weekday()) % 7
        return first + timedelta(days=delta + (n - 1) * 7)

    def last_weekday(year, month, weekday):
        """Last occurrence of weekday in month."""
        last = date(year, month, calendar.monthrange(year, month)[1])
        delta = (last.weekday() - weekday) % 7
        return last - timedelta(days=delta)

    def observed(d):
        """Shift to Monday if Sunday, Friday if Saturday."""
        if d.weekday() == 6:   # Sunday → Monday
            return d + timedelta(days=1)
        if d.weekday() == 5:   # Saturday → Friday
            return d - timedelta(days=1)
        return d

    holidays = set()
    holidays.add(observed(date(year, 1,  1)))                    # New Year's Day
    holidays.add(nth_weekday(year, 1, 0, 3))                     # MLK Day (3rd Mon Jan)
    holidays.add(nth_weekday(year, 2, 0, 3))                     # Presidents Day (3rd Mon Feb)
    holidays.add(last_weekday(year, 5, 0))                       # Memorial Day (last Mon May)
    holidays.add(observed(date(year, 6, 19)))                    # Juneteenth
    holidays.add(observed(date(year, 7,  4)))                    # Independence Day
    holidays.add(nth_weekday(year, 9, 0, 1))                     # Labor Day (1st Mon Sep)
    holidays.add(nth_weekday(year, 10, 0, 2))                    # Columbus Day (2nd Mon Oct)
    holidays.add(observed(date(year, 11, 11)))                   # Veterans Day
    holidays.add(nth_weekday(year, 11, 3, 4))                    # Thanksgiving (4th Thu Nov)
    holidays.add(observed(date(year, 12, 25)))                   # Christmas
    return holidays


_HOLIDAY_CACHE: dict = {}

def is_us_federal_holiday(d: date) -> bool:
    year = d.year
    if year not in _HOLIDAY_CACHE:
        _HOLIDAY_CACHE[year] = _federal_holidays(year)
    return d in _HOLIDAY_CACHE[year]


def is_business_day(d: date) -> bool:
    return d.weekday() < 5 and not is_us_federal_holiday(d)


# ── Business-hours diurnal weights ────────────────────────────────────────────
# Two profiles: workday (Mon–Fri, non-holiday) and reduced (weekend/holiday).
# Peak is 1 PM (13:00) as specified.

_WORKDAY_WEIGHTS = [
    0.00, 0.00, 0.00, 0.00, 0.00, 0.02,   # 00-05  overnight / very early
    0.05, 0.15, 0.50, 0.75, 0.88, 0.92,   # 06-11  morning ramp
    0.95, 1.00, 0.97, 0.93, 0.88, 0.75,   # 12-17  1 PM peak, afternoon
    0.45, 0.20, 0.08, 0.03, 0.01, 0.00,   # 18-23  evening wind-down
]

_REDUCED_WEIGHTS = [
    0.00, 0.00, 0.00, 0.00, 0.00, 0.01,   # 00-05
    0.02, 0.08, 0.25, 0.60, 0.85, 0.92,   # 06-11  slower ramp
    0.95, 1.00, 0.96, 0.88, 0.70, 0.45,   # 12-17  1 PM peak (lower overall)
    0.20, 0.08, 0.03, 0.01, 0.00, 0.00,   # 18-23
]

# Volume factors
WORKDAY_FACTOR  = 1.00
REDUCED_FACTOR  = 0.30   # weekends and holidays ≤ 30%


def day_volume_factor(d: date) -> float:
    """Return the volume multiplier for a given date (1.0 workday, 0.30 reduced)."""
    return WORKDAY_FACTOR if is_business_day(d) else REDUCED_FACTOR


def hour_weights_for_day(d: date) -> list:
    """Return the 24-element hour-weight list appropriate for this day."""
    return _WORKDAY_WEIGHTS if is_business_day(d) else _REDUCED_WEIGHTS


def doc_count_for_day(d: date, target_weekday: int) -> int:
    """
    Calculate the document count for a specific day.
    target_weekday is the target for a normal business day.
    Weekend/holiday gets REDUCED_FACTOR × target_weekday, capped at 30%.
    """
    factor = day_volume_factor(d)
    return max(1, round(target_weekday * factor))


def timestamps_for_day(d: date, count: int):
    """
    Generator: yield `count` ISO timestamp strings for date d,
    distributed according to the appropriate diurnal hour-weight profile.
    """
    from datetime import datetime, timezone
    import random

    day_start  = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    weights    = hour_weights_for_day(d)
    total_w    = sum(weights)

    hour_counts = []
    allocated   = 0
    for w in weights:
        n = round(count * w / total_w) if total_w > 0 else 0
        hour_counts.append(n)
        allocated += n
    hour_counts[13] += count - allocated   # adjust at peak hour

    for hour, n in enumerate(hour_counts):
        if n <= 0:
            continue
        h_start = (day_start + timedelta(hours=hour)).timestamp()
        for _ in range(n):
            import random as _r
            sec = _r.uniform(0, 3599)
            ts  = datetime.fromtimestamp(h_start + sec, tz=timezone.utc)
            yield ts.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
