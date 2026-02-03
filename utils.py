# -*- coding: utf-8 -*-
import random
import time
from datetime import datetime, timedelta, timezone

MOSCOW_TZ = timezone(timedelta(hours=3))


def jitter_sleep() -> None:
    """Короткий случайный sleep для разгрузки VK API."""
    time.sleep(random.uniform(0.10, 0.20))


def normalize_text(s: str) -> str:
    """Приведение текста к нормализованному виду."""
    return (s or "").strip().lower()


def now_ts() -> int:
    return int(time.time())


def now_moscow() -> datetime:
    return datetime.now(MOSCOW_TZ)


def timestamp_to_moscow(ts: int) -> datetime:
    return datetime.fromtimestamp(ts, MOSCOW_TZ)


def format_moscow_time(dt: datetime) -> str:
    return dt.strftime("%H:%M:%S")
