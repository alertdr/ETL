import logging
from datetime import datetime
from functools import wraps
from time import sleep
from typing import Optional


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    """
    Функция для повторного выполнения функции через некоторое время, если возникла ошибка.
    Использует наивный экспоненциальный рост времени повтора (factor) до граничного времени ожидания (border_sleep_time)

    :param start_sleep_time: начальное время повтора
    :param factor: во сколько раз нужно увеличить время ожидания
    :param border_sleep_time: граничное время ожидания
    :return: результат выполнения функции
    """

    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            n = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except BaseException as e:
                    seconds = start_sleep_time * factor ** n
                    if seconds < border_sleep_time:
                        n += 1
                    else:
                        seconds = border_sleep_time
                    logging.error(f'{e}\nRetry after {seconds} seconds')
                    sleep(seconds)

        return inner

    return func_wrapper


def get_format_time(*, time: str) -> str:
    """
    Функция форматирования времени для sql запроса

    :param time: начальное время повтора
    :return: результат форматирования, в случае. если поле отсутствует - возвращается None
    """
    return f'\'{time}\'' if time else None


def latest_modified(*, current: Optional[datetime] = None, obj_time: list) -> datetime:
    if not all(obj_time):
        return current

    if current:
        if current < max(obj_time):
            current = max(obj_time)
    else:
        current = max(obj_time)
    return current


def latest_modified_datetime(*, current: Optional[datetime] = None, obj_time: datetime) -> datetime:
    if not obj_time:
        return current

    if current:
        if current < obj_time:
            current = obj_time
    else:
        current = obj_time
    return current


def format_set(*, data: dict) -> tuple:
    if data:
        return list(data.keys()), [{'id': v, 'name': k} for k, v in data.items()]
    else:
        return None, None
