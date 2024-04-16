import time

from functools import wraps


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10, exceptions=()):
    """
    Функция для повторного выполнения функции через некоторое время, если возникла ошибка. Использует наивный экспоненциальный рост времени повтора (factor) до граничного времени ожидания (border_sleep_time)
        
    Формула:
        t = start_sleep_time * (factor ^ n), если t < border_sleep_time
        t = border_sleep_time, иначе
    :param start_sleep_time: начальное время ожидания
    :param factor: во сколько раз нужно увеличивать время ожидания на каждой итерации
    :param border_sleep_time: максимальное время ожидания
    :return: результат выполнения функции
    """
    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            n = 1
            while True:
                try:
                    return func(*args, **kwargs)
                except exceptions:
                    time_to_sleep = start_sleep_time * (factor ** n)
                    time.sleep(time_to_sleep if time_to_sleep < border_sleep_time else border_sleep_time)
                    n += 1
        return inner
    return func_wrapper
