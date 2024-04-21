import time
from typing import Any, Tuple, Callable
from functools import wraps

from etl.logger import logger


def backoff(start_sleep_time: float = 0.1,
            factor: int = 2,
            border_sleep_time: int = 10,
            exceptions: Tuple[Exception, ...] = ()) -> Callable[..., Any]:
    def func_wrapper(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        def inner(*args: Any, **kwargs: Any) -> Any:
            n = 1
            while True:
                try:
                    return func(*args, **kwargs)
                except exceptions as e :
                    logger.error(f"Exception has occurred: {e}")
                    time_to_sleep = start_sleep_time * (factor ** n)
                    time.sleep(time_to_sleep if time_to_sleep < border_sleep_time else border_sleep_time)
                    n += 1
        return inner
    return func_wrapper
