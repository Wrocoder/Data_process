import sys
import tracemalloc
from functools import wraps
from time import time

from logger import LOGGER

call_count = 0


def count_calls(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        global call_count
        call_count += 1
        LOGGER.info(f"Call {call_count} to {func.__name__}")
        return func(*args, **kwargs)

    return wrapper


def measure_memory_usage(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        tracemalloc.start()

        # Call the original function
        result = func(*args, **kwargs)

        snapshot = tracemalloc.take_snapshot()
        top_stats = snapshot.statistics("lineno")

        # Print the top memory-consuming lines
        LOGGER.info(f"Memory usage of {func.__name__}:")
        for stat in top_stats[:3]:
            LOGGER.info(stat)

        return result

    return wrapper


def timing_and_size(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time()
        result = func(*args, **kwargs)
        end = time()
        LOGGER.info(f'Elapsed time: {end - start}')
        LOGGER.info(f'Size in bytes: {sys.getsizeof(result)}')
        return result

    return wrapper
