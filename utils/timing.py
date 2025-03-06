import functools
import time


def timing(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        elapsed = end - start
        # Convert elapsed seconds to hours, minutes, and seconds
        hours, rem = divmod(elapsed, 3600)
        minutes, seconds = divmod(rem, 60)
        print(f"{func.__name__} executed in {int(hours)}h {int(minutes)}m {seconds:.2f}s")
        return result

    return wrapper
