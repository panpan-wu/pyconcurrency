import asyncio
import time

from pyconcurrency import Pool


def func(i: int) -> int:
    time.sleep(1)
    return i * 2


async def async_func(i: int) -> int:
    await asyncio.sleep(1)
    return i * 2


if __name__ == "__main__":
    num_processes = 2
    num_workers_per_process = 4
    pool = Pool(num_processes, num_workers_per_process)
    print("use thread")
    for result_item in pool.map(func, range(4)):
        print(result_item.result, result_item.exception)
    print("use asyncio")
    for result_item in pool.map(async_func, range(4)):
        print(result_item.result, result_item.exception)
