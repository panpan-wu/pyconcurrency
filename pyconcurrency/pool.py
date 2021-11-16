"""
def func(i: int) -> int:
    return 2 * i


num_processes = 2
num_workers_per_process = 4
pool = Pool(num_processes, num_workers_per_process)
for result_item in pool.map(func, range(10)):
    if result_item.exception:
        pool.shutdown()
"""
from typing import Any
from typing import Callable
from typing import Generator
from typing import Iterable
from typing import Union
import asyncio
import multiprocessing
import os
import queue
import signal
import threading


class Task:

    def __init__(self, func: Callable, item: Any, index: int):
        self.func = func
        self.item = item
        self.index = index


class ResultItem:

    def __init__(self, result, exception, task):
        self.result = result
        self.exception = exception
        self.task = task


class Pool:

    def __init__(
        self,
        num_processes: int,
        num_workers_per_process: int,
    ):
        self.num_processes = num_processes
        self.num_workers_per_process = num_workers_per_process

        self._shutdown = True

    def _init(
        self,
        process_class: Union["ProcessWithAio", "ProcessWithMultiThreads"],
    ):
        self._task_queue = multiprocessing.Queue(
            2 * self.num_processes * self.num_workers_per_process)
        self._result_queue = multiprocessing.Queue(
            2 * self.num_processes * self.num_workers_per_process)
        self._processes = []
        for _ in range(self.num_processes):
            process = process_class(
                self._task_queue,
                self._result_queue,
                self.num_workers_per_process,
            )
            self._processes.append(process)
            process.start()
        self._shutdown = False

    def map(
        self,
        func: Callable[[Any], Any],
        items: Iterable,
    ) -> Generator[ResultItem, None, None]:
        if asyncio.iscoroutinefunction(func):
            process_class = ProcessWithAio
        else:
            process_class = ProcessWithMultiThreads
        self._init(process_class)
        thread_write = threading.Thread(
            target=self._write_to_task_queue, args=(func, items))
        thread_write.start()
        thread_wait = threading.Thread(target=self._wait)
        thread_wait.start()
        while True:
            result_item = self._result_queue.get()
            if result_item is None:
                break
            yield result_item
        thread_write.join()
        thread_wait.join()

    def shutdown(self):
        if not self._shutdown:
            for process in self._processes:
                os.kill(process.pid, signal.SIGINT)
            self._shutdown = True

    def _write_to_task_queue(self, func: Callable, items: Iterable):
        for i, item in enumerate(items):
            task = Task(func, item, i)
            self._task_queue.put(task)
            if self._shutdown:
                break
        self._task_queue.put(None)

    def _wait(self):
        for process in self._processes:
            process.join()
        self._result_queue.put(None)


class ProcessWithAio(multiprocessing.Process):

    def __init__(
        self,
        task_queue: multiprocessing.Queue,
        result_queue: multiprocessing.Queue,
        num_workers: int,
    ):
        super().__init__()
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.num_workers = num_workers
        self._shutdown = False

    def run(self):
        loop = asyncio.get_event_loop()
        self._init(loop)
        loop.run_until_complete(self._join())

    def _init(self, loop):
        self._init_signals()

        self._task_queue_internal = asyncio.Queue(2 * self.num_workers)
        self._result_queue_internal = asyncio.Queue(2 * self.num_workers)

        self._coroutine_read = loop.create_task(
            self._read_from_task_queue())

        self._coroutine_write = loop.create_task(
            self._write_to_result_queue())

        self._coroutines = []
        for _ in range(self.num_workers):
            coroutine = loop.create_task(self._consume())
            self._coroutines.append(coroutine)

    def _init_signals(self):
        signal.signal(signal.SIGINT, self._handle_quit)

    def _handle_quit(self, sig, frame):
        self._shutdown = True

    async def _join(self):
        await self._coroutine_read
        for coroutine in self._coroutines:
            await coroutine
        await self._result_queue_internal.put(None)
        await self._coroutine_write

    async def _consume(self):
        while True:
            if self._shutdown:
                # 把 task_queue_internal 的任务消耗完，防止队列阻塞。
                while True:
                    task = await self._task_queue_internal.get()
                    if task is None:
                        await self._task_queue_internal.put(None)
                        break
                break

            task = await self._task_queue_internal.get()
            if task is None:
                await self._task_queue_internal.put(None)
                break
            try:
                func = task.func
                item = task.item
                result = await func(item)
                result_item = ResultItem(result, None, task)
            except Exception as e:
                result_item = ResultItem(None, e, task)
            await self._result_queue_internal.put(result_item)

    async def _read_from_task_queue(self):
        while True:
            if self._shutdown:
                await self._task_queue_internal.put(None)
                # 把 task_queue 的任务消耗完，防止队列阻塞。
                while True:
                    task = self.task_queue.get()
                    if task is None:
                        self.task_queue.put(None)
                        break
                break

            task = self.task_queue.get()
            await self._task_queue_internal.put(task)
            if task is None:
                self.task_queue.put(None)
                break

    async def _write_to_result_queue(self):
        while True:
            result_item = await self._result_queue_internal.get()
            if result_item is None:
                break
            self.result_queue.put(result_item)


class ProcessWithMultiThreads(multiprocessing.Process):

    def __init__(
        self,
        task_queue: multiprocessing.Queue,
        result_queue: multiprocessing.Queue,
        num_workers: int,
    ):
        super().__init__()
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.num_workers = num_workers
        self._shutdown = False

    def run(self):
        self._init()
        self._join()

    def _init(self):
        self._init_signals()

        self._task_queue_internal = queue.Queue(2 * self.num_workers)
        self._result_queue_internal = queue.Queue(2 * self.num_workers)

        self._thread_read = threading.Thread(
            target=self._read_from_task_queue)
        self._thread_read.start()

        self._thread_write = threading.Thread(
            target=self._write_to_result_queue)
        self._thread_write.start()

        self._threads = []
        for _ in range(self.num_workers):
            thread = threading.Thread(target=self._consume)
            self._threads.append(thread)
            thread.start()

    def _init_signals(self):
        signal.signal(signal.SIGINT, self._handle_quit)

    def _handle_quit(self, sig, frame):
        self._shutdown = True

    def _join(self):
        self._thread_read.join()
        for thread in self._threads:
            thread.join()
        self._result_queue_internal.put(None)
        self._thread_write.join()

    def _consume(self):
        while True:
            if self._shutdown:
                # 把 task_queue_internal 的任务消耗完，防止队列阻塞。
                while True:
                    task = self._task_queue_internal.get()
                    if task is None:
                        self._task_queue_internal.put(None)
                        break
                break

            task = self._task_queue_internal.get()
            if task is None:
                self._task_queue_internal.put(None)
                break
            try:
                func = task.func
                item = task.item
                result = func(item)
                result_item = ResultItem(result, None, task)
            except Exception as e:
                result_item = ResultItem(None, e, task)
            self._result_queue_internal.put(result_item)

    def _read_from_task_queue(self):
        while True:
            if self._shutdown:
                self._task_queue_internal.put(None)
                # 把 task_queue 的任务消耗完，防止队列阻塞。
                while True:
                    task = self.task_queue.get()
                    if task is None:
                        self.task_queue.put(None)
                        break
                break

            task = self.task_queue.get()
            self._task_queue_internal.put(task)
            if task is None:
                self.task_queue.put(None)
                break

    def _write_to_result_queue(self):
        while True:
            result_item = self._result_queue_internal.get()
            if result_item is None:
                break
            self.result_queue.put(result_item)


async def _async_func(i: int) -> int:
    await asyncio.sleep(1)
    return 2 * i


def _func(i: int) -> int:
    import time
    time.sleep(1)
    return 2 * i


if __name__ == "__main__":
    def test():
        pool = Pool(2, 1)
        print("test aio shutdown")
        for result_item in pool.map(_async_func, range(10)):
            print(
                result_item.result,
                result_item.exception,
                result_item.task.index,
            )
            pool.shutdown()

        print("test thread shutdown")
        for result_item in pool.map(_func, range(10)):
            print(
                result_item.result,
                result_item.exception,
                result_item.task.index,
            )
            pool.shutdown()

        print("test aio")
        for result_item in pool.map(_async_func, range(10)):
            print(
                result_item.result,
                result_item.exception,
                result_item.task.index,
            )

        print("test thread")
        for result_item in pool.map(_func, range(10)):
            print(
                result_item.result,
                result_item.exception,
                result_item.task.index,
            )
    test()
