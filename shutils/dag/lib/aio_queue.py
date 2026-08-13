"""Mixed sync/async priority queue supporting both threading and asyncio."""

import asyncio
import collections
import contextlib
import heapq
import threading
from asyncio import AbstractEventLoop, Future
from typing import Any, TypeVar

__all__ = [
    "PriorityQueue",
    "Queue",
    "QueueEmpty",
    "QueueFull",
]

T = TypeVar("T")

class QueueEmpty(Exception):  # noqa: N818
    """Raised when non-blocking get() is called on an empty queue."""

    pass

class QueueFull(Exception):  # noqa: N818
    """Raised when non-blocking put() is called on a full queue."""

    pass

class _BaseQueue[T]:
    """Core implementation of a mixed sync/async queue with locking and notification."""

    def __init__(self, maxsize: int = 0) -> None:
        """Initialize the queue with optional max size.

        Args:
            maxsize: Maximum queue size (0 means unlimited).
        """
        self._maxsize = maxsize
        self._loop: AbstractEventLoop | None = None

        # Internal storage (to be initialized by subclasses)
        self._queue: Any = None

        # Threading primitives
        self._mutex = threading.Lock()
        self._not_empty = threading.Condition(self._mutex)
        self._not_full = threading.Condition(self._mutex)

        # Async waiters (Futures)
        self._async_getters: collections.deque[Future] = collections.deque()
        self._async_putters: collections.deque[Future] = collections.deque()

        # Task tracking for join()
        self._unfinished_tasks = 0
        self._all_tasks_done = threading.Condition(self._mutex)
        self._async_all_tasks_done: collections.deque[Future] = collections.deque()

        # Interface facades
        self._sync_interface = _SyncAdapter(self)
        self._async_interface = _AsyncAdapter(self)

        # Initialize storage
        self._init_queue()

    def _init_queue(self):
        raise NotImplementedError

    def _qsize(self) -> int:
        raise NotImplementedError

    def _put(self, item: T) -> None:
        raise NotImplementedError

    def _get(self) -> T:
        raise NotImplementedError

    @property
    def sync_q(self) -> "_SyncAdapter[T]":
        """Synchronous interface for the queue."""
        return self._sync_interface

    @property
    def async_q(self) -> "_AsyncAdapter[T]":
        """Asynchronous interface for the queue."""
        return self._async_interface

    def _get_loop(self) -> AbstractEventLoop:
        """Lazily retrieve the running event loop."""
        if self._loop is None:
            with contextlib.suppress(RuntimeError):
                self._loop = asyncio.get_running_loop()
        return self._loop

    @staticmethod
    def _set_result_unless_done(waiter: Future) -> None:
        """Resolve a waiter unless it was completed after wakeup was scheduled."""
        if not waiter.done():
            waiter.set_result(None)

    def _wakeup_next(self, waiters: collections.deque[Future]) -> None:
        """Wake up the next async waiter safely."""
        while waiters:
            waiter = waiters.popleft()
            if not waiter.done():
                # The waiter can be cancelled before this callback runs.
                loop = waiter.get_loop()
                loop.call_soon_threadsafe(self._set_result_unless_done, waiter)
                break

    def close(self):
        """Close the queue (optional implementation)."""
        pass

    def wait_closed(self):
        pass


class Queue(_BaseQueue[T]):
    """FIFO queue supporting both sync and async operations."""
    def _init_queue(self):
        self._queue = collections.deque()

    def _qsize(self) -> int:
        return len(self._queue)

    def _put(self, item: T) -> None:
        self._queue.append(item)

    def _get(self) -> T:
        return self._queue.popleft()


class PriorityQueue(_BaseQueue[T]):
    """Priority queue supporting both sync and async operations."""
    def _init_queue(self):
        self._queue = []

    def _qsize(self) -> int:
        return len(self._queue)

    def _put(self, item: T) -> None:
        heapq.heappush(self._queue, item)

    def _get(self) -> T:
        return heapq.heappop(self._queue)


class _SyncAdapter[T]:
    """Synchronous (blocking) interface mimicking queue.Queue."""

    def __init__(self, queue: _BaseQueue):
        self._q = queue

    def qsize(self) -> int:
        with self._q._mutex:
            return self._q._qsize()

    def empty(self) -> bool:
        with self._q._mutex:
            return not self._q._qsize()

    def full(self) -> bool:
        with self._q._mutex:
            return 0 < self._q._maxsize <= self._q._qsize()

    def put(self, item: T, block: bool = True, timeout: float | None = None) -> None:
        with self._q._mutex:
            if self._q._maxsize > 0:
                if not block:
                    if self._q._qsize() >= self._q._maxsize:
                        raise QueueFull
                elif timeout is None:
                    while self._q._qsize() >= self._q._maxsize:
                        self._q._not_full.wait()
                elif timeout < 0:
                    raise ValueError("'timeout' must be a non-negative number")
                else:
                    endtime = threading.get_time() + timeout
                    while self._q._qsize() >= self._q._maxsize:
                        remaining = endtime - threading.get_time()
                        if remaining <= 0.0:
                            raise QueueFull
                        self._q._not_full.wait(remaining)

            self._q._put(item)
            self._q._unfinished_tasks += 1

            # Notify async getters first (preference logic can be tweaked)
            if self._q._async_getters:
                self._q._wakeup_next(self._q._async_getters)
            else:
                self._q._not_empty.notify()

    def put_nowait(self, item: T) -> None:
        self.put(item, block=False)

    def get(self, block: bool = True, timeout: float | None = None) -> T:
        with self._q._mutex:
            if not block:
                if not self._q._qsize():
                    raise QueueEmpty
            elif timeout is None:
                while not self._q._qsize():
                    self._q._not_empty.wait()
            elif timeout < 0:
                raise ValueError("'timeout' must be a non-negative number")
            else:
                endtime = threading.get_time() + timeout
                while not self._q._qsize():
                    remaining = endtime - threading.get_time()
                    if remaining <= 0.0:
                        raise QueueEmpty
                    self._q._not_empty.wait(remaining)

            item = self._q._get()

            if self._q._maxsize > 0:
                if self._q._async_putters:
                    self._q._wakeup_next(self._q._async_putters)
                else:
                    self._q._not_full.notify()

            return item

    def get_nowait(self) -> T:
        return self.get(block=False)

    def task_done(self) -> None:
        with self._q._mutex:
            unfinished = self._q._unfinished_tasks - 1
            if unfinished < 0:
                raise ValueError("task_done() called too many times")
            self._q._unfinished_tasks = unfinished
            if unfinished == 0:
                self._q._all_tasks_done.notify_all()
                self._q._wakeup_next(self._q._async_all_tasks_done)

    def join(self) -> None:
        with self._q._mutex:
            while self._q._unfinished_tasks:
                self._q._all_tasks_done.wait()


class _AsyncAdapter[T]:
    """Asynchronous (awaitable) interface mimicking asyncio.Queue."""

    def __init__(self, queue: _BaseQueue):
        self._q = queue

    def qsize(self) -> int:
        return self._q.sync_q.qsize()

    def empty(self) -> bool:
        return self._q.sync_q.empty()

    def full(self) -> bool:
        return self._q.sync_q.full()

    async def put(self, item: T) -> None:
        loop = asyncio.get_running_loop()
        while True:
            with self._q._mutex:
                if self._q._maxsize <= 0 or self._q._qsize() < self._q._maxsize:
                    self._q._put(item)
                    self._q._unfinished_tasks += 1

                    if self._q._async_getters:
                        self._q._wakeup_next(self._q._async_getters)
                    else:
                        self._q._not_empty.notify()
                    return

                # Queue is full, register a waiter
                waiter = loop.create_future()
                self._q._async_putters.append(waiter)

            try:
                await waiter
            except asyncio.CancelledError:
                # If cancelled, try to remove from deque
                with self._q._mutex:
                    if waiter in self._q._async_putters:
                        self._q._async_putters.remove(waiter)
                raise

    def put_nowait(self, item: T) -> None:
        try:
            self._q.sync_q.put_nowait(item)
        except QueueFull:
            raise asyncio.QueueFull from None

    async def get(self) -> T:
        loop = asyncio.get_running_loop()
        while True:
            with self._q._mutex:
                if self._q._qsize() > 0:
                    item = self._q._get()

                    if self._q._maxsize > 0:
                        if self._q._async_putters:
                            self._q._wakeup_next(self._q._async_putters)
                        else:
                            self._q._not_full.notify()
                    return item

                # Queue is empty, register a waiter
                waiter = loop.create_future()
                self._q._async_getters.append(waiter)

            try:
                await waiter
            except asyncio.CancelledError:
                with self._q._mutex:
                    if waiter in self._q._async_getters:
                        self._q._async_getters.remove(waiter)
                raise

    def get_nowait(self) -> T:
        try:
            return self._q.sync_q.get_nowait()
        except QueueEmpty:
            raise asyncio.QueueEmpty from None

    def task_done(self) -> None:
        self._q.sync_q.task_done()

    async def join(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            with self._q._mutex:
                if self._q._unfinished_tasks == 0:
                    return
                waiter = loop.create_future()
                self._q._async_all_tasks_done.append(waiter)

            try:
                await waiter
            except asyncio.CancelledError:
                with self._q._mutex:
                    if waiter in self._q._async_all_tasks_done:
                        self._q._async_all_tasks_done.remove(waiter)
                raise
