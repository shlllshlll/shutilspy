"""Read-write locks for both synchronous and asynchronous contexts."""

import asyncio
import threading

__all__ = [
    "AsyncRWLock",
    "RWLock",
]


class RWLock:
    """A thread-safe read-write lock allowing concurrent reads or exclusive writes.

    Multiple readers can hold the lock simultaneously, but a writer requires
    exclusive access.
    """

    def __init__(self):
        self._read_ready = threading.Condition(threading.Lock())
        self._readers = 0

    def read(self):
        """Return a context manager for acquiring and releasing the read lock."""
        return self.ReadLock(self)

    def write(self):
        """Return a context manager for acquiring and releasing the write lock."""
        return self.WriteLock(self)

    class ReadLock:
        """Context manager that acquires/releases a read lock."""

        def __init__(self, rwlock):
            self.rwlock = rwlock

        def __enter__(self):
            """Acquire the read lock on entering the context."""
            self.rwlock._acquire_read()

        def __exit__(self, exc_type, exc_val, exc_tb):
            """Release the read lock on exiting the context."""
            self.rwlock._release_read()

    class WriteLock:
        """Context manager that acquires/releases a write lock."""

        def __init__(self, rwlock):
            self.rwlock = rwlock

        def __enter__(self):
            """Acquire the write lock on entering the context."""
            self.rwlock._acquire_write()

        def __exit__(self, exc_type, exc_val, exc_tb):
            """Release the write lock on exiting the context."""
            self.rwlock._release_write()

    def _acquire_read(self):
        with self._read_ready:
            self._readers += 1

    def _release_read(self):
        with self._read_ready:
            self._readers -= 1
            if self._readers == 0:
                self._read_ready.notify_all()

    def _acquire_write(self):
        self._read_ready.acquire()
        while self._readers > 0:
            self._read_ready.wait()

    def _release_write(self):
        self._read_ready.release()


class AsyncRWLock:
    """An async read-write lock allowing concurrent reads or exclusive writes.

    Uses ``asyncio.Condition`` for coroutine-safe synchronization.
    """

    def __init__(self):
        self._read_ready = asyncio.Condition()
        self._readers = 0

    def read(self):
        """Return an async context manager for acquiring and releasing the read lock."""
        return self.ReadLock(self)

    def write(self):
        """Return an async context manager for acquiring and releasing the write lock."""
        return self.WriteLock(self)

    class ReadLock:
        """Async context manager that acquires/releases a read lock."""

        def __init__(self, rwlock):
            self.rwlock = rwlock

        async def __aenter__(self):
            """Acquire the read lock on entering the context."""
            await self.rwlock._acquire_read()

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            """Release the read lock on exiting the context."""
            await self.rwlock._release_read()

    class WriteLock:
        """Async context manager that acquires/releases a write lock."""

        def __init__(self, rwlock):
            self.rwlock = rwlock

        async def __aenter__(self):
            """Acquire the write lock on entering the context."""
            await self.rwlock._acquire_write()

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            """Release the write lock on exiting the context."""
            await self.rwlock._release_write()

    async def _acquire_read(self):
        async with self._read_ready:
            self._readers += 1

    async def _release_read(self):
        async with self._read_ready:
            self._readers -= 1
            if self._readers == 0:
                self._read_ready.notify_all()

    async def _acquire_write(self):
        await self._read_ready.acquire()
        try:
            while self._readers > 0:
                await self._read_ready.wait()
        except:
            self._read_ready.release()
            raise

    async def _release_write(self):
        self._read_ready.release()
