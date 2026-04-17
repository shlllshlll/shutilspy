import asyncio
import threading

from shutils.rwlock import AsyncRWLock, RWLock


class TestRWLock:
    def test_read_lock_allows_concurrent_reads(self):
        rwlock = RWLock()
        results = []

        def reader(idx):
            with rwlock.read():
                results.append(("read", idx))

        threads = [threading.Thread(target=reader, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(results) == 5

    def test_write_lock_exclusive(self):
        rwlock = RWLock()
        counter = {"value": 0}

        def writer():
            with rwlock.write():
                tmp = counter["value"]
                counter["value"] = tmp + 1

        threads = [threading.Thread(target=writer) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert counter["value"] == 10

    def test_read_write_context_managers(self):
        rwlock = RWLock()
        with rwlock.read():
            pass  # should not raise
        with rwlock.write():
            pass  # should not raise


class TestAsyncRWLock:
    async def test_read_lock_allows_concurrent_reads(self):
        rwlock = AsyncRWLock()
        results = []

        async def reader(idx):
            async with rwlock.read():
                results.append(("read", idx))

        await asyncio.gather(*[reader(i) for i in range(5)])
        assert len(results) == 5

    async def test_write_lock_exclusive(self):
        rwlock = AsyncRWLock()
        counter = {"value": 0}

        async def writer():
            async with rwlock.write():
                tmp = counter["value"]
                counter["value"] = tmp + 1

        for _ in range(10):
            await writer()

        assert counter["value"] == 10

    async def test_read_write_context_managers(self):
        rwlock = AsyncRWLock()
        async with rwlock.read():
            pass
        async with rwlock.write():
            pass
