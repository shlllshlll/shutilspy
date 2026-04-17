from shutils.dag.runtime import Runtime


class TestRuntime:
    def test_init(self):
        runtime = Runtime()
        assert runtime.counter == 0

    def test_sync_counter(self):
        runtime = Runtime()
        runtime.sync_counter.increase()
        assert runtime.counter == 1
        runtime.sync_counter.increase()
        assert runtime.counter == 2
        runtime.sync_counter.decrease()
        assert runtime.counter == 1

    async def test_async_counter(self):
        runtime = Runtime()
        await runtime.async_counter.increase()
        assert runtime.counter == 1
        await runtime.async_counter.increase()
        assert runtime.counter == 2
        await runtime.async_counter.decrease()
        assert runtime.counter == 1

    def test_sync_counter_thread_safety(self):
        import threading
        runtime = Runtime()

        def increase_many():
            for _ in range(100):
                runtime.sync_counter.increase()

        threads = [threading.Thread(target=increase_many) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert runtime.counter == 500
