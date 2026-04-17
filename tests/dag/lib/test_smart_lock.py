import asyncio
import time

from shutils.dag.lib.smart_lock import (
    AdaptiveMetrics,
    LockStrategy,
    SmartLock,
    SmartLockConfig,
    SmartRWLock,
    is_async_callable,
)


class TestIsAsyncCallable:
    def test_sync_function(self):
        def sync_func():
            pass
        assert is_async_callable(sync_func) is False

    def test_async_function(self):
        async def async_func():
            pass
        assert is_async_callable(async_func) is True

    def test_partial_async(self):
        from functools import partial
        async def async_func(x):
            pass
        p = partial(async_func, x=1)
        assert is_async_callable(p) is True


class TestAdaptiveMetrics:
    def test_record_and_get(self):
        config = SmartLockConfig(enable_sampling=False)
        metrics = AdaptiveMetrics(config)
        metrics.record(0.001)
        metrics.record(0.002)
        val = metrics.get_metric_value()
        assert val > 0

    def test_empty_returns_zero(self):
        config = SmartLockConfig()
        metrics = AdaptiveMetrics(config)
        assert metrics.get_metric_value() == 0.0

    def test_suggest_strategy_warmup(self):
        config = SmartLockConfig(warmup_count=100, enable_sampling=False)
        metrics = AdaptiveMetrics(config)
        # Not enough samples, should return default
        for _ in range(5):
            metrics.record(0.001)
        strategy = metrics.suggest_strategy()
        assert strategy == config.default_strategy

    def test_suggest_strategy_direct(self):
        config = SmartLockConfig(
            warmup_count=5,
            enable_sampling=False,
            default_strategy=LockStrategy.AUTO,
        )
        metrics = AdaptiveMetrics(config)
        for _ in range(10):
            metrics.record(0.0001)  # Very fast
        # Need to wait for calc_interval
        time.sleep(0.6)
        strategy = metrics.suggest_strategy()
        assert strategy == LockStrategy.DIRECT


class TestSmartLock:
    def test_sync_run(self):
        lock = SmartLock()
        result = lock.sync_run(lambda: 42)
        assert result == 42

    async def test_async_run_sync_func(self):
        lock = SmartLock()
        result = await lock.async_run(lambda: 42)
        assert result == 42

    async def test_async_run_async_func(self):
        lock = SmartLock()
        async def async_func():
            return 42
        result = await lock.async_run(async_func)
        assert result == 42

    async def test_async_run_force_strategy(self):
        lock = SmartLock()
        result = await lock.async_run(lambda: 42, force_strategy=LockStrategy.DIRECT)
        assert result == 42

    def test_sync_lock_context(self):
        lock = SmartLock()
        with lock.sync_lock():
            pass  # Should not raise

    async def test_async_lock_context(self):
        lock = SmartLock()
        async with lock.async_lock():
            pass  # Should not raise


class TestSmartRWLock:
    def test_read_sync_run(self):
        lock = SmartRWLock()
        result = lock.read_sync_run(lambda: 42)
        assert result == 42

    def test_write_sync_run(self):
        lock = SmartRWLock()
        result = lock.write_sync_run(lambda: 42)
        assert result == 42

    async def test_read_async_run(self):
        lock = SmartRWLock()
        result = await lock.read_async_run(lambda: 42)
        assert result == 42

    async def test_write_async_run(self):
        lock = SmartRWLock()
        result = await lock.write_async_run(lambda: 42)
        assert result == 42

    def test_read_write_sync_contexts(self):
        lock = SmartRWLock()
        with lock.read():
            pass
        with lock.write():
            pass

    async def test_read_write_async_contexts(self):
        lock = SmartRWLock()
        async with lock.async_read():
            pass
        async with lock.async_write():
            pass

    async def test_concurrent_reads(self):
        lock = SmartRWLock()
        results = []

        async def reader(idx):
            result = await lock.read_async_run(lambda: idx)
            results.append(result)

        await asyncio.gather(*[reader(i) for i in range(5)])
        assert len(results) == 5
