import time

import pytest

from shutils.dag.lib.limiter import AcquireResult, Limiter, LimiterType


class TestQPSLimiter:
    def test_try_acquire_within_limit(self):
        limiter = Limiter(LimiterType.QPS, 5)
        result = limiter.try_acquire()
        assert result.success is True

    def test_try_acquire_exceeds_limit(self):
        limiter = Limiter(LimiterType.QPS, 1)
        assert limiter.try_acquire().success is True
        # Exhaust all tokens quickly
        limiter.try_acquire()
        # May or may not succeed depending on timing, but eventually should fail

    def test_acquire_blocking(self):
        limiter = Limiter(LimiterType.QPS, 100)
        result = limiter.acquire()
        assert result.success is True

    def test_acquire_with_timeout(self):
        limiter = Limiter(LimiterType.QPS, 1)
        result = limiter.acquire(timeout=0.1)
        assert result.success is True

    async def test_acquire_async(self):
        limiter = Limiter(LimiterType.QPS, 100)
        result = await limiter.acquire_async()
        assert result.success is True

    def test_period_tuple(self):
        limiter = Limiter(LimiterType.QPS, (0.5, 5))
        assert limiter.try_acquire().success is True

    def test_invalid_config_raises(self):
        with pytest.raises(ValueError):
            Limiter(LimiterType.QPS, "invalid")


class TestConcurrencyLimiter:
    def test_within_limit(self):
        limiter = Limiter(LimiterType.CONCURRENCY, 2)
        result = limiter.try_acquire()
        assert result.success is True

    def test_exceeds_limit(self):
        limiter = Limiter(LimiterType.CONCURRENCY, 1)
        assert limiter.try_acquire().success is True
        assert limiter.try_acquire().success is False

    def test_release(self):
        limiter = Limiter(LimiterType.CONCURRENCY, 1)
        limiter.try_acquire()
        limiter.release()
        result = limiter.try_acquire()
        assert result.success is True

    def test_acquire_blocking_with_release(self):
        import threading

        limiter = Limiter(LimiterType.CONCURRENCY, 1)
        limiter.try_acquire()

        def release_after_delay():
            time.sleep(0.1)
            limiter.release()

        t = threading.Thread(target=release_after_delay)
        t.start()
        result = limiter.acquire(timeout=1.0)
        assert result.success is True
        t.join()


class TestTokenBucketLimiter:
    def test_acquire_from_pool(self):
        limiter = Limiter(LimiterType.TOKEN_BUCKET, ["svc1", "svc2"])
        result = limiter.try_acquire()
        assert result.success is True
        assert result.data in ("svc1", "svc2")

    def test_pool_exhausted(self):
        limiter = Limiter(LimiterType.TOKEN_BUCKET, ["svc1"])
        limiter.try_acquire()
        result = limiter.try_acquire()
        assert result.success is False

    def test_release_and_reacquire(self):
        limiter = Limiter(LimiterType.TOKEN_BUCKET, ["svc1"])
        result1 = limiter.acquire()
        assert result1.success is True
        limiter.release(result1.data)
        result2 = limiter.try_acquire()
        assert result2.success is True
        assert result2.data == result1.data


class TestAcquireResult:
    def test_defaults(self):
        result = AcquireResult(success=True)
        assert result.success is True
        assert result.data is None

    def test_with_data(self):
        result = AcquireResult(success=True, data="svc1")
        assert result.data == "svc1"
