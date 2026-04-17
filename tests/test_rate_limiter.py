from shutils.rate_limiter import RateLimiter, RateLimiterDecorator, RateLimitException


class TestRateLimiter:
    def test_allow_within_limit(self):
        limiter = RateLimiter(calls=5, period=1)
        for _ in range(5):
            assert limiter.allow() is True

    def test_allow_exceeds_limit(self):
        limiter = RateLimiter(calls=2, period=1)
        assert limiter.allow() is True
        assert limiter.allow() is True
        assert limiter.allow() is False

    def test_sleep_time_within_limit(self):
        limiter = RateLimiter(calls=5, period=1)
        assert limiter.sleep_time() == 0

    def test_sleep_time_exceeds_limit(self):
        limiter = RateLimiter(calls=1, period=1)
        limiter.allow()
        sleep_time = limiter.sleep_time()
        assert sleep_time > 0

    def test_period_zero_always_allows(self):
        limiter = RateLimiter(calls=5, period=0)
        for _ in range(10):
            assert limiter.allow() is True

    def test_calls_must_be_positive(self):
        import pytest
        with pytest.raises(ValueError):
            RateLimiter(calls=0)

    def test_period_must_be_non_negative(self):
        import pytest
        with pytest.raises(ValueError):
            RateLimiter(calls=1, period=-1)


class TestRateLimitException:
    def test_attributes(self):
        exc = RateLimitException("too many", 0.5)
        assert str(exc) == "too many"
        assert exc.period_remaining == 0.5


class TestRateLimiterDecorator:
    def test_raise_exception(self):
        import pytest

        @RateLimiterDecorator(calls=1, period=1, raise_exception=True)
        def func():
            return "ok"

        assert func() == "ok"
        with pytest.raises(RateLimitException):
            func()

    def test_sleep_and_retry(self):
        @RateLimiterDecorator(calls=1, period=0.1, sleep_and_retry=True, raise_exception=True)
        def func():
            return "ok"

        result1 = func()
        result2 = func()
        assert result1 == "ok"
        assert result2 == "ok"

    def test_no_raise_returns_none(self):
        @RateLimiterDecorator(calls=1, period=1, raise_exception=False)
        def func():
            return "ok"

        assert func() == "ok"
        assert func() is None
