"""Rate limiting utilities with token-bucket and decorator-based APIs."""

import threading
import time
from functools import wraps

__all__ = [
    "RateLimitException",
    "RateLimiter",
    "RateLimiterDecorator",
    "limiter",
]


class RateLimiter:
    """A token-bucket rate limiter that tracks call counts within a time window.

    Attributes:
        calls: Maximum number of calls allowed per period.
        period: Time window in seconds.
    """

    def __init__(self, calls: int, period: int = 1):
        """Initialize the rate limiter.

        Args:
            calls: Maximum number of allowed calls per period. Must be > 0.
            period: Time window in seconds. Must be >= 0.

        Raises:
            ValueError: If ``calls`` is <= 0 or ``period`` is < 0.
        """
        self.calls = calls
        self.period = period
        self.last_refill = self.now()
        self.lock = threading.Lock()
        self.tokens = 0
        if self.calls <= 0:
            raise ValueError("calls must be greater than 0")
        if self.period < 0:
            raise ValueError("period must be greater equal 0")

    @staticmethod
    def now():
        """Return a monotonic timestamp for the current time."""
        if hasattr(time, "monotonic"):
            return time.monotonic()
        else:
            return time.time()

    def __cal(self):
        if self.period == 0:
            return 0
        with self.lock:
            # 计算当前时间窗剩余时间
            current_time = self.now()
            time_since_last_refill = current_time - self.last_refill
            period_remaining = self.period - time_since_last_refill
            # 时间窗重制
            if period_remaining <= 0:
                self.tokens = 0
                self.last_refill = current_time

            self.tokens += 1

            # 判断是否超过限制
            if self.tokens > self.calls:
                return period_remaining

            return 0

    def allow(self):
        """Check whether a call is allowed within the current rate limit.

        Returns:
            True if the call is allowed, False otherwise.
        """
        return self.__cal() == 0

    def sleep_time(self):
        """Return the number of seconds to wait before the next call is allowed.

        Returns:
            Seconds remaining in the current rate-limit window, or 0 if allowed.
        """
        return self.__cal()


class RateLimitException(Exception):  # noqa: N818
    """Exception raised when a rate limit is exceeded.

    Attributes:
        period_remaining: Time remaining until the rate limit resets.
    """

    def __init__(self, message, period_remaining):
        """Initialize the rate limit exception.

        Args:
            message: Exception message string.
            period_remaining: Seconds remaining until the rate limit resets.
        """
        super().__init__(message)
        self.period_remaining = period_remaining


class RateLimiterDecorator:
    """Decorator for rate-limiting synchronous function calls.

    Attributes:
        rate_limiter: The underlying ``RateLimiter`` instance.
        sleep_and_retry: If True, sleep and retry instead of raising an exception.
        raise_exception: If True, raise ``RateLimitException`` when rate-limited.
    """

    def __init__(self, calls: int, period: int = 1, sleep_and_retry: bool = False, raise_exception: bool = True):
        """Initialize the rate limiter decorator.

        Args:
            calls: Maximum calls per period.
            period: Time window in seconds.
            sleep_and_retry: If True, sleep until the limit resets and retry.
            raise_exception: If True and not ``sleep_and_retry``, raise
                ``RateLimitException`` when the limit is exceeded.
        """
        self.rate_limiter = RateLimiter(calls, period)
        self.sleep_and_retry = sleep_and_retry
        self.raise_exception = raise_exception

    def __call__(self, func):
        """Wrap a function with rate-limiting logic.

        Args:
            func: The function to decorate.

        Returns:
            The rate-limited wrapper function.
        """
        @wraps(func)
        def wrapper(*args, **kwargs):
            while True:
                sleep_time = self.rate_limiter.sleep_time()
                if sleep_time == 0:
                    return func(*args, **kwargs)
                if self.sleep_and_retry:
                    time.sleep(sleep_time)
                else:
                    if self.raise_exception:
                        raise RateLimitException("Rate limit exceeded", sleep_time)
                    else:
                        return None
        return wrapper

limiter = RateLimiterDecorator
