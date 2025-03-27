#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
File: rate_limiter.py
Author: shllll(shlll7347@gmail.com)
Modified By: shllll(shlll7347@gmail.com)
Brief: qps limiter
"""

import time
import threading
from functools import wraps
class RateLimiter:
    def __init__(self, calls: int, period: int = 1):
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
        return self.__cal() == 0
    
    def sleep_time(self):
        return self.__cal()

class RateLimitException(Exception):
    '''
    Rate limit exception class.
    '''
    def __init__(self, message, period_remaining):
        '''
        Custom exception raise when the number of function invocations exceeds
        that imposed by a rate limit. Additionally the exception is aware of
        the remaining time period after which the rate limit is reset.

        :param string message: Custom exception message.
        :param float period_remaining: The time remaining until the rate limit is reset.
        '''
        super().__init__(message)
        self.period_remaining = period_remaining
    
class RateLimiterDecorator:
    def __init__(self, calls: int, period: int = 1, sleep_and_retry: bool = False, raise_exception: bool = True):
        self.rate_limiter = RateLimiter(calls, period)
        self.sleep_and_retry = sleep_and_retry
        self.raise_exception = raise_exception
    
    def __call__(self, func):
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
