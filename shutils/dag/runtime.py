#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
File: runtime.py
Author: shlll(shlll7347@gmail.com)
Modified By: shlll(shlll7347@gmail.com)
Brief:
"""

import asyncio
import threading
from typing import AsyncGenerator
from contextlib import asynccontextmanager
from .context import Context, StopContext


class RuntimeCounterMixin:
    def __init__(self):
        self._context_counter: int = 0
        self._async_conter_lock = asyncio.Lock()
        self._sync_conter_lock = threading.Lock()
        self._async_counter = None
        self._sync_counter = None

    @property
    def counter(self) -> int:
        return self._context_counter

    @property
    def sync_counter(self) -> "SyncRuntimeCounter":
        if self._sync_counter is None:
            self._sync_counter = SyncRuntimeCounter(self)
        return self._sync_counter

    @property
    def async_counter(self) -> "AsyncRuntimeCounter":
        if self._async_counter is None:
            self._async_counter = AsyncRuntimeCounter(self)
        return self._async_counter


class SyncRuntimeCounter:
    def __init__(self, runtime_counter: RuntimeCounterMixin):
        self.__runtime_counter = runtime_counter

    def increase(self):
        with self.__runtime_counter._sync_conter_lock:
            self.__runtime_counter._context_counter += 1

    def decrease(self):
        with self.__runtime_counter._sync_conter_lock:
            self.__runtime_counter._context_counter -= 1


class AsyncRuntimeCounter:
    def __init__(self, runtime_counter: RuntimeCounterMixin):
        self.__runtime_counter = runtime_counter

    async def increase(self):
        async with self.__runtime_counter._async_conter_lock:
            with self.__runtime_counter._sync_conter_lock:
                self.__runtime_counter._context_counter += 1

    async def decrease(self):
        async with self.__runtime_counter._async_conter_lock:
            with self.__runtime_counter._sync_conter_lock:
                self.__runtime_counter._context_counter -= 1


class Runtime(RuntimeCounterMixin):
    def __init__(self):
        RuntimeCounterMixin.__init__(self)
