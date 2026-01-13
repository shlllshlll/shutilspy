#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
File: smart_lock.py
Author: shlll(shlll7347@gmail.com)
Modified By: shlll(shlll7347@gmail.com)
Brief:
"""

import threading
import asyncio
import time
import statistics
from enum import Enum
from typing import Callable, Any, TypeVar, Awaitable
from collections import deque
from concurrent.futures import ThreadPoolExecutor

T = TypeVar("T")

class LockStrategy(Enum):
    AUTO = "auto"           # 根据历史数据自动选择
    DIRECT = "direct"       # [Sync/Async] 直接抢占 threading.Lock (Loop中阻塞)
    ASYNC_WAIT = "wait"     # [Async] 使用 asyncio 等待 threading.Lock (Loop中非阻塞等待)
    EXECUTOR = "executor"   # [Async] 扔到线程池运行 (彻底防止 Loop 阻塞)

class AdaptiveMetrics:
    """维护执行时间的统计窗口"""
    def __init__(self, window_size=100):
        self.history = deque(maxlen=window_size)
        self.lock = threading.Lock()
        # 阈值配置 (单位: 秒)
        self.direct_threshold = 0.001  # 1ms 以内直接阻塞 Loop
        self.executor_threshold = 0.01 # 10ms 以上扔进线程池

    def record(self, duration: float):
        with self.lock:
            self.history.append(duration)

    def suggest_async_strategy(self) -> LockStrategy:
        """根据 P90 耗时给出策略建议"""
        with self.lock:
            if not self.history:
                return LockStrategy.DIRECT # 默认激进策略

            # 简单的平均值，生产环境建议用 P90
            avg_duration = statistics.mean(self.history)

            if avg_duration < self.direct_threshold:
                return LockStrategy.DIRECT
            elif avg_duration > self.executor_threshold:
                return LockStrategy.EXECUTOR
            else:
                return LockStrategy.ASYNC_WAIT

# --------------------------------------------------------------------------------
# 1. 智能互斥锁 (SmartMutex)
# --------------------------------------------------------------------------------

class SmartMutex:
    def __init__(self, executor: ThreadPoolExecutor = None):
        self._t_lock = threading.Lock() # 底层物理锁
        self._metrics = AdaptiveMetrics()
        self._executor = executor or ThreadPoolExecutor(max_workers=4)

        # 强制策略覆盖
        self.force_strategy: LockStrategy = LockStrategy.AUTO

    # --- 核心逻辑: 执行包装器 ---

    def sync_run(self, func: Callable[..., T], *args, **kwargs) -> T:
        """同步接口：总是直接阻塞"""
        start = time.perf_counter()
        with self._t_lock:
            try:
                return func(*args, **kwargs)
            finally:
                self._metrics.record(time.perf_counter() - start)

    async def async_run(self, func: Callable[..., T], *args, **kwargs) -> T:
        """异步接口：自适应调度"""
        strategy = self.force_strategy if self.force_strategy != LockStrategy.AUTO \
                   else self._metrics.suggest_async_strategy()

        # 策略 1: Executor (卸载)
        if strategy == LockStrategy.EXECUTOR:
            loop = asyncio.get_running_loop()
            # 包装一下以记录时间，注意这里记录的是持有锁的耗时，不是调度的耗时
            def _wrapped():
                start = time.perf_counter()
                with self._t_lock:
                    try:
                        return func(*args, **kwargs)
                    finally:
                        self._metrics.record(time.perf_counter() - start)
            return await loop.run_in_executor(self._executor, _wrapped)

        # 策略 2: Direct (直接抢锁，省去 await 开销)
        elif strategy == LockStrategy.DIRECT:
            start = time.perf_counter()
            # 这里会阻塞 Loop，但预测耗时很短，所以由它去
            with self._t_lock:
                try:
                    return func(*args, **kwargs)
                finally:
                    self._metrics.record(time.perf_counter() - start)

        # 策略 3: Async Wait (非阻塞等待，原地执行)
        else: # ASYNC_WAIT
            # 快速检查：如果锁空闲，直接拿，避免创建 Future 的开销
            if self._t_lock.acquire(blocking=False):
                try:
                    start = time.perf_counter()
                    return func(*args, **kwargs)
                finally:
                    self._metrics.record(time.perf_counter() - start)
                    self._t_lock.release()

            # 锁被占用，使用 run_in_executor 等待锁释放，而不是等待执行
            # 这是一个技巧：我们只把“等锁”这个动作扔到线程池，拿到锁后回调回 Loop
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._t_lock.acquire)
            try:
                start = time.perf_counter()
                return func(*args, **kwargs)
            finally:
                self._metrics.record(time.perf_counter() - start)
                self._t_lock.release()

    # --- 传统 Context Manager 接口 (不支持 Executor 策略) ---

    def sync_lock(self):
        """返回标准的 threading.Lock 上下文"""
        return self._t_lock

    class _AsyncContextManager:
        def __init__(self, parent: 'SmartMutex'):
            self.p = parent

        async def __aenter__(self):
            # 这里的限制：Context Manager 无法实现 run_in_executor 策略
            # 只能在 DIRECT 和 ASYNC_WAIT 之间选择
            if self.p._t_lock.acquire(blocking=False):
                return
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self.p._t_lock.acquire)

        async def __aexit__(self, exc_type, exc, tb):
            self.p._t_lock.release()

    def async_lock(self):
        return self._AsyncContextManager(self)

# --------------------------------------------------------------------------------
# 2. 智能读写锁 (SmartRWLock)
# --------------------------------------------------------------------------------

class SmartRWLock:
    """
    读优先/写互斥的自适应锁
    注意：读操作通常很快，默认走 DIRECT；写操作根据耗时自适应。
    """
    def __init__(self, executor: ThreadPoolExecutor = None):
        self._executor = executor or ThreadPoolExecutor(max_workers=4)
        self._metrics = AdaptiveMetrics() # 仅用于统计“写”操作

        # 内部状态
        self._state_lock = threading.Lock()
        self._readers = 0
        self._writer_active = False
        self._cond = threading.Condition(self._state_lock)

    # --- 读操作 (假设总是很快，不卸载) ---

    def read_sync(self, func: Callable[..., T], *args, **kwargs) -> T:
        with self._read_guard():
            return func(*args, **kwargs)

    async def read_async(self, func: Callable[..., T], *args, **kwargs) -> T:
        # 读操作通常不建议扔到 executor，除非非常重
        # 这里演示“非阻塞等待”模式
        loop = asyncio.get_running_loop()

        # 尝试非阻塞获取
        if self._try_acquire_read():
            try:
                return func(*args, **kwargs)
            finally:
                self._release_read()

        # 必须等待：将 wait 操作扔到线程池
        await loop.run_in_executor(None, self._blocking_acquire_read)
        try:
            return func(*args, **kwargs)
        finally:
            self._release_read()

    # --- 写操作 (自适应) ---

    def write_sync(self, func: Callable[..., T], *args, **kwargs) -> T:
        start = time.perf_counter()
        with self._write_guard():
            try:
                return func(*args, **kwargs)
            finally:
                self._metrics.record(time.perf_counter() - start)

    async def write_async(self, func: Callable[..., T], *args, **kwargs) -> T:
        strategy = self._metrics.suggest_async_strategy()

        if strategy == LockStrategy.EXECUTOR:
            loop = asyncio.get_running_loop()
            def _wrapped():
                start = time.perf_counter()
                with self._write_guard():
                    try:
                        return func(*args, **kwargs)
                    finally:
                        self._metrics.record(time.perf_counter() - start)
            return await loop.run_in_executor(self._executor, _wrapped)

        else:
            # DIRECT 或 WAIT 模式
            loop = asyncio.get_running_loop()
            if not self._try_acquire_write():
                 await loop.run_in_executor(None, self._blocking_acquire_write)

            try:
                start = time.perf_counter()
                return func(*args, **kwargs)
            finally:
                self._metrics.record(time.perf_counter() - start)
                self._release_write()

    # --- 底层锁语意 helper ---

    def _try_acquire_read(self):
        with self._state_lock:
            if not self._writer_active:
                self._readers += 1
                return True
            return False

    def _blocking_acquire_read(self):
        with self._cond:
            while self._writer_active:
                self._cond.wait()
            self._readers += 1

    def _release_read(self):
        with self._cond:
            self._readers -= 1
            if self._readers == 0:
                self._cond.notify_all()

    def _try_acquire_write(self):
        with self._state_lock:
            if not self._writer_active and self._readers == 0:
                self._writer_active = True
                return True
            return False

    def _blocking_acquire_write(self):
        with self._cond:
            while self._writer_active or self._readers > 0:
                self._cond.wait()
            self._writer_active = True

    def _release_write(self):
        with self._cond:
            self._writer_active = False
            self._cond.notify_all()

    # Context Manager Helpers
    @contextmanager
    def _read_guard(self):
        self._blocking_acquire_read()
        try: yield
        finally: self._release_read()

    @contextmanager
    def _write_guard(self):
        self._blocking_acquire_write()
        try: yield
        finally: self._release_write()

# --------------------------------------------------------------------------------
# 测试代码
# --------------------------------------------------------------------------------
from contextlib import contextmanager

async def demo():
    mutex = SmartMutex()

    # 模拟任务
    def fast_task():
        # 极快，应该走 DIRECT 模式
        time.sleep(0.0001)
        return "fast"

    def heavy_task():
        # 慢，应该走 EXECUTOR 模式
        time.sleep(0.05)
        return "heavy"

    print("--- Training Metrics ---")
    # 预热：让 Metrics 学习到 heavy_task 很慢
    for _ in range(5):
        await mutex.async_run(heavy_task)

    print("--- Adaptive Execution ---")

    # 1. 执行耗时任务 -> 系统应检测到历史数据慢 -> 自动 throw 到 executor
    print(f"Running Heavy Task (Strategy: {mutex._metrics.suggest_async_strategy()})")
    start = time.time()
    await mutex.async_run(heavy_task)
    print(f"Heavy done in {time.time() - start:.4f}s (Loop not blocked)")

    # 2. 清除历史，模拟快任务环境
    mutex._metrics.history.clear()
    for _ in range(10): mutex.sync_run(fast_task)

    print(f"Running Fast Task (Strategy: {mutex._metrics.suggest_async_strategy()})")
    # 3. 执行快任务 -> 系统检测到历史数据快 -> 直接在 Loop 中抢锁执行
    start = time.time()
    await mutex.async_run(fast_task)
    print(f"Fast done in {time.time() - start:.4f}s")

if __name__ == "__main__":
    asyncio.run(demo())