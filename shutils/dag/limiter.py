#!/usr/bin/env python3
# -*- coding:utf-8 -*-

import time
import threading
import queue
import asyncio
from enum import Enum
from dataclasses import dataclass
from typing import overload, Literal
from concurrent.futures import ThreadPoolExecutor

# --- Python 3.12+ Type Alias Syntax ---
type ServiceName = str
type Period = float
type LimitCount = int
type QPSConfig = int | tuple[Period, LimitCount]
type ConcurrencyConfig = int
type TokenBucketConfig = list[ServiceName]

type LimiterConfig = QPSConfig | ConcurrencyConfig | TokenBucketConfig

class LimiterType(Enum):
    QPS = "qps"
    CONCURRENCY = "concurrency"
    TOKEN_BUCKET = "token_bucket"

@dataclass(slots=True)
class AcquireResult:
    """
    限流器获取结果
    """
    success: bool
    data: ServiceName | None = None

class Limiter:
    """
    全能限流器：支持 Sync/Async，支持 阻塞/非阻塞
    """

    @overload
    def __init__(self, limiter_type: Literal[LimiterType.QPS], rate: QPSConfig): ...
    @overload
    def __init__(self, limiter_type: Literal[LimiterType.CONCURRENCY], rate: ConcurrencyConfig): ...
    @overload
    def __init__(self, limiter_type: Literal[LimiterType.TOKEN_BUCKET], rate: TokenBucketConfig): ...

    def __init__(self, limiter_type: LimiterType, rate: LimiterConfig):
        self.type = limiter_type
        # 升级 Lock 为 Condition，用于并发模式下的等待通知
        self._cond = threading.Condition()

        match (limiter_type, rate):
            case (LimiterType.QPS, int() as limit_per_sec):
                self._init_qps(period=1.0, limit=limit_per_sec)
            case (LimiterType.QPS, (float() as period, int() as limit)):
                self._init_qps(period=period, limit=limit)
            case (LimiterType.CONCURRENCY, int() as max_conn):
                self.max_concurrency = max_conn
                self.current_concurrency = 0
            case (LimiterType.TOKEN_BUCKET, list() as services):
                # queue.Queue 内部自带锁和Condition，适合处理资源池
                self.service_queue: queue.Queue[str] = queue.Queue()
                for s in services:
                    self.service_queue.put(s)
            case _:
                raise ValueError(f"Invalid configuration for {limiter_type}: {rate}")

    def _init_qps(self, period: float, limit: int):
        if period <= 0 or limit < 0:
            raise ValueError("Period must be > 0 and limit >= 0")
        self.tokens_per_sec = limit / period
        self.capacity = float(limit)
        self.tokens = self.capacity
        self.last_update = time.monotonic()

    # ----------------------------------------------------------------
    # 1. 非阻塞接口 (Non-Blocking / Fail-Fast)
    # ----------------------------------------------------------------
    def try_acquire(self) -> AcquireResult:
        """尝试立即获取，失败则立即返回 False"""
        match self.type:
            case LimiterType.QPS:
                return self._try_acquire_qps()
            case LimiterType.CONCURRENCY:
                return self._try_acquire_concurrency()
            case LimiterType.TOKEN_BUCKET:
                return self._try_acquire_token_bucket()
            case _:
                return AcquireResult(False)

    # ----------------------------------------------------------------
    # 2. 同步阻塞接口 (Blocking Sync)
    # ----------------------------------------------------------------
    def acquire(self, timeout: float | None = None) -> AcquireResult:
        """
        阻塞直到获取成功或超时
        """
        start_time = time.monotonic()

        match self.type:
            case LimiterType.QPS:
                while True:
                    # 1. 尝试获取
                    res = self._try_acquire_qps()
                    if res.success:
                        return res

                    # 2. 检查总超时
                    if timeout is not None and (time.monotonic() - start_time) > timeout:
                        return AcquireResult(False)

                    # 3. 计算需要 sleep 的时间 (精确计算，避免忙轮询)
                    wait_time = self._calc_qps_wait_time()
                    # 如果设置了timeout，sleep时间不能超过剩余时间
                    if timeout is not None:
                        remaining = timeout - (time.monotonic() - start_time)
                        wait_time = min(wait_time, remaining)
                        if wait_time <= 0: return AcquireResult(False)

                    time.sleep(wait_time)

            case LimiterType.CONCURRENCY:
                # 使用 Condition 的 wait_for 方法
                def predicate():
                    return self.current_concurrency < self.max_concurrency

                with self._cond:
                    success = self._cond.wait_for(predicate, timeout=timeout)
                    if success:
                        self.current_concurrency += 1
                        return AcquireResult(True)
                    return AcquireResult(False)

            case LimiterType.TOKEN_BUCKET:
                try:
                    data = self.service_queue.get(block=True, timeout=timeout)
                    return AcquireResult(True, data=data)
                except queue.Empty:
                    return AcquireResult(False)

        return AcquireResult(False)

    # ----------------------------------------------------------------
    # 3. 异步阻塞接口 (Blocking Async)
    # ----------------------------------------------------------------
    async def acquire_async(self, timeout: float | None = None) -> AcquireResult:
        """
        Async 友好的阻塞获取
        """
        start_time = time.monotonic()

        match self.type:
            case LimiterType.QPS:
                # QPS 模式特殊优化：使用 asyncio.sleep 避免阻塞 EventLoop
                while True:
                    res = self._try_acquire_qps()
                    if res.success:
                        return res

                    if timeout is not None and (time.monotonic() - start_time) > timeout:
                        return AcquireResult(False)

                    wait_time = self._calc_qps_wait_time()
                    if timeout is not None:
                        remaining = timeout - (time.monotonic() - start_time)
                        wait_time = min(wait_time, remaining)
                        if wait_time <= 0: return AcquireResult(False)

                    await asyncio.sleep(wait_time)

            case LimiterType.CONCURRENCY | LimiterType.TOKEN_BUCKET:
                # 线程锁和 Queue 会阻塞物理线程，必须扔到 Executor 中运行
                # 否则会卡死当前的 Async Event Loop
                loop = asyncio.get_running_loop()
                try:
                    # 复用同步的 acquire 逻辑，但放在线程池中跑
                    return await loop.run_in_executor(None, self.acquire, timeout)
                except Exception:
                    return AcquireResult(False)

        return AcquireResult(False)

    # ----------------------------------------------------------------
    # 释放逻辑
    # ----------------------------------------------------------------
    def release(self, data: ServiceName | None = None) -> None:
        match self.type:
            case LimiterType.CONCURRENCY:
                with self._cond:
                    if self.current_concurrency > 0:
                        self.current_concurrency -= 1
                        # 唤醒一个正在等待的 acquire 线程
                        self._cond.notify()
            case LimiterType.TOKEN_BUCKET:
                if data:
                    self.service_queue.put(data)
            # QPS 不需要释放

    # ----------------------------------------------------------------
    # 内部实现 (Internal Strategies)
    # ----------------------------------------------------------------
    def _calc_qps_wait_time(self) -> float:
        """计算还需要多久才能产生下一个令牌"""
        with self._cond: # 使用 cond 作为 lock
            # 当前亏空多少令牌才能达到 1.0
            needed = 1.0 - self.tokens
            if needed <= 0: return 0.0
            return needed / self.tokens_per_sec

    def _try_acquire_qps(self) -> AcquireResult:
        now = time.monotonic()
        with self._cond:
            delta = now - self.last_update
            self.last_update = now
            new_tokens = self.tokens + (delta * self.tokens_per_sec)
            self.tokens = min(self.capacity, new_tokens)

            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return AcquireResult(True)
            return AcquireResult(False)

    def _try_acquire_concurrency(self) -> AcquireResult:
        with self._cond:
            if self.current_concurrency < self.max_concurrency:
                self.current_concurrency += 1
                return AcquireResult(True)
            return AcquireResult(False)

    def _try_acquire_token_bucket(self) -> AcquireResult:
        try:
            service = self.service_queue.get_nowait()
            return AcquireResult(True, data=service)
        except queue.Empty:
            return AcquireResult(False)

# --- 使用示例 / Test Case ---
async def main():
    print("--- 1. QPS Test (Async) ---")
    # 设置 5 QPS (0.2s 一个令牌)
    qps_limiter = Limiter(LimiterType.QPS, 5)

    async def worker_qps(idx):
        # 尝试异步阻塞获取
        await qps_limiter.acquire_async()
        print(f"Worker {idx} acquired QPS token at {time.strftime('%X')}")

    # 此时应该看到每秒打印 5 个左右
    await asyncio.gather(*(worker_qps(i) for i in range(10)))

    print("\n--- 2. Concurrency Test (Sync in ThreadPool) ---")
    # 限制并发为 2
    conn_limiter = Limiter(LimiterType.CONCURRENCY, 2)

    def worker_conn(idx):
        print(f"Worker {idx} waiting...")
        if conn_limiter.acquire(timeout=2).success:
            print(f"Worker {idx} GOT lock")
            time.sleep(0.5) # 模拟持有资源
            conn_limiter.release()
            print(f"Worker {idx} RELEASED")
        else:
            print(f"Worker {idx} TIMEOUT")

    with ThreadPoolExecutor(max_workers=5) as pool:
        for i in range(5):
            pool.submit(worker_conn, i)

    print("\n--- 3. Token Bucket Test (Resource Pool) ---")
    pool_limiter = Limiter(LimiterType.TOKEN_BUCKET, ["DB-1", "DB-2"])

    res1 = pool_limiter.acquire()
    print(f"Got: {res1.data}") # DB-1
    res2 = pool_limiter.acquire()
    print(f"Got: {res2.data}") # DB-2

    # 此时池子空了，尝试非阻塞获取应该失败
    print(f"Try No-Block: {pool_limiter.try_acquire().success}") # False

    # 释放一个
    pool_limiter.release(res1.data)
    print("Released DB-1")

    # 再次阻塞获取
    res3 = pool_limiter.acquire()
    print(f"Got again: {res3.data}") # DB-1

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass