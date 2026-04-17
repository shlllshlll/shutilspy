"""Smart lock for async/sync mixed environments with adaptive scheduling."""

import asyncio
import inspect
import itertools
import threading
import time
from collections import deque
from collections.abc import Awaitable, Callable
from concurrent.futures import Executor, ThreadPoolExecutor
from contextlib import AbstractAsyncContextManager, AbstractContextManager, asynccontextmanager, contextmanager
from dataclasses import dataclass
from enum import Enum
from typing import TypeVar

T = TypeVar("T")

__all__ = [
    "AdaptiveMetrics",
    "GlobalExecutor",
    "LockStrategy",
    "MetricType",
    "SmartLock",
    "SmartLockConfig",
    "SmartRWLock",
    "is_async_callable",
]

# [辅助函数] 健壮的异步函数检测 (支持 partial 等)
def is_async_callable(obj) -> bool:
    """Check if an object is an async callable, handling partials."""
    while isinstance(obj, (lambda:0).__class__) and hasattr(obj, "func"): # Handle partials
        obj = obj.func
    return inspect.iscoroutinefunction(obj) or (callable(obj) and inspect.iscoroutinefunction(obj.__call__))

class LockStrategy(Enum):
    """Strategy for how a lock should be acquired in async context."""

    AUTO = "auto"       # Auto-decide based on metrics
    DIRECT = "direct"   # Block the event loop (for < 1ms operations)
    ASYNC_WAIT = "wait" # Async wait without blocking the loop (1ms - 10ms)
    EXECUTOR = "executor"  # Offload to thread pool (> 10ms)


class MetricType(Enum):
    """Metric type for adaptive lock strategy selection."""

    MEAN = "mean"
    P90 = "p90"
    P95 = "p95"
    P99 = "p99"
    MAX = "max"


@dataclass
class SmartLockConfig:
    """Configuration for smart lock behavior.

    Attributes:
        window_size: Number of samples for the adaptive metrics window.
        metric_type: Which percentile metric to use for strategy decisions.
        threshold_direct: Seconds below which DIRECT strategy is used.
        threshold_executor: Seconds above which EXECUTOR strategy is used.
        enable_sampling: Whether to sample only a fraction of operations.
        sampling_interval: Record one sample every N calls.
        calc_interval_seconds: Minimum seconds between metric recalculations.
        executor: Custom thread pool executor, worker count, or None for global.
        warmup_count: Minimum samples before adaptive strategy kicks in.
        default_strategy: Fallback strategy when not enough data is available.
    """
    window_size: int = 1000
    metric_type: MetricType = MetricType.P90
    threshold_direct: float = 0.001
    threshold_executor: float = 0.01
    enable_sampling: bool = True
    sampling_interval: int = 10
    calc_interval_seconds: float = 0.5
    executor: None | int | Executor = None
    warmup_count: int = 10
    default_strategy: LockStrategy = LockStrategy.DIRECT


class GlobalExecutor:
    """Global shared thread pool singleton."""

    _instance: ThreadPoolExecutor | None = None
    _lock = threading.Lock()

    @classmethod
    def get(cls) -> ThreadPoolExecutor:
        """Get or create the global thread pool executor."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    # 默认根据 CPU 核数自动调整
                    cls._instance = ThreadPoolExecutor(
                        thread_name_prefix="SmartLock-Global"
                    )
        return cls._instance


class AdaptiveMetrics:
    """High-performance adaptive metrics engine with sampling and caching."""

    def __init__(self, config: SmartLockConfig):
        """Initialize the metrics engine.

        Args:
            config: SmartLock configuration for window size, metric type, etc.
        """
        self.cfg = config
        # 使用 deque 存储历史数据，maxlen 自动处理淘汰
        self.history = deque(maxlen=config.window_size)

        # --- 采样状态 ---
        # 使用 itertools.count 作为高性能原子计数器
        self._counter = itertools.count()

        # --- 缓存状态 ---
        self._cached_metric: float = 0.0    # 上次计算结果
        self._last_calc_time: float = 0.0   # 上次计算时间
        self._dirty: bool = False           # 数据是否已更新

    def record(self, duration: float):
        """Record an operation duration (write path with sampling)."""
        # 1. 采样逻辑
        if self.cfg.enable_sampling:
            # 获取当前计数值 (itertools.count 是 C 层面原子的)
            c = next(self._counter)
            # 模运算决定是否采样。
            # 比如 interval=10，只有 0, 10, 20... 会被记录
            if c % self.cfg.sampling_interval != 0:
                return

        # 2. 记录数据
        # deque.append 在 Python 中是原子的（GIL保护），线程安全
        self.history.append(duration)

        # 3. 标记脏位 (告诉读路径需要重算了)
        # 简单的布尔赋值也是原子的
        self._dirty = True

    def get_metric_value(self) -> float:
        """Get the current metric value (read path with TTL caching)."""
        # 1. 快速检查：如果没有数据，返回 0
        if not self.history:
            return 0.0

        now = time.monotonic()

        # 2. 缓存检查 (Cache Hit)
        # 如果数据没脏(没有新写入) OR 距离上次计算不足 TTL 时间
        # 直接返回旧值，避免高频调用时的重复排序计算
        if not self._dirty or (now - self._last_calc_time < self.cfg.calc_interval_seconds):
            return self._cached_metric

        # 3. 重新计算 (Cache Miss)
        # 这里为了计算准确性，我们可以选择 snapshot 当前数据
        # list(deque) 会复制数据，开销是 O(N)，但在 metric 计算频率被 TTL 限制的情况下是可以接受的
        data_snapshot = list(self.history)

        if not data_snapshot:
            return 0.0

        val = 0.0
        try:
            if self.cfg.metric_type == MetricType.MEAN:
                # 手写 sum/len 比 statistics.mean 快
                val = sum(data_snapshot) / len(data_snapshot)

            elif self.cfg.metric_type == MetricType.MAX:
                val = max(data_snapshot)

            elif self.cfg.metric_type in (MetricType.P90, MetricType.P95):
                # 排序是主要的 CPU 开销来源
                data_snapshot.sort()
                percentile = 0.90 if self.cfg.metric_type == MetricType.P90 else 0.95
                index = int(len(data_snapshot) * percentile)
                # 边界保护
                index = min(index, len(data_snapshot) - 1)
                val = data_snapshot[index]
        except Exception:
            # 兜底防止极端并发下的空列表错误
            val = 0.0

        # 4. 更新缓存
        self._cached_metric = val
        self._last_calc_time = now
        self._dirty = False # 标记为干净

        return val

    def suggest_strategy(self) -> LockStrategy:
        """Suggest a lock strategy based on the current metrics."""
        # 如果样本数太少（注意：这里指实际记录下来的样本数），使用预热/默认策略
        if len(self.history) < self.cfg.warmup_count:
            return self.cfg.default_strategy

        val = self.get_metric_value()

        # 根据阈值判断
        if val < self.cfg.threshold_direct:
            return LockStrategy.DIRECT
        elif val > self.cfg.threshold_executor:
            return LockStrategy.EXECUTOR
        else:
            return LockStrategy.ASYNC_WAIT

class SmartLock:
    """Smart mutex lock supporting sync/async mixed usage and adaptive scheduling."""

    def __init__(self, config: SmartLockConfig | None = None):
        """Initialize the smart lock.

        Args:
            config: Optional configuration; defaults to SmartLockConfig().
        """
        self.cfg = config or SmartLockConfig()
        self._metrics = AdaptiveMetrics(self.cfg)
        self._mutex = threading.Lock()

        # 初始化 Executor
        if self.cfg.executor is None:
            self._executor = GlobalExecutor.get()
        elif isinstance(self.cfg.executor, int):
            self._executor = ThreadPoolExecutor(max_workers=self.cfg.executor)
        else:
            self._executor = self.cfg.executor

    def sync_run(self, func: Callable[..., T], *args, **kwargs) -> T:
        """Run a function under the mutex lock synchronously."""
        start = time.perf_counter()
        with self._mutex:
            try:
                return func(*args, **kwargs)
            finally:
                self._metrics.record(time.perf_counter() - start)

    async def async_run(
        self,
        func: Callable[..., T] | Callable[..., Awaitable[T]],
        *args,
        force_strategy: LockStrategy | None = None,
        **kwargs,
    ) -> T:
        """Run a function under the mutex lock with adaptive strategy.

        Supports both sync and async functions.

        Args:
            func: The function to run under the lock.
            *args: Positional arguments for the function.
            force_strategy: Override the auto-detected strategy.
            **kwargs: Keyword arguments for the function.

        Returns:
            The function's return value.
        """
        # 1. 检测函数类型
        is_async = is_async_callable(func)

        strategy = force_strategy or self.cfg.default_strategy
        if not force_strategy and self.cfg.default_strategy == LockStrategy.AUTO:
            strategy = self._metrics.suggest_strategy()

        # [特殊处理] 如果是异步函数，且策略建议 EXECUTOR，必须降级为 ASYNC_WAIT
        # 因为协程不能在 ThreadPoolExecutor 里直接跑
        if is_async and strategy == LockStrategy.EXECUTOR:
            strategy = LockStrategy.ASYNC_WAIT

        # Case A: 线程池卸载 (仅 Sync 函数)
        if strategy == LockStrategy.EXECUTOR and not is_async:
            loop = asyncio.get_running_loop()
            def _wrapped_task():
                t0 = time.perf_counter()
                with self._mutex:
                    try:
                        return func(*args, **kwargs)
                    finally:
                        self._metrics.record(time.perf_counter() - t0)
            return await loop.run_in_executor(self._executor, _wrapped_task)

        # Case B: 直接模式 (Sync/Async)
        elif strategy == LockStrategy.DIRECT:
            t0 = time.perf_counter()
            with self._mutex: # 警告：阻塞 Loop
                try:
                    if is_async:
                        return await func(*args, **kwargs)
                    else:
                        return func(*args, **kwargs)
                finally:
                    self._metrics.record(time.perf_counter() - t0)

        # Case C: 异步等待模式 (Sync/Async)
        else: # ASYNC_WAIT
            if self._mutex.acquire(blocking=False):
                # Fast path: 拿到锁了
                try:
                    t0 = time.perf_counter()
                    if is_async:
                        return await func(*args, **kwargs)
                    else:
                        return func(*args, **kwargs)
                finally:
                    self._metrics.record(time.perf_counter() - t0)
                    self._mutex.release()
            else:
                # Slow path: 去线程池等锁
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, self._mutex.acquire)
                try:
                    t0 = time.perf_counter()
                    if is_async:
                        return await func(*args, **kwargs)
                    else:
                        return func(*args, **kwargs)
                finally:
                    self._metrics.record(time.perf_counter() - t0)
                    self._mutex.release()

    @contextmanager
    def sync_lock(self):
        """Synchronous context manager for the mutex lock."""
        with self._mutex:
            yield

    @asynccontextmanager
    async def async_lock(self):
        """Async context manager for the mutex lock.

        Note: Context manager syntax cannot support EXECUTOR strategy;
        only DIRECT and ASYNC_WAIT are available.
        """
        if self._mutex.acquire(blocking=False):
            try:
                yield
            finally:
                self._mutex.release()
        else:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._mutex.acquire)
            try:
                yield
            finally:
                self._mutex.release()


class SmartRWLock:
    """Smart read-write lock (write-priority) with adaptive scheduling.

    Read operations use DIRECT/ASYNC_WAIT strategy (reads are typically fast).
    Write operations support adaptive EXECUTOR offloading (writes may be slow).
    """

    def __init__(self, config: SmartLockConfig | None = None):
        """Initialize the read-write lock.

        Args:
            config: Optional configuration; defaults to SmartLockConfig().
        """
        self.cfg = config or SmartLockConfig()
        self._metrics = AdaptiveMetrics(self.cfg)  # 仅统计写操作耗时

        if self.cfg.executor is None:
            self._executor = GlobalExecutor.get()
        elif isinstance(self.cfg.executor, int):
            self._executor = ThreadPoolExecutor(max_workers=self.cfg.executor)
        else:
            self._executor = self.cfg.executor

        # 内部锁状态
        self._state_lock = threading.Lock()
        self._readers = 0
        self._writers_waiting = 0
        self._writer_active = False
        self._cond = threading.Condition(self._state_lock)

    # ================= 读操作 (Read) =================
    # 读操作通常不进行指标统计，也不建议卸载到线程池(除非极重)

    def read_sync_run(self, func: Callable[..., T], *args, **kwargs) -> T:
        """Run a function under a read lock synchronously."""
        with self._read_guard():
            return func(*args, **kwargs)

    async def read_async_run(self, func: Callable[..., T] | Callable[..., Awaitable[T]], *args, **kwargs) -> T:
        """Run a function under a read lock asynchronously (supports async functions)."""
        is_async = is_async_callable(func)
        loop = asyncio.get_running_loop()

        # 1. Fast Path
        if self._try_acquire_read():
            try:
                if is_async:
                    return await func(*args, **kwargs)
                else:
                    return func(*args, **kwargs)
            finally:
                self._release_read()

        # 2. Slow Path
        await loop.run_in_executor(None, self._blocking_acquire_read)
        try:
            if is_async:
                return await func(*args, **kwargs)
            else:
                return func(*args, **kwargs)
        finally:
            self._release_read()

    # ================= 写操作 (Write) =================
    # 写操作完全复用 SmartLock 的自适应逻辑

    def write_sync_run(self, func: Callable[..., T], *args, **kwargs) -> T:
        """Run a function under a write lock synchronously."""
        t0 = time.perf_counter()
        with self._write_guard():
            try:
                return func(*args, **kwargs)
            finally:
                self._metrics.record(time.perf_counter() - t0)

    async def write_async_run(
        self,
        func: Callable[..., T] | Callable[..., Awaitable[T]],
        *args,
        force_strategy: LockStrategy | None = None,
        **kwargs,
    ) -> T:
        """Run a function under a write lock with adaptive strategy (supports async functions)."""
        is_async = is_async_callable(func)

        strategy = force_strategy or self.cfg.default_strategy
        if not force_strategy and self.cfg.default_strategy == LockStrategy.AUTO:
            strategy = self._metrics.suggest_strategy()

        # Async 函数不能使用 Executor 策略
        if is_async and strategy == LockStrategy.EXECUTOR:
            strategy = LockStrategy.ASYNC_WAIT

        # 策略 1: EXECUTOR (仅 Sync)
        if strategy == LockStrategy.EXECUTOR and not is_async:
            loop = asyncio.get_running_loop()
            def _task():
                t0 = time.perf_counter()
                self._blocking_acquire_write()
                try:
                    return func(*args, **kwargs)
                finally:
                    duration = time.perf_counter() - t0
                    self._metrics.record(duration)
                    self._release_write()
            return await loop.run_in_executor(self._executor, _task)

        # 策略 2: DIRECT / ASYNC_WAIT (Sync/Async)
        else:
            loop = asyncio.get_running_loop()
            if not self._try_acquire_write():
                await loop.run_in_executor(None, self._blocking_acquire_write)

            # 拿到锁了
            try:
                t0 = time.perf_counter()
                if is_async:
                    return await func(*args, **kwargs)
                else:
                    return func(*args, **kwargs)
            finally:
                self._metrics.record(time.perf_counter() - t0)
                self._release_write()

    # ================= 底层锁原语 =================

    def _try_acquire_read(self) -> bool:
        with self._state_lock:
            # 写优先：如果有写者在等，读者也不能进
            if not self._writer_active and self._writers_waiting == 0:
                self._readers += 1
                return True
            return False

    def _blocking_acquire_read(self):
        with self._cond:
            # 等待直到没有活跃写者，且没有等待的写者(写优先)
            while self._writer_active or self._writers_waiting > 0:
                self._cond.wait()
            self._readers += 1

    def _release_read(self):
        with self._cond:
            self._readers -= 1
            if self._readers == 0:
                self._cond.notify_all()  # 唤醒等待的写者

    def _try_acquire_write(self) -> bool:
        with self._state_lock:
            if not self._writer_active and self._readers == 0:
                self._writer_active = True
                return True
            return False

    def _blocking_acquire_write(self):
        with self._cond:
            self._writers_waiting += 1
            try:
                while self._writer_active or self._readers > 0:
                    self._cond.wait()
                self._writer_active = True
            finally:
                self._writers_waiting -= 1

    def _release_write(self):
        with self._cond:
            self._writer_active = False
            self._cond.notify_all()  # 唤醒所有人

    @contextmanager
    def _read_guard(self):
        self._blocking_acquire_read()
        try:
            yield
        finally:
            self._release_read()

    @contextmanager
    def _write_guard(self):
        self._blocking_acquire_write()
        try:
            yield
        finally:
            self._release_write()

    # ==========================================
    # 1. 工厂方法：获取上下文管理器
    # ==========================================

    def read(self):
        """Get a synchronous read context manager."""
        return self._ReadSyncContext(self)

    def write(self):
        """Get a synchronous write context manager."""
        return self._WriteSyncContext(self)

    def async_read(self):
        """Get an async read context manager."""
        return self._ReadAsyncContext(self)

    def async_write(self):
        """Get an async write context manager."""
        return self._WriteAsyncContext(self)

    # ==========================================
    # 2. 上下文管理器实现类 (Helpers)
    # ==========================================

    class _ReadSyncContext(AbstractContextManager):
        def __init__(self, lock: 'SmartRWLock'): self.lock = lock
        def __enter__(self): self.lock._blocking_acquire_read()
        def __exit__(self, *args): self.lock._release_read()

    class _WriteSyncContext(AbstractContextManager):
        def __init__(self, lock: 'SmartRWLock'): self.lock = lock
        def __enter__(self):
            self.start = time.perf_counter()
            self.lock._blocking_acquire_write()
        def __exit__(self, *args):
            # 记录耗时，供 SmartLock 学习
            duration = time.perf_counter() - self.start
            self.lock._metrics.record(duration)
            self.lock._release_write()

    class _ReadAsyncContext(AbstractAsyncContextManager):
        def __init__(self, lock: 'SmartRWLock'): self.lock = lock
        async def __aenter__(self):
            # 尝试非阻塞获取
            if self.lock._try_acquire_read():
                return
            # 必须等待：将等待动作卸载到线程池
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self.lock._blocking_acquire_read)

        async def __aexit__(self, *args):
            self.lock._release_read()

    class _WriteAsyncContext(AbstractAsyncContextManager):
        def __init__(self, lock: 'SmartRWLock'): self.lock = lock
        async def __aenter__(self):
            # 注意：async with 无法实现 EXECUTOR 策略（自动扔到线程池执行）
            # 所以这里只实现了 ASYNC_WAIT 策略
            loop = asyncio.get_running_loop()

            # 尝试非阻塞获取
            if not self.lock._try_acquire_write():
                # 获取锁失败，去线程池里等锁，防止卡死 Loop
                await loop.run_in_executor(None, self.lock._blocking_acquire_write)

            # 拿到锁了，开始计时
            self.start = time.perf_counter()

        async def __aexit__(self, *args):
            duration = time.perf_counter() - self.start
            # 记录耗时：虽然本次没法用 Executor 策略，但数据可以供 run() 接口使用
            self.lock._metrics.record(duration)
            self.lock._release_write()

if __name__ == "__main__":
    import random

    # 配置：非常激进，超过 2ms 就扔到线程池
    config = SmartLockConfig(
        metric_type=MetricType.P95,
        window_size=50,
        threshold_direct=0.0005, # 0.5ms
        threshold_executor=0.002, # 2ms
        executor=4 # 独享4线程池
    )

    cache_lock = SmartRWLock(config)
    data_store = {"val": 0}

    # --- 模拟业务逻辑 ---

    def read_db(key: str):
        # 模拟快速读取
        time.sleep(0.0001)
        return f"{key}:{data_store['val']}"

    def write_db_heavy(key: str, val: int):
        # 模拟重型写入 (IO + CPU)
        # 这里的耗时是随机的，模拟真实世界的波动
        delay = random.uniform(0.001, 0.05) # 1ms ~ 50ms
        time.sleep(delay)
        data_store['val'] = val
        return f"Updated {val} (cost {delay:.4f}s)"

    # --- 异步 Worker ---

    async def reader_worker(id: int):
        for _i in range(5):
            # 读操作：并发安全
            await cache_lock.read_async_run(read_db, "key")
            # print(f"[R-{id}] {res}")
            await asyncio.sleep(0.01)

    async def writer_worker(id: int):
        for _i in range(5):
            val = random.randint(1, 100)

            # 写操作：SmartLock 会自动判断
            # 如果 write_db_heavy 很慢，它会自动被扔到 executor
            # 从而保证 reader_worker 的 Loop 不会被卡死
            start = time.time()
            res = await cache_lock.write_async_run(write_db_heavy, "key", val)
            duration = time.time() - start

            strategy = cache_lock._metrics.suggest_strategy()
            print(f"[W-{id}] {res} | TotalTime: {duration:.4f}s | Current Strategy: {strategy.name}")

            await asyncio.sleep(0.1)

    async def main():
        print("--- Starting Industrial SmartLock Demo ---")

        # 启动 10 个读者，2 个写者
        readers = [reader_worker(i) for i in range(10)]
        writers = [writer_worker(i) for i in range(2)]

        await asyncio.gather(*readers, *writers)

        print("\n--- Final Metrics ---")
        print(f"Recorded Writes: {len(cache_lock._metrics.history)}")
        print(f"P95 Duration: {cache_lock._metrics.get_metric_value():.6f}s")


    asyncio.run(main())
