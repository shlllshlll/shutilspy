"""Auto-saving cache implementations with TTL and LRU eviction strategies."""

import atexit
import dataclasses
import hashlib
import inspect
import json
import logging
import lzma
import os
import pickle
import signal
import sys
import time
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from datetime import date, datetime
from enum import Enum
from functools import wraps
from pathlib import Path
from typing import Any, Literal, ParamSpec, TypeVar

from .param import asdict
from .utils import calculate_md5, get_callable_info

__all__ = [
    "LRUCache",
    "PresistentMixin",
    "StableCacheEncoder",
    "TTLCache",
    "cache_async_wrapper",
    "cache_sync_wrapper",
    "cached",
    "make_stable_key",
]

logger = logging.getLogger(__name__)

T_Retval = TypeVar("T_Retval")
T_ParamSpec = ParamSpec("T_ParamSpec")


class PresistentMixin:
    """Mixin that adds auto-saving persistence to cache stores.

    Data is serialized with pickle and compressed with lzma (``.pkl.xz``).
    Supports step-based and interval-based auto-save, plus graceful shutdown
    via ``atexit`` and signal handlers.

    Attributes:
        cache_file_path: Absolute path to the ``.pkl.xz`` persistence file.
        save_step: Number of write operations between auto-saves. 0 disables.
        save_interval: Seconds between auto-saves. 0 disables.
    """

    def __init__(self, cache_file: str | None = None, save_step: int = 0, save_interval: float = 0):
        """Initialize the persistence mixin.

        Args:
            cache_file: Path to the cache file. If provided, a ``.pkl.xz`` suffix
                is appended automatically.
            save_step: Save automatically after every *save_step* write operations.
                0 disables step-based auto-save.
            save_interval: Save automatically when this many seconds have elapsed
                since the last save. 0 disables interval-based auto-save.
        """
        self.cache_file_path = None
        if cache_file:
            self._init_path(cache_file)

        # Auto-save config
        self.save_step = save_step
        self.save_interval = save_interval

        # Internal state
        self._write_count = 0
        self._last_save_time = time.time()

        # Data container (Managed by subclasses, but initialized here for safety)
        if not hasattr(self, 'cache'):
            self.cache = {}

        # Handle Normal Exit & SIGINT/Ctrl+C
        atexit.register(self._handle_exit)
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    def _init_path(self, cache_file: str):
        path = Path(cache_file).absolute()
        # 强制添加 .pkl.xz 后缀，防止误操作
        if not path.name.endswith(".pkl.xz"):
            path = path.parent / f"{path.name}.pkl.xz"
        self.cache_file_path = path
        self.cache_file_path.parent.mkdir(parents=True, exist_ok=True)

    def load_cache(self):
        """Load cache data from disk if the persistence file exists.

        Merges loaded data into the existing ``cache`` dict. Resets the
        save timer on success.
        """
        if not self.cache_file_path or not self.cache_file_path.exists():
            return

        self.cache_file_md5 = ""
        try:
            self.cache_file_md5 = calculate_md5(self.cache_file_path)
            with lzma.open(self.cache_file_path, "rb") as f:
                loaded_data = pickle.load(f)
                # 兼容处理：确保加载的数据能正确update到当前实例
                if isinstance(self.cache, OrderedDict) and isinstance(loaded_data, dict):
                    self.cache.update(loaded_data)
                    # 如果是LRU，加载后可能需要重新move_to_end? 暂时简单update
                else:
                    self.cache = loaded_data

            self._last_save_time = time.time() # 重置计时器
            logger.info(f"[Cache]: Loaded from {self.cache_file_path}")
        except Exception as e:
            logger.error(f"[Cache]: Load failed: {e}")

    def save_cache(self):
        """Persist the cache to disk with optimistic-lock merge.

        If the file was modified externally (detected via MD5), disk data is
        merged with in-memory data before writing.
        """
        if not self.cache_file_path:
            return

        try:
            # 简单的乐观锁逻辑：检查文件是否被外部修改
            if self.cache_file_path.exists():
                current_md5 = calculate_md5(self.cache_file_path)
                if hasattr(self, "cache_file_md5") and current_md5 != self.cache_file_md5:
                    logger.warning("[Cache]: File changed on disk, merging...")
                    with lzma.open(self.cache_file_path, "rb") as f:
                        disk_cache = pickle.load(f)
                        # 保留内存中较新的修改，合并磁盘上的旧Key
                        disk_cache.update(self.cache)
                        self.cache = disk_cache

            with lzma.open(self.cache_file_path, "wb") as f:
                pickle.dump(self.cache, f)

            # 更新状态
            self.cache_file_md5 = calculate_md5(self.cache_file_path)
            self._last_save_time = time.time()
            self._write_count = 0
            logger.debug(f"[Cache]: Saved to {self.cache_file_path}")
        except Exception as e:
            logger.error(f"[Cache]: Save failed: {e}")

    def _trigger_auto_save(self):
        """Check whether auto-save conditions are met after a write operation."""
        if not self.cache_file_path:
            return

        should_save = False

        # 1. Check Step
        self._write_count += 1
        if self.save_step > 0 and self._write_count >= self.save_step:
            should_save = True
            logger.debug("[Cache]: Auto-save triggered by step count")

        # 2. Check Interval (Time)
        if not should_save and self.save_interval > 0 and time.time() - self._last_save_time >= self.save_interval:
            should_save = True
            logger.debug("[Cache]: Auto-save triggered by time interval")

        if should_save:
            self.save_cache()

    def _signal_handler(self, signum, frame):
        """Handle termination signals by saving the cache before exit."""
        logger.info(f"[Cache]: Received signal {signum}, saving to {self.cache_file_path}...")
        self.save_cache()
        sys.exit(0)

    def _handle_exit(self):
        logger.info(f"[Cache]: Program will exit, saving to {self.cache_file_path}...")
        self.save_cache()

    def _get_hash_str(self, key_str: str) -> str:
        # 直接生成 64位长度的字符串
        return hashlib.sha256(key_str.encode('utf-8')).hexdigest()


class TTLCache(PresistentMixin):
    """A key-value cache with per-entry time-to-live expiration.

    Keys are hashed with SHA-256 before storage.

    Attributes:
        ttl: Default time-to-live in seconds for cache entries.
    """

    def __init__(self, ttl: float = 300, cache_file: str | None = None, save_step: int = 0, save_interval: float = 0):
        """Initialize the TTL cache.

        Args:
            ttl: Default time-to-live in seconds for entries.
            cache_file: Path to the persistence file.
            save_step: Auto-save after this many writes.
            save_interval: Auto-save after this many seconds.
        """
        self.cache: dict[str, tuple[Any, float]] = {}
        # 先初始化Mixin，设置好路径和参数
        super().__init__(cache_file, save_step, save_interval)
        self.ttl = ttl
        self.load_cache() # 初始化时加载
        self.cleanup()

    def get(self, key: str) -> Any | None:
        """Retrieve a value by key, returning None if missing or expired.

        Args:
            key: The cache key.

        Returns:
            The cached value, or None if the key does not exist or has expired.
        """
        hash_key = self._get_hash_str(key)
        if hash_key not in self.cache:
            return None
        value, expiry = self.cache[hash_key]
        if time.time() > expiry:
            self.delete(key) # 过期删除也会触发auto_save检查
            return None
        return value

    def set(self, key: str, value: Any, ttl: float | None = None) -> None:
        """Store a value with an optional per-key TTL.

        Args:
            key: The cache key.
            value: The value to store.
            ttl: Time-to-live in seconds. Falls back to the instance default.
        """
        ttl = ttl if ttl is not None else self.ttl
        expiry = time.time() + ttl
        hash_key = self._get_hash_str(key)
        self.cache[hash_key] = (value, expiry)
        self._trigger_auto_save() # 触发检查

    def delete(self, key: str) -> None:
        """Remove a key from the cache.

        Args:
            key: The cache key to delete.
        """
        hash_key = self._get_hash_str(key)
        if hash_key in self.cache:
            del self.cache[hash_key]
            self._trigger_auto_save()

    def clear(self) -> None:
        """Remove all entries from the cache and persist immediately."""
        self.cache.clear()
        self.save_cache() # Clear 属于重大变更，强制保存

    def cleanup(self) -> None:
        """Remove all expired entries from the cache."""
        expired_keys = [k for k, (_, exp) in self.cache.items() if time.time() > exp]
        for k in expired_keys:
            del self.cache[k]
        if expired_keys:
            self._trigger_auto_save() # 清理也算写入


class LRUCache(PresistentMixin):
    """A key-value cache with least-recently-used eviction.

    Keys are hashed with SHA-256 before storage. When the cache exceeds
    ``max_size``, the least recently accessed entries are evicted.

    Attributes:
        max_size: Maximum number of entries to keep.
    """

    def __init__(self, max_size=10000, cache_file: str | None = None, save_step: int = 0, save_interval: float = 0):
        """Initialize the LRU cache.

        Args:
            max_size: Maximum number of entries before eviction.
            cache_file: Path to the persistence file.
            save_step: Auto-save after this many writes.
            save_interval: Auto-save after this many seconds.
        """
        self.cache = OrderedDict()
        super().__init__(cache_file, save_step, save_interval)
        self.max_size = max_size
        self.load_cache()
        self.cleanup()

    def get(self, key):
        """Retrieve a value by key, promoting it to the most-recent position.

        Args:
            key: The cache key.

        Returns:
            The cached value, or None if the key does not exist.
        """
        hash_key = self._get_hash_str(key)
        if hash_key not in self.cache:
            return None
        self.cache.move_to_end(hash_key)
        return self.cache[hash_key]

    def set(self, key, value):
        """Store a key-value pair, moving it to the most-recent position.

        Args:
            key: The cache key.
            value: The value to store.
        """
        hash_key = self._get_hash_str(key)
        if hash_key in self.cache:
            self.cache.move_to_end(hash_key)
        self.cache[hash_key] = value
        self.cleanup() # cleanup 可能会删除元素
        self._trigger_auto_save() # 触发检查

    def delete(self, key):
        """Remove a key from the cache.

        Args:
            key: The cache key to delete.
        """
        hash_key = self._get_hash_str(key)
        if hash_key in self.cache:
            del self.cache[hash_key]
            self._trigger_auto_save()

    def clear(self):
        """Remove all entries from the cache and persist immediately."""
        self.cache.clear()
        self.save_cache()

    def cleanup(self):
        """Evict the least-recently-used entries until size is within max_size."""
        while len(self.cache) > self.max_size:
            self.cache.popitem(last=False)


# --- Wrappers (No changes needed here) ---

class StableCacheEncoder(json.JSONEncoder):
    """JSON encoder that handles dataclasses, Pydantic models, enums, and datetimes.

    Produces stable, deterministic output by serializing objects into
    dictionary representations without memory addresses.
    """

    def default(self, o: Any) -> Any:
        """Serialize a non-standard object to a JSON-compatible type.

        Args:
            o: The object to serialize.

        Returns:
            A JSON-serializable representation of the object.
        """
        # 1. 处理 Dataclass
        if dataclasses.is_dataclass(o):
            return asdict(o)

        # 2. 处理 Pydantic 模型 (兼容 v1 和 v2)
        if hasattr(o, "model_dump"):  # Pydantic v2
            return o.model_dump()
        if hasattr(o, "dict") and callable(o.dict):  # Pydantic v1
            return o.dict()

        # 3. 处理 Enum
        if isinstance(o, Enum):
            return o.value

        # 4. 处理时间
        if isinstance(o, (date, datetime)):
            return o.isoformat()

        # 5. 处理带有 __dict__ 的通用对象 (去除 object at 0x...)
        # 这是解决你那个 "Chat object" 的关键
        if hasattr(o, "__dict__"):
            # 只取对象的属性状态，忽略内存地址
            return o.__dict__

        # 6. 兜底：如果实在无法序列化，转字符串，但尝试正则去掉地址（可选）
        # 或者简单粗暴返回 str(o)，但在复杂对象上依然可能有风险
        try:
            return super().default(o)
        except TypeError:
            return str(o)

def make_stable_key(func_info: str, args: tuple, kwargs: dict, ignore_self: bool = False) -> str:
    """Generate a deterministic cache key from function info and arguments.

    Args:
        func_info: Fully qualified callable name.
        args: Positional arguments to the function.
        kwargs: Keyword arguments to the function.
        ignore_self: If True, drop the first positional argument (typically ``self``).

    Returns:
        A JSON string that uniquely and deterministically identifies the call.
    """
    if ignore_self and len(args) > 0:
        args = args[1:] # 去掉第一个参数

    # 构造一个包含所有信息的结构
    key_data = {
        "func": func_info,
        "args": args,
        "kwargs": kwargs
    }

    # 核心：使用 JSON 序列化，并开启 sort_keys=True 保证字典顺序一致
    # separators=(',', ':') 去除空格，减少 key 长度
    try:
        json_str = json.dumps(
            key_data,
            cls=StableCacheEncoder,
            sort_keys=True,
            separators=(',', ':'),
            ensure_ascii=False
        )
    except Exception as e:
        # 极端情况兜底：如果 JSON 失败，回退到 repr 但可能不命中
        logger.warn(f"[Cache Warning]: Key serialization failed: {e}")
        json_str = f"{func_info}-{args!s}-{kwargs!s}"

    return json_str

def cache_sync_wrapper[**T_ParamSpec, T_Retval](
    cache: TTLCache | LRUCache, func: Callable[T_ParamSpec, T_Retval], key: str | None = None, ignore_self: bool = False
) -> Callable[T_ParamSpec, T_Retval]:
    """Wrap a synchronous function with cache look-aside logic.

    Args:
        cache: The cache backend instance.
        func: The synchronous function to wrap.
        key: Optional fixed cache key. If None, a key is derived from arguments.
        ignore_self: If True, exclude the first argument when building the key.

    Returns:
        The wrapped function.
    """
    @wraps(func)
    def sync_wrapper(*args: T_ParamSpec.args, **kwargs: T_ParamSpec.kwargs) -> T_Retval:
        if os.environ.get("DISABLE_CACHE"):
            return func(*args, **kwargs)
        cache_key = key if key is not None else make_stable_key(
            get_callable_info(func), args, kwargs, ignore_self=ignore_self
        )
        cache_result = cache.get(cache_key)
        logger.debug(f"[Cache]: sync_wrapper func={func} hit={cache_result is not None}")
        if cache_result is not None:
            return cache_result
        result = func(*args, **kwargs)
        cache.set(cache_key, result)
        return result
    return sync_wrapper

def cache_async_wrapper[**T_ParamSpec, T_Retval](
    cache: TTLCache | LRUCache,
    func: Callable[T_ParamSpec, Awaitable[T_Retval]],
    key: str | None = None,
    ignore_self: bool = True,
) -> Callable[T_ParamSpec, Awaitable[T_Retval]]:
    """Wrap an async function with cache look-aside logic.

    Args:
        cache: The cache backend instance.
        func: The async function to wrap.
        key: Optional fixed cache key. If None, a key is derived from arguments.
        ignore_self: If True, exclude the first argument when building the key.

    Returns:
        The wrapped async function.
    """
    @wraps(func)
    async def async_wrapper(*args: T_ParamSpec.args, **kwargs: T_ParamSpec.kwargs) -> T_Retval:
        if os.environ.get("DISABLE_CACHE"):
            return await func(*args, **kwargs)
        cache_key = key if key is not None else make_stable_key(
            get_callable_info(func), args, kwargs, ignore_self=ignore_self
        )
        cache_result = cache.get(cache_key)
        logger.debug(f"[Cache]: async_wrapper func={func} hit={cache_result is not None}")
        if cache_result is not None:
            return cache_result
        result = await func(*args, **kwargs)
        cache.set(cache_key, result)
        return result
    return async_wrapper


def cached(
    backend: Literal["ttl", "lru"] = "ttl",
    ignore_self: bool = False,
    **kwargs
):
    """Decorator factory that caches function results using a TTL or LRU backend.

    Args:
        backend: Cache backend type, either ``"ttl"`` or ``"lru"``.
        ignore_self: If True, exclude the first argument when building cache keys.
        **kwargs: Additional keyword arguments forwarded to the cache constructor.
            Common options include ``ttl``, ``max_size``, ``cache_file``,
            ``save_step``, and ``save_interval``.

    Returns:
        A decorator that wraps the target function with caching.
    """

    # 统一将 auto-save 参数放入 kwargs 传给 Cache 构造函数
    cache_instance: TTLCache | LRUCache

    if backend == "ttl":
        cache_instance = TTLCache(**kwargs)
    elif backend == "lru":
        cache_instance = LRUCache(**kwargs)
    else:
        raise ValueError(f"Unsupported backend: {backend}")

    def decorator(func: Callable[T_ParamSpec, T_Retval] | Callable[T_ParamSpec, Awaitable[T_Retval]]) -> Any:
        wrapper: Callable
        if inspect.iscoroutinefunction(func):
            wrapper = cache_async_wrapper(cache_instance, func, ignore_self=ignore_self) # type: ignore
        else:
            wrapper = cache_sync_wrapper(cache_instance, func, ignore_self=ignore_self) # type: ignore

        wrapper.cache = cache_instance
        return wrapper

    return decorator
