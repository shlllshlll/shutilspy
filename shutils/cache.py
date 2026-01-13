#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
File: cache.py
Author: shlll(shlll7347@gmail.com)
Modified By: shlll(shlll7347@gmail.com)
Brief: Auto-saving Cache Implementation
"""

import time
import logging
import lzma
import pickle
import inspect
from functools import wraps
from pathlib import Path
from typing import Dict, Any, Awaitable, Callable, ParamSpec, TypeVar, Literal
from collections import OrderedDict
from .utils import get_callable_info, calculate_md5

logger = logging.getLogger(__name__)

T_Retval = TypeVar("T_Retval")
T_ParamSpec = ParamSpec("T_ParamSpec")


class PresistentMixin:
    def __init__(self, cache_file: str | None = None, save_step: int = 0, save_interval: float = 0):
        """
        Args:
            cache_file: 缓存文件路径
            save_step: 每多少次写入(set/delete)自动保存一次。0表示不启用。
            save_interval: 每多少秒自动保存一次。0表示不启用。
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

    def _init_path(self, cache_file: str):
        path = Path(cache_file).absolute()
        # 强制添加 .pkl.xz 后缀，防止误操作
        if not path.name.endswith(".pkl.xz"):
            path = path.parent / f"{path.name}.pkl.xz"
        self.cache_file_path = path
        self.cache_file_path.parent.mkdir(parents=True, exist_ok=True)

    def load_cache(self):
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
            logger.info(f"[Cache]: Saved to {self.cache_file_path}")
        except Exception as e:
            logger.error(f"[Cache]: Save failed: {e}")

    def _trigger_auto_save(self):
        """每一次写入操作(set/delete)后调用此函数判断是否需要落盘"""
        if not self.cache_file_path:
            return

        should_save = False

        # 1. Check Step
        self._write_count += 1
        if self.save_step > 0 and self._write_count >= self.save_step:
            should_save = True
            logger.debug("[Cache]: Auto-save triggered by step count")

        # 2. Check Interval (Time)
        # 注意：只有在发生写入时才会检查时间，这叫 "Lazy Expiration"
        if not should_save and self.save_interval > 0:
            if time.time() - self._last_save_time >= self.save_interval:
                should_save = True
                logger.debug("[Cache]: Auto-save triggered by time interval")

        if should_save:
            self.save_cache()


class TTLCache(PresistentMixin):
    def __init__(self, ttl: float = 300, cache_file: str | None = None, save_step: int = 0, save_interval: float = 0):
        self.cache: Dict[str, tuple[Any, float]] = {}
        # 先初始化Mixin，设置好路径和参数
        super().__init__(cache_file, save_step, save_interval)
        self.ttl = ttl
        self.load_cache() # 初始化时加载
        self.cleanup()

    def get(self, key: str) -> Any | None:
        if key not in self.cache:
            return None
        value, expiry = self.cache[key]
        if time.time() > expiry:
            self.delete(key) # 过期删除也会触发auto_save检查
            return None
        return value

    def set(self, key: str, value: Any, ttl: float | None = None) -> None:
        ttl = ttl if ttl is not None else self.ttl
        expiry = time.time() + ttl
        self.cache[key] = (value, expiry)
        self._trigger_auto_save() # 触发检查

    def delete(self, key: str) -> None:
        if key in self.cache:
            del self.cache[key]
            self._trigger_auto_save() # 触发检查

    def clear(self) -> None:
        self.cache.clear()
        self.save_cache() # Clear 属于重大变更，强制保存

    def cleanup(self) -> None:
        now = time.time()
        expired_keys = [k for k, (_, exp) in self.cache.items() if now > exp]
        for k in expired_keys:
            del self.cache[k]
        if expired_keys:
            self._trigger_auto_save() # 清理也算写入


class LRUCache(PresistentMixin):
    def __init__(self, max_size=10000, cache_file: str | None = None, save_step: int = 0, save_interval: float = 0):
        self.cache = OrderedDict()
        super().__init__(cache_file, save_step, save_interval)
        self.max_size = max_size
        self.load_cache()
        self.cleanup()

    def get(self, key):
        if key not in self.cache:
            return None
        self.cache.move_to_end(key)
        return self.cache[key]

    def set(self, key, value):
        if key in self.cache:
            self.cache.move_to_end(key)
        self.cache[key] = value
        self.cleanup() # cleanup 可能会删除元素
        self._trigger_auto_save() # 触发检查

    def delete(self, key):
        if key in self.cache:
            del self.cache[key]
            self._trigger_auto_save()

    def clear(self):
        self.cache.clear()
        self.save_cache()

    def cleanup(self):
        while len(self.cache) > self.max_size:
            self.cache.popitem(last=False)


# --- Wrappers (No changes needed here) ---

def cache_sync_wrapper(
    cache: TTLCache | LRUCache, func: Callable[T_ParamSpec, T_Retval], key: str | None = None
) -> Callable[T_ParamSpec, T_Retval]:
    @wraps(func)
    def sync_wrapper(*args: T_ParamSpec.args, **kwargs: T_ParamSpec.kwargs) -> T_Retval:
        cache_key = key if key is not None else f"{get_callable_info(func)}-{args}-{kwargs}"
        cache_result = cache.get(cache_key)
        logger.debug(f"[Cache]: sync_wrapper key={cache_key} hit={cache_result is not None}")
        if cache_result is not None:
            return cache_result
        result = func(*args, **kwargs)
        cache.set(cache_key, result)
        return result
    return sync_wrapper

def cache_async_wrapper(
    cache: TTLCache | LRUCache, func: Callable[T_ParamSpec, Awaitable[T_Retval]], key: str | None = None
) -> Callable[T_ParamSpec, Awaitable[T_Retval]]:
    @wraps(func)
    async def async_wrapper(*args: T_ParamSpec.args, **kwargs: T_ParamSpec.kwargs) -> T_Retval:
        cache_key = key if key is not None else f"{get_callable_info(func)}-{args}-{kwargs}"
        cache_result = cache.get(cache_key)
        logger.debug(f"[Cache]: async_wrapper key={cache_key} hit={cache_result is not None}")
        if cache_result is not None:
            return cache_result
        result = await func(*args, **kwargs)
        cache.set(cache_key, result)
        return result
    return async_wrapper


def cached(
    backend: Literal["ttl", "lru"] = "ttl",
    **kwargs
):
    """
    Args:
        backend: 'ttl' | 'lru'
        save_step: 每写入多少次自动保存(set/delete)
        save_interval: 距离上次保存超过多少秒自动保存(在写入时懒惰检查)
        **kwargs: 传给Cache构造器的其他参数 (ttl, max_size, cache_file)
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
            wrapper = cache_async_wrapper(cache_instance, func) # type: ignore
        else:
            wrapper = cache_sync_wrapper(cache_instance, func) # type: ignore

        setattr(wrapper, "cache", cache_instance)
        return wrapper

    return decorator
