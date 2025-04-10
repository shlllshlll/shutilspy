#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
File: cache.py
Author: shlll(shlll7347@gmail.com)
Modified By: shlll(shlll7347@gmail.com)
Brief:
"""

import time
from functools import wraps
import logging
import lzma
from pathlib import Path
from typing import Dict, Any, Awaitable, Callable, ParamSpec, TypeVar
import pickle
from collections import OrderedDict
from .utils import get_callable_info, calculate_md5

logger = logging.getLogger(__name__)

T_Retval = TypeVar("T_Retval")
T_ParamSpec = ParamSpec("T_ParamSpec")


class PresistentMixin:
    def _proc_dir(self, cache_file: str):
        cache_path = Path(cache_file).absolute()
        cache_path = cache_path.parent / f"{cache_path.stem}.pkl.xz"
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        return cache_path

    def load_cache(self, cache_file: str | None = None):
        if not hasattr(self, "cache_file"):
            if cache_file is None:
                return
            self.cache_file = self._proc_dir(cache_file)
        self.cache_file_md5 = ""

        if not self.cache_file.exists():
            return

        try:
            self.cache_file_md5 = calculate_md5(self.cache_file)
            with lzma.open(self.cache_file, "rb") as f:
                self.cache = pickle.load(f)
            logger.info(f"[PresistentMixin]: Cache loaded from disk: {self.cache_file}")
        except Exception as e:
            logger.error(f"[PresistentMixin]: Cache load failed: {e}")

    def save_cache(self, cache_file: str | None = None):
        if not hasattr(self, "cache_file"):
            if cache_file is None:
                return
            self.cache_file = self._proc_dir(cache_file)
        try:
            if self.cache_file.exists():
                current_md5 = calculate_md5(self.cache_file)
                if current_md5 != self.cache_file_md5:
                    # 此时落盘的cache发生了变化，需要重新读取并merge
                    with lzma.open(self.cache_file, "rb") as f:
                        cache = pickle.load(f)
                        self.cache.update(cache)

            with lzma.open(self.cache_file, "wb") as f:
                pickle.dump(self.cache, f)
            logger.info(f"[PresistentMixin]: Cache saved to disk: {self.cache_file}")
        except Exception as e:
            logger.error(f"[PresistentMixin]: Cache save failed: {e}")


class TTLCache(PresistentMixin):
    """A simple key-value cache with time-based expiration."""

    def __init__(self, ttl: float | None = None, cache_file: str | None = None):
        """Initialize cache with TTL in seconds (default 5 minutes)."""
        self.cache: Dict[str, tuple[Any, float]] = {}
        self.ttl = ttl
        self.load_cache(cache_file)
        self.cleanup()

    def get(self, key: str) -> Any | None:
        """Get value from cache if it exists and hasn't expired."""
        if key not in self.cache:
            return None

        value, expiry = self.cache[key]
        if time.time() > expiry:
            del self.cache[key]
            return None

        return value

    def set(self, key: str, value: Any, ttl: float | None = None) -> None:
        """Put value in cache with optional custom TTL."""
        ttl = ttl if ttl is not None else self.ttl
        if ttl is None:
            raise ValueError("TTL must be provided")
        expiry = time.time() + ttl
        self.cache[key] = (value, expiry)

    def delete(self, key: str) -> None:
        """Delete key from cache."""
        self.cache.pop(key, None)

    def clear(self) -> None:
        """Clear all entries from cache."""
        self.cache.clear()

    def cleanup(self) -> None:
        """Remove all expired entries from cache."""
        now = time.time()
        expired_keys = [k for k, (_, exp) in self.cache.items() if now > exp]
        for k in expired_keys:
            del self.cache[k]
    
    def save_cache(self, cache_file: str | None = None):
        """Save cache and cleanup expired entries."""
        self.cleanup()
        super().save_cache(cache_file)


class LRUCache(PresistentMixin):
    def __init__(self, max_size=10000, cache_file: str | None = None):
        self.cache = OrderedDict()
        self.max_size = max_size
        self.load_cache(cache_file)
        self.cleanup()

    def get(self, key):
        if key not in self.cache:
            logger.warning(f"[PersistentLRUCache]: Key not found: {key}")
            return None
        # Move the accessed item to the end (most recently used)
        self.cache.move_to_end(key)
        logger.info(f"[PersistentLRUCache]: Get key: {key}")
        return self.cache[key]

    def set(self, key, value):
        if key in self.cache:
            # Update existing key and move to end
            self.cache.move_to_end(key)
        self.cache[key] = value
        self.cleanup()
        logger.info(f"[PersistentLRUCache]: Set key: {key}")

    def delete(self, key):
        if key in self.cache:
            del self.cache[key]
            logger.info(f"[PersistentLRUCache]: Deleted key: {key}")
        else:
            logger.info(f"[PersistentLRUCache]: Key not found: {key}")

    def clear(self):
        self.cache.clear()
        logger.info("[PersistentLRUCache]: Cache cleared")

    def cleanup(self):
        """Remove all expired entries from cache."""
        while len(self.cache) > self.max_size:
            self.cache.popitem(last=False)


def cache_sync_wrapper(
    cache: TTLCache | LRUCache, func: Callable[T_ParamSpec, T_Retval], key: str | None = None
) -> Callable[T_ParamSpec, T_Retval]:
    @wraps(wrapped=func)
    def sync_wrapper(*args: T_ParamSpec.args, **kwargs: T_ParamSpec.kwargs) -> T_Retval:
        cache_key = key if key is not None else f"{get_callable_info(func)}-{args}-{kwargs}"
        cache_result = cache.get(cache_key)
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
        if cache_result is not None:
            return cache_result

        result = await func(*args, **kwargs)
        cache.set(cache_key, result)
        return result

    return async_wrapper
