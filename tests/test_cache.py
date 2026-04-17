import os
import time

import pytest

from shutils.cache import LRUCache, StableCacheEncoder, TTLCache, cached, make_stable_key


class TestTTLCache:
    def test_set_and_get(self):
        cache = TTLCache(ttl=60)
        cache.set("key1", "value1")
        assert cache.get("key1") == "value1"

    def test_get_missing_key(self):
        cache = TTLCache(ttl=60)
        assert cache.get("nonexistent") is None

    def test_expiry(self):
        cache = TTLCache(ttl=0.01)
        cache.set("key1", "value1")
        time.sleep(0.02)
        assert cache.get("key1") is None

    def test_custom_ttl(self):
        cache = TTLCache(ttl=60)
        cache.set("key1", "value1", ttl=0.01)
        time.sleep(0.02)
        assert cache.get("key1") is None

    def test_delete(self):
        cache = TTLCache(ttl=60)
        cache.set("key1", "value1")
        cache.delete("key1")
        assert cache.get("key1") is None

    def test_clear(self):
        cache = TTLCache(ttl=60)
        cache.set("key1", "value1")
        cache.set("key2", "value2")
        cache.clear()
        assert cache.get("key1") is None
        assert cache.get("key2") is None

    def test_cleanup(self):
        cache = TTLCache(ttl=0.01)
        cache.set("key1", "value1")
        time.sleep(0.02)
        cache.cleanup()
        assert len(cache.cache) == 0

    def test_key_hashing(self):
        cache = TTLCache(ttl=60)
        cache.set("key1", "value1")
        # Internally stored with hash, but accessible by original key
        assert cache.get("key1") == "value1"


class TestLRUCache:
    def test_set_and_get(self):
        cache = LRUCache(max_size=10)
        cache.set("key1", "value1")
        assert cache.get("key1") == "value1"

    def test_get_missing_key(self):
        cache = LRUCache(max_size=10)
        assert cache.get("nonexistent") is None

    def test_eviction(self):
        cache = LRUCache(max_size=2)
        cache.set("key1", "value1")
        cache.set("key2", "value2")
        cache.set("key3", "value3")
        # key1 should be evicted
        assert cache.get("key1") is None
        assert cache.get("key2") == "value2"
        assert cache.get("key3") == "value3"

    def test_lru_order_on_get(self):
        cache = LRUCache(max_size=2)
        cache.set("key1", "value1")
        cache.set("key2", "value2")
        cache.get("key1")  # access key1, making key2 LRU
        cache.set("key3", "value3")
        assert cache.get("key1") == "value1"  # key1 still there
        assert cache.get("key2") is None  # key2 evicted

    def test_delete(self):
        cache = LRUCache(max_size=10)
        cache.set("key1", "value1")
        cache.delete("key1")
        assert cache.get("key1") is None

    def test_clear(self):
        cache = LRUCache(max_size=10)
        cache.set("key1", "value1")
        cache.clear()
        assert cache.get("key1") is None


class TestPresistentMixin:
    def test_load_and_save(self, tmp_cache_dir):
        cache_file = str(tmp_cache_dir / "test_cache")

        # Create and save
        cache1 = TTLCache(ttl=60, cache_file=cache_file)
        cache1.set("key1", "value1")
        cache1.save_cache()

        # Load in new instance
        cache2 = TTLCache(ttl=60, cache_file=cache_file)
        assert cache2.get("key1") == "value1"

    def test_auto_save_step(self, tmp_cache_dir):
        cache_file = str(tmp_cache_dir / "step_cache")
        cache = TTLCache(ttl=60, cache_file=cache_file, save_step=2)
        cache.set("key1", "value1")
        # First write, no save yet (count=1)
        cache.set("key2", "value2")
        # Second write triggers save (count=2)

        # Verify by loading
        cache2 = TTLCache(ttl=60, cache_file=cache_file)
        assert cache2.get("key2") == "value2"

    def test_no_cache_file(self):
        cache = TTLCache(ttl=60)
        cache.set("key1", "value1")
        # Should work without file persistence
        assert cache.get("key1") == "value1"

    def test_path_suffix(self, tmp_cache_dir):
        cache_file = str(tmp_cache_dir / "mycache")
        cache = TTLCache(ttl=60, cache_file=cache_file)
        assert cache.cache_file_path.name == "mycache.pkl.xz"


class TestStableCacheEncoder:
    def test_enum(self):
        from enum import Enum

        class Color(Enum):
            RED = "red"

        encoder = StableCacheEncoder()
        assert encoder.default(Color.RED) == "red"

    def test_datetime(self):
        from datetime import datetime

        encoder = StableCacheEncoder()
        dt = datetime(2024, 1, 1, 12, 0, 0)
        result = encoder.default(dt)
        assert "2024" in result


class TestMakeStableKey:
    def test_deterministic(self):
        key1 = make_stable_key("func", (1, 2), {"a": 3})
        key2 = make_stable_key("func", (1, 2), {"a": 3})
        assert key1 == key2

    def test_different_args(self):
        key1 = make_stable_key("func", (1,), {})
        key2 = make_stable_key("func", (2,), {})
        assert key1 != key2

    def test_ignore_self(self):
        key1 = make_stable_key("func", ("self_arg", 1), {}, ignore_self=True)
        make_stable_key("func", (1,), {}, ignore_self=False)
        # With ignore_self, first arg is removed
        assert "1" in key1


class TestCached:
    def test_sync_ttl(self):
        call_count = 0

        @cached(backend="ttl", ttl=60)
        def func(x):
            nonlocal call_count
            call_count += 1
            return x * 2

        assert func(5) == 10
        assert func(5) == 10  # cached
        assert call_count == 1

    def test_sync_lru(self):
        call_count = 0

        @cached(backend="lru", max_size=10)
        def func(x):
            nonlocal call_count
            call_count += 1
            return x * 2

        assert func(5) == 10
        assert func(5) == 10  # cached
        assert call_count == 1

    async def test_async_ttl(self):
        call_count = 0

        @cached(backend="ttl", ttl=60)
        async def func(x):
            nonlocal call_count
            call_count += 1
            return x * 2

        assert await func(5) == 10
        assert await func(5) == 10
        assert call_count == 1

    def test_disable_cache_env(self, reset_env):
        call_count = 0

        @cached(backend="ttl", ttl=60)
        def func(x):
            nonlocal call_count
            call_count += 1
            return x * 2

        os.environ["DISABLE_CACHE"] = "1"
        assert func(5) == 10
        assert func(5) == 10
        assert call_count == 2  # Not cached

    def test_invalid_backend(self):
        with pytest.raises(ValueError):
            @cached(backend="invalid")
            def func():
                pass
