import os

import pytest


@pytest.fixture
def tmp_cache_dir(tmp_path):
    """Provide a temporary directory for cache file tests."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    return cache_dir


@pytest.fixture
def reset_env():
    """Ensure DISABLE_CACHE env var is unset after test."""
    yield
    os.environ.pop("DISABLE_CACHE", None)
