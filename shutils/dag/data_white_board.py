"""Thread-safe and async-safe data whiteboard for sharing data between DAG tasks."""

import copy
import logging
from collections.abc import Callable
from contextlib import asynccontextmanager
from typing import Any

from ..rwlock import AsyncRWLock, RWLock

__all__ = [
    "AsyncDataWhiteBoard",
    "DataWhiteBoardMixin",
    "SyncDataWhiteBoard",
]


logger = logging.getLogger(__name__)

class DataWhiteBoardMixin:
    """Mixin providing a thread-safe and async-safe key-value data store."""

    def __init__(self):
        """Initialize the whiteboard with sync and async locks and an empty data dict."""
        self._sync_lock = RWLock()
        self._async_lock = AsyncRWLock()
        self._data = {}
        self._sync_white_board = None
        self._async_white_board = None

    @property
    def sync_white_board(self):
        """Lazy accessor for the sync data whiteboard."""
        if self._sync_white_board is None:
            self._sync_white_board = SyncDataWhiteBoard(self)
        return self._sync_white_board

    @property
    def async_white_board(self):
        """Lazy accessor for the async data whiteboard."""
        if self._async_white_board is None:
            self._async_white_board = AsyncDataWhiteBoard(self)
        return self._async_white_board

    def __repr__(self):
        return f"DataWhiteBoard({self._data.keys()})"


class SyncDataWhiteBoard:
    """Synchronous thread-safe interface for the data whiteboard."""

    def __init__(self, data_white_board: DataWhiteBoardMixin):
        """Initialize with the underlying whiteboard mixin.

        Args:
            data_white_board: The mixin instance to wrap.
        """
        self.__data_white_board = data_white_board

    def __setitem__(self, key, value):
        with self.__data_white_board._sync_lock.write():
            self.__data_white_board._data[key] = value

    def __getitem__(self, key):
        with self.__data_white_board._sync_lock.read():
            return self.__data_white_board._data[key]

    def __contains__(self, key):
        with self.__data_white_board._sync_lock.read():
            return key in self.__data_white_board._data

    def __len__(self):
        with self.__data_white_board._sync_lock.read():
            return len(self.__data_white_board._data)

    def __iter__(self):
        with self.__data_white_board._sync_lock.read():
            yield from iter(self.__data_white_board._data)

    def __bool__(self):
        with self.__data_white_board._sync_lock.read():
            return bool(self.__data_white_board._data)

    def __delitem__(self, key):
        with self.__data_white_board._sync_lock.write():
            del self.__data_white_board._data[key]

    def set_data(self, **kwargs):
        """Set multiple key-value pairs at once."""
        with self.__data_white_board._sync_lock.write():
            self.__data_white_board._data.update(kwargs)

    def keys(self):
        """Iterate over data keys under a read lock."""
        with self.__data_white_board._sync_lock.read():
            yield from self.__data_white_board._data.keys()

    def values(self):
        """Iterate over data values under a read lock."""
        with self.__data_white_board._sync_lock.read():
            yield from self.__data_white_board._data.values()

    def items(self):
        """Iterate over data items under a read lock."""
        with self.__data_white_board._sync_lock.read():
            yield from self.__data_white_board._data.items()

    def get(self, key, default=None) -> Any:
        """Get a value by key with an optional default."""
        with self.__data_white_board._sync_lock.read():
            return self.__data_white_board._data.get(key, default)

    def rlock(self):
        """Acquire a sync read lock context manager."""
        return self.__data_white_board._sync_lock.read()

    def wlock(self):
        """Acquire a sync write lock context manager."""
        return self.__data_white_board._sync_lock.write()

    def copy(self, new_white_board: "DataWhiteBoardMixin", deep_copy: bool = False):
        """Copy data to another whiteboard, optionally deep-copying.

        Args:
            new_white_board: The target whiteboard mixin.
            deep_copy: Whether to deep-copy data instead of sharing references.
        """
        with self.__data_white_board._sync_lock.read():
            if deep_copy:
                new_white_board._data = copy.deepcopy(self.__data_white_board._data)
            else:
                new_white_board._sync_lock = self.__data_white_board._sync_lock
                new_white_board._async_lock = self.__data_white_board._async_lock
                new_white_board._data = self.__data_white_board._data


class AsyncDataWhiteBoard:
    """Asynchronous interface for the data whiteboard with async lock support."""

    async def read_wrapper[T](self, func: Callable[..., T], *args, **kwargs):
        """Execute a function under an async read lock."""
        async with self.__data_white_board._async_lock.read():
            return func(*args, **kwargs)

    async def write_wrapper[T](self, func: Callable[..., T], *args, **kwargs):
        """Execute a function under an async write lock."""
        async with self.__data_white_board._async_lock.write():
            return func(*args, **kwargs)

    def __init__(self, data_white_board: DataWhiteBoardMixin):
        """Initialize with the underlying whiteboard mixin.

        Args:
            data_white_board: The mixin instance to wrap.
        """
        self.__data_white_board = data_white_board

    def set_item(self, key, value):
        """Set a key-value pair via async write wrapper."""
        return self.write_wrapper(self.__data_white_board.sync_white_board.__setitem__, key, value)

    def get_item(self, key):
        """Get a value by key via async read wrapper."""
        return self.read_wrapper(self.__data_white_board.sync_white_board.__getitem__, key)

    def contains(self, key):
        """Check if a key exists via async read wrapper."""
        return self.read_wrapper(self.__data_white_board.sync_white_board.__contains__, key)

    def len(self):
        """Get the number of items via async read wrapper."""
        return self.read_wrapper(self.__data_white_board.sync_white_board.__len__)

    def iter(self):
        """Iterate over keys via async read wrapper."""
        return self.read_wrapper(self.__data_white_board.sync_white_board.__iter__)

    def repr(self):
        """Get string representation via async read wrapper."""
        return self.read_wrapper(self.__data_white_board.sync_white_board.__repr__)

    def bool(self):
        """Check if the whiteboard is non-empty via async read wrapper."""
        return self.read_wrapper(self.__data_white_board.sync_white_board.__bool__)

    def del_item(self, key):
        """Delete a key via async write wrapper."""
        return self.write_wrapper(self.__data_white_board.sync_white_board.__delitem__, key)

    def set_data(self, **kwargs):
        """Set multiple key-value pairs via async write wrapper."""
        return self.write_wrapper(self.__data_white_board.sync_white_board.set_data, **kwargs)

    def keys(self):
        """Iterate over keys via async read wrapper."""
        return self.read_wrapper(self.__data_white_board.sync_white_board.keys)

    def values(self):
        """Iterate over values via async read wrapper."""
        return self.read_wrapper(self.__data_white_board.sync_white_board.values)

    def items(self):
        """Iterate over items via async read wrapper."""
        return self.read_wrapper(self.__data_white_board.sync_white_board.items)

    def get(self, key, default=None):
        """Get a value by key with an optional default via async read wrapper."""
        return self.read_wrapper(self.__data_white_board.sync_white_board.get, key, default)

    @asynccontextmanager
    async def rlock(self):
        """Acquire both async and sync read locks."""
        async with self.__data_white_board._async_lock.read():
            with self.__data_white_board._sync_lock.read():
                yield

    @asynccontextmanager
    async def wlock(self):
        """Acquire both async and sync write locks."""
        async with self.__data_white_board._async_lock.write():
            with self.__data_white_board._sync_lock.write():
                yield

    async def copy(self, new_white_board: "DataWhiteBoardMixin", deep_copy: bool = False):
        """Async copy data to another whiteboard.

        Args:
            new_white_board: The target whiteboard mixin.
            deep_copy: Whether to deep-copy data instead of sharing references.
        """
        async with self.__data_white_board._async_lock.read():
            with self.__data_white_board._sync_lock.read():
                if deep_copy:
                    new_white_board._data = copy.deepcopy(self.__data_white_board._data)
                else:
                    new_white_board._sync_lock = self.__data_white_board._sync_lock
                    new_white_board._async_lock = self.__data_white_board._async_lock
                    new_white_board._data = self.__data_white_board._data
