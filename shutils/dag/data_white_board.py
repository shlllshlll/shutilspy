#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
File: data_white_board.py
Author: shlll(shlll7347@gmail.com)
Modified By: shlll(shlll7347@gmail.com)
Brief:
"""

from contextlib import asynccontextmanager, contextmanager
import copy
import logging
from typing import Callable
from ..rwlock import RWLock, AsyncRWLock


logger = logging.getLogger(__name__)

class DataWhiteBoardMixin:
    """
    DataWhiteBoard is a class that provides a whiteboard for _data sharing between tasks.
    """

    def __init__(self):
        self._sync_lock = RWLock()
        self._async_lock = AsyncRWLock()
        self._data = {}
        self._sync_white_board = None
        self._async_white_board = None

    @property
    def sync_white_board(self):
        if self._sync_white_board is None:
            self._sync_white_board = SyncDataWhiteBoard(self)
        return self._sync_white_board

    @property
    def async_white_board(self):
        if self._async_white_board is None:
            self._async_white_board = AsyncDataWhiteBoard(self)
        return self._async_white_board

    def __repr__(self):
        return f"DataWhiteBoard({self._data.keys()})"


class SyncDataWhiteBoard:
    def __init__(self, data_white_board: DataWhiteBoardMixin):
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

    @contextmanager
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
        with self.__data_white_board._sync_lock.write():
            self.__data_white_board._data.update(kwargs)

    @contextmanager
    def keys(self):
        with self.__data_white_board._sync_lock.read():
            yield from self.__data_white_board._data.keys()

    @contextmanager
    def values(self):
        with self.__data_white_board._sync_lock.read():
            yield from self.__data_white_board._data.values()

    @contextmanager
    def items(self):
        with self.__data_white_board._sync_lock.read():
            yield from self.__data_white_board._data.items()

    def get(self, key, default=None):
        with self.__data_white_board._sync_lock.read():
            return self.__data_white_board._data.get(key, default)

    def rlock(self):
        return self.__data_white_board._sync_lock.read()

    def wlock(self):
        return self.__data_white_board._sync_lock.write()

    def copy(self, new_white_board: "DataWhiteBoardMixin", deep_copy: bool = False):
        with self.__data_white_board._sync_lock.read():
            if deep_copy:
                new_white_board._data = copy.deepcopy(self.__data_white_board._data)
            else:
                new_white_board._sync_lock = self.__data_white_board._sync_lock
                new_white_board._async_lock = self.__data_white_board._async_lock
                new_white_board._data = self.__data_white_board._data


class AsyncDataWhiteBoard:
    async def read_wrapper[T](self, func: Callable[..., T], *args, **kwargs):
        async with self.__data_white_board._async_lock.read():
            return func(*args, **kwargs)

    async def write_wrapper[T](self, func: Callable[..., T], *args, **kwargs):
        async with self.__data_white_board._async_lock.write():
            return func(*args, **kwargs)

    def __init__(self, data_white_board: DataWhiteBoardMixin):
        self.__data_white_board = data_white_board

    def set_item(self, key, value):
        return self.write_wrapper(self.__data_white_board.sync_white_board.__setitem__, key, value)

    def get_item(self, key):
        return self.read_wrapper(self.__data_white_board.sync_white_board.__getitem__, key)

    def contains(self, key):
        return self.read_wrapper(self.__data_white_board.sync_white_board.__contains__, key)

    def len(self):
        return self.read_wrapper(self.__data_white_board.sync_white_board.__len__)

    def iter(self):
        return self.read_wrapper(self.__data_white_board.sync_white_board.__iter__)

    def repr(self):
        return self.read_wrapper(self.__data_white_board.sync_white_board.__repr__)

    def bool(self):
        return self.read_wrapper(self.__data_white_board.sync_white_board.__bool__)

    def del_item(self, key):
        return self.write_wrapper(self.__data_white_board.sync_white_board.__delitem__, key)

    def set_data(self, **kwargs):
        return self.write_wrapper(self.__data_white_board.sync_white_board.set_data, **kwargs)

    def keys(self):
        return self.read_wrapper(self.__data_white_board.sync_white_board.keys)

    def values(self):
        return self.read_wrapper(self.__data_white_board.sync_white_board.values)

    def items(self):
        return self.read_wrapper(self.__data_white_board.sync_white_board.items)

    def get(self, key, default=None):
        return self.read_wrapper(self.__data_white_board.sync_white_board.get, key, default)

    @asynccontextmanager
    async def rlock(self):
        async with self.__data_white_board._async_lock.read():
            with self.__data_white_board._sync_lock.read():
                yield

    @asynccontextmanager
    async def wlock(self):
        async with self.__data_white_board._async_lock.write():
            with self.__data_white_board._sync_lock.write():
                yield

    async def copy(self, new_white_board: "DataWhiteBoardMixin", deep_copy: bool = False):
        async with self.__data_white_board._async_lock.read():
            with self.__data_white_board._sync_lock.read():
                if deep_copy:
                    new_white_board._data = copy.deepcopy(self.__data_white_board._data)
                else:
                    new_white_board._sync_lock = self.__data_white_board._sync_lock
                    new_white_board._async_lock = self.__data_white_board._async_lock
                    new_white_board._data = self.__data_white_board._data
