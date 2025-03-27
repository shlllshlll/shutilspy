#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
File: rwlock.py
Author: shlll(shlll7347@gmail.com)
Modified By: shlll(shlll7347@gmail.com)
Brief:
"""

import threading
import asyncio


class RWLock(object):
    def __init__(self):
        self._read_ready = threading.Condition(threading.Lock())
        self._readers = 0

    def read(self):
        """返回一个上下文管理器用于获取和释放读锁。"""
        return self.ReadLock(self)

    def write(self):
        """返回一个上下文管理器用于获取和释放写锁。"""
        return self.WriteLock(self)

    class ReadLock:
        def __init__(self, rwlock):
            self.rwlock = rwlock

        def __enter__(self):
            """进入上下文时自动获取读锁。"""
            self.rwlock._acquire_read()

        def __exit__(self, exc_type, exc_val, exc_tb):
            """退出上下文时自动释放读锁。"""
            self.rwlock._release_read()

    class WriteLock:
        def __init__(self, rwlock):
            self.rwlock = rwlock

        def __enter__(self):
            """进入上下文时自动获取写锁。"""
            self.rwlock._acquire_write()

        def __exit__(self, exc_type, exc_val, exc_tb):
            """退出上下文时自动释放写锁。"""
            self.rwlock._release_write()

    def _acquire_read(self):
        with self._read_ready:
            self._readers += 1

    def _release_read(self):
        with self._read_ready:
            self._readers -= 1
            if self._readers == 0:
                self._read_ready.notify_all()

    def _acquire_write(self):
        self._read_ready.acquire()
        while self._readers > 0:
            self._read_ready.wait()

    def _release_write(self):
        self._read_ready.release()


class AsyncRWLock:
    def __init__(self):
        self._read_ready = asyncio.Condition()
        self._readers = 0

    def read(self):
        """返回一个上下文管理器用于获取和释放读锁。"""
        return self.ReadLock(self)

    def write(self):
        """返回一个上下文管理器用于获取和释放写锁。"""
        return self.WriteLock(self)

    class ReadLock:
        def __init__(self, rwlock):
            self.rwlock = rwlock

        async def __aenter__(self):
            """进入上下文时自动获取读锁。"""
            await self.rwlock._acquire_read()

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            """退出上下文时自动释放读锁。"""
            await self.rwlock._release_read()

    class WriteLock:
        def __init__(self, rwlock):
            self.rwlock = rwlock

        async def __aenter__(self):
            """进入上下文时自动获取写锁。"""
            await self.rwlock._acquire_write()

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            """退出上下文时自动释放写锁。"""
            await self.rwlock._release_write()

    async def _acquire_read(self):
        async with self._read_ready:
            self._readers += 1

    async def _release_read(self):
        async with self._read_ready:
            self._readers -= 1
            if self._readers == 0:
                self._read_ready.notify_all()

    async def _acquire_write(self):
        await self._read_ready.acquire()
        try:
            while self._readers > 0:
                await self._read_ready.wait()
        except:
            self._read_ready.release()
            raise

    async def _release_write(self):
        self._read_ready.release()
