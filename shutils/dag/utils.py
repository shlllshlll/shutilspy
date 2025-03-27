#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
File: utils.py
Author: shlll(shlll7347@gmail.com)
Modified By: shlll(shlll7347@gmail.com)
Brief:
"""

import asyncio
import threading
from typing import Coroutine
import concurrent.futures

def get_loop_safe_runner(coro: Coroutine) -> asyncio.Future | concurrent.futures.Future:
    """根据当前线程决定如何运行协程"""
    try:
        loop = asyncio.get_running_loop()
        # 如果能获取到当前运行的事件循环，说明是在主线程中
        if threading.current_thread() is threading.main_thread():
            # 在主线程事件循环中，可以直接创建一个任务
            return loop.create_task(coro)
        else:
            # 在子线程中，需要使用run_coroutine_threadsafe
            return asyncio.run_coroutine_threadsafe(coro, loop)
    except RuntimeError:
        # 如果没有运行中的事件循环，可能是在另一个线程中
        # 这种情况下可能需要特殊处理
        raise RuntimeError("No running event loop - cannot run coroutine")
