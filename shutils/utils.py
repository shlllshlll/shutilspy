#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
File: utils.py
Author: shlll(shlll7347@gmail.com)
Modified By: shlll(shlll7347@gmail.com)
Brief:
"""

import hashlib
import inspect
from typing import Any, Type, TypeVar
from pathlib import Path

T = TypeVar('T')

def singleton(cls):
    """单例装饰器"""
    instances = {}

    def get_instances(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]

    return get_instances

def static_vars(**kwargs):
    """定义函数内静态变量的修饰器"""
    def decorate(func):
        for k in kwargs:
            setattr(func, k, kwargs[k])
        return func
    return decorate

def get_callable_info(callable_obj):
    module_name = callable_obj.__module__
    function_name = callable_obj.__name__
    class_name = None

    if hasattr(callable_obj, '__self__') and callable_obj.__self__ is not None:
        class_name = callable_obj.__self__.__class__.__name__
    elif hasattr(callable_obj, '__qualname__'):
        parts = callable_obj.__qualname__.split('.')
        if len(parts) > 1:
            class_name = parts[-2]

    if class_name:
        return f"{module_name}.{class_name}.{function_name}"
    else:
        return f"{module_name}.{function_name}"

def get_caller_class():
    return inspect.stack()[1].frame.f_locals.get('self', None).__class__

@static_vars(subclassdict={})
def get_class(cls: Type[T], name: str) -> Type[T] | None:
    def find_subclasses(cls: Type[Any]):
        subclasses = set(cls.__subclasses__())
        for subclass in subclasses.copy():
            subclasses.update(find_subclasses(subclass))
        return subclasses

    if cls not in get_class.subclassdict:
        subclasses_set = find_subclasses(cls)
        subclasses_dict = {}
        for subclass in subclasses_set:
            subclasses_dict[subclass.__name__] = subclass
        get_class.subclassdict[cls] = subclasses_dict
    else:
        subclasses_dict = get_class.subclassdict[cls]

    if name in subclasses_dict:
        return subclasses_dict[name]
    else:
        return None

def calculate_md5(file_path: str | Path, buffer_size: int=65536) -> str:
    md5 = hashlib.md5()
    
    with open(file_path, 'rb') as f:
        while True:
            data = f.read(buffer_size)
            if not data:
                break
            md5.update(data)
    
    return md5.hexdigest()
