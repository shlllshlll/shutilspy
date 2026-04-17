"""Utility functions for singleton patterns, static variables, and hashing."""

import hashlib
import inspect
import threading
from functools import wraps
from pathlib import Path
from typing import Any, TypeVar

__all__ = [
    "SingletonMeta",
    "calculate_md5",
    "get_callable_info",
    "get_caller_class",
    "get_class",
    "singleton",
    "static_vars",
]

T = TypeVar('T')


def singleton(cls=None, *, ignore_args=True):
    """Decorator that turns a class into a singleton.

    Supports both ``@singleton`` (no parentheses) and
    ``@singleton(ignore_args=False)`` (with parentheses).

    Args:
        cls: The class to decorate. Automatically provided when used without parentheses.
        ignore_args: If True, all calls return the same instance regardless of arguments.
            If False, instances are keyed by arguments.

    Returns:
        The decorated class or a decorator function.
    """

    def _singleton_wrapper(target_cls):
        _instances = {}
        _instance_lock = threading.Lock()

        @wraps(target_cls)
        def wrapper(*args, **kwargs):
            if ignore_args:
                key = target_cls
            else:
                frozen_kwargs = frozenset(sorted(kwargs.items()))
                key = (args, frozen_kwargs)

            if key not in _instances:
                with _instance_lock:
                    if key not in _instances:
                        _instances[key] = target_cls(*args, **kwargs)

            return _instances[key]

        return wrapper

    if cls is None:
        return _singleton_wrapper
    else:
        return _singleton_wrapper(cls)


class SingletonMeta(type):
    """Metaclass for singleton/multiton patterns with per-class locking.

    Each class using this metaclass gets its own lock and instance store,
    so ClassA and ClassB do not contend with each other.

    Attributes:
        _instance_lock: Per-class threading lock.
        _instances: Per-class instance dictionary keyed by arguments.
    """

    def __init__(cls, name, bases, attrs):
        super().__init__(name, bases, attrs)
        cls._instance_lock = threading.Lock()
        cls._instances = {}

    def __call__(cls, *args, **kwargs):
        """Create or return the cached instance using double-checked locking."""
        ignore_args = getattr(cls, "_ignore_args", True)

        if ignore_args:
            key = "singleton_root"
        else:
            frozen_kwargs = frozenset(sorted(kwargs.items()))
            key = (args, frozen_kwargs)

        if key not in cls._instances:
            with cls._instance_lock:
                if key not in cls._instances:
                    instance = super().__call__(*args, **kwargs)
                    cls._instances[key] = instance

        return cls._instances[key]

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
def get_class[T](cls: type[T], name: str) -> type[T] | None:
    def find_subclasses(cls: type[Any]):
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
