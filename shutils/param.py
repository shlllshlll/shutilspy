"""Dataclass serialization utilities with support for hidden fields and type coercion."""

import importlib
import json
from dataclasses import fields, is_dataclass
from enum import Enum
from types import UnionType
from typing import Any, ForwardRef, TypeVar, Union, get_args, get_origin, get_type_hints

__all__ = [
    "HIDE",
    "Hidden",
    "Hide",
    "OptionHidden",
    "ParamMixin",
    "asdict",
    "asjson",
    "deref_forwardref",
    "deref_typestr",
    "dict_to_dataclass",
    "json_serializer",
]


class Hide:
    """Sentinel value to mark fields as hidden during serialization."""
    def __repr__(self) -> str:
        return "<HIDE>"


HIDE = Hide()

T = TypeVar("T")
Hidden = T | Hide
OptionHidden = T | Hide | None


def json_serializer(obj: Any) -> Any:
    """JSON serializer that handles Enum values.

    Args:
        obj: Object to serialize.

    Returns:
        The enum's value.

    Raises:
        TypeError: If obj is not an Enum.
    """
    if isinstance(obj, Enum):
        return obj.value
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

class ParamMixin:
    """Mixin for dataclasses that provides JSON string serialization."""

    def to_json_str(self):
        """Serialize the dataclass to a JSON string.

        Returns:
            A JSON string representation of the dataclass.
        """
        return json.dumps(obj=asdict(self), ensure_ascii=False, default=json_serializer)


def deref_forwardref(forward_ref: ForwardRef, module_name: str) -> Any:
    """Resolve a ForwardRef to its actual type.

    Args:
        forward_ref: The ForwardRef to resolve.
        module_name: The module name to resolve in.

    Returns:
        The resolved type.
    """
    def tmp() -> forward_ref:
        ...

    res =  get_type_hints(tmp, vars(importlib.import_module(module_name)))["return"]
    return res

def deref_typestr(type_str: str, module_name: str) -> Any:
    """Resolve a dotted type string to its actual type.

    Args:
        type_str: Dotted path like "module.ClassName".
        module_name: The base module to import from.

    Returns:
        The resolved type.
    """
    module = importlib.import_module(module_name)
    parts = type_str.split(".")
    obj = module
    for part in parts:
        obj = getattr(obj, part)
    return obj

def asjson(obj: Any, skip_private: bool = False, *args, **kwargs) -> str:
    """Serialize a dataclass to a JSON string.

    Args:
        obj: The dataclass instance to serialize.
        skip_private: If True, skip fields starting with underscore.
        *args: Extra positional arguments forwarded to ``json.dumps``.
        **kwargs: Extra keyword arguments forwarded to ``json.dumps``.

    Returns:
        A JSON string representation of the dataclass.
    """
    return json.dumps(asdict(obj, skip_private), *args, default=json_serializer, **kwargs)

def asdict(obj: Any, skip_private: bool = False) -> dict:
    """Convert a dataclass to a dict, handling nested dataclasses, enums, and Hide fields.

    Fields whose value is an instance of ``Hide`` are omitted from the output.

    Args:
        obj: The dataclass instance to convert.
        skip_private: If True, skip fields whose names start with underscore.

    Returns:
        A dictionary representation of the dataclass.

    Raises:
        ValueError: If obj is not a dataclass.
    """
    def asdict_internal(obj: Any) -> Any:
        if not is_dataclass(obj):
            if isinstance(obj, (list, tuple, set)):
                result = []
                for item in obj:
                    result.append(asdict_internal(item))
                result = type(obj)(result)
            elif isinstance(obj, dict):
                result = {}
                for k, v in obj.items():
                    result[k] = asdict_internal(v)
                result = type(obj)(result)
            elif type(obj) in set((str, bool, type(None), int, float)):
                result = obj
            elif isinstance(obj, Enum):
                result = obj.value
            else:
                raise ValueError(f"Unsupported type[{type(obj)}, {obj}]")
        else:
            result = {}
            for field_item in fields(obj):
                key = field_item.name
                if skip_private and key.startswith("_"):
                    continue
                value = getattr(obj, key)
                if isinstance(value, Hide):
                    continue
                result[key] = asdict_internal(value)
        return result

    if not is_dataclass(obj):
        raise ValueError("Not a dataclass")

    return asdict_internal(obj)


def dict_to_dataclass[T](data: dict[str, Any], cls: type[T]) -> T:
    """Convert a dictionary to a dataclass instance with type coercion.

    Handles ForwardRef resolution, Union types, nested dataclasses,
    enums, and generic containers (list, dict, tuple).

    Args:
        data: Dictionary with field values.
        cls: The target dataclass type.

    Returns:
        An instance of ``cls`` populated from ``data``.
    """
    def process_value(field_type: Any, value: Any, strict: bool = False) -> Any:
        if isinstance(field_type, ForwardRef):
            field_type = deref_forwardref(field_type, cls.__module__)
        elif isinstance(field_type, str):
            field_type = deref_typestr(field_type, cls.__module__)

        origin = get_origin(field_type)
        args = get_args(field_type)

        if origin in (Union, UnionType):
            for item_type in args:
                try:
                    return process_value(item_type, value, strict=True)
                except Exception:
                    continue
            for item_type in args:
                try:
                    return process_value(item_type, value, strict=False)
                except Exception:
                    continue
            raise ValueError(f"Convert value[{value}] to type[{field_type}] failed")
        elif origin is list:
            arg_type = args[0]
            result = []
            for item in value:
                result.append(process_value(arg_type, item))
            return result
        elif origin is dict:
            key_type = args[0]
            value_type = args[1]
            result = {}
            for k, v in value.items():
                result[process_value(key_type, k)] = process_value(value_type, v)
            return result
        elif origin is tuple:
            result = []

            if len(args) != len(value):
                raise ValueError(
                    f"Tuple length mismatch: expected {len(args)}, got {len(value)}"
                )

            for arg_type, arg_item in zip(args, value, strict=False):
                result.append(process_value(arg_type, arg_item))
            return tuple(result)
        else:
            if is_dataclass(field_type):
                return dict_to_dataclass(value, field_type)
            elif issubclass(field_type, Enum):
                return field_type(value)
            elif field_type == Any:
                return value
            else:
                if isinstance(value, field_type):
                    return value
                else:
                    if strict:
                        raise ValueError(
                            f"Value type mismatch: expected {field_type}, got {type(value)}"
                        )
                    return type(value)

    field_values = {}

    for field_item in fields(cls):
        if field_item.name in data:
            value = data[field_item.name]
            field_values[field_item.name] = process_value(field_item.type, value)

    return cls(**field_values)

