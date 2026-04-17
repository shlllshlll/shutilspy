import json
from dataclasses import dataclass, field
from enum import Enum

from shutils.param import (
    HIDE,
    Hide,
    ParamMixin,
    asdict,
    asjson,
    deref_forwardref,
    deref_typestr,
    dict_to_dataclass,
    json_serializer,
)


class Color(Enum):
    RED = "red"
    BLUE = "blue"


@dataclass
class Inner:
    x: int = 1


@dataclass
class Outer:
    name: str = "test"
    inner: Inner = field(default_factory=Inner)
    color: Color = Color.RED
    _private: str = "hidden"


@dataclass
class WithHide:
    name: str = "visible"
    secret: Hide | str = HIDE


class TestHide:
    def test_repr(self):
        assert repr(HIDE) == "<HIDE>"

    def test_is_instance(self):
        assert isinstance(HIDE, Hide)


class TestAsdict:
    def test_simple_dataclass(self):
        @dataclass
        class Simple:
            x: int = 1
            y: str = "hello"

        result = asdict(Simple())
        assert result == {"x": 1, "y": "hello"}

    def test_nested_dataclass(self):
        obj = Outer()
        result = asdict(obj)
        assert result == {"name": "test", "inner": {"x": 1}, "color": "red", "_private": "hidden"}

    def test_skip_private(self):
        obj = Outer()
        result = asdict(obj, skip_private=True)
        assert "_private" not in result

    def test_hide_field_excluded(self):
        obj = WithHide()
        result = asdict(obj)
        assert "secret" not in result
        assert "name" in result

    def test_enum_converted_to_value(self):
        @dataclass
        class WithEnum:
            color: Color = Color.BLUE

        result = asdict(WithEnum())
        assert result == {"color": "blue"}

    def test_list_field(self):
        @dataclass
        class WithList:
            items: list = field(default_factory=list)

        result = asdict(WithList(items=[1, 2, 3]))
        assert result == {"items": [1, 2, 3]}

    def test_not_dataclass_raises(self):
        import pytest
        with pytest.raises(ValueError):
            asdict("not a dataclass")


class TestAsjson:
    def test_basic(self):
        @dataclass
        class Simple:
            x: int = 1

        result = asjson(Simple())
        assert json.loads(result) == {"x": 1}

    def test_with_skip_private(self):
        obj = Outer()
        result = asjson(obj, skip_private=True)
        parsed = json.loads(result)
        assert "_private" not in parsed


class TestDictToDataclass:
    def test_simple(self):
        @dataclass
        class Simple:
            x: int = 0
            y: str = ""

        result = dict_to_dataclass({"x": 1, "y": "hello"}, Simple)
        assert result.x == 1
        assert result.y == "hello"

    def test_nested(self):
        result = dict_to_dataclass({"name": "test", "inner": {"x": 42}}, Outer)
        assert result.name == "test"
        assert result.inner.x == 42

    def test_enum(self):
        @dataclass
        class WithEnum:
            color: Color = Color.RED

        result = dict_to_dataclass({"color": "blue"}, WithEnum)
        assert result.color == Color.BLUE

    def test_list_type(self):
        @dataclass
        class WithList:
            items: list[int] = field(default_factory=list)

        result = dict_to_dataclass({"items": [1, 2, 3]}, WithList)
        assert result.items == [1, 2, 3]

    def test_union_type(self):
        @dataclass
        class WithUnion:
            value: int | str = 0

        result = dict_to_dataclass({"value": 42}, WithUnion)
        assert result.value == 42

    def test_dict_type(self):
        @dataclass
        class WithDict:
            mapping: dict[str, int] = field(default_factory=dict)

        result = dict_to_dataclass({"mapping": {"a": 1}}, WithDict)
        assert result.mapping == {"a": 1}


class TestDerefForwardref:
    def test_basic(self):
        from typing import ForwardRef
        ref = ForwardRef("Color")
        result = deref_forwardref(ref, __name__)
        assert result is Color


class TestDerefTypestr:
    def test_basic(self):
        result = deref_typestr("Color", __name__)
        assert result is Color


class TestParamMixin:
    def test_to_json_str(self):
        @dataclass
        class MyParam(ParamMixin):
            x: int = 1

        result = MyParam().to_json_str()
        assert json.loads(result) == {"x": 1}


class TestJsonSerializer:
    def test_enum(self):
        result = json_serializer(Color.RED)
        assert result == "red"

    def test_unsupported_raises(self):
        import pytest
        with pytest.raises(TypeError):
            json_serializer(object())
