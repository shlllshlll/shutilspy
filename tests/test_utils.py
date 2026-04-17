import threading
from pathlib import Path

from shutils.utils import (
    SingletonMeta,
    calculate_md5,
    get_callable_info,
    get_caller_class,
    get_class,
    singleton,
    static_vars,
)

# --- singleton decorator tests ---

class TestSingleton:
    def test_no_parens(self):
        """@singleton without parentheses."""
        @singleton
        class Foo:
            pass

        assert Foo() is Foo()

    def test_with_parens(self):
        """@singleton() with parentheses."""
        @singleton()
        class Bar:
            pass

        assert Bar() is Bar()

    def test_ignore_args_true(self):
        """singleton with ignore_args=True returns same instance regardless of args."""
        @singleton(ignore_args=True)
        class Baz:
            def __init__(self, val=0):
                self.val = val

        a = Baz(1)
        b = Baz(2)
        assert a is b
        assert a.val == 1  # first call wins

    def test_ignore_args_false(self):
        """singleton with ignore_args=False returns different instances for different args."""
        @singleton(ignore_args=False)
        class Qux:
            def __init__(self, val=0):
                self.val = val

        a = Qux(1)
        b = Qux(2)
        assert a is not b
        assert a.val == 1
        assert b.val == 2

    def test_ignore_args_false_same_args(self):
        """singleton with ignore_args=False returns same instance for same args."""
        @singleton(ignore_args=False)
        class Quux:
            def __init__(self, val=0):
                self.val = val

        a = Quux(1)
        b = Quux(1)
        assert a is b

    def test_thread_safety(self):
        """singleton is thread-safe."""
        @singleton
        class ThreadSafe:
            pass

        results = []

        def create():
            results.append(ThreadSafe())

        threads = [threading.Thread(target=create) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert all(r is results[0] for r in results)


# --- SingletonMeta tests ---

class TestSingletonMeta:
    def test_basic_singleton(self):
        class MySingleton(metaclass=SingletonMeta):
            pass

        assert MySingleton() is MySingleton()

    def test_different_classes_independent(self):
        class A(metaclass=SingletonMeta):
            pass

        class B(metaclass=SingletonMeta):
            pass

        assert A() is not B()
        assert A() is A()
        assert B() is B()


# --- static_vars tests ---

class TestStaticVars:
    def test_adds_attributes(self):
        @static_vars(counter=0, name="test")
        def my_func():
            return my_func.counter

        assert my_func.counter == 0
        assert my_func.name == "test"

    def test_mutable_static_var(self):
        @static_vars(data=[])
        def append_item(item):
            append_item.data.append(item)
            return append_item.data

        assert append_item("a") == ["a"]
        assert append_item("b") == ["a", "b"]


# --- get_callable_info tests ---

class TestGetCallableInfo:
    def test_module_function(self):
        def my_func():
            pass

        result = get_callable_info(my_func)
        assert "my_func" in result

    def test_method(self):
        class MyClass:
            def my_method(self):
                pass

        obj = MyClass()
        result = get_callable_info(obj.my_method)
        assert "MyClass" in result
        assert "my_method" in result


# --- get_caller_class tests ---

class TestGetCallerClass:
    def test_returns_caller_class(self):
        class Outer:
            def call(self):
                return get_caller_class()

        obj = Outer()
        assert obj.call() is Outer


# --- get_class tests ---

class TestGetClass:
    def test_finds_subclass(self):
        class Base:
            pass

        class Child(Base):
            pass

        result = get_class(Base, "Child")
        assert result is Child

    def test_not_found(self):
        class Base:
            pass

        result = get_class(Base, "NonExistent")
        assert result is None


# --- calculate_md5 tests ---

class TestCalculateMd5:
    def test_known_content(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_text("hello world")
        result = calculate_md5(str(f))
        assert isinstance(result, str)
        assert len(result) == 32  # MD5 hex digest length

    def test_path_object(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_text("hello world")
        result = calculate_md5(Path(f))
        assert isinstance(result, str)

    def test_empty_file(self, tmp_path):
        f = tmp_path / "empty.txt"
        f.write_bytes(b"")
        result = calculate_md5(str(f))
        assert result == "d41d8cd98f00b204e9800998ecf8427e"  # MD5 of empty string
