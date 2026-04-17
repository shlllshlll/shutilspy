import pytest

from shutils.dag.helper import get_callable_func, get_params


class TestGetCallableFunc:
    def test_module_function(self):
        func = get_callable_func("os.path.join")
        assert callable(func)

    def test_not_callable_raises(self):
        with pytest.raises(ValueError, match=r"not valid|not callable"):
            get_callable_func("os.path")  # module, not callable


class TestGetParams:
    def test_simple_dict(self):
        result = get_params({"a": 1, "b": "hello"})
        assert result == {"a": 1, "b": "hello"}

    def test_eval_values(self):
        result = get_params({"a": "eval(1+2)"})
        assert result == {"a": 3}

    def test_invalid_type_raises(self):
        with pytest.raises(Exception, match="Invalid params type"):
            get_params("not a dict")
