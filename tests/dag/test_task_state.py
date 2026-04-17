from shutils.dag.task_state import ErrorInfo, TaskStateMixin


class TestErrorInfo:
    def test_defaults(self):
        info = ErrorInfo()
        assert info.has_error is False
        assert info.exception is None
        assert info.error_node is None

    def test_with_values(self):
        exc = ValueError("test error")
        info = ErrorInfo(has_error=True, exception=exc, error_node="task1")
        assert info.has_error is True
        assert info.exception is exc
        assert info.error_node == "task1"


class TestTaskStateMixin:
    def test_init(self):
        state = TaskStateMixin()
        assert state.is_destory() is False
        assert len(state.available_tasks) == 0

    def test_set_destory(self):
        state = TaskStateMixin()
        state.set_destory(True)
        assert state.is_destory() is True

    def test_repr(self):
        state = TaskStateMixin()
        assert "destory=False" in repr(state)

    def test_error_info_property(self):
        state = TaskStateMixin()
        assert state.error_info.has_error is False
        state.error_info = ErrorInfo(has_error=True)
        assert state.error_info.has_error is True
