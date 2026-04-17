"""Runtime context counter for tracking active DAG contexts."""

from .lib.smart_lock import SmartLock

__all__ = [
    "AsyncRuntimeCounter",
    "Runtime",
    "RuntimeCounterMixin",
    "SyncRuntimeCounter",
]


class RuntimeCounterMixin:
    """Mixin providing a thread-safe counter for tracking active contexts."""

    def __init__(self):
        """Initialize the counter with sync and async interfaces."""
        self._context_counter: int = 0
        self._conter_lock = SmartLock()

        self._async_counter = None
        self._sync_counter = None

    @property
    def counter(self) -> int:
        """Current number of active contexts."""
        return self._context_counter

    @property
    def sync_counter(self) -> "SyncRuntimeCounter":
        """Lazy accessor for the synchronous counter interface."""
        if self._sync_counter is None:
            self._sync_counter = SyncRuntimeCounter(self)
        return self._sync_counter

    @property
    def async_counter(self) -> "AsyncRuntimeCounter":
        """Lazy accessor for the asynchronous counter interface."""
        if self._async_counter is None:
            self._async_counter = AsyncRuntimeCounter(self)
        return self._async_counter


class SyncRuntimeCounter:
    """Synchronous thread-safe counter for active contexts."""

    def __init__(self, runtime_counter: RuntimeCounterMixin):
        """Initialize with the underlying counter mixin.

        Args:
            runtime_counter: The mixin instance to wrap.
        """
        self.__runtime_counter = runtime_counter

    def increase(self):
        """Atomically increment the context counter."""
        with self.__runtime_counter._conter_lock.sync_lock():
            self.__runtime_counter._context_counter += 1

    def decrease(self):
        """Atomically decrement the context counter."""
        with self.__runtime_counter._conter_lock.sync_lock():
            self.__runtime_counter._context_counter -= 1


class AsyncRuntimeCounter:
    """Asynchronous counter for active contexts."""

    def __init__(self, runtime_counter: RuntimeCounterMixin):
        """Initialize with the underlying counter mixin.

        Args:
            runtime_counter: The mixin instance to wrap.
        """
        self.__runtime_counter = runtime_counter

    async def increase(self):
        """Atomically increment the context counter."""
        async with self.__runtime_counter._conter_lock.async_lock():
            self.__runtime_counter._context_counter += 1

    async def decrease(self):
        """Atomically decrement the context counter."""
        async with self.__runtime_counter._conter_lock.async_lock():
            self.__runtime_counter._context_counter -= 1


class Runtime(RuntimeCounterMixin):
    """Runtime object for tracking active DAG contexts."""

    def __init__(self):
        """Initialize the runtime counter."""
        RuntimeCounterMixin.__init__(self)
