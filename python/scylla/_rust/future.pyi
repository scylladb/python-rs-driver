from collections.abc import Callable, Generator
from typing import Any, Generic, TypeVar

_T = TypeVar("_T")

class ResponseFuture(Generic[_T]):
    """
    An awaitable handle representing a pending asynchronous database operation.

    This future is **lazy** — the underlying operation is not driven to
    completion until it is awaited. The simplest and recommended way to
    consume it::

        result = await session.execute("SELECT * FROM users")

    When callbacks are registered via :meth:`add_callback`, :meth:`add_errback`,
    or :meth:`add_callbacks`, the future becomes **eager**: it is spawned on a
    background thread and driven to completion without requiring ``await``.
    Callbacks are invoked as soon as the result is available::

        future = session.execute("SELECT * FROM users")
        future.add_callback(lambda result: print(result))

    The future can also be consumed synchronously by calling :meth:`result`,
    which blocks the calling thread (releasing the GIL) until done::

        result = session.execute("SELECT * FROM users").result()
    """

    def __await__(self) -> Generator[Any, None, _T]:
        """Return an iterator that drives this future to completion, yielding ``_T``."""

    def __iter__(self) -> Generator[Any, None, _T]: ...
    def __next__(self) -> Any: ...
    def send(self, value: Any) -> Any: ...
    def throw(self, exc: BaseException) -> Any: ...
    def close(self) -> None: ...
    def cancel(self) -> None:
        """
        Cancel the future.
        """

    def done(self) -> bool:
        """Return ``True`` if the future has completed (successfully or with an error)."""

    def result(self) -> _T:
        """
        Return the result of the operation, blocking until it completes if still pending.
        """

    def add_callback(
        self,
        fn: Callable[..., Any],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """
        Register a callback to be invoked when the operation completes successfully.

        The callback is called as ``fn(result, *args, **kwargs)``.
        If the future is already resolved successfully, the callback is invoked immediately.

        Parameters
        ----------
        fn : Callable
            The callable to invoke with the result value as the first argument.
        *args : Any
            Extra positional arguments forwarded to the callback.
        **kwargs : Any
            Extra keyword arguments forwarded to the callback.
        """

    def add_errback(
        self,
        fn: Callable[..., Any],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """
        Register a callback to be invoked when the operation completes with an error.

        The callback is called as ``fn(exception, *args, **kwargs)``.
        If the future is already resolved with an error, the callback is invoked immediately.

        Parameters
        ----------
        fn : Callable
            The callable to invoke with the exception as the first argument.
        *args : Any
            Extra positional arguments forwarded to the callback.
        **kwargs : Any
            Extra keyword arguments forwarded to the callback.
        """

    def add_callbacks(
        self,
        callback: Callable[..., Any],
        errback: Callable[..., Any],
        /,
        callback_args: tuple[Any, ...] | None = None,
        callback_kwargs: dict[str, Any] | None = None,
        errback_args: tuple[Any, ...] | None = None,
        errback_kwargs: dict[str, Any] | None = None,
    ) -> None:
        """
        Register both a success and an error callback in a single call.

        Equivalent to calling :meth:`add_callback` and :meth:`add_errback` separately.

        Parameters
        ----------
        callback : Callable
            Success callback, called as ``callback(result, *callback_args, **callback_kwargs)``.
        errback : Callable
            Error callback, called as ``errback(exception, *errback_args, **errback_kwargs)``.
        callback_args : tuple, optional
            Positional arguments forwarded to the success callback.
        callback_kwargs : dict, optional
            Keyword arguments forwarded to the success callback.
        errback_args : tuple, optional
            Positional arguments forwarded to the error callback.
        errback_kwargs : dict, optional
            Keyword arguments forwarded to the error callback.
        """
