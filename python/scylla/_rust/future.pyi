from collections.abc import Callable, Generator
from datetime import timedelta
from typing import Any, Generic, TypeVar

_T = TypeVar("_T")

class DriverFuture(Generic[_T]):
    """
    An awaitable handle representing a pending asynchronous database operation.

    This future is **lazy** — the underlying operation is not driven to
    completion until it is awaited. The simplest and recommended way to
    consume it::

        result = await session.execute("SELECT * FROM users")

    Preferred for most code — it avoids the overhead of spawning the
    operation onto a background task.

    When callbacks are registered via :meth:`on_result`, :meth:`on_error` or
    :meth:`on_done`, the future becomes **eager**: it is spawned on a background
    thread and driven to completion without requiring ``await``. Callbacks are
    invoked as soon as the result is available::

        future = session.execute("SELECT * FROM users")
        future.on_result(lambda result: print(result))

    The future can also be consumed synchronously by calling :meth:`result`,
    which blocks the calling thread until done::

        result = session.execute("SELECT * FROM users").result()
    """

    def __await__(self) -> Generator[Any, None, _T]:
        """Return an iterator that drives this future to completion, yielding ``_T``."""

    def __iter__(self) -> Generator[Any, None, _T]: ...
    def __next__(self) -> Any: ...
    def send(self, value: Any) -> Any: ...
    def throw(self, exc: BaseException) -> Any: ...
    def close(self) -> None: ...
    def start(self) -> DriverFuture[_T]:
        """
        Force the operation to start running in the background immediately,
        without waiting for it to be awaited, a callback to be registered,
        or :meth:`result` to be called.

        Returns ``self``, so calls can be chained::

            future = session.execute("SELECT * FROM users").start()
        """

    def on_result(self, fn: Callable[[_T], Any], /) -> None:
        """
        Register a callback to be invoked when the operation completes successfully.

        The callback is called as ``fn(result)``.
        If the future is already resolved successfully, the callback is invoked immediately.

        Parameters
        ----------
        fn : Callable
            The callable to invoke with the result value.
        """

    def on_error(self, fn: Callable[..., Any], /) -> None:
        """
        Register a callback to be invoked when the operation completes with an error.

        The callback is called as ``fn(exception)``.
        If the future is already resolved with an error, the callback is invoked immediately.

        Parameters
        ----------
        fn : Callable
            The callable to invoke with the exception instance.
        """

    def on_done(self, fn: Callable[[DriverFuture[_T]], Any], /) -> None:
        """
        Register a callback to be invoked when the operation completes, whether it
        succeeded or failed.

        The callback is called as ``fn(future)`` with the very future it was
        registered on. Read the outcome off it with :meth:`result`, which returns
        the value on success and raises the exception on failure::

            def handle(future: DriverFuture[RequestResult]) -> None:
                try:
                    result = future.result()
                except ScyllaError as exc:
                    log.error("query failed: %s", exc)
                else:
                    print(result.all().result())

            session.execute("SELECT * FROM users").on_done(handle)

        If the future is already resolved, the callback is invoked immediately.

        Parameters
        ----------
        fn : Callable
            The callable to invoke with this future.
        """

    def cancel(self) -> None:
        """
        Cancel the future.

        Aborts any pending work. Anyone still waiting on the result (via
        ``result()``, ``await``, or a registered callback) receives a
        ``scylla.errors.FutureCancelledError`` instead of a normal result.
        """

    def done(self) -> bool:
        """Return ``True`` if the future has completed (successfully or with an error)."""

    def cancelled(self) -> bool:
        """Return ``True`` if the future completed because :meth:`cancel` was called."""

    def result(self, timeout: timedelta | float | None = None) -> _T:
        """
        Return the result of the operation, blocking until it completes if still pending.

        Parameters
        ----------
        timeout : timedelta | float | None, optional
            Maximum time to block for. If ``float``, interpreted as seconds.
            If ``None`` (the default), blocks indefinitely. Raises
            ``TimeoutError`` if the operation does not complete in time.
        """
