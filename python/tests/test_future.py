from __future__ import annotations

import asyncio
import threading
from collections.abc import AsyncGenerator, Awaitable, Callable

import pytest
import pytest_asyncio
from scylla.errors import ExecuteError, FutureCancelledError, ScyllaError
from scylla.future import DriverFuture
from scylla.results import RequestResult
from scylla.session import Session
from scylla.session_builder import SessionBuilder

TableFactory = Callable[[str, str], Awaitable[str]]


async def set_up() -> Session:
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()
    await session.execute("""
        CREATE KEYSPACE IF NOT EXISTS future_testks
        WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};
    """)
    await session.execute("USE future_testks")
    return session


@pytest_asyncio.fixture(scope="module")
async def session() -> AsyncGenerator[Session, None]:
    session = await set_up()
    yield session
    await session.execute("DROP KEYSPACE future_testks")


@pytest_asyncio.fixture
async def table_factory(session: Session) -> AsyncGenerator[TableFactory, None]:
    created: list[str] = []

    async def create(schema: str, name: str) -> str:
        await session.execute(f"CREATE TABLE IF NOT EXISTS {name} ({schema});")
        created.append(name)
        return name

    yield create

    for table in created:
        await session.execute(f"DROP TABLE IF EXISTS {table};")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_execute_returns_future(session: Session) -> None:
    future = session.execute("SELECT release_version FROM system.local")
    assert isinstance(future, DriverFuture)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_future_is_awaitable(session: Session) -> None:
    result = await session.execute("SELECT release_version FROM system.local")
    assert result is not None


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_result_blocks_and_returns_value(session: Session) -> None:
    future = session.execute("SELECT release_version FROM system.local")
    # result() blocks the thread and returns the resolved value
    result: RequestResult = future.result()
    assert result is not None


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_result_can_be_called_twice_on_resolved_future(session: Session) -> None:
    future = session.execute("SELECT release_version FROM system.local")
    result1: RequestResult = future.result()
    result2: RequestResult = future.result()
    assert result1 is not None
    assert result2 is not None


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_result_from_separate_thread(session: Session) -> None:
    """result() can be called from a non-event-loop thread."""
    future = session.execute("SELECT release_version FROM system.local")

    outcome: list[RequestResult] = []

    def worker() -> None:
        outcome.append(future.result())

    t = threading.Thread(target=worker)
    t.start()
    t.join(timeout=10)

    assert not t.is_alive(), "worker thread timed out"
    assert outcome and outcome[0] is not None


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_on_result_called_on_completion(session: Session) -> None:
    results: list[RequestResult] = []
    future = session.execute("SELECT release_version FROM system.local")
    future.on_result(results.append)
    await future
    assert len(results) == 1


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_on_result_called_immediately_if_already_resolved(session: Session) -> None:
    future = session.execute("SELECT release_version FROM system.local")
    await future

    results: list[RequestResult] = []
    future.on_result(results.append)

    assert len(results) == 1


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_multiple_on_result_callbacks_all_called(session: Session) -> None:
    calls: list[int] = []
    future = session.execute("SELECT release_version FROM system.local")

    def cb1(_r: RequestResult) -> None:
        calls.append(1)

    def cb2(_r: RequestResult) -> None:
        calls.append(2)

    future.on_result(cb1)
    future.on_result(cb2)
    await future
    assert calls == [1, 2]


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_on_result_not_called_on_error(session: Session, table_factory: TableFactory) -> None:
    await table_factory("id int PRIMARY KEY, val int", "on_result_error_test")

    calls: list[RequestResult] = []
    future = session.execute("SELECT * FROM nonexistent_table_xyz")

    def on_result_cb(r: RequestResult) -> None:
        calls.append(r)

    future.on_result(on_result_cb)

    with pytest.raises(ExecuteError):
        await future

    assert calls == []


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_on_error_called_on_failed_future(session: Session) -> None:
    errors: list[Exception] = []
    future = session.execute("SELECT * FROM nonexistent_table_xyz")
    future.on_error(errors.append)

    with pytest.raises(ExecuteError):
        await future

    assert len(errors) == 1


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_on_error_called_immediately_if_already_failed(session: Session) -> None:
    future = session.execute("SELECT * FROM nonexistent_table_xyz")

    with pytest.raises(ExecuteError):
        await future

    errors: list[Exception] = []
    future.on_error(errors.append)  # register after failure

    assert len(errors) == 1


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_on_error_not_called_on_success(session: Session) -> None:
    errors: list[Exception] = []
    future = session.execute("SELECT release_version FROM system.local")
    future.on_error(errors.append)
    await future
    assert errors == []


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_close_resolves_future_with_error(session: Session) -> None:
    future = session.execute("SELECT release_version FROM system.local")
    future.close()

    with pytest.raises(RuntimeError, match="future was closed"):
        future.result()


# ── on_done (single callback for both outcomes) ───────────────────────────────


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_on_done_called_on_success(session: Session) -> None:
    """on_done fires on success; the outcome is read off the future it is passed."""
    results: list[RequestResult] = []
    future = session.execute("SELECT release_version FROM system.local")

    def on_done(f: DriverFuture[RequestResult]) -> None:
        results.append(f.result())

    future.on_done(on_done)
    await future

    assert len(results) == 1
    assert results[0] is not None


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_on_done_receives_the_registered_future(session: Session) -> None:
    """The callback argument is the very future on_done was registered on."""
    received: list[DriverFuture[RequestResult]] = []
    future = session.execute("SELECT release_version FROM system.local")
    future.on_done(received.append)
    await future

    assert len(received) == 1
    assert received[0] is future
    assert received[0].done()


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_on_done_called_on_error(session: Session) -> None:
    """on_done fires on failure too, and result() raises inside the callback."""
    errors: list[BaseException] = []
    future = session.execute("SELECT * FROM nonexistent_table_xyz")

    def on_done(f: DriverFuture[RequestResult]) -> None:
        try:
            f.result()
        except ScyllaError as exc:
            errors.append(exc)

    future.on_done(on_done)

    with pytest.raises(ExecuteError):
        await future

    assert len(errors) == 1
    assert isinstance(errors[0], ExecuteError)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_on_done_called_immediately_if_already_resolved(session: Session) -> None:
    future = session.execute("SELECT release_version FROM system.local")
    await future

    received: list[DriverFuture[RequestResult]] = []
    future.on_done(received.append)

    assert len(received) == 1
    assert received[0].result() is not None


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_on_done_called_immediately_if_already_failed(session: Session) -> None:
    future = session.execute("SELECT * FROM nonexistent_table_xyz")

    with pytest.raises(ExecuteError):
        await future

    received: list[DriverFuture[RequestResult]] = []
    future.on_done(received.append)  # register after failure

    assert len(received) == 1
    with pytest.raises(ExecuteError):
        received[0].result()


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_multiple_on_done_callbacks_all_called_in_order(session: Session) -> None:
    calls: list[int] = []
    future = session.execute("SELECT release_version FROM system.local")

    future.on_done(lambda _f: calls.append(1))
    future.on_done(lambda _f: calls.append(2))
    await future

    assert calls == [1, 2]


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_on_done_fires_alongside_on_result(session: Session) -> None:
    """on_done and on_result coexist and fire in registration order."""
    calls: list[str] = []
    future = session.execute("SELECT release_version FROM system.local")

    def on_error(_exc: BaseException) -> None:
        calls.append("error")

    future.on_done(lambda _f: calls.append("done"))
    future.on_result(lambda _r: calls.append("result"))
    future.on_error(on_error)
    await future

    assert calls == ["done", "result"]


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_on_done_fires_alongside_on_error(session: Session) -> None:
    calls: list[str] = []
    future = session.execute("SELECT * FROM nonexistent_table_xyz")

    def on_error(_exc: BaseException) -> None:
        calls.append("error")

    future.on_result(lambda _r: calls.append("result"))
    future.on_done(lambda _f: calls.append("done"))
    future.on_error(on_error)

    with pytest.raises(ExecuteError):
        await future

    assert calls == ["done", "error"]


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_on_done_fires_without_await(session: Session) -> None:
    """Registering on_done drives the future to completion on its own."""
    received: list[DriverFuture[RequestResult]] = []
    future = session.execute("SELECT release_version FROM system.local")
    future.on_done(received.append)

    await asyncio.sleep(0.2)

    assert len(received) == 1
    assert received[0].result() is not None


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_on_done_fires_without_await_on_error(session: Session) -> None:
    received: list[DriverFuture[RequestResult]] = []
    future = session.execute("SELECT * FROM nonexistent_table_xyz")
    future.on_done(received.append)

    await asyncio.sleep(0.2)

    assert len(received) == 1
    with pytest.raises(ExecuteError):
        received[0].result()


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_on_done_called_after_close(session: Session) -> None:
    """on_done registered after close() fires immediately with the close error."""
    future = session.execute("SELECT release_version FROM system.local")
    future.close()

    received: list[DriverFuture[RequestResult]] = []
    future.on_done(received.append)

    assert len(received) == 1
    with pytest.raises(RuntimeError, match="future was closed"):
        received[0].result()


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_on_done_called_after_cancel(session: Session) -> None:
    """on_done registered after cancel() fires immediately with the cancellation."""
    future = session.execute("SELECT release_version FROM system.local")
    future.cancel()

    received: list[DriverFuture[RequestResult]] = []
    future.on_done(received.append)

    assert len(received) == 1
    assert received[0].cancelled()
    with pytest.raises(FutureCancelledError):
        received[0].result()


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_pending_on_done_fires_on_cancel(session: Session) -> None:
    """A pending on_done still fires exactly once when cancel() resolves the future."""
    received: list[DriverFuture[RequestResult]] = []
    future = session.execute("SELECT release_version FROM system.local")
    future.on_done(received.append)

    # Race: either cancel() lands first, or the query resolved before it took
    # effect. Either way the callback must fire exactly once.
    future.cancel()
    await asyncio.sleep(0.2)

    assert len(received) == 1
    if received[0].cancelled():
        with pytest.raises(FutureCancelledError):
            received[0].result()
    else:
        assert received[0].result() is not None


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_failing_on_done_does_not_prevent_others(session: Session) -> None:
    """A raising on_done callback must not stop the remaining callbacks."""
    received: list[DriverFuture[RequestResult]] = []

    def bad_callback(_f: DriverFuture[RequestResult]) -> None:
        raise ValueError("callback exploded")

    future = session.execute("SELECT release_version FROM system.local")
    future.on_done(bad_callback)
    future.on_done(received.append)

    await asyncio.sleep(0.2)

    assert len(received) == 1


@pytest.mark.requires_db
def test_on_done_fires_without_event_loop() -> None:
    """on_done fires from a plain synchronous context (no event loop)."""
    import time

    session = SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect().result()

    received: list[DriverFuture[RequestResult]] = []
    session.execute("SELECT release_version FROM system.local").on_done(received.append)

    time.sleep(0.2)

    assert len(received) == 1
    assert received[0].result() is not None


# ── threading scenarios ────────────────────────────────────────────────────────


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_concurrent_result_calls_both_get_same_value(session: Session) -> None:
    """Two threads calling result() concurrently: one blocks, one waits on condvar.
    Both should receive the same non-None result."""
    future = session.execute("SELECT release_version FROM system.local")

    outcomes: list[RequestResult] = [None, None]  # type: ignore[list-item]
    errors: list[Exception] = []

    def worker(index: int) -> None:
        try:
            outcomes[index] = future.result()
        except ScyllaError as e:
            errors.append(e)

    t1 = threading.Thread(target=worker, args=(0,))
    t2 = threading.Thread(target=worker, args=(1,))
    t1.start()
    t2.start()
    t1.join(timeout=10)
    t2.join(timeout=10)

    assert not t1.is_alive(), "thread 1 timed out"
    assert not t2.is_alive(), "thread 2 timed out"
    assert not errors, f"unexpected errors: {errors}"
    assert outcomes[0] is not None
    assert outcomes[1] is not None


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_result_from_thread_while_awaiting(session: Session) -> None:
    """result() called from a background thread while the event loop is also
    awaiting the future. Both should complete — thread gets the value, await
    gets StopIteration and returns normally."""
    future = session.execute("SELECT release_version FROM system.local")

    thread_outcome: list[RequestResult] = []
    thread_errors: list[Exception] = []

    def worker() -> None:
        try:
            thread_outcome.append(future.result())
        except ScyllaError as e:
            thread_errors.append(e)

    t = threading.Thread(target=worker)
    t.start()

    # await on the event loop concurrently with the thread blocking
    await_result = await future

    t.join(timeout=10)

    assert not t.is_alive(), "worker thread timed out"
    assert not thread_errors, f"thread errors: {thread_errors}"
    assert await_result is not None
    assert thread_outcome and thread_outcome[0] is not None


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_many_threads_concurrent_result(session: Session) -> None:
    """N threads all call result() on the same future concurrently.
    All should return a non-None result with no errors."""
    future = session.execute("SELECT release_version FROM system.local")

    n = 8
    outcomes: list[RequestResult | None] = [None] * n
    errors: list[Exception] = []

    def worker(index: int) -> None:
        try:
            outcomes[index] = future.result()
        except ScyllaError as e:
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    assert all(not t.is_alive() for t in threads), "some threads timed out"
    assert not errors, f"unexpected errors: {errors}"
    assert all(r is not None for r in outcomes)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_result_after_close_from_thread(session: Session) -> None:
    """close() called from main thread while a background thread is blocking on result().
    The thread should get a RuntimeError('future was closed')."""

    future = session.execute("SELECT release_version FROM system.local")

    thread_outcomes: list[RequestResult] = []
    thread_errors: list[Exception] = []

    def worker() -> None:
        try:
            thread_outcomes.append(future.result())
        except RuntimeError as e:
            thread_errors.append(e)

    t = threading.Thread(target=worker)
    t.start()

    # give the thread a moment to enter block_on before we close
    await asyncio.sleep(0.05)
    future.close()

    t.join(timeout=10)

    assert not t.is_alive(), "worker thread timed out"
    # Race: either close() arrived before result() completed (RuntimeError)
    # or the future resolved before close() took effect (successful result).
    assert thread_errors or thread_outcomes
    if thread_errors:
        assert "future was closed" in str(thread_errors[0])
    else:
        assert thread_outcomes[0] is not None


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_on_result_callback_fires_even_with_concurrent_result(session: Session) -> None:
    """on_result callback registered before concurrent result() calls must fire exactly once."""
    future = session.execute("SELECT release_version FROM system.local")

    calls: list[RequestResult] = []
    future.on_result(calls.append)

    outcomes: list[RequestResult] = []
    errors: list[Exception] = []

    def worker() -> None:
        try:
            outcomes.append(future.result())
        except ScyllaError as e:
            errors.append(e)

    t1 = threading.Thread(target=worker)
    t2 = threading.Thread(target=worker)
    t1.start()
    t2.start()
    t1.join(timeout=10)
    t2.join(timeout=10)

    assert not errors
    assert len(calls) == 1, f"on_result fired {len(calls)} times, expected 1"


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_result_on_already_ready_future_does_not_block(session: Session) -> None:
    """Once a future is resolved, result() from any thread should return immediately."""
    future = session.execute("SELECT release_version FROM system.local")
    await future  # resolve via event loop first

    outcomes: list[RequestResult] = []
    errors: list[Exception] = []

    def worker() -> None:
        try:
            outcomes.append(future.result())
        except ScyllaError as e:
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)

    assert all(not t.is_alive() for t in threads), "some threads timed out"
    assert not errors
    assert len(outcomes) == 4
    assert all(r is not None for r in outcomes)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_on_result_fires_without_await(session: Session) -> None:
    """on_result callback fires automatically when the future completes,
    without any await or result() call."""

    results: list[RequestResult] = []
    future = session.execute("SELECT release_version FROM system.local")
    future.on_result(results.append)

    # Don't await or call result() — just wait for the tokio task to complete
    await asyncio.sleep(0.2)

    assert len(results) == 1
    assert results[0] is not None


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_on_error_fires_without_await(session: Session) -> None:
    """on_error callback fires automatically on failure without await."""

    errors: list[Exception] = []
    future = session.execute("SELECT * FROM nonexistent_table_xyz")
    future.on_error(errors.append)

    await asyncio.sleep(0.2)

    assert len(errors) == 1


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_multiple_callbacks_fire_without_await(session: Session) -> None:
    """Multiple on_result callbacks all fire without await."""

    results1: list[RequestResult] = []
    results2: list[RequestResult] = []
    results3: list[RequestResult] = []

    future = session.execute("SELECT release_version FROM system.local")
    future.on_result(results1.append)
    future.on_result(results2.append)
    future.on_result(results3.append)

    await asyncio.sleep(0.2)

    assert len(results1) == 1
    assert len(results2) == 1
    assert len(results3) == 1


@pytest.mark.requires_db
def test_callback_fires_without_event_loop() -> None:
    """Callbacks fire from a plain synchronous context (no event loop)."""
    import time

    builder = SessionBuilder().contact_points([("127.0.0.2", 9042)])
    session_future = builder.connect()
    session = session_future.result()

    results: list[RequestResult] = []
    future = session.execute("SELECT release_version FROM system.local")
    future.on_result(results.append)

    time.sleep(0.2)

    assert len(results) == 1
    assert results[0] is not None


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_await_after_close_raises(session: Session) -> None:
    """await on a closed future should raise RuntimeError."""
    future = session.execute("SELECT release_version FROM system.local")
    future.close()

    with pytest.raises(RuntimeError, match="future was closed"):
        await future


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_await_multiple_times_returns_same_result(session: Session) -> None:
    """Awaiting the same future multiple times returns the same result."""
    future = session.execute("SELECT release_version FROM system.local")
    result1 = await future
    result2 = await future
    assert result1 is not None
    assert result2 is not None


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_on_result_not_called_after_close(session: Session) -> None:
    """on_result registered after close() should NOT fire."""

    future = session.execute("SELECT release_version FROM system.local")
    future.close()

    results: list[RequestResult] = []
    future.on_result(results.append)

    await asyncio.sleep(0.2)
    assert results == []


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_on_error_called_after_close(session: Session) -> None:
    """on_error registered after close() should fire immediately with the error."""
    future = session.execute("SELECT release_version FROM system.local")
    future.close()

    errors: list[Exception] = []
    future.on_error(errors.append)

    assert len(errors) == 1
    assert "future was closed" in str(errors[0])


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_close_called_twice_is_noop(session: Session) -> None:
    """Calling close() twice should not crash."""
    future = session.execute("SELECT release_version FROM system.local")
    future.close()
    future.close()  # second call — should be no-op

    with pytest.raises(RuntimeError, match="future was closed"):
        future.result()


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_close_on_already_resolved_future_is_noop(session: Session) -> None:
    """close() on an already-resolved future is a no-op; result() still works."""
    future = session.execute("SELECT release_version FROM system.local")
    result1 = await future

    future.close()  # should be no-op

    result2 = future.result()
    assert result1 is not None
    assert result2 is not None


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_result_on_failed_future_raises(session: Session) -> None:
    """result() on a failed future should raise the exception."""
    future = session.execute("SELECT * FROM nonexistent_table_xyz")

    with pytest.raises(ExecuteError):
        await future

    with pytest.raises(ExecuteError):
        future.result()


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_failing_callback_does_not_prevent_others(session: Session) -> None:
    """A callback that raises should not prevent other callbacks from firing."""

    results: list[RequestResult] = []

    def bad_callback(_r: RequestResult) -> None:
        raise ValueError("callback exploded")

    future = session.execute("SELECT release_version FROM system.local")
    future.on_result(bad_callback)
    future.on_result(results.append)

    await asyncio.sleep(0.2)

    assert len(results) == 1
    assert results[0] is not None
