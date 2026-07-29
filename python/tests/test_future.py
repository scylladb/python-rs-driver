import asyncio
import threading
from collections.abc import AsyncGenerator, Awaitable, Callable

import pytest
import pytest_asyncio
from scylla.errors import ExecuteError, ScyllaError
from scylla.future import ResponseFuture
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
    assert isinstance(future, ResponseFuture)


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
async def test_add_callback_called_on_completion(session: Session) -> None:
    results: list[RequestResult] = []
    future = session.execute("SELECT release_version FROM system.local")
    future.add_callback(results.append)
    await future
    assert len(results) == 1


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_add_callback_called_immediately_if_already_resolved(session: Session) -> None:
    future = session.execute("SELECT release_version FROM system.local")
    await future

    results: list[RequestResult] = []
    future.add_callback(results.append)

    assert len(results) == 1


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_add_callback_forwards_extra_args(session: Session) -> None:
    calls: list[tuple[RequestResult, str, str]] = []

    def cb(result: RequestResult, extra_arg: str, *, extra_kwarg: str) -> None:
        calls.append((result, extra_arg, extra_kwarg))

    future = session.execute("SELECT release_version FROM system.local")
    future.add_callback(cb, "positional", extra_kwarg="keyword")
    await future

    assert len(calls) == 1
    _result, extra_arg, extra_kwarg = calls[0]
    assert extra_arg == "positional"
    assert extra_kwarg == "keyword"


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_multiple_add_callback_callbacks_all_called(session: Session) -> None:
    calls: list[int] = []
    future = session.execute("SELECT release_version FROM system.local")

    def cb1(_r: RequestResult) -> None:
        calls.append(1)

    def cb2(_r: RequestResult) -> None:
        calls.append(2)

    future.add_callback(cb1)
    future.add_callback(cb2)
    await future
    assert calls == [1, 2]


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_add_callback_not_called_on_error(session: Session, table_factory: TableFactory) -> None:
    await table_factory("id int PRIMARY KEY, val int", "add_callback_error_test")

    calls: list[RequestResult] = []
    future = session.execute("SELECT * FROM nonexistent_table_xyz")

    def add_callback_cb(r: RequestResult) -> None:
        calls.append(r)

    future.add_callback(add_callback_cb)

    with pytest.raises(ExecuteError):
        await future

    assert calls == []


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_add_errback_called_on_failed_future(session: Session) -> None:
    errors: list[Exception] = []
    future = session.execute("SELECT * FROM nonexistent_table_xyz")
    future.add_errback(errors.append)

    with pytest.raises(ExecuteError):
        await future

    assert len(errors) == 1


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_add_errback_called_immediately_if_already_failed(session: Session) -> None:
    future = session.execute("SELECT * FROM nonexistent_table_xyz")

    with pytest.raises(ExecuteError):
        await future

    errors: list[Exception] = []
    future.add_errback(errors.append)  # register after failure

    assert len(errors) == 1


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_add_errback_not_called_on_success(session: Session) -> None:
    errors: list[Exception] = []
    future = session.execute("SELECT release_version FROM system.local")
    future.add_errback(errors.append)
    await future
    assert errors == []


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_add_errback_forwards_extra_args(session: Session) -> None:
    calls: list[tuple[Exception, str]] = []

    def cb(exc: Exception, tag: str) -> None:
        calls.append((exc, tag))

    future = session.execute("SELECT * FROM nonexistent_table_xyz")
    future.add_errback(cb, "my_tag")

    with pytest.raises(ExecuteError):
        await future

    assert len(calls) == 1
    assert calls[0][1] == "my_tag"


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_close_resolves_future_with_error(session: Session) -> None:
    future = session.execute("SELECT release_version FROM system.local")
    future.close()

    with pytest.raises(RuntimeError, match="future was closed"):
        future.result()


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
async def test_add_callback_callback_fires_even_with_concurrent_result(session: Session) -> None:
    """add_callback callback registered before concurrent result() calls must fire exactly once."""
    future = session.execute("SELECT release_version FROM system.local")

    calls: list[RequestResult] = []
    future.add_callback(calls.append)

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
    assert len(calls) == 1, f"add_callback fired {len(calls)} times, expected 1"


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
async def test_add_callback_fires_without_await(session: Session) -> None:
    """add_callback callback fires automatically when the future completes,
    without any await or result() call."""

    results: list[RequestResult] = []
    future = session.execute("SELECT release_version FROM system.local")
    future.add_callback(results.append)

    # Don't await or call result() — just wait for the tokio task to complete
    await asyncio.sleep(0.2)

    assert len(results) == 1
    assert results[0] is not None


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_add_errback_fires_without_await(session: Session) -> None:
    """add_errback callback fires automatically on failure without await."""

    errors: list[Exception] = []
    future = session.execute("SELECT * FROM nonexistent_table_xyz")
    future.add_errback(errors.append)

    await asyncio.sleep(0.2)

    assert len(errors) == 1


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_multiple_callbacks_fire_without_await(session: Session) -> None:
    """Multiple add_callback callbacks all fire without await."""

    results1: list[RequestResult] = []
    results2: list[RequestResult] = []
    results3: list[RequestResult] = []

    future = session.execute("SELECT release_version FROM system.local")
    future.add_callback(results1.append)
    future.add_callback(results2.append)
    future.add_callback(results3.append)

    await asyncio.sleep(0.2)

    assert len(results1) == 1
    assert len(results2) == 1
    assert len(results3) == 1


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_add_callback_with_args_fires_without_await(session: Session) -> None:
    """add_callback with extra args/kwargs fires without await."""

    calls: list[tuple[object, ...]] = []

    def cb(result: RequestResult, tag: str, *, label: str) -> None:
        calls.append((result, tag, label))

    future = session.execute("SELECT release_version FROM system.local")
    future.add_callback(cb, "my_tag", label="my_label")

    await asyncio.sleep(0.2)

    assert len(calls) == 1
    assert calls[0][1] == "my_tag"
    assert calls[0][2] == "my_label"


@pytest.mark.requires_db
def test_callback_fires_without_event_loop() -> None:
    """Callbacks fire from a plain synchronous context (no event loop)."""
    import time

    builder = SessionBuilder().contact_points([("127.0.0.2", 9042)])
    session_future = builder.connect()
    session = session_future.result()

    results: list[RequestResult] = []
    future = session.execute("SELECT release_version FROM system.local")
    future.add_callback(results.append)

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
async def test_add_callback_not_called_after_close(session: Session) -> None:
    """add_callback registered after close() should NOT fire."""

    future = session.execute("SELECT release_version FROM system.local")
    future.close()

    results: list[RequestResult] = []
    future.add_callback(results.append)

    await asyncio.sleep(0.2)
    assert results == []


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_add_errback_called_after_close(session: Session) -> None:
    """add_errback registered after close() should fire immediately with the error."""
    future = session.execute("SELECT release_version FROM system.local")
    future.close()

    errors: list[Exception] = []
    future.add_errback(errors.append)

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
    future.add_callback(bad_callback)
    future.add_callback(results.append)

    await asyncio.sleep(0.2)

    assert len(results) == 1
    assert results[0] is not None
