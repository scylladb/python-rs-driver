import asyncio
import subprocess
import sys
import threading
from collections.abc import AsyncGenerator, Awaitable, Callable
from pathlib import Path
from typing import Generic, TypeVar

import pytest
import pytest_asyncio
from helpers.exit_scenarios import SCENARIO_READY
from helpers.session import connect
from scylla.errors import ExecuteError, FutureCancelledError, ScyllaError
from scylla.future import DriverFuture
from scylla.results import RequestResult
from scylla.session import Session
from scylla.session_builder import SessionBuilder

TableFactory = Callable[[str, str], Awaitable[str]]

T = TypeVar("T")

CALLBACK_TIMEOUT = 10.0


class Recorder(Generic[T]):
    def __init__(self, expected: int = 1) -> None:
        self._lock = threading.Lock()
        self._enough = threading.Event()
        self._expected = expected
        self.items: list[T] = []

    def __call__(self, item: T) -> None:
        with self._lock:
            self.items.append(item)
            if len(self.items) >= self._expected:
                self._enough.set()

    def wait(self, timeout: float = CALLBACK_TIMEOUT) -> list[T]:
        fired = self._enough.wait(timeout)
        with self._lock:
            items = list(self.items)
        assert fired
        return items

    async def awaited(self, timeout: float = CALLBACK_TIMEOUT) -> list[T]:
        return await asyncio.to_thread(self.wait, timeout)


async def set_up() -> Session:
    session = await connect()
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
async def test_on_success_called_on_completion(session: Session) -> None:
    results: list[RequestResult] = []
    future = session.execute("SELECT release_version FROM system.local")
    future.on_success(results.append)
    await future
    assert len(results) == 1


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_on_success_called_immediately_if_already_resolved(session: Session) -> None:
    future = session.execute("SELECT release_version FROM system.local")
    await future

    results: list[RequestResult] = []
    future.on_success(results.append)

    assert len(results) == 1


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_multiple_on_success_callbacks_all_called(session: Session) -> None:
    calls: list[int] = []
    future = session.execute("SELECT release_version FROM system.local")

    def cb1(_r: RequestResult) -> None:
        calls.append(1)

    def cb2(_r: RequestResult) -> None:
        calls.append(2)

    future.on_success(cb1)
    future.on_success(cb2)
    await future
    assert calls == [1, 2]


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_on_success_not_called_on_error(session: Session, table_factory: TableFactory) -> None:
    await table_factory("id int PRIMARY KEY, val int", "on_success_error_test")

    calls: list[RequestResult] = []
    future = session.execute("SELECT * FROM nonexistent_table_xyz")

    def on_success_cb(r: RequestResult) -> None:
        calls.append(r)

    future.on_success(on_success_cb)

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
async def test_on_done_fires_alongside_on_success(session: Session) -> None:
    """on_done and on_success coexist and fire in registration order."""
    calls: list[str] = []
    future = session.execute("SELECT release_version FROM system.local")

    def on_error(_exc: BaseException) -> None:
        calls.append("error")

    future.on_done(lambda _f: calls.append("done"))
    future.on_success(lambda _r: calls.append("result"))
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

    future.on_success(lambda _r: calls.append("result"))
    future.on_done(lambda _f: calls.append("done"))
    future.on_error(on_error)

    with pytest.raises(ExecuteError):
        await future

    assert calls == ["done", "error"]


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_on_done_fires_without_await(session: Session) -> None:
    """Registering on_done drives the future to completion on its own."""
    received = Recorder[DriverFuture[RequestResult]]()
    future = session.execute("SELECT release_version FROM system.local")
    future.on_done(received)

    fired = await received.awaited()

    assert len(fired) == 1
    assert fired[0].result() is not None


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_on_done_fires_without_await_on_error(session: Session) -> None:
    received = Recorder[DriverFuture[RequestResult]]()
    future = session.execute("SELECT * FROM nonexistent_table_xyz")
    future.on_done(received)

    fired = await received.awaited()

    assert len(fired) == 1
    with pytest.raises(ExecuteError):
        fired[0].result()


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
    received = Recorder[DriverFuture[RequestResult]]()
    future = session.execute("SELECT release_version FROM system.local")
    future.on_done(received)

    # Race: either cancel() lands first, or the query resolved before it took
    # effect. Either way the callback must fire exactly once.
    future.cancel()
    fired = await received.awaited()

    assert len(fired) == 1
    if fired[0].cancelled():
        with pytest.raises(FutureCancelledError):
            fired[0].result()
    else:
        assert fired[0].result() is not None


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_failing_on_done_does_not_prevent_others(session: Session) -> None:
    """A raising on_done callback must not stop the remaining callbacks."""
    received = Recorder[DriverFuture[RequestResult]]()

    def bad_callback(_f: DriverFuture[RequestResult]) -> None:
        raise ValueError("callback exploded")

    future = session.execute("SELECT release_version FROM system.local")
    future.on_done(bad_callback)
    future.on_done(received)

    assert len(await received.awaited()) == 1


@pytest.mark.requires_db
def test_on_done_fires_without_event_loop() -> None:
    """on_done fires from a plain synchronous context (no event loop)."""
    session = SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect().result()

    received = Recorder[DriverFuture[RequestResult]]()
    session.execute("SELECT release_version FROM system.local").on_done(received)

    fired = received.wait()

    assert len(fired) == 1
    assert fired[0].result() is not None


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

    entering = threading.Event()

    def worker() -> None:
        try:
            entering.set()
            thread_outcomes.append(future.result())
        except RuntimeError as e:
            thread_errors.append(e)

    t = threading.Thread(target=worker)
    t.start()

    # Wait until the worker is about to block on result(). This still cannot
    # close the window between entering.set() and block_on itself, so which
    # branch runs stays deliberately racy - the assertions below accept both.
    assert entering.wait(CALLBACK_TIMEOUT), "worker thread never started"
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
async def test_on_success_callback_fires_even_with_concurrent_result(session: Session) -> None:
    """on_success callback registered before concurrent result() calls must fire exactly once."""
    future = session.execute("SELECT release_version FROM system.local")

    calls: list[RequestResult] = []
    future.on_success(calls.append)

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
    assert len(calls) == 1, f"on_success fired {len(calls)} times, expected 1"


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
async def test_on_success_fires_without_await(session: Session) -> None:
    """on_success callback fires automatically when the future completes,
    without any await or result() call."""

    results = Recorder[RequestResult]()
    future = session.execute("SELECT release_version FROM system.local")
    future.on_success(results)

    # Don't await or call result() — the tokio task delivers the callback while
    # the event loop stays free.
    fired = await results.awaited()

    assert len(fired) == 1
    assert fired[0] is not None


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_on_error_fires_without_await(session: Session) -> None:
    """on_error callback fires automatically on failure without await."""

    errors = Recorder[Exception]()
    future = session.execute("SELECT * FROM nonexistent_table_xyz")
    future.on_error(errors)

    assert len(await errors.awaited()) == 1


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_multiple_callbacks_fire_without_await(session: Session) -> None:
    """Multiple on_success callbacks all fire without await."""

    recorders = [Recorder[RequestResult]() for _ in range(3)]

    future = session.execute("SELECT release_version FROM system.local")
    for recorder in recorders:
        future.on_success(recorder)

    for recorder in recorders:
        assert len(await recorder.awaited()) == 1


@pytest.mark.requires_db
def test_callback_fires_without_event_loop() -> None:
    """Callbacks fire from a plain synchronous context (no event loop)."""
    builder = SessionBuilder().contact_points([("127.0.0.2", 9042)])
    session_future = builder.connect()
    session = session_future.result()

    results = Recorder[RequestResult]()
    future = session.execute("SELECT release_version FROM system.local")
    future.on_success(results)

    fired = results.wait()

    assert len(fired) == 1
    assert fired[0] is not None


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
async def test_on_success_not_called_after_close(session: Session) -> None:
    """on_success registered after close() should NOT fire."""

    future = session.execute("SELECT release_version FROM system.local")
    future.close()

    # close() leaves the future resolved, so registration dispatches inline and
    # skips the success callback: there is no later pass to wait for.
    results: list[RequestResult] = []
    future.on_success(results.append)

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

    results = Recorder[RequestResult]()

    def bad_callback(_r: RequestResult) -> None:
        raise ValueError("callback exploded")

    future = session.execute("SELECT release_version FROM system.local")
    future.on_success(bad_callback)
    future.on_success(results)

    fired = await results.awaited()

    assert len(fired) == 1
    assert fired[0] is not None


# ── interpreter exit (atexit runtime shutdown) ────────────────────────────────

_EXIT_SCENARIOS = Path(__file__).parent / "helpers" / "exit_scenarios.py"

EXIT_SCENARIO_TIMEOUT = 40.0


def _run_exit_scenario(name: str) -> subprocess.CompletedProcess[str]:
    """Run one scenario from helpers/exit_scenarios.py to completion in a child."""
    try:
        return subprocess.run(
            [sys.executable, str(_EXIT_SCENARIOS), name],
            capture_output=True,
            text=True,
            timeout=EXIT_SCENARIO_TIMEOUT,
            check=False,
        )
    except subprocess.TimeoutExpired as expired:
        pytest.fail(
            f"the {name} scenario never exited within {EXIT_SCENARIO_TIMEOUT}s: it hung in finalization, "
            f"past the runtime shutdown timeout\nstdout: {expired.stdout!r}\nstderr: {expired.stderr!r}"
        )


@pytest.mark.requires_db
def test_exit_with_a_callback_blocked_on_a_pending_future() -> None:
    """Exiting with a callback blocked on a future that can never resolve.

    The callback sits on the driver's blocking pool, which the atexit runtime
    shutdown waits for, waiting on a future whose task that same shutdown drops.
    Dropping the task has to resolve the future, or the callback never returns:
    the shutdown burns its whole timeout, reports "runtime shutdown timed out",
    and finalization proceeds with the thread still blocked.

    The hook runs after pytest is done reporting, so the scenario runs in an
    interpreter of its own and only its exit status and stderr are left to read.
    """
    completed = _run_exit_scenario("blocked-callback")

    assert SCENARIO_READY in completed.stdout, (
        f"the child never reached the blocked state, so nothing was proven about exit\n{completed.stderr}"
    )
    assert "runtime shutdown timed out" not in completed.stderr, (
        f"the shutdown could not drain the blocked callback\n{completed.stderr}"
    )
    assert "Fatal Python error" not in completed.stderr, f"finalization crashed\n{completed.stderr}"
    assert completed.returncode == 0, f"the child exited with {completed.returncode}\n{completed.stderr}"
