from __future__ import annotations

import asyncio
import gc
import sys
import threading
import time
from collections.abc import Awaitable, Callable, Iterator
from typing import Any, TypeVar, cast

import pytest
from helpers.session import connect
from scylla.results import RequestResult
from scylla.session import Session
from scylla.statement import Statement

T = TypeVar("T")

QUERY = "SELECT release_version FROM system.local"

BURST = 128

LOOPS = 4

TIMEOUT = 30.0

# Time for a worker whose `wait_for` fired to record the error and close its loop.
CLEANUP_MARGIN = 5.0


@pytest.fixture(scope="module")
def session() -> Iterator[Session]:
    """A session outliving the loop it was created on."""
    yield asyncio.run(connect())


async def _burst(session: Session, count: int = BURST) -> list[RequestResult]:
    """Fire `count` queries at once and wait for all of them, even if some fail."""
    outcomes = await asyncio.gather(*(session.execute(QUERY) for _ in range(count)), return_exceptions=True)
    for outcome in outcomes:
        if isinstance(outcome, BaseException):
            raise outcome
    return cast(list[RequestResult], outcomes)


def _assert_burst(results: list[RequestResult], count: int = BURST) -> None:
    assert len(results) == count
    assert all(r is not None for r in results)


def _run_on_new_loop(
    body: Callable[[], Awaitable[T]],
    loop_factory: Callable[[], asyncio.AbstractEventLoop] = asyncio.new_event_loop,
) -> T:
    """Run `body` on a fresh loop, then close it and let it be collected."""
    loop = loop_factory()
    try:
        return loop.run_until_complete(asyncio.wait_for(body(), TIMEOUT))
    finally:
        loop.close()


def _join_all(threads: list[threading.Thread], runs: int = 1) -> None:
    """Join every thread before asserting, giving each `runs` loop timeouts plus cleanup."""
    deadline = time.monotonic() + runs * TIMEOUT + CLEANUP_MARGIN
    for thread in threads:
        thread.join(max(0.0, deadline - time.monotonic()))
    assert not any(thread.is_alive() for thread in threads), "a loop thread never finished"


# --------------------------------------------------------------------------- #
# One loop
# --------------------------------------------------------------------------- #


@pytest.mark.requires_db
def test_burst_completes_on_a_single_loop(session: Session) -> None:
    _assert_burst(_run_on_new_loop(lambda: _burst(session)))


@pytest.mark.requires_db
def test_alternating_single_and_burst_requests(session: Session) -> None:
    async def body() -> int:
        rounds = 0
        for _ in range(20):
            assert await session.execute(QUERY) is not None
            _assert_burst(await _burst(session, 32), 32)
            rounds += 1
        return rounds

    assert _run_on_new_loop(body) == 20


# --------------------------------------------------------------------------- #
# Several loops
# --------------------------------------------------------------------------- #


@pytest.mark.requires_db
def test_sequential_loops_each_complete(session: Session) -> None:
    """One loop after another on the same session, each completing its requests."""
    for _ in range(5):
        _assert_burst(_run_on_new_loop(lambda: _burst(session, 32)), 32)
        gc.collect()


@pytest.mark.requires_db
def test_loop_at_a_reused_address_completes(session: Session) -> None:
    """A new loop at a dead loop's address still completes its requests.

    Loops are created and dropped in a tight cycle, so CPython hands the same
    address out again. Skipped if no address was reused.
    """
    addresses: list[int] = []

    async def body() -> list[RequestResult]:
        addresses.append(id(asyncio.get_running_loop()))
        return await _burst(session, 16)

    for _ in range(30):
        _assert_burst(_run_on_new_loop(body), 16)
        gc.collect()

    if len(set(addresses)) == len(addresses):
        pytest.skip("no loop address was reused, so eviction was not exercised")


@pytest.mark.requires_db
def test_parallel_loops_in_threads(session: Session) -> None:
    """Several loops alive at once, each completing its own requests."""
    results: dict[int, list[RequestResult]] = {}
    errors: list[BaseException] = []
    lock = threading.Lock()

    def worker(index: int) -> None:
        try:
            burst = _run_on_new_loop(lambda: _burst(session, 32))
            with lock:
                results[index] = burst
        except BaseException as exc:  # noqa: BLE001 - reported below
            with lock:
                errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(LOOPS)]
    for thread in threads:
        thread.start()
    _join_all(threads)

    assert not errors, errors
    assert len(results) == LOOPS
    for burst in results.values():
        _assert_burst(burst, 32)


@pytest.mark.requires_db
def test_parallel_loops_churning(session: Session) -> None:
    """Loops created, used and destroyed concurrently in several threads."""
    runs = 5
    errors: list[BaseException] = []
    lock = threading.Lock()

    def worker() -> None:
        try:
            for _ in range(runs):
                _assert_burst(_run_on_new_loop(lambda: _burst(session, 16)), 16)
                gc.collect()
        except BaseException as exc:  # noqa: BLE001 - reported below
            with lock:
                errors.append(exc)

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(LOOPS)]
    for thread in threads:
        thread.start()
    _join_all(threads, runs=runs)

    assert not errors, errors


# --------------------------------------------------------------------------- #
# uvloop
# --------------------------------------------------------------------------- #

requires_uvloop = pytest.mark.skipif(sys.platform == "win32", reason="uvloop does not support Windows")


@pytest.mark.requires_db
@requires_uvloop
def test_uvloop_burst(session: Session) -> None:
    """A burst of requests completes on a uvloop loop."""
    import uvloop

    _assert_burst(_run_on_new_loop(lambda: _burst(session), uvloop.new_event_loop))


@pytest.mark.requires_db
@requires_uvloop
def test_uvloop_and_asyncio_side_by_side(session: Session) -> None:
    """A uvloop loop and an asyncio loop completing requests at the same time."""
    import uvloop

    results: dict[str, list[RequestResult]] = {}
    errors: list[BaseException] = []
    lock = threading.Lock()

    def worker(name: str, factory: Callable[[], asyncio.AbstractEventLoop]) -> None:
        try:
            burst = _run_on_new_loop(lambda: _burst(session, 64), factory)
            with lock:
                results[name] = burst
        except BaseException as exc:  # noqa: BLE001 - reported below
            with lock:
                errors.append(exc)

    threads = [
        threading.Thread(target=worker, args=("uvloop", uvloop.new_event_loop), daemon=True),
        threading.Thread(target=worker, args=("asyncio", asyncio.new_event_loop), daemon=True),
    ]
    for thread in threads:
        thread.start()
    _join_all(threads)

    assert not errors, errors
    assert set(results) == {"uvloop", "asyncio"}
    for burst in results.values():
        _assert_burst(burst, 64)


@pytest.mark.requires_db
@requires_uvloop
def test_uvloop_then_asyncio_on_the_same_thread(session: Session) -> None:
    """uvloop and asyncio loops replace each other, possibly at the same address,
    and each completes its requests."""
    import uvloop

    for factory in (uvloop.new_event_loop, asyncio.new_event_loop) * 3:
        _assert_burst(_run_on_new_loop(lambda: _burst(session, 32), factory), 32)
        gc.collect()


# --------------------------------------------------------------------------- #
# Loops without the usual capabilities
# --------------------------------------------------------------------------- #


class NoAddReaderLoop(asyncio.SelectorEventLoop):
    """A loop that refuses `add_reader`"""

    def add_reader(self, fd: Any, callback: Callable[..., object], *args: object) -> None:
        raise NotImplementedError("add_reader disabled for this test")


@pytest.mark.requires_db
def test_loop_without_add_reader_completes(session: Session) -> None:
    """A loop that refuses `add_reader` still completes its requests."""
    _assert_burst(_run_on_new_loop(lambda: _burst(session), NoAddReaderLoop))


class UnweakrefableLoop:
    """A duck-typed loop that cannot be weakly referenced.

    `__slots__` without `__weakref__` leaves `tp_weaklistoffset` at 0, which is
    exactly what `weakref.ref` refuses. Only the handful of methods the parking
    path touches are implemented.
    """

    __slots__ = ()

    def get_debug(self) -> bool:
        return False

    def create_future(self) -> asyncio.Future[None]:
        return asyncio.Future(loop=self)  # type: ignore[arg-type]


@pytest.mark.requires_db
def test_loop_that_cannot_be_weakly_referenced_panics(session: Session) -> None:
    """`all()` starts unspawned, so each `send` polls inline and parks on the next page.

    A page fetch finishing between the poll and parking yields None without parking;
    we then poll again for the next page. With one row per page there are hundreds of
    pages, and a network round-trip winning that race on every one is so unlikely that
    the test is not flaky in practice.
    """
    paged = Statement("SELECT * FROM system_schema.columns").with_page_size(1)
    first_page = _run_on_new_loop(lambda: session.execute(paged))
    assert first_page.has_more_pages()

    future = first_page.all()
    asyncio.events._set_running_loop(UnweakrefableLoop())  # type: ignore[arg-type]
    try:
        with pytest.raises(BaseException, match="cannot be weakly referenced") as excinfo:
            while future.send(None) is None:
                pass
        assert excinfo.type.__name__ == "PanicException"
    finally:
        asyncio.events._set_running_loop(None)
