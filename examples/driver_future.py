"""
DriverFuture — legacy (callback / synchronous) usage patterns.

The driver returns a DriverFuture from every operation. It can be consumed
in three ways:

1. **await**        — the standard asyncio approach
2. **result()**     — blocking synchronous call (releases the GIL)
3. **callbacks**    — register success / error handlers; the future is driven
                      to completion on a background thread automatically

Callbacks come in two flavours: `on_result`/`on_error` split the two outcomes
across two callables, while `on_done` hands the future itself to a single
callable that reads the outcome with `result()` inside a try/except.
"""

import asyncio
import threading
from typing import Any

from scylla.errors import FutureCancelledError, ScyllaError
from scylla.future import DriverFuture
from scylla.results import RequestResult
from scylla.session import Session
from scylla.session_builder import SessionBuilder

CONTACT_POINTS = [("127.0.0.2", 9042)]
KEYSPACE = "response_future_example_ks"


# ── helpers ────────────────────────────────────────────────────────────────────


async def setup() -> Session:
    session = await SessionBuilder().contact_points(CONTACT_POINTS).connect()
    await session.execute(
        f"CREATE KEYSPACE IF NOT EXISTS {KEYSPACE} "
        "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};"
    )
    await session.use_keyspace(KEYSPACE)
    await session.execute("CREATE TABLE IF NOT EXISTS users (id int PRIMARY KEY, name text);")
    await session.execute("TRUNCATE users;")
    # seed a few rows
    for i in range(5):
        await session.execute("INSERT INTO users (id, name) VALUES (?, ?)", [i, f"user_{i}"])
    return session


def example_blocking_result(session: Session) -> None:
    """
    Call result() to block until the operation completes.
    The GIL is released while waiting, so other Python threads can run.
    """
    print("\n=== Blocking result() ===")

    future = session.execute("SELECT * FROM users")
    result = future.result()  # blocks here

    rows: list[Any] = result.all().result()
    for row in rows:
        print(f"  {row}")


def example_on_result_and_on_error(session: Session) -> None:
    """
    Register a callback. The future is automatically driven to
    completion on a background thread — no await or result() needed.
    """
    print("\n=== on_result (no await) ===")

    event = threading.Event()

    def on_success(result: RequestResult) -> None:
        res = result.all().result()
        print(f"  callback received result: {res}")
        event.set()

    def on_failure(exc: BaseException) -> None:
        print(f"  on_error received: {type(exc).__name__}: {exc}")
        event.set()

    future = session.execute("SELECT * FROM users")
    future.on_result(on_success)
    future.on_error(on_failure)

    event.wait(timeout=1)


def example_on_done(session: Session) -> None:
    """
    Register a single callback for both outcomes. It receives the future, so the
    result is read with result() — which returns the value on success and raises
    on failure, letting one try/except cover both cases.
    """
    print("\n=== on_done (both outcomes, one callback) ===")

    done = threading.Semaphore(0)

    def on_done(future: DriverFuture[RequestResult]) -> None:
        try:
            result = future.result()
        except ScyllaError as exc:
            print(f"  on_done received error: {type(exc).__name__}: {exc}")
        else:
            print(f"  on_done received result: {result.all().result()}")
        finally:
            done.release()

    session.execute("SELECT * FROM users").on_done(on_done)
    session.execute("SELECT * FROM nonexistent_table_xyz").on_done(on_done)

    for _ in range(2):
        done.acquire(timeout=1)


def example_cancel(session: Session) -> None:
    """
    cancel() moves the future to a terminal error state.
    Subsequent result() calls raise FutureCancelledError.
    """
    print("\n=== cancel() ===")

    future = session.execute("SELECT * FROM users")
    future.cancel()

    try:
        future.result()
    except FutureCancelledError as e:
        print(f"  result() after cancel: {e}")


def example_fully_sync() -> None:
    """
    Everything can work without an asyncio event loop at all.
    Use result() for blocking calls and callbacks for async notifications.
    """
    print("\n=== Fully synchronous (no event loop) ===")

    session = SessionBuilder().contact_points(CONTACT_POINTS).connect().result()

    session.execute(
        f"CREATE KEYSPACE IF NOT EXISTS {KEYSPACE} "
        "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};"
    ).result()
    session.use_keyspace(KEYSPACE).result()

    result = session.execute("SELECT * FROM users").result()
    rows: list[Any] = result.all().result()
    print(f"  fetched {len(rows)} rows synchronously")


async def main() -> None:
    session = await setup()

    example_blocking_result(session)
    example_on_result_and_on_error(session)
    example_on_done(session)
    example_cancel(session)
    example_fully_sync()

    print("\nAll examples completed.")


asyncio.run(main())
