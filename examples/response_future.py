"""
ResponseFuture — legacy (callback / synchronous) usage patterns.

The driver returns a ResponseFuture from every operation. It can be consumed
in three ways:

1. **await**        — the standard asyncio approach
2. **result()**     — blocking synchronous call (releases the GIL)
3. **callbacks**    — register success / error handlers; the future is driven
                      to completion on a background thread automatically
"""

import asyncio
import threading
from typing import Any

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


def example_add_callback(session: Session) -> None:
    """
    Register a success callback. The future is automatically driven to
    completion on a background thread — no await or result() needed.
    """
    print("\n=== add_callback (no await) ===")

    event = threading.Event()

    def on_success(result: RequestResult) -> None:
        res = result.all().result()
        print(f"  callback received result: {res}")
        event.set()

    future = session.execute("SELECT * FROM users")
    future.add_callback(on_success)

    event.wait(timeout=1)


def example_add_errback(session: Session) -> None:
    """
    Register an error callback. Fires automatically when the query fails.
    """
    print("\n=== add_errback (error handling) ===")

    event = threading.Event()

    def on_error(exc: BaseException) -> None:
        print(f"  errback received: {type(exc).__name__}: {exc}")
        event.set()

    future = session.execute("SELECT * FROM nonexistent_table_xyz")
    future.add_errback(on_error)

    event.wait(timeout=1)


def example_add_callbacks(session: Session) -> None:
    """
    Register both success and error callbacks in a single call using
    add_callbacks(). Extra args/kwargs are forwarded to each callback.
    """
    print("\n=== add_callbacks (success + error in one call) ===")

    event = threading.Event()

    def on_success(result: RequestResult, tag: str) -> None:
        print(f"  [{tag}] success: got result {result}")
        event.set()

    def on_error(exc: BaseException, tag: str) -> None:
        print(f"  [{tag}] error: {exc}")
        event.set()

    future = session.execute("SELECT * FROM users")
    future.add_callbacks(
        on_success,
        on_error,
        callback_args=("query-1",),
        errback_args=("query-1",),
    )

    event.wait(timeout=1)


def example_callback_with_args(session: Session) -> None:
    """
    Callbacks accept extra positional and keyword arguments which are
    forwarded after the result (or exception) value.
    """
    print("\n=== add_callback with extra args/kwargs ===")

    event = threading.Event()

    def on_success(result: RequestResult, request_id: str, *, label: str) -> None:
        print(f"  request_id={request_id}, label={label}, result={result}")
        event.set()

    future = session.execute("SELECT * FROM users WHERE id = 1")
    future.add_callback(on_success, "req-42", label="user-lookup")

    event.wait(timeout=1)


def example_cancel(session: Session) -> None:
    """
    cancel() moves the future to a terminal error state.
    Subsequent result() calls raise RuntimeError('future was closed').
    """
    print("\n=== cancel() ===")

    future = session.execute("SELECT * FROM users")
    future.cancel()

    try:
        future.result()
    except RuntimeError as e:
        print(f"  result() after close: {e}")


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
    example_add_callback(session)
    example_add_errback(session)
    example_add_callbacks(session)
    example_callback_with_args(session)
    example_cancel(session)
    example_fully_sync()

    print("\nAll examples completed.")


asyncio.run(main())
