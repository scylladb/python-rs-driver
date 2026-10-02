"""
LegacySession — the ``cassandra-driver`` compatible API.

No event loop: ``connect_legacy()``, ``execute()`` and ``prepare()`` block,
``execute_async()`` returns a ``ResponseFuture`` that delivers its result
through ``result()`` or through ``add_callback``/``add_errback``.
"""

import threading
from typing import Any

from scylla.errors import ScyllaError
from scylla.legacy import LegacySession, ResponseFuture, ResultSet
from scylla.session_builder import SessionBuilder
from scylla.statement import Statement

CONTACT_POINTS = [("127.0.0.2", 9042)]
KEYSPACE = "legacy_example_ks"


def setup() -> LegacySession:
    session = SessionBuilder().contact_points(CONTACT_POINTS).connect_legacy()
    session.execute(
        f"CREATE KEYSPACE IF NOT EXISTS {KEYSPACE} "
        "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};"
    )
    session.set_keyspace(KEYSPACE)
    session.execute("CREATE TABLE IF NOT EXISTS users (id int PRIMARY KEY, name text);")
    session.execute("TRUNCATE users;")

    insert = session.prepare("INSERT INTO users (id, name) VALUES (?, ?)")
    for i in range(25):
        session.execute(insert, [i, f"user_{i}"])
    return session


def example_execute(session: LegacySession) -> None:
    """execute() blocks and returns a ResultSet: iterate it, or take one()/all()."""
    print("\n=== execute() ===")

    rows: ResultSet = session.execute("SELECT id, name FROM users WHERE id = ?", [3])
    print(f"  one(): {rows.one()}")

    for row in session.execute("SELECT id, name FROM users LIMIT 3"):
        print(f"  {row}")


def example_paging(session: LegacySession) -> None:
    """Iteration fetches further pages transparently; pages can also be walked by hand."""
    print("\n=== paging ===")

    statement = Statement("SELECT id FROM users").with_page_size(10)
    total = sum(1 for _ in session.execute(statement))
    print(f"  iterated {total} rows across pages")

    rows = session.execute(statement)
    pages = 1
    while rows.has_more_pages:
        rows.fetch_next_page()
        pages += 1
    print(f"  walked {pages} pages by hand")


def example_execute_async(session: LegacySession) -> None:
    """execute_async() returns immediately; result() blocks for the ResultSet."""
    print("\n=== execute_async() + result() ===")

    future: ResponseFuture = session.execute_async("SELECT count(*) FROM users")
    # ... do other work ...
    print(f"  count: {future.result().one()}")


def example_callbacks(session: LegacySession) -> None:
    """Callbacks get the rows of a page; errbacks get the exception. Extra args pass through."""
    print("\n=== add_callbacks ===")

    done = threading.Event()

    def on_rows(rows: list[Any], label: str) -> None:
        print(f"  {label}: {len(rows)} rows")
        done.set()

    def on_error(exc: Exception, label: str) -> None:
        print(f"  {label}: {type(exc).__name__}: {exc}")
        done.set()

    session.execute_async("SELECT id FROM users").add_callbacks(
        on_rows, on_error, callback_args=("ok",), errback_args=("failed",)
    )
    done.wait(timeout=5)

    done.clear()
    session.execute_async("SELECT id FROM no_such_table").add_callbacks(
        on_rows, on_error, callback_args=("ok",), errback_args=("failed",)
    )
    done.wait(timeout=5)


def example_callback_paging(session: LegacySession) -> None:
    """Callbacks persist across pages: start_fetching_next_page() fires them again."""
    print("\n=== callbacks across pages ===")

    finished = threading.Event()
    seen: list[Any] = []

    def on_page(rows: list[Any], future: ResponseFuture) -> None:
        seen.extend(rows)
        if future.has_more_pages:
            future.start_fetching_next_page()
        else:
            finished.set()

    future = session.execute_async(Statement("SELECT id FROM users").with_page_size(10))
    future.add_callback(on_page, future)
    finished.wait(timeout=5)
    print(f"  collected {len(seen)} rows page by page")


def example_errors(session: LegacySession) -> None:
    """result() raises the request's error."""
    print("\n=== errors ===")

    try:
        session.execute("SELECT * FROM no_such_table")
    except ScyllaError as exc:
        print(f"  {type(exc).__name__}: {exc}")


def main() -> None:
    session = setup()

    example_execute(session)
    example_paging(session)
    example_execute_async(session)
    example_callbacks(session)
    example_callback_paging(session)
    example_errors(session)

    session.shutdown()
    print("\nAll examples completed.")


main()
