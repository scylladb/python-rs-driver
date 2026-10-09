"""
paging.py

Example showcasing multiple ways to consume ScyllaDB query results with the new Python driver API.

This file demonstrates:
  1) Simple async iteration over all rows (auto-paging under the hood)
  2) Manual paging: first_page + Page.fetch_next_page(), or pages()
  3) Manual paging with explicit PagingState resume
  4) Convenience helpers: first_row() and all()
  5) Built-in row factories and custom row shaping

"""

import asyncio
import os
from typing import Any

from scylla.results import ColumnSpec, DictRowFactory, Page, RowBuilder, RowFactory, TupleRowFactory
from scylla.session import Session, SessionBuilder
from scylla.statement import Statement


# ----------------------------
# DB setup helpers
# ----------------------------
async def setup_schema(session: Session) -> None:
    # Create keyspace & table.
    await session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS examples_ks
        WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};
        """
    )
    await session.use_keyspace("examples_ks")

    await session.execute(
        """
        CREATE TABLE IF NOT EXISTS select_paging (
                                                     a int,
                                                     b int,
                                                     c text,
                                                     PRIMARY KEY (a, b)
            );
        """
    )

    # Insert a small deterministic dataset.
    # (Re-inserting is fine: primary key makes rows idempotent for the same keys.)
    for i in range(16):
        await session.execute(
            "INSERT INTO select_paging (a, b, c) VALUES (?, ?, 'abc')",
            (i, 2 * i),
        )


# ----------------------------
# 1) Easiest: async for (auto-paging)
# ----------------------------
async def example_async_for(session: Session) -> None:
    print("\n=== 1) Async iteration over all rows (auto-paging) ===")

    # Unprepared string query (supports str | Statement | PreparedStatement).
    result = await session.execute("SELECT a, b, c FROM select_paging")

    async for row in result:
        # Default row representation: a namedtuple
        print(f"row={row}")


# ----------------------------
# 2) Manual paging: first_page + Page.fetch_next_page(), or pages()
# ----------------------------
async def example_manual_paging_unprepared(session: Session) -> None:
    print("\n=== 2) Manual paging (unprepared Statement) ===")

    stmt = Statement("SELECT a, b, c FROM select_paging")
    stmt.page_size = 6
    result = await session.execute(stmt)

    # A Page never changes: fetch_next_page() returns a new Page, or None after the last one.
    page: Page | None = result.first_page
    page_no = 1
    while page is not None:
        # Iterating a page yields only its own rows and never fetches:
        page_rows: list[Any] = list(page)

        print(f"page {page_no}: {len(page_rows)} rows -> {page_rows}")

        page = await page.fetch_next_page()
        page_no += 1


async def example_manual_paging_prepared(session: Session) -> None:
    print("\n=== 3) Manual paging (prepared statement) ===")

    prepared = await session.prepare("SELECT a, b, c FROM select_paging")
    # Setting page size on the prepared statement applies to all executions of it
    prepared.page_size = 7

    result = await session.execute(prepared)

    # pages() walks the same pages, starting with first_page:
    page_no = 1
    async for page in result.pages():
        # Each page holds 7 rows, except maybe the last one:
        print(f"page {page_no}: {len(list(page))} rows")
        page_no += 1


# ----------------------------
# 3) PagingState: resume later
# ----------------------------
async def example_paging_state_resume(session: Session) -> None:
    print("\n=== 4) PagingState resume ===")

    prepared = await session.prepare("SELECT a, b, c FROM select_paging")
    prepared.page_size = 5

    # Fetch first page
    result = await session.execute(prepared)

    seen_rows: list[Any] = []

    while True:
        page = list(result.first_page)
        print(f"page size={len(page)}")
        seen_rows.extend(row for row in page)

        state = result.first_page.paging_state

        # Check if more pages are available via paging state. If None, no more pages.
        if state is None:
            break

        # Resume: new execute call with the returned paging_state starts from "after the first page"
        result = await session.execute(
            prepared,
            paging_state=state,
        )


# ----------------------------
# 4) Convenience helpers: first_row() and all()
# ----------------------------
async def example_first_row_and_all(session: Session) -> None:
    print("\n=== 5) Convenience helpers: first_row() and all() ===")

    prepared = await session.prepare("SELECT a, b, c FROM select_paging")
    prepared.page_size = 4

    result = await session.execute(prepared)

    # first_row(): returns one row or None (does not force consuming the full result set)
    one = await result.first_row()
    print(f"first_row() -> {one}")

    # all(): eagerly fetches all pages and materializes into a list; it always
    # starts from the first page, so calling it after first_row() still returns every row
    rows = await result.all()
    print(f"all() -> {len(rows)} rows")


# ----------------------------
# Custom row factory examples
# ----------------------------
class SelectedColumnsDictFactory(RowFactory):
    """
    Keep only selected columns in the produced row dict.

    `prepare` runs once per page, so the positions to keep are resolved
    against the result metadata before any row is built.
    """

    def __init__(self, columns: list[str]) -> None:
        self.columns = set(columns)

    def prepare(self, columns: tuple[ColumnSpec, ...]) -> RowBuilder:
        kept = [(index, spec.name) for index, spec in enumerate(columns) if spec.name in self.columns]

        return lambda values: {name: values[index] for index, name in kept}


class UppercaseKeysDictFactory(RowFactory):
    """
    Example: dict row, but keys uppercased.
    """

    def prepare(self, columns: tuple[ColumnSpec, ...]) -> RowBuilder:
        names = [spec.name.upper() for spec in columns]

        return lambda values: dict(zip(names, values))


# ----------------------------
# 5) Built-in and custom row shapes
# ----------------------------
async def example_custom_row_factory(session: Session) -> None:
    print("\n=== 5) Row factories ===")

    stmt = Statement("SELECT a, b, c FROM select_paging")
    stmt.page_size = 20

    # Without a factory, rows are named tuples: row.a, row[0] and unpacking all work.
    rows = await (await session.execute(stmt)).all()
    print(f"default (named tuples); first row -> {rows[:1]}")

    # Built-in factories cover the common shapes.
    rows = await (await session.execute(stmt, factory=DictRowFactory())).all()
    print(f"DictRowFactory(); first row -> {rows[:1]}")

    rows = await (await session.execute(stmt, factory=TupleRowFactory())).all()
    print(f"TupleRowFactory(); first row -> {rows[:1]}")

    # Any callable is accepted as the row builder directly.
    rows = await (await session.execute(stmt, factory=lambda values: values[0])).all()
    print(f"lambda taking the first column; first rows -> {rows[:3]}")

    result = await session.execute(stmt, factory=UppercaseKeysDictFactory())
    rows = await result.all()
    print(f"UppercaseKeysDictFactory(); first row -> {rows[:1]}")

    # And async iteration will now yield only the selected columns as dict keys:
    factory = SelectedColumnsDictFactory(["a", "c"])
    result = await session.execute(stmt, factory=factory)

    seen: list[Any] = []
    async for row in result:
        seen.append(row)

    print(f"async for with SelectedColumnsDictFactory (first 3) -> {seen[:3]}")


async def main() -> None:
    uri = os.getenv("SCYLLA_URI", "127.0.0.2:9042")
    host, port_str = uri.split(":")
    port = int(port_str)

    print(f"Connecting to {host}:{port} ...")
    session = await SessionBuilder().contact_points((host, port)).connect()

    await setup_schema(session)

    await example_async_for(session)
    await example_manual_paging_unprepared(session)
    await example_manual_paging_prepared(session)
    await example_paging_state_resume(session)
    await example_first_row_and_all(session)
    await example_custom_row_factory(session)

    # Cleanup
    await session.execute("DROP TABLE IF EXISTS examples_ks.select_paging")
    print("\nTable dropped.")

    print("\nOk.")


if __name__ == "__main__":
    asyncio.run(main())
