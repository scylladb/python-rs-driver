from collections.abc import AsyncGenerator, Awaitable, Callable
from typing import Any

import pytest
import pytest_asyncio
from helpers.ddl import ddl
from helpers.session import connect
from scylla.results import PagingState
from scylla.session import Session
from scylla.statement import Statement


async def set_up() -> Session:
    session = await connect()

    # 2. Create keyspace & table
    await ddl(
        session,
        """
            CREATE KEYSPACE IF NOT EXISTS testks
            WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};
        """,
    )

    await session.use_keyspace("testks")

    return session


@pytest_asyncio.fixture(scope="module")
async def session():
    session = await set_up()
    yield session
    await ddl(session, "DROP KEYSPACE testks")


TableFactory = Callable[[str, str], Awaitable[str]]


@pytest_asyncio.fixture
async def table_factory(session: Session) -> AsyncGenerator[TableFactory, None]:
    created_tables: list[str] = []

    async def create_table(schema: str, name: str) -> str:
        await ddl(session, f"CREATE TABLE IF NOT EXISTS {name} ({schema});")
        created_tables.append(name)
        return name

    yield create_table

    for table in created_tables:
        await ddl(session, f"DROP TABLE IF EXISTS {table};")


async def insert_rows(
    session: Session,
    table: str,
    count: int,
):
    for i in range(count):
        await session.execute(f"INSERT INTO {table} (id, x) VALUES ({i}, {i * 10});")


@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize("total_rows,page_size", [(25, 10), (100, 10), (100, 1), (20, 100), (0, 10)])
async def test_execute_paged_basic_flow(session: Session, table_factory: TableFactory, total_rows: int, page_size: int):
    table = await table_factory(
        "id int PRIMARY KEY, x int",
        "paging_basic_table",
    )

    await insert_rows(session, table, total_rows)

    prepared = await session.prepare(f"SELECT * FROM {table}")
    prepared = prepared.with_page_size(page_size)

    paging_result = await session.execute(prepared)

    seen_ids: list[int] = []

    while True:
        page = list(paging_result.iter_current_page())
        seen_ids.extend(row["id"] for row in page)
        if paging_result.has_more_pages():
            assert len(page) == page_size
            next_page = await paging_result.fetch_next_page()
            assert next_page is not None
            paging_result = next_page
        else:
            break
    assert paging_result.has_more_pages() is False
    assert len(seen_ids) == total_rows

    assert sorted(seen_ids) == list(range(total_rows))


@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize("total_rows,page_size", [(25, 10), (100, 10), (100, 1), (20, 100), (0, 10)])
async def test_execute_paged_basic_flow_for_unprepared_statements(
    session: Session, table_factory: TableFactory, total_rows: int, page_size: int
):
    table = await table_factory(
        "id int PRIMARY KEY, x int",
        "paging_basic_table",
    )

    await insert_rows(session, table, total_rows)

    statement = Statement(f"SELECT * FROM {table}")
    statement = statement.with_page_size(page_size)

    paging_result = await session.execute(statement)

    seen_ids: list[int] = []

    while True:
        page = list(paging_result.iter_current_page())
        seen_ids.extend(row["id"] for row in page)
        if paging_result.has_more_pages():
            assert len(page) == page_size
            next_page = await paging_result.fetch_next_page()
            assert next_page is not None
            paging_result = next_page
        else:
            break

    assert paging_result.has_more_pages() is False
    assert len(seen_ids) == total_rows

    assert sorted(seen_ids) == list(range(total_rows))


@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize(
    "total_rows,page_size",
    [(25, 10), (100, 10), (100, 1), (20, 100), (0, 10)],
)
async def test_execute_async_paged_basic_flow(
    session: Session,
    table_factory: TableFactory,
    total_rows: int,
    page_size: int,
):
    table = await table_factory(
        "id int PRIMARY KEY, x int",
        "paging_async_basic_table",
    )

    await insert_rows(session, table, total_rows)

    prepared = await session.prepare(f"SELECT * FROM {table}")
    prepared = prepared.with_page_size(page_size)

    rows_iter = await session.execute(prepared)

    seen_ids: list[Any] = []

    async for row in rows_iter:
        assert isinstance(row, dict)
        assert "id" in row

        seen_ids.append(row["id"])

    assert sorted(seen_ids) == list(range(total_rows))


@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize(
    "total_rows,page_size",
    [(25, 10), (100, 10), (100, 1), (20, 100), (0, 10)],
)
async def test_execute_async_paged_for_string_query(
    session: Session,
    table_factory: TableFactory,
    total_rows: int,
    page_size: int,
):
    table = await table_factory(
        "id int PRIMARY KEY, x int",
        "paging_async_basic_table",
    )

    await insert_rows(session, table, total_rows)

    statement = Statement(f"SELECT * FROM {table}")
    statement = statement.with_page_size(page_size)

    rows_iter = await session.execute(statement)

    seen_ids: list[Any] = []

    async for row in rows_iter:
        assert isinstance(row, dict)
        assert "id" in row

        seen_ids.append(row["id"])

    assert sorted(seen_ids) == list(range(total_rows))


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_paging_state_resume(
    session: Session,
    table_factory: TableFactory,
):
    table = await table_factory(
        "id int PRIMARY KEY, x int",
        "paging_resume_table",
    )

    await insert_rows(session, table, 20)

    prepared = await session.prepare(f"SELECT * FROM {table}")
    prepared = prepared.with_page_size(10)

    result1 = await session.execute(prepared)

    first_page = list(result1.iter_current_page())
    state = result1.paging_state()

    assert state is not None

    # Resume using paging state
    result2 = await session.execute(
        prepared,
        paging_state=state,
    )

    second_page = list(result2.iter_current_page())

    ids_first = {row["id"] for row in first_page}
    ids_second = {row["id"] for row in second_page}

    assert ids_first.isdisjoint(ids_second)


@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize("total_rows,page_size", [(0, 10), (5, 2), (25, 10), (1000, 10)])
async def test_paging_all_returns_all_rows(
    session: Session,
    table_factory: TableFactory,
    total_rows: int,
    page_size: int,
):
    table = await table_factory(
        "id int PRIMARY KEY, x int",
        "paging_all_table",
    )

    await insert_rows(session, table, total_rows)

    prepared = await session.prepare(f"SELECT * FROM {table}")
    prepared = prepared.with_page_size(page_size)

    result = await session.execute(prepared)

    rows = await result.all()

    assert isinstance(rows, list)
    assert len(rows) == total_rows

    ids = [row["id"] for row in rows]
    assert sorted(ids) == list(range(total_rows))


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_paging_one_returns_none_for_empty_result(
    session: Session,
    table_factory: TableFactory,
):
    table = await table_factory(
        "id int PRIMARY KEY, x int",
        "paging_one_empty_table",
    )

    prepared = await session.prepare(f"SELECT * FROM {table}")
    result = await session.execute(prepared)

    row = await result.first_row()

    assert row is None


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_paging_one_returns_first_row(
    session: Session,
    table_factory: TableFactory,
):
    table = await table_factory(
        "id int PRIMARY KEY, x int",
        "paging_one_table",
    )

    await insert_rows(session, table, 1)

    prepared = await session.prepare(f"SELECT * FROM {table}")
    prepared = prepared.with_page_size(2)

    result = await session.execute(prepared)

    row = await result.first_row()

    assert row is not None
    assert row["id"] == 0


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_first_for_non_rows_result_returns_none(
    session: Session,
    table_factory: TableFactory,
):
    table = await table_factory(
        "id int PRIMARY KEY, x int",
        "paging_one_table",
    )
    result = await session.execute(f"INSERT INTO {table} (id, x) VALUES (1000, 42)")

    row_first = await result.first_row()
    row_all = await result.all()

    assert row_first is None
    assert row_all == []


def test_paging_state_new_is_start_state():
    state = PagingState()

    assert state.as_bytes() is None


def test_paging_state_from_bytes_roundtrip():
    raw = b"\x01\x02\x03\x04"

    state = PagingState.from_bytes(raw)

    assert state.as_bytes() == raw


def test_paging_state_start_and_from_bytes_are_not_equal():
    start_state = PagingState()
    resumed_state = PagingState.from_bytes(b"\x01\x02\x03")

    assert start_state != resumed_state


def test_paging_state_equal_for_same_raw_bytes():
    state1 = PagingState.from_bytes(b"\x01\x02\x03")
    state2 = PagingState.from_bytes(b"\x01\x02\x03")

    assert state1 == state2


def test_paging_state_not_equal_for_different_raw_bytes():
    state1 = PagingState.from_bytes(b"\x01\x02\x03")
    state2 = PagingState.from_bytes(b"\x04\x05\x06")

    assert state1 != state2


def test_paging_state_from_empty_bytes_is_not_start_state():
    state = PagingState.from_bytes(b"")

    assert state.as_bytes() == b""
    assert state != PagingState()
