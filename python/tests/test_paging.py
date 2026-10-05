import asyncio
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
    prepared.page_size = page_size

    result = await session.execute(prepared)

    seen_ids: list[int] = []

    page = result.first_page
    while True:
        rows = list(page)
        seen_ids.extend(row["id"] for row in rows)
        if page.has_more_pages:
            assert len(rows) == page_size
            next_page = await page.fetch_next_page()
            assert next_page is not None
            page = next_page
        else:
            break
    assert page.has_more_pages is False
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
    statement.page_size = page_size

    result = await session.execute(statement)

    seen_ids: list[int] = []

    page = result.first_page
    while True:
        rows = list(page)
        seen_ids.extend(row["id"] for row in rows)
        if page.has_more_pages:
            assert len(rows) == page_size
            next_page = await page.fetch_next_page()
            assert next_page is not None
            page = next_page
        else:
            break

    assert page.has_more_pages is False
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
    prepared.page_size = page_size

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
    statement.page_size = page_size

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

    # Not a multiple of the page size, so the resumed page is the last one.
    await insert_rows(session, table, 15)

    prepared = await session.prepare(f"SELECT * FROM {table}")
    prepared.page_size = 10

    result1 = await session.execute(prepared)

    first_page = list(result1.first_page)
    state = result1.first_page.paging_state

    assert state is not None

    # Resume using paging state
    result2 = await session.execute(
        prepared,
        paging_state=state,
    )

    second_page = list(result2.first_page)

    ids_first = {row["id"] for row in first_page}
    ids_second = {row["id"] for row in second_page}

    assert ids_first.isdisjoint(ids_second)
    assert ids_first | ids_second == set(range(15))
    assert result2.first_page.has_more_pages is False


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_paging_state_resumes_after_bytes_roundtrip(
    session: Session,
    table_factory: TableFactory,
):
    table = await table_factory(
        "id int PRIMARY KEY, x int",
        "paging_resume_bytes_table",
    )

    await insert_rows(session, table, 15)

    prepared = await session.prepare(f"SELECT * FROM {table}")
    prepared.page_size = 10

    result = await session.execute(prepared)
    state = result.first_page.paging_state
    assert state is not None
    raw = state.as_bytes()
    assert raw is not None

    resumed = await session.execute(prepared, paging_state=PagingState.from_bytes(raw))

    expected = await result.first_page.fetch_next_page()
    assert expected is not None
    assert list(resumed.first_page) == list(expected)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_start_paging_state_starts_at_first_page(
    session: Session,
    table_factory: TableFactory,
):
    table = await table_factory(
        "id int PRIMARY KEY, x int",
        "paging_start_state_table",
    )

    await insert_rows(session, table, 15)

    prepared = await session.prepare(f"SELECT * FROM {table}")
    prepared.page_size = 10

    default = await session.execute(prepared)
    explicit = await session.execute(prepared, paging_state=PagingState())

    assert list(explicit.first_page) == list(default.first_page)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_unpaged_execution_returns_every_row_in_one_page(
    session: Session,
    table_factory: TableFactory,
):
    table = await table_factory(
        "id int PRIMARY KEY, x int",
        "paging_unpaged_table",
    )

    await insert_rows(session, table, 15)

    statement = Statement(f"SELECT * FROM {table}")
    statement.page_size = 10

    result = await session.execute(statement, paged=False)
    page = result.first_page

    assert sorted(row["id"] for row in page) == list(range(15))
    assert page.has_more_pages is False
    assert page.paging_state is None
    assert await page.fetch_next_page() is None


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
    prepared.page_size = page_size

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
    prepared.page_size = 2

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


@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize("total_rows,page_size", [(25, 10), (100, 1), (20, 100), (0, 10)])
async def test_pages_yields_every_page_starting_with_first_page(
    session: Session,
    table_factory: TableFactory,
    total_rows: int,
    page_size: int,
):
    table = await table_factory(
        "id int PRIMARY KEY, x int",
        "paging_pages_table",
    )

    await insert_rows(session, table, total_rows)

    prepared = await session.prepare(f"SELECT * FROM {table}")
    prepared.page_size = page_size

    result = await session.execute(prepared)

    pages = [page async for page in result.pages()]

    assert list(pages[0]) == list(result.first_page)
    assert all(page.has_more_pages for page in pages[:-1])
    assert pages[-1].has_more_pages is False

    ids = [row["id"] for page in pages for row in page]
    assert sorted(ids) == list(range(total_rows))


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_pages_can_be_read_with_blocking_result(
    session: Session,
    table_factory: TableFactory,
):
    table = await table_factory(
        "id int PRIMARY KEY, x int",
        "paging_pages_tokio_table",
    )

    await insert_rows(session, table, 5)

    prepared = await session.prepare(f"SELECT * FROM {table}")
    prepared.page_size = 2

    result = await session.execute(prepared)
    pages = result.pages()

    # `result()` runs each fetch on a tokio worker, not polled by the event loop.
    ids: list[int] = []
    while True:
        try:
            page = pages.__anext__().result()
        except StopAsyncIteration:
            break
        ids.extend(row["id"] for row in page)

    assert sorted(ids) == list(range(5))


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_pages_of_non_rows_result_is_one_empty_page(
    session: Session,
    table_factory: TableFactory,
):
    table = await table_factory(
        "id int PRIMARY KEY, x int",
        "paging_pages_non_rows_table",
    )

    result = await session.execute(f"INSERT INTO {table} (id, x) VALUES (1, 1)")

    pages = [page async for page in result.pages()]

    assert len(pages) == 1
    assert list(pages[0]) == []
    assert pages[0].has_more_pages is False
    assert pages[0].paging_state is None


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_page_is_immutable(
    session: Session,
    table_factory: TableFactory,
):
    table = await table_factory(
        "id int PRIMARY KEY, x int",
        "paging_page_immutable_table",
    )

    await insert_rows(session, table, 4)

    prepared = await session.prepare(f"SELECT * FROM {table}")
    prepared.page_size = 2

    result = await session.execute(prepared)
    page = result.first_page
    rows = list(page)
    state = page.paging_state

    next_page = await page.fetch_next_page()

    assert next_page is not None
    assert list(page) == rows
    assert page.paging_state == state
    assert page.has_more_pages is True
    assert {row["id"] for row in rows}.isdisjoint({row["id"] for row in next_page})


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_fetch_next_page_of_last_page_returns_none(
    session: Session,
    table_factory: TableFactory,
):
    table = await table_factory(
        "id int PRIMARY KEY, x int",
        "paging_last_page_table",
    )

    await insert_rows(session, table, 3)

    prepared = await session.prepare(f"SELECT * FROM {table}")
    prepared.page_size = 10

    result = await session.execute(prepared)
    page = result.first_page

    assert page.has_more_pages is False
    assert page.paging_state is None
    assert await page.fetch_next_page() is None


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_result_is_consumed_from_first_page_after_walking_pages(
    session: Session,
    table_factory: TableFactory,
):
    table = await table_factory(
        "id int PRIMARY KEY, x int",
        "paging_whole_result_table",
    )

    await insert_rows(session, table, 10)

    prepared = await session.prepare(f"SELECT * FROM {table}")
    prepared.page_size = 3

    result = await session.execute(prepared)
    second_page = await result.first_page.fetch_next_page()
    assert second_page is not None

    rows = await result.all()
    iterated = [row async for row in result]
    first_row = await result.first_row()
    paged = [row async for page in result.pages() for row in page]

    assert sorted(row["id"] for row in rows) == list(range(10))
    assert iterated == rows
    assert first_row == rows[0]
    assert paged == rows


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_pages_can_be_iterated_more_than_once(
    session: Session,
    table_factory: TableFactory,
):
    table = await table_factory(
        "id int PRIMARY KEY, x int",
        "paging_pages_twice_table",
    )

    await insert_rows(session, table, 10)

    prepared = await session.prepare(f"SELECT * FROM {table}")
    prepared.page_size = 3

    result = await session.execute(prepared)

    first = [list(page) async for page in result.pages()]
    second = [list(page) async for page in result.pages()]

    assert first == second
    assert sorted(row["id"] for page in first for row in page) == list(range(10))


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_fetch_next_page_twice_returns_the_same_rows(
    session: Session,
    table_factory: TableFactory,
):
    table = await table_factory(
        "id int PRIMARY KEY, x int",
        "paging_fetch_twice_table",
    )

    await insert_rows(session, table, 6)

    prepared = await session.prepare(f"SELECT * FROM {table}")
    prepared.page_size = 3

    page = (await session.execute(prepared)).first_page

    first = await page.fetch_next_page()
    second = await page.fetch_next_page()

    assert first is not None
    assert second is not None
    assert list(first) == list(second)
    assert first.paging_state == second.paging_state


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_concurrent_anext_on_pages_yields_each_page_once(
    session: Session,
    table_factory: TableFactory,
):
    table = await table_factory(
        "id int PRIMARY KEY, x int",
        "paging_pages_concurrent_table",
    )

    # Pages of 3, 3 and 1 rows: the last page is not full, so it is the last one.
    await insert_rows(session, table, 7)

    prepared = await session.prepare(f"SELECT * FROM {table}")
    prepared.page_size = 3

    pages = aiter((await session.execute(prepared)).pages())

    fetched = await asyncio.gather(anext(pages), anext(pages), anext(pages))
    ids = [row["id"] for page in fetched for row in page]

    assert sorted(ids) == list(range(7))
    with pytest.raises(StopAsyncIteration):
        await anext(pages)


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
