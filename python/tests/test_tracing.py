import copy
import datetime
import ipaddress
import uuid
from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio
from helpers.ddl import ddl
from helpers.session import connect, session_builder
from scylla.errors import TraceUnavailable
from scylla.session import Session, SessionBuilder
from scylla.statement import Batch, Consistency, Statement
from scylla.tracing import QueryTrace, TraceEvent

TABLE = "tracing_table"
ROWS = 5


@pytest_asyncio.fixture(scope="module")
async def session() -> AsyncGenerator[Session, None]:
    session = await connect()
    await ddl(
        session,
        """
            CREATE KEYSPACE IF NOT EXISTS tracingks
            WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};
        """,
    )
    await session.use_keyspace("tracingks")
    await ddl(session, f"CREATE TABLE IF NOT EXISTS {TABLE} (id int PRIMARY KEY, x int);")
    for i in range(ROWS):
        await session.execute(f"INSERT INTO {TABLE} (id, x) VALUES ({i}, {i});")
    yield session
    await ddl(session, "DROP KEYSPACE tracingks")


def paged_select(page_size: int = 2) -> Statement:
    statement = Statement(f"SELECT * FROM {TABLE}")
    statement.page_size = page_size
    return statement


def test_statement_tracing_defaults_to_false_and_is_copied() -> None:
    statement = Statement("SELECT * FROM t")
    assert statement.tracing is False

    statement.tracing = True
    assert statement.tracing is True
    assert statement.copy().tracing is True
    assert copy.copy(statement).tracing is True


def test_batch_tracing_defaults_to_false_and_is_copied() -> None:
    batch = Batch()
    assert batch.tracing is False

    batch.tracing = True
    assert batch.tracing is True
    assert batch.copy().tracing is True


def test_session_builder_tracing_info_fetch_options() -> None:
    config = (
        SessionBuilder()
        .tracing_info_fetch_attempts(4)
        .tracing_info_fetch_interval(0.05)
        .tracing_info_fetch_consistency(Consistency.Quorum)
        .get_config()
    )

    assert config.tracing_info_fetch_attempts == 4
    assert config.tracing_info_fetch_interval == datetime.timedelta(milliseconds=50)
    assert config.tracing_info_fetch_consistency == Consistency.Quorum


def test_session_builder_rejects_zero_tracing_info_fetch_attempts() -> None:
    with pytest.raises((ValueError, OverflowError)):
        SessionBuilder().tracing_info_fetch_attempts(0)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_untraced_request_has_no_trace(session: Session) -> None:
    result = await session.execute(f"SELECT * FROM {TABLE}")

    assert result.first_page.trace_id is None
    assert result.trace_ids == []
    assert await result.fetch_first_page_query_trace() is None
    assert await result.first_page.fetch_query_trace() is None
    assert await result.fetch_all_query_traces() == []


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_trace_argument_traces_the_request(session: Session) -> None:
    result = await session.execute(f"SELECT * FROM {TABLE}", trace=True)

    trace_id = result.first_page.trace_id
    assert isinstance(trace_id, uuid.UUID)
    assert result.trace_ids == [trace_id]


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_statement_tracing_attribute_and_trace_argument_override(session: Session) -> None:
    statement = Statement(f"SELECT * FROM {TABLE}")
    statement.tracing = True

    traced = await session.execute(statement)
    assert traced.first_page.trace_id is not None

    overridden = await session.execute(statement, trace=False)
    assert overridden.first_page.trace_id is None
    assert statement.tracing is True


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_fetched_trace_fields(session: Session) -> None:
    result = await session.execute(f"SELECT * FROM {TABLE}", trace=True)

    trace = await result.fetch_first_page_query_trace()

    assert isinstance(trace, QueryTrace)
    assert trace.trace_id == result.first_page.trace_id
    assert isinstance(trace.request_type, str)
    assert isinstance(trace.coordinator, ipaddress.IPv4Address | ipaddress.IPv6Address)
    assert isinstance(trace.started_at, datetime.datetime)
    assert trace.started_at.tzinfo is not None
    assert trace.duration is None or isinstance(trace.duration, datetime.timedelta)
    assert isinstance(trace.parameters, dict)
    assert isinstance(trace.events, tuple)
    assert trace.events
    for event in trace.events:
        assert isinstance(event, TraceEvent)
        assert isinstance(event.event_id, uuid.UUID)
        assert isinstance(event.datetime, datetime.datetime)
        assert isinstance(event.source, ipaddress.IPv4Address | ipaddress.IPv6Address)
        assert event.source_elapsed is None or isinstance(event.source_elapsed, datetime.timedelta)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_session_fetches_trace_by_id(session: Session) -> None:
    result = await session.execute(f"SELECT * FROM {TABLE}", trace=True)
    trace_id = result.first_page.trace_id
    assert trace_id is not None

    trace = await session.fetch_query_trace(trace_id)

    assert trace.trace_id == trace_id


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_all_adds_the_ids_of_every_page(session: Session) -> None:
    result = await session.execute(paged_select(), trace=True)
    first_id = result.first_page.trace_id

    rows = await result.all()

    assert len(rows) == ROWS
    ids = result.trace_ids
    assert ids[0] == first_id
    assert len(ids) == 3
    assert len(set(ids)) == len(ids)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_pages_add_their_ids_to_the_result(session: Session) -> None:
    result = await session.execute(paged_select(), trace=True)

    page_ids = [page.trace_id async for page in result.pages()]

    assert len(page_ids) == 3
    assert all(isinstance(page_id, uuid.UUID) for page_id in page_ids)
    assert result.trace_ids == page_ids


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_fetch_next_page_adds_its_id_to_the_result(session: Session) -> None:
    result = await session.execute(paged_select(), trace=True)

    second = await result.first_page.fetch_next_page()

    assert second is not None
    assert second.trace_id is not None
    assert result.trace_ids == [result.first_page.trace_id, second.trace_id]


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_every_async_for_adds_new_ids(session: Session) -> None:
    result = await session.execute(paged_select(), trace=True)

    async for _ in result:
        pass
    async for _ in result:
        pass

    ids = result.trace_ids
    assert len(ids) == 5
    assert len(set(ids)) == 5


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_fetch_all_query_traces_follows_trace_ids(session: Session) -> None:
    result = await session.execute(paged_select(), trace=True)
    await result.all()

    traces = await result.fetch_all_query_traces()

    assert [trace.trace_id for trace in traces] == result.trace_ids


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_batch_trace_argument(session: Session) -> None:
    batch = Batch()
    batch.add(f"UPDATE {TABLE} SET x = 1 WHERE id = 0")

    traced = await session.batch(batch, trace=True)
    untraced = await session.batch(batch)

    assert traced.first_page.trace_id is not None
    assert untraced.first_page.trace_id is None


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepare_trace_ids(session: Session) -> None:
    untraced = await session.prepare(f"SELECT * FROM {TABLE}")
    assert untraced.prepare_trace_ids == ()

    statement = Statement(f"SELECT * FROM {TABLE} WHERE id = ?")
    statement.tracing = True
    traced = await session.prepare(statement)

    assert traced.prepare_trace_ids
    assert all(isinstance(trace_id, uuid.UUID) for trace_id in traced.prepare_trace_ids)
    assert traced.tracing is True


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_missing_trace_raises_trace_unavailable() -> None:
    session = await session_builder().tracing_info_fetch_attempts(1).tracing_info_fetch_interval(0.001).connect()

    with pytest.raises(TraceUnavailable):
        await session.fetch_query_trace(uuid.uuid4())
