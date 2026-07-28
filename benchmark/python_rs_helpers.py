import asyncio
import os
from typing import Any, Awaitable, Callable

from scylla.session import Session
from scylla.session_builder import SessionBuilder
from scylla.statement import Statement


async def connect() -> Session:
    uri = os.getenv("SCYLLA_URI", "127.0.0.2:9042")
    builder = SessionBuilder().contact_points(uri)
    return await builder.connect()


async def init_common() -> Session:
    session = await connect()

    await session.execute("""
       CREATE KEYSPACE IF NOT EXISTS benchmarks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1 }
    """)

    await session.execute("USE benchmarks")
    return session


async def init_simple_table() -> Session:
    session = await init_common()
    await simple_table_cleanup(session)
    await session.execute("CREATE TABLE benchmarks.basic (id uuid, val int, PRIMARY KEY(id))")
    return session


async def init_complex_table() -> Session:
    session = await init_common()
    await complex_table_cleanup(session)
    await session.execute("CREATE TYPE benchmarks.udt1 (field1 text, field2 int)")
    await session.execute(
        "CREATE TABLE benchmarks.complex (id uuid, val int, tuuid timeuuid, ip inet, date date, time time, tuple frozen<tuple<text, int>>, udt frozen<udt1>, set1 set<int>, duration duration, PRIMARY KEY(id))"
    )
    return session


async def init_with_inserts(
    num_of_rows: int,
    query: str,
    init: Callable[[], Awaitable[Session]],
    get_data: Callable[[], Any],
) -> Session:
    session = await init()

    statement = Statement(query)
    prepared = await session.prepare(statement)

    tasks = [session.execute(prepared, get_data(), paged=False) for _ in range(num_of_rows)]

    await asyncio.gather(*tasks)

    return session


async def simple_table_cleanup(session: Session):
    await session.execute("DROP TABLE IF EXISTS benchmarks.basic")


async def complex_table_cleanup(session: Session):
    await session.execute("DROP TABLE IF EXISTS benchmarks.complex")
    await session.execute("DROP TYPE IF EXISTS benchmarks.udt1")


async def check_row_cnt(session: Session, cnt: int, query: str):
    res = await session.execute(query, None, paged=False)

    count = await res.first_row()
    assert count is not None
    assert count["count"] == cnt
