import asyncio
import sys

from cassandra.cluster import Session  # pyright: ignore[reportMissingTypeStubs]
from cassandra.query import PreparedStatement  # pyright: ignore[reportMissingTypeStubs]
from common import (
    COMPLEX_SELECT_QUERY,
)
from python_helpers import PAGE_SIZE, connect, to_asyncio


async def test(session: Session, prepared: PreparedStatement, cnt: int, num_of_rows: int):
    for _ in range(cnt):
        fut = to_asyncio(
            session.execute_async(  # pyright: ignore[reportUnknownMemberType]
                query=prepared,
                parameters=None,
                paging_state=None,
            )
        )
        result = await fut
        assert len(result) == num_of_rows  # pyright: ignore[reportUnknownArgumentType, reportUnknownMemberType]


async def select_data(
    session: Session,
    start_index: int,
    cnt: int,
    num_of_rows: int,
    prepared: PreparedStatement,
    concurrency: int,
) -> None:
    index = start_index

    while index < cnt:
        fut = to_asyncio(
            session.execute_async(  # pyright: ignore[reportUnknownMemberType]
                query=prepared,
                parameters=None,
                paging_state=None,
            )
        )
        result = await fut

        assert len(result) == num_of_rows
        index += concurrency


async def main() -> None:
    concurrency = int(sys.argv[1])
    num_of_rows = int(sys.argv[2])
    cnt = int(sys.argv[3])
    session = await connect()
    prepared = session.prepare(COMPLEX_SELECT_QUERY)  # pyright: ignore[reportUnknownMemberType]
    prepared.fetch_size = PAGE_SIZE

    tasks = [
        asyncio.create_task(select_data(session, i, cnt, num_of_rows, prepared, concurrency))
        for i in range(concurrency)
    ]

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
