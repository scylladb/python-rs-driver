import asyncio
import sys

from cassandra.cluster import Session  # pyright: ignore[reportMissingTypeStubs]
from cassandra.query import PreparedStatement  # pyright: ignore[reportMissingTypeStubs]
from common import (
    SIMPLE_INSERT_QUERY,
    get_simple_data,
)
from python_helpers import PAGE_SIZE, connect, to_asyncio


async def insert_data(
    session: Session,
    start_index: int,
    cnt: int,
    prepared: PreparedStatement,
    concurrency: int,
) -> None:
    index = start_index

    while index < cnt:
        fut = to_asyncio(
            session.execute_async(  # pyright: ignore[reportUnknownMemberType]
                query=prepared,
                parameters=get_simple_data(),
                paging_state=None,
            )
        )
        await fut
        index += concurrency


async def main() -> None:
    concurrency = int(sys.argv[1])
    cnt = int(sys.argv[2])
    session = await connect()
    prepared = session.prepare(SIMPLE_INSERT_QUERY)  # pyright: ignore[reportUnknownMemberType]
    prepared.fetch_size = PAGE_SIZE

    tasks = [asyncio.create_task(insert_data(session, i, cnt, prepared, concurrency)) for i in range(concurrency)]

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
