import asyncio
import sys

from cassandra.cluster import Session  # pyright: ignore[reportMissingTypeStubs]
from cassandra.query import PreparedStatement  # pyright: ignore[reportMissingTypeStubs]
from common import (
    SIMPLE_SELECT_QUERY,
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


async def main():
    num_of_rows = int(sys.argv[1])
    cnt = int(sys.argv[2])
    session = await connect()

    prepared = session.prepare(SIMPLE_SELECT_QUERY)  # pyright: ignore[reportUnknownMemberType]
    prepared.fetch_size = PAGE_SIZE

    await test(session, prepared, cnt, num_of_rows)


if __name__ == "__main__":
    asyncio.run(main())
