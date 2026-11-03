import asyncio
import sys

from cassandra.cluster import Session  # pyright: ignore[reportMissingTypeStubs]
from cassandra.query import PreparedStatement  # pyright: ignore[reportMissingTypeStubs]
from common import (
    COMPLEX_INSERT_QUERY,
    get_complex_data,
)
from python_helpers import PAGE_SIZE, connect, convert_complex_data_for_cassandra, to_asyncio


async def test(session: Session, prepared: PreparedStatement, cnt: int):
    for _ in range(cnt):
        fut = to_asyncio(
            session.execute_async(  # pyright: ignore[reportUnknownMemberType]
                query=prepared,
                parameters=convert_complex_data_for_cassandra(get_complex_data()),
                paging_state=None,
            )
        )
        await fut


async def main():
    cnt = int(sys.argv[1])
    session = await connect()
    prepared = session.prepare(COMPLEX_INSERT_QUERY)  # pyright: ignore[reportUnknownMemberType]
    prepared.fetch_size = PAGE_SIZE

    await test(session, prepared, cnt)


if __name__ == "__main__":
    asyncio.run(main())
