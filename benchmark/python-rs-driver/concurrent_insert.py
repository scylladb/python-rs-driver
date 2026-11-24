import asyncio
import sys

from common import (
    SIMPLE_INSERT_QUERY,
    get_simple_data,
)
from python_rs_helpers import connect
from scylla.session import Session
from scylla.statement import PreparedStatement


async def insert_data(
    session: Session,
    start_index: int,
    cnt: int,
    prepared: PreparedStatement,
    concurrency: int,
) -> None:
    index = start_index

    while index < cnt:
        await session.execute(prepared, get_simple_data(), paged=False)
        index += concurrency


async def main() -> None:
    concurrency = int(sys.argv[1])
    cnt = int(sys.argv[2])
    session = await connect()
    prepared = await session.prepare(SIMPLE_INSERT_QUERY)

    tasks = [asyncio.create_task(insert_data(session, i, cnt, prepared, concurrency)) for i in range(concurrency)]

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
