import asyncio
import sys

from common import (
    COMPLEX_SELECT_QUERY,
)
from python_rs_helpers import connect
from scylla.session import Session
from scylla.statement import PreparedStatement


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
        res = await session.execute(prepared, None, paged=False)

        rows = await res.all()

        assert len(rows) == num_of_rows
        index += concurrency


async def main() -> None:
    concurrency = int(sys.argv[1])
    num_of_rows = int(sys.argv[2])
    cnt = int(sys.argv[3])
    session = await connect()
    prepared = await session.prepare(COMPLEX_SELECT_QUERY)

    tasks = [
        asyncio.create_task(select_data(session, i, cnt, num_of_rows, prepared, concurrency))
        for i in range(concurrency)
    ]

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
