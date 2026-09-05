import asyncio
import sys

from common import (
    SIMPLE_SELECT_QUERY,
)
from python_rs_helpers import connect
from scylla.session import Session
from scylla.statement import PreparedStatement


async def paging_worker(
    session: Session,
    prepared: PreparedStatement,
    cnt: int,
    start_index: int,
    expected_sum: int,
    concurrency: int,
) -> None:
    index = start_index

    while index < cnt:
        result = await session.execute(prepared)

        total = 0
        while result is not None:
            for row in result.iter_current_page():
                total += row["val"]

            result = await result.fetch_next_page()

        assert total == expected_sum
        index += concurrency


async def main() -> None:
    concurrency = int(sys.argv[1])
    num_of_rows = int(sys.argv[2])
    cnt = int(sys.argv[3])
    session = await connect()
    prepared = await session.prepare(SIMPLE_SELECT_QUERY)
    prepared = prepared.with_page_size(1)

    expected_sum = num_of_rows * 100

    tasks = [
        asyncio.create_task(paging_worker(session, prepared, cnt, i, expected_sum, concurrency))
        for i in range(concurrency)
    ]

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
