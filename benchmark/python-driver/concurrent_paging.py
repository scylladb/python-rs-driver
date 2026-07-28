import asyncio
import sys

from cassandra.cluster import Session  # pyright: ignore[reportMissingTypeStubs]
from cassandra.query import PreparedStatement  # pyright: ignore[reportMissingTypeStubs]
from common import (
    SIMPLE_SELECT_QUERY,
)
from python_helpers import connect, to_asyncio


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
        total = 0
        paging_state = None

        while True:
            cass_fut = session.execute_async(  # pyright: ignore[reportUnknownMemberType]
                query=prepared,
                paging_state=paging_state,
            )
            result = await to_asyncio(cass_fut)

            # Count rows from current page
            for row in result:
                total += row.val

            # Get paging state from cassandra future's result object
            # This should not block as the future has already been awaited
            result_obj = cass_fut.result()  # pyright: ignore[reportUnknownMemberType]
            paging_state = result_obj.paging_state  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            if paging_state is None:
                break

        assert total == expected_sum
        index += concurrency


async def main():
    concurrency = int(sys.argv[1])
    num_of_rows = int(sys.argv[2])
    cnt = int(sys.argv[3])
    session = await connect()
    prepared = session.prepare(SIMPLE_SELECT_QUERY)  # pyright: ignore[reportUnknownMemberType]
    prepared.fetch_size = 1

    expected_sum = num_of_rows * 100

    tasks = [
        asyncio.create_task(paging_worker(session, prepared, cnt, i, expected_sum, concurrency))
        for i in range(concurrency)
    ]

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
