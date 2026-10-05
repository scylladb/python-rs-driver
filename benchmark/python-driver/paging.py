import asyncio
import sys

from cassandra.cluster import Session  # pyright: ignore[reportMissingTypeStubs]
from cassandra.query import PreparedStatement  # pyright: ignore[reportMissingTypeStubs]
from common import (
    SIMPLE_SELECT_QUERY,
)
from python_helpers import connect, to_asyncio


async def test(session: Session, prepared: PreparedStatement, cnt: int, num_of_rows: int):
    for _ in range(cnt):
        count = 0
        paging_state = None

        while True:
            cass_fut = session.execute_async(  # pyright: ignore[reportUnknownMemberType]
                query=prepared,
                paging_state=paging_state,
            )
            result = await to_asyncio(cass_fut)

            # Count rows from current page
            for row in result:
                count += row.val

            # Get paging state from cassandra future's result object
            # This should not block as the future has already been awaited
            result_obj = cass_fut.result()  # pyright: ignore[reportUnknownMemberType]
            paging_state = result_obj.paging_state  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            if paging_state is None:
                break

        assert count == num_of_rows * 100


async def main():
    num_of_rows = int(sys.argv[1])
    cnt = int(sys.argv[2])
    session = await connect()
    prepared = session.prepare(SIMPLE_SELECT_QUERY)  # pyright: ignore[reportUnknownMemberType]
    prepared.fetch_size = 1
    await test(session, prepared, cnt, num_of_rows)


if __name__ == "__main__":
    asyncio.run(main())
