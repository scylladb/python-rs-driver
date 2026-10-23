import asyncio
import sys

from common import (
    SIMPLE_SELECT_QUERY,
)
from python_rs_helpers import connect
from scylla.session import Session
from scylla.statement import PreparedStatement


async def test(session: Session, prepared: PreparedStatement, cnt: int, num_of_rows: int):
    for _ in range(cnt):
        count = 0
        result = await session.execute(prepared)

        while True:
            for row in result.iter_current_page():
                count += row["val"]

            next_result = await result.fetch_next_page()
            if next_result is None:
                break

            result = next_result

        assert count == num_of_rows * 100


async def main():
    num_of_rows = int(sys.argv[1])
    cnt = int(sys.argv[2])
    session = await connect()
    prepared = await session.prepare(SIMPLE_SELECT_QUERY)
    prepared = prepared.with_page_size(1)
    await test(session, prepared, cnt, num_of_rows)


if __name__ == "__main__":
    asyncio.run(main())
