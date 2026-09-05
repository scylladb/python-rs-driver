import asyncio
import sys

from common import (
    COMPLEX_INSERT_QUERY,
    get_complex_data,
)
from python_rs_helpers import connect
from scylla.session import Session
from scylla.statement import PreparedStatement, Statement


async def test(session: Session, prepared: PreparedStatement, cnt: int):
    for _ in range(cnt):
        await session.execute(prepared, get_complex_data(), paged=False)


async def main():
    cnt = int(sys.argv[1])
    session = await connect()
    statement = Statement(COMPLEX_INSERT_QUERY)
    prepared = await session.prepare(statement)

    await test(session, prepared, cnt)


if __name__ == "__main__":
    asyncio.run(main())
