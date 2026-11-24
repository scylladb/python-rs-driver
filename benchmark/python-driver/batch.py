import asyncio
import sys
import uuid

from cassandra.cluster import Session  # pyright: ignore[reportMissingTypeStubs]
from cassandra.query import BatchStatement, BatchType, PreparedStatement  # pyright: ignore[reportMissingTypeStubs]
from common import (
    SIMPLE_INSERT_QUERY,
)
from python_helpers import connect, to_asyncio

STEP = 3971


async def test(
    session: Session,
    prepared: PreparedStatement,
    cnt: int,
) -> None:
    for start in range(0, cnt, STEP):
        batch_len = min(cnt - start, STEP)

        batch = BatchStatement(batch_type=BatchType.LOGGED)  # pyright: ignore[reportUnknownMemberType]
        for _ in range(batch_len):
            batch.add(prepared, (uuid.uuid4(), 1))  # pyright: ignore[reportUnknownMemberType]

        cass_fut = session.execute_async(batch)  # pyright: ignore[reportUnknownMemberType]
        await to_asyncio(cass_fut)


async def main() -> None:
    cnt = int(sys.argv[1])
    session = await connect()
    prepared = session.prepare(SIMPLE_INSERT_QUERY)  # pyright: ignore[reportUnknownMemberType]
    await test(session, prepared, cnt)


if __name__ == "__main__":
    asyncio.run(main())
