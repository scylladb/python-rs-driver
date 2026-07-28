import asyncio
import sys
import uuid

from common import (
    SIMPLE_INSERT_QUERY,
)
from python_rs_helpers import connect
from scylla.batch import Batch, BatchType
from scylla.session import Session
from scylla.statement import PreparedStatement, Statement

STEP = 3971


async def test(
    session: Session,
    prepared: PreparedStatement,
    cnt: int,
) -> None:
    for start in range(0, cnt, STEP):
        batch_len = min(cnt - start, STEP)

        batch = Batch(BatchType.Logged)
        for _ in range(batch_len):
            batch.add(prepared, (uuid.uuid4(), 1))

        await session.batch(batch)


async def main() -> None:
    cnt = int(sys.argv[1])
    session = await connect()
    prepared = await session.prepare(Statement(SIMPLE_INSERT_QUERY))
    await test(session, prepared, cnt)


if __name__ == "__main__":
    asyncio.run(main())
