import uuid

import pytest
import pytest_asyncio
from helpers.ddl import ddl
from helpers.session import connect
from scylla.session import Session


async def set_up() -> Session:
    session = await connect()

    await ddl(
        session,
        """
            CREATE KEYSPACE IF NOT EXISTS testks
            WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};
        """,
    )

    await session.use_keyspace("testks")

    return session


@pytest_asyncio.fixture(scope="module")
async def session():
    session = await set_up()
    yield session
    await ddl(session, "DROP KEYSPACE testks")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_check_schema_agreement_returns_schema_version_or_none(session: Session):
    schema_version = await session.check_schema_agreement()

    assert schema_version is None or isinstance(schema_version, uuid.UUID)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_await_schema_agreement_returns_schema_version(session: Session):
    schema_version = await session.await_schema_agreement()

    assert isinstance(schema_version, uuid.UUID)
    assert schema_version
