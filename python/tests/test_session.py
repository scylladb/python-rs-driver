import uuid

import pytest
import pytest_asyncio
from helpers.ddl import ddl
from helpers.session import connect
from helpers.system_clients import own_client_options
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


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_refresh_metadata_replaces_cluster_state(session: Session):
    before = session.cluster_state

    await session.refresh_metadata()

    after = session.cluster_state
    assert after is not before
    assert after.nodes_info.keys() == before.nodes_info.keys()


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_session_id_identifies_the_connections_of_the_session(session: Session):
    session_id = session.session_id

    assert isinstance(session_id, uuid.UUID)
    # The session makes the object once. Each read returns the same object.
    assert session.session_id is session_id

    other_session = await connect()
    assert other_session.session_id != session_id

    # The server shows the session id as SESSION_ID in system.clients.
    assert await own_client_options(session), "system.clients has no row with the session id"


# Scylla warns about a replication factor below
# `minimum_replication_factor_warn_threshold`, which is 3 by default.
RF_WARNING = "minimum_replication_factor_warn_threshold"


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_warnings_hold_server_warnings(session: Session):
    result = await ddl(
        session,
        """
            CREATE KEYSPACE testks_warnings
            WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};
        """,
    )
    try:
        assert isinstance(result.first_page_warnings, tuple)
        assert any(RF_WARNING in warning for warning in result.first_page_warnings)
        assert result.first_page.warnings == result.first_page_warnings
    finally:
        await ddl(session, "DROP KEYSPACE testks_warnings")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_warnings_are_empty_without_server_warnings(session: Session):
    result = await session.execute("SELECT key FROM system.local WHERE key = 'local'")

    assert result.first_page_warnings == ()
    assert result.first_page.warnings == ()
