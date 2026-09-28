import time

import pytest
from helpers.ddl import ddl
from scylla.errors import BadKeyspaceName, InvalidRequest
from scylla.session import SessionBuilder


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_use_keyspace():
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()
    ks = f"test_keyspace_{int(time.time() * 1000)}"
    try:
        await ddl(
            session,
            f"CREATE KEYSPACE IF NOT EXISTS {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}",
        )
        await ddl(session, f"CREATE TABLE IF NOT EXISTS {ks}.test_table (id int PRIMARY KEY)")

        await session.use_keyspace(ks)
        await session.execute("INSERT INTO test_table (id) VALUES (1)")
    finally:
        await ddl(session, f"DROP KEYSPACE IF EXISTS {ks}")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_use_non_existing_keyspace():
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()
    with pytest.raises(InvalidRequest):
        await session.use_keyspace("non_existing_keyspace")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_use_keyspace_bad_name():
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()
    with pytest.raises(BadKeyspaceName):
        await session.use_keyspace("")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_use_keyspace_case_sensitive():
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()
    ks = f"CaseSensitiveKeyspace_{int(time.time() * 1000)}"
    try:
        await ddl(
            session,
            f"CREATE KEYSPACE IF NOT EXISTS \"{ks}\" WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}",
        )
        await ddl(session, f'CREATE TABLE IF NOT EXISTS "{ks}".test_table (id int PRIMARY KEY)')

        with pytest.raises(InvalidRequest):
            await session.use_keyspace(ks)

        await session.use_keyspace(ks, case_sensitive=True)
        await session.execute("INSERT INTO test_table (id) VALUES (1)")
    finally:
        await ddl(session, f'DROP KEYSPACE IF EXISTS "{ks}"')


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_change_use_keyspace():
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()
    ks1 = f"test_keyspace_{int(time.time() * 1000)}"
    ks2 = f"other_keyspace_{int(time.time() * 1000) + 1}"
    try:
        await ddl(
            session,
            f"CREATE KEYSPACE IF NOT EXISTS {ks1} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}",
        )
        await ddl(
            session,
            f"CREATE KEYSPACE IF NOT EXISTS {ks2} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}",
        )
        await ddl(session, f"CREATE TABLE IF NOT EXISTS {ks1}.test_table (id int PRIMARY KEY)")

        await session.use_keyspace(ks1)
        await session.execute("INSERT INTO test_table (id) VALUES (1)")

        await session.use_keyspace(ks2)

        with pytest.raises(InvalidRequest):
            await session.execute("INSERT INTO test_table (id) VALUES (1)")
    finally:
        await ddl(session, f"DROP KEYSPACE IF EXISTS {ks1}")
        await ddl(session, f"DROP KEYSPACE IF EXISTS {ks2}")
