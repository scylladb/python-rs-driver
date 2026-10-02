import pytest
from scylla.session import SessionBuilder
from scylla.statement import PreparedStatement


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepare_statement():
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()
    prepared = await session.prepare("SELECT * FROM system.local")
    print(prepared)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepare_and_execute():
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()
    prepared = await session.prepare("SELECT * FROM system.local")
    assert isinstance(prepared, PreparedStatement)
    result = await session.execute(prepared)
    print(result)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepare_and_str():
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()
    query_str = "SELECT cluster_name FROM system.local"
    prepared = await session.prepare(query_str)
    result_prepared = await session.execute(prepared)
    result_str = await session.execute(query_str)
    assert await result_prepared.all() == await result_str.all()


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_set_and_get_page_size():
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()

    prepared = await session.prepare("SELECT * FROM system.local")

    expected_page_size = 500
    prepared.page_size = expected_page_size

    actual_page_size = prepared.page_size

    assert isinstance(actual_page_size, int)
    assert actual_page_size == expected_page_size
