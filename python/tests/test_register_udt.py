from collections.abc import AsyncGenerator, Awaitable, Callable
from dataclasses import dataclass
from typing import Any, NamedTuple

import pytest
import pytest_asyncio
from helpers.ddl import ddl
from helpers.session import session_builder
from scylla.adapters import Adapters
from scylla.errors import PyConversionFailedError, SerializationError
from scylla.session import Session

KEYSPACE = "register_udt_ks"


@dataclass(frozen=True)
class Address:
    street: str
    zip: int


class Labeled(Address):
    pass


class Point(NamedTuple):
    x: int
    y: int


class Incomplete:
    def __init__(self, street: str):
        self.street = street


class AcceptsAnything:
    def __init__(self, **fields: Any):
        self.fields = fields


async def connect_with(adapters: Adapters) -> Session:
    session = await session_builder().adapters(adapters).connect()
    await session.use_keyspace(KEYSPACE)
    return session


def make_adapters() -> Adapters:
    adapters = Adapters()
    adapters.register_udt(KEYSPACE, "address", Address)
    adapters.register_udt(KEYSPACE, "point", Point)
    return adapters


@pytest_asyncio.fixture(scope="module")
async def session() -> AsyncGenerator[Session, None]:
    setup = await session_builder().connect()
    await ddl(
        setup,
        f"""
            CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
            WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}};
        """,
    )
    await ddl(setup, f"CREATE TYPE IF NOT EXISTS {KEYSPACE}.address (street text, zip int)")
    await ddl(setup, f"CREATE TYPE IF NOT EXISTS {KEYSPACE}.point (x int, y int)")
    await ddl(setup, f"CREATE TYPE IF NOT EXISTS {KEYSPACE}.route (start frozen<point>, stop frozen<point>)")
    await ddl(setup, f"CREATE TYPE IF NOT EXISTS {KEYSPACE}.plain (a int)")
    session = await connect_with(make_adapters())
    yield session
    await ddl(setup, f"DROP KEYSPACE {KEYSPACE}")


TableFactory = Callable[[str, str], Awaitable[str]]


@pytest_asyncio.fixture
async def table_factory(session: Session) -> AsyncGenerator[TableFactory, None]:
    created_tables: list[str] = []

    async def create_table(schema: str, name: str) -> str:
        await ddl(session, f"CREATE TABLE IF NOT EXISTS {name} ({schema});")
        created_tables.append(name)
        return name

    yield create_table
    for table in created_tables:
        await ddl(session, f"DROP TABLE IF EXISTS {table};")


async def read_back(session: Session, table: str, column: str) -> Any:
    result = await session.execute(f"SELECT {column} FROM {table} WHERE id = 1")
    row = await result.first_row()
    assert row is not None
    return row[column]


def test_register_udt_rejects_a_value_that_is_not_a_class():
    with pytest.raises(TypeError):
        Adapters().register_udt(KEYSPACE, "address", "not a class")  # pyright: ignore[reportArgumentType]


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_object_of_registered_class_round_trips(session: Session, table_factory: TableFactory):
    table = await table_factory("id int PRIMARY KEY, home frozen<address>", "round_trip")

    await session.execute(f"INSERT INTO {table} (id, home) VALUES (?, ?)", (1, Address("Main St", 12345)))

    assert await read_back(session, table, "home") == Address("Main St", 12345)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_dict_is_still_written(session: Session, table_factory: TableFactory):
    table = await table_factory("id int PRIMARY KEY, home frozen<address>", "dict_write")

    await session.execute(f"INSERT INTO {table} (id, home) VALUES (?, ?)", (1, {"street": "Main St", "zip": 1}))

    assert await read_back(session, table, "home") == Address("Main St", 1)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_object_of_a_subclass_is_written(session: Session, table_factory: TableFactory):
    table = await table_factory("id int PRIMARY KEY, home frozen<address>", "subclass_write")

    await session.execute(f"INSERT INTO {table} (id, home) VALUES (?, ?)", (1, Labeled("Main St", 1)))

    assert await read_back(session, table, "home") == Address("Main St", 1)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_missing_attribute_is_an_error(session: Session, table_factory: TableFactory):
    adapters = Adapters()
    adapters.register_udt(KEYSPACE, "address", Incomplete)
    incomplete_session = await connect_with(adapters)
    table = await table_factory("id int PRIMARY KEY, home frozen<address>", "missing_attribute")

    with pytest.raises(SerializationError):
        await incomplete_session.execute(f"INSERT INTO {table} (id, home) VALUES (?, ?)", (1, Incomplete("Main St")))


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_object_of_another_class_is_rejected(session: Session, table_factory: TableFactory):
    table = await table_factory("id int PRIMARY KEY, home frozen<address>", "other_class")

    with pytest.raises(SerializationError):
        await session.execute(f"INSERT INTO {table} (id, home) VALUES (?, ?)", (1, Point(1, 2)))


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_nested_values_are_created_in_one_pass(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, points list<frozen<point>>, homes set<frozen<address>>, "
        "by_name map<text, frozen<point>>, pair tuple<frozen<point>, int>, route frozen<route>",
        "nested",
    )

    await session.execute(
        f"INSERT INTO {table} (id, points, homes, by_name, pair, route) VALUES (?, ?, ?, ?, ?, ?)",
        (
            1,
            [Point(1, 2), Point(3, 4)],
            {Address("Main St", 1)},
            {"a": Point(5, 6)},
            (Point(7, 8), 9),
            {"start": Point(0, 0), "stop": Point(1, 1)},
        ),
    )

    assert await read_back(session, table, "points") == [Point(1, 2), Point(3, 4)]
    assert await read_back(session, table, "homes") == {Address("Main St", 1)}
    assert await read_back(session, table, "by_name") == {"a": Point(5, 6)}
    assert await read_back(session, table, "pair") == (Point(7, 8), 9)
    # A UDT with no class stays a dict, and its registered fields still become objects.
    assert await read_back(session, table, "route") == {"start": Point(0, 0), "stop": Point(1, 1)}


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_unregistered_udt_is_read_as_a_dict(session: Session, table_factory: TableFactory):
    table = await table_factory("id int PRIMARY KEY, value frozen<plain>", "unregistered")

    await session.execute(f"INSERT INTO {table} (id, value) VALUES (?, ?)", (1, {"a": 1}))

    assert await read_back(session, table, "value") == {"a": 1}


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_null_udt_is_read_as_none(session: Session, table_factory: TableFactory):
    table = await table_factory("id int PRIMARY KEY, home frozen<address>", "null_udt")

    await session.execute(f"INSERT INTO {table} (id, home) VALUES (?, ?)", (1, None))

    assert await read_back(session, table, "home") is None


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_null_field_is_passed_as_none(session: Session, table_factory: TableFactory):
    table = await table_factory("id int PRIMARY KEY, home frozen<address>", "null_field")

    await session.execute(f"INSERT INTO {table} (id, home) VALUES (?, ?)", (1, Address("Main St", None)))  # pyright: ignore[reportArgumentType]

    assert await read_back(session, table, "home") == Address("Main St", None)  # pyright: ignore[reportArgumentType]


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_constructor_that_rejects_a_field_is_reported(table_factory: TableFactory):
    adapters = Adapters()
    adapters.register_udt(KEYSPACE, "address", Incomplete)
    incomplete_session = await connect_with(adapters)
    table = await table_factory("id int PRIMARY KEY, home frozen<address>", "rejected_field")
    await incomplete_session.execute(f"INSERT INTO {table} (id, home) VALUES (?, ?)", (1, {"street": "a", "zip": 1}))

    with pytest.raises(PyConversionFailedError):
        await read_back(incomplete_session, table, "home")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_constructor_with_kwargs_accepts_every_field(table_factory: TableFactory):
    adapters = Adapters()
    adapters.register_udt(KEYSPACE, "address", AcceptsAnything)
    kwargs_session = await connect_with(adapters)
    table = await table_factory("id int PRIMARY KEY, home frozen<address>", "kwargs_constructor")
    await kwargs_session.execute(f"INSERT INTO {table} (id, home) VALUES (?, ?)", (1, {"street": "a", "zip": 1}))

    home = await read_back(kwargs_session, table, "home")

    assert home.fields == {"street": "a", "zip": 1}


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_changes_after_connect_do_not_reach_the_session(table_factory: TableFactory):
    adapters = Adapters()
    frozen_session = await connect_with(adapters)
    adapters.register_udt(KEYSPACE, "address", Address)
    table = await table_factory("id int PRIMARY KEY, home frozen<address>", "frozen_registry")

    with pytest.raises(SerializationError):
        await frozen_session.execute(f"INSERT INTO {table} (id, home) VALUES (?, ?)", (1, Address("Main St", 1)))
