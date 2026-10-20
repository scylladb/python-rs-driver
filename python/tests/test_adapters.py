from collections.abc import AsyncGenerator, Awaitable, Callable
from dataclasses import asdict, dataclass
from decimal import Decimal
from enum import Enum
from typing import Any

import pytest
import pytest_asyncio
from helpers.ddl import ddl
from helpers.session import session_builder
from scylla.cql_types import CqlColumnType, CqlTextType
from scylla.errors import ExecuteError
from scylla.serialization import Adapters
from scylla.session import Session
from scylla.statement import Batch

KEYSPACE = "adapters_ks"


@dataclass
class Money:
    amount: Decimal
    currency: str


@dataclass
class Point:
    x: int
    y: int


class Color(Enum):
    RED = "red"
    GREEN = "green"


class Celsius:
    def __init__(self, degrees: float):
        self.degrees = degrees

    def __cql_value__(self, cql_type: CqlColumnType) -> float:
        return self.degrees


class Kelvin(Celsius):
    """Has both an adapter and `__cql_value__`; the adapter must win."""


class Broken:
    pass


class Loop:
    pass


class Missing:
    """Adapted to None."""


class InstanceOnly:
    """Has `__cql_value__` only on the instance, which the driver must not call."""

    def __init__(self):
        self.__cql_value__: Callable[[CqlColumnType], float] = lambda cql_type: 1.0


def money(value: Money, cql_type: CqlColumnType) -> Any:
    if isinstance(cql_type, CqlTextType):
        return f"{value.amount} {value.currency}"
    return value.amount


def broken(value: Broken, cql_type: CqlColumnType) -> Any:
    raise ValueError("broken adapter")


def make_adapters() -> Adapters:
    adapters = Adapters()
    adapters.register_writer(Money, money)
    adapters.register_writer(Point, lambda point, cql_type: asdict(point))
    adapters.register_writer(Enum, lambda member, cql_type: member.value)
    adapters.register_writer(Kelvin, lambda kelvin, cql_type: kelvin.degrees + 273.15)
    adapters.register_writer(Broken, broken)
    adapters.register_writer(Loop, lambda loop, cql_type: Loop())
    adapters.register_writer(Missing, lambda missing, cql_type: None)
    # Never called: the driver writes an int with its built-in path.
    adapters.register_writer(int, lambda number, cql_type: number + 1)
    return adapters


async def connect_with(adapters: Adapters) -> Session:
    session = await session_builder().adapters(adapters).connect()
    await session.use_keyspace(KEYSPACE)
    return session


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
    await ddl(setup, f"CREATE TYPE IF NOT EXISTS {KEYSPACE}.point (x int, y int)")
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


def test_register_writer_rejects_a_value_that_is_not_callable():
    with pytest.raises(TypeError):
        Adapters().register_writer(Money, "not callable")  # pyright: ignore[reportArgumentType]


def test_register_writer_accepts_keyword_arguments():
    Adapters().register_writer(cls=Money, adapter=money)


@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize(
    "column_type,expected",
    [
        ("decimal", Decimal("9.99")),
        ("text", "9.99 EUR"),
    ],
)
async def test_adapter_gets_the_cql_type_of_the_column(
    session: Session, table_factory: TableFactory, column_type: str, expected: Any
):
    table = await table_factory(f"id int PRIMARY KEY, price {column_type}", f"money_{column_type}")

    await session.execute(f"INSERT INTO {table} (id, price) VALUES (?, ?)", (1, Money(Decimal("9.99"), "EUR")))

    assert await read_back(session, table, "price") == expected


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_adapter_applies_to_prepared_statements_and_batches(session: Session, table_factory: TableFactory):
    table = await table_factory("id int PRIMARY KEY, price decimal", "money_paths")
    prepared = await session.prepare(f"INSERT INTO {table} (id, price) VALUES (?, ?)")

    await session.execute(prepared, (1, Money(Decimal("1.50"), "EUR")))
    assert await read_back(session, table, "price") == Decimal("1.50")

    batch = Batch()
    batch.add(prepared, (1, Money(Decimal("2.50"), "EUR")))
    await session.batch(batch)
    assert await read_back(session, table, "price") == Decimal("2.50")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_adapter_of_a_base_class_applies_to_subclasses(session: Session, table_factory: TableFactory):
    table = await table_factory("id int PRIMARY KEY, color text", "enum_table")

    await session.execute(f"INSERT INTO {table} (id, color) VALUES (?, ?)", (1, Color.GREEN))

    assert await read_back(session, table, "color") == "green"


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_adapter_applies_to_nested_values(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, prices list<decimal>, points map<text, frozen<point>>",
        "nested_table",
    )

    await session.execute(
        f"INSERT INTO {table} (id, prices, points) VALUES (?, ?, ?)",
        (1, [Money(Decimal(1), "EUR"), Money(Decimal(2), "EUR")], {"a": Point(1, 2)}),
    )

    assert await read_back(session, table, "prices") == [Decimal(1), Decimal(2)]
    assert await read_back(session, table, "points") == {"a": {"x": 1, "y": 2}}


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_elements_of_one_list_get_the_same_cql_type_object(table_factory: TableFactory):
    seen: list[CqlColumnType] = []

    def point(value: Point, cql_type: CqlColumnType) -> dict[str, int]:
        seen.append(cql_type)
        return asdict(value)

    adapters = Adapters()
    adapters.register_writer(Point, point)
    session = await connect_with(adapters)
    table = await table_factory("id int PRIMARY KEY, points list<frozen<point>>", "point_list_table")

    await session.execute(f"INSERT INTO {table} (id, points) VALUES (?, ?)", (1, [Point(1, 2), Point(3, 4)]))

    assert len(seen) == 2
    assert seen[0] is seen[1]


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_cql_value_method_is_called_without_registration(session: Session, table_factory: TableFactory):
    table = await table_factory("id int PRIMARY KEY, temperature double", "celsius_table")

    await session.execute(f"INSERT INTO {table} (id, temperature) VALUES (?, ?)", (1, Celsius(21.5)))

    assert await read_back(session, table, "temperature") == 21.5


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_cql_value_on_the_instance_is_not_called(session: Session, table_factory: TableFactory):
    table = await table_factory("id int PRIMARY KEY, temperature double", "instance_only_table")

    with pytest.raises(ExecuteError):
        await session.execute(f"INSERT INTO {table} (id, temperature) VALUES (?, ?)", (1, InstanceOnly()))


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_none_from_adapter_is_rejected_as_a_vector_element(session: Session, table_factory: TableFactory):
    table = await table_factory("id int PRIMARY KEY, embedding vector<float, 2>", "vector_missing_table")

    with pytest.raises(ExecuteError) as exc_info:
        await session.execute(f"INSERT INTO {table} (id, embedding) VALUES (?, ?)", (1, [1.0, Missing()]))

    assert "vector element cannot be none" in str(exc_info.value).lower()


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_adapter_takes_precedence_over_cql_value_method(session: Session, table_factory: TableFactory):
    table = await table_factory("id int PRIMARY KEY, temperature double", "kelvin_table")

    await session.execute(f"INSERT INTO {table} (id, temperature) VALUES (?, ?)", (1, Kelvin(0.0)))

    assert await read_back(session, table, "temperature") == 273.15


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_adapter_does_not_change_types_the_driver_accepts(session: Session, table_factory: TableFactory):
    table = await table_factory("id int PRIMARY KEY, number int", "int_table")

    await session.execute(f"INSERT INTO {table} (id, number) VALUES (?, ?)", (1, 41))

    assert await read_back(session, table, "number") == 41


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_exception_from_adapter_is_reported(session: Session, table_factory: TableFactory):
    table = await table_factory("id int PRIMARY KEY, value text", "broken_table")

    with pytest.raises(ExecuteError) as exc_info:
        await session.execute(f"INSERT INTO {table} (id, value) VALUES (?, ?)", (1, Broken()))

    assert "broken adapter" in str(exc_info.value)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_result_of_adapter_is_not_adapted_again(session: Session, table_factory: TableFactory):
    table = await table_factory("id int PRIMARY KEY, value text", "loop_table")

    with pytest.raises(ExecuteError):
        await session.execute(f"INSERT INTO {table} (id, value) VALUES (?, ?)", (1, Loop()))


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_value_without_adapter_is_rejected(session: Session, table_factory: TableFactory):
    table = await table_factory("id int PRIMARY KEY, value text", "unknown_table")

    with pytest.raises(ExecuteError):
        await session.execute(f"INSERT INTO {table} (id, value) VALUES (?, ?)", (1, object()))


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_changes_after_connect_do_not_reach_the_session(table_factory: TableFactory):
    adapters = Adapters()
    session = await connect_with(adapters)
    adapters.register_writer(Money, money)
    table = await table_factory("id int PRIMARY KEY, price decimal", "frozen_table")

    with pytest.raises(ExecuteError):
        await session.execute(f"INSERT INTO {table} (id, price) VALUES (?, ?)", (1, Money(Decimal(1), "EUR")))


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_token_of_an_adapted_partition_key(session: Session, table_factory: TableFactory):
    table = await table_factory("price decimal PRIMARY KEY", "token_table")
    await session.refresh_metadata()
    cluster_state = session.cluster_state

    adapted = cluster_state.compute_token(KEYSPACE, table, (Money(Decimal("3.25"), "EUR"),))
    plain = cluster_state.compute_token(KEYSPACE, table, (Decimal("3.25"),))

    assert adapted == plain
