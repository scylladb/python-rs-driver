import ipaddress
import uuid
from collections.abc import AsyncGenerator, Awaitable, Callable
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal

import pytest
import pytest_asyncio
from dateutil.relativedelta import relativedelta
from helpers.ddl import ddl
from helpers.session import connect

# SerializationError is never raised directly, but it shapes the error message.
# We import ExecuteError which is raised for serialization issues during query execution.
from scylla.errors import ExecuteError
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


@dataclass
class Address:
    """Example UDT class for address"""

    street: str
    city: str
    zip_code: int

    def to_dict(self):
        return {"street": self.street, "city": self.city, "zip_code": self.zip_code}


@dataclass
class Person:
    """Example UDT class for person"""

    name: str
    age: int
    address: Address

    def to_dict(self):
        return {
            "name": self.name,
            "age": self.age,
            "address": self.address.to_dict() if self.address else None,
        }


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_basic_serialization(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, name text, score double",
        "text_table",
    )
    test_values = [1, "Test Name", 95.5]

    await session.execute(f"INSERT INTO {table} (id, name, score) VALUES (?, ?, ?)", test_values)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_ascii_serialization(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, col ascii",
        "ascii_table",
    )

    val = "HELLO"

    await session.execute(f"INSERT INTO {table} (id, col) VALUES (?, ?)", (1, val))
    await session.execute(f"SELECT * from {table}")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_boolean_serialization(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, col boolean",
        "boolean_table",
    )

    val = True

    await session.execute(f"INSERT INTO {table} (id, col) VALUES (?, ?)", (1, val))
    await session.execute(f"SELECT * from {table}")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_blob_serialization(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, col blob",
        "blob_table",
    )

    val = b"hello world"

    await session.execute(f"INSERT INTO {table} (id, col) VALUES (?, ?)", (1, val))
    await session.execute(f"SELECT * from {table}")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_date_serialization(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, col date",
        "date_table",
    )

    val = date(2004, 6, 16)

    await session.execute(f"INSERT INTO {table} (id, col) VALUES (?, ?)", (1, val))
    await session.execute(f"SELECT * from {table}")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_decimal_serialization(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, col decimal",
        "decimal_table",
    )

    val = Decimal("12.3E+7")

    await session.execute(f"INSERT INTO {table} (id, col) VALUES (?, ?)", (1, val))
    await session.execute(f"SELECT * from {table}")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_double_serialization(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, col double",
        "double_table",
    )

    val = 1.23456789

    await session.execute(f"INSERT INTO {table} (id, col) VALUES (?, ?)", (1, val))
    await session.execute(f"SELECT * from {table}")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_duration_serialization(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, col duration",
        "duration_table",
    )

    val = relativedelta(months=2, days=5, microseconds=36)

    await session.execute(f"INSERT INTO {table} (id, col) VALUES (?, ?)", (1, val))
    await session.execute(f"SELECT * from {table}")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_float_serialization(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, col float",
        "float_table",
    )

    val = 3.14

    await session.execute(f"INSERT INTO {table} (id, col) VALUES (?, ?)", (1, val))
    await session.execute(f"SELECT * from {table}")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_int_serialization(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, col int",
        "int_table",
    )

    val = 123

    await session.execute(f"INSERT INTO {table} (id, col) VALUES (?, ?)", (1, val))
    await session.execute(f"SELECT * from {table}")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_int_serialization_overflow(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, col int",
        "int_table_overflow",
    )

    val = 9999999999999999999999999

    with pytest.raises(ExecuteError) as exc_info:
        await session.execute(f"INSERT INTO {table} (id, col) VALUES (?, ?)", (1, val))

    assert "value overflow during serialization" in str(exc_info.value).lower()


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_bigint_serialization(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, col bigint",
        "bigint_table",
    )

    val = 9999999999

    await session.execute(f"INSERT INTO {table} (id, col) VALUES (?, ?)", (1, val))
    await session.execute(f"SELECT * from {table}")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_bigint_serialization_overflow(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, col bigint",
        "bigint_table_overflow",
    )

    val = 99999999999999999999999999999999

    with pytest.raises(ExecuteError) as exc_info:
        await session.execute(f"INSERT INTO {table} (id, col) VALUES (?, ?)", (1, val))

    assert "value overflow during serialization" in str(exc_info.value).lower()


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_text_serialization(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, col text",
        "text_table",
    )

    val = "Meow"

    await session.execute(f"INSERT INTO {table} (id, col) VALUES (?, ?)", (1, val))
    await session.execute(f"SELECT * from {table}")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_timestamp_serialization(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, col timestamp",
        "timestamp_table",
    )

    val = datetime(1, 1, 1, 23, 59, 59, 999000, tzinfo=timezone.utc)

    await session.execute(f"INSERT INTO {table} (id, col) VALUES (?, ?)", (1, val))
    await session.execute(f"SELECT * from {table}")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_inet_serialization(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, col inet",
        "inet_table",
    )

    ipv4 = ipaddress.ip_address("127.0.0.1")
    ipv6 = ipaddress.ip_address("::1")

    await session.execute(f"INSERT INTO {table} (id, col) VALUES (?, ?)", (1, ipv4))
    await session.execute(f"INSERT INTO {table} (id, col) VALUES (?, ?)", (1, ipv6))
    await session.execute(f"SELECT * from {table}")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_smallint_serialization(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, col smallint",
        "smallint_table",
    )

    val = 12

    await session.execute(f"INSERT INTO {table} (id, col) VALUES (?, ?)", (1, val))
    await session.execute(f"SELECT * from {table}")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_smallint_serialization_overflow(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, col smallint",
        "smallint_table_overflow",
    )

    val = 999999999999999999999999

    with pytest.raises(ExecuteError) as exc_info:
        await session.execute(f"INSERT INTO {table} (id, col) VALUES (?, ?)", (1, val))

    assert "value overflow during serialization" in str(exc_info.value).lower()


@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.skip(reason="Counter currently does not support NetworkTopologyStrategy; enable when supported")
async def test_counter_serialization(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, col counter",
        "counter_table",
    )

    val1 = 5
    val2 = 3
    id = 1

    await session.execute(f"UPDATE {table} SET col = col + ? WHERE id = ?", (val1, id))
    await session.execute(f"UPDATE {table} SET col = col + ? WHERE id = ?", (val2, id))
    await session.execute(f"SELECT * from {table}")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_tinyint_serialization(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, col tinyint",
        "tinyint_table",
    )

    val = 5

    await session.execute(f"INSERT INTO {table} (id, col) VALUES (?, ?)", (1, val))
    await session.execute(f"SELECT * from {table}")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_tinyint_serialization_overflow(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, col tinyint",
        "tinyint_table_overflow",
    )

    val = 99999999999999999999999

    with pytest.raises(ExecuteError) as exc_info:
        await session.execute(f"INSERT INTO {table} (id, col) VALUES (?, ?)", (1, val))

    assert "value overflow during serialization" in str(exc_info.value).lower()


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_time_serialization(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, col time",
        "time_table",
    )

    val = time(1, 30, 45, 500000)

    await session.execute(f"INSERT INTO {table} (id, col) VALUES (?, ?)", (1, val))
    await session.execute(f"SELECT * from {table}")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_timeuuid_serialization(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, col timeuuid",
        "timeuuid_table",
    )

    val = uuid.uuid1()

    await session.execute(f"INSERT INTO {table} (id, col) VALUES (?, ?)", (1, val))
    await session.execute(f"SELECT * from {table}")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_uuid_serialization(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, col uuid",
        "uuid_table",
    )

    val = uuid.uuid4()

    await session.execute(f"INSERT INTO {table} (id, col) VALUES (?, ?)", (1, val))
    await session.execute(f"SELECT * from {table}")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_varint_serialization(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, col varint",
        "varint_table",
    )

    values = [
        0,
        1,
        -1,
        127,
        128,
        255,
        256,
        2**63 - 1,
        -(2**63),
        2**128,
        -(2**128),
        2**56 + 1,
        -(2**56 + 1),
        123456789012345678901234567890,
        -123456789012345678901234567890,
    ]

    for i, val in enumerate(values):
        await session.execute(f"INSERT INTO {table} (id, col) VALUES (?, ?)", (i, val))

    await session.execute(f"SELECT * from {table}")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_basic_serialization_using_dict(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, name text, score double",
        "row_dict_table",
    )

    row = {"id": 1, "name": "Test Name", "score": 95.5}

    await session.execute(f"INSERT INTO {table} (id, name, score) VALUES (?, ?, ?)", row)
    await session.execute(f"SELECT * from {table}")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_map_serialization(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, attributes map<text, int>",
        "map_table",
    )

    map = {"health": 100, "mana": 50, "stamina": 75}

    await session.execute(f"INSERT INTO {table} (id, attributes) VALUES (?, ?)", {"id": 1, "attributes": map})
    await session.execute(f"SELECT * from {table}")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_map_int_key_serialization(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, attributes map<int, int>",
        "map_table",
    )

    map = {16: 100, 6: 50, 2004: 75}

    await session.execute(f"INSERT INTO {table} (id, attributes) VALUES (?, ?)", {"id": 1, "attributes": map})
    await session.execute(f"SELECT * from {table}")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_set_serialization(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, tags set<text>",
        "set_table",
    )

    set = {"fire", "water", "earth", "air"}

    await session.execute(f"INSERT INTO {table} (id, tags) VALUES (?, ?)", {"id": 1, "tags": set})
    await session.execute(f"SELECT * from {table}")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_set_serialization_rejects_dict(
    session: Session,
    table_factory: TableFactory,
):
    table = await table_factory(
        "id int PRIMARY KEY, tags set<text>",
        "set_table_rejects_dict",
    )

    invalid_values = {"brand": "Ford", "model": "Mustang"}

    with pytest.raises(ExecuteError) as exc_info:
        await session.execute(
            f"INSERT INTO {table} (id, tags) VALUES (?, ?)",
            {"id": 1, "tags": invalid_values},
        )

    assert "type mismatch" in str(exc_info.value).lower()


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_vector_list_serialization(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, embedding vector<int, 4>",
        "vector_list_table",
    )

    vector = [1, 2, 3, 4]

    await session.execute(f"INSERT INTO {table} (id, embedding) VALUES (?, ?)", (1, vector))
    await session.execute(f"SELECT * from {table}")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_vector_serialization_tuple(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, embedding vector<int, 4>",
        "vector_tuple_table",
    )

    vector = (1, 2, 3, 4)

    await session.execute(f"INSERT INTO {table} (id, embedding) VALUES (?, ?)", (1, vector))
    await session.execute(f"SELECT * from {table}")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_list_serialization(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, tags list<text>, scores list<int>",
        "list_table",
    )

    test_cases: list[tuple[int, list[str], list[int]]] = [
        (1, [], []),
        (2, ["tag1"], [100]),
        (3, ["a", "b", "c"], [1, 2, 3, 4, 5]),
        (4, ["test", "with", "spaces"], [0, -1, 999]),
    ]

    for values in test_cases:
        await session.execute(f"INSERT INTO {table} (id, tags, scores) VALUES (?, ?, ?)", values)

    await session.execute(f"SELECT * from {table}")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_list_serialization_rejects_tuple(
    session: Session,
    table_factory: TableFactory,
):
    table = await table_factory(
        "id int PRIMARY KEY, tags list<text>, scores list<int>",
        "list_table",
    )

    invalid_values = (
        1,
        ("a", "b", "c"),
        (1, 2, 3),
    )

    with pytest.raises(ExecuteError) as exc_info:
        await session.execute(
            f"INSERT INTO {table} (id, tags, scores) VALUES (?, ?, ?)",
            invalid_values,
        )

    assert "type mismatch" in str(exc_info.value).lower()


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_tuple_serialization(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, tags frozen<tuple<text, text, text>>, scores frozen<tuple<int, int, int>>",
        "tuple_table",
    )

    test_cases = [
        (1, ("", "", ""), (0, 0, 0)),
        (2, ("tag1", "", ""), (100, 0, 0)),
        (3, ("a", "b", "c"), (1, 2, 3)),
        (4, ("test", "with", "spaces"), (0, -1, 999)),
    ]

    for values in test_cases:
        await session.execute(f"INSERT INTO {table} (id, tags, scores) VALUES (?, ?, ?)", values)

    await session.execute(f"SELECT * from {table}")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_tuple_serialization_rejects_list(
    session: Session,
    table_factory: TableFactory,
):
    table = await table_factory(
        "id int PRIMARY KEY, tags frozen<tuple<text, text, text>>, scores frozen<tuple<int, int, int>>",
        "tuple_table_fail",
    )

    invalid_values = (
        1,
        ["a", "b", "c"],
        [1, 2, 3],
    )

    with pytest.raises(ExecuteError) as exc_info:
        await session.execute(
            f"INSERT INTO {table} (id, tags, scores) VALUES (?, ?, ?)",
            invalid_values,
        )

    assert "type mismatch" in str(exc_info.value).lower()


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_null_values(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, name text, score double, tags list<text>",
        "null_table",
    )

    test_cases: list[tuple[int, str | None, float | None, list[str] | None]] = [
        (1, "Not Null", 1.0, ["tag"]),
        (2, None, None, None),
        (3, "Mixed", None, []),
        (4, "Null in list", 2.5, None),
    ]

    for values in test_cases:
        await session.execute(f"INSERT INTO {table} (id, name, score, tags) VALUES (?, ?, ?, ?)", values)

    await session.execute(f"SELECT * from {table}")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_nested_lists(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, numbers list<frozen<list<int>>>",
        "nested_lists_table",
    )

    nested = [[1, 2, 3], [4, 5], [], [6]]

    await session.execute(f"INSERT INTO {table} (id, numbers) VALUES (?, ?)", (1, nested))
    await session.execute(f"SELECT * from {table}")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_udt_simple(session: Session, table_factory: TableFactory):
    await ddl(session, "CREATE TYPE IF NOT EXISTS cat (name text)")

    table = await table_factory(
        "id int PRIMARY KEY, info cat",
        "simple_udt_table",
    )

    cat = {"name": "Whiskers"}

    await session.execute(f"INSERT INTO {table} (id, info) VALUES (?, ?)", (1, cat))

    @dataclass
    class Cat:
        name: str

    cat = Cat("Mittens")

    await session.execute(f"INSERT INTO {table} (id, info) VALUES (?, ?)", (2, asdict(cat)))

    await session.execute(f"SELECT * from {table}")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_udt(session: Session, table_factory: TableFactory):
    await ddl(
        session,
        """
        CREATE TYPE IF NOT EXISTS address (
            street text,
            city text,
            zip_code int
        )
    """,
    )

    table = await table_factory(
        "id int PRIMARY KEY, addr address",
        "udt_table",
    )

    address = {"street": "123 Main St", "city": "Anytown", "zip_code": 12345}

    await session.execute(f"INSERT INTO {table} (id, addr) VALUES (?, ?)", (1, address))

    @dataclass
    class Address:
        street: str
        city: str
        zip_code: int

    address = Address("456 Oak Ave", "Springfield", 67890)
    await session.execute(f"INSERT INTO {table} (id, addr) VALUES (?, ?)", (2, asdict(address)))

    await session.execute(f"SELECT * from {table}")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_udt_with_lists(session: Session, table_factory: TableFactory):
    await ddl(
        session,
        """
        CREATE TYPE IF NOT EXISTS address_test (
            street text,
            city text,
            zip_codes list<int>,
            previous_streets list<text>
        )
    """,
    )
    await ddl(
        session,
        """
        CREATE TABLE IF NOT EXISTS users_test (
            id int PRIMARY KEY,
            addr frozen<address_test>
        )
    """,
    )

    table = await table_factory(
        "id int PRIMARY KEY, addr frozen<address_test>",
        "udt_with_list_table",
    )

    @dataclass
    class Address:
        street: str
        city: str
        zip_codes: list[int]
        previous_streets: list[str]

    addr = Address("456 Oak Ave", "Springfield", [11111, 22222], ["Elm St"])
    await session.execute(f"INSERT INTO {table} (id, addr) VALUES (?, ?)", (1, asdict(addr)))
    await session.execute(f"SELECT * from {table}")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_nested_udts(session: Session, table_factory: TableFactory):
    inner_udt = "address_inner"
    outer_udt = "person_outer"

    await ddl(
        session,
        f"""
        CREATE TYPE IF NOT EXISTS {inner_udt} (
            street text,
            city text,
            zip_code int
        )
    """,
    )

    await ddl(
        session,
        f"""
        CREATE TYPE IF NOT EXISTS {outer_udt} (
            name text,
            age int,
            address frozen<{inner_udt}>
        )
    """,
    )

    table = await table_factory(
        f"id int PRIMARY KEY, person frozen<{outer_udt}>",
        "nested_udts_table",
    )

    @dataclass
    class Address:
        street: str
        city: str
        zip_code: int

    @dataclass
    class Person:
        name: str
        age: int
        address: Address

    person = Person("Bob", 40, Address("456 Oak Ave", "Springfield", 67890))

    await session.execute(f"INSERT INTO {table} (id, person) VALUES (?, ?)", (2, asdict(person)))
    await session.execute(f"SELECT * from {table}")
