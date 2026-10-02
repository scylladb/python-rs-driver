import datetime
import ipaddress
import math
import uuid
from collections.abc import AsyncGenerator, Awaitable, Callable
from datetime import time
from decimal import Decimal
from typing import Any

import pytest
import pytest_asyncio
from dateutil.relativedelta import relativedelta
from helpers.ddl import ddl
from helpers.session import connect
from scylla._rust.cluster.metadata import CqlColumnType, CqlText  # pyright: ignore[reportMissingModuleSource]
from scylla._rust.errors import DeserializationError, RowIterationError  # pyright: ignore[reportMissingModuleSource]
from scylla._rust.session import Session  # pyright: ignore[reportMissingModuleSource]
from scylla._rust.value import CqlEmpty  # pyright: ignore[reportMissingModuleSource]
from scylla.cluster.metadata import ColumnSpec
from scylla.results import RowBuilder, RowFactory


async def set_up() -> Session:
    session = await connect()

    # 2. Create keyspace & table
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


async def insert_and_fetch_single_row(
    session: Session,
    table_factory: TableFactory,
    schema: str,
    table_name: str,
    row_id: int,
    value_sql: str,
) -> dict[str, Any]:
    table = await table_factory(schema, table_name)

    await session.execute(f"INSERT INTO {table} (id, value) VALUES ({row_id}, {value_sql});")

    result = await session.execute(f"SELECT * FROM {table} WHERE id = {row_id}")
    row = await result.first_row()
    assert row is not None, f"Expected to find row with id={row_id}, but query returned no results"
    return row


# Verifies that iter_rows() returns an iterator yielding row dictionaries
@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_rows_result_is_iterator_and_dicts(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, x int",
        "rows_result_basic_table",
    )

    await session.execute(f"INSERT INTO {table} (id, x) VALUES (1, 10);")
    await session.execute(f"INSERT INTO {table} (id, x) VALUES (2, 20);")

    result = await session.execute(f"SELECT * FROM {table}")

    first = await result.first_row()
    assert isinstance(first, dict)

    assert "id" in first
    assert "x" in first


# Verifies correct deserialization of CQL text values, including Unicode
@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize(
    "row_id,value",
    [(1, "hello"), (2, "Zażółć gęślą jaźń 🚀"), (3, "")],
)
async def test_text_deserialization(
    session: Session,
    table_factory: TableFactory,
    row_id: int,
    value: str,
):
    row = await insert_and_fetch_single_row(
        session=session,
        table_factory=table_factory,
        schema="id int PRIMARY KEY, value text",
        table_name="text_table",
        row_id=row_id,
        value_sql=f"'{value}'",
    )

    assert isinstance(row["value"], str)
    assert row["value"] == value


# Verifies correct deserialization of CQL float values
@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize(
    "row_id,value_sql,expected",
    [
        (1, "1.25", 1.25),
        (2, "-42.5", -42.5),
        (3, "3.234", 3.234),
        (4, "Infinity", math.inf),
        (5, "-Infinity", -math.inf),
        (6, "NaN", math.nan),
    ],
)
async def test_float_deserialization(
    session: Session,
    table_factory: TableFactory,
    row_id: int,
    value_sql: str,
    expected: float,
):
    def to_float32(val: float) -> float:
        import struct

        # Pack as 32-bit float (f), then unpack back to Python float
        return struct.unpack("f", struct.pack("f", val))[0]

    row = await insert_and_fetch_single_row(
        session=session,
        table_factory=table_factory,
        schema="id int PRIMARY KEY, value float",
        table_name="float_table",
        row_id=row_id,
        value_sql=value_sql,
    )

    assert isinstance(row["value"], float)
    if math.isnan(expected):
        assert math.isnan(row["value"])
    else:
        assert row["value"] == to_float32(expected)


# Verifies correct deserialization of CQL double values
@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize(
    "row_id,value",
    [
        (1, 3.141592653589793),
        (2, -0.00000012345),
    ],
)
async def test_double_deserialization(
    session: Session,
    table_factory: TableFactory,
    row_id: int,
    value: float,
):
    row = await insert_and_fetch_single_row(
        session=session,
        table_factory=table_factory,
        schema="id int PRIMARY KEY, value double",
        table_name="double_table",
        row_id=row_id,
        value_sql=str(value),
    )

    assert isinstance(row["value"], float)
    assert row["value"] == value


# Verifies correct deserialization of CQL list<int> into Python lists
@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize(
    "row_id,value",
    [(1, [1, 2, 3]), (2, [35, 53, 15, 12]), (3, [])],
)
async def test_list_deserialization(
    session: Session,
    table_factory: TableFactory,
    row_id: int,
    value: list[int],
):
    row = await insert_and_fetch_single_row(
        session=session,
        table_factory=table_factory,
        schema="id int PRIMARY KEY, value list<int>",
        table_name="list_table",
        row_id=row_id,
        value_sql=str(value),
    )

    assert isinstance(row["value"], list)
    assert row["value"] == value


# Verifies correct deserialization of nested CQL list<frozen<list<int>>
@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize(
    "row_id,value",
    [(1, [[1, 2, 3], [4, 5], [3, 2, 1]]), (2, [[10], [20, 30, 40], [3, 2, 1], [3, 2, 1]]), (3, [[]])],  # pyright: ignore
)
async def test_nested_list_deserialization(
    session: Session,
    table_factory: TableFactory,
    row_id: int,
    value: list[list[int]],
):
    row = await insert_and_fetch_single_row(
        session=session,
        table_factory=table_factory,
        schema="id int PRIMARY KEY, value list<frozen<list<int>>>",
        table_name="nested_list_table",
        row_id=row_id,
        value_sql=str(value),
    )

    nested = row["value"]

    assert isinstance(nested, list)
    for inner in nested:  # pyright: ignore[reportUnknownVariableType]
        assert isinstance(inner, list)

    assert nested == value


# Verifies correct deserialization of CQL UDT into Python dictionaries
@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize(
    "row_id,value_sql,expected",
    [
        (
            1,
            "{ street: 'Main St', number: 10 }",
            {"street": "Main St", "number": 10},
        ),
        (
            2,
            "{ street: 'Oak Ave', number: 42 }",
            {"street": "Oak Ave", "number": 42},
        ),
    ],
)
async def test_udt_deserialization(
    session: Session,
    table_factory: TableFactory,
    row_id: int,
    value_sql: str,
    expected: dict[str, int],
):
    # Create UDT (safe to re-run)
    await ddl(
        session,
        """
        CREATE TYPE IF NOT EXISTS testks.address (
            street text,
            number int
        )
        """,
    )

    row = await insert_and_fetch_single_row(
        session=session,
        table_factory=table_factory,
        schema="id int PRIMARY KEY, value address",
        table_name="udt_table",
        row_id=row_id,
        value_sql=value_sql,
    )

    assert isinstance(row["value"], dict)
    assert row["value"] == expected


# Verifies correct deserialization of list<frozen<UDT>> across multiple rows
@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_list_udt_deserialization(session: Session, table_factory: TableFactory):
    await ddl(
        session,
        """
        CREATE TYPE IF NOT EXISTS testks.address (
            street text,
            number int
        );
    """,
    )

    table = await table_factory("id int, name text, addrs list<frozen<address>>, PRIMARY KEY (id, name)", "nested_udt")

    # 3. Insert multiple rows with list<udt>
    rows_to_insert = 9  # 10–15 as requested
    for i in range(rows_to_insert):
        await session.execute(f"""
            INSERT INTO {table} (id, name, addrs)
            VALUES (
                0,
                'User{i}',
                [
                    {{ street: 'A-Street-{i}', number: {i * 10} }},
                    {{ street: 'B-Street-{i}', number: {i * 10 + 1} }}
                ]
            );
        """)

    # 4. Query + deserialize
    result = await session.execute(f"SELECT * FROM {table} WHERE id = 0 ORDER BY name ASC")
    row_list = await result.all()

    # 5. Assertions — verify all rows returned + structure is correct
    assert len(row_list) == rows_to_insert

    for i, row in enumerate(row_list):
        assert isinstance(row["addrs"], list)
        assert row["name"] == f"User{i}"
        assert row["addrs"][0]["street"] == f"A-Street-{i}"
        assert row["addrs"][0]["number"] == i * 10
        assert row["addrs"][1]["street"] == f"B-Street-{i}"
        assert row["addrs"][1]["number"] == i * 10 + 1


# Verifies that a custom RowFactory can transform rows into user-defined Python objects
@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_custom_row_factory_transforms_rows(session: Session, table_factory: TableFactory):
    # Define a simple Python class for output rows
    class UserRow:
        def __init__(self, id: int, name: str, scores: list[int]):
            self.id = id
            self.name = name
            self.scores = scores

    # A valid RowFactory implementation: `prepare` sees the column metadata once
    # and returns the builder that runs for every row.
    class UserFactory(RowFactory):
        cls = UserRow

        def prepare(self, columns: tuple[ColumnSpec, ...]) -> RowBuilder:
            names = [column.name for column in columns]

            def build(values: tuple[Any, ...]) -> UserRow:
                row = dict(zip(names, values))
                return UserRow(
                    int(row["id"]),  # type: ignore[arg-type]
                    str(row["name"]),
                    list(row["scores"]),  # type: ignore[arg-type]
                )

            return build

    # Create table
    table = await table_factory("id int PRIMARY KEY, name text, scores list<int>", "example_table")

    await session.execute(f"INSERT INTO {table} (id, name, scores) VALUES (1, 'Alice', [1, 2, 3]);")
    await session.execute(f"INSERT INTO {table} (id, name, scores) VALUES (2, 'Bob', [5, 10]);")

    # Query
    result = await session.execute(f"SELECT * FROM {table}", factory=UserFactory())

    # Apply the custom row factory

    collected = await result.all()

    assert len(collected) == 2
    assert all(isinstance(r, UserRow) for r in collected)

    # Check actual content transformation
    alice = next(r for r in collected if r.id == 1)
    assert alice.name == "Alice"
    assert alice.scores == [1, 2, 3]

    bob = next(r for r in collected if r.id == 2)
    assert bob.name == "Bob"
    assert bob.scores == [5, 10]


# Verifies correct deserialization of CQL uuid into Python UUID
@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize(
    "row_id,value",
    [
        (1, uuid.uuid4()),
        (2, uuid.uuid4()),
    ],
)
async def test_uuid_deserialization(
    session: Session,
    table_factory: TableFactory,
    row_id: int,
    value: uuid.UUID,
):
    row = await insert_and_fetch_single_row(
        session,
        table_factory,
        schema="id int PRIMARY KEY, value uuid",
        table_name="uuid_table",
        row_id=row_id,
        value_sql=str(value),
    )

    assert isinstance(row["value"], uuid.UUID)
    assert row["value"] == value


# Verifies correct deserialization of CQL inet into IPv4 and IPv6 address objects
@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize(
    "row_id,value,expected_type",
    [
        (1, ipaddress.ip_address("192.168.1.10"), ipaddress.IPv4Address),
        (2, ipaddress.ip_address("2001:db8::1"), ipaddress.IPv6Address),
    ],
)
async def test_inet_deserialization(
    session: Session,
    table_factory: TableFactory,
    row_id: int,
    value: ipaddress.IPv6Address | ipaddress.IPv4Address,
    expected_type: type,
):
    row = await insert_and_fetch_single_row(
        session=session,
        table_factory=table_factory,
        schema="id int PRIMARY KEY, value inet",
        table_name="inet_table",
        row_id=row_id,
        value_sql=f"'{value}'",
    )

    assert isinstance(row["value"], expected_type)
    assert row["value"] == value


# Verifies correct deserialization of CQL timeuuid into version-1 UUID
@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize(
    "row_id,value",
    [
        (1, uuid.uuid1()),
        (2, uuid.uuid1()),
    ],
)
async def test_timeuuid_deserialization(
    session: Session,
    table_factory: TableFactory,
    row_id: int,
    value: uuid.UUID,
):
    row = await insert_and_fetch_single_row(
        session=session,
        table_factory=table_factory,
        schema="id int PRIMARY KEY, value timeuuid",
        table_name="timeuuid_table",
        row_id=row_id,
        value_sql=str(value),
    )

    assert isinstance(row["value"], uuid.UUID)
    assert row["value"].version == 1
    assert row["value"] == value


# Verifies correct deserialization of CQL date into Python date objects
@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize(
    "row_id,value",
    [
        (1, datetime.date(2024, 5, 10)),
        (2, datetime.date(1999, 12, 31)),
    ],
)
async def test_date_deserialization(session: Session, table_factory: TableFactory, row_id: int, value: datetime.date):
    row = await insert_and_fetch_single_row(
        session=session,
        table_factory=table_factory,
        schema="id int PRIMARY KEY, value date",
        table_name="date_table",
        row_id=row_id,
        value_sql=f"'{value.isoformat()}'",
    )

    # Type checks
    assert isinstance(row["value"], datetime.date)

    # Value correctness
    assert row["value"] == value


# Verifies correct deserialization of CQL ascii into Python strings
@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize(
    "row_id,value",
    [
        (1, "HelloASCII"),
        (2, "Test123_ABC"),
    ],
)
async def test_ascii_deserialization(
    session: Session,
    table_factory: TableFactory,
    row_id: int,
    value: str,
):
    row = await insert_and_fetch_single_row(
        session,
        table_factory,
        schema="id int PRIMARY KEY, value ascii",
        table_name="ascii_table",
        row_id=row_id,
        value_sql=f"'{value}'",
    )

    assert isinstance(row["value"], str)
    assert row["value"] == value
    assert row["value"].isascii()


# Verifies exact round-trip deserialization of CQL decimal into Python Decimal
@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize(
    "row_id,value",
    [
        (1, Decimal(0)),
        (2, Decimal("0.0")),
        (3, Decimal("-0")),
        (4, Decimal("123.456")),
        (5, Decimal("-7890.00123")),
        (6, Decimal("123456789012345678901234567890.123456789")),
        (7, Decimal("-999999999999999999999999999999999999.9999")),
        (8, Decimal("0.00000000000000000001")),
        (9, Decimal("42.000000")),
        (10, Decimal(617283694)),
        (11, Decimal("617283694.4324")),
    ],
)
async def test_decimal_deserialization(
    session: Session,
    table_factory: TableFactory,
    row_id: int,
    value: str,
):
    row = await insert_and_fetch_single_row(
        session,
        table_factory,
        schema="id int PRIMARY KEY, value decimal",
        table_name="decimal_table",
        row_id=row_id,
        value_sql=str(value),
    )

    assert isinstance(row["value"], Decimal)
    assert row["value"] == value


# Verifies correct deserialization of CQL varint into Python int without precision loss
@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize(
    "row_id,value",
    [
        (1, 0),
        (2, 1),
        (3, -1),
        (4, 42),
        (5, -42),
        (6, 2**63 - 1),
        (7, -(2**63)),
        (8, 12345678901234567890),
        (9, -12345678901234567890),
        (10, 123456789012345678901234567890),
        (11, -99999999999999999999999999999999999999),
        (12, 10**100),
        (13, -(10**100)),
    ],
)
async def test_varint_deserialization_parametrized(
    session: Session,
    table_factory: TableFactory,
    row_id: int,
    value: str,
):
    row = await insert_and_fetch_single_row(
        session,
        table_factory,
        schema="id int PRIMARY KEY, value varint",
        table_name="varint_table",
        row_id=row_id,
        value_sql=str(value),
    )

    assert isinstance(row["value"], int)
    assert row["value"] == value


# Verifies correct deserialization of CQL timestamp into timezone-aware datetime
@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize(
    "row_id,value",
    [
        (
            1,
            datetime.datetime(2024, 5, 10, 12, 30, 45, 123000, tzinfo=datetime.timezone.utc),
        ),
        (
            2,
            datetime.datetime(1999, 12, 31, 23, 59, 59, 0, tzinfo=datetime.timezone.utc),
        ),
    ],
)
async def test_timestamp_deserialization(
    session: Session,
    table_factory: TableFactory,
    row_id: int,
    value: datetime.datetime,
):
    # timestamp expects epoch milliseconds
    value_ms = int(value.timestamp() * 1000)

    row = await insert_and_fetch_single_row(
        session,
        table_factory,
        schema="id int PRIMARY KEY, value timestamp",
        table_name="timestamp_table",
        row_id=row_id,
        value_sql=str(value_ms),
    )

    assert isinstance(row["value"], datetime.datetime)
    assert row["value"] == value


# Verifies correct deserialization of CQL time into Python time objects
@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize(
    "row_id,value",
    [
        (1, time(1, 30, 45, 500000)),  # 01:30:45.500
        (2, time(10, 15, 5, 123000)),  # 10:15:05.123
    ],
)
async def test_time_deserialization(
    session: Session,
    table_factory: TableFactory,
    row_id: int,
    value: time,
):
    row = await insert_and_fetch_single_row(
        session,
        table_factory,
        schema="id int PRIMARY KEY, value time",
        table_name="time_table",
        row_id=row_id,
        value_sql=f"'{value.isoformat()}'",
    )

    assert isinstance(row["value"], time)
    assert row["value"] == value


# Verifies correct deserialization of CQL duration into relativedelta
@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize(
    "row_id,expected,value_literal",
    [
        (
            1,
            relativedelta(months=2, days=5, microseconds=36),
            "2mo5d36000ns",
        ),
        (
            2,
            relativedelta(months=11, days=0, microseconds=123_456),
            "11mo0d123456000ns",
        ),
    ],
)
async def test_duration_deserialization(
    session: Session,
    table_factory: TableFactory,
    row_id: int,
    expected: relativedelta,
    value_literal: str,
):
    row = await insert_and_fetch_single_row(
        session,
        table_factory,
        schema="id int PRIMARY KEY, value duration",
        table_name="duration_table",
        row_id=row_id,
        value_sql=value_literal,
    )

    assert isinstance(row["value"], relativedelta)
    assert row["value"] == expected


# Verifies correct deserialization of CQL map<text, int> into Python dict
@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize(
    "row_id,literal,expected",
    [
        (1, "{'a':1,'b':2}", {"a": 1, "b": 2}),
        (2, "{'x':10,'y':20,'z':30}", {"x": 10, "y": 20, "z": 30}),
    ],
)
async def test_map_text_int_deserialization(
    session: Session,
    table_factory: TableFactory,
    row_id: int,
    literal: str,
    expected: dict[str, int],
):
    row = await insert_and_fetch_single_row(
        session,
        table_factory,
        schema="id int PRIMARY KEY, value map<text,int>",
        table_name="map_text_int_table",
        row_id=row_id,
        value_sql=literal,
    )

    assert isinstance(row["value"], dict)
    assert row["value"] == expected


# Verifies correct deserialization of CQL map<text, list<int>> into nested Python structures
@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize(
    "row_id,value_literal,expected",
    [
        (
            1,
            "{'a':[1,2,3],'b':[10]}",
            {"a": [1, 2, 3], "b": [10]},
        ),
        (
            2,
            "{'x':[],'y':[5,5,5]}",
            {"x": [], "y": [5, 5, 5]},
        ),
    ],
)
async def test_map_text_list_int_deserialization(
    session: Session,
    table_factory: TableFactory,
    row_id: int,
    value_literal: str,
    expected: dict[str, list[int]],
):
    row = await insert_and_fetch_single_row(
        session,
        table_factory,
        schema="id int PRIMARY KEY, value map<text, frozen<list<int>>>",
        table_name="map_text_list_int_table",
        row_id=row_id,
        value_sql=value_literal,
    )

    assert isinstance(row["value"], dict)
    assert row["value"] == expected


# Verifies correct deserialization of CQL set<int> into Python set
@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize(
    "row_id,value_literal,expected",
    [
        (1, "{1,2,3}", {1, 2, 3}),
        (2, "{10,20}", {10, 20}),
    ],
)
async def test_set_int_deserialization(
    session: Session,
    table_factory: TableFactory,
    row_id: int,
    value_literal: str,
    expected: set[int],
):
    row = await insert_and_fetch_single_row(
        session,
        table_factory,
        schema="id int PRIMARY KEY, value set<int>",
        table_name="set_int_table",
        row_id=row_id,
        value_sql=value_literal,
    )

    assert isinstance(row["value"], set)
    assert row["value"] == expected


# Verifies correct deserialization of 3-element CQL tuple into Python tuple
@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize(
    "row_id,value_literal,expected",
    [
        (
            1,
            "(1,'hello',true)",
            (1, "hello", True),
        ),
        (
            2,
            "(42,'world',false)",
            (42, "world", False),
        ),
    ],
)
async def test_tuple_3_deserialization(
    session: Session,
    table_factory: TableFactory,
    row_id: int,
    value_literal: str,
    expected: tuple[int, str, bool],
):
    row = await insert_and_fetch_single_row(
        session,
        table_factory,
        schema="id int PRIMARY KEY, value tuple<int, text, boolean>",
        table_name="tuple_3_table",
        row_id=row_id,
        value_sql=value_literal,
    )

    assert isinstance(row["value"], tuple)
    assert row["value"] == expected


# Verifies correct deserialization of large (10-element) CQL tuple into Python tuple
@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize(
    "row_id,value_literal,expected",
    [
        (
            1,
            "(1,'x',3.14,true,999,10,'abc',false,-1.25,777)",
            (1, "x", 3.14, True, 999, 10, "abc", False, -1.25, 777),
        ),
        (
            2,
            "(2,'y',-2.5,false,-555,20,'zzz',true,42.42,123456)",
            (2, "y", -2.5, False, -555, 20, "zzz", True, 42.42, 123456),
        ),
    ],
)
async def test_tuple_10_deserialization(
    session: Session,
    table_factory: TableFactory,
    row_id: int,
    value_literal: str,
    expected: tuple[int, str, float, bool, int, int, str, bool, float, int],
):
    row = await insert_and_fetch_single_row(
        session,
        table_factory,
        schema="""
            id int PRIMARY KEY,
            value tuple<int, text, double, boolean, bigint,
                        int, text, boolean, double, int>
        """,
        table_name="tuple_10_table",
        row_id=row_id,
        value_sql=value_literal,
    )

    assert isinstance(row["value"], tuple)
    assert row["value"] == expected


# Verifies correct deserialization of fixed-size CQL vector<float, 4> into Python list
@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize(
    "row_id,value_literal,expected",
    [
        (1, "[1.0,2.5,-3.0,4.25]", [1.0, 2.5, -3.0, 4.25]),
        (2, "[-1.0,0.0,0.5,99.9]", [-1.0, 0.0, 0.5, 99.9]),
    ],
)
async def test_vector_4_deserialization(
    session: Session,
    table_factory: TableFactory,
    row_id: int,
    value_literal: str,
    expected: list[float],
):
    row = await insert_and_fetch_single_row(
        session,
        table_factory,
        schema="id int PRIMARY KEY, value vector<float,4>",
        table_name="vec4_table",
        row_id=row_id,
        value_sql=value_literal,
    )

    assert isinstance(row["value"], list)
    assert row["value"] == pytest.approx(expected)  # pyright: ignore[reportUnknownMemberType]


# Verifies correct handling of NULL values in CQL Collections
@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize(
    "row_id,collection,table_name,expected",
    [
        (1, "set<int>", "null_set_table", set()),  # pyright: ignore[reportUnknownArgumentType]
        (1, "map<int, int>", "null_map_table", {}),  # pyright: ignore[reportUnknownArgumentType]
        (1, "list<int>", "null_list_table", []),  # pyright: ignore[reportUnknownArgumentType]
    ],
)
async def test_null_collections_deserialization(
    session: Session,
    table_factory: TableFactory,
    row_id: int,
    collection: str,
    table_name: str,
    expected: list[int] | set[int] | dict[int, int],
):
    row = await insert_and_fetch_single_row(
        session,
        table_factory,
        schema=f"id int PRIMARY KEY, value {collection}",
        table_name=table_name,
        row_id=row_id,
        value_sql="NULL",
    )

    assert row["value"] == expected


# Verifies correct deserialization of CQL blob into Python bytes, including empty blobs
@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize(
    "row_id,value_literal,expected",
    [
        (1, "0x00010203ff", b"\x00\x01\x02\x03\xff"),
        (2, "0x68656c6c6f20776f726c64", b"hello world"),
        (3, "0x", b""),
    ],
)
async def test_blob_deserialization(
    session: Session,
    table_factory: TableFactory,
    row_id: int,
    value_literal: str,
    expected: bytes,
):
    row = await insert_and_fetch_single_row(
        session,
        table_factory,
        schema="id int PRIMARY KEY, value blob",
        table_name="blob_table",
        row_id=row_id,
        value_sql=value_literal,
    )

    assert isinstance(row["value"], bytes)
    assert bytes(row["value"]) == expected


# Verifies correct handling of NULL tuples and NULL elements inside CQL tuples
@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize(
    "row_id,value_sql,expected",
    [
        (1, "(1, NULL, true, NULL)", (1, None, True, None)),
        (2, "(NULL, NULL, NULL, NULL)", (None, None, None, None)),
    ],
)
async def test_tuple_with_null_elements_deserialization(
    session: Session,
    table_factory: TableFactory,
    row_id: int,
    value_sql: str,
    expected: tuple[int, str, bool, float],
):
    row = await insert_and_fetch_single_row(
        session=session,
        table_factory=table_factory,
        schema="id int PRIMARY KEY, value tuple<int, text, boolean, double>",
        table_name="tuple_null_elem_table",
        row_id=row_id,
        value_sql=value_sql,
    )

    assert isinstance(row["value"], tuple)
    assert row["value"] == expected


# Verifies correct handling of NULL UDT and NULL elements inside CQL UDT
@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize(
    "row_id,value_sql,expected",
    [
        (
            1,
            "{a: 1, b: NULL, c: true, d: NULL}",
            {"a": 1, "b": None, "c": True, "d": None},
        ),
        (
            2,
            "{a: NULL, b: NULL, c: false, d: NULL}",
            {"a": None, "b": None, "c": False, "d": None},
        ),
    ],
)
async def test_udt_with_null_fields_deserialization(
    session: Session,
    table_factory: TableFactory,
    row_id: int,
    value_sql: str,
    expected: dict[str, Any],
):
    # Create UDT
    await ddl(
        session,
        """
        CREATE TYPE IF NOT EXISTS udt_null_test (
            a int,
            b text,
            c boolean,
            d double
        )
        """,
    )

    row = await insert_and_fetch_single_row(
        session=session,
        table_factory=table_factory,
        schema="id int PRIMARY KEY, value frozen<udt_null_test>",
        table_name="udt_null_elem_table",
        row_id=row_id,
        value_sql=value_sql,
    )

    assert isinstance(row["value"], dict)
    assert row["value"] == expected


@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize(
    "row_id,cql_type,table_name",
    [
        (1, "inet", "empty_inet_table"),
        (2, "date", "empty_date_table"),
    ],
)
async def test_empty_values_deserialization(
    session: Session,
    table_factory: TableFactory,
    row_id: int,
    cql_type: str,
    table_name: str,
):
    row = await insert_and_fetch_single_row(
        session,
        table_factory,
        schema=f"id int PRIMARY KEY, value {cql_type}",
        table_name=table_name,
        row_id=row_id,
        value_sql="''",
    )

    empty_value = row["value"]
    assert isinstance(empty_value, CqlEmpty)


@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.xfail(
    reason=(
        "Conversion from CqlDate to Python date is currently fallible. "
        "CQL date supports a wider range than Python datetime.date. "
        "This test documents the failure for extreme values and serves "
        "as a regression test once conversion logic is fixed."
    ),
)
@pytest.mark.parametrize(
    "row_id,value_sql",
    [
        # CQL date is stored as unsigned days since epoch (1970-01-01).
        # These values correspond to u32::MIN and u32::MAX on the Rust side.
        (1, "0"),  # CqlDate(u32::MIN)
        (2, "4294967295"),  # CqlDate(u32::MAX)
    ],
)
async def test_date_deserialization_extreme_ranges(
    session: Session,
    table_factory: TableFactory,
    row_id: int,
    value_sql: str,
):
    row = await insert_and_fetch_single_row(
        session=session,
        table_factory=table_factory,
        schema="id int PRIMARY KEY, value date",
        table_name="date_extreme_range_table",
        row_id=row_id,
        value_sql=value_sql,
    )

    # Expected behavior after fix:
    # - Either successfully return a Python date
    # - Or raise a well-defined, documented exception
    assert isinstance(row["value"], datetime.date)


@pytest.mark.asyncio
@pytest.mark.xfail(
    reason=(
        "CQL timestamp values may exceed the range supported by "
        "Python datetime / chrono. Deserialization overflows for "
        "extreme timestamps and currently fails."
    ),
)
@pytest.mark.requires_db
@pytest.mark.parametrize(
    "row_id,value_sql",
    [
        (1, 10**16),  # far future → overflow
        (2, -(10**16)),  # far past → overflow
    ],
)
async def test_timestamp_overflow(
    session: Session,
    table_factory: TableFactory,
    row_id: int,
    value_sql: str,
):
    row = await insert_and_fetch_single_row(
        session=session,
        table_factory=table_factory,
        schema="id int PRIMARY KEY, value timestamp",
        table_name="timestamp_overflow_table",
        row_id=row_id,
        value_sql=value_sql,
    )

    assert isinstance(row["value"], datetime.datetime)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_request_result_columns_for_rows(session: Session):
    result = await session.execute("SELECT cluster_name FROM system.local")

    assert len(result.columns) == 1

    column = result.columns[0]

    assert column.name == "cluster_name"
    assert column.table_name == "local"
    assert column.keyspace_name == "system"
    assert isinstance(column.cql_type, CqlColumnType)
    assert isinstance(column.cql_type, CqlText)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_request_result_columns_for_non_rows(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, value text",
        "result_metadata_non_rows_table",
    )

    result = await session.execute(f"INSERT INTO {table} (id, value) VALUES (1, 'hello')")

    assert result.columns == ()


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_timestamp_overflow_raises_deserialization_error(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, value timestamp",
        "timestamp_overflow_table",
    )

    await session.execute(f"INSERT INTO {table} (id, value) VALUES (1, {10**16})")

    result = await session.execute(f"SELECT * FROM {table} WHERE id = 1")

    with pytest.raises(DeserializationError) as exc_info:
        await result.first_row()

    assert "column" in str(exc_info.value).lower()


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_row_factory_python_error_is_wrapped(session: Session, table_factory: TableFactory):
    class FailingFactory(RowFactory):
        def prepare(self, columns: tuple[ColumnSpec, ...]) -> RowBuilder:
            def build(values: tuple[Any, ...]) -> Any:
                raise ValueError("factory boom")

            return build

    table = await table_factory("id int PRIMARY KEY, x int", "row_factory_fail_table")
    await session.execute(f"INSERT INTO {table} (id, x) VALUES (1, 10)")

    result = await session.execute(f"SELECT * FROM {table}", factory=FailingFactory())

    with pytest.raises(RowIterationError) as exc_info:
        await result.first_row()

    assert exc_info.value.__cause__ is not None
