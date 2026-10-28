import ipaddress
import pickle
import uuid
from collections.abc import AsyncGenerator, Awaitable, Callable
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, time, timedelta, timezone, tzinfo
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from dateutil.relativedelta import relativedelta
from helpers.ddl import ddl
from helpers.session import connect
from scylla.cql_types import CqlDate, CqlDuration, CqlTime, CqlTimestamp
from scylla.errors import (
    InvalidValueSerializationError,
    MissingValue,
    SerializationError,
    TypeMismatchSerializationError,
    ValueOverflowSerializationError,
    WrongNumberOfValues,
)
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
@pytest.mark.parametrize(
    "val,expected",
    [
        (date(2004, 6, 16), CqlDate.from_date(date(2004, 6, 16))),
        (CqlDate(-(2**31)), CqlDate(-(2**31))),
        (CqlDate(2**31 - 1), CqlDate(2**31 - 1)),
    ],
)
async def test_date_serialization(
    session: Session, table_factory: TableFactory, val: CqlDate | date, expected: CqlDate
):
    table = await table_factory(
        "id int PRIMARY KEY, col date",
        "date_table",
    )

    await session.execute(f"INSERT INTO {table} (id, col) VALUES (?, ?)", (1, val))
    result = await session.execute(f"SELECT col FROM {table} WHERE id = 1")
    row = await result.first_row()

    assert row is not None
    assert row["col"] == expected


def test_date_value_semantics():
    d = CqlDate.from_date(date(2024, 5, 10))

    assert d.days == (date(2024, 5, 10) - date(1970, 1, 1)).days
    assert d.to_date() == date(2024, 5, 10)
    assert CqlDate(0).to_date() == date(1970, 1, 1)
    assert CqlDate(-1) < CqlDate(0) < CqlDate(1)
    assert hash(d) == hash(CqlDate(d.days))
    assert repr(CqlDate(5)) == "CqlDate(days=5)"
    assert str(d) == "2024-05-10"
    assert str(CqlDate(2**31 - 1)) == str(2**31 - 1)
    assert pickle.loads(pickle.dumps(d)) == d
    with pytest.raises(ValueError):
        CqlDate(2**31 - 1).to_date()
    with pytest.raises(OverflowError):
        CqlDate(2**31)


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
@pytest.mark.parametrize(
    "val,expected",
    [
        (CqlDuration(months=14, days=40, nanoseconds=7), CqlDuration(months=14, days=40, nanoseconds=7)),
        (CqlDuration(months=-1, days=-2, nanoseconds=-3), CqlDuration(months=-1, days=-2, nanoseconds=-3)),
        (relativedelta(months=2, days=5, microseconds=36), CqlDuration(months=2, days=5, nanoseconds=36_000)),
        # relativedelta stores this as years=1, months=2, days=1, hours=1, seconds=5.
        (relativedelta(months=14, hours=25, seconds=5), CqlDuration(months=14, days=1, nanoseconds=3_605_000_000_000)),
        (relativedelta(weeks=2, minutes=90), CqlDuration(months=0, days=14, nanoseconds=5_400_000_000_000)),
        (relativedelta(hours=1, minutes=-30), CqlDuration(months=0, days=0, nanoseconds=1_800_000_000_000)),
    ],
)
async def test_duration_serialization(
    session: Session, table_factory: TableFactory, val: CqlDuration | relativedelta, expected: CqlDuration
):
    table = await table_factory(
        "id int PRIMARY KEY, col duration",
        "duration_table",
    )

    await session.execute(f"INSERT INTO {table} (id, col) VALUES (?, ?)", (1, val))
    result = await session.execute(f"SELECT col FROM {table} WHERE id = 1")
    row = await result.first_row()

    assert row is not None
    assert row["col"] == expected


def test_duration_value_semantics():
    d = CqlDuration(months=1, days=2, nanoseconds=3)

    assert (d.months, d.days, d.nanoseconds) == (1, 2, 3)
    assert CqlDuration() == CqlDuration(0, 0, 0)
    assert d == CqlDuration(1, 2, 3)
    assert d != CqlDuration(1, 2, 4)
    assert hash(d) == hash(CqlDuration(1, 2, 3))
    assert repr(d) == "CqlDuration(months=1, days=2, nanoseconds=3)"
    assert pickle.loads(pickle.dumps(d)) == d


@pytest.mark.parametrize(
    "kwargs,error",
    [
        ({"months": 1, "days": -1}, ValueError),
        ({"days": -1, "nanoseconds": 1}, ValueError),
        ({"months": 2**31}, OverflowError),
        ({"nanoseconds": 2**63}, OverflowError),
    ],
)
def test_duration_rejects_invalid_parts(kwargs: dict[str, int], error: type[Exception]):
    with pytest.raises(error):
        CqlDuration(**kwargs)


@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize(
    "val",
    [
        relativedelta(years=1, days=-1),
        relativedelta(days=1, hours=-1),
    ],
)
async def test_duration_serialization_mixed_sign(session: Session, table_factory: TableFactory, val: relativedelta):
    table = await table_factory(
        "id int PRIMARY KEY, col duration",
        "duration_table_mixed_sign",
    )

    with pytest.raises(InvalidValueSerializationError) as exc_info:
        await session.execute(f"INSERT INTO {table} (id, col) VALUES (?, ?)", (1, val))

    assert "all duration fields must have the same sign" in str(exc_info.value).lower()
    assert exc_info.value.parameter == 1


def test_duration_relativedelta_conversion():
    d = CqlDuration.from_relativedelta(relativedelta(years=1, months=2, days=3, hours=4, microseconds=5))

    assert d == CqlDuration(months=14, days=3, nanoseconds=14_400_000_005_000)
    assert d.to_relativedelta() == relativedelta(years=1, months=2, days=3, hours=4, microseconds=5)
    # The last three digits of the nanoseconds are lost, and 25 hours become 1 day and 1 hour.
    assert CqlDuration(nanoseconds=90_000_000_000_999).to_relativedelta() == relativedelta(days=1, hours=1)
    assert CqlDuration(months=-1, nanoseconds=-1000).to_relativedelta() == relativedelta(months=-1, microseconds=-1)
    with pytest.raises(TypeError):
        CqlDuration.from_relativedelta(timedelta(days=1))  # pyright: ignore[reportArgumentType]
    with pytest.raises(ValueError):
        CqlDuration.from_relativedelta(relativedelta(days=1, hours=-1))
    with pytest.raises(OverflowError):
        CqlDuration.from_relativedelta(relativedelta(years=2**31))


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

    with pytest.raises(ValueOverflowSerializationError) as exc_info:
        await session.execute(f"INSERT INTO {table} (id, col) VALUES (?, ?)", (1, val))

    assert exc_info.value.parameter == 1


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

    with pytest.raises(ValueOverflowSerializationError) as exc_info:
        await session.execute(f"INSERT INTO {table} (id, col) VALUES (?, ?)", (1, val))

    assert exc_info.value.parameter == 1


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
@pytest.mark.parametrize(
    "val,expected",
    [
        (
            datetime(1, 1, 1, 23, 59, 59, 999000, tzinfo=UTC),
            CqlTimestamp.from_datetime(datetime(1, 1, 1, 23, 59, 59, 999000, tzinfo=UTC)),
        ),
        (datetime(2024, 5, 10, 14, 30, tzinfo=timezone(timedelta(hours=2))), CqlTimestamp(1_715_344_200_000)),
        (datetime(2024, 5, 10, 14, 30, tzinfo=ZoneInfo("Europe/Warsaw")), CqlTimestamp(1_715_344_200_000)),
        (CqlTimestamp(0), CqlTimestamp(0)),
        (CqlTimestamp(-1), CqlTimestamp(-1)),
        (CqlTimestamp(10**16), CqlTimestamp(10**16)),
    ],
)
async def test_timestamp_serialization(
    session: Session, table_factory: TableFactory, val: CqlTimestamp | datetime, expected: CqlTimestamp
):
    table = await table_factory(
        "id int PRIMARY KEY, col timestamp",
        "timestamp_table",
    )

    await session.execute(f"INSERT INTO {table} (id, col) VALUES (?, ?)", (1, val))
    result = await session.execute(f"SELECT col FROM {table} WHERE id = 1")
    row = await result.first_row()

    assert row is not None
    assert row["col"] == expected


class _NoOffset(tzinfo):
    def utcoffset(self, dt: datetime | None) -> None:
        return None


def test_timestamp_value_semantics():
    t = CqlTimestamp.from_datetime(datetime(2024, 5, 10, 12, 30, 15, 123456, tzinfo=UTC))

    assert t.milliseconds == 1_715_344_215_123
    assert (
        CqlTimestamp.from_datetime(datetime(2024, 5, 10, 14, 30, 15, 123000, tzinfo=timezone(timedelta(hours=2)))) == t
    )
    assert CqlTimestamp.from_datetime(datetime(2024, 5, 10, 14, 30, 15, 123000, tzinfo=ZoneInfo("Europe/Warsaw"))) == t
    with pytest.raises(TypeError):
        CqlTimestamp.from_datetime(datetime(2024, 5, 10, tzinfo=UTC).replace(tzinfo=None))
    with pytest.raises(TypeError):
        CqlTimestamp.from_datetime(datetime(2024, 5, 10, tzinfo=_NoOffset()))
    assert t.to_datetime() == datetime(2024, 5, 10, 12, 30, 15, 123000, tzinfo=UTC)
    assert CqlTimestamp(-1) < CqlTimestamp(0) < CqlTimestamp(1)
    assert hash(t) == hash(CqlTimestamp(t.milliseconds))
    assert repr(CqlTimestamp(5)) == "CqlTimestamp(milliseconds=5)"
    assert str(t) == "2024-05-10T12:30:15.123Z"
    assert str(CqlTimestamp(10**16)) == str(10**16)
    assert pickle.loads(pickle.dumps(t)) == t
    with pytest.raises(ValueError):
        CqlTimestamp(10**16).to_datetime()
    with pytest.raises(OverflowError):
        CqlTimestamp(2**63)


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


class _IPv4Subclass(ipaddress.IPv4Address):
    pass


class _IPv6Subclass(ipaddress.IPv6Address):
    pass


@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize(
    "value",
    [
        _IPv4Subclass("192.168.1.10"),
        _IPv6Subclass("2001:db8::1"),
        ipaddress.IPv4Address("0.0.0.0"),
        ipaddress.IPv6Address("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"),
    ],
)
async def test_inet_serialization_reads_back_the_same_value(
    session: Session, table_factory: TableFactory, value: ipaddress.IPv4Address | ipaddress.IPv6Address
):
    table = await table_factory("id int PRIMARY KEY, col inet", "inet_roundtrip_table")

    await session.execute(f"INSERT INTO {table} (id, col) VALUES (?, ?)", (1, value))
    result = await session.execute(f"SELECT col FROM {table} WHERE id = 1")
    row = await result.first_row()

    assert row is not None
    assert row["col"] == ipaddress.ip_address(str(value))


class _PackedOnly:
    """Not an `ipaddress` object, but has the `packed` attribute that was enough before."""

    packed = ipaddress.IPv4Address("10.0.0.1").packed


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_inet_serialization_rejects_an_object_with_only_a_packed_attribute(
    session: Session, table_factory: TableFactory
):
    table = await table_factory("id int PRIMARY KEY, col inet", "inet_packed_only_table")

    with pytest.raises(SerializationError):
        await session.execute(f"INSERT INTO {table} (id, col) VALUES (?, ?)", (1, _PackedOnly()))


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

    with pytest.raises(ValueOverflowSerializationError) as exc_info:
        await session.execute(f"INSERT INTO {table} (id, col) VALUES (?, ?)", (1, val))

    assert exc_info.value.parameter == 1


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

    with pytest.raises(ValueOverflowSerializationError) as exc_info:
        await session.execute(f"INSERT INTO {table} (id, col) VALUES (?, ?)", (1, val))

    assert exc_info.value.parameter == 1


@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize(
    "val,expected",
    [
        (time(1, 30, 45, 500000), CqlTime(5_445_500_000_000)),
        (CqlTime(0), CqlTime(0)),
        (CqlTime(86_399_999_999_999), CqlTime(86_399_999_999_999)),
    ],
)
async def test_time_serialization(
    session: Session, table_factory: TableFactory, val: CqlTime | time, expected: CqlTime
):
    table = await table_factory(
        "id int PRIMARY KEY, col time",
        "time_table",
    )

    await session.execute(f"INSERT INTO {table} (id, col) VALUES (?, ?)", (1, val))
    result = await session.execute(f"SELECT col FROM {table} WHERE id = 1")
    row = await result.first_row()

    assert row is not None
    assert row["col"] == expected


def test_time_value_semantics():
    t = CqlTime(5_445_500_000_123)

    assert (t.hour, t.minute, t.second, t.nanosecond) == (1, 30, 45, 500_000_123)
    assert t.to_time() == time(1, 30, 45, 500000)
    assert CqlTime.from_time(time(1, 30, 45, 500000)) == CqlTime(5_445_500_000_000)
    assert CqlTime(1) < CqlTime(2)
    assert hash(t) == hash(CqlTime(t.nanoseconds))
    assert repr(CqlTime(5)) == "CqlTime(nanoseconds=5)"
    assert str(t) == "01:30:45.500000123"
    assert pickle.loads(pickle.dumps(t)) == t
    for nanoseconds in (-1, 86_400_000_000_000):
        with pytest.raises(ValueError):
            CqlTime(nanoseconds)


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


class _UuidSubclass(uuid.UUID):
    pass


@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize("column_type,value", [("uuid", uuid.uuid4()), ("timeuuid", uuid.uuid1())])
async def test_uuid_serialization_reads_back_the_same_value(
    session: Session, table_factory: TableFactory, column_type: str, value: uuid.UUID
):
    table = await table_factory(f"id int PRIMARY KEY, col {column_type}", f"{column_type}_roundtrip_table")

    await session.execute(f"INSERT INTO {table} (id, col) VALUES (?, ?)", (1, _UuidSubclass(int=value.int)))
    result = await session.execute(f"SELECT col FROM {table} WHERE id = 1")
    row = await result.first_row()

    assert row is not None
    assert row["col"] == value


class _BytesOnly:
    """Not a `uuid.UUID`, but has the 16-byte `bytes` attribute that was enough before."""

    bytes = uuid.uuid4().bytes


@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize("column_type", ["uuid", "timeuuid"])
async def test_uuid_serialization_rejects_an_object_with_only_a_bytes_attribute(
    session: Session, table_factory: TableFactory, column_type: str
):
    table = await table_factory(f"id int PRIMARY KEY, col {column_type}", f"{column_type}_bytes_only_table")

    with pytest.raises(SerializationError):
        await session.execute(f"INSERT INTO {table} (id, col) VALUES (?, ?)", (1, _BytesOnly()))


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

    with pytest.raises(TypeMismatchSerializationError) as exc_info:
        await session.execute(
            f"INSERT INTO {table} (id, tags) VALUES (?, ?)",
            {"id": 1, "tags": invalid_values},
        )

    assert exc_info.value.parameter == "tags"


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_serialization_rejects_wrong_number_of_values(session: Session, table_factory: TableFactory):
    table = await table_factory("id int PRIMARY KEY, col int", "wrong_value_count")

    with pytest.raises(WrongNumberOfValues) as exc_info:
        await session.execute(f"INSERT INTO {table} (id, col) VALUES (?, ?)", (1,))

    assert exc_info.value.expected == 2
    assert exc_info.value.received == 1


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_serialization_rejects_missing_named_value(session: Session, table_factory: TableFactory):
    table = await table_factory("id int PRIMARY KEY, col int", "missing_named_value")

    with pytest.raises(MissingValue) as exc_info:
        await session.execute(f"INSERT INTO {table} (id, col) VALUES (:id, :col)", {"id": 1, "other": 2})

    assert exc_info.value.parameter == "col"


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_serialization_rejects_wrong_value_type(session: Session, table_factory: TableFactory):
    table = await table_factory("id int PRIMARY KEY, col int", "wrong_value_type")

    with pytest.raises(TypeMismatchSerializationError) as exc_info:
        await session.execute(f"INSERT INTO {table} (id, col) VALUES (?, ?)", (1, "x"))

    assert isinstance(exc_info.value, TypeError)
    assert exc_info.value.parameter == 1


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_serialization_rejects_wrong_element_type(session: Session, table_factory: TableFactory):
    table = await table_factory("id int PRIMARY KEY, col list<int>", "wrong_element_type")

    with pytest.raises(TypeMismatchSerializationError) as exc_info:
        await session.execute(f"INSERT INTO {table} (id, col) VALUES (?, ?)", (1, [1, "x"]))

    assert exc_info.value.parameter == 1


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
async def test_vector_serialization_rejects_wrong_dimension(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, embedding vector<int, 4>",
        "vector_wrong_dimension_table",
    )

    with pytest.raises(InvalidValueSerializationError) as exc_info:
        await session.execute(f"INSERT INTO {table} (id, embedding) VALUES (?, ?)", (1, [1, 2, 3]))

    assert exc_info.value.parameter == 1


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

    with pytest.raises(TypeMismatchSerializationError) as exc_info:
        await session.execute(
            f"INSERT INTO {table} (id, tags, scores) VALUES (?, ?, ?)",
            invalid_values,
        )

    assert exc_info.value.parameter == 1


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

    with pytest.raises(TypeMismatchSerializationError) as exc_info:
        await session.execute(
            f"INSERT INTO {table} (id, tags, scores) VALUES (?, ?, ?)",
            invalid_values,
        )

    assert exc_info.value.parameter == 1


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_tuple_serialization_rejects_too_many_elements(session: Session, table_factory: TableFactory):
    table = await table_factory("id int PRIMARY KEY, v frozen<tuple<int, int>>", "tuple_too_long_table")

    with pytest.raises(SerializationError) as exc_info:
        await session.execute(f"INSERT INTO {table} (id, v) VALUES (?, ?)", (1, (1, 2, 3)))

    assert "wrong tuple element count" in str(exc_info.value).lower()


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_tuple_serialization_accepts_too_few_elements(session: Session, table_factory: TableFactory):
    table = await table_factory("id int PRIMARY KEY, v frozen<tuple<int, int>>", "tuple_too_short_table")

    await session.execute(f"INSERT INTO {table} (id, v) VALUES (?, ?)", (1, (1,)))
    result = await session.execute(f"SELECT id FROM {table} WHERE v = (1, null) ALLOW FILTERING")
    row = await result.first_row()

    # The database reads the missing trailing element as null.
    assert row is not None
    assert row["id"] == 1


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
