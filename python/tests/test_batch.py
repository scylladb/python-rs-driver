import copy
from collections.abc import AsyncGenerator, Awaitable, Callable

import pytest
import pytest_asyncio
from helpers.ddl import ddl
from helpers.session import connect
from scylla.errors import BatchError, CqlSyntaxError
from scylla.policies.retry import DefaultRetryPolicy
from scylla.results import TupleRowFactory
from scylla.session import ExecutionProfile, Session
from scylla.statement import UNSET, Batch, BatchType, Consistency, SerialConsistency, Statement


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


async def set_up_without_tablets() -> Session:
    session = await connect()

    await ddl(
        session,
        """
            CREATE KEYSPACE IF NOT EXISTS testks_without_tablets
            WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}
            AND tablets = {'enabled': false};
        """,
    )

    await session.use_keyspace("testks_without_tablets")

    return session


@pytest_asyncio.fixture(scope="module")
async def session_without_tablets():
    session = await set_up_without_tablets()
    yield session
    await ddl(session, "DROP KEYSPACE testks_without_tablets")


@pytest_asyncio.fixture
async def table_factory_without_tablets(session_without_tablets: Session) -> AsyncGenerator[TableFactory, None]:
    created_tables: list[str] = []

    async def create_table(schema: str, name: str) -> str:
        await ddl(session_without_tablets, f"CREATE TABLE IF NOT EXISTS {name} ({schema});")
        created_tables.append(name)
        return name

    yield create_table

    for table in created_tables:
        await ddl(session_without_tablets, f"DROP TABLE IF EXISTS {table};")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_simple_batch(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, name text",
        "users",
    )

    batch = Batch()
    batch.add(f"INSERT INTO {table} (id, name) VALUES (1, 'Alice')")
    batch.add(f"INSERT INTO {table} (id, name) VALUES (2, 'Bob')")
    batch.add(f"INSERT INTO {table} (id, name) VALUES (3, 'Charlie')")

    await session.batch(batch)

    res = await session.execute(f"SELECT id, name FROM {table}")
    rows = sorted(await res.all(), key=lambda r: r["id"])

    assert rows == [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
        {"id": 3, "name": "Charlie"},
    ]


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_simple_batch_bad_query(session: Session):
    batch = Batch(BatchType.Logged)
    batch.add("meow")

    with pytest.raises(CqlSyntaxError):
        await session.batch(batch)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_batch_with_values(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, name text",
        "users",
    )

    batch = Batch()
    batch.add(f"INSERT INTO {table} (id, name) VALUES (?, ?)", (1, "Alice"))
    batch.add(f"INSERT INTO {table} (id, name) VALUES (?, ?)", (2, "Bob"))
    batch.add(f"INSERT INTO {table} (id, name) VALUES (?, ?)", (3, "Charlie"))

    await session.batch(batch)

    res = await session.execute(f"SELECT id, name FROM {table}")
    rows = sorted(await res.all(), key=lambda r: r["id"])

    assert rows == [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
        {"id": 3, "name": "Charlie"},
    ]


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_batch_with_statements(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, name text",
        "users",
    )

    statement = Statement(f"INSERT INTO {table} (id, name) VALUES (?, ?)")
    values = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]

    batch = Batch()

    for value in values:
        batch.add(statement, value)

    await session.batch(batch)

    res = await session.execute(f"SELECT id, name FROM {table}")
    rows = sorted(await res.all(), key=lambda r: r["id"])

    assert rows == [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
        {"id": 3, "name": "Charlie"},
    ]


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_batch_with_prepared_statements(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, name text",
        "users",
    )

    query_str = f"INSERT INTO {table} (id, name) VALUES (?, ?)"
    prepared = await session.prepare(query_str)
    values = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]

    batch = Batch()

    for value in values:
        batch.add(prepared, value)

    await session.batch(batch)

    res = await session.execute(f"SELECT id, name FROM {table}")
    rows = sorted(await res.all(), key=lambda r: r["id"])

    assert rows == [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
        {"id": 3, "name": "Charlie"},
    ]


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_batch_with_prepared_and_unprepared(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, name text",
        "users",
    )

    query_str = f"INSERT INTO {table} (id, name) VALUES (?, ?)"
    prepared = await session.prepare(query_str)
    statement = Statement(query_str)

    batch = Batch()

    batch.add(query_str, (1, "Alice"))
    batch.add(statement, (2, "Bob"))
    batch.add(prepared, (3, "Charlie"))

    await session.batch(batch)

    res = await session.execute(f"SELECT id, name FROM {table}")
    rows = sorted(await res.all(), key=lambda r: r["id"])

    assert rows == [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
        {"id": 3, "name": "Charlie"},
    ]


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_batch_with_lwt_not_applied(
    session_without_tablets: Session, table_factory_without_tablets: TableFactory
):
    table = await table_factory_without_tablets(
        "id int, subid int, name text, PRIMARY KEY (id, subid)",
        "users",
    )

    await session_without_tablets.execute(f"INSERT INTO {table} (id, subid, name) VALUES (1, 1, 'Alice')")

    batch = Batch()
    query_str = f"INSERT INTO {table} (id, subid, name) VALUES (?, ?, ?) IF NOT EXISTS"
    batch.add_all(
        [
            (query_str, (1, 1, "Bob")),
            (query_str, (1, 1, "Charlie")),
            (query_str, (1, 2, "Daniel")),
            (query_str, (1, 3, "Edward")),
        ]
    )

    res = await session_without_tablets.batch(batch)
    rows = list(await res.all())
    expected_row_alice = {"[applied]": False, "id": 1, "subid": 1, "name": "Alice"}
    expected_row_rest = {"[applied]": False, "id": None, "subid": None, "name": None}
    assert rows == [expected_row_alice, expected_row_alice, expected_row_rest, expected_row_rest]

    res = await session_without_tablets.execute(f"SELECT * FROM {table}")
    rows = sorted(await res.all(), key=lambda r: r["subid"])
    assert rows == [{"id": 1, "subid": 1, "name": "Alice"}]


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_batch_with_lwt_applied(session_without_tablets: Session, table_factory_without_tablets: TableFactory):
    table = await table_factory_without_tablets(
        "id int, subid int, name text, PRIMARY KEY (id, subid)",
        "users",
    )

    await session_without_tablets.execute(f"INSERT INTO {table} (id, subid, name) VALUES (1, 1, 'Alice')")

    batch = Batch()
    query_str = f"INSERT INTO {table} (id, subid, name) VALUES (?, ?, ?) IF NOT EXISTS"
    batch.add_all([(query_str, (1, 2, "Charlie")), (query_str, (1, 3, "Daniel"))])

    res = await session_without_tablets.batch(batch)
    rows = list(await res.all())
    expected_row = {"[applied]": True, "id": None, "subid": None, "name": None}
    assert rows == [expected_row, expected_row]

    res = await session_without_tablets.execute(f"SELECT * FROM {table}")
    rows = sorted(await res.all(), key=lambda r: r["subid"])
    assert rows == [
        {"id": 1, "subid": 1, "name": "Alice"},
        {"id": 1, "subid": 2, "name": "Charlie"},
        {"id": 1, "subid": 3, "name": "Daniel"},
    ]


def test_batch_type():
    batch = Batch()
    assert str(batch.type) == "BatchType.Logged"

    batch = Batch(BatchType.Logged)
    assert str(batch.type) == "BatchType.Logged"

    batch = Batch(BatchType.Unlogged)
    assert str(batch.type) == "BatchType.Unlogged"

    batch = Batch(BatchType.Counter)
    assert str(batch.type) == "BatchType.Counter"


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_batch_add_all(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, name text",
        "users",
    )

    batch = Batch()
    batch.add_all(
        [
            (f"INSERT INTO {table} (id, name) VALUES (1, 'Alice')", None),
            (f"INSERT INTO {table} (id, name) VALUES (?, ?)", (2, "Bob")),
            (f"INSERT INTO {table} (id, name) VALUES (?, ?)", (3, "Charlie")),
        ]
    )

    await session.batch(batch)

    res = await session.execute(f"SELECT id, name FROM {table}")
    rows = sorted(await res.all(), key=lambda r: r["id"])

    assert rows == [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
        {"id": 3, "name": "Charlie"},
    ]


@pytest.mark.parametrize("copy_fn", [Batch.copy, copy.copy])
def test_batch_copy_configuration(copy_fn: Callable[[Batch], Batch]):
    profile = ExecutionProfile()
    factory = TupleRowFactory()
    batch = Batch(BatchType.Unlogged)
    batch.consistency = Consistency.Quorum
    batch.execution_profile = profile
    batch.row_factory = factory

    copied = copy_fn(batch)

    assert isinstance(copied, Batch)
    assert copied is not batch
    assert copied.type == BatchType.Unlogged
    assert copied.consistency == Consistency.Quorum
    assert copied.execution_profile is profile
    assert copied.row_factory is factory

    copied.consistency = Consistency.One
    copied.execution_profile = None
    copied.row_factory = None

    assert batch.consistency == Consistency.Quorum
    assert batch.execution_profile is profile
    assert batch.row_factory is factory


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_batch_copy_statements(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, name text",
        "users_copy",
    )

    batch = Batch()
    batch.add(f"INSERT INTO {table} (id, name) VALUES (?, ?)", (1, "Alice"))

    copied = batch.copy()
    copied.add(f"INSERT INTO {table} (id, name) VALUES (?, ?)", (2, "Bob"))

    await session.batch(batch)
    res = await session.execute(f"SELECT id, name FROM {table}")
    assert await res.all() == [{"id": 1, "name": "Alice"}]

    await session.batch(copied)
    res = await session.execute(f"SELECT id, name FROM {table}")
    rows = sorted(await res.all(), key=lambda r: r["id"])
    assert rows == [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]


def test_batch_execution_profile():
    batch = Batch()
    profile = ExecutionProfile()

    batch.execution_profile = profile
    assert batch.execution_profile is profile

    batch.execution_profile = None
    assert batch.execution_profile is None


def test_batch_consistency():
    batch = Batch()

    assert batch.consistency is UNSET

    batch.consistency = Consistency.All
    assert isinstance(batch.consistency, Consistency)

    batch.consistency = UNSET
    assert batch.consistency is UNSET


def test_batch_serial_consistency():
    batch = Batch()

    assert batch.serial_consistency is UNSET

    batch.serial_consistency = None
    assert batch.serial_consistency is None

    batch.serial_consistency = SerialConsistency.LocalSerial
    assert isinstance(batch.serial_consistency, SerialConsistency)

    batch.serial_consistency = UNSET
    assert batch.serial_consistency is UNSET


def test_batch_request_timeout():
    batch = Batch()

    assert batch.request_timeout is UNSET

    batch.request_timeout = 30.0
    assert batch.request_timeout == 30.0

    batch.request_timeout = None
    assert batch.request_timeout is None

    batch.request_timeout = UNSET
    assert batch.request_timeout is UNSET


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_batch_multiple_batch_executions(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, name text",
        "users",
    )

    batch = Batch()
    batch.add(f"INSERT INTO {table} (id, name) VALUES (?, ?)", (1, "Alice"))

    await session.batch(batch)

    res = await session.execute(f"SELECT id, name FROM {table}")
    rows = sorted(await res.all(), key=lambda r: r["id"])

    assert rows == [
        {"id": 1, "name": "Alice"},
    ]

    await session.execute(f"TRUNCATE TABLE {table}")

    await session.batch(batch)

    res = await session.execute(f"SELECT id, name FROM {table}")
    rows = sorted(await res.all(), key=lambda r: r["id"])

    assert rows == [
        {"id": 1, "name": "Alice"},
    ]


def test_batch_timeout_too_large():
    batch = Batch()

    with pytest.raises(BatchError) as exc_info:
        batch.request_timeout = 1e30

    assert "timeout must be a non-negative, finite number" in str(exc_info.value).lower()


def test_batch_negative_timeout():
    batch = Batch()

    with pytest.raises(BatchError) as exc_info:
        batch.request_timeout = -1.0

    assert "timeout must be a non-negative, finite number" in str(exc_info.value).lower()


def test_batch_timeout_not_finite():
    batch = Batch()

    with pytest.raises(BatchError) as exc_info:
        batch.request_timeout = float("inf")

    assert "timeout must be a non-negative, finite number" in str(exc_info.value).lower()


def test_batch_retry_policy_default():
    batch = Batch()

    assert batch.retry_policy is None


def test_batch_set_retry_policy():
    batch = Batch()
    policy = DefaultRetryPolicy()

    batch.retry_policy = policy

    assert batch.retry_policy is policy


def test_batch_clear_retry_policy():
    batch = Batch()
    batch.retry_policy = DefaultRetryPolicy()

    batch.retry_policy = None

    assert batch.retry_policy is None


def test_batch_is_idempotent_default():
    batch = Batch()

    assert batch.is_idempotent is False


def test_batch_set_is_idempotent_true():
    batch = Batch()

    batch.is_idempotent = True

    assert batch.is_idempotent is True


def test_batch_set_is_idempotent_false():
    batch = Batch()
    batch.is_idempotent = True

    batch.is_idempotent = False

    assert batch.is_idempotent is False
