import dataclasses
from collections.abc import AsyncGenerator, Awaitable, Callable
from typing import Any

import pytest
import pytest_asyncio
from helpers.ddl import ddl
from helpers.session import CONTACT_POINTS, session_builder
from scylla.batch import Batch
from scylla.cluster.metadata import ColumnSpec
from scylla.errors import RowFactoryError, RowIterationError
from scylla.execution_profile import ExecutionProfile
from scylla.results import (
    ClassRowFactory,
    DictRowFactory,
    NamedTupleRowFactory,
    RowBuilder,
    RowFactory,
    TupleRowFactory,
)
from scylla.session import Session
from scylla.session_builder import SessionBuilder
from scylla.statement import Statement

KEYSPACE = "row_factory_ks"


async def set_up(builder: SessionBuilder) -> Session:
    session = await builder.connect()

    await ddl(
        session,
        f"""
            CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
            WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}
            AND tablets = {{'enabled': false}};
        """,
    )

    await session.use_keyspace(KEYSPACE)

    return session


@pytest_asyncio.fixture(scope="module")
async def session() -> AsyncGenerator[Session, None]:
    """A session that names no default factory, so requests reach the built-in one."""
    session = await set_up(SessionBuilder().contact_points(CONTACT_POINTS))
    yield session
    await ddl(session, f"DROP KEYSPACE {KEYSPACE}")


@pytest_asyncio.fixture(scope="module")
async def dict_session() -> Session:
    """A session whose default execution profile asks for `dict` rows."""
    return await set_up(session_builder())


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


@pytest_asyncio.fixture
async def users(session: Session, table_factory: TableFactory) -> str:
    table = await table_factory("id int PRIMARY KEY, name text", "row_factory_users")
    await session.execute(f"INSERT INTO {table} (id, name) VALUES (1, 'alice')")
    return table


@pytest_asyncio.fixture
async def lwt_table(table_factory: TableFactory) -> str:
    return await table_factory("id int PRIMARY KEY, name text", "row_factory_lwt")


def conditional_batch(table: str) -> Batch:
    batch = Batch()
    batch.add(f"INSERT INTO {table} (id, name) VALUES (1, 'alice') IF NOT EXISTS")
    return batch


@dataclasses.dataclass
class User:
    id: int
    name: str


class JoinedRowFactory(RowFactory):
    """A factory of the `prepare` kind: the column names are resolved once, up
    front, and the builder returned from `prepare` runs for every row."""

    def __init__(self) -> None:
        self.prepared = 0

    def prepare(self, columns: tuple[ColumnSpec, ...]) -> RowBuilder:
        self.prepared += 1
        names = [column.name for column in columns]

        return lambda values: "|".join(f"{name}={value}" for name, value in zip(names, values))


# ---------------------------------------------------------------------------
# What each kind of factory builds
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize(
    "factory,expected",
    [
        (DictRowFactory(), {"id": 1, "name": "alice"}),
        (TupleRowFactory(), (1, "alice")),
        (ClassRowFactory(User), User(id=1, name="alice")),
        (list, [1, "alice"]),
        (JoinedRowFactory(), "id=1|name=alice"),
    ],
    ids=["dict", "tuple", "class", "callable", "prepare"],
)
async def test_factory_shapes(session: Session, users: str, factory: Any, expected: Any):
    result = await session.execute(f"SELECT id, name FROM {users}", factory=factory)

    assert await result.first_row() == expected


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_default_factory_builds_namedtuples(session: Session, users: str):
    row = await (await session.execute(f"SELECT id, name FROM {users}")).first_row()

    assert row is not None
    assert row._fields == ("id", "name")
    assert row.id == 1
    assert row.name == "alice"
    # A namedtuple is still a tuple: indexing and comparison keep working.
    assert row == (1, "alice")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_class_row_factory_passes_column_names_as_keywords(session: Session, users: str):
    # `cls(**columns)`, so the column order in the query does not matter.
    result = await session.execute(f"SELECT name, id FROM {users}", factory=ClassRowFactory(User))

    assert await result.first_row() == User(id=1, name="alice")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepare_runs_once_per_request(session: Session, users: str):
    factory = JoinedRowFactory()

    for _ in range(3):
        await session.execute(f"SELECT id, name FROM {users}", factory=factory)

    assert factory.prepared == 3


@pytest_asyncio.fixture
async def paged_table(session: Session, table_factory: TableFactory) -> str:
    table = await table_factory("id int, ck int, b int, PRIMARY KEY (id, ck)", "row_factory_paged")
    for ck in range(4):
        await session.execute(f"INSERT INTO {table} (id, ck, b) VALUES (0, {ck}, {ck})")
    return table


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepare_runs_once_per_page(session: Session, paged_table: str):
    factory = JoinedRowFactory()
    statement = Statement(f"SELECT id, ck FROM {paged_table}").with_page_size(2)

    rows = await (await session.execute(statement, factory=factory)).all()

    assert len(rows) == 4
    # Two full pages, plus the empty last page that tells there is no more.
    assert factory.prepared in (2, 3)


class FailsOnSecondPage(JoinedRowFactory):
    def prepare(self, columns: tuple[ColumnSpec, ...]) -> RowBuilder:
        if self.prepared:
            raise ValueError("prepare failed")
        return super().prepare(columns)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_all_wraps_prepare_errors_of_later_pages(session: Session, paged_table: str):
    statement = Statement(f"SELECT id, ck FROM {paged_table}").with_page_size(2)
    result = await session.execute(statement, factory=FailsOnSecondPage())

    with pytest.raises(RowIterationError) as exc_info:
        await result.all()

    assert isinstance(exc_info.value.__cause__, ValueError)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_fetch_next_page_follows_columns_added_between_pages(session: Session, paged_table: str):
    statement = Statement(f"SELECT * FROM {paged_table}").with_page_size(2)
    result = await session.execute(statement, factory=DictRowFactory())
    assert list(result.iter_current_page()) == [{"id": 0, "ck": 0, "b": 0}, {"id": 0, "ck": 1, "b": 1}]

    # Regular columns are ordered by name, so `a` lands before `b`.
    await ddl(session, f"ALTER TABLE {paged_table} ADD a int")
    next_page = await result.fetch_next_page()

    assert next_page is not None
    assert list(next_page.iter_current_page()) == [
        {"id": 0, "ck": 2, "a": None, "b": 2},
        {"id": 0, "ck": 3, "a": None, "b": 3},
    ]


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_async_iteration_follows_columns_added_between_pages(session: Session, paged_table: str):
    statement = Statement(f"SELECT * FROM {paged_table}").with_page_size(2)
    result = await session.execute(statement)

    rows: list[Any] = []
    async for row in result:
        rows.append(row)
        if len(rows) == 2:
            await ddl(session, f"ALTER TABLE {paged_table} ADD a int")

    assert rows[1]._fields == ("id", "ck", "b")
    assert rows[2]._fields == ("id", "ck", "a", "b")
    assert rows[2] == (0, 2, None, 2)


# ---------------------------------------------------------------------------
# Precedence: execute > statement > statement's profile > session's profile
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_execute_factory_wins_over_statement(session: Session, users: str):
    statement = Statement(f"SELECT id, name FROM {users}").with_row_factory(TupleRowFactory())

    result = await session.execute(statement, factory=DictRowFactory())

    assert await result.first_row() == {"id": 1, "name": "alice"}


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_statement_factory_wins_over_its_execution_profile(session: Session, users: str):
    profile = ExecutionProfile(row_factory=TupleRowFactory())
    statement = (
        Statement(f"SELECT id, name FROM {users}").with_execution_profile(profile).with_row_factory(DictRowFactory())
    )

    result = await session.execute(statement)

    assert await result.first_row() == {"id": 1, "name": "alice"}


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_statement_execution_profile_wins_over_session_default(dict_session: Session, users: str):
    profile = ExecutionProfile(row_factory=TupleRowFactory())
    statement = Statement(f"SELECT id, name FROM {users}").with_execution_profile(profile)

    result = await dict_session.execute(statement)

    assert await result.first_row() == (1, "alice")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_session_default_wins_over_builtin(dict_session: Session, users: str):
    result = await dict_session.execute(f"SELECT id, name FROM {users}")

    assert await result.first_row() == {"id": 1, "name": "alice"}


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_without_row_factory_falls_back_to_execution_profile(session: Session, users: str):
    profile = ExecutionProfile(row_factory=DictRowFactory())
    statement = (
        Statement(f"SELECT id, name FROM {users}")
        .with_execution_profile(profile)
        .with_row_factory(TupleRowFactory())
        .without_row_factory()
    )

    result = await session.execute(statement)

    assert await result.first_row() == {"id": 1, "name": "alice"}


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_statement_keeps_the_factory_it_was_prepared_with(session: Session, users: str):
    prepared = await session.prepare(Statement(f"SELECT id, name FROM {users}").with_row_factory(DictRowFactory()))

    result = await session.execute(prepared)

    assert await result.first_row() == {"id": 1, "name": "alice"}


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_statement_factory_can_be_replaced(session: Session, users: str):
    prepared = await session.prepare(Statement(f"SELECT id, name FROM {users}").with_row_factory(DictRowFactory()))

    result = await session.execute(prepared.with_row_factory(TupleRowFactory()))

    assert await result.first_row() == (1, "alice")


# ---------------------------------------------------------------------------
# The same chain, for batches
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_batch_factory_argument_wins_over_batch(session: Session, lwt_table: str):
    batch = conditional_batch(lwt_table).with_row_factory(TupleRowFactory())

    row = await (await session.batch(batch, factory=DictRowFactory())).first_row()

    assert row is not None
    assert row["[applied]"] is True


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_batch_factory_wins_over_its_execution_profile(session: Session, lwt_table: str):
    profile = ExecutionProfile(row_factory=TupleRowFactory())
    batch = conditional_batch(lwt_table).with_execution_profile(profile).with_row_factory(DictRowFactory())

    row = await (await session.batch(batch)).first_row()

    assert row is not None
    assert row["[applied]"] is True


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_batch_execution_profile_wins_over_session_default(dict_session: Session, lwt_table: str):
    profile = ExecutionProfile(row_factory=TupleRowFactory())
    batch = conditional_batch(lwt_table).with_execution_profile(profile)

    row = await (await dict_session.batch(batch)).first_row()

    assert isinstance(row, tuple)
    assert row[0] is True


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_batch_falls_back_to_builtin_namedtuple(session: Session, lwt_table: str):
    row = await (await session.batch(conditional_batch(lwt_table))).first_row()

    assert row is not None
    # `[applied]` is not a usable field name, so it is cleaned down to `applied`.
    assert row.applied is True


# ---------------------------------------------------------------------------
# Rejected factories
# ---------------------------------------------------------------------------


def test_class_row_factory_rejects_a_non_callable_target():
    with pytest.raises(RowFactoryError) as exc_info:
        ClassRowFactory(42)  # pyright: ignore[reportArgumentType]

    assert "invalid ClassRowFactory target" in str(exc_info.value)


def test_class_row_factory_keeps_its_target():
    assert ClassRowFactory(User).cls is User


class UncallablePrepare:
    prepare = 42

    def __call__(self, values: tuple[Any, ...]) -> Any:
        return values


@pytest.mark.parametrize(
    "factory",
    [object(), 42, "NamedTupleRowFactory", UncallablePrepare()],
    ids=["object", "int", "str", "uncallable-prepare"],
)
def test_statement_rejects_an_unusable_factory(factory: Any):
    with pytest.raises(RowFactoryError) as exc_info:
        Statement("SELECT 1").with_row_factory(factory)

    assert "invalid row factory" in str(exc_info.value)


class FailingPrepareLookup:
    @property
    def prepare(self) -> Any:
        raise ValueError("lookup failed")


def test_statement_keeps_the_prepare_lookup_error():
    with pytest.raises(RowFactoryError) as exc_info:
        Statement("SELECT 1").with_row_factory(FailingPrepareLookup())  # pyright: ignore[reportArgumentType]

    assert isinstance(exc_info.value.__cause__, ValueError)


def test_execution_profile_rejects_an_unusable_factory():
    with pytest.raises(RowFactoryError) as exc_info:
        ExecutionProfile(row_factory=object())  # pyright: ignore[reportArgumentType]

    assert "invalid row factory" in str(exc_info.value)


def test_execution_profile_hands_back_the_factory_it_was_given():
    factory = NamedTupleRowFactory()

    assert ExecutionProfile(row_factory=factory).row_factory is factory
    assert ExecutionProfile().row_factory is None
