import dataclasses
from collections.abc import AsyncGenerator, Awaitable, Callable
from typing import Any

import pytest
import pytest_asyncio
from helpers.ddl import ddl
from helpers.session import CONTACT_POINTS, session_builder
from scylla.errors import RowFactoryError, RowIterationError
from scylla.results import (
    ClassRowFactory,
    ColumnSpec,
    DictRowFactory,
    NamedTupleRowFactory,
    RequestResult,
    RowBuilder,
    RowFactory,
    TupleRowFactory,
)
from scylla.session import ExecutionProfile, Session, SessionBuilder
from scylla.statement import Batch, Statement

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


# ---------------------------------------------------------------------------
# Namedtuple field names
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize(
    "aliases,fields",
    [
        (('"a b"', '"a_b"'), ("a_b", "a_b_")),
        (('"class"', "name"), ("field_0_", "name")),
        (('"1x"', '"!!"'), ("field_0_", "field_1_")),
        (('"_x"', '"y-z"'), ("x", "y_z")),
    ],
    ids=["duplicate-after-cleaning", "keyword", "digit-and-empty", "leading-underscore"],
)
async def test_namedtuple_field_names(session: Session, users: str, aliases: tuple[str, str], fields: tuple[str, str]):
    query = f"SELECT id AS {aliases[0]}, name AS {aliases[1]} FROM {users}"

    row = await (await session.execute(query)).first_row()

    assert row is not None
    assert row._fields == fields
    assert row == (1, "alice")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_namedtuple_renaming_logs_a_warning(session: Session, users: str, caplog: pytest.LogCaptureFixture):
    # Aliases not used by other tests, so the namedtuple class is not cached yet.
    await (await session.execute(f'SELECT id AS "lambda", name FROM {users}')).first_row()

    assert any("cannot all be used as namedtuple fields" in record.getMessage() for record in caplog.records)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_namedtuple_cleaning_alone_does_not_warn(session: Session, users: str, caplog: pytest.LogCaptureFixture):
    await (await session.execute(f'SELECT id AS "[id]", name AS "(name)" FROM {users}')).first_row()

    assert not any("namedtuple" in record.getMessage() for record in caplog.records)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_namedtuple_class_is_reused_for_the_same_columns(session: Session, users: str):
    query = f"SELECT id, name FROM {users}"

    first = await (await session.execute(query)).first_row()
    second = await (await session.execute(query)).first_row()

    assert type(first) is type(second)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_class_row_factory_passes_column_names_as_keywords(session: Session, users: str):
    # `cls(**columns)`, so the column order in the query does not matter.
    result = await session.execute(f"SELECT name, id FROM {users}", factory=ClassRowFactory(User))

    assert await result.first_row() == User(id=1, name="alice")


# ---------------------------------------------------------------------------
# `prepare` on the built-in factories
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "factory",
    [NamedTupleRowFactory(), DictRowFactory(), TupleRowFactory(), ClassRowFactory(User)],
    ids=["namedtuple", "dict", "tuple", "class"],
)
def test_builtin_factories_are_row_factories(factory: Any):
    assert isinstance(factory, RowFactory)


def test_base_prepare_is_not_implemented():
    with pytest.raises(NotImplementedError):
        RowFactory().prepare(())


def test_subclass_init_can_take_arguments():
    class Prefixed(RowFactory):
        def __init__(self, prefix: str) -> None:
            self.prefix = prefix

        def prepare(self, columns: tuple[ColumnSpec, ...]) -> RowBuilder:
            return lambda values: [f"{self.prefix}{value}" for value in values]

    factory = Prefixed("x")

    statement = Statement("SELECT 1")
    statement.row_factory = factory

    assert factory.prefix == "x"
    assert statement.row_factory is factory


@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize(
    "factory",
    [NamedTupleRowFactory(), DictRowFactory(), TupleRowFactory(), ClassRowFactory(User)],
    ids=["namedtuple", "dict", "tuple", "class"],
)
async def test_builtin_prepare_builds_what_the_driver_builds(session: Session, users: str, factory: Any):
    query = f"SELECT id, name FROM {users}"
    result = await session.execute(query, factory=factory)
    values = await (await session.execute(query, factory=TupleRowFactory())).first_row()

    row = factory.prepare(result.columns)(values)
    expected = await result.first_row()

    assert row == expected
    assert type(row) is type(expected)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_custom_factory_can_delegate_to_a_builtin(session: Session, users: str):
    class UppercaseKeys(RowFactory):
        def prepare(self, columns: tuple[ColumnSpec, ...]) -> RowBuilder:
            as_dict = DictRowFactory().prepare(columns)
            return lambda values: {name.upper(): value for name, value in as_dict(values).items()}

    result = await session.execute(f"SELECT id, name FROM {users}", factory=UppercaseKeys())

    assert await result.first_row() == {"ID": 1, "NAME": "alice"}


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_builtin_builder_rejects_a_wrong_number_of_values(session: Session, users: str):
    result = await session.execute(f"SELECT id, name FROM {users}")
    build = NamedTupleRowFactory().prepare(result.columns)

    with pytest.raises(ValueError, match="expected 2 column values, got 1"):
        build((1,))


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


async def read_all_rows(result: RequestResult, mode: str) -> list[Any]:
    if mode == "all":
        return await result.all()

    if mode == "async-for":
        return [row async for row in result]

    rows = list(result.first_page)
    page = await result.first_page.fetch_next_page()
    while page is not None:
        rows.extend(page)
        page = await page.fetch_next_page()
    return rows


async def count_pages(session: Session, statement: Statement) -> int:
    pages = 1
    page = await (await session.execute(statement)).first_page.fetch_next_page()
    while page is not None:
        pages += 1
        page = await page.fetch_next_page()
    return pages


@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize("mode", ["all", "async-for", "fetch-next-page"])
async def test_prepare_runs_once_per_page(session: Session, paged_table: str, mode: str):
    factory = JoinedRowFactory()
    statement = Statement(f"SELECT id, ck FROM {paged_table}")
    statement.page_size = 2
    # The server can end with an empty page, so count the pages it sends.
    pages = await count_pages(session, statement)

    rows = await read_all_rows(await session.execute(statement, factory=factory), mode)

    assert len(rows) == 4
    assert factory.prepared == pages


class FailsOnSecondPage(JoinedRowFactory):
    def prepare(self, columns: tuple[ColumnSpec, ...]) -> RowBuilder:
        if self.prepared:
            raise ValueError("prepare failed")
        return super().prepare(columns)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_all_wraps_prepare_errors_of_later_pages(session: Session, paged_table: str):
    statement = Statement(f"SELECT id, ck FROM {paged_table}")
    statement.page_size = 2
    result = await session.execute(statement, factory=FailsOnSecondPage())

    with pytest.raises(RowIterationError) as exc_info:
        await result.all()

    assert isinstance(exc_info.value.__cause__, ValueError)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_async_iteration_ends_after_error_on_later_page(session: Session, paged_table: str):
    statement = Statement(f"SELECT id, ck FROM {paged_table}")
    statement.page_size = 2
    rows = aiter(await session.execute(statement, factory=FailsOnSecondPage()))

    assert await anext(rows) == "id=0|ck=0"
    assert await anext(rows) == "id=0|ck=1"
    with pytest.raises(RowIterationError):
        await anext(rows)
    with pytest.raises(StopAsyncIteration):
        await anext(rows)


class FailsOnSecondRow(RowFactory):
    def prepare(self, columns: tuple[ColumnSpec, ...]) -> RowBuilder:
        def build(values: tuple[Any, ...]) -> Any:
            if values[1] == 1:
                raise ValueError("build failed")
            return values

        return build


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_async_iteration_ends_after_error_within_page(session: Session, paged_table: str):
    statement = Statement(f"SELECT id, ck FROM {paged_table}")
    statement.page_size = 10
    rows = aiter(await session.execute(statement, factory=FailsOnSecondRow()))

    assert await anext(rows) == (0, 0)
    with pytest.raises(RowIterationError):
        await anext(rows)
    with pytest.raises(StopAsyncIteration):
        await anext(rows)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_fetch_next_page_follows_columns_added_between_pages(session: Session, paged_table: str):
    statement = Statement(f"SELECT * FROM {paged_table}")
    statement.page_size = 2
    result = await session.execute(statement, factory=DictRowFactory())
    assert list(result.first_page) == [{"id": 0, "ck": 0, "b": 0}, {"id": 0, "ck": 1, "b": 1}]

    # Regular columns are ordered by name, so `a` lands before `b`.
    await ddl(session, f"ALTER TABLE {paged_table} ADD a int")
    next_page = await result.first_page.fetch_next_page()

    assert next_page is not None
    assert list(next_page) == [
        {"id": 0, "ck": 2, "a": None, "b": 2},
        {"id": 0, "ck": 3, "a": None, "b": 3},
    ]


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_async_iteration_follows_columns_added_between_pages(session: Session, paged_table: str):
    statement = Statement(f"SELECT * FROM {paged_table}")
    statement.page_size = 2
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
    statement = Statement(f"SELECT id, name FROM {users}")
    statement.row_factory = TupleRowFactory()

    result = await session.execute(statement, factory=DictRowFactory())

    assert await result.first_row() == {"id": 1, "name": "alice"}


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_statement_factory_wins_over_its_execution_profile(session: Session, users: str):
    statement = Statement(f"SELECT id, name FROM {users}")
    statement.execution_profile = ExecutionProfile(row_factory=TupleRowFactory())
    statement.row_factory = DictRowFactory()

    result = await session.execute(statement)

    assert await result.first_row() == {"id": 1, "name": "alice"}


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_statement_execution_profile_wins_over_session_default(dict_session: Session, users: str):
    statement = Statement(f"SELECT id, name FROM {users}")
    statement.execution_profile = ExecutionProfile(row_factory=TupleRowFactory())

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
    statement = Statement(f"SELECT id, name FROM {users}")
    statement.execution_profile = ExecutionProfile(row_factory=DictRowFactory())
    statement.row_factory = TupleRowFactory()
    statement.row_factory = None

    result = await session.execute(statement)

    assert await result.first_row() == {"id": 1, "name": "alice"}


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_statement_keeps_the_factory_it_was_prepared_with(session: Session, users: str):
    statement = Statement(f"SELECT id, name FROM {users}")
    statement.row_factory = DictRowFactory()
    prepared = await session.prepare(statement)

    result = await session.execute(prepared)

    assert await result.first_row() == {"id": 1, "name": "alice"}


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_statement_factory_can_be_replaced(session: Session, users: str):
    statement = Statement(f"SELECT id, name FROM {users}")
    statement.row_factory = DictRowFactory()
    prepared = await session.prepare(statement)
    prepared.row_factory = TupleRowFactory()

    result = await session.execute(prepared)

    assert await result.first_row() == (1, "alice")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_statement_execution_profile_wins_over_session_default(dict_session: Session, users: str):
    statement = Statement(f"SELECT id, name FROM {users}")
    statement.execution_profile = ExecutionProfile(row_factory=TupleRowFactory())
    prepared = await dict_session.prepare(statement)

    result = await dict_session.execute(prepared)

    assert await result.first_row() == (1, "alice")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_statement_without_row_factory_falls_back_to_session_default(dict_session: Session, users: str):
    statement = Statement(f"SELECT id, name FROM {users}")
    statement.row_factory = TupleRowFactory()
    prepared = await dict_session.prepare(statement)
    prepared.row_factory = None

    result = await dict_session.execute(prepared)

    assert await result.first_row() == {"id": 1, "name": "alice"}


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_row_factory_getters(session: Session, users: str):
    factory = TupleRowFactory()
    statement = Statement(f"SELECT id, name FROM {users}")
    statement.row_factory = factory
    prepared = await session.prepare(statement)
    batch = Batch()
    batch.row_factory = factory

    assert statement.row_factory is factory
    assert prepared.row_factory is factory
    assert batch.row_factory is factory

    statement.row_factory = None
    prepared.row_factory = None
    batch.row_factory = None

    assert statement.row_factory is None
    assert prepared.row_factory is None
    assert batch.row_factory is None


# ---------------------------------------------------------------------------
# The same chain, for batches
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_batch_factory_argument_wins_over_batch(session: Session, lwt_table: str):
    batch = conditional_batch(lwt_table)
    batch.row_factory = TupleRowFactory()

    row = await (await session.batch(batch, factory=DictRowFactory())).first_row()

    assert row is not None
    assert row["[applied]"] is True


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_batch_factory_wins_over_its_execution_profile(session: Session, lwt_table: str):
    batch = conditional_batch(lwt_table)
    batch.execution_profile = ExecutionProfile(row_factory=TupleRowFactory())
    batch.row_factory = DictRowFactory()

    row = await (await session.batch(batch)).first_row()

    assert row is not None
    assert row["[applied]"] is True


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_batch_without_row_factory_falls_back_to_execution_profile(session: Session, lwt_table: str):
    batch = conditional_batch(lwt_table)
    batch.execution_profile = ExecutionProfile(row_factory=DictRowFactory())
    batch.row_factory = TupleRowFactory()
    batch.row_factory = None

    row = await (await session.batch(batch)).first_row()

    assert row is not None
    assert row["[applied]"] is True


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_batch_execution_profile_wins_over_session_default(dict_session: Session, lwt_table: str):
    batch = conditional_batch(lwt_table)
    batch.execution_profile = ExecutionProfile(row_factory=TupleRowFactory())

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


class PrepareWithoutBase:
    """Has `prepare`, but is not a `RowFactory`, so it is not a factory."""

    def prepare(self, columns: tuple[ColumnSpec, ...]) -> RowBuilder:
        return tuple


@pytest.mark.parametrize(
    "factory",
    [object(), 42, "NamedTupleRowFactory", PrepareWithoutBase()],
    ids=["object", "int", "str", "prepare-without-base"],
)
def test_statement_rejects_an_unusable_factory(factory: Any):
    statement = Statement("SELECT 1")

    with pytest.raises(RowFactoryError) as exc_info:
        statement.row_factory = factory

    assert "invalid row factory" in str(exc_info.value)


class CallableWithPrepare:
    """A bare builder that happens to have a `prepare` method of its own."""

    def prepare(self) -> None: ...

    def __call__(self, values: tuple[Any, ...]) -> Any:
        return values


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_callable_with_prepare_is_used_as_a_builder(session: Session, users: str):
    result = await session.execute(f"SELECT id, name FROM {users}", factory=CallableWithPrepare())

    assert await result.first_row() == (1, "alice")


def test_execution_profile_rejects_an_unusable_factory():
    with pytest.raises(RowFactoryError) as exc_info:
        ExecutionProfile(row_factory=object())  # pyright: ignore[reportArgumentType]

    assert "invalid row factory" in str(exc_info.value)


def test_execution_profile_hands_back_the_factory_it_was_given():
    factory = NamedTupleRowFactory()

    assert ExecutionProfile(row_factory=factory).row_factory is factory
    assert ExecutionProfile().row_factory is None
