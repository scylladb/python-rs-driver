import types
from collections.abc import AsyncGenerator, Awaitable, Callable
from typing import Any, Literal

import pytest
import pytest_asyncio
from helpers.ddl import ddl
from helpers.session import session_builder
from scylla.adapters import Adapters
from scylla.cql_types import (
    CqlColumnType,
    CqlEmpty,
    CqlFloatType,
    CqlInetType,
    CqlIntType,
    CqlListType,
    CqlMapType,
    CqlSetType,
    CqlTextType,
    CqlTupleType,
    CqlUserDefinedType,
    CqlVectorType,
)
from scylla.errors import PyConversionFailedError
from scylla.session import Session

KEYSPACE = "register_reader_ks"


class Tagged:
    def __init__(self, tag: str, value: Any):
        self.tag = tag
        self.value = value

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Tagged) and (self.tag, self.value) == (other.tag, other.value)

    def __repr__(self) -> str:
        return f"Tagged({self.tag!r}, {self.value!r})"


def tag(name: str) -> Callable[[Any], Tagged]:
    return lambda value: Tagged(name, value)


async def connect_with(adapters: Adapters) -> Session:
    session = await session_builder().adapters(adapters).connect()
    await session.use_keyspace(KEYSPACE)
    return session


@pytest_asyncio.fixture(scope="module")
async def setup() -> AsyncGenerator[Session, None]:
    setup = await session_builder().connect()
    await ddl(
        setup,
        f"""
            CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
            WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}};
        """,
    )
    await ddl(setup, f"CREATE TYPE IF NOT EXISTS {KEYSPACE}.box (n int)")
    await setup.use_keyspace(KEYSPACE)
    yield setup
    await ddl(setup, f"DROP KEYSPACE {KEYSPACE}")


TableFactory = Callable[[str, str], Awaitable[str]]


@pytest_asyncio.fixture
async def table_factory(setup: Session) -> AsyncGenerator[TableFactory, None]:
    created_tables: list[str] = []

    async def create_table(schema: str, name: str) -> str:
        await ddl(setup, f"CREATE TABLE IF NOT EXISTS {name} ({schema});")
        created_tables.append(name)
        return name

    yield create_table
    for table in created_tables:
        await ddl(setup, f"DROP TABLE IF EXISTS {table};")


async def read_back(session: Session, table: str, column: str) -> Any:
    result = await session.execute(f"SELECT {column} FROM {table} WHERE id = 1")
    row = await result.first_row()
    assert row is not None
    return row[column]


@pytest.mark.parametrize(
    "cql_type",
    [
        CqlIntType,
        CqlListType,
        CqlListType[CqlIntType],
        CqlSetType[CqlTextType],
        CqlMapType[CqlTextType, CqlListType[CqlIntType]],
        CqlTupleType[CqlIntType, CqlTextType],
        CqlVectorType[CqlFloatType],
        CqlListType[CqlVectorType],
    ],
)
def test_register_reader_accepts_a_cql_type(cql_type: Any):
    Adapters().register_reader(cql_type, str)


def test_register_reader_rejects_a_reader_that_is_not_callable():
    with pytest.raises(TypeError):
        Adapters().register_reader(CqlIntType, "not callable")  # pyright: ignore[reportArgumentType]


@pytest.mark.parametrize(
    "cql_type",
    [
        CqlUserDefinedType,
        CqlColumnType,
        int,
        "int",
        list[CqlIntType],
        CqlListType[int],  # pyright: ignore[reportInvalidTypeArguments]
        CqlListType[CqlIntType, CqlIntType],  # pyright: ignore[reportInvalidTypeArguments]
        CqlVectorType[CqlFloatType, Literal[3]],  # pyright: ignore[reportInvalidTypeArguments]
        # Type checkers accept it, because of the type argument defaults.
        CqlMapType[CqlIntType],
        # A native class is not generic: only a hand-made alias gives it arguments, also none.
        types.GenericAlias(CqlIntType, (CqlIntType,)),
        types.GenericAlias(CqlIntType, ()),
        # A hand-made alias whose origin is not a class.
        types.GenericAlias("not a class", (CqlIntType,)),  # pyright: ignore[reportArgumentType]
    ],
)
def test_register_reader_rejects_a_key_that_is_not_a_supported_cql_type(cql_type: Any):
    with pytest.raises(TypeError):
        Adapters().register_reader(cql_type, str)


def test_register_reader_error_names_the_wrong_key():
    with pytest.raises(TypeError, match="invalid CQL type 'int'"):
        Adapters().register_reader("int", str)  # pyright: ignore[reportArgumentType]
    with pytest.raises(TypeError, match="wrong number of type arguments in 'scylla.cql_types.CqlListType"):
        Adapters().register_reader(CqlListType[CqlIntType, CqlIntType], str)  # pyright: ignore[reportInvalidTypeArguments]


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_native_reader_applies_at_every_depth(setup: Session, table_factory: TableFactory):
    adapters = Adapters()
    adapters.register_reader(CqlIntType, tag("int"))
    session = await connect_with(adapters)
    table = await table_factory(
        "id int PRIMARY KEY, n int, items list<int>, by_name map<text, int>, pair tuple<int, text>, boxed frozen<box>",
        "native_depth",
    )
    await setup.execute(
        f"INSERT INTO {table} (id, n, items, by_name, pair, boxed) VALUES (1, 5, [1, 2], {{'a': 3}}, (4, 'x'), {{n: 6}})"
    )

    # The key column `id` is an int too, but it is not selected.
    assert await read_back(session, table, "n") == Tagged("int", 5)
    assert await read_back(session, table, "items") == [Tagged("int", 1), Tagged("int", 2)]
    assert await read_back(session, table, "by_name") == {"a": Tagged("int", 3)}
    assert await read_back(session, table, "pair") == (Tagged("int", 4), "x")
    assert await read_back(session, table, "boxed") == {"n": Tagged("int", 6)}


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_type_arguments_beat_bare_class(setup: Session, table_factory: TableFactory):
    adapters = Adapters()
    adapters.register_reader(CqlListType, tag("any list"))
    adapters.register_reader(CqlListType[CqlIntType], tag("list<int>"))
    session = await connect_with(adapters)
    table = await table_factory("id int PRIMARY KEY, ints list<int>, texts list<text>", "arguments_beat_class")
    await setup.execute(f"INSERT INTO {table} (id, ints, texts) VALUES (1, [1], ['a'])")

    assert await read_back(session, table, "ints") == Tagged("list<int>", [1])
    assert await read_back(session, table, "texts") == Tagged("any list", ["a"])


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_most_specific_nested_key_wins(setup: Session, table_factory: TableFactory):
    adapters = Adapters()
    adapters.register_reader(CqlListType, tag("list"))
    adapters.register_reader(CqlListType[CqlListType], tag("list<list>"))
    adapters.register_reader(CqlListType[CqlListType[CqlIntType]], tag("list<list<int>>"))
    session = await connect_with(adapters)
    table = await table_factory(
        "id int PRIMARY KEY, ints frozen<list<frozen<list<int>>>>, texts frozen<list<frozen<list<text>>>>, flat list<int>",
        "nested_specificity",
    )
    await setup.execute(f"INSERT INTO {table} (id, ints, texts, flat) VALUES (1, [[1]], [['a']], [2])")

    # The inner lists convert first, with the reader for a bare list.
    assert await read_back(session, table, "ints") == Tagged("list<list<int>>", [Tagged("list", [1])])
    assert await read_back(session, table, "texts") == Tagged("list<list>", [Tagged("list", ["a"])])
    assert await read_back(session, table, "flat") == Tagged("list", [2])


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_tuple_elements_decide_from_left_to_right(setup: Session, table_factory: TableFactory):
    adapters = Adapters()
    adapters.register_reader(CqlTupleType[CqlListType, CqlListType[CqlIntType]], tag("right"))
    adapters.register_reader(CqlTupleType[CqlListType[CqlIntType], CqlListType], tag("left"))
    session = await connect_with(adapters)
    table = await table_factory("id int PRIMARY KEY, pair tuple<list<int>, list<int>>", "tuple_specificity")
    await setup.execute(f"INSERT INTO {table} (id, pair) VALUES (1, ([1], [2]))")

    assert await read_back(session, table, "pair") == Tagged("left", ([1], [2]))


@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize(
    ("cql_type", "literal", "expected"),
    [
        ("map<text, frozen<list<int>>>", "{'a': [1]}", {"a": [1]}),
        ("set<int>", "{1, 2}", {1, 2}),
        ("tuple<int, text>", "(1, 'x')", (1, "x")),
        ("vector<float, 2>", "[1.0, 2.0]", [1.0, 2.0]),
    ],
    ids=["map", "set", "tuple", "vector"],
)
async def test_type_object_from_schema_is_a_key(
    setup: Session, table_factory: TableFactory, cql_type: str, literal: str, expected: Any
):
    name = f"schema_key_{cql_type.split('<')[0]}"
    table = await table_factory(f"id int PRIMARY KEY, value {cql_type}", name)
    await setup.execute(f"INSERT INTO {table} (id, value) VALUES (1, {literal})")
    await setup.refresh_metadata()
    keyspace = setup.cluster_state.get_keyspace(KEYSPACE)
    assert keyspace is not None
    schema_table = keyspace.tables.get(table)
    assert schema_table is not None
    column = schema_table.columns.get("value")
    assert column is not None

    adapters = Adapters()
    adapters.register_reader(column.typ, tag("schema"))
    session = await connect_with(adapters)

    assert await read_back(session, table, "value") == Tagged("schema", expected)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_matching_ignores_frozen(setup: Session, table_factory: TableFactory):
    adapters = Adapters()
    adapters.register_reader(CqlListType[CqlListType[CqlIntType]], tag("nested"))
    session = await connect_with(adapters)
    table = await table_factory("id int PRIMARY KEY, items frozen<list<frozen<list<int>>>>", "ignores_frozen")
    await setup.execute(f"INSERT INTO {table} (id, items) VALUES (1, [[1]])")

    assert await read_back(session, table, "items") == Tagged("nested", [[1]])


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_vector_reader_ignores_dimensions(setup: Session, table_factory: TableFactory):
    adapters = Adapters()
    adapters.register_reader(CqlVectorType[CqlFloatType], tuple)
    session = await connect_with(adapters)
    table = await table_factory(
        "id int PRIMARY KEY, two vector<float, 2>, three vector<float, 3>, ints vector<int, 2>", "vector_dims"
    )
    await setup.execute(f"INSERT INTO {table} (id, two, three, ints) VALUES (1, [1.0, 2.0], [1.0, 2.0, 3.0], [1, 2])")

    assert await read_back(session, table, "two") == (1.0, 2.0)
    assert await read_back(session, table, "three") == (1.0, 2.0, 3.0)
    # The element type still has to match.
    assert await read_back(session, table, "ints") == [1, 2]


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_elements_convert_before_their_collection(setup: Session, table_factory: TableFactory):
    adapters = Adapters()
    adapters.register_reader(CqlIntType, str)
    adapters.register_reader(CqlListType[CqlIntType], "-".join)
    session = await connect_with(adapters)
    table = await table_factory("id int PRIMARY KEY, items list<int>", "children_first")
    await setup.execute(f"INSERT INTO {table} (id, items) VALUES (1, [1, 2])")

    assert await read_back(session, table, "items") == "1-2"


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_reader_is_not_called_for_null_or_empty(setup: Session, table_factory: TableFactory):
    adapters = Adapters()
    adapters.register_reader(CqlInetType, tag("inet"))
    session = await connect_with(adapters)
    table = await table_factory("id int PRIMARY KEY, missing inet, empty inet", "null_or_empty")
    await setup.execute(f"INSERT INTO {table} (id, empty) VALUES (1, '')")

    assert await read_back(session, table, "missing") is None
    assert isinstance(await read_back(session, table, "empty"), CqlEmpty)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_null_collection_reaches_the_reader_as_empty(setup: Session, table_factory: TableFactory):
    adapters = Adapters()
    adapters.register_reader(CqlListType, tag("list"))
    adapters.register_reader(CqlSetType, tag("set"))
    adapters.register_reader(CqlMapType, tag("map"))
    session = await connect_with(adapters)
    table = await table_factory(
        "id int PRIMARY KEY, items list<int>, tags set<text>, by_name map<text, int>", "null_collection"
    )
    await setup.execute(f"INSERT INTO {table} (id) VALUES (1)")

    assert await read_back(session, table, "items") == Tagged("list", [])
    assert await read_back(session, table, "tags") == Tagged("set", set())
    assert await read_back(session, table, "by_name") == Tagged("map", {})


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_new_reader_replaces_the_old_one(setup: Session, table_factory: TableFactory):
    adapters = Adapters()
    adapters.register_reader(CqlListType[CqlIntType], tag("old"))
    adapters.register_reader(CqlListType[CqlIntType], tag("new"))
    session = await connect_with(adapters)
    table = await table_factory("id int PRIMARY KEY, items list<int>", "replaced")
    await setup.execute(f"INSERT INTO {table} (id, items) VALUES (1, [1])")

    assert await read_back(session, table, "items") == Tagged("new", [1])


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_reader_error_is_reported(setup: Session, table_factory: TableFactory):
    def fail(value: Any) -> Any:
        raise ValueError(value)

    adapters = Adapters()
    adapters.register_reader(CqlIntType, fail)
    session = await connect_with(adapters)
    table = await table_factory("id int PRIMARY KEY, n int", "reader_error")
    await setup.execute(f"INSERT INTO {table} (id, n) VALUES (1, 1)")

    with pytest.raises(PyConversionFailedError) as exc_info:
        await read_back(session, table, "n")

    assert isinstance(exc_info.value.__cause__, ValueError)
