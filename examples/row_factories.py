"""
row_factories.py

Every way to control how rows are handed back to Python.

  1) Defaults and built-ins
  2) Bare callables - a builder with no per-request setup
  3) Custom factories - `prepare(columns)` returns the builder
  4) Factories that use the column *types*, not just the names
  5) Builders that keep state across rows
"""

import asyncio
import dataclasses
import json
import os
from collections import Counter
from collections.abc import Callable
from typing import Any, TypeAlias

from scylla.cluster.metadata import ColumnSpec, CqlBlob, CqlColumnType, CqlText
from scylla.results import ClassRowFactory, DictRowFactory, RowBuilder, TupleRowFactory
from scylla.session import Session
from scylla.session_builder import SessionBuilder


# ----------------------------
# DB setup helpers
# ----------------------------
async def setup_schema(session: Session) -> None:
    await session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS examples_ks
        WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};
        """
    )
    await session.use_keyspace("examples_ks")

    await session.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id int PRIMARY KEY,
            name text,
            email text,
            country text,
            payload blob
        );
        """
    )

    for i in range(8):
        await session.execute(
            "INSERT INTO users (id, name, email, country, payload) VALUES (?, ?, ?, ?, ?)",
            (i, f"user{i}", f"user{i}@example.com", "PL" if i % 2 else "DE", json.dumps({"n": i}).encode()),
        )


@dataclasses.dataclass
class User:
    id: int
    name: str
    country: str


# ----------------------------
# 1) Defaults and built-ins
# ----------------------------
async def example_builtins(session: Session) -> None:
    print("\n=== 1) Defaults and built-ins ===")

    query = "SELECT id, name, country FROM users"

    # No factory: rows are named tuples. Attribute access, indexing, unpacking
    # and comparison against a plain tuple all work.
    row = await (await session.execute(query)).first_row()
    assert row is not None
    print(f"default            -> {row}")
    print(f"  row.name={row.name!r}  row[1]={row[1]!r}  tuple(row)={tuple(row)}")

    rows = await (await session.execute(query, factory=DictRowFactory())).all()
    print(f"DictRowFactory()   -> {rows[0]}")

    rows = await (await session.execute(query, factory=TupleRowFactory())).all()
    print(f"TupleRowFactory()  -> {rows[0]}")

    # ClassRowFactory calls the target with the column names as keywords, so it
    # builds dataclasses, attrs classes, pydantic models or any other callable.
    rows = await (await session.execute(query, factory=ClassRowFactory(User))).all()
    print(f"ClassRowFactory()  -> {rows[0]}")


# ----------------------------
# 2) Bare callables
# ----------------------------
async def example_callables(session: Session) -> None:
    print("\n=== 2) Bare callables ===")

    # A callable is used as the row builder directly - no `prepare` step. It is
    # handed one tuple of column values per row, in column order.

    count = await (await session.execute("SELECT count(*) FROM users", factory=lambda values: values[0])).first_row()
    print(f"lambda values: values[0]      -> {count}")

    ids = await (await session.execute("SELECT id FROM users", factory=lambda values: values[0])).all()
    print(f"scalar column                 -> {sorted(ids)}")

    # Builtins work too, since they are already callables of the right shape.
    rows = await (await session.execute("SELECT id, name FROM users", factory=list)).all()
    print(f"factory=list                  -> {rows[0]}")


# ----------------------------
# 3) Custom factories: prepare() -> builder
# ----------------------------
class StrictClassRowFactory:
    """
    Like the built-in `ClassRowFactory`, but checks the query against the target
    up front.

    This is what `prepare` is for: the check depends only on the columns, so it
    runs once per request. The built-in does not do it, and a query missing a
    field only fails when the first row is built.
    """

    def __init__(self, cls: type[Any]) -> None:
        self.cls = cls

    def prepare(self, columns: tuple[ColumnSpec, ...]) -> RowBuilder:
        names = [spec.name for spec in columns]
        fields = {field.name for field in dataclasses.fields(self.cls)}

        if missing := fields - set(names):
            raise ValueError(f"query does not select {sorted(missing)}, required by {self.cls.__name__}")

        cls = self.cls
        return lambda values: cls(**dict(zip(names, values)))


class SelectedColumnsRowFactory:
    """
    Keep a subset of the columns, optionally under different names.

    `prepare` resolves the wanted columns to positions once; the builder then
    indexes into the values tuple and never looks at a column name again.
    """

    def __init__(self, **wanted: str) -> None:
        self.wanted = wanted

    def prepare(self, columns: tuple[ColumnSpec, ...]) -> RowBuilder:
        positions = {spec.name: index for index, spec in enumerate(columns)}
        picked = [(alias, positions[source]) for alias, source in self.wanted.items()]

        return lambda values: {alias: values[index] for alias, index in picked}


async def example_prepare(session: Session) -> None:
    print("\n=== 3) Custom factories ===")

    query = "SELECT id, name, country FROM users"

    factory = SelectedColumnsRowFactory(who="name", where="country")
    rows = await (await session.execute(query, factory=factory)).all()
    print(f"SelectedColumnsRowFactory -> {rows[0]}")

    # A query missing a required field fails at execute, not on the first row.
    try:
        await session.execute("SELECT id, name FROM users", factory=StrictClassRowFactory(User))
    except Exception as err:  # noqa: BLE001
        print(f"missing column            -> {type(err).__name__}: {err.__cause__ or err}")


# ----------------------------
# 4) Factories that use the column types
# ----------------------------
Decoder: TypeAlias = Callable[[Any], Any]


def _decoder(cql_type: CqlColumnType) -> Decoder:
    """Pick a per-column converter from the CQL type, once per request."""
    if isinstance(cql_type, CqlBlob):
        return lambda value: json.loads(value) if value else None
    if isinstance(cql_type, CqlText):
        return lambda value: value.upper() if value else value
    return lambda value: value


class DecodingRowFactory:
    """
    Convert values based on their CQL type rather than their name.

    Column specs carry the full type, so the converters are chosen once and the
    builder just applies them positionally.
    """

    def prepare(self, columns: tuple[ColumnSpec, ...]) -> RowBuilder:
        names = [spec.name for spec in columns]
        decoders = [_decoder(spec.cql_type) for spec in columns]

        return lambda values: {name: decode(value) for name, decode, value in zip(names, decoders, values)}


async def example_typed(session: Session) -> None:
    print("\n=== 4) Type-driven conversion ===")

    result = await session.execute("SELECT id, name, payload FROM users", factory=DecodingRowFactory())
    print(f"DecodingRowFactory() -> {await result.first_row()}")


# ----------------------------
# 5) Builders that keep state
# ----------------------------
class CountingRowFactory:
    """
    The builder is an ordinary callable, so it can close over mutable state and
    observe the whole result set as it is consumed.
    """

    def __init__(self) -> None:
        self.seen = Counter[str]()

    def prepare(self, columns: tuple[ColumnSpec, ...]) -> RowBuilder:
        country = next(index for index, spec in enumerate(columns) if spec.name == "country")
        seen = self.seen

        def build(values: tuple[Any, ...]) -> Any:
            seen[values[country]] += 1
            return values

        return build


async def example_stateful(session: Session) -> None:
    print("\n=== 5) Stateful builders ===")

    factory = CountingRowFactory()
    rows = await (await session.execute("SELECT id, country FROM users", factory=factory)).all()
    print(f"CountingRowFactory() -> {len(rows)} rows, tally={dict(factory.seen)}")


async def main() -> None:
    uri = os.getenv("SCYLLA_URI", "127.0.0.2:9042")
    host, port_str = uri.split(":")

    print(f"Connecting to {host}:{port_str} ...")
    session = await SessionBuilder().contact_points((host, int(port_str))).connect()

    await setup_schema(session)

    await example_builtins(session)
    await example_callables(session)
    await example_prepare(session)
    await example_typed(session)
    await example_stateful(session)

    await session.execute("DROP TABLE IF EXISTS examples_ks.users")
    print("\nOk.")


if __name__ == "__main__":
    asyncio.run(main())
