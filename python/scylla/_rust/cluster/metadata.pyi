from collections.abc import Mapping
from enum import IntEnum
from typing import Generic, TypeAlias

from typing_extensions import TypeVar, TypeVarTuple

class StrategyKind(IntEnum):
    Simple = ...
    NetworkTopology = ...
    Local = ...
    Other = ...

class Strategy:
    @property
    def kind(self) -> StrategyKind:
        """
        Access the kind of this strategy.
        """
    @property
    def replication_factor(self) -> Mapping[str, int] | int | None:
        """
        Access the replication factor for this strategy.

        For Simple and Local strategies, this is a positive integer.
        For Network Topology strategy, this is a read-only dictionary mapping datacenter names to replication factors.
        None means Driver cannot determine the replication factor based on Strategy.
        """
    @property
    def other_name(self) -> str | None:
        """
        Access the name of the strategy, if it is of the Other kind.
        """
    @property
    def other_data(self) -> Mapping[str, str] | None:
        """
        Access the data of the strategy, if it is of the Other kind.
        """

class CqlColumnType:
    """Base class for all CQL column types (native, collections, vectors, tuples, UDTs)."""

class CqlNativeType(CqlColumnType):
    """Base class for native Cassandra scalar types."""

class CqlAsciiType(CqlNativeType):
    """CQL ``ascii``: US-ASCII string."""

class CqlBigIntType(CqlNativeType):
    """CQL ``bigint``: 64-bit signed integer."""

class CqlBlobType(CqlNativeType):
    """CQL ``blob``: arbitrary bytes."""

class CqlBooleanType(CqlNativeType):
    """CQL ``boolean``: true or false."""

class CqlCounterType(CqlNativeType):
    """CQL ``counter``: 64-bit signed integer that is only incremented or decremented."""

class CqlDateType(CqlNativeType):
    """CQL ``date``: calendar date without a time of day."""

class CqlDecimalType(CqlNativeType):
    """CQL ``decimal``: arbitrary-precision decimal number."""

class CqlDoubleType(CqlNativeType):
    """CQL ``double``: 64-bit floating point number."""

class CqlDurationType(CqlNativeType):
    """CQL ``duration``: length of time in months, days and nanoseconds."""

class CqlFloatType(CqlNativeType):
    """CQL ``float``: 32-bit floating point number."""

class CqlIntType(CqlNativeType):
    """CQL ``int``: 32-bit signed integer."""

class CqlInetType(CqlNativeType):
    """CQL ``inet``: IPv4 or IPv6 address."""

class CqlSmallIntType(CqlNativeType):
    """CQL ``smallint``: 16-bit signed integer."""

class CqlTextType(CqlNativeType):
    """CQL ``text``: UTF-8 string."""

class CqlTimeType(CqlNativeType):
    """CQL ``time``: time of day with nanosecond precision."""

class CqlTimestampType(CqlNativeType):
    """CQL ``timestamp``: date and time with millisecond precision."""

class CqlTimeuuidType(CqlNativeType):
    """CQL ``timeuuid``: version 1, time-based UUID."""

class CqlTinyIntType(CqlNativeType):
    """CQL ``tinyint``: 8-bit signed integer."""

class CqlUuidType(CqlNativeType):
    """CQL ``uuid``: UUID of any version."""

class CqlVarintType(CqlNativeType):
    """CQL ``varint``: arbitrary-precision integer."""

class CqlCollectionType(CqlColumnType):
    """Base class for CQL collection types (List, Map, Set)."""

    frozen: bool

# The element types, as with list[int]. A type from the schema has CqlType as its type
# arguments, so annotate such a type with CqlType: isinstance then keeps known type arguments.
# The defaults let a class without type arguments mean "all element types", the same as a
# type from the schema.
# One type variable per class, not one shared _E_co. The bound CqlType contains these classes.
# When pyright resolves CqlListType, it resolves the bound of its type variable, and so it also
# resolves CqlSetType. A shared type variable would then still be unresolved, and pyright would
# report CqlSetType as invalid. Which class fails depends on the import order of the user's code.
_ListE_co = TypeVar("_ListE_co", bound=CqlType, default=CqlType, covariant=True)
_SetE_co = TypeVar("_SetE_co", bound=CqlType, default=CqlType, covariant=True)
_MapK_co = TypeVar("_MapK_co", bound=CqlType, default=CqlType, covariant=True)
_MapV_co = TypeVar("_MapV_co", bound=CqlType, default=CqlType, covariant=True)
_VectorE_co = TypeVar("_VectorE_co", bound=CqlType, default=CqlType, covariant=True)
_Ts = TypeVarTuple("_Ts")

class CqlListType(CqlCollectionType, Generic[_ListE_co]):
    """
    list<T> — ordered sequence of elements.

    ``CqlListType[CqlIntType]`` names a list of ints, for example as a key of
    ``Adapters.register_reader``.
    """

    @property
    def column_type(self) -> _ListE_co: ...

class CqlMapType(CqlCollectionType, Generic[_MapK_co, _MapV_co]):
    """
    map<K, V> — key-value pairs.

    ``CqlMapType[CqlTextType, CqlIntType]`` names a map from texts to ints, for
    example as a key of ``Adapters.register_reader``.
    """

    @property
    def key_type(self) -> _MapK_co: ...
    @property
    def value_type(self) -> _MapV_co: ...

class CqlSetType(CqlCollectionType, Generic[_SetE_co]):
    """
    set<T> — unordered set of elements.

    ``CqlSetType[CqlIntType]`` names a set of ints, for example as a key of
    ``Adapters.register_reader``.
    """

    @property
    def column_type(self) -> _SetE_co: ...

class CqlTupleType(CqlColumnType, Generic[*_Ts]):
    """
    tuple<T1, T2, ...> — positional tuple of column types.

    ``CqlTupleType[CqlIntType, CqlTextType]`` names a tuple of an int and a text,
    for example as a key of ``Adapters.register_reader``.
    """

    element_types: list[CqlType]

class CqlVectorType(CqlColumnType, Generic[_VectorE_co]):
    """
    vector<T, N> — fixed-length vector of elements.

    ``CqlVectorType[CqlFloatType]`` names a vector of floats of any length, for
    example as a key of ``Adapters.register_reader``.
    """

    @property
    def typ(self) -> _VectorE_co: ...
    dimensions: int

class CqlUserDefinedType(CqlColumnType):
    """CQL user-defined type (UDT) — custom type with named fields."""

    name: str
    frozen: bool
    keyspace: str
    field_types: list[tuple[str, CqlType]]

# Only for type checkers. Users import it from scylla.cql_types. It is defined here because it
# is the bound of the type variables above, and a bound from another module makes an import cycle.
CqlType: TypeAlias = (
    CqlNativeType
    | CqlListType[CqlType]
    | CqlSetType[CqlType]
    | CqlMapType[CqlType, CqlType]
    | CqlTupleType[*tuple[CqlType, ...]]
    | CqlVectorType[CqlType]
    | CqlUserDefinedType
)
"""A CQL type from the schema or from a prepared statement."""

class ColumnKind(IntEnum):
    Regular = ...
    Static = ...
    Clustering = ...
    PartitionKey = ...

class Column:
    @property
    def typ(self) -> CqlType:
        """
        Access the type of this column.
        """
    @property
    def kind(self) -> ColumnKind:
        """
        Access the kind of this column.
        """

class Table:
    @property
    def columns(self) -> Mapping[str, Column]:
        """
        Access the columns of this table as a read-only dictionary of name to column.
        """
    @property
    def partition_key(self) -> Mapping[str, Column]:
        """
        Access the partition key of this table as a read-only dictionary of name to column.
        """
    @property
    def clustering_key(self) -> Mapping[str, Column]:
        """
        Access the clustering key of this table as a read-only dictionary of name to column.
        """
    @property
    def partitioner(self) -> str | None:
        """
        Access the name of partitioner used by this table or None.
        """

class MaterializedView:
    @property
    def base_table_name(self) -> str:
        """
        Access the name of the base table of this materialized view.
        """
    @property
    def partition_key(self) -> Mapping[str, Column]:
        """
        Access the partition key of this view as a read-only dictionary of name to column.
        """
    @property
    def clustering_key(self) -> Mapping[str, Column]:
        """
        Access the clustering key of this view as a read-only dictionary of name to column.
        """
    @property
    def partitioner(self) -> str | None:
        """
        Access the name of partitioner used by this materialized view or None.
        """

class Keyspace:
    @property
    def strategy(self) -> Strategy:
        """
        Access the strategy used by this keyspace.
        """
    @property
    def tables(self) -> Mapping[str, Table]:
        """
        Access the tables of this keyspace as a read-only dictionary of name to table.
        """
    @property
    def views(self) -> Mapping[str, MaterializedView]:
        """
        Access the materialized views of this keyspace as a read-only dictionary of name to view.
        """

class ColumnSpec:
    """
    Specification of a column.
    """

    @property
    def name(self) -> str: ...
    @property
    def table_name(self) -> str: ...
    @property
    def keyspace_name(self) -> str: ...
    @property
    def cql_type(self) -> CqlType: ...
