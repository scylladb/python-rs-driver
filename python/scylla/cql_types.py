"""
CQL column types and the Python values they map to.

Every public name starts with ``Cql``; the rest of the name tells what it is:

- ``Cql<Name>Type``, such as ``CqlIntType``, ``CqlListType`` or
  ``CqlUserDefinedType``, describes one CQL type, as reported by the schema
  or by a prepared statement. ``<Name>`` is the CQL type's own name.
- ``CqlColumnType``, ``CqlNativeType`` and ``CqlCollectionType`` are base
  classes that group those descriptors, for ``isinstance`` checks.
- ``Cql*Value`` (``CqlValue``, ``CqlScalarValue``, ``CqlCollectionValue``)
  is a type alias for the Python objects the driver produces when reading a
  column. Writing accepts the same objects, except ``CqlEmpty``, and also
  the standard types listed below.
- ``CqlEmpty`` is the value read from a column that holds CQL's empty value.
  It is read-only: the driver does not accept it when writing.
- ``CqlDate``, ``CqlTime``, ``CqlTimestamp`` and ``CqlDuration`` are the values
  of CQL ``date``, ``time``, ``timestamp`` and ``duration`` columns.

Writing also accepts ``datetime.date`` for ``date``, ``datetime.time`` for
``time``, a ``datetime.datetime`` with a time zone for ``timestamp`` and
``dateutil.relativedelta.relativedelta`` for ``duration``.
"""

import ipaddress
from decimal import Decimal
from typing import TYPE_CHECKING, TypeAlias
from uuid import UUID

from ._rust.cluster.metadata import (  # pyright: ignore[reportMissingModuleSource]
    CqlAsciiType,
    CqlBigIntType,
    CqlBlobType,
    CqlBooleanType,
    CqlCollectionType,
    CqlColumnType,
    CqlCounterType,
    CqlDateType,
    CqlDecimalType,
    CqlDoubleType,
    CqlDurationType,
    CqlFloatType,
    CqlInetType,
    CqlIntType,
    CqlListType,
    CqlMapType,
    CqlNativeType,
    CqlSetType,
    CqlSmallIntType,
    CqlTextType,
    CqlTimestampType,
    CqlTimeType,
    CqlTimeuuidType,
    CqlTinyIntType,
    CqlTupleType,
    CqlUserDefinedType,
    CqlUuidType,
    CqlVarintType,
    CqlVectorType,
)
from ._rust.cql_types import (  # pyright: ignore[reportMissingModuleSource]
    CqlDate,
    CqlDuration,
    CqlEmpty,
    CqlTime,
    CqlTimestamp,
)

if TYPE_CHECKING:
    CqlScalarValue: TypeAlias = (
        # CQL:
        # - Counter
        # - TinyInt
        # - SmallInt
        # - Int
        # - BigInt
        # - Varint
        int
        # CQL:
        # - Float
        # - Double
        | float
        # CQL:
        # - Ascii
        # - Text
        | str
        # CQL:
        # - Boolean
        | bool
        # CQL:
        # - Blob
        | bytes
        # CQL:
        # - Decimal
        | Decimal
        # CQL:
        # - Uuid
        # - Timeuuid
        | UUID
        # CQL:
        # - Inet (IPv4)
        | ipaddress.IPv4Address
        # CQL:
        # - Inet (IPv6)
        | ipaddress.IPv6Address
        # CQL:
        # - Date
        | CqlDate
        # CQL:
        # - Timestamp
        | CqlTimestamp
        # CQL:
        # - Time
        | CqlTime
        # CQL:
        # - Duration
        | CqlDuration
        # CQL:
        # - Empty (read only; the driver does not accept CqlEmpty when writing)
        | CqlEmpty
        # CQL:
        # - null
        | None
    )
    """Python value of a CQL column of a scalar (non-collection) type."""

    # CQL list and vector -> list, set -> set, tuple -> tuple,
    # map and user defined type -> dict.
    CqlCollectionValue: TypeAlias = (
        list["CqlValue"] | set["CqlValue"] | tuple["CqlValue", ...] | dict["CqlValue", "CqlValue"]
    )
    """Python value of a CQL collection, tuple, vector or user defined type column."""

    CqlValue: TypeAlias = CqlScalarValue | CqlCollectionValue
    """Python value of any CQL column, as read by the driver."""
else:
    # Runtime stand-ins without recursion, so get_type_hints() can resolve them.
    CqlScalarValue = (
        int
        | float
        | str
        | bool
        | bytes
        | Decimal
        | UUID
        | ipaddress.IPv4Address
        | ipaddress.IPv6Address
        | CqlDate
        | CqlTimestamp
        | CqlTime
        | CqlDuration
        | CqlEmpty
        | None
    )
    CqlCollectionValue = list | set | tuple | dict
    CqlValue = CqlScalarValue | CqlCollectionValue

__all__ = [
    "CqlAsciiType",
    "CqlBigIntType",
    "CqlBlobType",
    "CqlBooleanType",
    "CqlCollectionType",
    "CqlCollectionValue",
    "CqlColumnType",
    "CqlCounterType",
    "CqlDate",
    "CqlDateType",
    "CqlDecimalType",
    "CqlDoubleType",
    "CqlDuration",
    "CqlDurationType",
    "CqlEmpty",
    "CqlFloatType",
    "CqlInetType",
    "CqlIntType",
    "CqlListType",
    "CqlMapType",
    "CqlNativeType",
    "CqlScalarValue",
    "CqlSetType",
    "CqlSmallIntType",
    "CqlTextType",
    "CqlTime",
    "CqlTimeType",
    "CqlTimestamp",
    "CqlTimestampType",
    "CqlTimeuuidType",
    "CqlTinyIntType",
    "CqlTupleType",
    "CqlUserDefinedType",
    "CqlUuidType",
    "CqlValue",
    "CqlVarintType",
    "CqlVectorType",
]
