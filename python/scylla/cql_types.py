"""
CQL column types and the Python values they map to.

Every public name starts with ``Cql``; the rest of the name tells what it is:

- ``Cql<Name>``, such as ``CqlInt``, ``CqlList`` or ``CqlUserDefinedType``,
  describes one CQL type, as reported by the schema or by a prepared
  statement. The name is the CQL type's own name.
- ``Cql*Type`` (``CqlColumnType``, ``CqlNativeType``, ``CqlCollectionType``)
  is a base class that groups those descriptors, for ``isinstance`` checks.
- ``Cql*Value`` (``CqlValue``, ``CqlScalarValue``, ``CqlCollectionValue``)
  is a type alias for the Python objects the driver produces when reading a
  column. Writing accepts the same objects, except ``CqlEmpty``.
- ``CqlEmpty`` is the value read from a column that holds CQL's empty value.
  It is read-only: the driver does not accept it when writing.
"""

import ipaddress
from datetime import date, datetime, time
from decimal import Decimal
from typing import TYPE_CHECKING, TypeAlias
from uuid import UUID

from ._rust.cluster.metadata import (  # pyright: ignore[reportMissingModuleSource]
    CqlAscii,
    CqlBigInt,
    CqlBlob,
    CqlBoolean,
    CqlCollectionType,
    CqlColumnType,
    CqlCounter,
    CqlDate,
    CqlDecimal,
    CqlDouble,
    CqlDuration,
    CqlFloat,
    CqlInet,
    CqlInt,
    CqlList,
    CqlMap,
    CqlNativeType,
    CqlSet,
    CqlSmallInt,
    CqlText,
    CqlTime,
    CqlTimestamp,
    CqlTimeuuid,
    CqlTinyInt,
    CqlTuple,
    CqlUserDefinedType,
    CqlUuid,
    CqlVarint,
    CqlVector,
)
from ._rust.types import CqlEmpty  # pyright: ignore[reportMissingModuleSource]

if TYPE_CHECKING:
    from dateutil.relativedelta import relativedelta

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
        | date
        # CQL:
        # - Timestamp
        | datetime
        # CQL:
        # - Time
        | time
        # CQL:
        # - Duration
        | relativedelta
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
    """Python value of any CQL column, as read and written by the driver."""
else:
    # Runtime stand-ins without recursion, so get_type_hints() can resolve them.
    try:
        from dateutil.relativedelta import relativedelta
    except ImportError:
        relativedelta = None

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
        | date
        | datetime
        | time
        | relativedelta
        | CqlEmpty
        | None
    )
    CqlCollectionValue = list | set | tuple | dict
    CqlValue = CqlScalarValue | CqlCollectionValue

__all__ = [
    "CqlAscii",
    "CqlBigInt",
    "CqlBlob",
    "CqlBoolean",
    "CqlCollectionType",
    "CqlCollectionValue",
    "CqlColumnType",
    "CqlCounter",
    "CqlDate",
    "CqlDecimal",
    "CqlDouble",
    "CqlDuration",
    "CqlEmpty",
    "CqlFloat",
    "CqlInet",
    "CqlInt",
    "CqlList",
    "CqlMap",
    "CqlNativeType",
    "CqlScalarValue",
    "CqlSet",
    "CqlSmallInt",
    "CqlText",
    "CqlTime",
    "CqlTimestamp",
    "CqlTimeuuid",
    "CqlTinyInt",
    "CqlTuple",
    "CqlUserDefinedType",
    "CqlUuid",
    "CqlValue",
    "CqlVarint",
    "CqlVector",
]
