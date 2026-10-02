import ipaddress
from collections.abc import AsyncIterator, Callable
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, TypeAlias
from uuid import UUID

from dateutil.relativedelta import relativedelta

from .cluster.metadata import ColumnSpec
from .future import DriverFuture

CqlNative: TypeAlias = (
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
    # - Empty
    # - null
    | None
)

CqlCollection: TypeAlias = (
    # CQL:
    # - List
    # - Vector
    list[CqlValue]
    # CQL:
    # - Set
    | set[CqlValue]
    # CQL:
    # - Tuple
    | tuple[CqlValue, ...]
    # CQL:
    # - Map
    # - UserDefinedType (UDT)
    | dict[CqlValue, CqlValue]
)

CqlValue: TypeAlias = CqlNative | CqlCollection

class NamedTupleRowFactory:
    """
    Builds every row as a `collections.namedtuple`. This is the default.

    Field names come from the column names, with characters that cannot appear
    in a Python identifier stripped or replaced. A column whose name is still
    unusable is renamed after its position.

    Recognized by the driver and executed in Rust, so unlike a user-defined
    factory it does not implement the `prepare` protocol.
    """

    def __init__(self) -> None: ...

class DictRowFactory:
    """
    Builds every row as a `dict` mapping column names to values, in column order.
    """

    def __init__(self) -> None: ...

class TupleRowFactory:
    """
    Builds every row as a plain `tuple` of values, in column order.
    """

    def __init__(self) -> None: ...

class ClassRowFactory:
    """
    Builds every row as `cls(**columns)`, passing the column names as keyword
    arguments, so the row's shape follows the query rather than column order.

    `cls` must be callable. A query whose columns do not match what `cls`
    accepts raises on the first row, with the `TypeError` raised by `cls`.
    """

    def __init__(self, cls: Callable[..., Any]) -> None: ...
    @property
    def cls(self) -> Callable[..., Any]: ...

class SinglePageIterator:
    """
    Iterates over rows in a single page of query results.

    Yields rows materialized by the request's row factory.
    Does not fetch additional pages - use AsyncRowsIterator for automatic paging.
    """

    def __iter__(self) -> SinglePageIterator: ...
    def __next__(self) -> Any: ...

class PagingState:
    """
    Represents paging state for paged queries.

    Used to continue a query from where the previous page ended.
    Can be passed to execute() to resume paging from a specific position.
    """

    def __init__(self) -> None:
        """
        Creates a new paging state starting from the first page.
        """
    def as_bytes(self) -> bytes | None:
        """
        Returns the inner representation of `PagingState` as bytes.

        Use this to store paging state for a longer time, and later restore it
        using `from_bytes()`. Returns `None` if this represents the start state
        (no previous page).

        Returns
        -------
        bytes | None
            Raw paging state bytes, or `None` for the start state.
        """

    @staticmethod
    def from_bytes(raw_bytes: bytes) -> PagingState:
        """
        Creates `PagingState` from raw bytes.

        Use this to restore paging state after longer time, having previously
        stored it using `as_bytes()`.

        Parameters
        ----------
        raw_bytes : bytes
            Raw paging state bytes previously obtained from `as_bytes()`.

        Returns
        -------
        PagingState
            A new `PagingState` restored from the raw bytes.
        """

    def __eq__(self, other: object) -> bool: ...

class RequestResult:
    """
    Immutable result of a query execution.
    """

    def has_more_pages(self) -> bool:
        """
        Returns True if more pages are available.
        """

    def paging_state(self) -> PagingState | None:
        """
        Returns current paging state. Can be `None` if there are no more pages available.
        """

    def fetch_next_page(self) -> DriverFuture[RequestResult | None]:
        """
        Fetches the next page if available.

        Returns a new RequestResult with the next page's data if more pages
        are available. Returns None if no more pages exist.

        Returns
        -------
        DriverFuture[RequestResult | None]
            A future resolving to the next page data, or None if no more pages.
        """

    def iter_current_page(self) -> SinglePageIterator:
        """
        Returns an iterator over rows in the current page.
        """

    def __aiter__(self) -> AsyncRowsIterator: ...
    def first_row(self) -> DriverFuture[Any | None]:
        """
        Returns a future resolving to the first row starting from the current state.

        Fetches the first available row from the current page onwards,
        automatically retrieving additional pages as needed. This method
        does not modify the RequestResult object. Returns None if no more
        rows are available.

        Returns
        -------
        DriverFuture[Any | None]
            A future resolving to the first row, or None if no more rows exist.
        """

    def all(self) -> DriverFuture[list[Any]]:
        """
        Return a future resolving to all rows of the result set as a list.

        This method eagerly fetches all remaining pages and materializes
        the entire result set in memory. It should be used with care
        for large queries.
        """

    @property
    def columns(self) -> tuple[ColumnSpec, ...]:
        """
        Specifications of the columns in this result.

        Empty for a result that carries no rows, such as an ``INSERT``.
        """

class AsyncRowsIterator(AsyncIterator[Any]):
    """
    Async iterator over rows with automatic paging.

    Transparently fetches subsequent pages as iteration progresses.
    When the current page is exhausted, automatically retrieves the next page.
    """

    def __aiter__(self) -> AsyncRowsIterator: ...
    def __anext__(self) -> DriverFuture[Any]: ...
