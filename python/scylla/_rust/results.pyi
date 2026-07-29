import ipaddress
from collections.abc import AsyncIterator
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, TypeAlias
from uuid import UUID

from dateutil.relativedelta import relativedelta

from .future import ResponseFuture

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

class ColumnIterator:
    """
    Iterator over columns of a single row.

    Yields Column objects representing individual column values
    in the current row.
    """
    def __iter__(self) -> ColumnIterator: ...
    def __next__(self) -> Column: ...

class RowFactory:
    """
    Factory used to construct a row object from a column iterator.

    Allows custom row representations (e.g. dicts, dataclasses).
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None: ...
    def build(self, column_iterator: ColumnIterator) -> dict[str, CqlValue]:
        """
        Build a row object from the provided column iterator.
        """

class Column:
    """
    Represents a single column in a result row.
    """
    @property
    def column_name(self) -> str:
        """Name of the column."""

    @property
    def value(self) -> CqlValue:
        """Deserialized value of the column."""

class SinglePageIterator:
    """
    Iterates over rows in a single page of query results.

    Yields deserialized rows materialized using a `RowFactory`.
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

    def fetch_next_page(self) -> ResponseFuture[RequestResult | None]:
        """
        Fetches the next page if available.

        Returns a new RequestResult with the next page's data if more pages
        are available. Returns None if no more pages exist.

        Returns
        -------
        ResponseFuture[RequestResult | None]
            A future resolving to the next page data, or None if no more pages.
        """

    def iter_current_page(self) -> SinglePageIterator:
        """
        Returns an iterator over rows in the current page.
        """

    def __aiter__(self) -> AsyncRowsIterator: ...
    def first_row(self) -> ResponseFuture[Any | None]:
        """
        Returns a future resolving to the first row starting from the current state.

        Fetches the first available row from the current page onwards,
        automatically retrieving additional pages as needed. This method
        does not modify the RequestResult object. Returns None if no more
        rows are available.

        Returns
        -------
        ResponseFuture[Any | None]
            A future resolving to the first row, or None if no more rows exist.
        """

    def all(self) -> ResponseFuture[list[Any]]:
        """
        Return a future resolving to all rows of the result set as a list.

        This method eagerly fetches all remaining pages and materializes
        the entire result set in memory. It should be used with care
        for large queries.
        """

class AsyncRowsIterator(AsyncIterator[Any]):
    """
    Async iterator over rows with automatic paging.

    Transparently fetches subsequent pages as iteration progresses.
    When the current page is exhausted, automatically retrieves the next page.
    """

    def __aiter__(self) -> AsyncRowsIterator: ...
    def __anext__(self) -> ResponseFuture[Any]: ...
