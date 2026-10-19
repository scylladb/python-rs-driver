from collections.abc import Callable, Iterable, Iterator
from datetime import timedelta
from typing import Any
from uuid import UUID

from scylla._rust.types import UnsetType
from scylla.cluster import Node
from scylla.cql_types import CqlColumnType
from scylla.results import ColumnSpec, PagingState, RowFactoryLike
from scylla.routing import Target
from scylla.session import ExecutionProfile
from scylla.statement import Batch, PreparedStatement, Statement

class LegacySession:
    """
    The legacy (``cassandra-driver`` compatible) session: blocking ``execute()``,
    ``execute_async()`` returning a :class:`ResponseFuture`.

    Created with ``SessionBuilder.connect_legacy()``.
    """

    def execute(
        self,
        query: PreparedStatement | Statement | Batch | str,
        parameters: Any | None = None,
        timeout: timedelta | float | None | UnsetType = ...,
        trace: bool | None = None,
        execution_profile: ExecutionProfile | None = None,
        paging_state: PagingState | bytes | None = None,
        host: Target | Node | UUID | None = None,
    ) -> ResultSet:
        """
        Execute ``query`` and block for its :class:`ResultSet`.

        ``timeout`` defaults to the execution profile's; ``None`` disables it.
        ``trace`` overrides the ``tracing`` attribute of the statement; ``None`` keeps it.
        ``host`` pins the request to one node (and optionally shard).
        """

    def execute_async(
        self,
        query: PreparedStatement | Statement | Batch | str,
        parameters: Any | None = None,
        trace: bool | None = None,
        timeout: timedelta | float | None | UnsetType = ...,
        execution_profile: ExecutionProfile | None = None,
        paging_state: PagingState | bytes | None = None,
        host: Target | Node | UUID | None = None,
    ) -> ResponseFuture:
        """Execute ``query`` and return the :class:`ResponseFuture` delivering its result."""

class QueryExhausted(Exception):
    """
    Raised by `ResponseFuture.start_fetching_next_page()` when there are no more pages.
    """

class ResponseFuture:
    """
    A request in flight, delivering its result synchronously via :meth:`result`
    or asynchronously via callbacks. Returned by ``LegacySession.execute_async()``.

    Callbacks receive the rows of one page (a list, or ``None`` for a result
    without rows) and persist across pages: after
    :meth:`start_fetching_next_page` they fire again for the next page.
    """

    def result(self) -> ResultSet:
        """Block until the request settles; return its :class:`ResultSet` or raise."""

    def add_callback(self, callback: Callable[..., Any], *args: Any, **kwargs: Any) -> ResponseFuture:
        """
        Register ``callback(rows, *args, **kwargs)`` to run when a page arrives.
        Runs immediately if one already has. Returns ``self``.
        """

    def add_errback(self, errback: Callable[..., Any], *args: Any, **kwargs: Any) -> ResponseFuture:
        """
        Register ``errback(exception, *args, **kwargs)`` to run when a request fails.
        Runs immediately if one already has. Returns ``self``.
        """

    def add_callbacks(
        self,
        callback: Callable[..., Any],
        errback: Callable[..., Any],
        callback_args: Iterable[Any] | None = None,
        callback_kwargs: dict[str, Any] | None = None,
        errback_args: Iterable[Any] | None = None,
        errback_kwargs: dict[str, Any] | None = None,
    ) -> None:
        """:meth:`add_callback` and :meth:`add_errback` in one call."""

    def clear_callbacks(self) -> None: ...
    def start_fetching_next_page(self) -> None:
        """
        Start fetching the next page; callbacks fire again when it arrives.
        Raises ``QueryExhausted`` if there is none.
        """

    @property
    def has_more_pages(self) -> bool:
        """Whether the last page received says more follow."""

    @property
    def paging_state(self) -> PagingState | None: ...
    @property
    def columns(self) -> tuple[ColumnSpec, ...] | None:
        """Specifications of the result columns, ``None`` until a page with rows arrived."""

    @property
    def column_names(self) -> list[str] | None: ...
    @property
    def column_types(self) -> list[CqlColumnType] | None:
        """CQL types of the result columns, ``None`` until a page with rows arrived."""

    @property
    def warnings(self) -> list[str] | None:
        """
        Warnings the server attached to the last page, ``None`` if it sent none.
        Raises until the request settled.
        """

    @property
    def custom_payload(self) -> None:
        """Not supported; raises ``NotImplementedError``."""

    @property
    def query(self) -> Any:
        """The statement being executed."""

    @property
    def timeout(self) -> float | None:
        """Client-side timeout of the request in seconds, ``None`` for no timeout."""

    @property
    def row_factory(self) -> RowFactoryLike | None: ...
    @property
    def coordinator_host(self) -> Any:
        """Not supported; raises ``NotImplementedError``."""

    @property
    def attempted_hosts(self) -> list[Any]:
        """Not supported; raises ``NotImplementedError``."""

    @property
    def is_schema_agreed(self) -> bool:
        """Not supported; raises ``NotImplementedError``."""

    def get_query_trace_ids(self) -> list[UUID]:
        """Tracing id of every page received so far, if tracing was enabled."""

    def get_query_trace(self, max_wait: float | None = None, query_cl: Any = None) -> Any:
        """Not implemented yet; raises ``NotImplementedError``."""

    def get_all_query_traces(self, max_wait_per: float | None = None, query_cl: Any = None) -> Any:
        """Not implemented yet; raises ``NotImplementedError``."""

class ResultSet:
    """
    Rows of a request, iterated page by page with further pages fetched
    transparently. Handed out by :meth:`ResponseFuture.result`; not constructible.

    ``==`` and indexing materialize every remaining row first.
    """

    def __iter__(self) -> Iterator[Any]: ...
    def __next__(self) -> Any: ...
    def __getitem__(self, index: Any) -> Any: ...
    def __eq__(self, other: object) -> bool: ...
    def __bool__(self) -> bool: ...
    def one(self) -> Any | None:
        """The first row of the current page, ``None`` if it is empty."""

    def all(self) -> list[Any]:
        """Every remaining row as a list; ``list(result_set)``."""

    def fetch_next_page(self) -> None:
        """
        Fetch the next page synchronously into ``current_rows``; not needed when
        iterating. If iteration has started, it continues from the new page's
        first row.
        """

    @property
    def has_more_pages(self) -> bool: ...
    @property
    def current_rows(self) -> list[Any]:
        """Rows of the current page. Empty does not mean exhausted; see ``has_more_pages``."""

    @property
    def paging_state(self) -> PagingState | None: ...
    @property
    def column_names(self) -> list[str] | None: ...
    @property
    def column_types(self) -> list[CqlColumnType] | None: ...
    @property
    def was_applied(self) -> bool:
        """For an LWT result, whether the transaction was applied."""

    def get_query_trace(self, max_wait_sec: float | None = None) -> Any:
        """Not implemented yet; raises ``NotImplementedError``."""

    def get_all_query_traces(self, max_wait_sec_per: float | None = None) -> Any:
        """Not implemented yet; raises ``NotImplementedError``."""
