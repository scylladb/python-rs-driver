import uuid
from collections.abc import Iterator
from datetime import timedelta
from typing import Any

from scylla.cluster import Node
from scylla.routing import Target

from .batch import Batch
from .cluster import ClusterState
from .cluster.metadata import CqlColumnType
from .execution_profile import ExecutionProfile
from .future import ResponseFuture
from .results import PagingState, RowFactory
from .statement import PreparedStatement, Statement
from .types import UnsetType

class LegacySession:
    """
    The legacy (``cassandra-driver`` compatible) session: blocking ``execute()``
    and ``prepare()``, ``execute_async()`` returning a :class:`ResponseFuture`.

    Created with ``SessionBuilder.connect_legacy()``.
    """

    def execute(
        self,
        query: PreparedStatement | Statement | Batch | str,
        parameters: Any | None = None,
        timeout: timedelta | float | None | UnsetType = ...,
        trace: bool = False,
        custom_payload: None = None,
        execution_profile: ExecutionProfile | None = None,
        paging_state: PagingState | bytes | None = None,
        host: Target | Node | uuid.UUID | None = None,
        execute_as: None = None,
    ) -> ResultSet:
        """
        Execute ``query`` and block for its :class:`ResultSet`.

        ``timeout`` defaults to the execution profile's; ``None`` disables it.
        ``host`` pins the request to one node (and optionally shard).
        ``custom_payload`` and ``execute_as`` raise ``NotImplementedError`` when given.
        """

    def execute_async(
        self,
        query: PreparedStatement | Statement | Batch | str,
        parameters: Any | None = None,
        trace: bool = False,
        custom_payload: None = None,
        timeout: timedelta | float | None | UnsetType = ...,
        execution_profile: ExecutionProfile | None = None,
        paging_state: PagingState | bytes | None = None,
        host: Target | Node | uuid.UUID | None = None,
        execute_as: None = None,
    ) -> ResponseFuture:
        """Execute ``query`` and return the :class:`ResponseFuture` delivering its result."""

    def prepare(
        self,
        query: Statement | str,
        custom_payload: None = None,
        keyspace: None = None,
    ) -> PreparedStatement:
        """Prepare ``query``, blocking until the server answers."""

    def set_keyspace(self, keyspace: str) -> None:
        """Set the keyspace of every connection, blocking until done. Case-sensitive."""

    def shutdown(self) -> None:
        """
        Mark the session shut down: ``execute``, ``execute_async`` and ``prepare``
        then raise.
        """

    @property
    def is_shutdown(self) -> bool: ...
    @property
    def row_factory(self) -> RowFactory | None:
        """Row factory of requests that do not set one; ``None`` for the driver default."""

    @row_factory.setter
    def row_factory(self, row_factory: RowFactory | None) -> None: ...
    @property
    def default_fetch_size(self) -> int | None:
        """
        Page size of requests whose statement sets none: the driver default until
        set, ``None`` once paging was disabled for them.
        """

    @default_fetch_size.setter
    def default_fetch_size(self, fetch_size: int | None) -> None:
        """
        A positive page size, or ``None`` to run such requests unpaged, as the
        legacy driver did. Raises ``ValueError`` otherwise.
        """

    @property
    def default_timeout(self) -> float | None:
        """
        Timeout in seconds of a request given none whose statement has none of its
        own: the one set here, else the default execution profile's. ``None`` for
        no timeout.
        """

    @default_timeout.setter
    def default_timeout(self, timeout: timedelta | float | None | UnsetType) -> None:
        """Seconds, ``None`` for no timeout, or ``Unset`` to fall back to the default profile's."""

    @property
    def keyspace(self) -> str | None:
        """The keyspace in use, if any."""

    @property
    def cluster_state(self) -> ClusterState: ...

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
        """Not supported; raises ``NotImplementedError``."""

    def get_all_query_traces(self, max_wait_sec_per: float | None = None) -> Any:
        """Not supported; raises ``NotImplementedError``."""
