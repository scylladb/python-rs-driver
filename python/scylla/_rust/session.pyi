import uuid
from typing import Any

from scylla.cluster import ClusterState, Node
from scylla.future import DriverFuture
from scylla.results import PagingState, RequestResult, RowFactoryLike
from scylla.routing import Target
from scylla.statement import Batch, PreparedStatement, Statement
from scylla.tracing import QueryTrace

class Session:
    """
    Represents a CQL session, which can be used to communicate with the database.
    """

    @property
    def cluster_state(self) -> ClusterState:
        """
        Access information about the cluster topology or schema through ClusterState object.
        """

    def use_keyspace(self, keyspace: str, case_sensitive: bool = False) -> DriverFuture[None]:
        """
        Sends `USE <keyspace>` request on all connections.
        This allows to write `SELECT * FROM table` instead of `SELECT * FROM keyspace.table`

        Note that even failed `use_keyspace` can change currently used keyspace - the request is sent on all connections and
        can overwrite previously used keyspace.

        Raises
        ------
        UseKeyspaceError
            If an error occurred when trying to use the provided keyspace.
        """

    def prepare(self, statement: Statement | str) -> DriverFuture[PreparedStatement]:
        """
        Prepare a statement for repeated execution.

        Parameters
        ----------
        statement : Statement | str
            The statement to prepare.

        Returns
        -------
        DriverFuture[PreparedStatement]
            A future resolving to a prepared statement ready for execution with parameters.
        """

    def execute(
        self,
        statement: PreparedStatement | Statement | str,
        values: Any | None = None,
        /,
        *,
        factory: RowFactoryLike | None = None,
        paging_state: PagingState | None = None,
        paged: bool = True,
        target: Target | Node | uuid.UUID | None = None,
        trace: bool | None = None,
    ) -> DriverFuture[RequestResult]:
        """
        Execute a query and return results.

        Parameters
        ----------
        statement : PreparedStatement | Statement | str
            The statement to execute.
        values : Any | None, optional
            Query parameters to bind to the statement. Default is None.
        factory : RowFactoryLike | None, optional
            Row factory used to construct row objects, or a bare callable used
            directly as the row builder. When None, falls back to the statement's
            row factory, then its execution profile's, then the session's default
            execution profile's, and finally NamedTupleRowFactory().
        paging_state : PagingState | None, optional
            Paging state to resume from a previous query. Default is None.
        paged : bool, optional
            Enable automatic paging if True. Otherwise, all rows come in a single result frame,
            which is **strongly discouraged** for large (over thousands of rows) responses,
            and acceptable for responses containing few or no rows.
            Default is True.
        target : Target | Node | uuid.UUID | None, optional
            Pin this request to a single node, and optionally to a single shard on
            that node. A bare node means "this node, any shard"; see
            `scylla.routing.Target` for the node and shard semantics. Default is None,
            meaning the request is routed by the applicable load balancing policy.

            Pinning is the most specific setting there is: it replaces the load
            balancing policy set on the statement, which in turn replaces the one set
            on the execution profile. It does not narrow that policy's plan - it
            discards it, token awareness included.

            A pinned request has nowhere to fall back to, so it either reaches the
            given target or fails.
        trace : bool | None, optional
            If `True`, the server traces this execution. If `False`, the server
            does not trace it. If `None`, the `tracing` attribute of the statement
            decides. Default is None. The tracing ids are in
            `RequestResult.trace_ids` and `Page.trace_id`.

        Returns
        -------
        DriverFuture[RequestResult]
            A future resolving to query results with paging support.
        """

    def batch(
        self,
        batch: Batch,
        /,
        *,
        factory: RowFactoryLike | None = None,
        target: Target | Node | uuid.UUID | None = None,
        trace: bool | None = None,
    ) -> DriverFuture[RequestResult]:
        """
        Execute a batch statement, which can contain many `Statement` and `PreparedStatement` objects.

        Parameters
        ----------
        batch : ~scylla.statement.Batch
            The batch of statements and their values to execute.
        factory : RowFactoryLike | None, optional
            Row factory used to construct row objects, or a bare callable used
            directly as the row builder. When None, falls back to the batch's
            row factory, then its execution profile's, then the session's default
            execution profile's, and finally NamedTupleRowFactory().
        target : Target | Node | uuid.UUID | None, optional
            Pin this request to a single node, and optionally to a single shard on
            that node. A bare node means "this node, any shard"; see
            `scylla.routing.Target` for the node and shard semantics. Default is None,
            meaning the request is routed by the applicable load balancing policy.

            Pinning is the most specific setting there is: it replaces the load
            balancing policy set on the statement, which in turn replaces the one set
            on the execution profile. It does not narrow that policy's plan - it
            discards it, token awareness included.

            A pinned request has nowhere to fall back to, so it either reaches the
            given target or fails.
        trace : bool | None, optional
            If `True`, the server traces this execution. If `False`, the server
            does not trace it. If `None`, the `tracing` attribute of the batch
            decides. Default is None.

        Returns
        -------
        DriverFuture[RequestResult]
            A future resolving to the batch result.
            For non-LWT batches, the result does not contain rows.
            For LWT batches, the result contains rows with a boolean `[applied]` column.
            In each returned row, columns other than `[applied]` contain either the current
            values of that row (if the condition was not met) or `None` values (if it was met).
        """

    def fetch_query_trace(self, trace_id: uuid.UUID) -> DriverFuture[QueryTrace]:
        """
        Fetches the trace of the request with this tracing id.

        The server writes a trace some time after it sends the response. Thus the
        driver tries again until the trace is available. `SessionBuilder` sets the
        number of attempts, the interval between them, and their consistency level.

        Raises
        ------
        TraceUnavailable
            If the trace is not available after all attempts, or if the driver
            cannot read it.
        """

    def await_schema_agreement(self) -> DriverFuture[uuid.UUID]:
        """
        Wait until all nodes in the cluster agree on the current schema version.

        This is useful after performing schema-altering operations to ensure that all nodes
        have updated their schema before proceeding with operations that depend on the new schema.

        Returns
        -------
        DriverFuture[uuid.UUID]
            A future resolving to the agreed schema version as a UUID object.

        Raises
        ------
        RuntimeError
            If the schema agreement could not be reached.
        """

    def check_schema_agreement(self) -> DriverFuture[uuid.UUID | None]:
        """
        Check if all nodes in the cluster agree on the current schema version.

        Unlike `await_schema_agreement`, this method does not wait for agreement to be reached,
        but instead returns the current state immediately.

        Returns
        -------
        DriverFuture[uuid.UUID | None]
            A future resolving to the agreed schema version if all nodes agree, None otherwise.

        Raises
        ------
        RuntimeError
            If the schema agreement check failed.
        """

    def refresh_metadata(self) -> DriverFuture[None]:
        """
        Fetch the current nodes and schema of the cluster and update `cluster_state`.

        Normally this is not necessary. The driver finds metadata changes in the
        cluster automatically. Use this method to see a change immediately, for
        example after a topology change.

        Raises
        ------
        MetadataRefreshError
            If the metadata refresh failed.
        """
