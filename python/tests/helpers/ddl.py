"""Helper for executing DDL statements in tests.

ScyllaDB occasionally rejects concurrent schema changes with a group 0 error.
To keep the test suite from flaking on it, every DDL statement (creating /
altering / dropping a keyspace / table / type) should go through `ddl()`
instead of `Session.execute()`. It pins schema changes to a single node and
shard, and retries the group 0 conflict if it happens anyway.

This mirrors the `PerformDDL` trait used by the tests of the Rust driver.
"""

import logging
from collections.abc import Iterable

from scylla.cluster import ClusterState, Node
from scylla.errors import ServerError
from scylla.policies.load_balancing import RoutingInfo
from scylla.policies.retry import (
    RequestInfo,
    RetryDecision,
    RetrySession,
)
from scylla.routing import Shard
from scylla.session import Session
from scylla.statement import Statement

logger = logging.getLogger(__name__)

GROUP0_CONFLICT_MESSAGE = "Failed to apply group 0 change due to concurrent modification"

MAX_GROUP0_CONFLICT_RETRIES = 10


class SchemaQueriesLBP:
    """
    Produces a predictable query plan - it orders the nodes by host id. This is
    to make sure that all DDL queries land on the same node, to prevent errors
    from concurrent DDL queries executed on different nodes.

    Note that we cannot rely on the order of `ClusterState.nodes_info` here: it
    is derived from a `HashMap`, so it is arbitrary and gets
    reshuffled on every metadata refresh. Host ids are stable, so sorting by them gives
    every session the same target node.
    """

    def pick_targets(self, routing_info: RoutingInfo, cluster_state: ClusterState) -> Iterable[tuple[Node, Shard]]:
        nodes = sorted(cluster_state.nodes_info.values(), key=lambda node: node.host_id)
        # It is unclear whether Scylla can handle concurrent DDL queries to
        # different shards, in other words if its local lock is per-node or per
        # shard. Just to be safe, let's use an explicit shard.
        return [(node, 0) for node in nodes]


class SchemaQueriesRetrySession:
    def __init__(self) -> None:
        self.count = 0

    def decide_should_retry(self, request_info: RequestInfo) -> RetryDecision:
        error = request_info.error
        if not (isinstance(error, ServerError) and error.reason == GROUP0_CONFLICT_MESSAGE):
            return RetryDecision.DontRetry()

        self.count += 1
        # Give up if there are many failures. In this case we really should do
        # something about it in the core, because it is absurd for DDL queries
        # to fail this often.
        if self.count >= MAX_GROUP0_CONFLICT_RETRIES:
            logger.error(
                "Received group 0 concurrent modification error during DDL %d times. Please fix Scylla Core.",
                self.count,
            )
            return RetryDecision.DontRetry()

        logger.warning(
            "Received group 0 concurrent modification error during DDL. Performing retry #%d.",
            self.count,
        )
        return RetryDecision.RetrySameTarget()

    def reset(self) -> None:
        self.count = 0


class SchemaQueriesRetryPolicy:
    def new_session(self) -> RetrySession:
        return SchemaQueriesRetrySession()


_SCHEMA_QUERIES_LBP = SchemaQueriesLBP()
_SCHEMA_QUERIES_RETRY_POLICY = SchemaQueriesRetryPolicy()


async def ddl(session: Session, query: Statement | str) -> None:
    """Execute a DDL statement, guarded against group 0 conflicts.

    A passed `Statement` gets its load balancing and retry policies overwritten in place.
    """
    statement = Statement(query) if isinstance(query, str) else query
    statement.load_balancing_policy = _SCHEMA_QUERIES_LBP
    statement.retry_policy = _SCHEMA_QUERIES_RETRY_POLICY
    await session.execute(statement)
