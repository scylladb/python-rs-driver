"""Built-in retry policies against a real `Unavailable`: a 3-node cluster with one node down."""

import asyncio
from collections.abc import Generator, Iterator
from dataclasses import dataclass
from typing import Any

import pytest
from helpers.ccm import (  # pyright: ignore[reportMissingTypeStubs]
    create_scylla_cluster,
    get_contact_points,
    start_cluster,
    stop_and_remove_cluster,
)
from scylla.cluster import ClusterState, Node
from scylla.errors import Unavailable
from scylla.policies.load_balancing import RoutingInfo
from scylla.policies.retry import (
    DefaultRetryPolicy,
    DowngradingConsistencyRetryPolicy,
    FallthroughRetryPolicy,
    RetryPolicy,
)
from scylla.results import DictRowFactory
from scylla.routing import Shard
from scylla.session import ExecutionProfile, Session, SessionBuilder
from scylla.statement import Consistency, Statement

pytestmark = pytest.mark.requires_ccm

KEYSPACE = "retry_policy_ks"
SELECT = f"SELECT v FROM {KEYSPACE}.t WHERE k = 1"


@dataclass
class DegradedCluster:
    live: list[tuple[str, int]]
    down: tuple[str, int]


async def _connect(contact_points: list[tuple[str, int]]) -> Session:
    # Not `helpers.session.session_builder()`: `contact_points()` adds to the
    # builder's list, so that would also connect to the shared test cluster.
    return (
        await SessionBuilder()
        .contact_points(contact_points)
        .execution_profile(ExecutionProfile(row_factory=DictRowFactory()))
        .connect()
    )


async def _create_data(contact_points: list[tuple[str, int]]) -> None:
    session = await _connect(contact_points)
    # A fresh cluster used by this module alone: no concurrent DDL to guard against.
    await session.execute(
        f"CREATE KEYSPACE {KEYSPACE} WITH replication = "
        + "{'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'enabled': false}"
    )
    await session.execute(f"CREATE TABLE {KEYSPACE}.t (k int PRIMARY KEY, v int)")
    insert = Statement(f"INSERT INTO {KEYSPACE}.t (k, v) VALUES (1, 42)")
    insert.consistency = Consistency.All
    await session.execute(insert)


@pytest.fixture(scope="module")
def degraded_cluster() -> Generator[DegradedCluster, Any, None]:
    cluster = create_scylla_cluster(
        name="retry_policy_cluster",
        scylla_version="release:6.2.2",
        nodes=3,
        ipprefix="127.0.2.",
    )
    try:
        start_cluster(cluster, jvm_args=["--smp", "1", "--memory", "512M"])
        contact_points = get_contact_points(cluster)
        asyncio.run(_create_data(contact_points))

        # Returns once the other two nodes have marked it down, so their
        # coordinators count two live replicas out of three.
        cluster.nodelist()[2].stop(wait_other_notice=True)

        yield DegradedCluster(live=contact_points[:2], down=contact_points[2])
    finally:
        stop_and_remove_cluster(cluster)


class LiveNodesPlan:
    """Sends requests to the live nodes only, and counts how many of them the driver used.

    The driver pulls the next target from the plan only when a retry policy
    asks for another node, so `targets_used` tells whether it retried there.
    """

    def __init__(self) -> None:
        self.down: tuple[str, int] = ("", 0)
        self.targets_used = 0

    def pick_targets(
        self, routing_info: RoutingInfo, cluster_state: ClusterState
    ) -> Iterator[tuple[Node, Shard | None]]:
        live = [
            node for node in cluster_state.nodes_info.values() if (str(node.address[0]), node.address[1]) != self.down
        ]
        for node in sorted(live, key=lambda node: node.host_id):
            self.targets_used += 1
            yield node, None


def _read_at_all(cluster: DegradedCluster, policy: RetryPolicy) -> tuple[Statement, LiveNodesPlan]:
    plan = LiveNodesPlan()
    plan.down = cluster.down
    statement = Statement(SELECT)
    statement.consistency = Consistency.All
    statement.retry_policy = policy
    statement.load_balancing_policy = plan
    return statement, plan


@pytest.mark.asyncio
async def test_downgrading_policy_reads_with_a_node_down(degraded_cluster: DegradedCluster):
    session = await _connect(degraded_cluster.live)
    statement, plan = _read_at_all(degraded_cluster, DowngradingConsistencyRetryPolicy())

    result = await session.execute(statement)

    assert await result.first_row() == {"v": 42}
    # Downgraded on the same node.
    assert plan.targets_used == 1


@pytest.mark.asyncio
async def test_default_policy_tries_the_next_node_then_raises_unavailable(degraded_cluster: DegradedCluster):
    session = await _connect(degraded_cluster.live)
    statement, plan = _read_at_all(degraded_cluster, DefaultRetryPolicy())

    with pytest.raises(Unavailable) as excinfo:
        await session.execute(statement)

    assert excinfo.value.consistency == Consistency.All
    assert excinfo.value.required == 3
    assert excinfo.value.alive == 2
    assert plan.targets_used == 2


@pytest.mark.asyncio
async def test_fallthrough_policy_raises_unavailable_without_retrying(degraded_cluster: DegradedCluster):
    session = await _connect(degraded_cluster.live)
    statement, plan = _read_at_all(degraded_cluster, FallthroughRetryPolicy())

    with pytest.raises(Unavailable) as excinfo:
        await session.execute(statement)

    assert excinfo.value.required == 3
    assert excinfo.value.alive == 2
    assert plan.targets_used == 1
