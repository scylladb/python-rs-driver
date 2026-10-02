from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator, Iterable

import pytest
import pytest_asyncio
from helpers.ddl import ddl
from helpers.session import connect
from scylla.batch import Batch
from scylla.cluster import ClusterState, Node
from scylla.errors import ExecuteError
from scylla.policies.load_balancing import RoutingInfo
from scylla.routing import Shard
from scylla.session import Session
from scylla.statement import Statement

KEYSPACE = "test_pinning_ks"
TABLE = "pinned_rows"

COORDINATOR_QUERY = "SELECT host_id FROM system.local"


async def set_up() -> Session:
    session = await connect()
    await ddl(
        session,
        f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}};
    """,
    )
    await session.use_keyspace(KEYSPACE)
    await ddl(session, f"CREATE TABLE IF NOT EXISTS {TABLE} (id int PRIMARY KEY, v int);")
    return session


@pytest_asyncio.fixture(scope="module")
async def session() -> AsyncGenerator[Session, None]:
    s = await set_up()
    yield s
    await ddl(s, f"DROP KEYSPACE {KEYSPACE}")


class CountingPolicy:
    """Custom policy that records how many times the driver asked it for a plan."""

    def __init__(self) -> None:
        self.calls = 0

    def pick_targets(
        self,
        routing_info: RoutingInfo,
        cluster_state: ClusterState,
    ) -> Iterable[tuple[Node, Shard | None]]:
        self.calls += 1
        return [(node, None) for node in cluster_state.nodes_info.values()]


def insert_batch(key: int) -> Batch:
    batch = Batch()
    batch.add(f"INSERT INTO {TABLE} (id, v) VALUES ({key}, {key})")
    return batch


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_pinning_replaces_the_load_balancing_policy(session: Session) -> None:
    node = next(iter(session.cluster_state.nodes_info.values()))
    policy = CountingPolicy()
    statement = Statement(COORDINATOR_QUERY).with_load_balancing_policy(policy)

    await session.execute(statement)
    unpinned_calls = policy.calls
    assert unpinned_calls > 0, "without a target the statement policy plans the request"

    row = await (await session.execute(statement, target=node)).first_row()

    assert row is not None
    assert row["host_id"] == node.host_id
    assert policy.calls == unpinned_calls, "pinning should replace the statement policy, not narrow its plan"


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_pinning_to_an_unknown_host_id_fails(session: Session) -> None:
    with pytest.raises(ExecuteError):
        await session.execute(COORDINATOR_QUERY, target=uuid.uuid4())


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_batch_pinning_replaces_the_load_balancing_policy(session: Session) -> None:
    node = next(iter(session.cluster_state.nodes_info.values()))
    policy = CountingPolicy()
    batch = insert_batch(1).with_load_balancing_policy(policy)

    await session.batch(batch)
    unpinned_calls = policy.calls
    assert unpinned_calls > 0, "without a target the batch policy plans the request"

    await session.batch(batch, target=node)

    assert policy.calls == unpinned_calls, "pinning should replace the batch policy, not narrow its plan"


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_batch_pinning_to_an_unknown_host_id_fails(session: Session) -> None:
    with pytest.raises(ExecuteError):
        await session.batch(insert_batch(2), target=uuid.uuid4())
