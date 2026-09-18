from __future__ import annotations

import logging
from collections.abc import AsyncGenerator, Awaitable, Callable, Iterable
from typing import Any

import pytest
import pytest_asyncio
from helpers.ddl import ddl
from helpers.session import connect
from pytest import LogCaptureFixture
from scylla.cluster import ClusterState, Node
from scylla.enums import Consistency, SerialConsistency
from scylla.errors import ExecuteError
from scylla.policies.load_balancing import DefaultPolicy, LoadBalancingPolicy, NodeLocationPreference, RoutingInfo
from scylla.session import Session
from scylla.statement import Statement


async def set_up() -> Session:
    session = await connect()
    await ddl(
        session,
        """
        CREATE KEYSPACE IF NOT EXISTS test_lb_ks
        WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};
    """,
    )
    await session.use_keyspace("test_lb_ks")
    return session


@pytest_asyncio.fixture(scope="module")
async def session() -> AsyncGenerator[Session, None]:
    s = await set_up()
    yield s
    await ddl(s, "DROP KEYSPACE test_lb_ks")


TableFactory = Callable[[str, str], Awaitable[str]]


@pytest_asyncio.fixture
async def table_factory(session: Session) -> AsyncGenerator[TableFactory, None]:
    created_tables: list[str] = []

    async def create_table(schema: str, name: str) -> str:
        await ddl(session, f"CREATE TABLE IF NOT EXISTS {name} ({schema});")
        created_tables.append(name)
        return name

    yield create_table

    for table in created_tables:
        await ddl(session, f"DROP TABLE IF EXISTS {table};")


class TrackingPolicy:
    """Custom policy that records every call and returns all known nodes."""

    def __init__(self) -> None:
        self.call_count = 0
        self.nodes_seen: list[dict[str, Any]] = []

    def pick_targets(self, routing_info: RoutingInfo, cluster_state: ClusterState) -> Iterable[tuple[Node, int | None]]:
        self.call_count += 1
        nodes_info = cluster_state.nodes_info
        for host_id, node in nodes_info.items():
            self.nodes_seen.append(
                {
                    "host_id": host_id,
                    "address": node.address,
                    "dc": node.datacenter,
                }
            )
        return [(node, None) for node in nodes_info.values()]


class ExplodingPolicy:
    """Custom policy whose pick_targets raises an exception."""

    def pick_targets(self, routing_info: RoutingInfo, cluster_state: ClusterState) -> Iterable[tuple[Node, int | None]]:
        raise RuntimeError("Policy exploded!")


class NonIterablePolicy:
    """Custom policy whose pick_targets returns a non-iterable."""

    def pick_targets(self, routing_info: RoutingInfo, cluster_state: ClusterState) -> Any:
        return 42


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_default_policy_basic(session: Session, table_factory: TableFactory) -> None:
    table = await table_factory("id int PRIMARY KEY, x int", "lb_default_basic")
    for i in range(5):
        await session.execute(f"INSERT INTO {table} (id, x) VALUES ({i}, {i * 10})")

    prepared = await session.prepare(f"SELECT * FROM {table}")
    prepared = prepared.with_load_balancing_policy(DefaultPolicy())
    result = await session.execute(prepared)
    rows = await result.all()
    assert len(rows) == 5


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_default_policy_with_options(session: Session, table_factory: TableFactory) -> None:
    table = await table_factory("id int PRIMARY KEY, x int", "lb_default_options")

    policy = DefaultPolicy(
        token_aware=True,
        permit_dc_failover=True,
        enable_shuffling_replicas=False,
    )
    stmt = Statement(f"SELECT * FROM {table}").with_load_balancing_policy(policy)
    assert await session.execute(stmt) is not None


def test_default_policy_properties() -> None:
    policy = DefaultPolicy(
        node_location_preference=NodeLocationPreference.datacenter("dc1"),
        token_aware=True,
        permit_dc_failover=True,
        enable_shuffling_replicas=False,
    )
    assert policy.preferred_datacenter == "dc1"
    assert policy.preferred_rack is None
    assert policy.token_aware is True
    assert policy.permit_dc_failover is True
    assert policy.enable_shuffling_replicas is False


def test_default_policy_property_defaults() -> None:
    policy = DefaultPolicy()
    assert policy.preferred_datacenter is None
    assert policy.preferred_rack is None
    assert policy.token_aware is True
    assert policy.permit_dc_failover is False
    assert policy.enable_shuffling_replicas is True


def test_default_policy_options_are_keyword_only() -> None:
    with pytest.raises(TypeError):
        DefaultPolicy(NodeLocationPreference.ANY)  # pyright: ignore[reportCallIssue]


def test_node_location_preference_any() -> None:
    pref = NodeLocationPreference.ANY
    assert pref.preferred_datacenter is None
    assert pref.preferred_rack is None
    assert repr(pref) == "NodeLocationPreference.ANY"


def test_node_location_preference_datacenter() -> None:
    pref = NodeLocationPreference.datacenter("us-east-1")
    assert pref.preferred_datacenter == "us-east-1"
    assert pref.preferred_rack is None
    assert repr(pref) == "NodeLocationPreference.datacenter(us-east-1)"


def test_node_location_preference_datacenter_and_rack() -> None:
    pref = NodeLocationPreference.datacenter_and_rack("us-east-1", "rack1")
    assert pref.preferred_datacenter == "us-east-1"
    assert pref.preferred_rack == "rack1"
    assert repr(pref) == "NodeLocationPreference.datacenter_and_rack(us-east-1, rack1)"


def test_default_policy_node_location_preference_getter() -> None:
    policy = DefaultPolicy(node_location_preference=NodeLocationPreference.datacenter_and_rack("dc1", "rack1"))
    assert policy.preferred_datacenter == "dc1"
    assert policy.preferred_rack == "rack1"


def test_default_policy_node_location_preference_none() -> None:
    policy = DefaultPolicy()
    assert policy.node_location_preference is None
    assert policy.preferred_datacenter is None
    assert policy.preferred_rack is None


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_custom_policy_is_called(session: Session, table_factory: TableFactory) -> None:
    table = await table_factory("id int PRIMARY KEY, x int", "lb_custom_called")
    await session.execute(f"INSERT INTO {table} (id, x) VALUES (1, 10)")

    policy = TrackingPolicy()
    stmt = Statement(f"SELECT * FROM {table}").with_load_balancing_policy(policy)
    await session.execute(stmt)
    assert policy.call_count >= 1


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_custom_policy_receives_cluster_state(session: Session, table_factory: TableFactory) -> None:
    table = await table_factory("id int PRIMARY KEY, x int", "lb_custom_state")
    await session.execute(f"INSERT INTO {table} (id, x) VALUES (1, 10)")

    policy = TrackingPolicy()
    stmt = Statement(f"SELECT * FROM {table}").with_load_balancing_policy(policy)
    await session.execute(stmt)
    assert len(policy.nodes_seen) > 0
    assert "host_id" in policy.nodes_seen[0]
    assert "address" in policy.nodes_seen[0]
    assert "dc" in policy.nodes_seen[0]


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_exploding_policy_logs_error_and_fails(
    session: Session,
    table_factory: TableFactory,
    caplog: LogCaptureFixture,
) -> None:
    table = await table_factory("id int PRIMARY KEY, x int", "lb_exploding")
    await session.execute(f"INSERT INTO {table} (id, x) VALUES (1, 10)")

    stmt = Statement(f"SELECT * FROM {table}").with_load_balancing_policy(ExplodingPolicy())
    with caplog.at_level(logging.ERROR), pytest.raises(ExecuteError):
        await session.execute(stmt)

    assert "Failed to call 'pick_targets' method on LoadBalancing Policy" in caplog.text
    assert "Policy exploded!" in caplog.text


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_non_iterable_policy_logs_error_and_fails(
    session: Session,
    table_factory: TableFactory,
    caplog: LogCaptureFixture,
) -> None:
    table = await table_factory("id int PRIMARY KEY, x int", "lb_non_iterable")
    await session.execute(f"INSERT INTO {table} (id, x) VALUES (1, 10)")

    stmt = Statement(f"SELECT * FROM {table}").with_load_balancing_policy(NonIterablePolicy())
    with caplog.at_level(logging.ERROR), pytest.raises(ExecuteError):
        await session.execute(stmt)

    assert "The value returned by 'pick_targets' is not iterable" in caplog.text


def test_default_policy_conforms_to_protocol() -> None:
    assert isinstance(DefaultPolicy(), LoadBalancingPolicy)


def test_custom_policy_conforms_to_protocol() -> None:
    class Good:
        def pick_targets(
            self, routing_info: RoutingInfo, cluster_state: ClusterState
        ) -> Iterable[tuple[Node, int | None]]:
            return iter([])

    assert isinstance(Good(), LoadBalancingPolicy)


def test_class_without_pick_targets_does_not_conform() -> None:
    class Bad:
        pass

    assert not isinstance(Bad(), LoadBalancingPolicy)


class RoutingInfoCapturingPolicy:
    """Custom policy that captures the RoutingInfo for later inspection."""

    def __init__(self) -> None:
        self.captured: RoutingInfo | None = None

    def pick_targets(self, routing_info: RoutingInfo, cluster_state: ClusterState) -> Iterable[tuple[Node, int | None]]:
        self.captured = routing_info
        return [(node, None) for node in cluster_state.nodes_info.values()]


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_routing_info_fields_on_prepared_lwt(session: Session, table_factory: TableFactory) -> None:
    table = await table_factory("id int PRIMARY KEY, v int", "lb_routing_info")
    await session.execute(f"INSERT INTO {table} (id, v) VALUES (1, 10)")

    prepared = await session.prepare(f"UPDATE {table} SET v = ? WHERE id = ? IF v = ?")

    policy = RoutingInfoCapturingPolicy()
    prepared = (
        prepared.with_consistency(Consistency.Quorum)
        .with_serial_consistency(SerialConsistency.Serial)
        .with_load_balancing_policy(policy)
    )
    await session.execute(prepared, [42, 1, 10])

    ri = policy.captured
    assert ri is not None, "RoutingInfo was never captured"

    assert ri.consistency == Consistency.Quorum
    assert ri.serial_consistency == SerialConsistency.Serial
    assert ri.keyspace == "test_lb_ks"
    assert ri.table == table
    assert ri.is_confirmed_lwt is True
    assert ri.token is not None
    assert ri.preferred_datacenter is None
    assert ri.preferred_rack is None
