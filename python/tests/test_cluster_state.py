import asyncio
import ipaddress
import uuid
from collections.abc import AsyncGenerator
from types import MappingProxyType

import pytest
import pytest_asyncio
from helpers.ddl import ddl
from scylla.cluster import ClusterState, ColumnKind, Keyspace, Node, StrategyKind
from scylla.cql_types import (
    CqlCollectionType,
    CqlColumnType,
    CqlIntType,
    CqlListType,
    CqlMapType,
    CqlNativeType,
    CqlTextType,
    CqlTupleType,
)
from scylla.routing import ReplicaLocator, Shard, Token
from scylla.session import Session, SessionBuilder
from scylla.statement import Statement

KEYSPACE = "cs_test_ks"
TABLE = "cs_test_table"
TEST_PARTITION_KEY = 1  # Default partition key value for testing


async def set_up() -> Session:
    builder = SessionBuilder().contact_points(["127.0.0.2:9042"])
    session = await builder.connect()
    await ddl(
        session,
        f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{'class': 'NetworkTopologyStrategy', 'datacenter1': '1'}};
    """,
    )
    await ddl(
        session,
        f"""
        CREATE TABLE IF NOT EXISTS {KEYSPACE}.{TABLE}
        (id int PRIMARY KEY, name text);
    """,
    )

    # Execute some queries to initialize replica locators for tablets
    # This is needed because replica locators are lazily initialized for tablet-enabled tables
    insert_stmt = Statement(f"INSERT INTO {KEYSPACE}.{TABLE} (id, name) VALUES (?, ?)")
    p = await session.prepare(insert_stmt)

    warm_up = [session.execute(p, (i % 6, f"test{i % 6}")) for i in range(600)]
    await asyncio.gather(*warm_up)

    return session


@pytest_asyncio.fixture(scope="module")
async def session() -> AsyncGenerator[Session, None]:
    s = await set_up()
    yield s
    await ddl(s, f"DROP KEYSPACE IF EXISTS {KEYSPACE}")


@pytest_asyncio.fixture(scope="module")
async def cluster_state(session: Session) -> ClusterState:
    return session.cluster_state


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_cluster_state_type(cluster_state: ClusterState) -> None:
    assert isinstance(cluster_state, ClusterState)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_get_keyspaces_returns(cluster_state: ClusterState) -> None:
    ks = cluster_state.keyspaces
    assert isinstance(ks, MappingProxyType)
    assert "system" in ks
    assert isinstance(ks["system"], Keyspace)
    assert KEYSPACE in ks
    assert isinstance(ks[KEYSPACE], Keyspace)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_get_keyspace(cluster_state: ClusterState) -> None:
    assert isinstance(cluster_state.get_keyspace("system"), Keyspace)
    assert cluster_state.get_keyspace(KEYSPACE) is cluster_state.keyspaces[KEYSPACE]
    assert cluster_state.get_keyspace("no_such_keyspace_xyz") is None


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_keyspace_tables(cluster_state: ClusterState) -> None:
    ks = cluster_state.get_keyspace(KEYSPACE)
    assert ks is not None
    assert isinstance(ks.tables, MappingProxyType)
    assert TABLE in ks.tables


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_keyspace_views(cluster_state: ClusterState) -> None:
    ks = cluster_state.get_keyspace(KEYSPACE)
    assert ks is not None
    assert isinstance(ks.views, MappingProxyType)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_keyspace_strategy(cluster_state: ClusterState) -> None:
    ks = cluster_state.get_keyspace("system")
    assert ks is not None
    assert ks.strategy.kind == StrategyKind.Local


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_table_partition_key(cluster_state: ClusterState) -> None:
    ks = cluster_state.get_keyspace(KEYSPACE)
    assert ks is not None
    pk = ks.tables[TABLE].partition_key
    assert isinstance(pk, MappingProxyType)
    pk_typ = next(iter(pk.values())).typ
    assert isinstance(pk_typ, CqlIntType)
    assert "id" in pk


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_table_columns_has_expected_names(cluster_state: ClusterState) -> None:
    ks = cluster_state.get_keyspace(KEYSPACE)
    assert ks is not None
    cols = ks.tables[TABLE].columns
    assert isinstance(cols, MappingProxyType)
    assert "id" in cols
    assert "name" in cols


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_column_kind_partition_key(cluster_state: ClusterState) -> None:
    ks = cluster_state.get_keyspace(KEYSPACE)
    assert ks is not None
    assert ks.tables[TABLE].columns["id"].kind == ColumnKind.PartitionKey


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_node_type(cluster_state: ClusterState) -> None:
    assert isinstance(cluster_state.nodes_info, MappingProxyType)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_node_host_id_is_uuid(cluster_state: ClusterState) -> None:
    assert isinstance(next(iter(cluster_state.nodes_info.items()))[0], uuid.UUID)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_node_host_address(cluster_state: ClusterState) -> None:
    assert isinstance(next(iter(cluster_state.nodes_info.items()))[1].address[0], ipaddress.IPv4Address)
    assert isinstance(next(iter(cluster_state.nodes_info.items()))[1].address[1], int)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_compute_token_returns_token(cluster_state: ClusterState) -> None:
    token = cluster_state.compute_token(KEYSPACE, TABLE, {"id": 42})
    assert isinstance(token, Token)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_replica_locator_type(cluster_state: ClusterState) -> None:
    assert isinstance(cluster_state.replica_locator, ReplicaLocator)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_replica_locator_unique_nodes_in_global_ring(cluster_state: ClusterState) -> None:
    nodes = cluster_state.replica_locator.unique_token_owning_nodes_in_cluster()
    assert isinstance(nodes, list)
    assert len(nodes) > 0
    assert all(isinstance(n, Node) for n in nodes)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_replica_locator_primary_replica(cluster_state: ClusterState) -> None:
    ks = cluster_state.get_keyspace(KEYSPACE)
    assert ks is not None
    token = cluster_state.compute_token(KEYSPACE, TABLE, (TEST_PARTITION_KEY,))
    replica = cluster_state.replica_locator.primary_replica_for_token(token, ks.strategy, KEYSPACE, TABLE)
    assert replica is not None
    node, shard = replica
    assert isinstance(node, Node)
    assert isinstance(shard, Shard)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_replica_locator_primary_replica_with_dc(cluster_state: ClusterState) -> None:
    ks = cluster_state.get_keyspace(KEYSPACE)
    assert ks is not None
    token = cluster_state.compute_token(KEYSPACE, TABLE, (TEST_PARTITION_KEY,))
    dc = cluster_state.replica_locator.datacenter_names[0]
    replica = cluster_state.replica_locator.primary_replica_for_token(
        token, ks.strategy, KEYSPACE, TABLE, datacenter=dc
    )
    assert replica is not None
    node, shard = replica
    assert isinstance(node, Node)
    assert isinstance(shard, Shard)
    assert node.datacenter == dc


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_replica_locator_all_replicas_with_dc(cluster_state: ClusterState) -> None:
    ks = cluster_state.get_keyspace(KEYSPACE)
    assert ks is not None
    token = cluster_state.compute_token(KEYSPACE, TABLE, (TEST_PARTITION_KEY,))
    dc = cluster_state.replica_locator.datacenter_names[0]
    replicas = cluster_state.replica_locator.all_replicas_for_token(token, ks.strategy, KEYSPACE, TABLE, datacenter=dc)
    assert replicas is not None
    assert len(replicas) > 0
    for node, shard in replicas:
        assert isinstance(node, Node)
        assert isinstance(shard, Shard)
        assert node.datacenter == dc


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_replica_locator_nodes_in_dc(cluster_state: ClusterState) -> None:
    dc = cluster_state.replica_locator.datacenter_names[0]
    nodes = cluster_state.replica_locator.unique_token_owning_nodes_in_datacenter(dc)
    assert nodes is not None
    assert len(nodes) > 0
    for node in nodes:
        assert isinstance(node, Node)
        assert node.datacenter == dc


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_replica_set_iteration(cluster_state: ClusterState) -> None:
    ks = cluster_state.get_keyspace(KEYSPACE)
    assert ks is not None
    token = cluster_state.compute_token(KEYSPACE, TABLE, (TEST_PARTITION_KEY,))
    replicas = cluster_state.replica_locator.all_replicas_for_token(token, ks.strategy, KEYSPACE, TABLE)
    all_nodes = cluster_state.nodes_info
    assert isinstance(replicas, list)
    assert len(replicas) > 0
    for node, shard in replicas:
        info_node = all_nodes.get(node.host_id)
        assert info_node is node
        assert isinstance(node, Node)
        assert isinstance(shard, int)
        assert isinstance(shard, Shard)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_get_token_endpoints_matches_replica_locator(
    cluster_state: ClusterState,
) -> None:
    ks = cluster_state.get_keyspace(KEYSPACE)
    assert ks is not None
    token = cluster_state.compute_token(KEYSPACE, TABLE, [TEST_PARTITION_KEY])
    replicas_from_locator = cluster_state.replica_locator.all_replicas_for_token(token, ks.strategy, KEYSPACE, TABLE)
    replicas_from_get_token = cluster_state.get_token_endpoints(KEYSPACE, TABLE, token)
    assert len(replicas_from_locator) > 0
    assert len(replicas_from_get_token) > 0
    assert replicas_from_get_token == replicas_from_locator


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_node_identity(cluster_state: ClusterState) -> None:
    # Get all nodes from cluster state
    node_info = list(cluster_state.nodes_info.values())

    # Get replicas for a token
    replicas = cluster_state.get_endpoints(KEYSPACE, TABLE, [TEST_PARTITION_KEY])
    assert len(replicas) > 0

    # Find a node from the replica list and check for object identity
    found = False
    for node_from_replica, _ in replicas:
        for node_from_info in node_info:
            if node_from_replica.host_id == node_from_info.host_id:
                assert node_from_replica is node_from_info
                found = True
                break
    assert found, "Node from cluster_state.nodes_info not found in replica set"


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_replica_locator_primary_replica_invalid_dc(
    cluster_state: ClusterState,
) -> None:
    ks = cluster_state.get_keyspace(KEYSPACE)
    assert ks is not None
    token = cluster_state.compute_token(KEYSPACE, TABLE, (TEST_PARTITION_KEY,))
    replica = cluster_state.replica_locator.primary_replica_for_token(
        token, ks.strategy, KEYSPACE, TABLE, datacenter="invalid_dc"
    )
    assert replica is None


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_replica_locator_nodes_in_invalid_dc(cluster_state: ClusterState) -> None:
    nodes = cluster_state.replica_locator.unique_token_owning_nodes_in_datacenter("invalid_dc")
    assert nodes is None


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_replica_locator_all_replicas_invalid_dc(
    cluster_state: ClusterState,
) -> None:
    ks = cluster_state.get_keyspace(KEYSPACE)
    assert ks is not None
    token = cluster_state.compute_token(KEYSPACE, TABLE, (TEST_PARTITION_KEY,))
    replicas = cluster_state.replica_locator.all_replicas_for_token(
        token, ks.strategy, KEYSPACE, TABLE, datacenter="invalid_dc"
    )
    assert replicas == []


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_token_value_property(cluster_state: ClusterState) -> None:
    token = cluster_state.compute_token(KEYSPACE, TABLE, [TEST_PARTITION_KEY])
    assert hasattr(token, "value")
    assert isinstance(token.value, int)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_token_equality(cluster_state: ClusterState) -> None:
    token1 = cluster_state.compute_token(KEYSPACE, TABLE, [TEST_PARTITION_KEY])
    token2 = cluster_state.compute_token(KEYSPACE, TABLE, [TEST_PARTITION_KEY])
    assert token1 == token2
    assert token1.value == token2.value


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_token_inequality(cluster_state: ClusterState) -> None:
    token1 = cluster_state.compute_token(KEYSPACE, TABLE, [TEST_PARTITION_KEY])
    token2 = cluster_state.compute_token(KEYSPACE, TABLE, [2])
    assert token1 != token2
    assert token1.value != token2.value


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_token_hashable(cluster_state: ClusterState) -> None:
    token = cluster_state.compute_token(KEYSPACE, TABLE, [TEST_PARTITION_KEY])
    token_set = {token}
    assert token in token_set
    token_dict = {token: "value"}
    assert token_dict[token] == "value"


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_node_has_required_attributes(cluster_state: ClusterState) -> None:
    nodes = list(cluster_state.nodes_info.values())
    assert len(nodes) > 0
    node = nodes[0]
    assert hasattr(node, "host_id")
    assert hasattr(node, "address")
    assert hasattr(node, "datacenter")
    assert isinstance(node.host_id, uuid.UUID)
    assert isinstance(node.address, tuple)
    assert len(node.address) == 2
    assert isinstance(node.datacenter, str)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_compute_token_with_different_types(cluster_state: ClusterState) -> None:
    token1 = cluster_state.compute_token(KEYSPACE, TABLE, [TEST_PARTITION_KEY])
    token2 = cluster_state.compute_token(KEYSPACE, TABLE, (TEST_PARTITION_KEY,))
    token3 = cluster_state.compute_token(KEYSPACE, TABLE, {"id": TEST_PARTITION_KEY})
    assert token1 == token2 == token3


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_replicas_are_from_cluster_nodes(cluster_state: ClusterState) -> None:
    all_node_ids = set(cluster_state.nodes_info.keys())
    token = cluster_state.compute_token(KEYSPACE, TABLE, [TEST_PARTITION_KEY])
    replicas = cluster_state.get_token_endpoints(KEYSPACE, TABLE, token)

    for node, shard in replicas:
        assert node.host_id in all_node_ids
        assert 0 <= shard < 2**32


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_primary_replica_is_in_all_replicas(cluster_state: ClusterState) -> None:
    ks = cluster_state.get_keyspace(KEYSPACE)
    assert ks is not None
    token = cluster_state.compute_token(KEYSPACE, TABLE, [TEST_PARTITION_KEY])

    primary = cluster_state.replica_locator.primary_replica_for_token(token, ks.strategy, KEYSPACE, TABLE)
    all_replicas = cluster_state.replica_locator.all_replicas_for_token(token, ks.strategy, KEYSPACE, TABLE)

    assert primary is not None
    assert len(all_replicas) > 0
    assert primary in all_replicas


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_keyspace_immutability(cluster_state: ClusterState) -> None:
    keyspaces = cluster_state.keyspaces
    assert isinstance(keyspaces, MappingProxyType)
    try:
        keyspaces["test"] = None  # type: ignore
        assert False, "Should not be able to modify keyspaces"
    except TypeError:
        pass


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_table_columns_immutability(cluster_state: ClusterState) -> None:
    ks = cluster_state.get_keyspace(KEYSPACE)
    assert ks is not None
    table = ks.tables[TABLE]
    columns = table.columns
    assert isinstance(columns, MappingProxyType)
    try:
        columns["test"] = None  # type: ignore
        assert False, "Should not be able to modify columns"
    except TypeError:
        pass


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_multiple_tokens_different_replicas(cluster_state: ClusterState) -> None:
    ks = cluster_state.get_keyspace(KEYSPACE)
    assert ks is not None
    token_replica_pairs: list[tuple[Token, list[tuple[Node, Shard]]]] = []
    for i in range(1, 6):
        token = cluster_state.compute_token(KEYSPACE, TABLE, [i])
        replicas = cluster_state.replica_locator.all_replicas_for_token(token, ks.strategy, KEYSPACE, TABLE)
        token_replica_pairs.append((token, replicas))

    for token, replicas in token_replica_pairs:
        assert len(replicas) > 0
        for node, shard in replicas:
            assert isinstance(node, Node)
            assert isinstance(shard, Shard)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_complex_column_type(session: Session) -> None:
    table_name = "complex_column_type_table"
    await ddl(
        session,
        f"""
        CREATE TABLE IF NOT EXISTS {KEYSPACE}.{table_name}
        (
            id int PRIMARY KEY,
            complex_column map<frozen<list<tuple<text, int>>>, int>
        );
    """,
    )
    cs = session.cluster_state
    keyspace = cs.get_keyspace(KEYSPACE)
    assert keyspace is not None
    table = keyspace.tables.get(table_name)
    assert table is not None
    complex_col = table.columns.get("complex_column")
    assert complex_col is not None
    assert isinstance(complex_col.typ, CqlColumnType)
    assert isinstance(complex_col.typ, CqlCollectionType)
    assert isinstance(complex_col.typ, CqlMapType)
    assert isinstance(complex_col.typ.key_type, CqlColumnType)
    assert isinstance(complex_col.typ.key_type, CqlCollectionType)
    assert isinstance(complex_col.typ.key_type, CqlListType)
    assert complex_col.typ.key_type.frozen
    assert isinstance(complex_col.typ.value_type, CqlColumnType)
    assert isinstance(complex_col.typ.value_type, CqlNativeType)
    assert isinstance(complex_col.typ.value_type, CqlIntType)
    assert isinstance(complex_col.typ.key_type.column_type, CqlColumnType)
    assert isinstance(complex_col.typ.key_type.column_type, CqlTupleType)
    assert isinstance(complex_col.typ.key_type.column_type.element_types[0], CqlColumnType)
    assert isinstance(complex_col.typ.key_type.column_type.element_types[0], CqlNativeType)
    assert isinstance(complex_col.typ.key_type.column_type.element_types[0], CqlTextType)
    assert isinstance(complex_col.typ.key_type.column_type.element_types[1], CqlColumnType)
    assert isinstance(complex_col.typ.key_type.column_type.element_types[1], CqlNativeType)
    assert isinstance(complex_col.typ.key_type.column_type.element_types[1], CqlIntType)
