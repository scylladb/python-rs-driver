from typing import Any, cast

import pytest
from helpers.ddl import ddl
from helpers.session import connect
from scylla.cluster.metadata import CqlColumnType, CqlText
from scylla.enums import Consistency, SerialConsistency
from scylla.errors import LoadBalancingPolicyError, PrepareError, StatementConfigError, StatementConversionError
from scylla.execution_profile import ExecutionProfile
from scylla.policies.load_balancing import DefaultPolicy
from scylla.policies.retry_policy import DefaultRetryPolicy
from scylla.session_builder import SessionBuilder
from scylla.statement import PreparedStatement, Statement
from scylla.types import Unset


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepare_statement_with_str():
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()
    prepared = await session.prepare("SELECT * FROM system.local")
    print(prepared)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepare_statement_with_statement():
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()
    statement = Statement("SELECT * FROM system.local")
    assert isinstance(statement, Statement)
    prepared = await session.prepare(statement)
    print(prepared)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepare_and_execute():
    session = await connect()
    query_str = "SELECT cluster_name FROM system.local"
    prepare_with_statement = await session.prepare(Statement(query_str))
    prepared_with_str = await session.prepare(query_str)
    assert isinstance(prepared_with_str, PreparedStatement)
    assert isinstance(prepare_with_statement, PreparedStatement)
    result_str = await session.execute(prepared_with_str)
    result_statement = await session.execute(prepare_with_statement)

    row_str = await result_str.first_row()
    row_statement = await result_statement.first_row()
    assert row_str is not None
    assert row_statement is not None
    assert row_str["cluster_name"] == row_statement["cluster_name"]


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepare_and_str():
    session = await connect()
    query_str = "SELECT cluster_name FROM system.local;"
    statement = Statement(query_str)
    prepared = await session.prepare(query_str)
    result_prepared = await session.execute(prepared)
    result_statement = await session.execute(statement)
    result_str = await session.execute(query_str)

    row_str = await result_str.first_row()
    row_prepared = await result_prepared.first_row()
    row_statement = await result_statement.first_row()

    assert row_str is not None
    assert row_prepared is not None
    assert row_statement is not None

    cluster_name_str = row_str["cluster_name"]
    assert row_prepared["cluster_name"] == cluster_name_str
    assert cluster_name_str == row_statement["cluster_name"]


def test_statement_with_page_size():
    query_str = "SELECT cluster_name FROM system.local;"
    statement = Statement(query_str)

    expected_page_size = 500
    statement = statement.with_page_size(expected_page_size)

    actual_page_size = statement.page_size

    assert isinstance(actual_page_size, int)
    assert actual_page_size == expected_page_size


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_statement_query_id():
    builder = SessionBuilder().contact_points([("127.0.0.2", 9042)])
    session = await builder.connect()

    prepared = await session.prepare("SELECT cluster_name FROM system.local WHERE key = ?")

    assert isinstance(prepared.query_id, bytes)
    assert len(prepared.query_id) > 0

    reprepared = await session.prepare("SELECT cluster_name FROM system.local WHERE key = ?")
    assert reprepared.query_id == prepared.query_id


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_statement_partition_key_indexes():
    builder = SessionBuilder().contact_points([("127.0.0.2", 9042)])
    session = await builder.connect()

    await ddl(
        session,
        "CREATE KEYSPACE IF NOT EXISTS stmt_pk_test_ks "
        "WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': '1'}",
    )
    await ddl(
        session, "CREATE TABLE IF NOT EXISTS stmt_pk_test_ks.t (p1 int, p2 int, c int, PRIMARY KEY ((p1, p2), c))"
    )

    try:
        prepared = await session.prepare("SELECT c FROM stmt_pk_test_ks.t WHERE p1 = ? AND p2 = ?")
        assert prepared.partition_key_indexes == (0, 1)

        prepared = await session.prepare("SELECT c FROM stmt_pk_test_ks.t WHERE p2 = ? AND p1 = ?")
        assert prepared.partition_key_indexes == (1, 0)

        prepared = await session.prepare(
            "SELECT c FROM stmt_pk_test_ks.t WHERE c = ? AND p2 = ? AND p1 = ? ALLOW FILTERING"
        )
        assert prepared.partition_key_indexes == (2, 1)

        prepared = await session.prepare("SELECT c FROM stmt_pk_test_ks.t WHERE p1 = ? ALLOW FILTERING")
        assert prepared.partition_key_indexes == ()
    finally:
        await ddl(session, "DROP KEYSPACE IF EXISTS stmt_pk_test_ks")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_statement_bind_columns():
    builder = SessionBuilder().contact_points([("127.0.0.2", 9042)])
    session = await builder.connect()

    prepared = await session.prepare("SELECT cluster_name FROM system.local WHERE key = ?")

    assert len(prepared.bind_columns) == 1

    bind_col = prepared.bind_columns[0]

    assert bind_col.name == "key"
    assert bind_col.table_name == "local"
    assert bind_col.keyspace_name == "system"
    assert isinstance(bind_col.cql_type, CqlColumnType)
    assert isinstance(bind_col.cql_type, CqlText)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_statement_result_columns():
    builder = SessionBuilder().contact_points([("127.0.0.2", 9042)])
    session = await builder.connect()

    prepared = await session.prepare("SELECT cluster_name FROM system.local WHERE key = ?")

    assert len(prepared.result_columns) == 1

    result_col = prepared.result_columns[0]

    assert result_col.name == "cluster_name"
    assert result_col.table_name == "local"
    assert result_col.keyspace_name == "system"
    assert isinstance(result_col.cql_type, CqlColumnType)
    assert isinstance(result_col.cql_type, CqlText)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_statement_result_columns_cached_until_schema_change():
    builder = SessionBuilder().contact_points([("127.0.0.2", 9042)])
    session = await builder.connect()

    await ddl(
        session,
        "CREATE KEYSPACE IF NOT EXISTS stmt_result_cols_test_ks "
        "WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': '1'}",
    )
    await ddl(session, "CREATE TABLE IF NOT EXISTS stmt_result_cols_test_ks.t (id int PRIMARY KEY, a int)")

    try:
        prepared = await session.prepare("SELECT * FROM stmt_result_cols_test_ks.t WHERE id = ?")
        await session.execute(prepared, [1])

        result_columns = prepared.result_columns
        assert len(result_columns) == 2
        assert prepared.result_columns is result_columns

        await ddl(session, "ALTER TABLE stmt_result_cols_test_ks.t ADD b int")
        await session.execute(prepared, [1])

        new_result_columns = prepared.result_columns
        assert new_result_columns is not result_columns
        assert len(new_result_columns) == 3
        assert [c.name for c in new_result_columns] == ["id", "a", "b"]

        assert prepared.result_columns is new_result_columns
    finally:
        await ddl(session, "DROP KEYSPACE IF EXISTS stmt_result_cols_test_ks")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepare_prepared_statement_raises_session_query_error():
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()

    prepared = await session.prepare("SELECT * FROM system.local")

    with pytest.raises(PrepareError) as exc_info:
        await session.prepare(cast(Any, prepared))

    assert "cannot prepare a preparedstatement" in str(exc_info.value).lower()


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepare_invalid_query_raises_session_query_error():
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()

    with pytest.raises(PrepareError) as exc_info:
        await session.prepare("THIS IS NOT CQL")

    assert "failed to prepare statement" in str(exc_info.value).lower()


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepare_invalid_statement_type_raises_statement_conversion_error():
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()

    with pytest.raises(StatementConversionError) as exc_info:
        await session.prepare(123)  # type: ignore[arg-type]

    assert "invalid statement type" in str(exc_info.value).lower()
    assert "expected a str, statement, or preparedstatement" in str(exc_info.value).lower()


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_execute_invalid_statement_type_raises_statement_conversion_error():
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()

    with pytest.raises(StatementConversionError) as exc_info:
        await session.execute(cast(Any, 123))

    assert "invalid statement type" in str(exc_info.value).lower()


def test_statement_timeout_too_large():
    query_str = "SELECT cluster_name FROM system.local;"
    statement = Statement(query_str)

    with pytest.raises(StatementConfigError) as exc_info:
        statement.with_request_timeout(1e30)

    assert "timeout must be a non-negative, finite number" in str(exc_info.value).lower()


def test_statement__negative_timeout():
    query_str = "SELECT cluster_name FROM system.local;"
    statement = Statement(query_str)

    with pytest.raises(StatementConfigError) as exc_info:
        statement.with_request_timeout(-1)

    assert "timeout must be a non-negative, finite number" in str(exc_info.value).lower()


def test_statement_with_request_timeout_not_finite():
    query_str = "SELECT cluster_name FROM system.local;"
    statement = Statement(query_str)

    with pytest.raises(StatementConfigError) as exc_info:
        statement.with_request_timeout(float("inf"))

    assert "timeout must be a non-negative, finite number" in str(exc_info.value).lower()


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_timeout_too_large():
    builder = SessionBuilder().contact_points(("127.0.0.2", 9042))
    session = await builder.connect()

    query_str = "SELECT cluster_name FROM system.local"
    prepared = await session.prepare(query_str)

    with pytest.raises(StatementConfigError) as exc_info:
        prepared.with_request_timeout(1e30)

    assert "timeout must be a non-negative, finite number" in str(exc_info.value).lower()


def test_statement_serial_consistency():
    query_str = "SELECT cluster_name FROM system.local;"
    statement = Statement(query_str)

    assert statement.serial_consistency is Unset

    statement = statement.with_serial_consistency(None)
    assert statement.serial_consistency is None

    statement = statement.with_serial_consistency(SerialConsistency.LocalSerial)
    assert isinstance(statement.serial_consistency, SerialConsistency)

    statement = statement.without_serial_consistency()
    assert statement.serial_consistency is Unset


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_serial_consistency():
    builder = SessionBuilder().contact_points(("127.0.0.2", 9042))
    session = await builder.connect()

    query_str = "SELECT cluster_name FROM system.local"
    prepared = await session.prepare(query_str)

    assert prepared.serial_consistency is Unset

    prepared = prepared.with_serial_consistency(None)
    assert prepared.serial_consistency is None

    prepared = prepared.with_serial_consistency(SerialConsistency.LocalSerial)
    assert isinstance(prepared.serial_consistency, SerialConsistency)

    prepared = prepared.without_serial_consistency()
    assert prepared.serial_consistency is Unset


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_statement_preserves_execution_profile_after_prepare():
    builder = SessionBuilder().contact_points(("127.0.0.2", 9042))
    session = await builder.connect()

    query_stmt = Statement("SELECT cluster_name FROM system.local").with_execution_profile(
        ExecutionProfile(timeout=12.234)
    )
    prepared = await session.prepare(query_stmt)

    prepared_ep = prepared.execution_profile
    assert prepared_ep is query_stmt.execution_profile
    assert prepared_ep is not None
    assert prepared_ep.request_timeout == 12.234


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_statement_preserves_settings_after_prepare():
    builder = SessionBuilder().contact_points(("127.0.0.2", 9042))
    session = await builder.connect()

    query_stmt = (
        Statement("SELECT cluster_name FROM system.local").with_page_size(500).with_consistency(Consistency.EachQuorum)
    )
    prepared = await session.prepare(query_stmt)

    prepared_ps = prepared.page_size
    assert prepared_ps == 500

    prepared_c = prepared.consistency
    assert prepared_c == Consistency.EachQuorum


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_statement_with_lb_policy_executes() -> None:
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()
    stmt = Statement("SELECT * FROM system.local").with_load_balancing_policy(DefaultPolicy())
    row = await (await session.execute(stmt)).first_row()
    assert row is not None


def test_statement_retry_policy_default():
    statement = Statement("SELECT * FROM system.local")

    assert statement.retry_policy is None


def test_statement_with_retry_policy():
    statement = Statement("SELECT * FROM system.local")
    policy = DefaultRetryPolicy()

    new_statement = statement.with_retry_policy(policy)

    assert statement.retry_policy is None
    assert new_statement.retry_policy is policy


def test_statement_without_retry_policy():
    policy = DefaultRetryPolicy()

    statement = Statement("SELECT * FROM system.local").with_retry_policy(policy)
    new_statement = statement.without_retry_policy()

    assert statement.retry_policy is policy
    assert new_statement.retry_policy is None


def test_statement_retry_policy_returns_same_object():
    policy = DefaultRetryPolicy()
    statement = Statement("SELECT * FROM system.local").with_retry_policy(policy)

    assert statement.retry_policy is policy


def test_statement_is_idempotent_default():
    statement = Statement("SELECT * FROM system.local")

    assert statement.is_idempotent is False


def test_statement_set_is_idempotent_true():
    statement = Statement("SELECT * FROM system.local")

    new_statement = statement.set_is_idempotent(True)

    assert statement.is_idempotent is False
    assert new_statement.is_idempotent is True


def test_statement_set_is_idempotent_false():
    statement = Statement("SELECT * FROM system.local").set_is_idempotent(True)

    new_statement = statement.set_is_idempotent(False)

    assert statement.is_idempotent is True
    assert new_statement.is_idempotent is False


def test_statement_set_is_idempotent_returns_new_instance():
    statement = Statement("SELECT * FROM system.local")
    new_statement = statement.set_is_idempotent(True)

    assert statement is not new_statement


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_statement_retry_policy_default():
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()
    prepared = await session.prepare("SELECT * FROM system.local")

    assert prepared.retry_policy is None


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_with_lb_policy_executes() -> None:
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()
    prepared = (await session.prepare("SELECT * FROM system.local")).with_load_balancing_policy(DefaultPolicy())
    row = await (await session.execute(prepared)).first_row()
    assert row is not None


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_statement_with_retry_policy():
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()
    prepared = await session.prepare("SELECT * FROM system.local")
    policy = DefaultRetryPolicy()

    new_prepared = prepared.with_retry_policy(policy)

    assert prepared.retry_policy is None
    assert new_prepared.retry_policy is policy


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_statement_without_lb_policy() -> None:
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()
    stmt = Statement("SELECT * FROM system.local").with_load_balancing_policy(DefaultPolicy())
    assert stmt.load_balancing_policy is not None
    stmt2 = stmt.without_load_balancing_policy()
    assert stmt2.load_balancing_policy is None
    assert await session.execute(stmt2) is not None


def test_statement_get_lb_policy_returns_original() -> None:
    stmt = Statement("SELECT * FROM system.local").with_load_balancing_policy(DefaultPolicy())
    assert stmt.load_balancing_policy is not None
    assert isinstance(stmt.load_balancing_policy, DefaultPolicy)


def test_statement_is_immutable_after_with_lb_policy() -> None:
    stmt = Statement("SELECT * FROM system.local")
    stmt2 = stmt.with_load_balancing_policy(DefaultPolicy())
    assert stmt.load_balancing_policy is None
    assert stmt2.load_balancing_policy is not None


def test_policy_without_pick_targets_raises_on_set() -> None:
    class NoPickTargets:
        pass

    with pytest.raises(LoadBalancingPolicyError):
        Statement("SELECT * FROM system.local").with_load_balancing_policy(NoPickTargets())  # type: ignore[arg-type]


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_statement_without_retry_policy():
    policy = DefaultRetryPolicy()

    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()
    prepared = await session.prepare("SELECT * FROM system.local")

    prepared = prepared.with_retry_policy(policy)
    new_prepared = prepared.without_retry_policy()

    assert prepared.retry_policy is policy
    assert new_prepared.retry_policy is None


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_statement_retry_policy_returns_same_object():
    policy = DefaultRetryPolicy()
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()
    prepared = await session.prepare("SELECT * FROM system.local")

    prepared = prepared.with_retry_policy(policy)

    assert prepared.retry_policy is policy


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_statement_is_idempotent_default():
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()
    prepared = await session.prepare("SELECT * FROM system.local")

    assert prepared.is_idempotent is False


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_statement_set_is_idempotent_true():
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()
    prepared = await session.prepare("SELECT * FROM system.local")

    new_prepared = prepared.set_is_idempotent(True)

    assert prepared.is_idempotent is False
    assert new_prepared.is_idempotent is True


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_statement_set_is_idempotent_false():
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()
    prepared = await session.prepare("SELECT * FROM system.local")

    prepared = prepared.set_is_idempotent(True)

    new_prepared = prepared.set_is_idempotent(False)

    assert prepared.is_idempotent is True
    assert new_prepared.is_idempotent is False


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_statement_set_is_idempotent_returns_new_instance():
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()
    prepared = await session.prepare("SELECT * FROM system.local")

    new_prepared = prepared.set_is_idempotent(True)

    assert prepared is not new_prepared
