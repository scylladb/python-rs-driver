from typing import Any, cast

import pytest
from helpers.ddl import ddl
from scylla.cql_types import CqlColumnType, CqlText
from scylla.errors import LoadBalancingPolicyError, PrepareError, StatementConfigError, StatementConversionError
from scylla.policies.load_balancing import DefaultPolicy
from scylla.policies.retry import DefaultRetryPolicy
from scylla.session import ExecutionProfile, SessionBuilder
from scylla.statement import UNSET, Consistency, PreparedStatement, SerialConsistency, Statement


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
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()
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
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()
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


def test_statement_set_and_get_page_size():
    query_str = "SELECT cluster_name FROM system.local;"
    statement = Statement(query_str)

    expected_page_size = 500
    statement.page_size = expected_page_size

    actual_page_size = statement.page_size

    assert isinstance(actual_page_size, int)
    assert actual_page_size == expected_page_size


@pytest.mark.parametrize("page_size", [0, -1])
def test_statement_non_positive_page_size_raises(page_size: int):
    statement = Statement("SELECT cluster_name FROM system.local;")

    with pytest.raises(StatementConfigError) as exc_info:
        statement.page_size = page_size

    assert "page size must be positive" in str(exc_info.value).lower()


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
        statement.request_timeout = 1e30

    assert "timeout must be a non-negative, finite number" in str(exc_info.value).lower()


def test_statement__negative_timeout():
    query_str = "SELECT cluster_name FROM system.local;"
    statement = Statement(query_str)

    with pytest.raises(StatementConfigError) as exc_info:
        statement.request_timeout = -1

    assert "timeout must be a non-negative, finite number" in str(exc_info.value).lower()


def test_statement_set_request_timeout_not_finite():
    query_str = "SELECT cluster_name FROM system.local;"
    statement = Statement(query_str)

    with pytest.raises(StatementConfigError) as exc_info:
        statement.request_timeout = float("inf")

    assert "timeout must be a non-negative, finite number" in str(exc_info.value).lower()


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_timeout_too_large():
    builder = SessionBuilder().contact_points(("127.0.0.2", 9042))
    session = await builder.connect()

    query_str = "SELECT cluster_name FROM system.local"
    prepared = await session.prepare(query_str)

    with pytest.raises(StatementConfigError) as exc_info:
        prepared.request_timeout = 1e30

    assert "timeout must be a non-negative, finite number" in str(exc_info.value).lower()


def test_statement_serial_consistency():
    query_str = "SELECT cluster_name FROM system.local;"
    statement = Statement(query_str)

    assert statement.serial_consistency is UNSET

    statement.serial_consistency = None
    assert statement.serial_consistency is None

    statement.serial_consistency = SerialConsistency.LocalSerial
    assert isinstance(statement.serial_consistency, SerialConsistency)

    statement.serial_consistency = UNSET
    assert statement.serial_consistency is UNSET


def test_statement_consistency():
    statement = Statement("SELECT cluster_name FROM system.local;")

    assert statement.consistency is UNSET

    statement.consistency = Consistency.Quorum
    assert statement.consistency == Consistency.Quorum

    statement.consistency = UNSET
    assert statement.consistency is UNSET


def test_statement_request_timeout():
    statement = Statement("SELECT cluster_name FROM system.local;")

    assert statement.request_timeout is UNSET

    statement.request_timeout = None
    assert statement.request_timeout is None

    statement.request_timeout = 2.5
    assert statement.request_timeout == 2.5

    statement.request_timeout = UNSET
    assert statement.request_timeout is UNSET


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_serial_consistency():
    builder = SessionBuilder().contact_points(("127.0.0.2", 9042))
    session = await builder.connect()

    query_str = "SELECT cluster_name FROM system.local"
    prepared = await session.prepare(query_str)

    assert prepared.serial_consistency is UNSET

    prepared.serial_consistency = None
    assert prepared.serial_consistency is None

    prepared.serial_consistency = SerialConsistency.LocalSerial
    assert isinstance(prepared.serial_consistency, SerialConsistency)

    prepared.serial_consistency = UNSET
    assert prepared.serial_consistency is UNSET


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_statement_preserves_execution_profile_after_prepare():
    builder = SessionBuilder().contact_points(("127.0.0.2", 9042))
    session = await builder.connect()

    query_stmt = Statement("SELECT cluster_name FROM system.local")
    query_stmt.execution_profile = ExecutionProfile(timeout=12.234)
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

    query_stmt = Statement("SELECT cluster_name FROM system.local")
    query_stmt.page_size = 500
    query_stmt.consistency = Consistency.EachQuorum
    prepared = await session.prepare(query_stmt)

    prepared_ps = prepared.page_size
    assert prepared_ps == 500

    prepared_c = prepared.consistency
    assert prepared_c == Consistency.EachQuorum


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_statement_preserves_explicit_none_serial_consistency_after_prepare():
    session = await SessionBuilder().contact_points(("127.0.0.2", 9042)).connect()

    query_stmt = Statement("SELECT cluster_name FROM system.local")
    query_stmt.serial_consistency = None
    prepared = await session.prepare(query_stmt)

    assert prepared.serial_consistency is None


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_statement_set_lb_policy_executes() -> None:
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()
    stmt = Statement("SELECT * FROM system.local")
    stmt.load_balancing_policy = DefaultPolicy()
    row = await (await session.execute(stmt)).first_row()
    assert row is not None


def test_statement_retry_policy_default():
    statement = Statement("SELECT * FROM system.local")

    assert statement.retry_policy is None


def test_statement_set_retry_policy():
    statement = Statement("SELECT * FROM system.local")
    policy = DefaultRetryPolicy()

    statement.retry_policy = policy

    assert statement.retry_policy is policy


def test_statement_clear_retry_policy():
    statement = Statement("SELECT * FROM system.local")
    statement.retry_policy = DefaultRetryPolicy()

    statement.retry_policy = None

    assert statement.retry_policy is None


def test_statement_is_idempotent_default():
    statement = Statement("SELECT * FROM system.local")

    assert statement.is_idempotent is False


def test_statement_set_is_idempotent_true():
    statement = Statement("SELECT * FROM system.local")

    statement.is_idempotent = True

    assert statement.is_idempotent is True


def test_statement_set_is_idempotent_false():
    statement = Statement("SELECT * FROM system.local")
    statement.is_idempotent = True

    statement.is_idempotent = False

    assert statement.is_idempotent is False


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_statement_retry_policy_default():
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()
    prepared = await session.prepare("SELECT * FROM system.local")

    assert prepared.retry_policy is None


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_set_lb_policy_executes() -> None:
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()
    prepared = await session.prepare("SELECT * FROM system.local")
    prepared.load_balancing_policy = DefaultPolicy()
    row = await (await session.execute(prepared)).first_row()
    assert row is not None


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_statement_set_retry_policy():
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()
    prepared = await session.prepare("SELECT * FROM system.local")
    policy = DefaultRetryPolicy()

    prepared.retry_policy = policy

    assert prepared.retry_policy is policy


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_statement_clear_lb_policy() -> None:
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()
    stmt = Statement("SELECT * FROM system.local")
    stmt.load_balancing_policy = DefaultPolicy()
    assert stmt.load_balancing_policy is not None
    stmt.load_balancing_policy = None
    assert stmt.load_balancing_policy is None
    assert await session.execute(stmt) is not None


def test_statement_get_lb_policy_returns_original() -> None:
    stmt = Statement("SELECT * FROM system.local")
    stmt.load_balancing_policy = DefaultPolicy()
    assert stmt.load_balancing_policy is not None
    assert isinstance(stmt.load_balancing_policy, DefaultPolicy)


def test_statement_lb_policy_set_in_place() -> None:
    stmt = Statement("SELECT * FROM system.local")
    assert stmt.load_balancing_policy is None
    stmt.load_balancing_policy = DefaultPolicy()
    assert stmt.load_balancing_policy is not None


def test_policy_without_pick_targets_raises_on_set() -> None:
    class NoPickTargets:
        pass

    stmt = Statement("SELECT * FROM system.local")
    with pytest.raises(LoadBalancingPolicyError):
        stmt.load_balancing_policy = NoPickTargets()  # type: ignore[assignment]
    assert stmt.load_balancing_policy is None


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_statement_clear_retry_policy():
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()
    prepared = await session.prepare("SELECT * FROM system.local")
    prepared.retry_policy = DefaultRetryPolicy()

    prepared.retry_policy = None

    assert prepared.retry_policy is None


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

    prepared.is_idempotent = True

    assert prepared.is_idempotent is True


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_statement_set_is_idempotent_false():
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()
    prepared = await session.prepare("SELECT * FROM system.local")
    prepared.is_idempotent = True

    prepared.is_idempotent = False

    assert prepared.is_idempotent is False
