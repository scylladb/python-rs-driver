import pytest
from scylla.errors import StatementConfigError, Unavailable
from scylla.policies.load_balancing import DefaultPolicy
from scylla.policies.retry import DefaultRetryPolicy
from scylla.session import ExecutionProfile, SessionBuilder
from scylla.statement import UNSET, Consistency, SerialConsistency, Statement


def test_execution_profile_builder():
    profile = ExecutionProfile()
    assert isinstance(profile, ExecutionProfile)


def test_execution_profile_negative_timeout():
    with pytest.raises(StatementConfigError) as exc_info:
        ExecutionProfile(timeout=-1.0)

    assert "timeout must be a non-negative, finite number" in str(exc_info.value)


def test_execution_profile_nan_timeout():
    with pytest.raises(StatementConfigError) as exc_info:
        ExecutionProfile(timeout=float("nan"))

    assert "timeout must be a non-negative, finite number" in str(exc_info.value)


def test_execution_profile_infinity_timeout():
    with pytest.raises(StatementConfigError) as exc_info:
        ExecutionProfile(timeout=float("inf"))

    assert "timeout must be a non-negative, finite number" in str(exc_info.value)


def test_execution_profile_builder_consistency():
    expected_consistency = Consistency.One
    profile = ExecutionProfile(consistency=expected_consistency)
    assert isinstance(profile, ExecutionProfile)
    actual_consistency = profile.consistency
    assert actual_consistency != Consistency.Two
    assert actual_consistency == expected_consistency


def test_execution_profile_builder_serial_consistency():
    expected_serial_consistency = SerialConsistency.Serial
    profile = ExecutionProfile(serial_consistency=expected_serial_consistency)
    assert isinstance(profile, ExecutionProfile)
    actual_serial_consistency = profile.serial_consistency
    assert actual_serial_consistency != SerialConsistency.LocalSerial
    assert actual_serial_consistency == expected_serial_consistency


def test_execution_profile_timeout():
    expected_timeout = 10.5
    profile = ExecutionProfile(timeout=expected_timeout)
    assert isinstance(profile, ExecutionProfile)
    actual_timeout = profile.request_timeout
    assert actual_timeout == expected_timeout


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_create_session_with_profile():
    expected_timeout = 10.5
    expected_consistency = Consistency.All
    profile = ExecutionProfile(timeout=expected_timeout, consistency=expected_consistency)
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).execution_profile(profile).connect()
    result = await session.execute("SELECT * FROM system.local")
    print(result)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_invalid_consistency_for_query():
    profile = ExecutionProfile(consistency=Consistency.Three)
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).execution_profile(profile).connect()
    with pytest.raises(Unavailable) as exc_info:
        _ = await session.execute("SELECT * FROM system.local")
    assert exc_info.value.consistency == Consistency.Three
    assert exc_info.value.required == 3
    assert exc_info.value.alive < exc_info.value.required
    assert exc_info.value.reason


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_invalid_consistency_for_prepared_statement():
    profile = ExecutionProfile(consistency=Consistency.Three)
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).execution_profile(profile).connect()
    prepared = await session.prepare("SELECT * FROM system.local")
    with pytest.raises(Unavailable) as exc_info:
        _ = await session.execute(prepared)
    assert exc_info.value.consistency == Consistency.Three


def test_statement_creation():
    query = "SELECT * FROM system.local"
    stmt = Statement(query)
    assert isinstance(stmt, Statement)
    assert stmt.contents == query


def test_statement_set_and_get_consistency():
    stmt = Statement("SELECT * FROM system.local")
    expected_consistency = Consistency.All
    stmt.consistency = expected_consistency

    actual_consistency = stmt.consistency
    assert isinstance(actual_consistency, Consistency)
    assert actual_consistency == expected_consistency


def test_statement_unset_consistency():
    stmt = Statement("SELECT * FROM system.local")
    stmt.consistency = Consistency.Quorum
    stmt.consistency = UNSET

    actual_consistency = stmt.consistency
    assert actual_consistency is UNSET


def test_statement_set_and_get_serial_consistency():
    stmt = Statement("SELECT * FROM system.local")
    expected_serial_consistency = SerialConsistency.LocalSerial
    stmt.serial_consistency = expected_serial_consistency

    actual_serial_consistency = stmt.serial_consistency
    assert isinstance(actual_serial_consistency, SerialConsistency)
    assert actual_serial_consistency == expected_serial_consistency


def test_statement_unset_serial_consistency():
    stmt = Statement("SELECT * FROM system.local")
    stmt.serial_consistency = SerialConsistency.Serial
    stmt.serial_consistency = UNSET

    actual_serial_consistency = stmt.serial_consistency
    assert actual_serial_consistency is UNSET


def test_statement_set_and_get_request_timeout():
    stmt = Statement("SELECT * FROM system.local")
    expected_timeout = 7.25
    stmt.request_timeout = expected_timeout

    actual_timeout = stmt.request_timeout
    assert isinstance(actual_timeout, float)
    assert actual_timeout == expected_timeout


def test_statement_with_timeout_set_to_none():
    stmt = Statement("SELECT * FROM system.local")
    stmt.request_timeout = None

    actual_timeout = stmt.request_timeout
    assert actual_timeout is None


def test_statement_unset_request_timeout():
    stmt = Statement("SELECT * FROM system.local")
    stmt.request_timeout = 10.0
    stmt.request_timeout = UNSET

    actual_timeout = stmt.request_timeout
    assert actual_timeout is UNSET


def test_statement_with_negative_timeout():
    stmt = Statement("SELECT * FROM system.local")
    with pytest.raises(StatementConfigError) as exc_info:
        stmt.request_timeout = -1.0
    assert "timeout must be a non-negative, finite number" in str(exc_info.value)


def test_statement_with_nan_timeout():
    stmt = Statement("SELECT * FROM system.local")
    with pytest.raises(StatementConfigError) as exc_info:
        stmt.request_timeout = float("nan")
    assert "timeout must be a non-negative, finite number" in str(exc_info.value)


def test_statement_with_infinity_timeout():
    stmt = Statement("SELECT * FROM system.local")
    with pytest.raises(StatementConfigError) as exc_info:
        stmt.request_timeout = float("inf")
    assert "timeout must be a non-negative, finite number" in str(exc_info.value)


def test_statement_invalid_timeout_keeps_previous_value():
    stmt = Statement("SELECT * FROM system.local")
    stmt.request_timeout = 3.0
    with pytest.raises(StatementConfigError):
        stmt.request_timeout = -1.0
    assert stmt.request_timeout == 3.0


def test_statement_set_and_get_execution_profile():
    stmt = Statement("SELECT * FROM system.local")
    expected_timeout = 2.5
    profile = ExecutionProfile(timeout=expected_timeout)
    stmt.execution_profile = profile

    actual_profile = stmt.execution_profile
    assert actual_profile is profile
    assert actual_profile is not None
    assert actual_profile.request_timeout == expected_timeout


def test_statement_clear_execution_profile():
    stmt = Statement("SELECT * FROM system.local")
    profile = ExecutionProfile(timeout=3.0)
    stmt.execution_profile = profile
    stmt.execution_profile = None

    actual_profile = stmt.execution_profile
    assert actual_profile is None


def test_statement_set_multiple_options():
    stmt = Statement("SELECT * FROM system.local")
    stmt.consistency = Consistency.Quorum
    stmt.serial_consistency = SerialConsistency.Serial
    stmt.request_timeout = 5.0

    assert stmt.consistency == Consistency.Quorum
    assert stmt.serial_consistency == SerialConsistency.Serial
    assert stmt.request_timeout == 5.0


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_invalid_consistency_for_statement():
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()
    stmt = Statement("SELECT * FROM system.local")
    stmt.consistency = Consistency.Three
    with pytest.raises(Unavailable) as exc_info:
        _ = await session.execute(stmt)
    assert exc_info.value.consistency == Consistency.Three


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_set_and_get_consistency():
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()

    prepared = await session.prepare("SELECT * FROM system.local")
    assert prepared.consistency is UNSET
    expected_consistency = Consistency.All
    prepared.consistency = expected_consistency

    actual_consistency = prepared.consistency
    assert isinstance(actual_consistency, Consistency)
    assert actual_consistency == expected_consistency


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_unset_consistency():
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()

    prepared = await session.prepare("SELECT * FROM system.local")
    prepared.consistency = Consistency.All
    prepared.consistency = UNSET

    actual_consistency = prepared.consistency
    assert actual_consistency is UNSET


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_set_and_get_execution_profile():
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()

    expected_timeout = 1.5
    prepared = await session.prepare("SELECT * FROM system.local")
    expected_profile = ExecutionProfile(timeout=expected_timeout)
    prepared.execution_profile = expected_profile

    actual_profile = prepared.execution_profile
    assert actual_profile is expected_profile
    assert actual_profile is not None
    assert actual_profile.request_timeout == expected_profile.request_timeout


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_clear_execution_profile():
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()

    expected_timeout = 1.5
    expected_profile = ExecutionProfile(timeout=expected_timeout)
    prepared = await session.prepare("SELECT * FROM system.local")
    prepared.execution_profile = expected_profile
    prepared.execution_profile = None

    actual_profile = prepared.execution_profile
    assert actual_profile is None


def test_statement_execution_profile_preserves_load_balancing_policy():
    policy = DefaultPolicy()
    profile = ExecutionProfile(load_balancing_policy=policy)
    stmt = Statement("SELECT * FROM system.local")
    stmt.execution_profile = profile

    actual_profile = stmt.execution_profile
    assert actual_profile is profile
    assert actual_profile is not None
    assert actual_profile.load_balancing_policy is policy


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_set_and_get_request_timeout():
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()

    prepared = await session.prepare("SELECT * FROM system.local")
    expected_timeout = 10.5
    prepared.request_timeout = expected_timeout

    actual_timeout = prepared.request_timeout
    assert isinstance(actual_timeout, float)
    assert actual_timeout == expected_timeout


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_unset_request_timeout():
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()

    prepared = await session.prepare("SELECT * FROM system.local")
    prepared.request_timeout = 10.5
    prepared.request_timeout = UNSET

    actual_timeout = prepared.request_timeout
    assert type(actual_timeout) is type(UNSET)
    assert actual_timeout is UNSET
    assert str(actual_timeout) == "UNSET"


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_with_negative_timeout():
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()

    prepared = await session.prepare("SELECT * FROM system.local")

    with pytest.raises(StatementConfigError) as exc_info:
        prepared.request_timeout = -1.0

    assert "timeout must be a non-negative, finite number" in str(exc_info.value)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_with_timeout_set_to_none():
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()

    prepared = await session.prepare("SELECT * FROM system.local")
    prepared.request_timeout = None

    actual_timeout = prepared.request_timeout
    assert actual_timeout is None


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_set_and_get_serial_consistency():
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()

    prepared = await session.prepare("SELECT * FROM system.local")
    expected_serial_consistency = SerialConsistency.Serial
    prepared.serial_consistency = expected_serial_consistency

    actual_serial_consistency = prepared.serial_consistency
    assert isinstance(actual_serial_consistency, SerialConsistency)
    assert actual_serial_consistency == expected_serial_consistency


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_unset_serial_consistency():
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()

    prepared = await session.prepare("SELECT * FROM system.local")
    prepared.serial_consistency = SerialConsistency.Serial
    prepared.serial_consistency = UNSET

    actual_serial_consistency = prepared.serial_consistency
    assert actual_serial_consistency is UNSET


def test_retry_policy_returns_same_object():
    policy = DefaultRetryPolicy()
    profile = ExecutionProfile(retry_policy=policy)

    assert profile.retry_policy is policy
