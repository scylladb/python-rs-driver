import logging
from collections.abc import AsyncGenerator, Callable
from dataclasses import dataclass
from typing import Any

import pytest
import pytest_asyncio
from _pytest.logging import LogCaptureFixture
from helpers.ddl import GROUP0_CONFLICT_MESSAGE, MAX_GROUP0_CONFLICT_RETRIES, SchemaQueriesLBP, ddl
from helpers.session import connect
from scylla.errors import AlreadyExists, CqlSyntaxError, RetryPolicyError, ServerError, Unavailable
from scylla.policies.retry import (
    DefaultRetryPolicy,
    DefaultRetrySession,
    DowngradingConsistencyRetryPolicy,
    DowngradingConsistencyRetrySession,
    FallthroughRetryPolicy,
    FallthroughRetrySession,
    RequestInfo,
    RetryDecision,
    RetryPolicy,
    RetrySession,
)
from scylla.session import ExecutionProfile, Session, SessionBuilder
from scylla.statement import Consistency, Statement


def test_default_retry_policy_isinstance():
    policy = DefaultRetryPolicy()
    assert isinstance(policy, DefaultRetryPolicy)
    assert isinstance(policy, RetryPolicy)


def test_downgrading_consistency_retry_policy_isinstance():
    policy = DowngradingConsistencyRetryPolicy()
    assert isinstance(policy, DowngradingConsistencyRetryPolicy)
    assert isinstance(policy, RetryPolicy)


def test_fallthrough_retry_policy_isinstance():
    policy = FallthroughRetryPolicy()
    assert isinstance(policy, FallthroughRetryPolicy)
    assert isinstance(policy, RetryPolicy)


def test_custom_retry_policy_isinstance():
    class MySession:
        def __init__(self):
            self.attempts = 0

        def decide_should_retry(self, request_info: RequestInfo) -> RetryDecision:
            self.attempts += 1
            if request_info.is_idempotent and self.attempts <= 2:
                return RetryDecision.DontRetry()
            return RetryDecision.DontRetry()

        def reset(self):
            self.attempts = 0

    class MyPolicy:
        def new_session(self):
            return MySession()

    policy = MyPolicy()
    session = policy.new_session()

    assert isinstance(policy, RetryPolicy)
    assert isinstance(session, RetrySession)


def test_default_retry_policy_identity_preserved():
    policy = DefaultRetryPolicy()
    profile = ExecutionProfile(retry_policy=policy)
    assert profile.retry_policy is policy


def test_downgrading_consistency_retry_policy_identity_preserved():
    policy = DowngradingConsistencyRetryPolicy()
    profile = ExecutionProfile(retry_policy=policy)
    assert profile.retry_policy is policy


def test_fallthrough_retry_policy_identity_preserved():
    policy = FallthroughRetryPolicy()
    profile = ExecutionProfile(retry_policy=policy)
    assert profile.retry_policy is policy


def test_no_retry_policy_is_none():
    profile = ExecutionProfile()
    assert profile.retry_policy is None


def test_explicit_none_retry_policy():
    profile = ExecutionProfile(retry_policy=None)
    assert profile.retry_policy is None


def test_custom_retry_policy_accepted():
    class MySession:
        def __init__(self):
            self.attempts = 0

        def decide_should_retry(self, request_info: RequestInfo) -> RetryDecision:
            self.attempts += 1
            if request_info.is_idempotent and self.attempts <= 2:
                return RetryDecision.DontRetry()
            return RetryDecision.DontRetry()

        def reset(self):
            self.attempts = 0

    class MyPolicy:
        def new_session(self) -> MySession:
            return MySession()

    policy = MyPolicy()
    profile = ExecutionProfile(retry_policy=policy)
    assert profile.retry_policy is policy


def test_different_profiles_have_independent_policies():
    policy_a = DefaultRetryPolicy()
    policy_b = FallthroughRetryPolicy()

    profile_a = ExecutionProfile(retry_policy=policy_a)
    profile_b = ExecutionProfile(retry_policy=policy_b)

    assert profile_a.retry_policy is policy_a
    assert profile_b.retry_policy is policy_b
    assert profile_a.retry_policy is not profile_b.retry_policy


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_custom_retry_policy_new_session_called_when_set_on_statement():
    class RecordingRetrySession:
        def decide_should_retry(self, request_info: RequestInfo) -> RetryDecision:
            return RetryDecision.DontRetry()

        def reset(self) -> None:
            pass

    class RecordingRetryPolicy:
        def __init__(self) -> None:
            self.new_session_called = False

        def new_session(self) -> RecordingRetrySession:
            self.new_session_called = True
            return RecordingRetrySession()

    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()

    # A failed attempt starts a retry session.
    failing_policy = RecordingRetryPolicy()
    failing = Statement("THIS STATEMENT FAILS")
    failing.retry_policy = failing_policy
    with pytest.raises(CqlSyntaxError):
        await session.execute(failing)
    assert failing_policy.new_session_called is True


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_custom_retry_policy_decide_should_retry_called_on_invalid_request():
    class RecordingRetrySession:
        def __init__(self) -> None:
            self.decide_should_retry_called = False
            self.request_info = None

        def decide_should_retry(self, request_info: RequestInfo) -> RetryDecision:
            self.decide_should_retry_called = True
            self.request_info = request_info
            return RetryDecision.DontRetry()

        def reset(self) -> None:
            pass

    class RecordingRetryPolicy:
        def __init__(self) -> None:
            self.session = RecordingRetrySession()

        def new_session(self) -> RecordingRetrySession:
            return self.session

    policy = RecordingRetryPolicy()
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()
    statement = Statement("THIS IS NOT VALID CQL")
    statement.retry_policy = policy

    with pytest.raises(CqlSyntaxError):
        await session.execute(statement)

    assert policy.session.decide_should_retry_called is True
    assert policy.session.request_info is not None
    assert isinstance(policy.session.request_info, RequestInfo)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_custom_retry_policy_retry_decision_is_used_by_driver():
    class RetryOnceSession:
        def __init__(self) -> None:
            self.calls = 0

        def decide_should_retry(self, request_info: RequestInfo) -> RetryDecision:
            self.calls += 1
            if self.calls == 1:
                return RetryDecision.RetrySameTarget()
            return RetryDecision.DontRetry()

        def reset(self) -> None:
            pass

    class RetryOncePolicy:
        def __init__(self) -> None:
            self.session = RetryOnceSession()

        def new_session(self) -> RetryOnceSession:
            return self.session

    policy = RetryOncePolicy()
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()
    statement = Statement("THIS IS NOT VALID CQL")
    statement.retry_policy = policy

    with pytest.raises(CqlSyntaxError):
        await session.execute(statement)

    assert policy.session.calls == 2


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_custom_retry_policy_receives_request_info_fields_from_driver():
    class RecordingRetrySession:
        def __init__(self) -> None:
            self.error = None
            self.is_idempotent = None
            self.consistency = None

        def decide_should_retry(self, request_info: RequestInfo) -> RetryDecision:
            self.error = request_info.error
            self.is_idempotent = request_info.is_idempotent
            self.consistency = request_info.consistency
            return RetryDecision.DontRetry()

        def reset(self) -> None:
            pass

    class RecordingRetryPolicy:
        def __init__(self) -> None:
            self.session = RecordingRetrySession()

        def new_session(self) -> RecordingRetrySession:
            return self.session

    policy = RecordingRetryPolicy()
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()
    statement = Statement("THIS IS NOT VALID CQL")
    statement.retry_policy = policy

    with pytest.raises(CqlSyntaxError):
        await session.execute(statement)

    assert policy.session.error is not None
    assert isinstance(policy.session.error, CqlSyntaxError)
    assert policy.session.is_idempotent is False
    assert policy.session.consistency is not None


class NotARetryPolicy:
    def decide_should_retry(self, request_info: RequestInfo) -> RetryDecision:
        return RetryDecision.DontRetry()


def _set_on_profile(policy: Any) -> None:
    ExecutionProfile(retry_policy=policy)


def _set_on_statement(policy: Any) -> None:
    Statement("SELECT 1").retry_policy = policy


@pytest.mark.parametrize("set_policy", [_set_on_profile, _set_on_statement])
@pytest.mark.parametrize(("policy", "type_name"), [(NotARetryPolicy(), "NotARetryPolicy"), (42, "int")])
def test_object_without_new_session_is_rejected(set_policy: Callable[[Any], None], policy: Any, type_name: str):
    with pytest.raises(RetryPolicyError) as excinfo:
        set_policy(policy)

    assert str(excinfo.value) == (
        f"Invalid retry policy '{type_name}': Object does not implement the RetryPolicy "
        "protocol (missing required 'new_session' method)."
    )


@pytest.mark.parametrize(
    ("policy", "session_type"),
    [
        (DefaultRetryPolicy(), DefaultRetrySession),
        (DowngradingConsistencyRetryPolicy(), DowngradingConsistencyRetrySession),
        (FallthroughRetryPolicy(), FallthroughRetrySession),
    ],
)
def test_builtin_policy_new_session_returns_its_session(policy: RetryPolicy, session_type: type[object]):
    first = policy.new_session()
    second = policy.new_session()

    assert type(first) is session_type
    assert type(second) is session_type
    # Each request gets a fresh session.
    assert first is not second


# Tests against the test cluster.


class CountingSession:
    """Returns `result` (or raises it) from every `decide_should_retry()` call."""

    def __init__(self, result: object) -> None:
        self.result = result
        self.calls = 0

    def decide_should_retry(self, request_info: RequestInfo) -> RetryDecision:
        self.calls += 1
        if isinstance(self.result, BaseException):
            raise self.result
        return self.result  # pyright: ignore[reportReturnType]

    def reset(self) -> None:
        pass


class FixedSessionPolicy:
    """Hands out `session` from every `new_session()` call."""

    def __init__(self) -> None:
        self.session: object = None
        self.new_session_calls = 0

    def new_session(self) -> RetrySession:
        self.new_session_calls += 1
        return self.session  # pyright: ignore[reportReturnType]


def returning(session: object) -> FixedSessionPolicy:
    policy = FixedSessionPolicy()
    policy.session = session
    return policy


@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize(
    ("result", "expected_log"),
    [
        (
            RuntimeError("decide exploded"),
            "Failed to call decide_should_retry() on custom retry session. Fallback action: 'DontRetry'. "
            + "Reason: RuntimeError: decide exploded",
        ),
        (
            "not a decision",
            "Failed to extract 'PyRetryDecision'. Fallback action: 'DontRetry'.",
        ),
    ],
)
async def test_custom_session_failure_is_logged_and_not_retried(
    caplog: LogCaptureFixture, result: object, expected_log: str
):
    retry_session = CountingSession(result)
    policy = returning(retry_session)
    session = await connect()
    statement = Statement("THIS IS NOT VALID CQL")
    statement.is_idempotent = True
    statement.retry_policy = policy

    with pytest.raises(CqlSyntaxError):
        await session.execute(statement)

    # A retry would have asked the session again.
    assert retry_session.calls == 1
    assert policy.new_session_calls == 1
    errors = [r for r in caplog.records if r.levelno == logging.ERROR and expected_log in r.getMessage()]
    assert len(errors) == 1, caplog.text

    # The session is still usable afterwards.
    result_set = await session.execute("SELECT key FROM system.local")
    assert await result_set.first_row() is not None


OVER_REPLICATED_KEYSPACE = "retry_policy_over_replicated_ks"


@dataclass
class OverReplicated:
    """A session, and how many nodes the cluster it talks to has."""

    session: Session
    nodes: int


@pytest_asyncio.fixture(scope="module")
async def over_replicated() -> AsyncGenerator[OverReplicated, None]:
    """A table replicated to one node more than the cluster has.

    A read at `ALL` then always fails with `Unavailable` (required: nodes + 1,
    alive: nodes) while every node is up, which gives a retry policy a
    retryable error to decide on.
    """
    session = await connect()
    nodes = list(session.cluster_state.nodes_info.values())
    datacenters = {node.datacenter for node in nodes}
    # The replication factor applies per datacenter, so the counts below hold for one only.
    assert len(datacenters) == 1, f"these tests need a single-datacenter cluster, got {datacenters}"
    # Start clean: an aborted earlier run may have left the keyspace behind.
    await ddl(session, f"DROP KEYSPACE IF EXISTS {OVER_REPLICATED_KEYSPACE}")
    try:
        await ddl(
            session,
            f"CREATE KEYSPACE {OVER_REPLICATED_KEYSPACE} WITH replication = "
            f"{{'class': 'NetworkTopologyStrategy', 'replication_factor': {len(nodes) + 1}}} "
            "AND tablets = {'enabled': false}",
        )
        await ddl(session, f"CREATE TABLE {OVER_REPLICATED_KEYSPACE}.t (k int PRIMARY KEY, v int)")
        yield OverReplicated(session, len(nodes))
    finally:
        await ddl(session, f"DROP KEYSPACE IF EXISTS {OVER_REPLICATED_KEYSPACE}")


def _read_at_all(policy: RetryPolicy) -> Statement:
    statement = Statement(f"SELECT v FROM {OVER_REPLICATED_KEYSPACE}.t WHERE k = 1")
    statement.consistency = Consistency.All
    statement.retry_policy = policy
    return statement


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_custom_policy_returning_downgrading_session_downgrades(over_replicated: OverReplicated):
    policy = returning(DowngradingConsistencyRetryPolicy().new_session())

    result = await over_replicated.session.execute(_read_at_all(policy))

    assert await result.first_row() is None
    assert policy.new_session_calls == 1


@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize("builtin", [DefaultRetryPolicy, FallthroughRetryPolicy])
async def test_custom_policy_returning_builtin_session_does_not_downgrade(
    over_replicated: OverReplicated, builtin: Callable[[], RetryPolicy]
):
    policy = returning(builtin().new_session())

    with pytest.raises(Unavailable) as excinfo:
        await over_replicated.session.execute(_read_at_all(policy))

    assert excinfo.value.consistency == Consistency.All
    assert excinfo.value.required == over_replicated.nodes + 1
    assert excinfo.value.alive == over_replicated.nodes
    assert policy.new_session_calls == 1


class RecordingSession:
    """Records every error it sees; retries only group 0 conflicts, as `helpers.ddl` does."""

    def __init__(self) -> None:
        self.errors: list[Exception] = []
        self.group0_conflicts = 0

    def decide_should_retry(self, request_info: RequestInfo) -> RetryDecision:
        error = request_info.error
        is_group0_conflict = isinstance(error, ServerError) and error.reason == GROUP0_CONFLICT_MESSAGE
        if is_group0_conflict and self.group0_conflicts < MAX_GROUP0_CONFLICT_RETRIES:
            self.group0_conflicts += 1
            return RetryDecision.RetrySameTarget()
        self.errors.append(error)
        return RetryDecision.DontRetry()

    def reset(self) -> None:
        pass


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_unavailable_reaches_custom_policy_with_its_fields(over_replicated: OverReplicated):
    retry_session = RecordingSession()

    with pytest.raises(Unavailable):
        await over_replicated.session.execute(_read_at_all(returning(retry_session)))

    assert len(retry_session.errors) == 1
    error = retry_session.errors[0]
    assert isinstance(error, Unavailable)
    assert error.consistency == Consistency.All
    assert error.required == over_replicated.nodes + 1
    assert error.alive == over_replicated.nodes


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_already_exists_reaches_custom_policy_with_keyspace_and_table(over_replicated: OverReplicated):
    session = over_replicated.session
    create = f"CREATE TABLE {OVER_REPLICATED_KEYSPACE}.twice (k int PRIMARY KEY)"
    await ddl(session, f"DROP TABLE IF EXISTS {OVER_REPLICATED_KEYSPACE}.twice")
    await ddl(session, create)
    retry_session = RecordingSession()
    statement = Statement(create)
    # `ddl()` would replace the retry policy under test, so only take its routing:
    # every DDL statement goes to the same node.
    statement.load_balancing_policy = SchemaQueriesLBP()
    statement.retry_policy = returning(retry_session)

    with pytest.raises(AlreadyExists):
        await session.execute(statement)

    assert len(retry_session.errors) == 1
    error = retry_session.errors[0]
    assert isinstance(error, AlreadyExists)
    assert error.keyspace == OVER_REPLICATED_KEYSPACE
    assert error.table == "twice"
