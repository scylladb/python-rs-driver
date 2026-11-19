import uuid
from collections.abc import AsyncGenerator, Callable, Iterator
from dataclasses import dataclass
from typing import Any, cast

import pytest
import pytest_asyncio
from helpers.ddl import ddl
from helpers.session import connect
from scylla.cluster import ClusterState, Node
from scylla.errors import CqlSyntaxError, RetryPolicyError, Unavailable
from scylla.policies.load_balancing import RoutingInfo
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
from scylla.routing import Shard
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


def set_on_profile(policy: Any) -> None:
    ExecutionProfile(retry_policy=policy)


def set_on_statement(policy: Any) -> None:
    Statement("SELECT 1").retry_policy = policy


@pytest.mark.parametrize("set_policy", [set_on_profile, set_on_statement])
@pytest.mark.parametrize("policy", [NotARetryPolicy(), 42])
def test_object_without_new_session_is_rejected(set_policy: Callable[[Any], None], policy: Any):
    with pytest.raises(RetryPolicyError):
        set_policy(policy)


@pytest.mark.parametrize(
    ("policy", "session_type"),
    [
        (DefaultRetryPolicy(), DefaultRetrySession),
        (DowngradingConsistencyRetryPolicy(), DowngradingConsistencyRetrySession),
        (FallthroughRetryPolicy(), FallthroughRetrySession),
    ],
)
def test_builtin_policy_new_session_returns_its_session(policy: RetryPolicy, session_type: type[object]):
    assert type(policy.new_session()) is session_type


# Tests against the test cluster.


class RecordingSession:
    """Records the request of every `decide_should_retry()` call, then returns `result` (or raises it)."""

    result: RetryDecision | BaseException
    infos: list[RequestInfo]

    def __init__(self, result: RetryDecision | BaseException | None = None) -> None:
        self.result = RetryDecision.DontRetry() if result is None else result
        self.infos = []

    def decide_should_retry(self, request_info: RequestInfo) -> RetryDecision:
        self.infos.append(request_info)
        if isinstance(self.result, BaseException):
            raise self.result
        return self.result

    def reset(self) -> None:
        pass


class FixedSessionPolicy:
    """Hands out `session` from every `new_session()` call (or raises it)."""

    session: RetrySession | BaseException
    new_session_calls: int

    # `session` is optional because the `RetryPolicy` protocol declares an
    # `__init__` without arguments, which one that requires `session` does not match.
    def __init__(self, session: RetrySession | BaseException | None = None) -> None:
        self.session = RecordingSession() if session is None else session
        self.new_session_calls = 0

    def new_session(self) -> RetrySession:
        self.new_session_calls += 1
        if isinstance(self.session, BaseException):
            raise self.session
        return self.session


@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize(
    "new_policy",
    [
        pytest.param(
            lambda: FixedSessionPolicy(RecordingSession(RuntimeError("decide exploded"))),
            id="decide_should_retry_raises",
        ),
        pytest.param(
            # The cast stands for user code that returns something that is not a decision.
            lambda: FixedSessionPolicy(RecordingSession(cast(RetryDecision, "not a decision"))),
            id="decide_should_retry_returns_a_non_decision",
        ),
        pytest.param(
            lambda: FixedSessionPolicy(RuntimeError("new_session exploded")),
            id="new_session_raises",
        ),
    ],
)
async def test_failing_custom_policy_does_not_retry_or_hide_the_error(new_policy: Callable[[], FixedSessionPolicy]):
    policy = new_policy()
    session = await connect()
    statement = Statement("THIS IS NOT VALID CQL")
    statement.is_idempotent = True
    statement.retry_policy = policy

    # The caller gets the error of the request, not the failure of the policy.
    with pytest.raises(CqlSyntaxError):
        await session.execute(statement)

    # A retry would have asked for a decision again.
    assert policy.new_session_calls == 1
    if isinstance(policy.session, RecordingSession):
        assert len(policy.session.infos) == 1

    # The session is still usable afterwards.
    result_set = await session.execute("SELECT key FROM system.local")
    assert await result_set.first_row() is not None


@dataclass
class OverReplicated:
    """A session, the over-replicated table, and how many nodes the cluster has."""

    session: Session
    table: str
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
    # A name of its own, so a keyspace left behind by an aborted run is not in the way.
    keyspace = f"retry_policy_over_replicated_{uuid.uuid4().hex}"
    try:
        await ddl(
            session,
            f"CREATE KEYSPACE {keyspace} WITH replication = "
            f"{{'class': 'NetworkTopologyStrategy', 'replication_factor': {len(nodes) + 1}}} "
            "AND tablets = {'enabled': false}",
        )
        await ddl(session, f"CREATE TABLE {keyspace}.t (k int PRIMARY KEY, v int)")
        # The row a read finds once a retry policy has downgraded its consistency.
        await session.execute(f"INSERT INTO {keyspace}.t (k, v) VALUES (1, 42)")
        yield OverReplicated(session, f"{keyspace}.t", len(nodes))
    finally:
        await ddl(session, f"DROP KEYSPACE IF EXISTS {keyspace}")


def read_at_all(table: str, policy: RetryPolicy) -> Statement:
    statement = Statement(f"SELECT v FROM {table} WHERE k = 1")
    statement.consistency = Consistency.All
    statement.retry_policy = policy
    return statement


def assert_unavailable(error: BaseException, nodes: int) -> None:
    """Asserts that `error` is the `Unavailable` of a read at `ALL` from the over-replicated table."""
    assert isinstance(error, Unavailable)
    assert error.consistency == Consistency.All
    assert error.required == nodes + 1
    assert error.alive == nodes


class CountingPlan:
    """Offers every node, and counts how many of them the driver used.

    The driver pulls the next target from the plan only when a retry policy
    asks for another node, so `targets_used` tells whether it retried there.
    """

    targets_used: int

    def __init__(self) -> None:
        self.targets_used = 0

    def pick_targets(
        self, routing_info: RoutingInfo, cluster_state: ClusterState
    ) -> Iterator[tuple[Node, Shard | None]]:
        for node in sorted(cluster_state.nodes_info.values(), key=lambda node: node.host_id):
            self.targets_used += 1
            yield node, None


@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize("from_custom_policy", [False, True], ids=["builtin", "from_custom_policy"])
@pytest.mark.parametrize(
    ("builtin", "session_type", "reads_the_row", "targets_used"),
    [
        # Downgrades on the same node.
        (DowngradingConsistencyRetryPolicy, DowngradingConsistencyRetrySession, True, 1),
        # Tries the next node once.
        (DefaultRetryPolicy, DefaultRetrySession, False, 2),
        # Does not retry.
        (FallthroughRetryPolicy, FallthroughRetrySession, False, 1),
    ],
)
async def test_builtin_policy_on_an_unavailable_read(
    over_replicated: OverReplicated,
    builtin: Callable[[], RetryPolicy],
    session_type: Callable[[], RetrySession],
    reads_the_row: bool,
    targets_used: int,
    from_custom_policy: bool,
):
    # A custom policy that hands out a built-in session, here one the user
    # constructed, must act like the built-in policy.
    custom = FixedSessionPolicy(session_type())
    plan = CountingPlan()
    statement = read_at_all(over_replicated.table, custom if from_custom_policy else builtin())
    statement.load_balancing_policy = plan

    if reads_the_row:
        result = await over_replicated.session.execute(statement)
        assert await result.first_row() == {"v": 42}
    else:
        with pytest.raises(Unavailable) as excinfo:
            await over_replicated.session.execute(statement)
        assert_unavailable(excinfo.value, over_replicated.nodes)

    assert plan.targets_used == targets_used
    assert custom.new_session_calls == (1 if from_custom_policy else 0)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_unavailable_reaches_custom_policy_with_its_fields(over_replicated: OverReplicated):
    retry_session = RecordingSession()

    with pytest.raises(Unavailable):
        await over_replicated.session.execute(read_at_all(over_replicated.table, FixedSessionPolicy(retry_session)))

    assert len(retry_session.infos) == 1
    assert_unavailable(retry_session.infos[0].error, over_replicated.nodes)
