import pytest
from scylla.errors import CqlSyntaxError
from scylla.policies.retry import (
    DefaultRetryPolicy,
    DowngradingConsistencyRetryPolicy,
    FallthroughRetryPolicy,
    RequestAttemptError,
    RequestInfo,
    RetryDecision,
    RetryPolicy,
    RetrySession,
)
from scylla.session import ExecutionProfile, SessionBuilder
from scylla.statement import Statement


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

    policy = RecordingRetryPolicy()
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()
    statement = Statement("SELECT * FROM system.local")
    statement.retry_policy = policy

    await session.execute(statement)

    assert policy.new_session_called is True


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
    assert isinstance(policy.session.error, RequestAttemptError.DbError)
    assert isinstance(policy.session.error.error, CqlSyntaxError)
    assert policy.session.is_idempotent is False
    assert policy.session.consistency is not None
