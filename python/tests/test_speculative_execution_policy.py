from datetime import timedelta

import pytest
from scylla.errors import SessionConfigError, SpeculativeExecutionPolicyError
from scylla.execution_profile import ExecutionProfile
from scylla.policies.speculative_execution import SimpleSpeculativeExecutionPolicy


def test_simple_speculative_execution_policy_attributes():
    policy = SimpleSpeculativeExecutionPolicy(delay=0.5, max_attempts=3)
    assert policy.delay == timedelta(seconds=0.5)
    assert policy.max_attempts == 3


def test_simple_speculative_execution_policy_positional_arguments():
    policy = SimpleSpeculativeExecutionPolicy(0.25, 10)
    assert policy.delay == timedelta(seconds=0.25)
    assert policy.max_attempts == 10


def test_delay_accepts_timedelta():
    policy = SimpleSpeculativeExecutionPolicy(timedelta(milliseconds=500), 3)
    assert policy.delay == timedelta(seconds=0.5)


def test_zero_delay_and_zero_attempts_are_allowed():
    policy = SimpleSpeculativeExecutionPolicy(0.0, 0)
    assert policy.delay == timedelta(0)
    assert policy.max_attempts == 0


@pytest.mark.parametrize("delay", [-1.0, float("nan"), float("inf"), timedelta(seconds=-1), "1.0"])
def test_invalid_delay_is_rejected(delay: object):
    with pytest.raises(SessionConfigError):
        SimpleSpeculativeExecutionPolicy(delay, 3)  # pyright: ignore[reportArgumentType]


def test_negative_max_attempts_is_rejected():
    with pytest.raises(OverflowError):
        SimpleSpeculativeExecutionPolicy(0.5, -1)


def test_policy_identity_preserved():
    policy = SimpleSpeculativeExecutionPolicy(0.5, 3)
    profile = ExecutionProfile(speculative_execution_policy=policy)
    assert profile.speculative_execution_policy is policy


def test_no_speculative_execution_policy_is_none():
    profile = ExecutionProfile()
    assert profile.speculative_execution_policy is None


def test_explicit_none_speculative_execution_policy():
    profile = ExecutionProfile(speculative_execution_policy=None)
    assert profile.speculative_execution_policy is None


def test_invalid_policy_is_rejected():
    class NotAPolicy:
        pass

    with pytest.raises(SpeculativeExecutionPolicyError):
        ExecutionProfile(speculative_execution_policy=NotAPolicy())  # pyright: ignore[reportArgumentType]
