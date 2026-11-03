from typing import Protocol, runtime_checkable

from .._rust.policies.retry import (  # pyright: ignore[reportMissingModuleSource]
    DefaultRetryPolicy,
    DefaultRetrySession,
    DowngradingConsistencyRetryPolicy,
    DowngradingConsistencyRetrySession,
    FallthroughRetryPolicy,
    FallthroughRetrySession,
    RequestInfo,
    RetryDecision,
)


@runtime_checkable
class RetrySession(Protocol):
    def __init__(self) -> None: ...
    def decide_should_retry(self, request_info: RequestInfo) -> RetryDecision: ...
    def reset(self) -> None: ...


@runtime_checkable
class RetryPolicy(Protocol):
    def __init__(self) -> None: ...
    def new_session(self) -> RetrySession: ...


__all__ = [
    "DefaultRetryPolicy",
    "DefaultRetrySession",
    "DowngradingConsistencyRetryPolicy",
    "DowngradingConsistencyRetrySession",
    "FallthroughRetryPolicy",
    "FallthroughRetrySession",
    "RequestInfo",
    "RetryDecision",
    "RetryPolicy",
    "RetrySession",
]
