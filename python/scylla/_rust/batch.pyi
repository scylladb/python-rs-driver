from collections.abc import Sequence
from enum import IntEnum
from typing import Any

from scylla._rust.types import UnsetType
from scylla.policies.load_balancing import LoadBalancingPolicy
from scylla.policies.retry import RetryPolicy
from scylla.session import ExecutionProfile
from scylla.statement import Consistency, PreparedStatement, SerialConsistency, Statement

class BatchType(IntEnum):
    Logged = ...
    Unlogged = ...
    Counter = ...

class Batch:
    """
    CQL batch statement.

    Any mix of prepared and unprepared statements is allowed.
    For maximum performance, it is recommended to use prepared statements
    whenever possible.

    Its configuration is changed in place by assigning to its attributes.
    Assigning `UNSET` makes the batch fall back to its execution profile's
    value.
    """
    def __init__(self, batch_type: BatchType = BatchType.Logged) -> None: ...
    def add(self, statement: str | Statement | PreparedStatement, values: Any | None = None) -> None: ...
    def add_all(self, items: Sequence[tuple[str | Statement | PreparedStatement, Any | None]]) -> None: ...
    @property
    def type(self) -> BatchType: ...
    execution_profile: ExecutionProfile | None
    consistency: Consistency | UnsetType
    serial_consistency: SerialConsistency | None | UnsetType
    request_timeout: float | None | UnsetType
    load_balancing_policy: LoadBalancingPolicy | None
    retry_policy: RetryPolicy | None
    is_idempotent: bool
