from scylla._rust.types import UnsetType
from scylla.policies.load_balancing import LoadBalancingPolicy
from scylla.policies.retry import RetryPolicy
from scylla.results import ColumnSpec
from scylla.session import ExecutionProfile
from scylla.statement import Consistency, SerialConsistency

class PreparedStatement:
    """
    A prepared statement.

    Its configuration is changed in place by assigning to its attributes, and
    applies to every later execution of the statement. Assigning `UNSET` makes
    the statement fall back to its execution profile's value.
    """

    execution_profile: ExecutionProfile | None
    consistency: Consistency | UnsetType
    serial_consistency: SerialConsistency | None | UnsetType
    request_timeout: float | None | UnsetType
    page_size: int
    load_balancing_policy: LoadBalancingPolicy | None
    retry_policy: RetryPolicy | None
    is_idempotent: bool
    @property
    def query_id(self) -> bytes:
        """
        Retrieves the ID of this prepared statement.
        """
    @property
    def bind_columns(self) -> tuple[ColumnSpec, ...]:
        """
        Specifications of the bind variables of this statement.
        """
    @property
    def partition_key_indexes(self) -> tuple[int, ...]:
        """
        Bind variable indexes of the partition key columns, in partition key order.

        Element ``i`` is the index into ``bind_columns`` of the ``i``-th component of the
        partition key, so the tuple can be used directly to build a routing key.
        """
    @property
    def result_columns(self) -> tuple[ColumnSpec, ...]:
        """
        Specifications of the columns this statement returns.

        May be empty until the statement has been executed once: for some statements the
        server sends no result metadata in response to PREPARE, only to EXECUTE.
        """

class Statement:
    """
    An unprepared statement.

    Its configuration is changed in place by assigning to its attributes.
    Assigning `UNSET` makes the statement fall back to its execution
    profile's value.
    """

    execution_profile: ExecutionProfile | None
    consistency: Consistency | UnsetType
    serial_consistency: SerialConsistency | None | UnsetType
    request_timeout: float | None | UnsetType
    page_size: int
    load_balancing_policy: LoadBalancingPolicy | None
    retry_policy: RetryPolicy | None
    is_idempotent: bool
    def __init__(self, query_str: str) -> None: ...
    @property
    def contents(self) -> str: ...
