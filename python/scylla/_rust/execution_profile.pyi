from scylla.policies.load_balancing import LoadBalancingPolicy

from .enums import Consistency, SerialConsistency
from .policies.retry_policy import RetryPolicy
from .policies.speculative_execution import SimpleSpeculativeExecutionPolicy

class ExecutionProfile:
    def __init__(
        self,
        timeout: float | None = 30.0,
        consistency: Consistency = Consistency.LocalQuorum,
        serial_consistency: SerialConsistency | None = SerialConsistency.LocalSerial,
        load_balancing_policy: LoadBalancingPolicy | None = None,
        retry_policy: RetryPolicy | None = None,
        speculative_execution_policy: SimpleSpeculativeExecutionPolicy | None = None,
    ) -> None: ...
    @property
    def request_timeout(self) -> float | None: ...
    @property
    def consistency(self) -> Consistency: ...
    @property
    def serial_consistency(self) -> SerialConsistency | None: ...
    @property
    def load_balancing_policy(self) -> LoadBalancingPolicy | None: ...
    @property
    def retry_policy(self) -> RetryPolicy | None: ...
    @property
    def speculative_execution_policy(self) -> SimpleSpeculativeExecutionPolicy | None: ...
