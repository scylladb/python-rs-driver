from datetime import timedelta

class SimpleSpeculativeExecutionPolicy:
    """
    Speculative execution policy that starts a new execution of the request every
    `delay`, up to `max_attempts` times.

    Speculative execution only fires for statements explicitly marked as idempotent
    (`Statement.is_idempotent` / `PreparedStatement.is_idempotent`). Non-idempotent
    statements bypass the policy entirely, so the driver never duplicates
    side-effecting writes.

    Attributes:
        delay (`timedelta` | `float`): Delay between consecutive speculative executions.
            A `float` is interpreted as a non-negative, finite number of seconds.
        max_attempts (`int`): Maximum number of speculative executions started for a
            single request. This does **not** count the original execution, so
            `max_attempts=3` means at most 4 executions in total.
    """

    @property
    def delay(self) -> timedelta: ...
    @property
    def max_attempts(self) -> int: ...
    def __init__(self, delay: timedelta | float, max_attempts: int) -> None: ...
