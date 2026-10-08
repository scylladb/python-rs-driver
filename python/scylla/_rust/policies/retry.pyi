from enum import IntEnum
from typing import Protocol, runtime_checkable

from scylla.errors import DatabaseError
from scylla.statement import Consistency

class WriteType:
    """Type of write operation requested."""

    class Simple(WriteType):
        """Non-batched non-counter write."""

    class Batch(WriteType):
        """Logged batch write.

        Indicates that the batch log has been successfully written
        (otherwise BatchLog type would be present).
        """

    class UnloggedBatch(WriteType):
        """Unlogged batch. No batch log write has been attempted."""

    class Counter(WriteType):
        """Counter write (batched or not)."""

    class BatchLog(WriteType):
        """Timeout occurred during the write to the batch log when a logged batch was requested."""

    class Cas(WriteType):
        """Timeout occurred during Compare And Set write/update."""

    class View(WriteType):
        """Write involves VIEW update and failure to acquire local view (MV) lock for key within timeout."""

    class Cdc(WriteType):
        """Timeout occurred when a cdc_total_space_in_mb is exceeded when doing a write to data tracked by CDC."""

    class Other(WriteType):
        """Other type not specified in the specification."""

        @property
        def value(self) -> str: ...
        def __init__(self, value: str) -> None: ...

class OperationType:
    """Type of the operation rejected by rate limiting."""

    class Read(OperationType): ...
    class Write(OperationType): ...

    class Other(OperationType):
        @property
        def code(self) -> int: ...
        def __init__(self, value: int) -> None: ...

class CqlResponseKind(IntEnum):
    """Possible CQL responses received from the server."""

    Error = ...
    """Indicates an error processing a request."""

    Ready = ...
    """
    Indicates that the server is ready to process queries. This message will be
    sent by the server either after a STARTUP message if no authentication is
    required (if authentication is required, the server indicates readiness by
    sending a AUTH_RESPONSE message).
    """

    Authenticate = ...
    """
    Indicates that the server requires authentication, and which authentication
    mechanism to use.

    The authentication is SASL based and thus consists of a number of server
    challenges (AUTH_CHALLENGE) followed by client responses (AUTH_RESPONSE).
    The initial exchange is however bootstrapped by an initial client response.
    The details of that exchange (including how many challenge-response pairs
    are required) are specific to the authenticator in use. The exchange ends
    when the server sends an AUTH_SUCCESS message or an ERROR message.

    This message will be sent following a STARTUP message if authentication is
    required and must be answered by a AUTH_RESPONSE message from the client.
    """

    Supported = ...
    """
    Indicates which startup options are supported by the server. This message
    comes as a response to an OPTIONS message.
    """

    Result = ...
    """
    The result to a query (PREPARE, EXECUTE or BATCH messages).
    It has multiple kinds:
    - Void: for results carrying no information.
    - Rows: for results to select queries, returning a set of rows.
    - Set_keyspace: the result to a `USE` statement.
    - Prepared: result to a PREPARE message.
    - Schema_change: the result to a schema altering statement.
    """

    Event = ...
    """
    An event pushed by the server. A client will only receive events for the
    types it has REGISTER-ed to. The valid event types are:

    - "TOPOLOGY_CHANGE": events related to change in the cluster topology.
      Currently, events are sent when new nodes are added to the cluster, and
      when nodes are removed.
    - "STATUS_CHANGE": events related to change of node status. Currently,
      up/down events are sent.
    - "SCHEMA_CHANGE": events related to schema change.
      The type of changed involved may be one of "CREATED", "UPDATED" or
      "DROPPED".
    """

    AuthChallenge = ...
    """
    A server authentication challenge (see AUTH_RESPONSE for more details).
    Clients are expected to answer the server challenge with an AUTH_RESPONSE
    message.
    """

    AuthSuccess = ...
    """Indicates the success of the authentication phase."""

class RetryDecision:
    """
    Returned by the `decide_should_retry()` method of `RetryPolicy`. Instructs the driver on what
    to do about the request after it failed.
    """

    class RetrySameTarget(RetryDecision):
        """
        Request will be sent to the same shard on the same host.

        Attributes:
            consistency (`Consistency` | `None`): The consistency level to use for the retry.
                If set to `None`, the driver will reuse the same consistency level as the
                original failed request. Defaults to `None`.
        """

        @property
        def consistency(self) -> Consistency | None: ...
        def __init__(self, consistency: Consistency | None = None) -> None: ...

    class RetryNextTarget(RetryDecision):
        """
        Request will be sent to the next target generated by load balancing policy.

        Attributes:
            consistency (`Consistency` | `None`): The consistency level to use for the retry.
                If set to `None`, the driver will reuse the same consistency level as the
                original failed request. Defaults to `None`.
        """

        @property
        def consistency(self) -> Consistency | None: ...
        def __init__(self, consistency: Consistency | None = None) -> None: ...

    class DontRetry(RetryDecision):
        """Fails the whole request."""

        def __init__(self) -> None: ...

    class IgnoreWriteError(RetryDecision):
        """Will cause the driver to return an empty successful response."""

        def __init__(self) -> None: ...

class RequestAttemptError:
    """
    An error that occurred during a single attempt of:
    - `PREPARE`
    - `EXECUTE`
    - `BATCH`

    requests. The retry decision is made based on this error.
    """

    class SerializationError(RequestAttemptError):
        """Failed to serialize query parameters."""

    class CqlRequestSerialization(RequestAttemptError):
        """Failed to serialize CQL request."""

    class UnableToAllocStreamId(RequestAttemptError):
        """Driver was unable to allocate a stream id to execute a query on."""

    class BrokenConnectionError(RequestAttemptError):
        """A connection has been broken during query execution."""

    class BodyExtensionsParseError(RequestAttemptError):
        """Failed to deserialize frame body extensions."""

    class CqlResultParseError(RequestAttemptError):
        """Received a RESULT server response, but failed to deserialize it."""

    class CqlErrorParseError(RequestAttemptError):
        """Received an ERROR server response, but failed to deserialize it."""

    class RepreparedIdMissingInBatch(RequestAttemptError):
        """Driver tried to reprepare a statement in the batch, but the reprepared
        statement's id is not included in the batch."""

    class NonfinishedPagingState(RequestAttemptError):
        """A result with nonfinished paging state received for unpaged query."""

    class Unknown(RequestAttemptError): ...

    class DbError(RequestAttemptError):
        """
        The database returned an error.

        Attributes:
            error (`DatabaseError`): The exception for the error code, for example `ReadTimeout`.
            message (`str`): The error message from the database.
        """

        @property
        def error(self) -> DatabaseError: ...
        @property
        def message(self) -> str: ...
        def __init__(self, error: DatabaseError, message: str) -> None: ...

    class UnexpectedResponse(RequestAttemptError):
        """Received an unexpected response from the server."""

        @property
        def kind(self) -> CqlResponseKind: ...
        def __init__(self, kind: CqlResponseKind) -> None: ...

    class RepreparedIdChanged(RequestAttemptError):
        """Prepared statement id changed after repreparation."""

        @property
        def statement(self) -> str: ...
        @property
        def expected_id(self) -> bytes: ...
        @property
        def reprepared_id(self) -> bytes: ...
        def __init__(self, statement: str, expected_id: bytes, reprepared_id: bytes) -> None: ...

class RequestInfo:
    """
    Information about a failed request.

    Attributes:
        error (`RequestAttemptError`): The error with which the request failed

        is_idempotent (`bool`): A request is idempotent if it can be applied multiple times
            without changing the result of the initial application.
            If set to `true` we can be sure that it is idempotent.
            If set to `false` it is unknown whether it is idempotent.

        consistency (`Consistency`): Consistency with which the request failed.
    """

    @property
    def error(self) -> RequestAttemptError: ...
    @property
    def is_idempotent(self) -> bool: ...
    @property
    def consistency(self) -> Consistency: ...

@runtime_checkable
class RetrySession(Protocol):
    """
    Used throughout a single request to decide when to retry it.
    After this request is finished it is destroyed or reset.
    """

    def __init__(self) -> None: ...
    def decide_should_retry(self, request_info: RequestInfo) -> RetryDecision: ...
    """Called after the request failed - decide what to do next."""

    def reset(self) -> None: ...
    """Reset before using for a new request."""

class DefaultRetrySession:
    """`RetrySession` for `DefaultRetryPolicy`."""

    def __init__(self) -> None: ...
    def decide_should_retry(self, request_info: RequestInfo) -> RetryDecision: ...
    def reset(self) -> None: ...

class DowngradingConsistencyRetrySession:
    """`RetrySession` for `DowngradingConsistencyRetryPolicy`."""

    def __init__(self) -> None: ...
    def decide_should_retry(self, request_info: RequestInfo) -> RetryDecision: ...
    def reset(self) -> None: ...

class FallthroughRetrySession:
    """`RetrySession` for `FallthroughRetryPolicy`."""

    def __init__(self) -> None: ...
    def decide_should_retry(self, request_info: RequestInfo) -> RetryDecision: ...
    def reset(self) -> None: ...

@runtime_checkable
class RetryPolicy(Protocol):
    """Specifies a policy used to decide when to retry a request."""

    def __init__(self) -> None: ...
    def new_session(self) -> RetrySession: ...
    """Called for each new request, starts a session of deciding about retries."""

class DefaultRetryPolicy:
    """Retries when there is a high chance that a retry might help."""

    def __init__(self) -> None: ...
    def new_session(self) -> RetrySession: ...

class DowngradingConsistencyRetryPolicy:
    """
    Retries with lower consistency level if it knows that the initial CL is unreachable.
    Also, it behaves as `DefaultRetryPolicy` when it believes that the initial CL is reachable.
    """

    def __init__(self) -> None: ...
    def new_session(self) -> RetrySession: ...

class FallthroughRetryPolicy:
    """Forwards all errors directly to the user, never retries."""

    def __init__(self) -> None: ...
    def new_session(self) -> RetrySession: ...
