from enum import IntEnum
from typing import Protocol, runtime_checkable

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

class CqlRequestKind(IntEnum):
    """Possible CQL requests sent by the driver to the server."""

    Startup = ...
    """Initializes the connection; the first request sent on it."""

    AuthResponse = ...
    """Answers a server authentication challenge."""

    Options = ...
    """Asks the server which options it supports."""

    Query = ...
    """Executes an unprepared statement."""

    Prepare = ...
    """Prepares a statement."""

    Execute = ...
    """Executes a prepared statement."""

    Batch = ...
    """Executes a batch of statements."""

    Register = ...
    """Registers the connection to receive server events."""

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

class DbError:
    """An error sent from the database in response to a query."""

    class SyntaxError(DbError):
        """The submitted query has a syntax error."""

    class Invalid(DbError):
        """The query is syntactically correct but invalid."""

    class AuthenticationError(DbError):
        """Authentication failed - bad credentials."""

    class Unauthorized(DbError):
        """The logged user doesn't have the right to perform the query."""

    class ConfigError(DbError):
        """The query is invalid because of some configuration issue."""

    class Overloaded(DbError):
        """The request cannot be processed because the coordinator node is overloaded."""

    class IsBootstrapping(DbError):
        """The coordinator node is still bootstrapping."""

    class TruncateError(DbError):
        """Error during truncate operation."""

    class ServerError(DbError):
        """Internal server error. This indicates a server-side bug."""

    class ProtocolError(DbError):
        """Invalid protocol message received from the driver."""

    class AlreadyExists(DbError):
        """
        Attempted to create a keyspace or a table that was already existing.

        Attributes:
            keyspace (`str`): Created keyspace name or name of the keyspace in which table was created.
            table (`str`): Name of the table created, in case of keyspace creation it's an empty string.
        """

        @property
        def keyspace(self) -> str: ...
        @property
        def table(self) -> str: ...
        def __init__(self, keyspace: str, table: str) -> None: ...

    class FunctionFailure(DbError):
        """
        User defined function failed during execution.

        Attributes:
            keyspace (`str`): Keyspace of the failed function.
            function (`str`): Name of the failed function.
            arg_types (`list[str]`): Types of arguments passed to the function.
        """

        @property
        def keyspace(self) -> str: ...
        @property
        def function(self) -> str: ...
        @property
        def arg_types(self) -> list[str]: ...
        def __init__(self, keyspace: str, function: str, arg_types: list[str]) -> None: ...

    class Unavailable(DbError):
        """
        Not enough nodes are alive to satisfy required consistency level.

        Attributes:
            consistency (`Consistency`): Consistency level of the query.
            required (`int`): Number of nodes required to be alive to satisfy required consistency level.
            alive (`int`): Found number of active nodes.
        """

        @property
        def consistency(self) -> Consistency: ...
        @property
        def required(self) -> int: ...
        @property
        def alive(self) -> int: ...
        def __init__(self, consistency: Consistency, required: int, alive: int) -> None: ...

    class ReadTimeout(DbError):
        """
        Not enough nodes responded to the read request in time to satisfy required consistency level.

        Attributes:
            consistency (`Consistency`): Consistency level of the query.
            received (`int`): Number of nodes that responded to the read request.
            required (`int`): Number of nodes required to respond to satisfy required consistency level.
            data_present (`bool`): Replica that was asked for data has responded.
        """

        @property
        def consistency(self) -> Consistency: ...
        @property
        def received(self) -> int: ...
        @property
        def required(self) -> int: ...
        @property
        def data_present(self) -> bool: ...
        def __init__(self, consistency: Consistency, received: int, required: int, data_present: bool) -> None: ...

    class WriteTimeout(DbError):
        """
        Not enough nodes responded to the write request in time to satisfy required consistency level.

        Attributes:
            consistency (`Consistency`): Consistency level of the query.
            received (`int`): Number of nodes that responded to the write request.
            required (`int`): Number of nodes required to respond to satisfy required consistency level.
            write_type (`WriteType`): Type of write operation requested.
        """

        @property
        def consistency(self) -> Consistency: ...
        @property
        def received(self) -> int: ...
        @property
        def required(self) -> int: ...
        @property
        def write_type(self) -> WriteType: ...
        def __init__(self, consistency: Consistency, received: int, required: int, write_type: WriteType) -> None: ...

    class ReadFailure(DbError):
        """
        A non-timeout error during a read request.

        Attributes:
            consistency (`Consistency`): Consistency level of the query.
            received (`int`): Number of nodes that responded to the read request.
            required (`int`): Number of nodes required to respond to satisfy required consistency level.
            numfailures (`i32`): Number of nodes that experience a failure while executing the request.
            data_present: (`bool`): Replica that was asked for data has responded.
        """

        @property
        def consistency(self) -> Consistency: ...
        @property
        def received(self) -> int: ...
        @property
        def required(self) -> int: ...
        @property
        def numfailures(self) -> int: ...
        @property
        def data_present(self) -> bool: ...
        def __init__(
            self, consistency: Consistency, received: int, required: int, numfailures: int, data_present: bool
        ) -> None: ...

    class WriteFailure(DbError):
        """
        A non-timeout error during a write request.

        Attributes:
            consistency (`Consistency`): Consistency level of the query.
            received (`int`): Number of nodes that responded to the write request.
            required (`int`): Number of nodes required to respond to satisfy required consistency level.
            numfailures (`i32`): Number of nodes that experience a failure while executing the request.
            write_type (`WriteType`): Type of write operation requested.
        """

        @property
        def consistency(self) -> Consistency: ...
        @property
        def received(self) -> int: ...
        @property
        def required(self) -> int: ...
        @property
        def numfailures(self) -> int: ...
        @property
        def write_type(self) -> WriteType: ...
        def __init__(
            self, consistency: Consistency, received: int, required: int, numfailures: int, write_type: WriteType
        ) -> None: ...

    class Unprepared(DbError):
        """
        Tried to execute a prepared statement that is not prepared. Driver should prepare it again.

        Attributes:
            statement_id (`bytes`): Statement id of the requested prepared query.
        """

        @property
        def statement_id(self) -> bytes: ...
        def __init__(self, statement_id: bytes) -> None: ...

    class RateLimitReached(DbError):
        """
        Rate limit was exceeded for a partition affected by the request.

        Attributes:
            op_type (`OperationType`): Type of the operation rejected by rate limiting.
            rejected_by_coordinator (`bool`): Whether the operation was rate limited on the coordinator or not.
                Writes rejected on the coordinator are guaranteed not to be applied
                on any replica.
        """

        @property
        def op_type(self) -> OperationType: ...
        @property
        def rejected_by_coordinator(self) -> bool: ...
        def __init__(self, op_type: OperationType, rejected_by_coordinator: bool) -> None: ...

    class Other(DbError):
        """
        Other error code not specified in the specification.

        Attributes:
            code (`int`): Code of the error.
        """

        @property
        def code(self) -> int: ...
        def __init__(self, code: int) -> None: ...

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
        """Database sent a response containing some error with a message"""

        @property
        def error(self) -> DbError: ...
        @property
        def message(self) -> str: ...
        def __init__(self, error: DbError, message: str) -> None: ...

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
