from enum import IntEnum

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

class ScyllaError(Exception): ...
class InternalDriverError(ScyllaError): ...
class RowIterationError(ScyllaError): ...

class DeserializationError(ScyllaError):
    column_name: str | None
    column_index: int | None
    inner_path: list[str] | None

class UnsupportedTypeDeserializationError(DeserializationError): ...
class DecodeFailedError(DeserializationError): ...
class PyConversionFailedError(DeserializationError): ...
class SessionConnectionError(ScyllaError): ...
class SessionConfigError(ScyllaError): ...
class StatementConversionError(ScyllaError): ...

# Invalid input, rejected before sending - retrying won't help
class BadQuery(ScyllaError, ValueError): ...

class ValuesTooLongForKey(BadQuery):
    length: int
    max_length: int

class TooManyStatementsInBatch(BadQuery):
    count: int

class PartitionKeyExtractionFailed(BadQuery): ...
class PagingStateNotAllowed(BadQuery): ...

# Failures while executing a request
class ExecutionError(ScyllaError): ...

class OperationTimedOut(ExecutionError, TimeoutError):
    timeout: float

class NoHostAvailable(ExecutionError): ...
class MetadataError(ExecutionError): ...
class SchemaAgreementError(ExecutionError): ...
class ConnectionPoolError(ExecutionError): ...
class ConnectionPoolBroken(ConnectionPoolError): ...
class PoolInitializing(ConnectionPoolError): ...
class NodeDisabledByHostFilter(ConnectionPoolError): ...
class PrepareError(ExecutionError): ...

class RepreparedIdChanged(PrepareError):
    statement: str
    expected_id: bytes
    reprepared_id: bytes

class RepreparedIdMissingInBatch(PrepareError): ...
class RequestFailedError(ExecutionError): ...
class BrokenConnection(RequestFailedError): ...
class ConnectionBusy(RequestFailedError): ...
class NonfinishedPagingState(RequestFailedError): ...
class ProtocolError(RequestFailedError): ...
class ResponseParseError(ProtocolError): ...
class RequestSerializationError(ProtocolError): ...

class UnexpectedResponse(ProtocolError):
    response_kind: CqlResponseKind

# Errors returned by the database
class DatabaseError(RequestFailedError):
    reason: str

class RequestExecutionError(DatabaseError): ...

class Unavailable(RequestExecutionError):
    consistency: Consistency
    required: int
    alive: int

class ReadTimeout(RequestExecutionError):
    consistency: Consistency
    received: int
    required: int
    data_present: bool

class WriteTimeout(RequestExecutionError):
    consistency: Consistency
    received: int
    required: int
    write_type: WriteType

class ReadFailure(RequestExecutionError):
    consistency: Consistency
    received: int
    required: int
    numfailures: int
    data_present: bool

class WriteFailure(RequestExecutionError):
    consistency: Consistency
    received: int
    required: int
    numfailures: int
    write_type: WriteType

class FunctionFailure(RequestExecutionError):
    keyspace: str
    function: str
    arg_types: list[str]

class Overloaded(RequestExecutionError): ...
class IsBootstrapping(RequestExecutionError): ...
class TruncateError(RequestExecutionError): ...

class RateLimitReached(RequestExecutionError):
    op_type: OperationType
    rejected_by_coordinator: bool

class RequestValidationError(DatabaseError): ...
class CqlSyntaxError(RequestValidationError): ...
class InvalidRequest(RequestValidationError): ...
class Unauthorized(RequestValidationError): ...
class ServerConfigError(RequestValidationError): ...

class AlreadyExists(RequestValidationError):
    keyspace: str
    table: str

class AuthenticationFailed(DatabaseError): ...
class ServerError(DatabaseError): ...
class ServerProtocolError(DatabaseError): ...

class Unprepared(DatabaseError):
    statement_id: bytes

class UnknownDatabaseError(DatabaseError):
    code: int

# Other errors
class StatementConfigError(ScyllaError): ...
class BatchError(ScyllaError): ...

class SerializationError(ScyllaError):
    parameter: str | int | None

class UnsupportedTypeSerializationError(SerializationError): ...
class TypeMismatchSerializationError(SerializationError): ...
class ValueOverflowSerializationError(SerializationError): ...
class SerializeFailedError(SerializationError): ...
class PySerializationFailedError(SerializationError): ...
class ClusterStateTokenError(ScyllaError): ...
class UseKeyspaceError(ScyllaError): ...
class BadKeyspaceNameError(UseKeyspaceError): ...
class RequestError(UseKeyspaceError): ...
class KeyspaceNameMismatchError(UseKeyspaceError): ...
class RequestTimeoutError(UseKeyspaceError): ...
class RuntimeTaskJoinFailedError(UseKeyspaceError): ...
class AddressTranslationError(ScyllaError): ...
class HostFilterError(ScyllaError): ...
class TlsError(ScyllaError): ...
class LoadBalancingPolicyError(ScyllaError): ...
class RetryPolicyError(ScyllaError): ...
class RowFactoryError(ScyllaError): ...
class QueryMetadataError(ScyllaError): ...
class SpeculativeExecutionPolicyError(ScyllaError): ...

class FutureCancelledError(Exception):
    """Raised when a `DriverFuture` is cancelled via `cancel()`."""
