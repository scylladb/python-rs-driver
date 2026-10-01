from datetime import timedelta
from enum import IntEnum
from uuid import UUID

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
    """Initializes the connection. The driver sends it after `Options` and before all other requests."""

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

class ContactPointError(ScyllaError):
    """The driver cannot use the contact points. Other connect failures raise their own class."""

class HostnameResolutionFailed(ContactPointError):
    hostnames: list[str]

class NoKnownNodes(ContactPointError): ...
class SessionConfigError(ScyllaError): ...

# Invalid input, rejected before sending - retrying won't help
class BadQuery(ScyllaError, ValueError): ...

class ValuesTooLongForKey(BadQuery):
    length: int
    max_length: int

class TooManyStatementsInBatch(BadQuery):
    count: int

class PartitionKeyExtractionFailed(BadQuery): ...
class PagingStateNotAllowed(BadQuery): ...
class StatementConversionError(BadQuery): ...
class InvalidStatementType(StatementConversionError, TypeError): ...
class AlreadyPrepared(StatementConversionError, TypeError): ...

class SerializationError(BadQuery):
    """Raised when the driver cannot serialize the bound values of a statement."""

class RowSerializationError(SerializationError):
    """Raised when the bound values do not match the bind markers of the statement.

    Raised as this class itself when the driver cannot read the values
    container. The Python error is the `__cause__`.
    """

class WrongNumberOfValues(RowSerializationError):
    """Raised when the number of values is not the number of bind markers."""

    expected: int
    received: int

class MissingValue(RowSerializationError):
    """Raised when the values mapping has no value for a named bind marker."""

    bind_marker: str
    """The name of the bind marker that has no value."""

class ValueSerializationError(SerializationError):
    """Raised when the driver cannot serialize one bound value."""

    bind_marker: str | int | None
    """The bind marker of the value that failed.

    The 0-based position of the value when the values are a sequence, or the
    bind marker name when the values are a mapping. `None` when the value is
    not bound to a statement.
    """

class UnsupportedTypeSerializationError(ValueSerializationError): ...
class TypeMismatchSerializationError(ValueSerializationError, TypeError): ...
class ValueOverflowSerializationError(ValueSerializationError, OverflowError): ...
class InvalidValueSerializationError(ValueSerializationError, ValueError): ...
class SerializeFailedError(ValueSerializationError): ...
class PySerializationFailedError(ValueSerializationError): ...

# Failures while executing a request
class ExecutionError(ScyllaError): ...

class OperationTimedOut(ExecutionError, TimeoutError):
    timeout: timedelta

class NoHostAvailable(ExecutionError): ...

class TraceUnavailable(ExecutionError):
    """Raised when a query trace is not available yet."""

class MetadataError(ExecutionError):
    """Failures specific to cluster metadata. Request failures raise their own class."""

class MetadataFetchFailed(MetadataError):
    table: str

class InvalidClusterMetadata(MetadataError): ...

class SchemaAgreementError(ExecutionError):
    """Failures specific to schema agreement. Request failures raise their own class."""

class SchemaAgreementTimeout(SchemaAgreementError, OperationTimedOut): ...

class RequiredHostAbsent(SchemaAgreementError):
    host_id: UUID

class UseKeyspaceError(ExecutionError):
    """Failures specific to `use_keyspace`. Request failures raise their own class."""

class BadKeyspaceName(UseKeyspaceError, ValueError): ...

class KeyspaceNameMismatch(UseKeyspaceError):
    expected: str
    received: str

class ConnectionPoolError(ExecutionError): ...
class ConnectionPoolBroken(ConnectionPoolError): ...
class ConnectionPoolInitializing(ConnectionPoolError): ...
class NodeDisabledByHostFilter(ConnectionPoolError): ...
class ConnectTimeout(ConnectionPoolBroken, TimeoutError): ...
class AddressTranslationFailed(ConnectionPoolBroken, AddressTranslationError): ...
class ConnectionLost(ConnectionPoolBroken): ...

class ConnectionSetupFailed(ConnectionPoolBroken):
    request_kind: CqlRequestKind
    reason: str | None

class ConnectionAuthenticationFailed(ConnectionSetupFailed): ...
class RequestFailedError(ExecutionError): ...
class PrepareError(ExecutionError): ...
class PreparedStatementIdsMismatch(PrepareError): ...

class RepreparedIdChanged(PrepareError, RequestFailedError):
    statement: str
    expected_id: bytes
    reprepared_id: bytes

class RepreparedIdMissingInBatch(PrepareError, RequestFailedError): ...
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

class Unavailable(DatabaseError):
    consistency: Consistency
    required: int
    alive: int

class ReadTimeout(DatabaseError):
    consistency: Consistency
    received: int
    required: int
    data_present: bool

class WriteTimeout(DatabaseError):
    consistency: Consistency
    received: int
    required: int
    write_type: WriteType

class ReadFailure(DatabaseError):
    consistency: Consistency
    received: int
    required: int
    numfailures: int
    data_present: bool

class WriteFailure(DatabaseError):
    consistency: Consistency
    received: int
    required: int
    numfailures: int
    write_type: WriteType

class FunctionFailure(DatabaseError):
    keyspace: str
    function: str
    arg_types: list[str]

class Overloaded(DatabaseError): ...
class IsBootstrapping(DatabaseError): ...
class TruncateError(DatabaseError): ...

class RateLimitReached(DatabaseError):
    op_type: OperationType
    rejected_by_coordinator: bool

class CqlSyntaxError(DatabaseError): ...
class InvalidRequest(DatabaseError): ...
class Unauthorized(DatabaseError): ...
class ServerConfigError(DatabaseError): ...

class AlreadyExists(DatabaseError):
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
class ClusterStateTokenError(ScyllaError): ...
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
