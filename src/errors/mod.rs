use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyTimeoutError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyModule;

#[macro_use]
mod macros;

mod attrs;
pub(crate) mod config;
pub(crate) mod execution;
pub(crate) mod request;
pub(crate) mod types;

pub(crate) use attrs::{ToPyAttr, with_attrs};

/* Python exception classes */

create_exception!(scylla.errors, ScyllaError, PyException);

create_exception!(scylla.errors, RowIterationError, ScyllaError);

create_exception!(scylla.errors, DeserializationError, ScyllaError);
create_exception!(
    scylla.errors,
    UnsupportedTypeDeserializationError,
    DeserializationError
);
create_exception!(scylla.errors, DecodeFailedError, DeserializationError);
create_exception!(scylla.errors, PyConversionFailedError, DeserializationError);

create_exception!(scylla.errors, SessionConnectionError, ScyllaError);

create_exception!(scylla.errors, SessionConfigError, ScyllaError);

create_exception!(scylla.errors, StatementConversionError, ScyllaError);

create_exception!(scylla.errors, InternalDriverError, ScyllaError);

/* Invalid input, rejected before sending - retrying won't help */

create_exception_multi!(scylla.errors, BadQuery, (ScyllaError, PyValueError));
create_exception!(scylla.errors, ValuesTooLongForKey, BadQuery);
create_exception!(scylla.errors, TooManyStatementsInBatch, BadQuery);
create_exception!(scylla.errors, PartitionKeyExtractionFailed, BadQuery);
create_exception!(scylla.errors, PagingStateNotAllowed, BadQuery);

/* Failures while executing a request */

create_exception!(scylla.errors, ExecutionError, ScyllaError);
create_exception_multi!(
    scylla.errors,
    OperationTimedOut,
    (ExecutionError, PyTimeoutError)
);
create_exception!(scylla.errors, NoHostAvailable, ExecutionError);
create_exception!(scylla.errors, MetadataError, ExecutionError);
create_exception!(scylla.errors, SchemaAgreementError, ExecutionError);

create_exception!(scylla.errors, ConnectionPoolError, ExecutionError);
create_exception!(scylla.errors, ConnectionPoolBroken, ConnectionPoolError);
create_exception!(scylla.errors, PoolInitializing, ConnectionPoolError);
create_exception!(scylla.errors, NodeDisabledByHostFilter, ConnectionPoolError);

create_exception!(scylla.errors, PrepareError, ExecutionError);
create_exception!(scylla.errors, RepreparedIdChanged, PrepareError);
create_exception!(scylla.errors, RepreparedIdMissingInBatch, PrepareError);

create_exception!(scylla.errors, RequestFailedError, ExecutionError);
create_exception!(scylla.errors, BrokenConnection, RequestFailedError);
create_exception!(scylla.errors, ConnectionBusy, RequestFailedError);
create_exception!(scylla.errors, NonfinishedPagingState, RequestFailedError);
create_exception!(scylla.errors, ProtocolError, RequestFailedError);
create_exception!(scylla.errors, ResponseParseError, ProtocolError);
create_exception!(scylla.errors, RequestSerializationError, ProtocolError);
create_exception!(scylla.errors, UnexpectedResponse, ProtocolError);

/* Errors returned by the database */

create_exception!(scylla.errors, DatabaseError, RequestFailedError);

create_exception!(scylla.errors, RequestExecutionError, DatabaseError);
create_exception!(scylla.errors, Unavailable, RequestExecutionError);
create_exception!(scylla.errors, ReadTimeout, RequestExecutionError);
create_exception!(scylla.errors, WriteTimeout, RequestExecutionError);
create_exception!(scylla.errors, ReadFailure, RequestExecutionError);
create_exception!(scylla.errors, WriteFailure, RequestExecutionError);
create_exception!(scylla.errors, FunctionFailure, RequestExecutionError);
create_exception!(scylla.errors, Overloaded, RequestExecutionError);
create_exception!(scylla.errors, IsBootstrapping, RequestExecutionError);
create_exception!(scylla.errors, TruncateError, RequestExecutionError);
create_exception!(scylla.errors, RateLimitReached, RequestExecutionError);

create_exception!(scylla.errors, RequestValidationError, DatabaseError);
create_exception!(scylla.errors, CqlSyntaxError, RequestValidationError);
create_exception!(scylla.errors, InvalidRequest, RequestValidationError);
create_exception!(scylla.errors, Unauthorized, RequestValidationError);
create_exception!(scylla.errors, ServerConfigError, RequestValidationError);
create_exception!(scylla.errors, AlreadyExists, RequestValidationError);

create_exception!(scylla.errors, AuthenticationFailed, DatabaseError);
create_exception!(scylla.errors, ServerError, DatabaseError);
create_exception!(scylla.errors, ServerProtocolError, DatabaseError);
create_exception!(scylla.errors, Unprepared, DatabaseError);
create_exception!(scylla.errors, UnknownDatabaseError, DatabaseError);

/* Other errors */

create_exception!(scylla.errors, StatementConfigError, ScyllaError);

create_exception!(scylla.errors, BatchError, ScyllaError);

create_exception!(scylla.errors, SerializationError, ScyllaError);
create_exception!(
    scylla.errors,
    UnsupportedTypeSerializationError,
    SerializationError
);
create_exception!(
    scylla.errors,
    TypeMismatchSerializationError,
    SerializationError
);
create_exception!(
    scylla.errors,
    ValueOverflowSerializationError,
    SerializationError
);
create_exception!(scylla.errors, SerializeFailedError, SerializationError);
create_exception!(
    scylla.errors,
    PySerializationFailedError,
    SerializationError
);

create_exception!(scylla.errors, ClusterStateTokenError, ScyllaError);
create_exception!(scylla.errors, UseKeyspaceError, ScyllaError);
create_exception!(scylla.errors, BadKeyspaceNameError, UseKeyspaceError);
create_exception!(scylla.errors, RequestError, UseKeyspaceError);
create_exception!(scylla.errors, KeyspaceNameMismatchError, UseKeyspaceError);
create_exception!(scylla.errors, RequestTimeoutError, UseKeyspaceError);
create_exception!(scylla.errors, RuntimeTaskJoinFailedError, UseKeyspaceError);
create_exception!(scylla.errors, AddressTranslationError, ScyllaError);
create_exception!(scylla.errors, HostFilterError, ScyllaError);
create_exception!(scylla.errors, TlsError, ScyllaError);
create_exception!(scylla.errors, RowFactoryError, ScyllaError);

create_exception!(scylla.errors, LoadBalancingPolicyError, ScyllaError);
create_exception!(scylla.errors, RetryPolicyError, ScyllaError);
create_exception!(scylla.errors, FutureCancelledError, PyException);
create_exception!(scylla.errors, SpeculativeExecutionPolicyError, ScyllaError);

create_exception!(scylla.errors, QueryMetadataError, ScyllaError);

// Policy: DriverError types are pure Rust and contain PyErr only as source
// in cases where the error originated from Python code (e.g. during extraction or user callbacks).
// Conversion to PyErr happens at the boundary (e.g. in #[pymethods] implementations)
// using the From<DriverError> for PyErr implementation, which maps each DriverError variant to
// an appropriate Python exception class and attaches any relevant information as attributes or causes.

// For errors originating from Python code, we attach the original PyErr as the cause
// in the PyErr going back to Python, so that users can inspect the original exception type and message if needed.

// For errors originating from the Rust driver, we include the original error message in our custom Python exception
// and attach any relevant structured information as attributes.

// For errors originating from our own Rust code, we create a custom Python exception with a descriptive message,
// and we can include any relevant information in the message or as attributes.

/// Sets `cause` as the `__cause__` of `err`.
pub(crate) fn with_cause(err: PyErr, cause: PyErr) -> PyErr {
    Python::attach(|py| err.set_cause(py, Some(cause)));
    err
}

pub(crate) fn get_type_name(obj: Borrowed<PyAny>) -> String {
    obj.get_type()
        .name()
        .map(|n| n.to_string())
        .unwrap_or_else(|_| "UnknownType".to_string())
}

#[pymodule]
pub(crate) fn errors(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("ScyllaError", py.get_type::<ScyllaError>())?;
    module.add("RowIterationError", py.get_type::<RowIterationError>())?;
    module.add(
        "DeserializationError",
        py.get_type::<DeserializationError>(),
    )?;
    module.add(
        "UnsupportedTypeDeserializationError",
        py.get_type::<UnsupportedTypeDeserializationError>(),
    )?;
    module.add("DecodeFailedError", py.get_type::<DecodeFailedError>())?;
    module.add(
        "PyConversionFailedError",
        py.get_type::<PyConversionFailedError>(),
    )?;
    module.add(
        "SessionConnectionError",
        py.get_type::<SessionConnectionError>(),
    )?;
    module.add("SessionConfigError", py.get_type::<SessionConfigError>())?;
    module.add(
        "StatementConversionError",
        py.get_type::<StatementConversionError>(),
    )?;
    module.add("InternalDriverError", py.get_type::<InternalDriverError>())?;
    module.add("BadQuery", py.get_type::<BadQuery>())?;
    module.add("ValuesTooLongForKey", py.get_type::<ValuesTooLongForKey>())?;
    module.add(
        "TooManyStatementsInBatch",
        py.get_type::<TooManyStatementsInBatch>(),
    )?;
    module.add(
        "PartitionKeyExtractionFailed",
        py.get_type::<PartitionKeyExtractionFailed>(),
    )?;
    module.add(
        "PagingStateNotAllowed",
        py.get_type::<PagingStateNotAllowed>(),
    )?;
    module.add("ExecutionError", py.get_type::<ExecutionError>())?;
    module.add("OperationTimedOut", py.get_type::<OperationTimedOut>())?;
    module.add("NoHostAvailable", py.get_type::<NoHostAvailable>())?;
    module.add("MetadataError", py.get_type::<MetadataError>())?;
    module.add(
        "SchemaAgreementError",
        py.get_type::<SchemaAgreementError>(),
    )?;
    module.add("ConnectionPoolError", py.get_type::<ConnectionPoolError>())?;
    module.add(
        "ConnectionPoolBroken",
        py.get_type::<ConnectionPoolBroken>(),
    )?;
    module.add("PoolInitializing", py.get_type::<PoolInitializing>())?;
    module.add(
        "NodeDisabledByHostFilter",
        py.get_type::<NodeDisabledByHostFilter>(),
    )?;
    module.add("PrepareError", py.get_type::<PrepareError>())?;
    module.add("RepreparedIdChanged", py.get_type::<RepreparedIdChanged>())?;
    module.add(
        "RepreparedIdMissingInBatch",
        py.get_type::<RepreparedIdMissingInBatch>(),
    )?;
    module.add("RequestFailedError", py.get_type::<RequestFailedError>())?;
    module.add("BrokenConnection", py.get_type::<BrokenConnection>())?;
    module.add("ConnectionBusy", py.get_type::<ConnectionBusy>())?;
    module.add(
        "NonfinishedPagingState",
        py.get_type::<NonfinishedPagingState>(),
    )?;
    module.add("ProtocolError", py.get_type::<ProtocolError>())?;
    module.add("ResponseParseError", py.get_type::<ResponseParseError>())?;
    module.add(
        "RequestSerializationError",
        py.get_type::<RequestSerializationError>(),
    )?;
    module.add("UnexpectedResponse", py.get_type::<UnexpectedResponse>())?;
    module.add("DatabaseError", py.get_type::<DatabaseError>())?;
    module.add(
        "RequestExecutionError",
        py.get_type::<RequestExecutionError>(),
    )?;
    module.add("Unavailable", py.get_type::<Unavailable>())?;
    module.add("ReadTimeout", py.get_type::<ReadTimeout>())?;
    module.add("WriteTimeout", py.get_type::<WriteTimeout>())?;
    module.add("ReadFailure", py.get_type::<ReadFailure>())?;
    module.add("WriteFailure", py.get_type::<WriteFailure>())?;
    module.add("FunctionFailure", py.get_type::<FunctionFailure>())?;
    module.add("Overloaded", py.get_type::<Overloaded>())?;
    module.add("IsBootstrapping", py.get_type::<IsBootstrapping>())?;
    module.add("TruncateError", py.get_type::<TruncateError>())?;
    module.add("RateLimitReached", py.get_type::<RateLimitReached>())?;
    module.add(
        "RequestValidationError",
        py.get_type::<RequestValidationError>(),
    )?;
    module.add("CqlSyntaxError", py.get_type::<CqlSyntaxError>())?;
    module.add("InvalidRequest", py.get_type::<InvalidRequest>())?;
    module.add("Unauthorized", py.get_type::<Unauthorized>())?;
    module.add("ServerConfigError", py.get_type::<ServerConfigError>())?;
    module.add("AlreadyExists", py.get_type::<AlreadyExists>())?;
    module.add(
        "AuthenticationFailed",
        py.get_type::<AuthenticationFailed>(),
    )?;
    module.add("ServerError", py.get_type::<ServerError>())?;
    module.add("ServerProtocolError", py.get_type::<ServerProtocolError>())?;
    module.add("Unprepared", py.get_type::<Unprepared>())?;
    module.add(
        "UnknownDatabaseError",
        py.get_type::<UnknownDatabaseError>(),
    )?;
    module.add(
        "StatementConfigError",
        py.get_type::<StatementConfigError>(),
    )?;
    module.add("BatchError", py.get_type::<BatchError>())?;
    module.add("SerializationError", py.get_type::<SerializationError>())?;
    module.add(
        "UnsupportedTypeSerializationError",
        py.get_type::<UnsupportedTypeSerializationError>(),
    )?;
    module.add(
        "TypeMismatchSerializationError",
        py.get_type::<TypeMismatchSerializationError>(),
    )?;
    module.add(
        "ValueOverflowSerializationError",
        py.get_type::<ValueOverflowSerializationError>(),
    )?;
    module.add(
        "SerializeFailedError",
        py.get_type::<SerializeFailedError>(),
    )?;
    module.add(
        "PySerializationFailedError",
        py.get_type::<PySerializationFailedError>(),
    )?;
    module.add(
        "ClusterStateTokenError",
        py.get_type::<ClusterStateTokenError>(),
    )?;
    module.add("UseKeyspaceError", py.get_type::<UseKeyspaceError>())?;
    module.add(
        "BadKeyspaceNameError",
        py.get_type::<BadKeyspaceNameError>(),
    )?;
    module.add("RequestError", py.get_type::<RequestError>())?;
    module.add(
        "KeyspaceNameMismatchError",
        py.get_type::<KeyspaceNameMismatchError>(),
    )?;
    module.add("RequestTimeoutError", py.get_type::<RequestTimeoutError>())?;
    module.add(
        "RuntimeTaskJoinFailedError",
        py.get_type::<RuntimeTaskJoinFailedError>(),
    )?;
    module.add(
        "AddressTranslationError",
        py.get_type::<AddressTranslationError>(),
    )?;
    module.add("HostFilterError", py.get_type::<HostFilterError>())?;
    module.add("TlsError", py.get_type::<TlsError>())?;
    module.add("RowFactoryError", py.get_type::<RowFactoryError>())?;
    module.add(
        "LoadBalancingPolicyError",
        py.get_type::<LoadBalancingPolicyError>(),
    )?;
    module.add("RetryPolicyError", py.get_type::<RetryPolicyError>())?;
    module.add("QueryMetadataError", py.get_type::<QueryMetadataError>())?;
    module.add(
        "FutureCancelledError",
        py.get_type::<FutureCancelledError>(),
    )?;
    module.add(
        "SpeculativeExecutionPolicyError",
        py.get_type::<SpeculativeExecutionPolicyError>(),
    )?;
    module.add_class::<types::PyCqlResponseKind>()?;
    module.add_class::<types::PyOperationType>()?;
    module.add_class::<types::PyWriteType>()?;
    Ok(())
}
