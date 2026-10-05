use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyModule;

pub(crate) mod config;
pub(crate) mod execution;

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

create_exception!(scylla.errors, ExecuteError, ScyllaError);

create_exception!(scylla.errors, PrepareError, ScyllaError);

create_exception!(scylla.errors, SchemaAgreementError, ScyllaError);
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

create_exception!(scylla.errors, LoadBalancingPolicyError, ScyllaError);
create_exception!(scylla.errors, RetryPolicyError, ScyllaError);
create_exception!(scylla.errors, FutureCancelledError, PyException);
create_exception!(scylla.errors, QueryExhausted, PyException);
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
    module.add("PrepareError", py.get_type::<PrepareError>())?;
    module.add(
        "SchemaAgreementError",
        py.get_type::<SchemaAgreementError>(),
    )?;
    module.add("ExecuteError", py.get_type::<ExecuteError>())?;
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
    module.add(
        "LoadBalancingPolicyError",
        py.get_type::<LoadBalancingPolicyError>(),
    )?;
    module.add("RetryPolicyError", py.get_type::<RetryPolicyError>())?;
    module.add("QueryMetadataError", py.get_type::<QueryMetadataError>())?;
    module.add("QueryExhausted", py.get_type::<QueryExhausted>())?;
    module.add(
        "FutureCancelledError",
        py.get_type::<FutureCancelledError>(),
    )?;
    module.add(
        "SpeculativeExecutionPolicyError",
        py.get_type::<SpeculativeExecutionPolicyError>(),
    )?;
    Ok(())
}
