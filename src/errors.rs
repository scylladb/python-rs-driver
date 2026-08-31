// src/errors.rs
use pyo3::PyErr;
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::{PyModule, PyNone};
use scylla::errors::UseKeyspaceError as RustUseKeyspaceError;
use scylla::errors::{ClusterStateTokenError as RustClusterStateTokenError, TranslationError};
use std::error::Error;
use std::fmt;
use std::path::PathBuf;
/* Python exception classes */

create_exception!(errors, ScyllaError, PyException);

create_exception!(errors, RowIterationError, ScyllaError);

create_exception!(errors, DeserializationError, ScyllaError);
create_exception!(
    errors,
    UnsupportedTypeDeserializationError,
    DeserializationError
);
create_exception!(errors, DecodeFailedError, DeserializationError);
create_exception!(errors, PyConversionFailedError, DeserializationError);

create_exception!(errors, SessionConnectionError, ScyllaError);

create_exception!(errors, SessionConfigError, ScyllaError);

create_exception!(errors, StatementConversionError, ScyllaError);

create_exception!(errors, ExecuteError, ScyllaError);

create_exception!(errors, PrepareError, ScyllaError);

create_exception!(errors, SchemaAgreementError, ScyllaError);
create_exception!(errors, StatementConfigError, ScyllaError);

create_exception!(errors, BatchError, ScyllaError);

create_exception!(errors, SerializationError, ScyllaError);
create_exception!(
    errors,
    UnsupportedTypeSerializationError,
    SerializationError
);
create_exception!(errors, TypeMismatchSerializationError, SerializationError);
create_exception!(errors, ValueOverflowSerializationError, SerializationError);
create_exception!(errors, SerializeFailedError, SerializationError);
create_exception!(errors, PySerializationFailedError, SerializationError);

create_exception!(errors, ClusterStateTokenError, ScyllaError);
create_exception!(errors, UseKeyspaceError, ScyllaError);
create_exception!(errors, BadKeyspaceNameError, UseKeyspaceError);
create_exception!(errors, RequestError, UseKeyspaceError);
create_exception!(errors, KeyspaceNameMismatchError, UseKeyspaceError);
create_exception!(errors, RequestTimeoutError, UseKeyspaceError);
create_exception!(errors, RuntimeTaskJoinFailedError, UseKeyspaceError);
create_exception!(errors, AddressTranslationError, ScyllaError);
create_exception!(errors, HostFilterError, ScyllaError);
create_exception!(errors, TlsError, ScyllaError);

create_exception!(errors, LoadBalancingPolicyError, ScyllaError);
create_exception!(errors, RetryPolicyError, ScyllaError);
create_exception!(errors, SpeculativeExecutionPolicyError, ScyllaError);

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

/* Retry policy errors */

#[derive(Debug)]
#[must_use]
pub enum DriverRetryPolicyError {
    InvalidPolicy { type_name: String },
}

impl DriverRetryPolicyError {
    pub fn invalid_policy(obj: Borrowed<PyAny>) -> Self {
        let type_name = get_type_name(obj);
        Self::InvalidPolicy { type_name }
    }
}

impl From<DriverRetryPolicyError> for PyErr {
    fn from(e: DriverRetryPolicyError) -> PyErr {
        match e {
            DriverRetryPolicyError::InvalidPolicy { type_name } => {
                RetryPolicyError::new_err(format!(
                    "Invalid retry policy '{type_name}': Object does not implement the \
                     RetryPolicy protocol (missing required 'new_session' method)."
                ))
            }
        }
    }
}

/* Speculative execution policy errors */

/// Errors that can occur while extracting a speculative execution policy from a Python object.
#[derive(Debug, thiserror::Error)]
pub enum DriverSpeculativeExecutionPolicyError {
    #[error(
        "invalid speculative execution policy '{type_name}': expected an instance of 'SimpleSpeculativeExecutionPolicy'"
    )]
    InvalidPolicy { type_name: String },
}

impl DriverSpeculativeExecutionPolicyError {
    pub fn invalid_policy(obj: Borrowed<PyAny>) -> Self {
        Self::InvalidPolicy {
            type_name: get_type_name(obj),
        }
    }
}

impl From<DriverSpeculativeExecutionPolicyError> for PyErr {
    fn from(e: DriverSpeculativeExecutionPolicyError) -> PyErr {
        SpeculativeExecutionPolicyError::new_err(e.to_string())
    }
}

/* Row iteration errors */

#[derive(Debug)]
pub enum DriverRowIterationError {
    /// An error occurred during deserialization of a CQL value into a Python object.
    Deserialization(DriverDeserializationError),
    /// An error occurred while fetching the next page of results from the Rust driver during iteration.
    FailedToFetchNextPage(DriverExecuteError),
    /// An error occurred in Python code during processing of a row.
    PythonError(PyErr),
}

impl From<DriverRowIterationError> for PyErr {
    fn from(e: DriverRowIterationError) -> PyErr {
        match e {
            DriverRowIterationError::Deserialization(e) => e.into(),
            DriverRowIterationError::FailedToFetchNextPage(e) => {
                // Add extra context while preserving the original ExecuteErrorPy as cause
                Python::attach(|py| {
                    let err = RowIterationError::new_err(
                        "Row iteration error: failed to fetch next page of results",
                    );

                    // Wrap the inner ExecuteErrorPy as the cause
                    let cause: PyErr = e.into();
                    err.set_cause(py, Some(cause));
                    err
                })
            }
            DriverRowIterationError::PythonError(e) => {
                Python::attach(|py| {
                    let err = RowIterationError::new_err(
                        "Row iteration error: a Python error occurred during processing of a row",
                    );

                    // Attach original python exception as cause
                    err.set_cause(py, Some(e));

                    err
                })
            }
        }
    }
}

/* Deserialization errors */

/// Errors that can occur during deserialization of CQL values into Python objects.
#[derive(Debug)]
#[must_use]
pub struct DriverDeserializationError {
    pub kind: DeserializationErrorKind,
    pub location: DeserializationErrorLocation,
}

/// Structured information about where in the data the deserialization error occurred,
/// to provide better context in error messages and for debugging.
#[derive(Debug, Clone, Default)]
pub struct DeserializationErrorLocation {
    pub column_name: Option<Box<str>>,
    pub column_index: Option<usize>,
    pub inner: Box<[InnerSegment]>,
}

/// Represents a segment in the path to the value that failed to deserialize, for nested structures.
#[derive(Debug, Clone)]
pub enum InnerSegment {
    /// An index into a sequence (list/set) where the error occurred.
    SequenceIndex(usize),
    /// An index into a map where the error occurred.
    MapIndex(usize),
    /// An index into a tuple where the error occurred.
    TupleIndex(usize),
    /// A field name in a UDT where the error occurred.
    UdtField(Box<str>),
    /// An index into a vector where the error occurred.
    VectorIndex(usize),
}

#[derive(Debug)]
pub enum DeserializationErrorKind {
    /// The CQL type is not supported by the deserializer
    /// (e.g. an unknown custom type, or a new type added in Scylla that we haven't implemented yet).
    UnsupportedType { cql: Box<str> },
    /// An error occurred during deserialization in the Rust driver.
    ScyllaDecodeFailed {
        source: scylla::deserialize::DeserializationError,
    },
    /// An error occurred during conversion to a Python object
    /// (e.g. invalid UTF-8, unsupported type for Python conversion, etc.).
    PythonConversionFailed { source: Box<pyo3::PyErr> },
    /// Driver invariant violated: a deserializer was called for a mismatched ColumnType.
    /// This indicates a bug in our dispatch logic.
    WrongDeserializer { message: Box<str> },
}

impl DriverDeserializationError {
    /* Constructors */

    pub fn unsupported_type(cql: impl Into<Box<str>>) -> Self {
        Self {
            kind: DeserializationErrorKind::UnsupportedType { cql: cql.into() },
            location: DeserializationErrorLocation::default(),
        }
    }

    pub fn scylla_decode_failed(source: scylla::deserialize::DeserializationError) -> Self {
        Self {
            kind: DeserializationErrorKind::ScyllaDecodeFailed { source },
            location: DeserializationErrorLocation::default(),
        }
    }

    pub fn python_conversion_failed(source: pyo3::PyErr) -> Self {
        Self {
            kind: DeserializationErrorKind::PythonConversionFailed {
                source: Box::new(source),
            },
            location: DeserializationErrorLocation::default(),
        }
    }

    pub fn wrong_deserializer(expected: &'static str, got: impl Into<Box<str>>) -> Self {
        let got = got.into();
        let message = format!(
            "Internal driver error: wrong deserializer selected (expected {expected}, got {got})"
        );

        Self {
            kind: DeserializationErrorKind::WrongDeserializer {
                message: message.into_boxed_str(),
            },
            location: DeserializationErrorLocation::default(),
        }
    }

    /* Column setters */

    pub fn at_column_name(mut self, name: impl Into<Box<str>>) -> Self {
        self.location.column_name = Some(name.into());
        self
    }

    pub fn at_column_index(mut self, index: usize) -> Self {
        self.location.column_index = Some(index);
        self
    }

    /* Inner path pushers (nesting) */

    fn push_inner(&mut self, segment: InnerSegment) {
        let mut v = self.location.inner.to_vec();
        v.push(segment);
        self.location.inner = v.into_boxed_slice();
    }

    pub fn in_sequence_index(mut self, index: usize) -> Self {
        self.push_inner(InnerSegment::SequenceIndex(index));
        self
    }

    pub fn in_map_index(mut self, index: usize) -> Self {
        self.push_inner(InnerSegment::MapIndex(index));
        self
    }

    pub fn in_tuple_index(mut self, index: usize) -> Self {
        self.push_inner(InnerSegment::TupleIndex(index));
        self
    }

    pub fn in_udt_field(mut self, field: impl Into<Box<str>>) -> Self {
        self.push_inner(InnerSegment::UdtField(field.into()));
        self
    }

    pub fn in_vector_index(mut self, index: usize) -> Self {
        self.push_inner(InnerSegment::VectorIndex(index));
        self
    }
}

/// Helper function to format the location information into a human-readable string for error messages.
fn format_location(loc: &DeserializationErrorLocation) -> String {
    let mut parts: Vec<String> = Vec::new();

    if let Some(col) = &loc.column_name {
        parts.push(format!("column_name={col}"));
    }

    if let Some(index) = &loc.column_index {
        parts.push(format!("column_index={index}"));
    }

    for seg in &loc.inner {
        let s = match seg {
            InnerSegment::SequenceIndex(i) => format!("sequence[{i}]"),
            InnerSegment::MapIndex(i) => format!("map[{i}]"),
            InnerSegment::TupleIndex(i) => format!("tuple[{i}]"),
            InnerSegment::UdtField(f) => format!("udt.{f}"),
            InnerSegment::VectorIndex(i) => format!("vector[{i}]"),
        };
        parts.push(s);
    }

    if parts.is_empty() {
        String::new()
    } else {
        format!(" ({})", parts.join(" -> "))
    }
}

fn attach_deserialization_error_location<'py>(
    err: &Bound<'_, pyo3::exceptions::PyBaseException>,
    location: &DeserializationErrorLocation,
    py: Python<'py>,
) {
    // Attach column name for easier inspection in Python (if available, otherwise set to None).
    match &location.column_name {
        Some(col_name) => {
            let _ = err.setattr("column_name", col_name.to_string());
        }
        None => {
            let _ = err.setattr("column_name", PyNone::get(py));
        }
    }

    // Attach column index for easier inspection in Python (if available, otherwise set to None).
    match &location.column_index {
        Some(col_index) => {
            let _ = err.setattr("column_index", *col_index);
        }
        None => {
            let _ = err.setattr("column_index", PyNone::get(py));
        }
    }

    // Attach inner path for easier inspection in Python.
    // If there is no nested path information, set `None` for consistency with other optional location attributes.
    if location.inner.is_empty() {
        let _ = err.setattr("inner_path", PyNone::get(py));
    } else {
        let inner_path: Vec<String> = location
            .inner
            .iter()
            .map(|seg| match seg {
                InnerSegment::SequenceIndex(i) => format!("sequence[{i}]"),
                InnerSegment::MapIndex(i) => format!("map[{i}]"),
                InnerSegment::TupleIndex(i) => format!("tuple[{i}]"),
                InnerSegment::UdtField(f) => format!("udt.{f}"),
                InnerSegment::VectorIndex(i) => format!("vector[{i}]"),
            })
            .collect();
        let _ = err.setattr("inner_path", inner_path);
    }
}

/// Helper function to build a deserialization error PyErr with optional cause and location information attached.
fn build_deserialization_pyerr(
    py: Python<'_>,
    err: PyErr,
    location: &DeserializationErrorLocation,
    cause: Option<PyErr>,
) -> PyErr {
    if let Some(cause) = cause {
        err.set_cause(py, Some(cause));
    }

    attach_deserialization_error_location(err.value(py), location, py);
    err
}

impl From<DriverDeserializationError> for PyErr {
    fn from(e: DriverDeserializationError) -> PyErr {
        Python::attach(|py| {
            let location_as_string = format_location(&e.location);

            match e.kind {
                DeserializationErrorKind::UnsupportedType { cql } => {
                    let message = if location_as_string.is_empty() {
                        format!("Unsupported CQL type: {cql}")
                    } else {
                        format!("Unsupported CQL type: {cql}{location_as_string}")
                    };

                    build_deserialization_pyerr(
                        py,
                        UnsupportedTypeDeserializationError::new_err(message),
                        &e.location,
                        None,
                    )
                }

                DeserializationErrorKind::ScyllaDecodeFailed { source } => {
                    let base = source.to_string();
                    let message = if location_as_string.is_empty() {
                        base
                    } else {
                        format!("{base}{location_as_string}")
                    };

                    build_deserialization_pyerr(
                        py,
                        DecodeFailedError::new_err(message),
                        &e.location,
                        None,
                    )
                }

                DeserializationErrorKind::PythonConversionFailed { source } => {
                    let message = if location_as_string.is_empty() {
                        "Python conversion failed".to_string()
                    } else {
                        format!("Python conversion failed{location_as_string}")
                    };

                    build_deserialization_pyerr(
                        py,
                        PyConversionFailedError::new_err(message),
                        &e.location,
                        Some(*source),
                    )
                }

                DeserializationErrorKind::WrongDeserializer { message } => {
                    let message = if location_as_string.is_empty() {
                        message.to_string()
                    } else {
                        format!("{message}{location_as_string}")
                    };

                    build_deserialization_pyerr(
                        py,
                        PyConversionFailedError::new_err(message),
                        &e.location,
                        None,
                    )
                }
            }
        })
    }
}

/* Connection errors */

/// Errors that can occur during session creation and connection establishment.
#[derive(Debug)]
#[must_use]
pub enum DriverSessionConnectionError {
    /// The Tokio task running session creation failed to join.
    RuntimeTaskJoinFailed {
        message: String,
    },
    /// The Rust driver failed to establish a new session.
    NewSessionError {
        source: Box<scylla::errors::NewSessionError>,
    },

    PythonConversionError {
        source: PyErr,
    },
}

impl DriverSessionConnectionError {
    /* Constructors */

    pub fn runtime_task_join_failed(message: String) -> Self {
        Self::RuntimeTaskJoinFailed { message }
    }

    pub fn new_session_error(source: scylla::errors::NewSessionError) -> Self {
        Self::NewSessionError {
            source: Box::new(source),
        }
    }

    pub(crate) fn python_conversion_error(source: PyErr) -> Self {
        Self::PythonConversionError { source }
    }
}

impl From<DriverSessionConnectionError> for PyErr {
    fn from(e: DriverSessionConnectionError) -> PyErr {
        match e {
            DriverSessionConnectionError::RuntimeTaskJoinFailed { message } => {
                SessionConnectionError::new_err(format!(
                    "Internal driver error: runtime error while creating session: {message}"
                ))
            }

            DriverSessionConnectionError::NewSessionError { source } => {
                SessionConnectionError::new_err(format!("failed to establish session: {source}"))
            }

            DriverSessionConnectionError::PythonConversionError { source } => source,
        }
    }
}

// Allow converting a tokio::task::JoinError into SessionConnectionError
// so that callers that spawn tasks can map JoinError -> SessionConnectionError via the `From` trait.
impl From<tokio::task::JoinError> for DriverSessionConnectionError {
    fn from(err: tokio::task::JoinError) -> Self {
        DriverSessionConnectionError::runtime_task_join_failed(err.to_string())
    }
}

/* Session configuration errors */

/* TLS config errors */

/// Errors that can occur while building an [`openssl::ssl::SslContext`] from a TLS config.
#[allow(clippy::enum_variant_names)]
#[derive(Debug, thiserror::Error)]
pub enum TlsConfigError {
    #[error("failed to create SSL context builder: {0}")]
    ContextCreationFailed(String),

    #[error("failed to load default CA certificate locations: {0}")]
    DefaultVerifyPathsLoadFailed(String),

    #[error("failed to load CA locations (cafile: {cafile:?}, capath: {capath:?}): {cause}")]
    CaLocationsLoadFailed {
        cafile: Option<PathBuf>,
        capath: Option<PathBuf>,
        cause: String,
    },

    #[error("at least one of cafile, capath, or cadata must be specified")]
    NoCaLocationsSpecified,

    #[error("failed to load CA certificate data: {0}")]
    CaDataLoadFailed(String),

    #[error("failed to load certificate file '{path}': {cause}")]
    CertFileLoadFailed { path: PathBuf, cause: String },

    #[error("failed to load private key file '{path}': {cause}")]
    KeyFileLoadFailed { path: PathBuf, cause: String },
}

impl From<TlsConfigError> for PyErr {
    fn from(e: TlsConfigError) -> PyErr {
        TlsError::new_err(e.to_string())
    }
}

impl From<TlsConfigError> for DriverSessionConfigError {
    fn from(e: TlsConfigError) -> Self {
        DriverSessionConfigError::InvalidTlsConfig { source: e }
    }
}

/* Address parsing errors */

/// Error type for address parsing failures.
#[derive(Debug)]
pub enum AddressParseError {
    /// The Python object is not a valid address type (str, tuple(str, int), tuple(IpAddr, int)).
    InvalidType { type_name: String },
    /// A string could not be parsed into a SocketAddr.
    InvalidSocketAddr {
        addr: String,
        source: std::net::AddrParseError,
    },
    /// Failed to iterate over a sequence of addresses.
    IterationFailed { source: Box<PyErr> },
    /// An individual item in an address sequence failed to extract at the given index.
    InvalidItem { index: usize, source: Box<PyErr> },
}

impl AddressParseError {
    pub fn invalid_type(obj: Borrowed<PyAny>) -> Self {
        Self::InvalidType {
            type_name: get_type_name(obj),
        }
    }

    pub fn iteration_failed(source: PyErr) -> Self {
        Self::IterationFailed {
            source: Box::new(source),
        }
    }

    pub fn invalid_item(index: usize, source: PyErr) -> Self {
        Self::InvalidItem {
            index,
            source: Box::new(source),
        }
    }
}

impl fmt::Display for AddressParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidType { type_name } => write!(
                f,
                "Invalid address type: expected str | tuple(str, int) | tuple(ipaddress, int) or a sequence of these, got {type_name}"
            ),
            Self::InvalidSocketAddr { addr, source } => {
                write!(f, "Invalid socket address '{addr}': {source}")
            }
            Self::IterationFailed { .. } => {
                write!(f, "Failed to iterate over sequence of addresses")
            }
            Self::InvalidItem { index, .. } => {
                write!(f, "Error processing address at index {index}")
            }
        }
    }
}

impl From<AddressParseError> for PyErr {
    fn from(e: AddressParseError) -> PyErr {
        let message = e.to_string();
        match e {
            AddressParseError::IterationFailed { source } => Python::attach(|py| {
                let err = pyo3::exceptions::PyValueError::new_err(message);
                err.set_cause(py, Some(*source));
                err
            }),
            AddressParseError::InvalidItem { source, .. } => Python::attach(|py| {
                let err = pyo3::exceptions::PyValueError::new_err(message);
                err.set_cause(py, Some(*source));
                err
            }),
            _ => pyo3::exceptions::PyValueError::new_err(message),
        }
    }
}

/// Errors related to invalid session configuration.
#[derive(Debug)]
#[must_use]
pub enum DriverSessionConfigError {
    InvalidPortRange,

    ZeroDurationNotAllowed,

    /// The address object is not a valid type (str, tuple, or IpAddr tuple).
    InvalidAddress {
        source: AddressParseError,
    },

    /// The object does not have a `translate` method and is not a dict-based address translator.
    InvalidAddressTranslator {
        type_name: String,
    },

    /// The duration/timedelta object is neither a datetime.timedelta nor a non-negative finite float.
    InvalidDuration {
        type_name: String,
    },

    /// The object is not AuthenticatorProvider subclass.
    InvalidAuthenticatorProvider {
        type_name: String,
    },

    /// The object does not have a `next_timestamp` method and is not a built-in timestamp generator.
    InvalidTimestampGenerator {
        type_name: String,
    },

    /// The object does not have an `accept` method and is not a built-in host filter class.
    InvalidHostFilter {
        type_name: String,
    },

    /// An OpenSSL operation failed while building the TLS context.
    InvalidTlsConfig {
        source: TlsConfigError,
    },
}

impl DriverSessionConfigError {
    /* Constructors */
    pub fn invalid_duration(obj: Borrowed<PyAny>) -> Self {
        Self::InvalidDuration {
            type_name: get_type_name(obj),
        }
    }

    pub fn invalid_authenticator_provider(obj: Borrowed<PyAny>) -> Self {
        Self::InvalidAuthenticatorProvider {
            type_name: get_type_name(obj),
        }
    }

    pub fn invalid_address_translator(obj: Borrowed<PyAny>) -> Self {
        Self::InvalidAddressTranslator {
            type_name: get_type_name(obj),
        }
    }

    pub fn invalid_timestamp_generator(obj: Borrowed<PyAny>) -> Self {
        Self::InvalidTimestampGenerator {
            type_name: get_type_name(obj),
        }
    }

    pub fn invalid_host_filter(obj: Borrowed<PyAny>) -> Self {
        Self::InvalidHostFilter {
            type_name: get_type_name(obj),
        }
    }
}

/// Helper function to build a SessionConfigErrorPy with optional cause and index attributes.
fn build_session_config_pyerr(
    py: Python<'_>,
    message: impl Into<String>,
    cause: Option<PyErr>,
    index: Option<usize>,
) -> PyErr {
    let err = SessionConfigError::new_err(message.into());

    if let Some(cause) = cause {
        err.set_cause(py, Some(cause));
    }

    let inst = err.value(py);
    if let Some(index) = index {
        let _ = inst.setattr("index", index);
    }

    err
}

fn get_type_name(obj: Borrowed<PyAny>) -> String {
    obj.get_type()
        .name()
        .map(|n| n.to_string())
        .unwrap_or_else(|_| "UnknownType".to_string())
}

impl From<DriverSessionConfigError> for PyErr {
    fn from(e: DriverSessionConfigError) -> PyErr {
        Python::attach(|py| match e {
            DriverSessionConfigError::InvalidAddress { source } => {
                let message = "Failed to parse address".to_string();
                let cause: PyErr = source.into();
                build_session_config_pyerr(py, message, Some(cause), None)
            }

            DriverSessionConfigError::InvalidPortRange => {
                let message = "Invalid port range: start port must be less than or equal to end port, and both ports must be greater than or equal to 1024";
                build_session_config_pyerr(py, message, None, None)
            }

            DriverSessionConfigError::InvalidDuration { type_name } => {
                let message = format!(
                    "Expected a datetime.timedelta or a non-negative finite float (seconds), got: {type_name}"
                );
                build_session_config_pyerr(py, message, None, None)
            }

            DriverSessionConfigError::ZeroDurationNotAllowed => {
                let message = "Duration must be greater than zero.";
                build_session_config_pyerr(py, message, None, None)
            }

            DriverSessionConfigError::InvalidAuthenticatorProvider { type_name } => {
                let message =
                    format!("Expected an AuthenticatorProvider subclass, got {type_name}");
                build_session_config_pyerr(py, message, None, None)
            }

            DriverSessionConfigError::InvalidAddressTranslator { type_name } => {
                let message = format!(
                    "Expected a class implementing AddressTranslator protocol, got {type_name}"
                );
                build_session_config_pyerr(py, message, None, None)
            }

            DriverSessionConfigError::InvalidTimestampGenerator { type_name } => {
                let message = format!(
                    "Expected a class implementing TimestampGenerator protocol, got {type_name}"
                );
                build_session_config_pyerr(py, message, None, None)
            }

            DriverSessionConfigError::InvalidHostFilter { type_name } => {
                let message =
                    format!("Expected a class implementing HostFilter protocol, got {type_name}");
                build_session_config_pyerr(py, message, None, None)
            }

            DriverSessionConfigError::InvalidTlsConfig { source } => {
                let cause = TlsError::new_err(source.to_string());
                build_session_config_pyerr(py, "TLS configuration error", Some(cause), None)
            }
        })
    }
}

#[derive(Debug)]
#[must_use]
pub enum DriverAddressTranslationError {
    TranslationError {
        source: Box<TranslationError>,
    },

    InvalidAddressAtIndex {
        index: usize,
        source: AddressParseError,
    },
}

impl DriverAddressTranslationError {
    pub fn translation_error(source: TranslationError) -> Self {
        Self::TranslationError {
            source: Box::new(source),
        }
    }
    pub fn invalid_address(index: usize, source: AddressParseError) -> Self {
        Self::InvalidAddressAtIndex { index, source }
    }
}

impl From<TranslationError> for DriverAddressTranslationError {
    fn from(source: TranslationError) -> Self {
        Self::translation_error(source)
    }
}
impl From<DriverAddressTranslationError> for PyErr {
    fn from(e: DriverAddressTranslationError) -> PyErr {
        match e {
            DriverAddressTranslationError::TranslationError { source } => {
                AddressTranslationError::new_err(format!("Address translation failed: {source}"))
            }
            DriverAddressTranslationError::InvalidAddressAtIndex { index, source } => {
                Python::attach(|py| {
                    let message = format!("Error processing address at index {index}");
                    let err = AddressTranslationError::new_err(message);
                    let cause: PyErr = source.into();
                    err.set_cause(py, Some(cause));
                    let inst = err.value(py);
                    let _ = inst.setattr("index", index);
                    err
                })
            }
        }
    }
}

#[derive(Debug)]
#[must_use]
pub enum DriverHostFilterError {
    InvalidAddress { source: std::io::Error },
}

impl DriverHostFilterError {
    pub fn invalid_address(source: std::io::Error) -> Self {
        Self::InvalidAddress { source }
    }
}

impl From<DriverHostFilterError> for PyErr {
    fn from(e: DriverHostFilterError) -> PyErr {
        match e {
            DriverHostFilterError::InvalidAddress { source } => {
                let message = format!("Invalid address in host filter allow list: {source}");
                HostFilterError::new_err(message)
            }
        }
    }
}

/// Errors that can occur during conversion of Python objects into statements for execution.
#[derive(Debug)]
#[must_use]
pub enum DriverStatementConversionError {
    /// The provided statement argument is of an unsupported type.
    InvalidStatementType { type_name: String },
    /// Failed to convert a Python string object into a Rust string when extracting a statement.
    StatementStringConversionFailed { source: Box<PyErr> },
}

impl DriverStatementConversionError {
    /* Constructors */

    pub fn invalid_statement_type(obj: Borrowed<PyAny>) -> Self {
        let type_name = get_type_name(obj);
        Self::InvalidStatementType { type_name }
    }

    pub fn statement_string_conversion_failed(source: PyErr) -> Self {
        Self::StatementStringConversionFailed {
            source: Box::new(source),
        }
    }
}

impl From<DriverStatementConversionError> for PyErr {
    fn from(e: DriverStatementConversionError) -> PyErr {
        Python::attach(|py| match e {
            DriverStatementConversionError::InvalidStatementType { type_name: got } => {
                StatementConversionError::new_err(format!(
                    "Invalid statement type: expected a str, Statement, or PreparedStatement, got {got}"
                ))
            }

            DriverStatementConversionError::StatementStringConversionFailed { source } => {
                let err = StatementConversionError::new_err(
                    "Failed to convert statement string to Rust string",
                );

                err.set_cause(py, Some(*source));
                err
            }
        })
    }
}

/// Errors that can occur during execution of a query (session.execute),
/// excluding deserialization errors which are represented separately in RowIterationError.
#[derive(Debug)]
#[must_use]
pub enum DriverExecuteError {
    /// paging_state parameter in session.execute must be None.
    PagingStateMustBeNoneForUnpagedExecution,
    /// The Rust driver failed while executing a query.
    RustDriverExecutionError {
        source: Box<scylla::errors::ExecutionError>,
    },
    /// Serialization of values failed before execution.
    SerializationFailed {
        source: scylla::serialize::SerializationError,
    },
    /// The Tokio runtime task responsible for executing the query failed to join.
    RuntimeTaskJoinFailed { message: Box<str> },
}

impl DriverExecuteError {
    /* Constructors */

    pub fn paging_state_must_be_none_for_unpaged_execution() -> Self {
        Self::PagingStateMustBeNoneForUnpagedExecution
    }

    pub fn rust_driver_execution_error(source: scylla::errors::ExecutionError) -> Self {
        Self::RustDriverExecutionError {
            source: Box::new(source),
        }
    }

    pub fn runtime_task_join_failed(err: tokio::task::JoinError) -> Self {
        Self::RuntimeTaskJoinFailed {
            message: err.to_string().into_boxed_str(),
        }
    }

    pub fn serialization_failed(source: scylla::serialize::SerializationError) -> Self {
        Self::SerializationFailed { source }
    }
}

impl From<DriverExecuteError> for PyErr {
    fn from(e: DriverExecuteError) -> PyErr {
        match e {
            DriverExecuteError::PagingStateMustBeNoneForUnpagedExecution => {
                ExecuteError::new_err("Paging state must be None for unpaged execution")
            }

            DriverExecuteError::RustDriverExecutionError { source } => {
                let message = format!("Failed to execute statement: {source}");

                ExecuteError::new_err(message)
            }

            DriverExecuteError::RuntimeTaskJoinFailed { message } => ExecuteError::new_err(
                format!("Internal driver error: runtime error while executing query: {message}"),
            ),

            DriverExecuteError::SerializationFailed { source } => {
                let message = format!("Failed to serialize values: {source}");
                ExecuteError::new_err(message)
            }
        }
    }
}

// Allow converting a tokio::task::JoinError into ExecuteError
// so that callers that spawn tasks can map JoinError -> ExecuteError via the `From` trait.
impl From<tokio::task::JoinError> for DriverExecuteError {
    fn from(err: tokio::task::JoinError) -> Self {
        // Use the existing constructor which accepts JoinError
        DriverExecuteError::runtime_task_join_failed(err)
    }
}

/// Errors that can occur during preparation of a statement.
#[derive(Debug)]
#[must_use]
pub enum DriverPrepareError {
    /// The Rust driver failed while preparing a statement.
    #[allow(clippy::enum_variant_names)]
    RustDriverPrepareError {
        source: Box<scylla::errors::PrepareError>,
    },
    /// Attempted to prepare an already prepared statement.
    CannotPreparePreparedStatement,
}

impl DriverPrepareError {
    /* Constructors */

    pub fn rust_driver_prepare_error(source: scylla::errors::PrepareError) -> Self {
        Self::RustDriverPrepareError {
            source: Box::new(source),
        }
    }

    pub fn cannot_prepare_prepared_statement() -> Self {
        Self::CannotPreparePreparedStatement
    }
}

impl From<DriverPrepareError> for PyErr {
    fn from(e: DriverPrepareError) -> PyErr {
        match e {
            DriverPrepareError::RustDriverPrepareError { source } => {
                let message = format!("Failed to prepare statement: {source}");

                PrepareError::new_err(message)
            }

            DriverPrepareError::CannotPreparePreparedStatement => PrepareError::new_err(
                "Cannot prepare a PreparedStatement; expected a str or Statement",
            ),
        }
    }
}

/// Errors that can occur during schema agreement checks.
#[derive(Debug)]
#[must_use]
pub enum DriverSchemaAgreementError {
    /// The Rust driver failed to check for schema agreement.
    RustDriverSchemaAgreementError {
        source: Box<scylla::errors::SchemaAgreementError>,
    },
    /// The Tokio runtime task responsible for checking schema agreement failed to join.
    RuntimeTaskJoinFailed { message: Box<str> },
}

impl DriverSchemaAgreementError {
    /* Constructors */

    pub fn rust_driver_schema_agreement_error(
        source: scylla::errors::SchemaAgreementError,
    ) -> Self {
        Self::RustDriverSchemaAgreementError {
            source: Box::new(source),
        }
    }

    pub fn runtime_task_join_failed(err: tokio::task::JoinError) -> Self {
        Self::RuntimeTaskJoinFailed {
            message: err.to_string().into_boxed_str(),
        }
    }
}

impl From<DriverSchemaAgreementError> for PyErr {
    fn from(e: DriverSchemaAgreementError) -> PyErr {
        match e {
            DriverSchemaAgreementError::RustDriverSchemaAgreementError { source } => {
                let message = format!("Failed to check schema agreement: {source}");

                SchemaAgreementError::new_err(message)
            }

            DriverSchemaAgreementError::RuntimeTaskJoinFailed { message } => {
                SchemaAgreementError::new_err(format!(
                    "Internal driver error: runtime error while checking schema agreement: {message}"
                ))
            }
        }
    }
}

// Allow converting a tokio::task::JoinError into SchemaAgreementError
// so that callers that spawn tasks can map JoinError -> SchemaAgreementError via the `From` trait.
impl From<tokio::task::JoinError> for DriverSchemaAgreementError {
    fn from(err: tokio::task::JoinError) -> Self {
        DriverSchemaAgreementError::runtime_task_join_failed(err)
    }
}

#[derive(Debug)]
#[must_use]
pub enum DriverLoadBalancingPolicyError {
    InvalidPolicy { type_name: String },
    DefaultPolicyStringConversionFailed { source: Box<PyErr> },
}

impl DriverLoadBalancingPolicyError {
    pub fn invalid_policy(obj: Borrowed<PyAny>) -> Self {
        let type_name = obj
            .get_type()
            .name()
            .map(|n| n.to_string())
            .unwrap_or_else(|_| "UnknownType".to_string());
        Self::InvalidPolicy { type_name }
    }

    pub fn default_policy_string_conversion_failed(source: PyErr) -> Self {
        Self::DefaultPolicyStringConversionFailed {
            source: Box::new(source),
        }
    }
}

impl From<DriverLoadBalancingPolicyError> for PyErr {
    fn from(e: DriverLoadBalancingPolicyError) -> PyErr {
        match e {
            DriverLoadBalancingPolicyError::InvalidPolicy { type_name } => {
                LoadBalancingPolicyError::new_err(format!(
                    "Invalid load balancing policy '{type_name}': Object does not implement the \
                     LoadBalancingPolicy protocol (missing required 'pick_targets' method)."
                ))
            }
            DriverLoadBalancingPolicyError::DefaultPolicyStringConversionFailed { source } => {
                Python::attach(|py| {
                    let err = LoadBalancingPolicyError::new_err(
                        "String conversion failed while creating default load balancing policy",
                    );
                    err.set_cause(py, Some(*source));
                    err
                })
            }
        }
    }
}

/// Errors related to invalid statement configuration.
#[derive(Debug)]
#[must_use]
pub enum DriverStatementConfigError {
    /// The provided request timeout is not a non-negative finite number of seconds.
    InvalidRequestTimeout { value: f64 },
    /// An error occurred in Python code while handling a statement value.
    PythonConversionFailed { source: Box<PyErr> },
    /// The provided retry policy is invalid.
    InvalidRetryPolicy { source: Box<DriverRetryPolicyError> },
}

impl DriverStatementConfigError {
    /* Constructors */

    pub fn invalid_request_timeout(value: f64) -> Self {
        Self::InvalidRequestTimeout { value }
    }

    pub fn python_conversion_failed(source: PyErr) -> Self {
        Self::PythonConversionFailed {
            source: Box::new(source),
        }
    }

    pub fn invalid_retry_policy(source: DriverRetryPolicyError) -> Self {
        Self::InvalidRetryPolicy {
            source: Box::new(source),
        }
    }
}

impl From<DriverRetryPolicyError> for DriverStatementConfigError {
    fn from(e: DriverRetryPolicyError) -> Self {
        Self::invalid_retry_policy(e)
    }
}

impl From<DriverStatementConfigError> for PyErr {
    fn from(e: DriverStatementConfigError) -> PyErr {
        match e {
            DriverStatementConfigError::InvalidRequestTimeout { value } => {
                StatementConfigError::new_err(format!(
                    "timeout must be a non-negative, finite number (in seconds), got {value}"
                ))
            }
            DriverStatementConfigError::PythonConversionFailed { source } => Python::attach(|py| {
                let err = StatementConfigError::new_err(
                    "Python conversion failed while handling batch value",
                );

                err.set_cause(py, Some(*source));
                err
            }),
            DriverStatementConfigError::InvalidRetryPolicy { source } => Python::attach(|py| {
                let err = StatementConfigError::new_err("Invalid retry policy");
                err.set_cause(py, Some(PyErr::from(*source)));
                err
            }),
        }
    }
}

/// Errors related to batch execution and batch statement configuration.
#[derive(Debug)]
#[must_use]
pub enum DriverBatchError {
    /// The provided request timeout is not a non-negative finite number of seconds.
    InvalidRequestTimeout { value: f64 },
    /// An error occurred in Python code while handling a batch value.
    PythonConversionFailed { source: Box<PyErr> },
    /// The provided retry policy is invalid.
    InvalidRetryPolicy { source: Box<DriverRetryPolicyError> },
}

impl DriverBatchError {
    /* Constructors */

    pub fn invalid_request_timeout(value: f64) -> Self {
        Self::InvalidRequestTimeout { value }
    }

    pub fn python_conversion_failed(source: PyErr) -> Self {
        Self::PythonConversionFailed {
            source: Box::new(source),
        }
    }

    pub fn invalid_retry_policy(source: DriverRetryPolicyError) -> Self {
        Self::InvalidRetryPolicy {
            source: Box::new(source),
        }
    }
}

impl From<DriverRetryPolicyError> for DriverBatchError {
    fn from(e: DriverRetryPolicyError) -> Self {
        Self::invalid_retry_policy(e)
    }
}

impl From<DriverBatchError> for PyErr {
    fn from(e: DriverBatchError) -> PyErr {
        match e {
            DriverBatchError::InvalidRequestTimeout { value } => BatchError::new_err(format!(
                "timeout must be a non-negative, finite number (in seconds), got {value}"
            )),
            DriverBatchError::PythonConversionFailed { source } => Python::attach(|py| {
                let err =
                    BatchError::new_err("Python conversion failed while handling batch value");

                err.set_cause(py, Some(*source));
                err
            }),
            DriverBatchError::InvalidRetryPolicy { source } => Python::attach(|py| {
                let err = BatchError::new_err("Invalid retry policy for batch");
                err.set_cause(py, Some(PyErr::from(*source)));
                err
            }),
        }
    }
}

/* Serialization errors */

/// Errors that can occur during serialization of Python values into CQL values.
#[derive(Debug)]
#[must_use]
pub struct DriverSerializationError {
    pub kind: SerializationErrorKind,
    pub location: Option<ParameterReference>,
}

#[derive(Debug)]
pub enum SerializationErrorKind {
    /// Represents a segment in the path to the value that failed to serialize.
    UnsupportedType { cql: Box<str> },
    /// The Python value has the wrong top-level shape for the target CQL type.
    TypeMismatch { expected: TypeExpected },
    /// The Python value could not fit into the requested CQL representation.
    ValueOverflow,
    /// An error occurred while interacting with Python objects during serialization.
    PythonInteropFailed { source: Box<PyErr> },
    /// An error occurred in the Rust driver's serialization layer.
    ScyllaSerializeFailed {
        source: scylla::serialize::SerializationError,
    },
}

/// References a parameter that failed to serialize, either by index or by name.
#[derive(Debug)]
pub enum ParameterReference {
    Index(usize),
    Name(Box<str>),
}

#[derive(Debug)]
pub enum TypeExpected {
    /// Expected a list of values for a CQL list or set.
    List,
    /// Expected a tuple of values for a CQL tuple.
    Tuple,
    /// Expected an iterable of numbers for a CQL vector.
    Vector,
    /// Expected a set of values for a CQL set.
    Set,
    /// Expected a map for a CQL map.
    Map,
    /// Expected a user-defined type (Udt) for a CQL Udt.
    Udt,
}

impl fmt::Display for TypeExpected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TypeExpected::List => write!(f, "list"),
            TypeExpected::Tuple => write!(f, "tuple"),
            TypeExpected::Vector => write!(f, "vector"),
            TypeExpected::Set => write!(f, "set"),
            TypeExpected::Map => write!(f, "map"),
            TypeExpected::Udt => write!(f, "Udt"),
        }
    }
}

impl fmt::Display for DriverSerializationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let location = format_serialization_location(&self.location);

        match &self.kind {
            SerializationErrorKind::UnsupportedType { cql } => {
                if location.is_empty() {
                    write!(f, "Unsupported CQL type: {cql}")
                } else {
                    write!(f, "Unsupported CQL type: {cql}{location}")
                }
            }
            SerializationErrorKind::TypeMismatch { expected } => {
                if location.is_empty() {
                    write!(f, "Type mismatch: expected {expected}")
                } else {
                    write!(f, "Type mismatch: expected {expected}{location}")
                }
            }
            SerializationErrorKind::ValueOverflow => {
                if location.is_empty() {
                    write!(f, "Value overflow during serialization")
                } else {
                    write!(f, "Value overflow during serialization{location}")
                }
            }
            SerializationErrorKind::PythonInteropFailed { source } => {
                if location.is_empty() {
                    write!(f, "Python serialization failed: {source}")
                } else {
                    write!(f, "Python serialization failed: {source}{location}")
                }
            }
            SerializationErrorKind::ScyllaSerializeFailed { source } => {
                if location.is_empty() {
                    write!(f, "{source}")
                } else {
                    write!(f, "{source}{location}")
                }
            }
        }
    }
}

impl Error for DriverSerializationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            SerializationErrorKind::PythonInteropFailed { source } => Some(source.as_ref()),
            SerializationErrorKind::ScyllaSerializeFailed { source } => Some(source),
            _ => None,
        }
    }
}

impl DriverSerializationError {
    /* Constructors */

    pub fn unsupported_type(cql: impl Into<Box<str>>) -> Self {
        Self {
            kind: SerializationErrorKind::UnsupportedType { cql: cql.into() },
            location: None,
        }
    }

    pub fn type_mismatch(expected: TypeExpected) -> Self {
        Self {
            kind: SerializationErrorKind::TypeMismatch { expected },
            location: None,
        }
    }

    pub fn value_overflow() -> Self {
        Self {
            kind: SerializationErrorKind::ValueOverflow,
            location: None,
        }
    }

    pub fn scylla_serialize_failed(source: scylla::serialize::SerializationError) -> Self {
        Self {
            kind: SerializationErrorKind::ScyllaSerializeFailed { source },
            location: None,
        }
    }

    pub fn python_interop_failed(source: PyErr) -> Self {
        Self {
            kind: SerializationErrorKind::PythonInteropFailed {
                source: Box::new(source),
            },
            location: None,
        }
    }

    /* Top-level location setters */

    pub fn at_parameter_index(mut self, index: usize) -> Self {
        self.location = Some(ParameterReference::Index(index));
        self
    }

    pub fn at_parameter_name(mut self, name: impl Into<Box<str>>) -> Self {
        self.location = Some(ParameterReference::Name(name.into()));
        self
    }
}

/// Helper function to format serialization location information into a readable string.
fn format_serialization_location(loc: &Option<ParameterReference>) -> String {
    let mut parts: Vec<String> = Vec::new();

    if let Some(parameter) = &loc {
        match parameter {
            ParameterReference::Index(i) => parts.push(format!("parameter_index={i}")),
            ParameterReference::Name(n) => parts.push(format!("parameter={n}")),
        }
    }

    if parts.is_empty() {
        String::new()
    } else {
        format!(" ({})", parts.join(" -> "))
    }
}

/// Attaches serialization location attributes to the given Python exception instance.
fn attach_serialization_location_attrs(
    py: Python<'_>,
    err: &Bound<'_, pyo3::exceptions::PyBaseException>,
    loc: &Option<ParameterReference>,
) {
    match &loc {
        Some(ParameterReference::Index(i)) => {
            let _ = err.setattr("parameter", *i);
        }
        Some(ParameterReference::Name(name)) => {
            let _ = err.setattr("parameter", name.to_string());
        }
        None => {
            let _ = err.setattr("parameter", py.None());
        }
    }
}

fn build_serialization_pyerr(
    py: Python<'_>,
    err: PyErr,
    location: &Option<ParameterReference>,
    cause: Option<PyErr>,
) -> PyErr {
    if let Some(cause) = cause {
        err.set_cause(py, Some(cause));
    }

    attach_serialization_location_attrs(py, err.value(py), location);
    err
}

impl From<DriverSerializationError> for PyErr {
    fn from(e: DriverSerializationError) -> PyErr {
        Python::attach(|py| {
            let location_as_string = format_serialization_location(&e.location);

            match e.kind {
                SerializationErrorKind::UnsupportedType { cql } => {
                    let message = if location_as_string.is_empty() {
                        format!("Unsupported CQL type: {cql}")
                    } else {
                        format!("Unsupported CQL type: {cql}{location_as_string}")
                    };

                    build_serialization_pyerr(
                        py,
                        UnsupportedTypeSerializationError::new_err(message),
                        &e.location,
                        None,
                    )
                }

                SerializationErrorKind::TypeMismatch { expected } => {
                    let message = if location_as_string.is_empty() {
                        format!("Type mismatch: expected {expected}")
                    } else {
                        format!("Type mismatch: expected {expected}{location_as_string}")
                    };

                    build_serialization_pyerr(
                        py,
                        TypeMismatchSerializationError::new_err(message),
                        &e.location,
                        None,
                    )
                }

                SerializationErrorKind::ValueOverflow => {
                    let message = if location_as_string.is_empty() {
                        "Value overflow during serialization".to_string()
                    } else {
                        format!("Value overflow during serialization{location_as_string}")
                    };

                    build_serialization_pyerr(
                        py,
                        ValueOverflowSerializationError::new_err(message),
                        &e.location,
                        None,
                    )
                }

                SerializationErrorKind::PythonInteropFailed { source } => {
                    let message = if location_as_string.is_empty() {
                        "Python interop failed".to_string()
                    } else {
                        format!("Python interop failed{location_as_string}")
                    };

                    build_serialization_pyerr(
                        py,
                        PySerializationFailedError::new_err(message),
                        &e.location,
                        Some(*source),
                    )
                }

                SerializationErrorKind::ScyllaSerializeFailed { source } => {
                    let base = source.to_string();
                    let message = if location_as_string.is_empty() {
                        base
                    } else {
                        format!("{base}{location_as_string}")
                    };

                    build_serialization_pyerr(
                        py,
                        SerializeFailedError::new_err(message),
                        &e.location,
                        None,
                    )
                }
            }
        })
    }
}

impl From<DriverSerializationError> for scylla::serialize::SerializationError {
    fn from(err: DriverSerializationError) -> Self {
        scylla::serialize::SerializationError::new(err)
    }
}

/// Errors that can occur during use_keyspace operation on a session object.
#[derive(Debug)]
pub(crate) enum DriverUseKeyspaceError {
    BadKeyspaceName { message: String },
    RequestError { message: String },
    KeyspaceNameMismatch { message: String },
    RequestTimeout { message: String },
    RuntimeTaskJoinFailed { message: String },
}

impl From<RustUseKeyspaceError> for DriverUseKeyspaceError {
    fn from(e: RustUseKeyspaceError) -> Self {
        #[deny(clippy::wildcard_enum_match_arm)]
        match e {
            RustUseKeyspaceError::BadKeyspaceName(_) => Self::BadKeyspaceName {
                message: e.to_string(),
            },
            RustUseKeyspaceError::RequestError(_) => Self::RequestError {
                message: e.to_string(),
            },
            RustUseKeyspaceError::KeyspaceNameMismatch { .. } => Self::KeyspaceNameMismatch {
                message: e.to_string(),
            },
            RustUseKeyspaceError::RequestTimeout(_) => Self::RequestTimeout {
                message: e.to_string(),
            },
            _ => unreachable!("clippy testifies that the match is exhaustive"),
        }
    }
}

impl From<tokio::task::JoinError> for DriverUseKeyspaceError {
    fn from(e: tokio::task::JoinError) -> Self {
        let message = e.to_string();
        Self::RuntimeTaskJoinFailed {
            message: format!(
                "Internal driver error: runtime error while using keyspace: {message}"
            ),
        }
    }
}

impl From<DriverUseKeyspaceError> for PyErr {
    fn from(e: DriverUseKeyspaceError) -> Self {
        match e {
            DriverUseKeyspaceError::BadKeyspaceName { message } => {
                BadKeyspaceNameError::new_err(message)
            }
            DriverUseKeyspaceError::RequestError { message } => RequestError::new_err(message),
            DriverUseKeyspaceError::KeyspaceNameMismatch { message } => {
                KeyspaceNameMismatchError::new_err(message)
            }
            DriverUseKeyspaceError::RequestTimeout { message } => {
                RequestTimeoutError::new_err(message)
            }
            DriverUseKeyspaceError::RuntimeTaskJoinFailed { message } => {
                RuntimeTaskJoinFailedError::new_err(message)
            }
        }
    }
}

/// Errors that can occur during cluster state operations.
#[derive(Debug)]
pub(crate) enum DriverClusterStateTokenError {
    /// Failed to calculate token.
    TokenCalculation { message: String },
    /// Failed to serialize values required to compute partition key.
    Serialization { message: String },
    /// `ClusterState` doesn't currently have metadata for the requested table.
    UnknownTable { message: String },
    /// An FFI-related error occurred (e.g., Python conversion, node creation).
    PythonConversionFailed(PyErr),
}

impl DriverClusterStateTokenError {
    pub(crate) fn python_conversion_failed(err: PyErr) -> Self {
        Self::PythonConversionFailed(err)
    }
}

impl From<RustClusterStateTokenError> for DriverClusterStateTokenError {
    fn from(e: RustClusterStateTokenError) -> Self {
        #[deny(clippy::wildcard_enum_match_arm)]
        match e {
            RustClusterStateTokenError::TokenCalculation(e) => Self::TokenCalculation {
                message: e.to_string(),
            },
            RustClusterStateTokenError::Serialization(e) => Self::Serialization {
                message: e.to_string(),
            },
            RustClusterStateTokenError::UnknownTable { keyspace, table } => Self::UnknownTable {
                message: format!("Can't find metadata for requested table ({keyspace}.{table})."),
            },
            _ => unreachable!("clippy testifies that the match is exhaustive"),
        }
    }
}

impl From<DriverClusterStateTokenError> for PyErr {
    fn from(e: DriverClusterStateTokenError) -> PyErr {
        match e {
            DriverClusterStateTokenError::TokenCalculation { message } => {
                ClusterStateTokenError::new_err(message)
            }
            DriverClusterStateTokenError::Serialization { message } => {
                ClusterStateTokenError::new_err(message)
            }
            DriverClusterStateTokenError::UnknownTable { message } => {
                ClusterStateTokenError::new_err(message)
            }
            DriverClusterStateTokenError::PythonConversionFailed(err) => err,
        }
    }
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
    module.add(
        "SpeculativeExecutionPolicyError",
        py.get_type::<SpeculativeExecutionPolicyError>(),
    )?;
    Ok(())
}
