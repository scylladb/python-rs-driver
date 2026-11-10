use std::fmt;

use pyo3::prelude::*;

use crate::errors::{
    InvalidValueSerializationError, MissingValue, PySerializationFailedError,
    RowSerializationError, SerializeFailedError, ToPyAttr, TypeMismatchSerializationError,
    UnsupportedTypeSerializationError, ValueOverflowSerializationError, WrongNumberOfValues,
    py_err, with_cause,
};

/// Errors that can occur while serializing the values of one statement as a row.
#[derive(Debug, thiserror::Error)]
#[must_use]
pub(crate) enum DriverRowSerializationError {
    /// The number of values is not the number of bind markers.
    #[error("Wrong number of values: expected {expected}, got {received}")]
    WrongNumberOfValues { expected: usize, received: usize },
    /// The mapping has no value for a named bind marker.
    #[error("Missing value (bind_marker={bind_marker})")]
    MissingValue { bind_marker: Box<str> },
    /// Python code failed while it read the values container.
    #[error("Python serialization failed")]
    PythonInteropFailed { source: Box<PyErr> },
    /// The value for one bind marker failed to serialize.
    #[error("{source} ({bind_marker})")]
    ValueFailed {
        bind_marker: BindMarker,
        source: scylla::serialize::SerializationError,
    },
}

/// Errors that can occur during serialization of one Python value into a CQL value.
#[derive(Debug, thiserror::Error)]
#[must_use]
pub(crate) enum DriverValueSerializationError {
    /// Represents a segment in the path to the value that failed to serialize.
    #[error("Unsupported CQL type: {cql}")]
    UnsupportedType { cql: Box<str> },
    /// The Python value has the wrong top-level shape for the target CQL type.
    #[error("Type mismatch: expected {expected}")]
    TypeMismatch { expected: TypeExpected },
    /// The Python value could not fit into the requested CQL representation.
    #[error("Value overflow during serialization")]
    ValueOverflow,
    /// The duration fields have different signs, which CQL cannot represent.
    #[error("All duration fields must have the same sign")]
    MixedSignDuration,
    /// An error occurred while interacting with Python objects during serialization.
    #[error("Python serialization failed")]
    PythonInteropFailed { source: Box<PyErr> },
}

/// Identifies the bind marker of a value, by position or by name.
#[derive(Debug)]
pub(crate) enum BindMarker {
    Index(usize),
    Name(Box<str>),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TypeExpected {
    /// Expected a list of values for a CQL list or set.
    #[error("list")]
    List,
    /// Expected a tuple of values for a CQL tuple.
    #[error("tuple")]
    Tuple,
    /// Expected an iterable of numbers for a CQL vector.
    #[error("vector")]
    Vector,
    /// Expected a set of values for a CQL set.
    #[error("set")]
    Set,
    /// Expected a map for a CQL map.
    #[error("map")]
    Map,
    /// Expected a user-defined type (Udt) for a CQL Udt.
    #[error("Udt")]
    Udt,
}

impl fmt::Display for BindMarker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BindMarker::Index(index) => write!(f, "bind_marker={index}"),
            BindMarker::Name(name) => write!(f, "bind_marker={name}"),
        }
    }
}

impl DriverRowSerializationError {
    /* Constructors */

    pub(crate) fn wrong_number_of_values(expected: usize, received: usize) -> Self {
        Self::WrongNumberOfValues { expected, received }
    }

    pub(crate) fn missing_value(bind_marker: impl Into<Box<str>>) -> Self {
        Self::MissingValue {
            bind_marker: bind_marker.into(),
        }
    }

    pub(crate) fn python_interop_failed(source: PyErr) -> Self {
        Self::PythonInteropFailed {
            source: Box::new(source),
        }
    }

    pub(crate) fn value_failed(
        bind_marker: BindMarker,
        source: impl Into<scylla::serialize::SerializationError>,
    ) -> Self {
        Self::ValueFailed {
            bind_marker,
            source: source.into(),
        }
    }

    /// Builds the Python exception by reference, since the error may be reachable only through a shared Rust driver error.
    fn to_pyerr(&self, py: Python<'_>, message: String) -> PyErr {
        match self {
            Self::WrongNumberOfValues { expected, received } => {
                py_err!(py, WrongNumberOfValues, message; expected, received)
            }
            Self::MissingValue { bind_marker } => {
                let bind_marker: &str = bind_marker;
                py_err!(py, MissingValue, message; bind_marker)
            }
            Self::PythonInteropFailed { source } => with_cause(
                py_err!(RowSerializationError, message),
                source.clone_ref(py),
            ),
            Self::ValueFailed {
                bind_marker,
                source,
            } => value_error_to_pyerr(source, py, message, Some(bind_marker)),
        }
    }
}

impl DriverValueSerializationError {
    /* Constructors */

    pub(crate) fn unsupported_type(cql: impl Into<Box<str>>) -> Self {
        Self::UnsupportedType { cql: cql.into() }
    }

    pub(crate) fn type_mismatch(expected: TypeExpected) -> Self {
        Self::TypeMismatch { expected }
    }

    pub(crate) fn value_overflow() -> Self {
        Self::ValueOverflow
    }

    pub(crate) fn mixed_sign_duration() -> Self {
        Self::MixedSignDuration
    }

    pub(crate) fn python_interop_failed(source: PyErr) -> Self {
        Self::PythonInteropFailed {
            source: Box::new(source),
        }
    }

    fn to_pyerr(&self, py: Python<'_>, message: String, bind_marker: Option<&BindMarker>) -> PyErr {
        match self {
            Self::UnsupportedType { .. } => {
                py_err!(py, UnsupportedTypeSerializationError, message; bind_marker)
            }
            Self::TypeMismatch { .. } => {
                py_err!(py, TypeMismatchSerializationError, message; bind_marker)
            }
            Self::ValueOverflow => {
                py_err!(py, ValueOverflowSerializationError, message; bind_marker)
            }
            Self::MixedSignDuration => {
                py_err!(py, InvalidValueSerializationError, message; bind_marker)
            }
            Self::PythonInteropFailed { source } => with_cause(
                py_err!(py, PySerializationFailedError, message; bind_marker),
                source.clone_ref(py),
            ),
        }
    }
}

/// Maps a failure of one value.
fn value_error_to_pyerr(
    err: &scylla::serialize::SerializationError,
    py: Python<'_>,
    message: String,
    bind_marker: Option<&BindMarker>,
) -> PyErr {
    if let Some(e) = err.downcast_ref::<DriverValueSerializationError>() {
        return e.to_pyerr(py, message, bind_marker);
    }
    py_err!(py, SerializeFailedError, message; bind_marker)
}

impl ToPyAttr for BindMarker {
    fn to_py_attr<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            BindMarker::Index(index) => index.to_py_attr(py),
            BindMarker::Name(name) => name.to_py_attr(py),
        }
    }
}

impl From<DriverValueSerializationError> for PyErr {
    fn from(e: DriverValueSerializationError) -> PyErr {
        Python::attach(|py| e.to_pyerr(py, e.to_string(), None))
    }
}

/// Maps a Rust driver serialization error, recovering our own row or value error from
/// inside it; `message` is the full description including the failed operation.
pub(crate) fn serialization_error_to_pyerr(
    py: Python<'_>,
    err: &scylla::serialize::SerializationError,
    message: String,
) -> PyErr {
    if let Some(e) = err.downcast_ref::<DriverRowSerializationError>() {
        return e.to_pyerr(py, message);
    }
    value_error_to_pyerr(err, py, message, None)
}

impl From<DriverRowSerializationError> for scylla::serialize::SerializationError {
    fn from(err: DriverRowSerializationError) -> Self {
        scylla::serialize::SerializationError::new(err)
    }
}

impl From<DriverValueSerializationError> for scylla::serialize::SerializationError {
    fn from(err: DriverValueSerializationError) -> Self {
        scylla::serialize::SerializationError::new(err)
    }
}
