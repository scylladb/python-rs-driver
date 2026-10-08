use std::error::Error;
use std::fmt;

use pyo3::prelude::*;

use crate::errors::{
    InvalidValueSerializationError, PySerializationFailedError, SerializationError,
    SerializeFailedError, ToPyAttr, TypeMismatchSerializationError,
    UnsupportedTypeSerializationError, ValueOverflowSerializationError, py_err, with_cause,
};

/// Errors that can occur during serialization of Python values into CQL values.
#[derive(Debug)]
#[must_use]
pub(crate) struct DriverSerializationError {
    pub kind: SerializationErrorKind,
    pub location: Option<ParameterReference>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SerializationErrorKind {
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
    /// The number of values is not the number of bind markers.
    #[error("Wrong number of values: expected {expected}, got {received}")]
    WrongNumberOfValues { expected: usize, received: usize },
    /// The mapping has no value for a named bind marker; the location names it.
    #[error("Missing value")]
    MissingValue,
    /// An error occurred while interacting with Python objects during serialization.
    #[error("Python serialization failed")]
    PythonInteropFailed { source: Box<PyErr> },
    /// An error occurred in the Rust driver's serialization layer.
    #[error("{source}")]
    ScyllaSerializeFailed {
        source: scylla::serialize::SerializationError,
    },
}

/// References a parameter that failed to serialize, either by index or by name.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ParameterReference {
    #[error("parameter_index={0}")]
    Index(usize),
    #[error("parameter={0}")]
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

impl fmt::Display for DriverSerializationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.kind)?;
        if let Some(location) = &self.location {
            write!(f, " ({location})")?;
        }
        Ok(())
    }
}

impl Error for DriverSerializationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.kind.source()
    }
}

impl DriverSerializationError {
    /* Constructors */

    pub(crate) fn unsupported_type(cql: impl Into<Box<str>>) -> Self {
        Self {
            kind: SerializationErrorKind::UnsupportedType { cql: cql.into() },
            location: None,
        }
    }

    pub(crate) fn type_mismatch(expected: TypeExpected) -> Self {
        Self {
            kind: SerializationErrorKind::TypeMismatch { expected },
            location: None,
        }
    }

    pub(crate) fn value_overflow() -> Self {
        Self {
            kind: SerializationErrorKind::ValueOverflow,
            location: None,
        }
    }

    pub(crate) fn mixed_sign_duration() -> Self {
        Self {
            kind: SerializationErrorKind::MixedSignDuration,
            location: None,
        }
    }

    pub(crate) fn wrong_number_of_values(expected: usize, received: usize) -> Self {
        Self {
            kind: SerializationErrorKind::WrongNumberOfValues { expected, received },
            location: None,
        }
    }

    pub(crate) fn missing_value(name: impl Into<Box<str>>) -> Self {
        Self {
            kind: SerializationErrorKind::MissingValue,
            location: Some(ParameterReference::Name(name.into())),
        }
    }

    pub(crate) fn scylla_serialize_failed(
        source: scylla::serialize::SerializationError,
        location: Option<ParameterReference>,
    ) -> Self {
        Self {
            kind: SerializationErrorKind::ScyllaSerializeFailed { source },
            location,
        }
    }

    pub(crate) fn python_interop_failed(source: PyErr) -> Self {
        Self {
            kind: SerializationErrorKind::PythonInteropFailed {
                source: Box::new(source),
            },
            location: None,
        }
    }

    /// Attaches `location` to a `DriverSerializationError` inside `err` in place, so it is not wrapped twice;
    /// errors from elsewhere are wrapped with the location.
    pub(crate) fn locate(
        mut err: scylla::serialize::SerializationError,
        location: ParameterReference,
    ) -> scylla::serialize::SerializationError {
        if let Some(inner) = err.try_downcast_mut::<DriverSerializationError>() {
            inner.location = Some(location);
            return err;
        }

        let wrapped = Self::scylla_serialize_failed(err, Some(location));
        wrapped.into()
    }
}

impl DriverSerializationError {
    /// Builds the Python exception by reference, since the error may be reachable only through a shared Rust driver error.
    fn to_pyerr(&self, py: Python<'_>, message: String) -> PyErr {
        kind_to_pyerr(&self.kind, py, message, &self.location)
    }
}

fn kind_to_pyerr(
    kind: &SerializationErrorKind,
    py: Python<'_>,
    message: String,
    parameter: &Option<ParameterReference>,
) -> PyErr {
    match kind {
        SerializationErrorKind::UnsupportedType { .. } => {
            py_err!(py, UnsupportedTypeSerializationError, message; parameter)
        }
        SerializationErrorKind::TypeMismatch { .. } => {
            py_err!(py, TypeMismatchSerializationError, message; parameter)
        }
        SerializationErrorKind::ValueOverflow => {
            py_err!(py, ValueOverflowSerializationError, message; parameter)
        }
        SerializationErrorKind::MixedSignDuration => {
            py_err!(py, InvalidValueSerializationError, message; parameter)
        }
        SerializationErrorKind::WrongNumberOfValues { .. }
        | SerializationErrorKind::MissingValue => {
            py_err!(py, SerializationError, message; parameter)
        }
        SerializationErrorKind::PythonInteropFailed { source } => with_cause(
            py_err!(py, PySerializationFailedError, message; parameter),
            source.clone_ref(py),
        ),
        SerializationErrorKind::ScyllaSerializeFailed { .. } => {
            py_err!(py, SerializeFailedError, message; parameter)
        }
    }
}

impl ToPyAttr for ParameterReference {
    fn to_py_attr<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            ParameterReference::Index(index) => index.to_py_attr(py),
            ParameterReference::Name(name) => name.to_py_attr(py),
        }
    }
}

impl From<DriverSerializationError> for PyErr {
    fn from(e: DriverSerializationError) -> PyErr {
        Python::attach(|py| e.to_pyerr(py, e.to_string()))
    }
}

/// Maps a Rust driver serialization error, recovering our own `DriverSerializationError` from inside it;
/// `message` is the full description including the failed operation.
pub(crate) fn serialization_error_to_pyerr(
    py: Python<'_>,
    err: &scylla::serialize::SerializationError,
    message: String,
) -> PyErr {
    if let Some(e) = err.downcast_ref::<DriverSerializationError>() {
        return e.to_pyerr(py, message);
    }
    let parameter: Option<ParameterReference> = None;
    py_err!(py, SerializeFailedError, message; parameter)
}

impl From<DriverSerializationError> for scylla::serialize::SerializationError {
    fn from(err: DriverSerializationError) -> Self {
        scylla::serialize::SerializationError::new(err)
    }
}
