use std::error::Error;
use std::fmt;

use pyo3::prelude::*;

use crate::errors::{
    PySerializationFailedError, SerializeFailedError, TypeMismatchSerializationError,
    UnsupportedTypeSerializationError, ValueOverflowSerializationError,
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
    /// An error occurred while interacting with Python objects during serialization.
    #[error("Python interop failed: {source}")]
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

    pub(crate) fn scylla_serialize_failed(source: scylla::serialize::SerializationError) -> Self {
        Self {
            kind: SerializationErrorKind::ScyllaSerializeFailed { source },
            location: None,
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

    /* Top-level location setters */

    pub(crate) fn at_parameter_index(mut self, index: usize) -> Self {
        self.location = Some(ParameterReference::Index(index));
        self
    }

    pub(crate) fn at_parameter_name(mut self, name: impl Into<Box<str>>) -> Self {
        self.location = Some(ParameterReference::Name(name.into()));
        self
    }
}

impl From<DriverSerializationError> for PyErr {
    fn from(e: DriverSerializationError) -> PyErr {
        let message = e.to_string();
        let (err, cause) = match e.kind {
            SerializationErrorKind::UnsupportedType { .. } => {
                (UnsupportedTypeSerializationError::new_err(message), None)
            }
            SerializationErrorKind::TypeMismatch { .. } => {
                (TypeMismatchSerializationError::new_err(message), None)
            }
            SerializationErrorKind::ValueOverflow => {
                (ValueOverflowSerializationError::new_err(message), None)
            }
            SerializationErrorKind::PythonInteropFailed { source } => {
                (PySerializationFailedError::new_err(message), Some(*source))
            }
            SerializationErrorKind::ScyllaSerializeFailed { .. } => {
                (SerializeFailedError::new_err(message), None)
            }
        };

        Python::attach(|py| {
            if let Some(cause) = cause {
                err.set_cause(py, Some(cause));
            }

            let value = err.value(py);
            let _ = match &e.location {
                Some(ParameterReference::Index(i)) => value.setattr("parameter", *i),
                Some(ParameterReference::Name(name)) => value.setattr("parameter", &**name),
                None => value.setattr("parameter", py.None()),
            };
        });

        err
    }
}

impl From<DriverSerializationError> for scylla::serialize::SerializationError {
    fn from(err: DriverSerializationError) -> Self {
        scylla::serialize::SerializationError::new(err)
    }
}
