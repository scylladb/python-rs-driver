use std::fmt;

use pyo3::intern;
use pyo3::prelude::*;

use crate::errors::{
    PySerializationFailedError, SerializationError, TypeMismatchSerializationError,
    UnsupportedTypeSerializationError, ValueOverflowSerializationError,
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
    #[error("Python interop failed: {source}")]
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
    #[error("Python interop failed: {source}")]
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
}

impl From<DriverValueSerializationError> for PyErr {
    fn from(e: DriverValueSerializationError) -> PyErr {
        let message = e.to_string();
        let (err, cause) = match e {
            DriverValueSerializationError::UnsupportedType { .. } => {
                (UnsupportedTypeSerializationError::new_err(message), None)
            }
            DriverValueSerializationError::TypeMismatch { .. } => {
                (TypeMismatchSerializationError::new_err(message), None)
            }
            DriverValueSerializationError::ValueOverflow => {
                (ValueOverflowSerializationError::new_err(message), None)
            }
            DriverValueSerializationError::MixedSignDuration => {
                (SerializationError::new_err(message), None)
            }
            DriverValueSerializationError::PythonInteropFailed { source } => {
                (PySerializationFailedError::new_err(message), Some(*source))
            }
        };

        Python::attach(|py| {
            if let Some(cause) = cause {
                err.set_cause(py, Some(cause));
            }

            let _ = err.value(py).setattr(intern!(py, "bind_marker"), py.None());
        });

        err
    }
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
