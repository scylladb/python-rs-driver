use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;

/// Errors that can occur while accepting the CQL type of a reader from Python.
#[derive(Debug, thiserror::Error)]
pub(super) enum DriverReadKeyError {
    #[error(
        "invalid CQL type '{key}': expected a native, collection, tuple or vector type. \
         Use register_udt for a user defined type"
    )]
    NotACqlType { key: String },

    #[error("wrong number of type arguments in '{key}'")]
    WrongArgumentCount { key: String },

    /// Python failed while the driver read the key.
    #[error(transparent)]
    PythonError(#[from] PyErr),
}

impl DriverReadKeyError {
    /* Constructors */

    pub(super) fn not_a_cql_type(key: &Bound<'_, PyAny>) -> Self {
        Self::NotACqlType {
            key: key.to_string(),
        }
    }

    pub(super) fn wrong_argument_count(key: &Bound<'_, PyAny>) -> Self {
        Self::WrongArgumentCount {
            key: key.to_string(),
        }
    }
}

impl From<DriverReadKeyError> for PyErr {
    fn from(e: DriverReadKeyError) -> PyErr {
        match e {
            DriverReadKeyError::PythonError(err) => err,
            DriverReadKeyError::NotACqlType { .. }
            | DriverReadKeyError::WrongArgumentCount { .. } => PyTypeError::new_err(e.to_string()),
        }
    }
}
