use std::error::Error;
use std::fmt;

use pyo3::prelude::*;

use crate::errors::execution::DriverExecuteError;
use crate::errors::{
    DecodeFailedError, PyConversionFailedError, RowFactoryError, RowIterationError,
    UnsupportedTypeDeserializationError, get_type_name, with_cause,
};

/* Row factory errors */

/// Errors that can occur while accepting a row factory from Python.
#[derive(Debug, thiserror::Error)]
pub enum DriverRowFactoryError {
    #[error(
        "invalid row factory '{type_name}': expected a built-in row factory, a callable taking \
         the row values, or an object with a 'prepare' method returning one"
    )]
    InvalidFactory { type_name: String },

    #[error(
        "invalid ClassRowFactory target '{type_name}': expected a callable accepting the column \
         names as keyword arguments"
    )]
    InvalidClass { type_name: String },

    #[error(
        "'prepare' returned an invalid row builder '{type_name}': expected a callable taking the \
         row values"
    )]
    UncallableBuilder { type_name: String },

    #[error("failed to look up 'prepare' on row factory '{type_name}'")]
    PrepareLookupFailed { type_name: String, cause: PyErr },
}

impl DriverRowFactoryError {
    /* Constructors */

    pub(crate) fn invalid_factory(obj: Borrowed<PyAny>) -> Self {
        let type_name = get_type_name(obj);
        Self::InvalidFactory { type_name }
    }

    pub(crate) fn invalid_class(obj: Borrowed<PyAny>) -> Self {
        let type_name = get_type_name(obj);
        Self::InvalidClass { type_name }
    }

    pub(crate) fn uncallable_builder(obj: Borrowed<PyAny>) -> Self {
        let type_name = get_type_name(obj);
        Self::UncallableBuilder { type_name }
    }

    pub(crate) fn prepare_lookup_failed(obj: Borrowed<PyAny>, cause: PyErr) -> Self {
        let type_name = get_type_name(obj);
        Self::PrepareLookupFailed { type_name, cause }
    }
}

impl From<DriverRowFactoryError> for PyErr {
    fn from(e: DriverRowFactoryError) -> PyErr {
        let err = RowFactoryError::new_err(e.to_string());
        match e {
            DriverRowFactoryError::PrepareLookupFailed { cause, .. } => with_cause(err, cause),
            _ => err,
        }
    }
}

/* Row iteration errors */

#[derive(Debug, thiserror::Error)]
pub enum DriverRowIterationError {
    /// An error occurred during deserialization of a CQL value into a Python object.
    #[error(transparent)]
    Deserialization(#[from] DriverDeserializationError),
    /// An error occurred while fetching the next page of results from the Rust driver during iteration.
    #[error("Row iteration error: failed to fetch next page of results")]
    FailedToFetchNextPage(#[source] DriverExecuteError),
    /// An error occurred in Python code during processing of a row.
    #[error("Row iteration error: a Python error occurred during processing of a row")]
    PythonError(#[from] PyErr),
}

impl From<DriverRowIterationError> for PyErr {
    fn from(e: DriverRowIterationError) -> PyErr {
        let message = e.to_string();
        match e {
            DriverRowIterationError::Deserialization(e) => e.into(),
            DriverRowIterationError::FailedToFetchNextPage(e) => {
                with_cause(RowIterationError::new_err(message), e.into())
            }
            DriverRowIterationError::PythonError(e) => {
                with_cause(RowIterationError::new_err(message), e)
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
#[derive(Debug, Clone, thiserror::Error)]
pub enum InnerSegment {
    /// An index into a sequence (list/set) where the error occurred.
    #[error("sequence[{0}]")]
    SequenceIndex(usize),
    /// An index into a map where the error occurred.
    #[error("map[{0}]")]
    MapIndex(usize),
    /// An index into a tuple where the error occurred.
    #[error("tuple[{0}]")]
    TupleIndex(usize),
    /// A field name in a UDT where the error occurred.
    #[error("udt.{0}")]
    UdtField(Box<str>),
    /// An index into a vector where the error occurred.
    #[error("vector[{0}]")]
    VectorIndex(usize),
}

#[derive(Debug, thiserror::Error)]
pub enum DeserializationErrorKind {
    /// The CQL type is not supported by the deserializer
    /// (e.g. an unknown custom type, or a new type added in Scylla that we haven't implemented yet).
    #[error("Unsupported CQL type: {cql}")]
    UnsupportedType { cql: Box<str> },
    /// An error occurred during deserialization in the Rust driver.
    #[error("{source}")]
    ScyllaDecodeFailed {
        source: scylla::deserialize::DeserializationError,
    },
    /// An error occurred during conversion to a Python object
    /// (e.g. invalid UTF-8, unsupported type for Python conversion, etc.).
    #[error("Python conversion failed")]
    PythonConversionFailed { source: Box<pyo3::PyErr> },
}

impl DeserializationErrorLocation {
    fn is_empty(&self) -> bool {
        self.column_name.is_none() && self.column_index.is_none() && self.inner.is_empty()
    }
}

impl fmt::Display for DeserializationErrorLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut parts: Vec<String> = Vec::new();

        if let Some(col) = &self.column_name {
            parts.push(format!("column_name={col}"));
        }
        if let Some(index) = &self.column_index {
            parts.push(format!("column_index={index}"));
        }
        parts.extend(self.inner.iter().map(InnerSegment::to_string));

        write!(f, "{}", parts.join(" -> "))
    }
}

impl fmt::Display for DriverDeserializationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.kind)?;
        if !self.location.is_empty() {
            write!(f, " ({})", self.location)?;
        }
        Ok(())
    }
}

impl Error for DriverDeserializationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.kind.source()
    }
}

impl DriverDeserializationError {
    /* Constructors */

    pub(crate) fn unsupported_type(cql: impl Into<Box<str>>) -> Self {
        Self {
            kind: DeserializationErrorKind::UnsupportedType { cql: cql.into() },
            location: DeserializationErrorLocation::default(),
        }
    }

    pub(crate) fn scylla_decode_failed(source: scylla::deserialize::DeserializationError) -> Self {
        Self {
            kind: DeserializationErrorKind::ScyllaDecodeFailed { source },
            location: DeserializationErrorLocation::default(),
        }
    }

    pub(crate) fn python_conversion_failed(source: pyo3::PyErr) -> Self {
        Self {
            kind: DeserializationErrorKind::PythonConversionFailed {
                source: Box::new(source),
            },
            location: DeserializationErrorLocation::default(),
        }
    }

    /* Column setters */

    pub(crate) fn at_column_name(mut self, name: impl Into<Box<str>>) -> Self {
        self.location.column_name = Some(name.into());
        self
    }

    pub(crate) fn at_column_index(mut self, index: usize) -> Self {
        self.location.column_index = Some(index);
        self
    }

    /* Inner path pushers (nesting) */

    fn push_inner(&mut self, segment: InnerSegment) {
        let mut v = self.location.inner.to_vec();
        v.push(segment);
        self.location.inner = v.into_boxed_slice();
    }

    pub(crate) fn in_sequence_index(mut self, index: usize) -> Self {
        self.push_inner(InnerSegment::SequenceIndex(index));
        self
    }

    pub(crate) fn in_map_index(mut self, index: usize) -> Self {
        self.push_inner(InnerSegment::MapIndex(index));
        self
    }

    pub(crate) fn in_tuple_index(mut self, index: usize) -> Self {
        self.push_inner(InnerSegment::TupleIndex(index));
        self
    }

    pub(crate) fn in_udt_field(mut self, field: impl Into<Box<str>>) -> Self {
        self.push_inner(InnerSegment::UdtField(field.into()));
        self
    }

    pub(crate) fn in_vector_index(mut self, index: usize) -> Self {
        self.push_inner(InnerSegment::VectorIndex(index));
        self
    }
}

/// Attaches `column_name`, `column_index` and `inner_path` to the Python exception; each is `None` when unknown.
fn attach_location_attrs(
    py: Python<'_>,
    err: &Bound<'_, pyo3::exceptions::PyBaseException>,
    location: &DeserializationErrorLocation,
) {
    let _ = match &location.column_name {
        Some(col_name) => err.setattr("column_name", &**col_name),
        None => err.setattr("column_name", py.None()),
    };

    let _ = match location.column_index {
        Some(col_index) => err.setattr("column_index", col_index),
        None => err.setattr("column_index", py.None()),
    };

    let _ = if location.inner.is_empty() {
        err.setattr("inner_path", py.None())
    } else {
        let inner_path: Vec<String> = location.inner.iter().map(InnerSegment::to_string).collect();
        err.setattr("inner_path", inner_path)
    };
}

impl From<DriverDeserializationError> for PyErr {
    fn from(e: DriverDeserializationError) -> PyErr {
        let message = e.to_string();
        let (err, cause) = match e.kind {
            DeserializationErrorKind::UnsupportedType { .. } => {
                (UnsupportedTypeDeserializationError::new_err(message), None)
            }
            DeserializationErrorKind::ScyllaDecodeFailed { .. } => {
                (DecodeFailedError::new_err(message), None)
            }
            DeserializationErrorKind::PythonConversionFailed { source } => {
                (PyConversionFailedError::new_err(message), Some(*source))
            }
        };

        Python::attach(|py| {
            if let Some(cause) = cause {
                err.set_cause(py, Some(cause));
            }
            attach_location_attrs(py, err.value(py), &e.location);
        });

        err
    }
}
