use std::error::Error;
use std::fmt;

use pyo3::prelude::*;
use scylla::serialize::row::{BuiltinTypeCheckError, BuiltinTypeCheckErrorKind};
use scylla::serialize::value::{
    BuiltinSerializationError, BuiltinSerializationErrorKind,
    BuiltinTypeCheckError as ValueTypeCheckError, MapSerializationErrorKind,
    SetOrListSerializationErrorKind, TupleSerializationErrorKind, UdtSerializationErrorKind,
};
use scylla::value::ValueOverflow;
use scylla_cql::serialize::value::VectorSerializationErrorKind;

use crate::errors::{
    MissingValue, PySerializationFailedError, SerializeFailedError, ToPyAttr,
    TypeMismatchSerializationError, UnsupportedTypeSerializationError,
    ValueOverflowSerializationError, WrongNumberOfValues, py_err, with_cause,
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
            py_err!(UnsupportedTypeSerializationError, message; parameter)
        }
        SerializationErrorKind::TypeMismatch { .. } => {
            py_err!(TypeMismatchSerializationError, message; parameter)
        }
        SerializationErrorKind::ValueOverflow => {
            py_err!(ValueOverflowSerializationError, message; parameter)
        }
        SerializationErrorKind::PythonInteropFailed { source } => with_cause(
            py_err!(PySerializationFailedError, message; parameter),
            source.clone_ref(py),
        ),
        SerializationErrorKind::ScyllaSerializeFailed { source } => {
            value_error_to_pyerr(source, py, message, parameter)
        }
    }
}

/// Maps a failure of one value, following collection element errors down to the innermost cause.
fn value_error_to_pyerr(
    err: &scylla::serialize::SerializationError,
    py: Python<'_>,
    message: String,
    parameter: &Option<ParameterReference>,
) -> PyErr {
    // An element failed in our own serializer; the location is only set on the top-level value.
    if let Some(e) = err.downcast_ref::<DriverSerializationError>() {
        return kind_to_pyerr(&e.kind, py, message, parameter);
    }
    if err.downcast_ref::<ValueTypeCheckError>().is_some() {
        return py_err!(TypeMismatchSerializationError, message; parameter);
    }
    if err.downcast_ref::<ValueOverflow>().is_some() {
        return py_err!(ValueOverflowSerializationError, message; parameter);
    }
    if let Some(e) = err.downcast_ref::<BuiltinSerializationError>() {
        return builtin_value_error_to_pyerr(&e.kind, py, message, parameter);
    }
    py_err!(SerializeFailedError, message; parameter)
}

#[deny(clippy::wildcard_enum_match_arm)]
fn builtin_value_error_to_pyerr(
    kind: &BuiltinSerializationErrorKind,
    py: Python<'_>,
    message: String,
    parameter: &Option<ParameterReference>,
) -> PyErr {
    let inner = match kind {
        BuiltinSerializationErrorKind::ValueOverflow => {
            return py_err!(ValueOverflowSerializationError, message; parameter);
        }
        BuiltinSerializationErrorKind::SetOrListError(
            SetOrListSerializationErrorKind::ElementSerializationFailed(inner),
        )
        | BuiltinSerializationErrorKind::VectorError(
            VectorSerializationErrorKind::ElementSerializationFailed(inner),
        )
        | BuiltinSerializationErrorKind::MapError(
            MapSerializationErrorKind::KeySerializationFailed(inner)
            | MapSerializationErrorKind::ValueSerializationFailed(inner),
        )
        | BuiltinSerializationErrorKind::TupleError(
            TupleSerializationErrorKind::ElementSerializationFailed { err: inner, .. },
        )
        | BuiltinSerializationErrorKind::UdtError(
            UdtSerializationErrorKind::FieldSerializationFailed { err: inner, .. },
        ) => inner,
        BuiltinSerializationErrorKind::SizeOverflow
        | BuiltinSerializationErrorKind::SetOrListError(_)
        | BuiltinSerializationErrorKind::VectorError(_)
        | BuiltinSerializationErrorKind::MapError(_)
        | BuiltinSerializationErrorKind::TupleError(_)
        | BuiltinSerializationErrorKind::UdtError(_) => {
            return py_err!(SerializeFailedError, message; parameter);
        }
        _ => unreachable!("clippy testifies that the match is exhaustive"),
    };
    value_error_to_pyerr(inner, py, message, parameter)
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

/// Maps a Rust driver serialization error, recovering our own `DriverSerializationError` or the
/// Rust driver's row check error from inside it; `message` is the full description including the failed operation.
pub(crate) fn serialization_error_to_pyerr(
    err: &scylla::serialize::SerializationError,
    message: String,
) -> PyErr {
    if let Some(e) = err.downcast_ref::<DriverSerializationError>() {
        return Python::attach(|py| e.to_pyerr(py, message));
    }
    if let Some(e) = err.downcast_ref::<BuiltinTypeCheckError>() {
        // Drops the internal Rust type name that the row check error prints.
        let message = message.replace(&e.to_string(), &e.kind.to_string());
        return row_type_check_error_to_pyerr(&e.kind, message);
    }
    py_err!(SerializeFailedError, message)
}

/// Maps the row checks done before serializing any value: the number of values and missing named values.
#[deny(clippy::wildcard_enum_match_arm)]
fn row_type_check_error_to_pyerr(kind: &BuiltinTypeCheckErrorKind, message: String) -> PyErr {
    match kind {
        BuiltinTypeCheckErrorKind::WrongColumnCount {
            rust_cols: received,
            cql_cols: expected,
        } => py_err!(WrongNumberOfValues, message; expected, received),
        BuiltinTypeCheckErrorKind::ValueMissingForColumn { name: parameter } => {
            py_err!(MissingValue, message; parameter)
        }
        BuiltinTypeCheckErrorKind::NoColumnWithName { .. }
        | BuiltinTypeCheckErrorKind::ColumnNameMismatch { .. } => {
            py_err!(SerializeFailedError, message)
        }
        _ => unreachable!("clippy testifies that the match is exhaustive"),
    }
}

impl From<DriverSerializationError> for scylla::serialize::SerializationError {
    fn from(err: DriverSerializationError) -> Self {
        scylla::serialize::SerializationError::new(err)
    }
}
