use std::time::Duration;

use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyBaseException;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use scylla::errors::{CqlResponseKind, OperationType, WriteType};
use scylla::statement::Consistency;

use crate::enums::PyConsistency;
use crate::errors::types::{PyCqlResponseKind, PyOperationType, PyWriteType};

/// Sets attributes on the exception instance; a failing `setattr` is logged and skips the remaining attributes.
pub(crate) fn with_attrs(
    err: PyErr,
    set: impl FnOnce(&Bound<'_, PyBaseException>) -> PyResult<()>,
) -> PyErr {
    Python::attach(|py| {
        if let Err(e) = set(err.value(py)) {
            log::error!("failed to set attributes on {}: {e}", err.get_type(py));
        }
    });
    err
}

/// Converts a field of a Rust error into the value of a Python exception attribute.
pub(crate) trait ToPyAttr {
    fn to_py_attr<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>>;
}

macro_rules! impl_to_py_attr_by_value {
    ($($ty:ty),+ $(,)?) => {
        $(impl ToPyAttr for $ty {
            fn to_py_attr<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
                self.into_bound_py_any(py)
            }
        })+
    };
}

impl_to_py_attr_by_value!(bool, i32, usize, str, Vec<String>);

/// Seconds, the unit of every timeout in the Python API.
impl ToPyAttr for Duration {
    fn to_py_attr<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.as_secs_f64().into_bound_py_any(py)
    }
}

impl ToPyAttr for [u8] {
    fn to_py_attr<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        Ok(PyBytes::new(py, self).into_any())
    }
}

impl ToPyAttr for Consistency {
    fn to_py_attr<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        PyConsistency::from(*self).into_bound_py_any(py)
    }
}

impl ToPyAttr for WriteType {
    fn to_py_attr<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        PyWriteType::from(self.clone()).into_bound_py_any(py)
    }
}

impl ToPyAttr for OperationType {
    fn to_py_attr<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        PyOperationType::from(self.clone()).into_bound_py_any(py)
    }
}

impl ToPyAttr for CqlResponseKind {
    fn to_py_attr<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        PyCqlResponseKind::from(*self).into_bound_py_any(py)
    }
}
