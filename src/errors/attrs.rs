use std::net::IpAddr;
use std::time::Duration;

use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyBaseException;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use scylla::errors::{CqlRequestKind, CqlResponseKind, OperationType, WriteType};
use scylla::statement::Consistency;
use uuid::Uuid;

use crate::enums::PyConsistency;
use crate::policies::retry::types::{
    PyCqlRequestKind, PyCqlResponseKind, PyOperationType, PyWriteType,
};

/// Sets attributes on the exception instance; a failing `setattr` leaves the exception without them.
pub(crate) fn with_attrs(
    err: PyErr,
    set: impl FnOnce(&Bound<'_, PyBaseException>) -> PyResult<()>,
) -> PyErr {
    Python::attach(|py| {
        let _ = set(err.value(py));
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

impl_to_py_attr_by_value!(bool, i32, usize, str, String, Vec<String>, Uuid, IpAddr);

impl<T: ToPyAttr + ?Sized> ToPyAttr for &T {
    fn to_py_attr<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        (**self).to_py_attr(py)
    }
}

impl ToPyAttr for CqlRequestKind {
    fn to_py_attr<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        PyCqlRequestKind::from(*self).into_bound_py_any(py)
    }
}

/// Seconds, the unit of every timeout in the Python API.
impl ToPyAttr for Duration {
    fn to_py_attr<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.as_secs_f64().into_bound_py_any(py)
    }
}

impl<T: ToPyAttr> ToPyAttr for Option<T> {
    fn to_py_attr<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Some(value) => value.to_py_attr(py),
            None => Ok(py.None().into_bound(py)),
        }
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
