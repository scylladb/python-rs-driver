use std::time::Duration;

use bytes::Bytes;
use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use scylla::errors::{CqlResponseKind, OperationType, WriteType};
use scylla::statement::Consistency;

use crate::enums::PyConsistency;
use crate::policies::retry::types::{PyCqlResponseKind, PyOperationType, PyWriteType};

/// Sets each `(name, value)` pair as an attribute of the exception instance; a failing attribute is logged and skipped.
pub(crate) fn with_attrs(py: Python<'_>, err: PyErr, attrs: &[(&str, &dyn ToPyAttr)]) -> PyErr {
    let exc = err.value(py);
    for (name, value) in attrs {
        if let Err(e) = value.to_py_attr(py).and_then(|v| exc.setattr(*name, v)) {
            log::error!("failed to set {name} on {}: {e}", err.get_type(py));
        }
    }
    err
}

/// Converts a field of a Rust error into the value of a Python exception attribute.
///
/// `IntoPyObject` cannot be implemented for Rust driver types such as `Consistency` because of
/// the orphan rule. This trait lets a call site pass them as they are, instead of converting each
/// one to its Python equivalent first.
pub(crate) trait ToPyAttr {
    fn to_py_attr<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>>;
}

impl<T: ToPyAttr + ?Sized> ToPyAttr for &T {
    fn to_py_attr<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        (**self).to_py_attr(py)
    }
}

macro_rules! impl_to_py_attr_by_value {
    ($ty:ty) => {
        impl ToPyAttr for $ty {
            fn to_py_attr<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
                self.into_bound_py_any(py)
            }
        }
    };
}

impl_to_py_attr_by_value!(bool);
impl_to_py_attr_by_value!(i32);
impl_to_py_attr_by_value!(usize);
impl_to_py_attr_by_value!(str);
impl_to_py_attr_by_value!(String);
impl_to_py_attr_by_value!(Vec<String>);
impl_to_py_attr_by_value!(Duration);
impl_to_py_attr_by_value!([u8]);
impl_to_py_attr_by_value!(Vec<u8>);

impl ToPyAttr for Bytes {
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
