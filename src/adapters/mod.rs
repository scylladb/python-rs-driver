//! Adapters that change values of user types into values the serializer accepts.

use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Mutex;

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::sync::MutexExt;
use pyo3::types::PyType;

type WriterMap = HashMap<TypeKey, Py<PyAny>>;

/// A Python type, compared by identity. The key owns the type, so its address stays valid.
#[derive(Debug, Clone)]
struct TypeKey {
    address: usize,
    _typ: Py<PyType>,
}

impl TypeKey {
    fn new(typ: Bound<'_, PyType>) -> Self {
        Self {
            address: address_of(&typ),
            _typ: typ.unbind(),
        }
    }
}

/// The address of `typ`. A map lookup uses it in place of a [`TypeKey`].
fn address_of(typ: &Bound<'_, PyType>) -> usize {
    typ.as_ptr() as usize
}

impl PartialEq for TypeKey {
    fn eq(&self, other: &Self) -> bool {
        self.address == other.address
    }
}

impl Eq for TypeKey {}

/// Hashes as the address alone, as [`Borrow<usize>`] requires.
impl Hash for TypeKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.address.hash(state);
    }
}

impl Borrow<usize> for TypeKey {
    fn borrow(&self) -> &usize {
        &self.address
    }
}

/// The Python `Adapters` registry.
#[pyclass(module = "scylla.adapters", name = "Adapters", frozen)]
pub(crate) struct PyAdapters {
    writers: Mutex<WriterMap>,
}

#[pymethods]
impl PyAdapters {
    #[new]
    fn new() -> Self {
        Self {
            writers: Mutex::default(),
        }
    }

    fn register_writer(
        &self,
        py: Python<'_>,
        cls: Bound<'_, PyType>,
        adapter: Bound<'_, PyAny>,
    ) -> PyResult<()> {
        if !adapter.is_callable() {
            return Err(PyTypeError::new_err("The adapter must be callable."));
        }
        self.writers
            .lock_py_attached(py)
            .unwrap()
            .insert(TypeKey::new(cls), adapter.unbind());
        Ok(())
    }
}

#[pymodule]
pub(crate) fn adapters(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyAdapters>()?;
    Ok(())
}
