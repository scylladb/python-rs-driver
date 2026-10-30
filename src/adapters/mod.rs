//! Adapters that convert values of user types when the driver writes or reads them.

pub(crate) mod resolved;

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

/// The Python `Adapters` registry. A session copies it when it connects.
#[pyclass(module = "scylla.adapters", name = "Adapters", frozen)]
pub(crate) struct PyAdapters {
    registry: Mutex<Adapters>,
}

#[pymethods]
impl PyAdapters {
    #[new]
    fn new() -> Self {
        Self {
            registry: Mutex::default(),
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
        self.registry
            .lock_py_attached(py)
            .unwrap()
            .writers
            .insert(TypeKey::new(cls), adapter.unbind());
        Ok(())
    }
}

impl PyAdapters {
    /// Copies the registry, so later changes do not reach a connected session.
    pub(crate) fn snapshot(&self, py: Python<'_>) -> Adapters {
        // Cloning a `Py` is safe here: `py` shows that this thread is attached to Python.
        self.registry.lock_py_attached(py).unwrap().clone()
    }
}

/// A frozen copy of the adapters registered for a session.
#[derive(Debug, Default, Clone)]
pub(crate) struct Adapters {
    writers: WriterMap,
}

impl Adapters {
    fn writer_for_type<'py>(
        &self,
        py: Python<'py>,
        typ: &Bound<'py, PyType>,
    ) -> Option<Bound<'py, PyAny>> {
        self.writers
            .get(&address_of(typ))
            .map(|adapter| adapter.bind(py).clone())
    }

    /// The writer for the type of `value`, else for its nearest base class.
    pub(crate) fn writer_for<'py>(&self, value: &Bound<'py, PyAny>) -> Option<Bound<'py, PyAny>> {
        if self.writers.is_empty() {
            return None;
        }
        let py = value.py();
        let typ = value.get_type();

        self.writer_for_type(py, &typ).or_else(|| {
            typ.mro()
                .iter()
                .skip(1)
                .find_map(|base| self.writer_for_type(py, base.cast::<PyType>().ok()?))
        })
    }
}

#[pymodule]
pub(crate) fn adapters(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyAdapters>()?;
    Ok(())
}
