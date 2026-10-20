//! Adapters that change values of user types into values the serializer accepts.

use std::collections::HashMap;
use std::sync::Mutex;

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::sync::MutexExt;
use pyo3::types::PyType;

type AdapterMap = HashMap<TypeKey, (Py<PyType>, Py<PyAny>)>;

/// The identity of a Python type. The type object stored with the key keeps it valid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct TypeKey(usize);

impl TypeKey {
    fn of(typ: &Bound<'_, PyType>) -> Self {
        Self(typ.as_ptr() as usize)
    }
}

/// The Python `Adapters` registry. A session copies it when it connects.
#[pyclass(module = "scylla.serialization", name = "Adapters", frozen)]
pub(crate) struct PyAdapters {
    adapter_map: Mutex<AdapterMap>,
}

#[pymethods]
impl PyAdapters {
    #[new]
    fn new() -> Self {
        Self {
            adapter_map: Mutex::default(),
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
        self.adapter_map
            .lock_py_attached(py)
            .unwrap()
            .insert(TypeKey::of(&cls), (cls.unbind(), adapter.unbind()));
        Ok(())
    }
}

impl PyAdapters {
    /// Copies the registry, so later changes do not reach a connected session.
    pub(crate) fn snapshot(&self, py: Python<'_>) -> Adapters {
        Adapters {
            // Cloning a `Py` is safe here: `py` shows that this thread is attached to Python.
            adapter_map: self.adapter_map.lock_py_attached(py).unwrap().clone(),
        }
    }
}

/// A frozen copy of the adapters registered for a session.
#[derive(Debug, Default)]
pub(crate) struct Adapters {
    adapter_map: AdapterMap,
}

impl Adapters {
    fn get_adapter<'py>(
        &self,
        py: Python<'py>,
        typ: &Bound<'py, PyType>,
    ) -> Option<Bound<'py, PyAny>> {
        self.adapter_map
            .get(&TypeKey::of(typ))
            .map(|(_, adapter)| adapter.bind(py).clone())
    }

    /// Finds the adapter for the type of `value`, else for its nearest base class.
    pub(crate) fn find<'py>(&self, value: &Bound<'py, PyAny>) -> Option<Bound<'py, PyAny>> {
        if self.adapter_map.is_empty() {
            return None;
        }
        let py = value.py();
        let typ = value.get_type();

        self.get_adapter(py, &typ).or_else(|| {
            typ.mro()
                .iter()
                .skip(1)
                .find_map(|base| self.get_adapter(py, base.cast::<PyType>().ok()?))
        })
    }
}

#[pymodule]
pub(crate) fn serialization(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyAdapters>()?;
    Ok(())
}
