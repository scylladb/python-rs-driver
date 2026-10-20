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

/// The Python `Adapters` registry.
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

    fn register(
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

#[pymodule]
pub(crate) fn serialization(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyAdapters>()?;
    Ok(())
}
