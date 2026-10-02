use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;
use std::sync::OnceLock;

static UNSET_INSTANCE: OnceLock<Py<UnsetType>> = OnceLock::new();

#[pyclass(module = "scylla.statement")]
pub(crate) struct UnsetType;

#[pymethods]
impl UnsetType {
    #[new]
    fn new(py: Python<'_>) -> Py<UnsetType> {
        Self::get_instance(py)
    }

    fn __repr__(&self) -> &'static str {
        "UNSET"
    }

    fn __str__(&self) -> &'static str {
        "UNSET"
    }
}

impl UnsetType {
    pub(crate) fn get_instance(py: Python<'_>) -> Py<UnsetType> {
        UNSET_INSTANCE
            // There is nothing we can do when creating a global instance fails.
            .get_or_init(|| Py::new(py, UnsetType).expect("Failed to create UnsetType instance"))
            .clone_ref(py)
    }
}

/// A setting that may be left unset, represented on the Python side by `Unset`.
pub(crate) enum MaybeUnset<T> {
    Unset,
    Set(T),
}

impl<'a, 'py, T> FromPyObject<'a, 'py> for MaybeUnset<T>
where
    T: FromPyObject<'a, 'py>,
{
    type Error = T::Error;

    fn extract(obj: Borrowed<'a, 'py, PyAny>) -> Result<Self, Self::Error> {
        if obj.is_instance_of::<UnsetType>() {
            return Ok(Self::Unset);
        }
        obj.extract().map(Self::Set)
    }
}

impl<'py, T> IntoPyObject<'py> for MaybeUnset<T>
where
    T: IntoPyObject<'py>,
{
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        match self {
            Self::Unset => Ok(UnsetType::get_instance(py).into_bound(py).into_any()),
            Self::Set(value) => value.into_bound_py_any(py),
        }
    }
}

#[pymodule]
pub(crate) fn types(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<UnsetType>()?;
    Ok(())
}
