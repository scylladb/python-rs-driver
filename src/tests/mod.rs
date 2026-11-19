mod cache_tests;
mod error_mapping_tests;
mod retry_policy_tests;

use pyo3::prelude::*;

/// Reads the attribute `name` of a Python object as a `T`.
fn attr<'py, T: FromPyObjectOwned<'py>>(obj: &Bound<'py, PyAny>, name: &str) -> T {
    let value = obj.getattr(name).unwrap();
    value.extract().map_err(Into::<PyErr>::into).unwrap()
}
