use crate::enums::PyConsistency;
use crate::errors::request::request_attempt_error_to_pyerr;
use pyo3::exceptions::PyBaseException;
use pyo3::prelude::*;
use scylla::errors::RequestAttemptError;
use scylla::policies::retry::RequestInfo;

#[pyclass(
    module = "scylla.policies.retry",
    name = "RequestInfo",
    frozen,
    from_py_object
)]
#[derive(Debug, Clone)]
pub(crate) struct PyRequestInfo {
    #[pyo3(get)]
    pub(crate) error: Py<PyBaseException>,
    #[pyo3(get)]
    pub(crate) is_idempotent: bool,
    #[pyo3(get)]
    pub(crate) consistency: PyConsistency,
    rust_error: RequestAttemptError,
}

impl PyRequestInfo {
    pub(crate) fn new(py: Python<'_>, value: &RequestInfo<'_>) -> Self {
        let error = request_attempt_error_to_pyerr(value.error, value.error.to_string());
        Self {
            error: error.into_value(py),
            is_idempotent: value.is_idempotent,
            consistency: value.consistency.into(),
            rust_error: value.error.clone(),
        }
    }

    pub fn to_request_info<'a>(&'a self) -> RequestInfo<'a> {
        RequestInfo::new(
            &self.rust_error,
            self.is_idempotent,
            self.consistency.into(),
        )
    }
}

#[pymethods]
impl PyRequestInfo {
    fn __str__(&self) -> String {
        format!(
            "RequestInfo(error={}, is_idempotent={}, consistency={:?})",
            self.rust_error, self.is_idempotent, self.consistency
        )
    }

    fn __repr__(&self) -> String {
        self.__str__()
    }
}
