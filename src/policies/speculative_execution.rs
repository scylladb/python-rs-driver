use crate::errors::DriverSpeculativeExecutionPolicyError;
use crate::session_builder::PyDuration;
use pyo3::prelude::{PyModule, PyModuleMethods};
use pyo3::{Borrowed, Bound, FromPyObject, PyAny, PyResult, Python, pyclass, pymethods, pymodule};
use scylla::policies::speculative_execution::{
    SimpleSpeculativeExecutionPolicy, SpeculativeExecutionPolicy,
};
use std::sync::Arc;
use std::time::Duration;

// TODO
// Custom, Python-implemented policies are not supported yet. They are not urgent - the
// legacy test suite we are migrating does not use custom speculative execution policies.
//
// Implementing them would first require exposing metrics to Python: the Rust
// `SpeculativeExecutionPolicy` trait passes a `Context` carrying the session metrics, so
// without them a custom policy would get an object with nothing on it.
//
// We would also need to decide on the shape of the policy. The Rust trait is queried once
// per request, while the legacy python driver one is called per execution, which gives the
// user freedom to change the delay eg. after the 5th attempt. Allowing that would require
// implementing it in the Rust driver first, so we need to decide whether exposing the
// narrower Rust-shaped interface is ok for us.

/// Built-in speculative execution policy that starts a new execution of the request
/// every `delay` seconds, at most `max_attempts` times.
#[pyclass(name = "SimpleSpeculativeExecutionPolicy", frozen)]
#[derive(Debug)]
pub(crate) struct PySimpleSpeculativeExecutionPolicy {
    pub(crate) inner: Arc<SimpleSpeculativeExecutionPolicy>,
}

#[pymethods]
impl PySimpleSpeculativeExecutionPolicy {
    #[new]
    fn new(delay: PyDuration, max_attempts: usize) -> Self {
        Self {
            inner: Arc::new(SimpleSpeculativeExecutionPolicy {
                max_retry_count: max_attempts,
                retry_interval: delay.0,
            }),
        }
    }

    #[getter]
    fn get_delay(&self) -> Duration {
        self.inner.retry_interval
    }

    #[getter]
    fn get_max_attempts(&self) -> usize {
        self.inner.max_retry_count
    }
}

/// Python-facing input type for speculative execution policy. Extracts from the built-in
/// `PySimpleSpeculativeExecutionPolicy`.
pub(crate) struct PySpeculativeExecutionPolicy {
    inner: Arc<dyn SpeculativeExecutionPolicy>,
}

impl PySpeculativeExecutionPolicy {
    pub(crate) fn into_inner(self) -> Arc<dyn SpeculativeExecutionPolicy> {
        self.inner
    }
}

impl<'py> FromPyObject<'_, 'py> for PySpeculativeExecutionPolicy {
    type Error = DriverSpeculativeExecutionPolicyError;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(policy) = obj.cast::<PySimpleSpeculativeExecutionPolicy>() {
            return Ok(Self {
                inner: Arc::clone(&policy.get().inner) as Arc<dyn SpeculativeExecutionPolicy>,
            });
        }

        Err(DriverSpeculativeExecutionPolicyError::invalid_policy(obj))
    }
}

#[pymodule]
pub(crate) fn speculative_execution(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PySimpleSpeculativeExecutionPolicy>()?;
    Ok(())
}
