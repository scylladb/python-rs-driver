use crate::enums::{PyConsistency, PySerialConsistency};
use crate::errors::DriverStatementConfigError;
use crate::policies::load_balancing::PyLoadBalancingPolicy;
use crate::policies::retry::policies::PyRetryPolicy;
use crate::policies::speculative_execution::PySpeculativeExecutionPolicy;
use crate::utils::WithOriginalPyObject;
use pyo3::prelude::*;
use scylla::client::execution_profile::ExecutionProfile;
use std::time::Duration;

#[pyclass(name = "ExecutionProfile", frozen, from_py_object)]
#[derive(Clone)]
pub(crate) struct PyExecutionProfile {
    pub(crate) inner: ExecutionProfile,
    pub(crate) retry_policy: Option<Py<PyAny>>,
    pub(crate) load_balancing_policy: Option<Py<PyAny>>,
    pub(crate) speculative_execution_policy: Option<Py<PyAny>>,
}

#[pymethods]
impl PyExecutionProfile {
    #[new]
    #[pyo3(signature = (
        timeout=30.0,
        consistency=PyConsistency::LocalQuorum,
        serial_consistency=PySerialConsistency::LocalSerial,
        load_balancing_policy=None,
        retry_policy=None,
        speculative_execution_policy=None,
    ))]
    pub(crate) fn new(
        _py: Python<'_>,
        timeout: Option<f64>,
        consistency: PyConsistency,
        serial_consistency: Option<PySerialConsistency>,
        load_balancing_policy: Option<WithOriginalPyObject<PyLoadBalancingPolicy>>,
        retry_policy: Option<WithOriginalPyObject<PyRetryPolicy>>,
        speculative_execution_policy: Option<WithOriginalPyObject<PySpeculativeExecutionPolicy>>,
    ) -> Result<Self, DriverStatementConfigError> {
        let mut profile_builder = ExecutionProfile::builder();

        if let Some(secs) = timeout {
            let duration = Duration::try_from_secs_f64(secs)
                .map_err(|_| DriverStatementConfigError::invalid_request_timeout(secs))?;

            profile_builder = profile_builder.request_timeout(Some(duration));
        }

        profile_builder = profile_builder.consistency(consistency.into());

        profile_builder =
            profile_builder.serial_consistency(serial_consistency.map(|sc| sc.into()));

        let original_lbp = if let Some(policy) = load_balancing_policy {
            profile_builder = profile_builder.load_balancing_policy(policy.extracted.into_inner());
            Some(policy.original)
        } else {
            None
        };

        let original_retry_policy = if let Some(rp) = retry_policy {
            profile_builder = profile_builder.retry_policy(rp.extracted.into_inner());
            Some(rp.original)
        } else {
            None
        };

        let original_speculative_execution_policy = if let Some(sep) = speculative_execution_policy
        {
            profile_builder =
                profile_builder.speculative_execution_policy(Some(sep.extracted.into_inner()));
            Some(sep.original)
        } else {
            None
        };

        Ok(PyExecutionProfile {
            inner: profile_builder.build(),
            retry_policy: original_retry_policy,
            load_balancing_policy: original_lbp,
            speculative_execution_policy: original_speculative_execution_policy,
        })
    }

    #[getter]
    pub(crate) fn get_request_timeout(&self) -> Option<f64> {
        self.inner.get_request_timeout().map(|d| d.as_secs_f64())
    }

    #[getter]
    pub(crate) fn get_consistency(&self) -> PyConsistency {
        PyConsistency::from(self.inner.get_consistency())
    }

    #[getter]
    pub(crate) fn get_serial_consistency(&self) -> Option<PySerialConsistency> {
        self.inner
            .get_serial_consistency()
            .map(PySerialConsistency::from)
    }

    #[getter]
    fn get_load_balancing_policy(&self) -> Option<Py<PyAny>> {
        self.load_balancing_policy.clone()
    }

    #[getter]
    pub(crate) fn get_retry_policy(&self) -> Option<Py<PyAny>> {
        self.retry_policy.clone()
    }

    #[getter]
    pub(crate) fn get_speculative_execution_policy(&self) -> Option<Py<PyAny>> {
        self.speculative_execution_policy.clone()
    }
}

#[pymodule]
pub(crate) fn execution_profile(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyExecutionProfile>()?;
    Ok(())
}
