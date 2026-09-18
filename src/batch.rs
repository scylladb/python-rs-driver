use crate::core::session::ExecutableStatement;
use crate::deserialize::row_factory::PyRowFactory;
use crate::enums::{PyConsistency, PySerialConsistency};
use crate::errors::DriverBatchError;
use crate::execution_profile::PyExecutionProfile;
use crate::policies::load_balancing::{PyLoadBalancingPolicy, PyTargetPolicy};
use crate::policies::retry::policies::PyRetryPolicy;
use crate::serialize::value_list::PyValueList;
use crate::statement::PyStatementSettings;
use crate::types::UnsetType;
use crate::utils::WithOriginalPyObject;
use pyo3::types::PyFloat;
use pyo3::{IntoPyObjectExt, prelude::*};
use scylla::statement::SerialConsistency;
use scylla::statement::batch::{Batch, BatchType};
use std::time::Duration;

#[pyclass(name = "BatchType", from_py_object, eq, eq_int, frozen)]
#[derive(Clone, Copy, PartialEq)]
pub(crate) enum PyBatchType {
    Logged,
    Unlogged,
    Counter,
}

impl From<PyBatchType> for BatchType {
    fn from(value: PyBatchType) -> Self {
        match value {
            PyBatchType::Logged => Self::Logged,
            PyBatchType::Unlogged => Self::Unlogged,
            PyBatchType::Counter => Self::Counter,
        }
    }
}

impl From<BatchType> for PyBatchType {
    fn from(value: BatchType) -> Self {
        match value {
            BatchType::Logged => Self::Logged,
            BatchType::Unlogged => Self::Unlogged,
            BatchType::Counter => Self::Counter,
        }
    }
}

#[pyclass(name = "Batch", from_py_object)]
#[derive(Clone)]
pub(crate) struct PyBatch {
    pub(crate) inner: Batch,
    pub(crate) values: Vec<PyValueList>,
    // Because `get_serial_consistency` in the Rust driver returns `Option<SerialConsistency>`,
    // it cannot represent the `Unset` state. Therefore, the Python-rs driver must distinguish
    // between `Unset` and `None` in a different way. To preserve this distinction, an additional
    // flag `is_serial_consistency_set` is required.
    is_serial_consistency_set: bool,
    pub(crate) settings: PyStatementSettings,
}

impl PyBatch {
    pub(crate) fn new(
        inner: Batch,
        values: Vec<PyValueList>,
        is_serial_consistency_set: bool,
        settings: PyStatementSettings,
    ) -> Self {
        Self {
            inner,
            values,
            is_serial_consistency_set,
            settings,
        }
    }

    /// Pins this batch to a single target for one execution.
    pub(crate) fn set_target(&mut self, target: PyTargetPolicy) {
        self.inner
            .set_load_balancing_policy(Some(target.into_inner()));
    }
}

#[pymethods]
impl PyBatch {
    #[new]
    #[pyo3(signature = (batch_type=PyBatchType::Logged))]
    fn py_new(batch_type: PyBatchType) -> Self {
        Self::new(
            Batch::new(batch_type.into()),
            vec![],
            false,
            PyStatementSettings::default(),
        )
    }

    #[pyo3(signature = (statement, values=None))]
    fn add(&mut self, statement: ExecutableStatement, values: Option<PyValueList>) {
        self.inner.append_statement(statement);
        self.values.push(values.unwrap_or(PyValueList::Empty));
    }

    fn add_all(&mut self, items: Vec<(ExecutableStatement, Option<PyValueList>)>) {
        self.values.reserve_exact(items.len());
        for (statement, values) in items {
            self.add(statement, values);
        }
    }

    #[getter]
    fn get_type(&self) -> PyBatchType {
        self.inner.get_type().into()
    }

    fn with_row_factory(&self, factory: WithOriginalPyObject<PyRowFactory>) -> Self {
        let mut b = self.clone();
        b.settings = self.settings.with_row_factory(Some(factory));
        b
    }

    fn without_row_factory(&self) -> Self {
        let mut b = self.clone();
        b.settings = self.settings.with_row_factory(None);
        b
    }

    #[getter]
    fn get_row_factory(&self) -> Option<Py<PyAny>> {
        self.settings.py_row_factory()
    }

    fn with_execution_profile(&self, profile: Py<PyExecutionProfile>) -> Self {
        let mut batch = self.inner.clone();
        let inner = profile.get().inner.clone();
        batch.set_execution_profile_handle(Some(inner.into_handle()));

        Self::new(
            batch,
            self.values.clone(),
            self.is_serial_consistency_set,
            self.settings.with_execution_profile(Some(profile)),
        )
    }

    fn without_execution_profile(&self) -> Self {
        let mut batch = self.inner.clone();
        batch.set_execution_profile_handle(None);

        Self::new(
            batch,
            self.values.clone(),
            self.is_serial_consistency_set,
            self.settings.with_execution_profile(None),
        )
    }

    #[getter]
    fn get_execution_profile(&self) -> Option<Py<PyExecutionProfile>> {
        self.settings.execution_profile.clone()
    }

    fn with_load_balancing_policy(
        &self,
        py_policy: WithOriginalPyObject<PyLoadBalancingPolicy>,
    ) -> Result<Self, DriverBatchError> {
        let mut batch = self.inner.clone();
        batch.set_load_balancing_policy(Some(py_policy.extracted.into_inner()));
        Ok(Self::new(
            batch,
            self.values.clone(),
            self.is_serial_consistency_set,
            self.settings
                .with_load_balancing_policy(Some(py_policy.original)),
        ))
    }

    fn without_load_balancing_policy(&self) -> Self {
        let mut batch = self.inner.clone();
        batch.set_load_balancing_policy(None);
        Self::new(
            batch,
            self.values.clone(),
            self.is_serial_consistency_set,
            self.settings.with_load_balancing_policy(None),
        )
    }

    #[getter]
    fn get_load_balancing_policy(&self) -> Option<Py<PyAny>> {
        self.settings.load_balancing_policy.clone()
    }

    fn with_consistency(&self, c: PyConsistency) -> Self {
        let mut batch = self.inner.clone();
        batch.set_consistency(c.into());

        Self::new(
            batch,
            self.values.clone(),
            self.is_serial_consistency_set,
            self.settings.clone(),
        )
    }

    fn without_consistency(&self) -> Self {
        let mut batch = self.inner.clone();
        batch.unset_consistency();

        Self::new(
            batch,
            self.values.clone(),
            self.is_serial_consistency_set,
            self.settings.clone(),
        )
    }

    #[getter]
    fn get_consistency(&self) -> Option<PyConsistency> {
        self.inner.get_consistency().map(PyConsistency::from)
    }

    fn with_serial_consistency(&self, sc: Option<PySerialConsistency>) -> Self {
        let mut batch = self.inner.clone();
        batch.set_serial_consistency(sc.map(SerialConsistency::from));
        Self::new(batch, self.values.clone(), true, self.settings.clone())
    }

    fn without_serial_consistency(&self) -> Self {
        let mut batch = self.inner.clone();
        batch.unset_serial_consistency();
        Self::new(batch, self.values.clone(), false, self.settings.clone())
    }

    #[getter]
    fn get_serial_consistency(&self, py: Python) -> Result<Py<PyAny>, DriverBatchError> {
        if !self.is_serial_consistency_set {
            return UnsetType::get_instance(py)
                .into_py_any(py)
                .map_err(DriverBatchError::python_conversion_failed);
        }
        match self.inner.get_serial_consistency() {
            Some(sc) => PySerialConsistency::from(sc)
                .into_py_any(py)
                .map_err(DriverBatchError::python_conversion_failed),
            None => Ok(py.None()),
        }
    }

    fn with_request_timeout(&self, timeout: Option<f64>) -> Result<Self, DriverBatchError> {
        let timeout = match timeout {
            None => Duration::MAX,
            Some(secs) => Duration::try_from_secs_f64(secs)
                .map_err(|_| DriverBatchError::invalid_request_timeout(secs))?,
        };

        let mut batch = self.inner.clone();
        batch.set_request_timeout(Some(timeout));

        Ok(Self::new(
            batch,
            self.values.clone(),
            self.is_serial_consistency_set,
            self.settings.clone(),
        ))
    }

    fn without_request_timeout(&self) -> Self {
        let mut batch = self.inner.clone();
        batch.set_request_timeout(None);
        Self::new(
            batch,
            self.values.clone(),
            self.is_serial_consistency_set,
            self.settings.clone(),
        )
    }

    #[getter]
    fn get_request_timeout(&self, py: Python<'_>) -> Py<PyAny> {
        match self.inner.get_request_timeout() {
            Some(t) if t == Duration::MAX => py.None(),
            Some(t) => PyFloat::new(py, t.as_secs_f64()).into(),
            None => UnsetType::get_instance(py).into(),
        }
    }

    fn with_retry_policy(
        &self,
        py_policy: WithOriginalPyObject<PyRetryPolicy>,
    ) -> Result<Self, DriverBatchError> {
        let mut batch = self.inner.clone();
        batch.set_retry_policy(Some(py_policy.extracted.into_inner()));

        Ok(Self::new(
            batch,
            self.values.clone(),
            self.is_serial_consistency_set,
            self.settings.with_retry_policy(Some(py_policy.original)),
        ))
    }

    fn without_retry_policy(&self) -> Self {
        let mut batch = self.inner.clone();
        batch.set_retry_policy(None);

        Self::new(
            batch,
            self.values.clone(),
            self.is_serial_consistency_set,
            self.settings.with_retry_policy(None),
        )
    }

    #[getter]
    fn get_retry_policy(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.settings
            .retry_policy
            .as_ref()
            .map(|rp| rp.clone_ref(py))
    }

    fn set_is_idempotent(&self, is_idempotent: bool) -> Self {
        let mut batch = self.inner.clone();
        batch.set_is_idempotent(is_idempotent);

        Self::new(
            batch,
            self.values.clone(),
            self.is_serial_consistency_set,
            self.settings.clone(),
        )
    }

    #[getter]
    fn get_is_idempotent(&self) -> bool {
        self.inner.get_is_idempotent()
    }
}

#[pymodule]
pub(crate) fn batch(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyBatch>()?;
    module.add_class::<PyBatchType>()?;
    Ok(())
}
