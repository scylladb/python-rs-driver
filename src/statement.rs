use crate::deserialize::row_factory::PyRowFactory;
use crate::enums::{PyConsistency, PySerialConsistency};
use crate::errors::DriverStatementConfigError;
use crate::execution_profile::PyExecutionProfile;
use crate::policies::retry::policies::PyRetryPolicy;
use crate::types::UnsetType;
use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;
use pyo3::sync::{MutexExt, PyOnceLock};
use pyo3::types::{PyBytes, PyFloat, PyString, PyTuple};
use scylla::statement::SerialConsistency;
use scylla::statement::prepared::{ColumnSpecsGuard, PreparedStatement};
use scylla::statement::unprepared::Statement;
use std::sync::Mutex;
use std::time::Duration;

use crate::cluster::metadata::query_metadata::{column_spec_tuple, partition_key_index_tuple};
use crate::policies::load_balancing::PyLoadBalancingPolicy;
use crate::utils::WithOriginalPyObject;

/// The Python-side settings a statement or a batch carries.
#[derive(Clone, Default)]
pub(crate) struct PyStatementSettings {
    pub(crate) execution_profile: Option<Py<PyExecutionProfile>>,
    pub(crate) load_balancing_policy: Option<Py<PyAny>>,
    pub(crate) retry_policy: Option<Py<PyAny>>,
    /// Row factory for requests running this statement, unless the call names
    /// one itself. Falls back to the execution profile's when unset.
    pub(crate) row_factory: Option<WithOriginalPyObject<PyRowFactory>>,
}

impl PyStatementSettings {
    /// The row factory the request target asks for: its own, else the one on
    /// its execution profile. `None` when it asks for neither.
    pub(crate) fn row_factory(&self) -> Option<PyRowFactory> {
        self.row_factory
            .as_ref()
            .or_else(|| self.execution_profile.as_ref()?.get().row_factory.as_ref())
            .map(|f| f.extracted.clone())
    }

    /// The row factory object the user passed, for the Python getter.
    pub(crate) fn py_row_factory(&self) -> Option<Py<PyAny>> {
        self.row_factory.as_ref().map(|f| f.original.clone())
    }

    pub(crate) fn with_execution_profile(&self, profile: Option<Py<PyExecutionProfile>>) -> Self {
        Self {
            execution_profile: profile,
            ..self.clone()
        }
    }

    pub(crate) fn with_load_balancing_policy(&self, policy: Option<Py<PyAny>>) -> Self {
        Self {
            load_balancing_policy: policy,
            ..self.clone()
        }
    }

    pub(crate) fn with_retry_policy(&self, policy: Option<Py<PyAny>>) -> Self {
        Self {
            retry_policy: policy,
            ..self.clone()
        }
    }

    pub(crate) fn with_row_factory(
        &self,
        factory: Option<WithOriginalPyObject<PyRowFactory>>,
    ) -> Self {
        Self {
            row_factory: factory,
            ..self.clone()
        }
    }
}

#[pyclass(name = "PreparedStatement", frozen)]
pub(crate) struct PyPreparedStatement {
    pub(crate) inner: PreparedStatement,
    // Because `get_serial_consistency` in the Rust driver returns `Option<SerialConsistency>`,
    // it cannot represent the `Unset` state. Therefore, the Python-rs driver must distinguish
    // between `Unset` and `None` in a different way. To preserve this distinction, an additional
    // flag `is_serial_consistency_set` is required.
    is_serial_consistency_set: bool,
    /// Inherited from the statement this was prepared from.
    pub(crate) settings: PyStatementSettings,

    /// Cached Python-side query id.
    query_id: PyOnceLock<Py<PyBytes>>,
    /// Cached Python-side bind variable column specifications.
    bind_columns: PyOnceLock<Py<PyTuple>>,
    /// Cached Python-side partition key indexes of the bind variables.
    partition_key_indexes: PyOnceLock<Py<PyTuple>>,

    /// Cached Python-side result column specifications, with the `ColumnSpecsGuard` they were
    /// built from. The guard is kept alive to avoid ABA on the pointer comparison below.
    result_columns: Mutex<Option<(ColumnSpecsGuard, Py<PyTuple>)>>,
}

impl PyPreparedStatement {
    pub(crate) fn new(
        inner: PreparedStatement,
        is_serial_consistency_set: bool,
        settings: PyStatementSettings,
    ) -> Self {
        Self {
            inner,
            is_serial_consistency_set,
            settings,

            query_id: PyOnceLock::new(),
            bind_columns: PyOnceLock::new(),
            partition_key_indexes: PyOnceLock::new(),
            result_columns: Mutex::new(None),
        }
    }
}

#[pymethods]
impl PyPreparedStatement {
    fn with_row_factory(&self, factory: WithOriginalPyObject<PyRowFactory>) -> Self {
        Self::new(
            self.inner.clone(),
            self.is_serial_consistency_set,
            self.settings.with_row_factory(Some(factory)),
        )
    }

    fn without_row_factory(&self) -> Self {
        Self::new(
            self.inner.clone(),
            self.is_serial_consistency_set,
            self.settings.with_row_factory(None),
        )
    }

    #[getter]
    fn get_row_factory(&self) -> Option<Py<PyAny>> {
        self.settings.py_row_factory()
    }

    fn with_execution_profile(&self, profile: Py<PyExecutionProfile>) -> Self {
        let mut p = self.inner.clone();
        p.set_execution_profile_handle(Some(profile.get().inner.clone().into_handle()));
        Self::new(
            p,
            self.is_serial_consistency_set,
            self.settings.with_execution_profile(Some(profile)),
        )
    }

    fn without_execution_profile(&self) -> Self {
        let mut p = self.inner.clone();
        p.set_execution_profile_handle(None);
        Self::new(
            p,
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
    ) -> Result<Self, DriverStatementConfigError> {
        let mut p = self.inner.clone();
        p.set_load_balancing_policy(Some(py_policy.extracted.into_inner()));
        Ok(Self::new(
            p,
            self.is_serial_consistency_set,
            self.settings
                .with_load_balancing_policy(Some(py_policy.original)),
        ))
    }

    fn without_load_balancing_policy(&self) -> Self {
        let mut p = self.inner.clone();
        p.set_load_balancing_policy(None);
        Self::new(
            p,
            self.is_serial_consistency_set,
            self.settings.with_load_balancing_policy(None),
        )
    }

    #[getter]
    fn get_load_balancing_policy(&self) -> Option<Py<PyAny>> {
        self.settings.load_balancing_policy.clone()
    }

    fn with_consistency(&self, c: PyConsistency) -> Self {
        let mut p = self.inner.clone();
        p.set_consistency(c.into());
        Self::new(p, self.is_serial_consistency_set, self.settings.clone())
    }

    fn without_consistency(&self) -> Self {
        let mut p = self.inner.clone();
        p.unset_consistency();
        Self::new(p, self.is_serial_consistency_set, self.settings.clone())
    }

    #[getter]
    fn get_consistency(&self) -> Option<PyConsistency> {
        self.inner.get_consistency().map(PyConsistency::from)
    }

    fn with_serial_consistency(&self, sc: Option<PySerialConsistency>) -> Self {
        let mut p = self.inner.clone();
        p.set_serial_consistency(sc.map(SerialConsistency::from));

        Self::new(p, true, self.settings.clone())
    }

    fn without_serial_consistency(&self) -> Self {
        let mut p = self.inner.clone();
        p.unset_serial_consistency();

        Self::new(p, false, self.settings.clone())
    }

    #[getter]
    fn get_serial_consistency(&self, py: Python) -> Result<Py<PyAny>, DriverStatementConfigError> {
        if !self.is_serial_consistency_set {
            return UnsetType::get_instance(py)
                .into_py_any(py)
                .map_err(DriverStatementConfigError::python_conversion_failed);
        }
        match self.inner.get_serial_consistency() {
            Some(sc) => PySerialConsistency::from(sc)
                .into_py_any(py)
                .map_err(DriverStatementConfigError::python_conversion_failed),
            None => Ok(py.None()),
        }
    }

    fn with_request_timeout(
        &self,
        timeout: Option<f64>,
    ) -> Result<Self, DriverStatementConfigError> {
        let timeout = match timeout {
            None => Duration::MAX,
            Some(secs) => Duration::try_from_secs_f64(secs)
                .map_err(|_| DriverStatementConfigError::invalid_request_timeout(secs))?,
        };

        let mut p = self.inner.clone();

        p.set_request_timeout(Some(timeout));

        Ok(Self::new(
            p,
            self.is_serial_consistency_set,
            self.settings.clone(),
        ))
    }

    fn without_request_timeout(&self) -> Self {
        let mut p = self.inner.clone();
        p.set_request_timeout(None);
        Self::new(p, self.is_serial_consistency_set, self.settings.clone())
    }

    #[getter]
    fn get_request_timeout(&self, py: Python<'_>) -> Py<PyAny> {
        match self.inner.get_request_timeout() {
            Some(t) if t == Duration::MAX => py.None(),
            Some(t) => PyFloat::new(py, t.as_secs_f64()).into(),
            None => UnsetType::get_instance(py).into(),
        }
    }

    fn with_page_size(&self, page_size: i32) -> Self {
        let mut p = self.inner.clone();
        p.set_page_size(page_size);
        Self::new(p, self.is_serial_consistency_set, self.settings.clone())
    }

    #[getter]
    fn get_page_size(&self) -> i32 {
        self.inner.get_page_size()
    }

    fn with_retry_policy(
        &self,
        py_policy: WithOriginalPyObject<PyRetryPolicy>,
    ) -> Result<Self, DriverStatementConfigError> {
        let mut p = self.inner.clone();
        p.set_retry_policy(Some(py_policy.extracted.into_inner()));

        Ok(Self::new(
            p,
            self.is_serial_consistency_set,
            self.settings.with_retry_policy(Some(py_policy.original)),
        ))
    }

    fn without_retry_policy(&self) -> Self {
        let mut p = self.inner.clone();
        p.set_retry_policy(None);

        Self::new(
            p,
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
        let mut p = self.inner.clone();
        p.set_is_idempotent(is_idempotent);

        Self::new(p, self.is_serial_consistency_set, self.settings.clone())
    }

    #[getter]
    fn get_is_idempotent(&self) -> bool {
        self.inner.get_is_idempotent()
    }

    /// The identifier the server assigned to this prepared statement.
    #[getter]
    fn get_query_id(&self, py: Python<'_>) -> Py<PyBytes> {
        let query_id = self
            .query_id
            .get_or_init(py, || PyBytes::new(py, self.inner.get_id()).unbind());
        query_id.clone_ref(py)
    }

    /// Specifications of the bind variables of this statement.
    #[getter]
    fn get_bind_columns(&self, py: Python<'_>) -> PyResult<Py<PyTuple>> {
        let columns = self.bind_columns.get_or_try_init(py, || {
            column_spec_tuple(py, self.inner.get_variable_col_specs().as_slice())
        })?;
        Ok(columns.clone_ref(py))
    }

    /// Bind variable indexes of the partition key columns, in partition key order.
    ///
    /// Element `i` is the index into `bind_columns` of the `i`-th component of the partition
    /// key.
    #[getter]
    fn get_partition_key_indexes(&self, py: Python<'_>) -> PyResult<Py<PyTuple>> {
        let indexes = self.partition_key_indexes.get_or_try_init(py, || {
            partition_key_index_tuple(py, self.inner.get_variable_pk_indexes())
        })?;
        Ok(indexes.clone_ref(py))
    }

    /// Specifications of the columns this statement returns.
    ///
    /// The server can replace a prepared statement's result metadata (e.g. after a schema
    /// change). We detect that by comparing `ColumnSpecs` slice addresses; the guard is cached
    /// to avoid ABA.
    #[getter]
    fn get_result_columns(&self, py: Python<'_>) -> PyResult<Py<PyTuple>> {
        let guard = self.inner.get_current_result_set_col_specs();
        let specs = guard.get();
        let current_key = specs.as_slice().as_ptr();

        let mut cache = self.result_columns.lock_py_attached(py).unwrap();
        if let Some((cached_key, cached_tuple)) = cache.as_ref()
            && std::ptr::eq(cached_key.get().as_slice().as_ptr(), current_key)
        {
            return Ok(cached_tuple.clone_ref(py));
        }

        let tuple = column_spec_tuple(py, specs.as_slice())?;
        *cache = Some((guard, tuple.clone_ref(py)));
        Ok(tuple)
    }
}

#[derive(Clone)]
#[pyclass(name = "Statement", frozen, skip_from_py_object)]
pub(crate) struct PyStatement {
    pub(crate) inner: Statement,
    // Because `get_serial_consistency` in the Rust driver returns `Option<SerialConsistency>`,
    // it cannot represent the `Unset` state. Therefore, the Python-rs driver must distinguish
    // between `Unset` and `None` in a different way. To preserve this distinction, an additional
    // flag `is_serial_consistency_set` is required.
    is_serial_consistency_set: bool,
    pub(crate) settings: PyStatementSettings,
}

impl PyStatement {
    pub(crate) fn new(
        inner: Statement,
        is_serial_consistency_set: bool,
        settings: PyStatementSettings,
    ) -> Self {
        Self {
            inner,
            is_serial_consistency_set,
            settings,
        }
    }
}

#[pymethods]
impl PyStatement {
    #[new]
    fn py_new(query_str: String) -> Self {
        let s = Statement::from(query_str);
        Self::new(s, false, PyStatementSettings::default())
    }

    #[getter]
    fn contents<'py>(&self, py: Python<'py>) -> Bound<'py, PyString> {
        PyString::new(py, &self.inner.contents)
    }

    fn with_row_factory(&self, factory: WithOriginalPyObject<PyRowFactory>) -> Self {
        Self::new(
            self.inner.clone(),
            self.is_serial_consistency_set,
            self.settings.with_row_factory(Some(factory)),
        )
    }

    fn without_row_factory(&self) -> Self {
        Self::new(
            self.inner.clone(),
            self.is_serial_consistency_set,
            self.settings.with_row_factory(None),
        )
    }

    #[getter]
    fn get_row_factory(&self) -> Option<Py<PyAny>> {
        self.settings.py_row_factory()
    }

    fn with_execution_profile(&self, profile: Py<PyExecutionProfile>) -> Self {
        let mut s = self.inner.clone();
        s.set_execution_profile_handle(Some(profile.get().inner.clone().into_handle()));
        Self::new(
            s,
            self.is_serial_consistency_set,
            self.settings.with_execution_profile(Some(profile)),
        )
    }

    fn without_execution_profile(&self) -> Self {
        let mut s = self.inner.clone();
        s.set_execution_profile_handle(None);
        Self::new(
            s,
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
    ) -> Result<Self, DriverStatementConfigError> {
        let mut s = self.inner.clone();
        s.set_load_balancing_policy(Some(py_policy.extracted.into_inner()));
        Ok(Self::new(
            s,
            self.is_serial_consistency_set,
            self.settings
                .with_load_balancing_policy(Some(py_policy.original)),
        ))
    }

    fn without_load_balancing_policy(&self) -> Self {
        let mut s = self.inner.clone();
        s.set_load_balancing_policy(None);
        Self::new(
            s,
            self.is_serial_consistency_set,
            self.settings.with_load_balancing_policy(None),
        )
    }

    #[getter]
    fn get_load_balancing_policy(&self) -> Option<Py<PyAny>> {
        self.settings.load_balancing_policy.clone()
    }

    fn with_consistency(&self, c: PyConsistency) -> Self {
        let mut s = self.inner.clone();
        s.set_consistency(c.into());
        Self::new(s, self.is_serial_consistency_set, self.settings.clone())
    }

    fn without_consistency(&self) -> Self {
        let mut s = self.inner.clone();
        s.unset_consistency();
        Self::new(s, self.is_serial_consistency_set, self.settings.clone())
    }

    #[getter]
    fn get_consistency(&self) -> Option<PyConsistency> {
        self.inner.get_consistency().map(PyConsistency::from)
    }

    fn with_serial_consistency(&self, sc: Option<PySerialConsistency>) -> Self {
        let mut s = self.inner.clone();
        s.set_serial_consistency(sc.map(SerialConsistency::from));
        Self::new(s, true, self.settings.clone())
    }

    fn without_serial_consistency(&self) -> Self {
        let mut s = self.inner.clone();
        s.unset_serial_consistency();
        Self::new(s, false, self.settings.clone())
    }

    #[getter]
    fn get_serial_consistency(&self, py: Python) -> Result<Py<PyAny>, DriverStatementConfigError> {
        if !self.is_serial_consistency_set {
            return UnsetType::get_instance(py)
                .into_py_any(py)
                .map_err(DriverStatementConfigError::python_conversion_failed);
        }
        match self.inner.get_serial_consistency() {
            Some(sc) => PySerialConsistency::from(sc)
                .into_py_any(py)
                .map_err(DriverStatementConfigError::python_conversion_failed),
            None => Ok(py.None()),
        }
    }

    fn with_request_timeout(
        &self,
        timeout: Option<f64>,
    ) -> Result<Self, DriverStatementConfigError> {
        let timeout = match timeout {
            None => Duration::MAX,
            Some(secs) => Duration::try_from_secs_f64(secs)
                .map_err(|_| DriverStatementConfigError::invalid_request_timeout(secs))?,
        };

        let mut s = self.inner.clone();
        s.set_request_timeout(Some(timeout));
        Ok(Self::new(
            s,
            self.is_serial_consistency_set,
            self.settings.clone(),
        ))
    }

    fn without_request_timeout(&self) -> Self {
        let mut s = self.inner.clone();
        s.set_request_timeout(None);
        Self::new(s, self.is_serial_consistency_set, self.settings.clone())
    }

    #[getter]
    fn get_request_timeout(&self, py: Python<'_>) -> Py<PyAny> {
        match self.inner.get_request_timeout() {
            Some(t) if t == Duration::MAX => py.None(),
            Some(t) => PyFloat::new(py, t.as_secs_f64()).into(),
            None => UnsetType::get_instance(py).into(),
        }
    }

    fn with_page_size(&self, page_size: i32) -> Self {
        let mut s = self.inner.clone();
        s.set_page_size(page_size);
        Self::new(s, self.is_serial_consistency_set, self.settings.clone())
    }

    #[getter]
    fn get_page_size(&self) -> i32 {
        self.inner.get_page_size()
    }

    fn with_retry_policy(
        &self,
        py_policy: WithOriginalPyObject<PyRetryPolicy>,
    ) -> Result<Self, DriverStatementConfigError> {
        let mut s = self.inner.clone();
        s.set_retry_policy(Some(py_policy.extracted.into_inner()));

        Ok(Self::new(
            s,
            self.is_serial_consistency_set,
            self.settings.with_retry_policy(Some(py_policy.original)),
        ))
    }

    fn without_retry_policy(&self) -> Self {
        let mut s = self.inner.clone();
        s.set_retry_policy(None);

        Self::new(
            s,
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
        let mut s = self.inner.clone();
        s.set_is_idempotent(is_idempotent);

        Self::new(s, self.is_serial_consistency_set, self.settings.clone())
    }

    #[getter]
    fn get_is_idempotent(&self) -> bool {
        self.inner.get_is_idempotent()
    }
}

#[pymodule]
pub(crate) fn statement(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyPreparedStatement>()?;
    module.add_class::<PyStatement>()?;
    Ok(())
}
