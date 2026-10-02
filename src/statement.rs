use crate::enums::{PyConsistency, PySerialConsistency};
use crate::errors::config::DriverStatementConfigError;
use crate::execution_profile::PyExecutionProfile;
use crate::policies::retry::policies::PyRetryPolicy;
use crate::types::MaybeUnset;
use pyo3::prelude::*;
use pyo3::sync::{MutexExt, PyOnceLock};
use pyo3::types::{PyBytes, PyString, PyTuple};
use scylla::client::execution_profile::ExecutionProfileHandle;
use scylla::policies::load_balancing::LoadBalancingPolicy;
use scylla::policies::retry::RetryPolicy;
use scylla::statement::batch::Batch;
use scylla::statement::prepared::{ColumnSpecsGuard, PreparedStatement};
use scylla::statement::unprepared::Statement;
use scylla::statement::{Consistency, SerialConsistency};
use std::sync::{Arc, Mutex};
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
}

/// The configuration API that `Statement`, `PreparedStatement` and `Batch` share in the Rust
/// driver, which has no common trait for it.
pub(crate) trait ConfigurableStatement {
    fn set_consistency(&mut self, c: Consistency);
    fn unset_consistency(&mut self);
    fn get_consistency(&self) -> Option<Consistency>;
    fn set_serial_consistency(&mut self, sc: Option<SerialConsistency>);
    fn unset_serial_consistency(&mut self);
    fn get_serial_consistency(&self) -> Option<SerialConsistency>;
    fn set_request_timeout(&mut self, timeout: Option<Duration>);
    fn get_request_timeout(&self) -> Option<Duration>;
    fn set_is_idempotent(&mut self, is_idempotent: bool);
    fn get_is_idempotent(&self) -> bool;
    fn set_retry_policy(&mut self, policy: Option<Arc<dyn RetryPolicy>>);
    fn set_load_balancing_policy(&mut self, policy: Option<Arc<dyn LoadBalancingPolicy>>);
    fn set_execution_profile_handle(&mut self, handle: Option<ExecutionProfileHandle>);
}

macro_rules! impl_configurable_statement {
    ($($ty:ty),*) => {$(
        impl ConfigurableStatement for $ty {
            fn set_consistency(&mut self, c: Consistency) {
                <$ty>::set_consistency(self, c)
            }
            fn unset_consistency(&mut self) {
                <$ty>::unset_consistency(self)
            }
            fn get_consistency(&self) -> Option<Consistency> {
                <$ty>::get_consistency(self)
            }
            fn set_serial_consistency(&mut self, sc: Option<SerialConsistency>) {
                <$ty>::set_serial_consistency(self, sc)
            }
            fn unset_serial_consistency(&mut self) {
                <$ty>::unset_serial_consistency(self)
            }
            fn get_serial_consistency(&self) -> Option<SerialConsistency> {
                <$ty>::get_serial_consistency(self)
            }
            fn set_request_timeout(&mut self, timeout: Option<Duration>) {
                <$ty>::set_request_timeout(self, timeout)
            }
            fn get_request_timeout(&self) -> Option<Duration> {
                <$ty>::get_request_timeout(self)
            }
            fn set_is_idempotent(&mut self, is_idempotent: bool) {
                <$ty>::set_is_idempotent(self, is_idempotent)
            }
            fn get_is_idempotent(&self) -> bool {
                <$ty>::get_is_idempotent(self)
            }
            fn set_retry_policy(&mut self, policy: Option<Arc<dyn RetryPolicy>>) {
                <$ty>::set_retry_policy(self, policy)
            }
            fn set_load_balancing_policy(&mut self, policy: Option<Arc<dyn LoadBalancingPolicy>>) {
                <$ty>::set_load_balancing_policy(self, policy)
            }
            fn set_execution_profile_handle(&mut self, handle: Option<ExecutionProfileHandle>) {
                <$ty>::set_execution_profile_handle(self, handle)
            }
        }
    )*};
}

impl_configurable_statement!(Statement, PreparedStatement, Batch);

/// A Rust driver statement together with the Python-side view of its configuration.
///
/// Setters return the Python object they replace, so that the caller drops it only after
/// releasing the statement's lock: dropping it may run arbitrary Python code.
#[derive(Clone)]
pub(crate) struct StatementOptions<S> {
    pub(crate) inner: S,
    // Because `get_serial_consistency` in the Rust driver returns `Option<SerialConsistency>`,
    // it cannot represent the `Unset` state. Therefore, the Python-rs driver must distinguish
    // between `Unset` and `None` in a different way. To preserve this distinction, an additional
    // flag `is_serial_consistency_set` is required.
    pub(crate) is_serial_consistency_set: bool,
    pub(crate) settings: PyStatementSettings,
}

impl<S: ConfigurableStatement> StatementOptions<S> {
    pub(crate) fn new(
        inner: S,
        is_serial_consistency_set: bool,
        settings: PyStatementSettings,
    ) -> Self {
        Self {
            inner,
            is_serial_consistency_set,
            settings,
        }
    }

    pub(crate) fn execution_profile(&self, py: Python<'_>) -> Option<Py<PyExecutionProfile>> {
        self.settings
            .execution_profile
            .as_ref()
            .map(|p| p.clone_ref(py))
    }

    pub(crate) fn set_execution_profile(
        &mut self,
        profile: Option<Py<PyExecutionProfile>>,
    ) -> Option<Py<PyExecutionProfile>> {
        self.inner.set_execution_profile_handle(
            profile
                .as_ref()
                .map(|p| p.get().inner.clone().into_handle()),
        );
        std::mem::replace(&mut self.settings.execution_profile, profile)
    }

    pub(crate) fn load_balancing_policy(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.settings
            .load_balancing_policy
            .as_ref()
            .map(|p| p.clone_ref(py))
    }

    pub(crate) fn set_load_balancing_policy(
        &mut self,
        policy: Option<WithOriginalPyObject<PyLoadBalancingPolicy>>,
    ) -> Option<Py<PyAny>> {
        let (policy, original) = policy
            .map(|p| (p.extracted.into_inner(), p.original))
            .unzip();
        self.inner.set_load_balancing_policy(policy);
        std::mem::replace(&mut self.settings.load_balancing_policy, original)
    }

    pub(crate) fn retry_policy(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.settings.retry_policy.as_ref().map(|p| p.clone_ref(py))
    }

    pub(crate) fn set_retry_policy(
        &mut self,
        policy: Option<WithOriginalPyObject<PyRetryPolicy>>,
    ) -> Option<Py<PyAny>> {
        let (policy, original) = policy
            .map(|p| (p.extracted.into_inner(), p.original))
            .unzip();
        self.inner.set_retry_policy(policy);
        std::mem::replace(&mut self.settings.retry_policy, original)
    }

    pub(crate) fn consistency(&self) -> MaybeUnset<PyConsistency> {
        // The Rust driver's `None` means unset; a statement always sends some consistency.
        match self.inner.get_consistency() {
            Some(c) => MaybeUnset::Set(c.into()),
            None => MaybeUnset::Unset,
        }
    }

    pub(crate) fn set_consistency(&mut self, c: MaybeUnset<PyConsistency>) {
        match c {
            MaybeUnset::Set(c) => self.inner.set_consistency(c.into()),
            MaybeUnset::Unset => self.inner.unset_consistency(),
        }
    }

    pub(crate) fn serial_consistency(&self) -> MaybeUnset<Option<PySerialConsistency>> {
        if !self.is_serial_consistency_set {
            return MaybeUnset::Unset;
        }
        MaybeUnset::Set(
            self.inner
                .get_serial_consistency()
                .map(PySerialConsistency::from),
        )
    }

    pub(crate) fn set_serial_consistency(&mut self, sc: MaybeUnset<Option<PySerialConsistency>>) {
        match sc {
            MaybeUnset::Set(sc) => {
                self.inner
                    .set_serial_consistency(sc.map(SerialConsistency::from));
                self.is_serial_consistency_set = true;
            }
            MaybeUnset::Unset => {
                self.inner.unset_serial_consistency();
                self.is_serial_consistency_set = false;
            }
        }
    }

    pub(crate) fn request_timeout(&self) -> MaybeUnset<Option<f64>> {
        match self.inner.get_request_timeout() {
            None => MaybeUnset::Unset,
            Some(t) if t == Duration::MAX => MaybeUnset::Set(None),
            Some(t) => MaybeUnset::Set(Some(t.as_secs_f64())),
        }
    }

    /// Calls `invalid` with the offending value if `secs` is not a valid duration.
    pub(crate) fn set_request_timeout<E>(
        &mut self,
        secs: MaybeUnset<Option<f64>>,
        invalid: impl FnOnce(f64) -> E,
    ) -> Result<(), E> {
        let timeout = match secs {
            MaybeUnset::Unset => None,
            // The Rust driver's `None` means unset, so "no timeout" is stored as `Duration::MAX`.
            MaybeUnset::Set(None) => Some(Duration::MAX),
            MaybeUnset::Set(Some(secs)) => {
                Some(Duration::try_from_secs_f64(secs).map_err(|_| invalid(secs))?)
            }
        };
        self.inner.set_request_timeout(timeout);
        Ok(())
    }

    pub(crate) fn is_idempotent(&self) -> bool {
        self.inner.get_is_idempotent()
    }

    pub(crate) fn set_is_idempotent(&mut self, is_idempotent: bool) {
        self.inner.set_is_idempotent(is_idempotent);
    }
}

/// Emits the `#[pymethods]` block of a statement class: its own `$items`, followed by the
/// configuration properties all statement classes share. `$err` is the class's error type.
/// The class's own items are passed in because PyO3 allows only one `#[pymethods]` block
/// per class.
///
/// The class must provide `with_options`, giving locked access to its `StatementOptions`.
macro_rules! statement_pymethods {
    ($class:ty, $err:ty, { $($items:tt)* }) => {
        #[::pyo3::pymethods]
        impl $class {
            $($items)*

            #[getter]
            fn get_execution_profile(
                &self,
                py: ::pyo3::Python<'_>,
            ) -> Option<::pyo3::Py<$crate::execution_profile::PyExecutionProfile>> {
                self.with_options(py, |o| o.execution_profile(py))
            }

            #[setter]
            fn set_execution_profile(
                &self,
                py: ::pyo3::Python<'_>,
                profile: Option<::pyo3::Py<$crate::execution_profile::PyExecutionProfile>>,
            ) {
                self.with_options(py, |o| o.set_execution_profile(profile));
            }

            #[getter]
            fn get_load_balancing_policy(
                &self,
                py: ::pyo3::Python<'_>,
            ) -> Option<::pyo3::Py<::pyo3::PyAny>> {
                self.with_options(py, |o| o.load_balancing_policy(py))
            }

            #[setter]
            fn set_load_balancing_policy(
                &self,
                py: ::pyo3::Python<'_>,
                policy: Option<
                    $crate::utils::WithOriginalPyObject<
                        $crate::policies::load_balancing::PyLoadBalancingPolicy,
                    >,
                >,
            ) {
                self.with_options(py, |o| o.set_load_balancing_policy(policy));
            }

            #[getter]
            fn get_retry_policy(&self, py: ::pyo3::Python<'_>) -> Option<::pyo3::Py<::pyo3::PyAny>> {
                self.with_options(py, |o| o.retry_policy(py))
            }

            #[setter]
            fn set_retry_policy(
                &self,
                py: ::pyo3::Python<'_>,
                policy: Option<
                    $crate::utils::WithOriginalPyObject<
                        $crate::policies::retry::policies::PyRetryPolicy,
                    >,
                >,
            ) {
                self.with_options(py, |o| o.set_retry_policy(policy));
            }

            #[getter]
            fn get_consistency(
                &self,
                py: ::pyo3::Python<'_>,
            ) -> $crate::types::MaybeUnset<$crate::enums::PyConsistency> {
                self.with_options(py, |o| o.consistency())
            }

            #[setter]
            fn set_consistency(
                &self,
                py: ::pyo3::Python<'_>,
                c: $crate::types::MaybeUnset<$crate::enums::PyConsistency>,
            ) {
                self.with_options(py, |o| o.set_consistency(c));
            }

            #[getter]
            fn get_serial_consistency(
                &self,
                py: ::pyo3::Python<'_>,
            ) -> $crate::types::MaybeUnset<Option<$crate::enums::PySerialConsistency>> {
                self.with_options(py, |o| o.serial_consistency())
            }

            #[setter]
            fn set_serial_consistency(
                &self,
                py: ::pyo3::Python<'_>,
                sc: $crate::types::MaybeUnset<Option<$crate::enums::PySerialConsistency>>,
            ) {
                self.with_options(py, |o| o.set_serial_consistency(sc));
            }

            #[getter]
            fn get_request_timeout(
                &self,
                py: ::pyo3::Python<'_>,
            ) -> $crate::types::MaybeUnset<Option<f64>> {
                self.with_options(py, |o| o.request_timeout())
            }

            #[setter]
            fn set_request_timeout(
                &self,
                py: ::pyo3::Python<'_>,
                timeout: $crate::types::MaybeUnset<Option<f64>>,
            ) -> Result<(), $err> {
                self.with_options(py, |o| {
                    o.set_request_timeout(timeout, <$err>::invalid_request_timeout)
                })
            }

            #[getter]
            fn get_is_idempotent(&self, py: ::pyo3::Python<'_>) -> bool {
                self.with_options(py, |o| o.is_idempotent())
            }

            #[setter]
            fn set_is_idempotent(&self, py: ::pyo3::Python<'_>, is_idempotent: bool) {
                self.with_options(py, |o| o.set_is_idempotent(is_idempotent));
            }
        }
    };
}

pub(crate) use statement_pymethods;

fn check_page_size(page_size: i32) -> Result<i32, DriverStatementConfigError> {
    if page_size <= 0 {
        return Err(DriverStatementConfigError::non_positive_page_size(
            page_size,
        ));
    }
    Ok(page_size)
}

#[pyclass(module = "scylla.statement", name = "PreparedStatement", frozen)]
pub(crate) struct PyPreparedStatement {
    options: Mutex<StatementOptions<PreparedStatement>>,

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
    pub(crate) fn new(options: StatementOptions<PreparedStatement>) -> Self {
        Self {
            options: Mutex::new(options),

            query_id: PyOnceLock::new(),
            bind_columns: PyOnceLock::new(),
            partition_key_indexes: PyOnceLock::new(),
            result_columns: Mutex::new(None),
        }
    }

    pub(crate) fn with_options<R>(
        &self,
        py: Python<'_>,
        f: impl FnOnce(&mut StatementOptions<PreparedStatement>) -> R,
    ) -> R {
        f(&mut self.options.lock_py_attached(py).unwrap())
    }

    /// A snapshot of the Rust statement with its current configuration.
    pub(crate) fn snapshot(&self, py: Python<'_>) -> PreparedStatement {
        self.with_options(py, |o| o.inner.clone())
    }
}

statement_pymethods!(PyPreparedStatement, DriverStatementConfigError, {
    #[getter]
    fn get_page_size(&self, py: Python<'_>) -> i32 {
        self.with_options(py, |o| o.inner.get_page_size())
    }

    #[setter]
    fn set_page_size(
        &self,
        py: Python<'_>,
        page_size: i32,
    ) -> Result<(), DriverStatementConfigError> {
        let page_size = check_page_size(page_size)?;
        self.with_options(py, |o| o.inner.set_page_size(page_size));
        Ok(())
    }

    /// The identifier the server assigned to this prepared statement.
    #[getter]
    fn get_query_id(&self, py: Python<'_>) -> Py<PyBytes> {
        let query_id = self
            .query_id
            .get_or_init(py, || PyBytes::new(py, self.snapshot(py).get_id()).unbind());
        query_id.clone_ref(py)
    }

    /// Specifications of the bind variables of this statement.
    #[getter]
    fn get_bind_columns(&self, py: Python<'_>) -> PyResult<Py<PyTuple>> {
        let columns = self.bind_columns.get_or_try_init(py, || {
            column_spec_tuple(py, self.snapshot(py).get_variable_col_specs().as_slice())
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
            partition_key_index_tuple(py, self.snapshot(py).get_variable_pk_indexes())
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
        let guard = self.with_options(py, |o| o.inner.get_current_result_set_col_specs());
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
});

#[pyclass(module = "scylla.statement", name = "Statement", frozen)]
pub(crate) struct PyStatement {
    options: Mutex<StatementOptions<Statement>>,
}

impl PyStatement {
    pub(crate) fn with_options<R>(
        &self,
        py: Python<'_>,
        f: impl FnOnce(&mut StatementOptions<Statement>) -> R,
    ) -> R {
        f(&mut self.options.lock_py_attached(py).unwrap())
    }

    /// A snapshot of the Rust statement with its current configuration.
    pub(crate) fn snapshot(&self, py: Python<'_>) -> Statement {
        self.with_options(py, |o| o.inner.clone())
    }
}

statement_pymethods!(PyStatement, DriverStatementConfigError, {
    #[new]
    fn py_new(query_str: String) -> Self {
        let options = StatementOptions::new(
            Statement::from(query_str),
            false,
            PyStatementSettings::default(),
        );
        Self {
            options: Mutex::new(options),
        }
    }

    #[getter]
    fn contents<'py>(&self, py: Python<'py>) -> Bound<'py, PyString> {
        let contents = self.with_options(py, |o| o.inner.contents.clone());
        PyString::new(py, &contents)
    }

    #[getter]
    fn get_page_size(&self, py: Python<'_>) -> i32 {
        self.with_options(py, |o| o.inner.get_page_size())
    }

    #[setter]
    fn set_page_size(
        &self,
        py: Python<'_>,
        page_size: i32,
    ) -> Result<(), DriverStatementConfigError> {
        let page_size = check_page_size(page_size)?;
        self.with_options(py, |o| o.inner.set_page_size(page_size));
        Ok(())
    }
});

#[pymodule]
pub(crate) fn statement(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyPreparedStatement>()?;
    module.add_class::<PyStatement>()?;
    Ok(())
}
