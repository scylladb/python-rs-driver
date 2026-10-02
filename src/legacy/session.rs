//! `LegacySession`: the blocking, callback-driven session of the legacy driver.

use std::num::NonZeroI32;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use pyo3::exceptions::{PyNotImplementedError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::sync::MutexExt;
use pyo3::types::PyBytes;
use scylla::client::execution_profile::ExecutionProfileHandle;
use scylla::client::session::Session;
use scylla::statement::Statement;
use scylla_cql::frame::request::query::PagingState;

use crate::RUNTIME;
use crate::batch::PyBatch;
use crate::cluster::state::PyClusterState;
use crate::core::session::{ExecutableStatement, PageFetch, PreparableStatement, SessionCore};
use crate::deserialize::results::{PyPagingState, RowFactory};
use crate::errors::ScyllaError;
use crate::errors::execution::DriverStatementConversionError;
use crate::execution_profile::PyExecutionProfile;
use crate::future::{PyResponseFuture, catch_panics_typed};
use crate::legacy::PyResultSet;
use crate::policies::load_balancing::PyTargetPolicy;
use crate::serialize::value_list::PyValueList;
use crate::statement::PyPreparedStatement;
use crate::types::UnsetType;
use crate::utils::{PyDuration, WithOriginalPyObject};

/// Session-wide defaults a request falls back to.
struct Defaults {
    row_factory: Option<Py<RowFactory>>,
    fetch_size: FetchSize,
    /// For a request given no `timeout` whose statement has none of its own.
    timeout: RequestTimeout,
}

/// The legacy `default_fetch_size`: how a statement without a page size of its own pages.
#[derive(Clone, Copy)]
enum FetchSize {
    /// Never set: the driver's default page size.
    Driver,
    Rows(NonZeroI32),
    /// `None`: such statements run unpaged.
    Unpaged,
}

/// The legacy (`cassandra-driver` compatible) session: blocking `execute()`
/// and `prepare()`, `execute_async()` returning a `ResponseFuture`.
///
/// A facade over [`SessionCore`] like `Session`, but its requests run on the
/// runtime from the start, so pages are fetched in place rather than through
/// a spawned task.
#[pyclass(name = "LegacySession", frozen)]
pub(crate) struct PyLegacySession {
    core: SessionCore,
    defaults: Mutex<Defaults>,
    is_shutdown: AtomicBool,
}

impl TryFrom<Arc<Session>> for PyLegacySession {
    type Error = PyErr;

    fn try_from(inner: Arc<Session>) -> Result<Self, Self::Error> {
        Ok(Self {
            core: SessionCore::try_from(inner)?.with_page_fetch(PageFetch::Inline),
            defaults: Mutex::new(Defaults {
                row_factory: None,
                fetch_size: FetchSize::Driver,
                timeout: RequestTimeout::Default,
            }),
            is_shutdown: AtomicBool::new(false),
        })
    }
}

impl PyLegacySession {
    /// Refuses a request once the session was shut down, as the legacy driver
    /// did by closing its pools.
    fn reject_if_shutdown(&self) -> PyResult<()> {
        if self.is_shutdown.load(Ordering::Relaxed) {
            return Err(ScyllaError::new_err("the session has been shut down"));
        }
        Ok(())
    }

    /// Request timeout of the default execution profile, `None` for no timeout.
    fn profile_timeout(&self) -> Option<Duration> {
        self.core
            .inner
            .get_default_execution_profile_handle()
            .to_profile()
            .get_request_timeout()
    }
}

#[pymethods]
impl PyLegacySession {
    /// Sends `query` and returns the `ResponseFuture` delivering its result.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (query, parameters=None, trace=false, custom_payload=None, timeout=RequestTimeout::Default, execution_profile=None, paging_state=None, host=None, execute_as=None))]
    fn execute_async(
        &self,
        py: Python<'_>,
        query: WithOriginalPyObject<LegacyQuery>,
        parameters: Option<PyValueList>,
        trace: bool,
        custom_payload: Option<Bound<'_, PyAny>>,
        timeout: RequestTimeout,
        execution_profile: Option<Py<PyExecutionProfile>>,
        paging_state: Option<LegacyPagingState>,
        host: Option<PyTargetPolicy>,
        execute_as: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyResponseFuture>> {
        unsupported("custom_payload", custom_payload)?;
        unsupported("execute_as", execute_as)?;
        self.reject_if_shutdown()?;

        let (row_factory, fetch_size, default_timeout) = {
            let defaults = self.defaults.lock_py_attached(py).unwrap();
            let row_factory = defaults.row_factory.as_ref().map(|f| f.clone_ref(py));
            (row_factory, defaults.fetch_size, defaults.timeout)
        };
        // One for the request's pages, one for the future to build their rows.
        let request_factory = row_factory.as_ref().map(|f| f.clone_ref(py));

        // The future keeps `query` as extracted; the overrides go on a copy.
        let mut executable = query.extracted.clone();
        if let Some(d) = timeout.duration() {
            executable.set_request_timeout(d);
        } else if let Some(d) = default_timeout.duration()
            && executable.request_timeout().is_none()
        {
            executable.set_request_timeout(d);
        }
        if trace {
            executable.set_tracing(true);
        }
        if let Some(profile) = execution_profile {
            executable.set_execution_profile_handle(profile.get().inner.clone().into_handle());
        }
        if let Some(target) = host {
            executable.set_target(target);
        }
        let paged = match fetch_size {
            FetchSize::Driver => true,
            FetchSize::Rows(size) => {
                executable.apply_default_page_size(size);
                true
            }
            // Only a statement asking for a page size itself still pages.
            FetchSize::Unpaged => executable.is_page_size_set(),
        };
        // Reported by the future: what the driver resolves once the overrides are applied.
        let timeout_secs = executable
            .effective_timeout(&self.core.inner)
            .map(|d| d.as_secs_f64());

        let request = match executable {
            LegacyQuery::Batch(batch) => {
                if parameters.is_some() {
                    return Err(PyTypeError::new_err(
                        "parameters cannot be passed with a Batch; bind values per statement",
                    ));
                }
                if paging_state.is_some() {
                    return Err(PyTypeError::new_err("a Batch cannot be paged"));
                }

                self.core.clone().batch(batch, request_factory)
            }

            LegacyQuery::Statement(statement) => self.core.clone().execute(
                statement,
                parameters.unwrap_or_default(),
                request_factory,
                paging_state.map(|state| state.0),
                paged,
            )?,
        };

        PyResponseFuture::spawn(py, request, query, timeout_secs, row_factory)
    }

    /// Sends `query` and blocks for its `ResultSet`.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (query, parameters=None, timeout=RequestTimeout::Default, trace=false, custom_payload=None, execution_profile=None, paging_state=None, host=None, execute_as=None))]
    fn execute(
        &self,
        py: Python<'_>,
        query: WithOriginalPyObject<LegacyQuery>,
        parameters: Option<PyValueList>,
        timeout: RequestTimeout,
        trace: bool,
        custom_payload: Option<Bound<'_, PyAny>>,
        execution_profile: Option<Py<PyExecutionProfile>>,
        paging_state: Option<LegacyPagingState>,
        host: Option<PyTargetPolicy>,
        execute_as: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyResultSet>> {
        let future = self.execute_async(
            py,
            query,
            parameters,
            trace,
            custom_payload,
            timeout,
            execution_profile,
            paging_state,
            host,
            execute_as,
        )?;
        PyResponseFuture::result(future, py)
    }

    /// Prepares `query`, blocking until the server answers.
    #[pyo3(signature = (query, custom_payload=None, keyspace=None))]
    fn prepare(
        &self,
        py: Python<'_>,
        query: PreparableStatement,
        custom_payload: Option<Bound<'_, PyAny>>,
        keyspace: Option<Bound<'_, PyAny>>,
    ) -> PyResult<PyPreparedStatement> {
        unsupported("custom_payload", custom_payload)?;
        unsupported("keyspace", keyspace)?;
        self.reject_if_shutdown()?;

        let prepared = self.core.clone().prepare(query);
        Ok(RUNTIME.block_on(py, catch_panics_typed(prepared))??)
    }

    /// Sets the keyspace of every connection, blocking until done.
    fn set_keyspace(&self, py: Python<'_>, keyspace: String) -> PyResult<()> {
        self.reject_if_shutdown()?;

        let request = self.core.clone().use_keyspace(keyspace, true);
        RUNTIME.block_on(py, catch_panics_typed(request))??;
        Ok(())
    }

    /// Row factory of requests that do not set one; `None` for the driver default.
    #[getter]
    fn get_row_factory(&self, py: Python<'_>) -> Option<Py<RowFactory>> {
        let defaults = self.defaults.lock_py_attached(py).unwrap();
        defaults.row_factory.as_ref().map(|f| f.clone_ref(py))
    }

    #[setter]
    fn set_row_factory(&self, py: Python<'_>, row_factory: Option<Py<RowFactory>>) {
        self.defaults.lock_py_attached(py).unwrap().row_factory = row_factory;
    }

    /// Page size of requests whose statement sets none: the driver default until
    /// set, `None` once paging was disabled for them.
    #[getter]
    fn get_default_fetch_size(&self, py: Python<'_>) -> Option<i32> {
        /// The Rust driver does not expose its default page size; a fresh statement carries it.
        static DRIVER_PAGE_SIZE: LazyLock<i32> =
            LazyLock::new(|| Statement::new("").get_page_size());
        match self.defaults.lock_py_attached(py).unwrap().fetch_size {
            FetchSize::Driver => Some(*DRIVER_PAGE_SIZE),
            FetchSize::Rows(size) => Some(size.get()),
            FetchSize::Unpaged => None,
        }
    }

    /// A positive page size, or `None` to run such requests unpaged, as the legacy driver did.
    #[setter]
    fn set_default_fetch_size(&self, py: Python<'_>, fetch_size: Option<i32>) -> PyResult<()> {
        let fetch_size = match fetch_size {
            None => FetchSize::Unpaged,
            Some(size) => NonZeroI32::new(size)
                .filter(|size| size.is_positive())
                .map(FetchSize::Rows)
                .ok_or_else(|| {
                    PyValueError::new_err("default_fetch_size must be positive or None")
                })?,
        };
        self.defaults.lock_py_attached(py).unwrap().fetch_size = fetch_size;
        Ok(())
    }

    /// Timeout in seconds of a request given none whose statement has none of
    /// its own: the one set here, else the default profile's. `None` for no timeout.
    #[getter]
    fn get_default_timeout(&self, py: Python<'_>) -> Option<f64> {
        let timeout = match self.defaults.lock_py_attached(py).unwrap().timeout {
            RequestTimeout::Default => self.profile_timeout(),
            RequestTimeout::Never => None,
            RequestTimeout::After(d) => Some(d),
        };
        timeout.map(|d| d.as_secs_f64())
    }

    /// Seconds, `None` for no timeout, or `Unset` to fall back to the default profile's.
    #[setter]
    fn set_default_timeout(&self, py: Python<'_>, timeout: RequestTimeout) {
        self.defaults.lock_py_attached(py).unwrap().timeout = timeout;
    }

    /// The keyspace in use, if any.
    #[getter]
    fn keyspace(&self) -> Option<String> {
        self.core.inner.get_keyspace().map(|k| k.to_string())
    }

    #[getter]
    fn cluster_state(&self, py: Python<'_>) -> PyResult<Py<PyClusterState>> {
        self.core.cluster_state(py)
    }

    #[getter]
    fn is_shutdown(&self) -> bool {
        self.is_shutdown.load(Ordering::Relaxed)
    }

    /// Marks the session shut down: later requests are refused, but the
    /// connections only close once the last reference is dropped.
    ///
    /// TODO: implement a proper shutdown, here and on `Session`, closing the
    /// connections instead of waiting for the last reference to go away.
    fn shutdown(&self) {
        self.is_shutdown.store(true, Ordering::Relaxed);
    }
}

/// What `execute_async` accepts: a batch, or anything executable as one statement.
#[derive(Clone)]
pub(crate) enum LegacyQuery {
    Batch(PyBatch),
    Statement(ExecutableStatement),
}

/// Per-execution overrides, applied to whichever kind of request this is.
impl LegacyQuery {
    fn set_request_timeout(&mut self, timeout: Duration) {
        match self {
            Self::Batch(batch) => batch.inner.set_request_timeout(Some(timeout)),
            Self::Statement(statement) => statement.set_request_timeout(timeout),
        }
    }

    fn set_tracing(&mut self, tracing: bool) {
        match self {
            Self::Batch(batch) => batch.inner.set_tracing(tracing),
            Self::Statement(statement) => statement.set_tracing(tracing),
        }
    }

    fn set_execution_profile_handle(&mut self, handle: ExecutionProfileHandle) {
        match self {
            Self::Batch(batch) => batch.inner.set_execution_profile_handle(Some(handle)),
            Self::Statement(statement) => statement.set_execution_profile_handle(handle),
        }
    }

    fn set_target(&mut self, target: PyTargetPolicy) {
        match self {
            Self::Batch(batch) => batch.set_target(target),
            Self::Statement(statement) => statement.set_target(target),
        }
    }

    /// Whether the statement asks for a page size itself. A batch never pages.
    fn is_page_size_set(&self) -> bool {
        match self {
            Self::Batch(_) => false,
            Self::Statement(statement) => statement.is_page_size_set(),
        }
    }

    /// The session's default page size, applied only to a statement without
    /// one of its own, as in the legacy driver.
    fn apply_default_page_size(&mut self, size: NonZeroI32) {
        if let Self::Statement(statement) = self
            && !statement.is_page_size_set()
        {
            statement.set_page_size(size.get());
        }
    }

    /// The request's own timeout; `None` defers to its execution profile.
    fn request_timeout(&self) -> Option<Duration> {
        match self {
            Self::Batch(batch) => batch.inner.get_request_timeout(),
            Self::Statement(statement) => statement.request_timeout(),
        }
    }

    /// The request's own execution profile; `None` defers to the session's default.
    fn execution_profile_handle(&self) -> Option<&ExecutionProfileHandle> {
        match self {
            Self::Batch(batch) => batch.inner.get_execution_profile_handle(),
            Self::Statement(statement) => statement.execution_profile_handle(),
        }
    }

    /// The client-side timeout the driver applies, in its order of precedence:
    /// the request's own, else its execution profile's, else the session
    /// default profile's. `None` for no timeout.
    fn effective_timeout(&self, session: &Session) -> Option<Duration> {
        self.request_timeout()
            .or_else(|| {
                self.execution_profile_handle()
                    .unwrap_or_else(|| session.get_default_execution_profile_handle())
                    .to_profile()
                    .get_request_timeout()
            })
            .filter(|timeout| *timeout != Duration::MAX)
    }

    /// Whether this is a batch: a `Batch`, or a statement whose text begins one.
    pub(crate) fn is_batch(&self) -> bool {
        match self {
            Self::Batch(_) => true,
            Self::Statement(statement) => begins_batch(statement.contents()),
        }
    }
}

impl<'py> FromPyObject<'_, 'py> for LegacyQuery {
    type Error = DriverStatementConversionError;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(batch) = obj.cast::<PyBatch>() {
            return Ok(Self::Batch(batch.borrow().clone()));
        }

        Ok(Self::Statement(ExecutableStatement::extract(obj)?))
    }
}

/// `^\s*BEGIN\s+[a-zA-Z]*\s*BATCH`, as the legacy driver matched it.
fn begins_batch(text: &str) -> bool {
    let Some(rest) = text.trim_start().strip_prefix("BEGIN") else {
        return false;
    };
    let trimmed = rest.trim_start();
    if trimmed.len() == rest.len() {
        return false;
    }
    trimmed.starts_with("BATCH")
        || trimmed
            .trim_start_matches(|c: char| c.is_ascii_alphabetic())
            .trim_start()
            .starts_with("BATCH")
}

/// The `timeout` argument of `execute()`; the legacy `_NOT_SET` sentinel is `Unset`.
#[derive(Clone, Copy)]
enum RequestTimeout {
    /// Not given: the statement's, else the profile's, timeout applies.
    Default,
    /// `None`: no client-side timeout.
    Never,
    After(Duration),
}

impl RequestTimeout {
    /// The timeout to set on the statement, if any.
    fn duration(&self) -> Option<Duration> {
        match self {
            Self::Default => None,
            Self::Never => Some(Duration::MAX),
            Self::After(d) => Some(*d),
        }
    }
}

impl<'py> FromPyObject<'_, 'py> for RequestTimeout {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
        if obj.is_none() {
            return Ok(Self::Never);
        }
        if obj.is_instance_of::<UnsetType>() {
            return Ok(Self::Default);
        }
        let duration: PyDuration = obj.extract()?;
        Ok(Self::After(duration.0))
    }
}

/// The `paging_state` argument: a `PagingState`, or its raw bytes as the legacy driver kept them.
struct LegacyPagingState(PagingState);

impl<'py> FromPyObject<'_, 'py> for LegacyPagingState {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
        if let Ok(state) = obj.cast::<PyPagingState>() {
            return Ok(Self(state.get().inner.clone()));
        }
        if let Ok(bytes) = obj.cast::<PyBytes>() {
            return Ok(Self(PagingState::new_from_raw_bytes(bytes.as_bytes())));
        }
        Err(PyTypeError::new_err(
            "paging_state must be a PagingState or bytes",
        ))
    }
}

/// Rejects a legacy argument this driver has no equivalent for, if given.
fn unsupported(name: &str, value: Option<Bound<'_, PyAny>>) -> PyResult<()> {
    match value {
        None => Ok(()),
        Some(_) => Err(PyNotImplementedError::new_err(format!(
            "{name} is not supported by this driver"
        ))),
    }
}
