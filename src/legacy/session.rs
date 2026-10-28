//! `LegacySession`: the blocking, callback-driven session of the legacy driver.

use std::num::NonZeroI32;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use pyo3::exceptions::{PyNotImplementedError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::sync::MutexExt;
use pyo3::types::{PyBytes, PyDict, PyTuple};
use scylla::client::execution_profile::{ExecutionProfile, ExecutionProfileHandle};
use scylla::client::session::Session;
use scylla::errors::SchemaAgreementError;
use scylla::statement::{Consistency, SerialConsistency, Statement};
use scylla_cql::frame::request::query::PagingState;

use crate::RUNTIME;
use crate::batch::{BatchState, PyBatch};
use crate::cluster::state::PyClusterState;
use crate::core::session::{ExecutableStatement, PreparableStatement, SessionCore};
use crate::deserialize::row_factory::PyRowFactory;
use crate::enums::{PyConsistency, PySerialConsistency};
use crate::errors::ScyllaError;
use crate::errors::execution::{DriverSchemaAgreementError, DriverStatementConversionError};
use crate::execution_profile::PyExecutionProfile;
use crate::future::{LegacyRequestFuture, PyResponseFuture, catch_unwind};
use crate::legacy::PyResultSet;
use crate::policies::load_balancing::PyTargetPolicy;
use crate::results::PyPagingState;
use crate::serialize::value_list::PyValueList;
use crate::statement::PyPreparedStatement;
use crate::types::MaybeUnset;
use crate::utils::{PyDuration, WithOriginalPyObject};

/// The session settings that can change after connect.
struct SessionSettings {
    fetch_size: FetchSize,
    mode: ConfigMode,
}

impl SessionSettings {
    /// Applies the legacy defaults or `profile`, and the fetch size, to `query`.
    /// Returns the row factory and whether the request pages.
    fn configure_request(
        &self,
        py: Python<'_>,
        query: &mut LegacyQuery,
        profile: Option<&Py<PyExecutionProfile>>,
    ) -> PyResult<(Option<WithOriginalPyObject<PyRowFactory>>, bool)> {
        let factory = match (&self.mode, profile) {
            (ConfigMode::Legacy(_), Some(_)) => {
                return Err(PyValueError::new_err(
                    "Cannot specify execution_profile while using legacy parameters.",
                ));
            }
            (ConfigMode::Legacy(legacy), None) => {
                legacy.apply_defaults(query);
                legacy.row_factory.as_ref()
            }
            (_, Some(profile)) => {
                let profile = profile.get();
                query.set_execution_profile_handle(profile.inner.clone().into_handle());
                profile.row_factory.as_ref()
            }
            (_, None) => None,
        };
        let paged = match self.fetch_size {
            FetchSize::Driver => true,
            FetchSize::Rows(size) => {
                query.apply_default_page_size(size);
                true
            }
            // Only a statement asking for a page size itself still pages.
            FetchSize::Unpaged => query.is_page_size_set(),
        };
        let factory = factory.map(|f| WithOriginalPyObject {
            original: f.original.clone_ref(py),
            extracted: f.extracted.clone(),
        });
        Ok((factory, paged))
    }
}

/// Which configuration style the session uses. As in the legacy driver, session
/// defaults and execution profiles do not mix: the first one used locks out the other.
enum ConfigMode {
    /// Neither style used yet.
    Uncommitted,
    /// A session default was set.
    Legacy(LegacyDefaults),
    /// The session was built with an explicit execution profile.
    Profiles,
}

/// The legacy session defaults, which execution profiles replace.
#[derive(Default)]
struct LegacyDefaults {
    row_factory: Option<WithOriginalPyObject<PyRowFactory>>,
    timeout: RequestTimeout,
    /// `None` defers to the execution profile.
    consistency: Option<Consistency>,
    /// `None` defers to the execution profile; `Some(None)` sends no serial consistency.
    serial_consistency: Option<Option<SerialConsistency>>,
}

impl LegacyDefaults {
    /// Sets each default on `query` that the query does not set itself.
    fn apply_defaults(&self, query: &mut LegacyQuery) {
        if let Some(d) = self.timeout.duration()
            && query.request_timeout().is_none()
        {
            query.set_request_timeout(d);
        }
        if let Some(consistency) = self.consistency
            && query.consistency().is_none()
        {
            query.set_consistency(consistency);
        }
        if let Some(serial) = self.serial_consistency
            && !query.is_serial_consistency_set()
        {
            query.set_serial_consistency(serial);
        }
    }
}

impl ConfigMode {
    fn legacy_defaults(&self) -> Option<&LegacyDefaults> {
        match self {
            Self::Legacy(defaults) => Some(defaults),
            Self::Uncommitted | Self::Profiles => None,
        }
    }

    /// The legacy defaults, to set `name` on them; switches to legacy mode, and
    /// is refused once the session uses execution profiles.
    fn legacy_defaults_mut(&mut self, name: &str) -> PyResult<&mut LegacyDefaults> {
        match self {
            Self::Legacy(defaults) => Ok(defaults),
            // The first session default set switches the session to legacy mode.
            Self::Uncommitted => {
                *self = Self::Legacy(LegacyDefaults::default());
                self.legacy_defaults_mut(name)
            }
            Self::Profiles => Err(PyValueError::new_err(format!(
                "Cannot set Session.{name} while using Configuration Profiles. Set this in a profile instead."
            ))),
        }
    }
}

/// The legacy (`cassandra-driver` compatible) session: blocking `execute()`
/// and `prepare()`, `execute_async()` returning a `ResponseFuture`.
///
/// A facade over [`SessionCore`] like `Session`.
#[pyclass(module = "scylla.legacy", name = "LegacySession", frozen)]
pub(crate) struct PyLegacySession {
    core: SessionCore,
    settings: Mutex<SessionSettings>,
    is_shutdown: AtomicBool,
}

impl PyLegacySession {
    /// `uses_profiles`: the session was built with an explicit execution profile.
    pub(crate) fn new(
        inner: Arc<Session>,
        default_row_factory: Option<PyRowFactory>,
        uses_profiles: bool,
    ) -> Result<Self, PyErr> {
        Ok(Self {
            core: SessionCore::new(inner, default_row_factory)?,
            settings: Mutex::new(SessionSettings {
                fetch_size: FetchSize::Driver,
                mode: if uses_profiles {
                    ConfigMode::Profiles
                } else {
                    ConfigMode::Uncommitted
                },
            }),
            is_shutdown: AtomicBool::new(false),
        })
    }

    /// Refuses a request once the session was shut down, as the legacy driver
    /// did by closing its pools.
    fn reject_if_shutdown(&self) -> PyResult<()> {
        if self.is_shutdown.load(Ordering::Relaxed) {
            return Err(ScyllaError::new_err("the session has been shut down"));
        }
        Ok(())
    }

    /// Reads a legacy default; `None` outside legacy mode.
    fn legacy_default<T>(
        &self,
        py: Python<'_>,
        read: impl FnOnce(&LegacyDefaults) -> T,
    ) -> Option<T> {
        let settings = self.settings.lock_py_attached(py).unwrap();
        settings.mode.legacy_defaults().map(read)
    }

    fn default_profile(&self) -> ExecutionProfile {
        self.core
            .inner
            .get_default_execution_profile_handle()
            .to_profile()
    }
}

#[pymethods]
impl PyLegacySession {
    /// Sends `query` and returns the `ResponseFuture` delivering its result.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (query, parameters=None, trace=None, custom_payload=None, timeout=RequestTimeout::Default, execution_profile=None, paging_state=None, host=None, execute_as=None))]
    fn execute_async(
        &self,
        py: Python<'_>,
        query: WithOriginalPyObject<LegacyQuery>,
        parameters: Option<PyValueList>,
        trace: Option<bool>,
        custom_payload: Option<Bound<'_, PyAny>>,
        timeout: RequestTimeout,
        execution_profile: Option<Py<PyExecutionProfile>>,
        paging_state: Option<LegacyPagingState>,
        host: Option<PyTargetPolicy>,
        execute_as: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyResponseFuture>> {
        error_if_present("custom_payload", custom_payload)?;
        error_if_present("execute_as", execute_as)?;
        self.reject_if_shutdown()?;

        // The future keeps `query` as extracted; the overrides go on a copy.
        let mut executable = query.extracted.clone();
        // Before configure_request, so that this argument wins over the session's default_timeout.
        if let Some(d) = timeout.duration() {
            executable.set_request_timeout(d);
        }
        let (row_factory, paged) = self
            .settings
            .lock_py_attached(py)
            .unwrap()
            .configure_request(py, &mut executable, execution_profile.as_ref())?;

        if let Some(trace) = trace {
            executable.set_tracing(trace);
        }

        if let Some(target) = host {
            executable.set_target(target);
        }

        let request_factory = row_factory.as_ref().map(|f| f.extracted.clone());
        let timeout_secs = executable
            .effective_timeout(&self.core.inner)
            .map(|d| d.as_secs_f64());

        let request: LegacyRequestFuture<_> = match executable {
            LegacyQuery::Batch(batch) => {
                if parameters.is_some() {
                    return Err(PyTypeError::new_err(
                        "parameters cannot be passed with a Batch; bind values per statement",
                    ));
                }
                if paging_state.is_some() {
                    return Err(PyTypeError::new_err("a Batch cannot be paged"));
                }

                Box::pin(self.core.clone().batch(batch, request_factory))
            }

            LegacyQuery::Statement(statement) => Box::pin(self.core.clone().execute(
                statement,
                parameters.unwrap_or_default(),
                request_factory,
                paging_state.map(|state| state.0),
                paged,
            )?),
        };

        PyResponseFuture::spawn(
            py,
            request,
            query,
            timeout_secs,
            row_factory.map(|f| f.original),
        )
    }

    /// Sends `query` and blocks for its `ResultSet`.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (query, parameters=None, timeout=RequestTimeout::Default, trace=None, custom_payload=None, execution_profile=None, paging_state=None, host=None, execute_as=None))]
    fn execute(
        &self,
        py: Python<'_>,
        query: WithOriginalPyObject<LegacyQuery>,
        parameters: Option<PyValueList>,
        timeout: RequestTimeout,
        trace: Option<bool>,
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
        error_if_present("custom_payload", custom_payload)?;
        error_if_present("keyspace", keyspace)?;
        self.reject_if_shutdown()?;

        let prepared = self.core.clone().prepare(query);
        Ok(RUNTIME.block_on(py, catch_unwind(prepared))??)
    }

    /// Blocks until every reachable node reports the same schema version. `False`
    /// when they do not agree within the session's schema agreement timeout.
    #[pyo3(signature = (wait_time=None, scope=None))]
    fn wait_for_schema_agreement(
        &self,
        py: Python<'_>,
        wait_time: Option<Bound<'_, PyAny>>,
        scope: Option<Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        error_if_present("wait_time", wait_time)?;
        error_if_present("scope", scope)?;

        let agreement = self.core.clone().await_schema_agreement();
        match RUNTIME.block_on(py, catch_unwind(agreement))? {
            Ok(_) => Ok(true),
            Err(DriverSchemaAgreementError::RustDriverSchemaAgreementError { source })
                if matches!(*source, SchemaAgreementError::Timeout(_)) =>
            {
                Ok(false)
            }
            Err(err) => Err(err.into()),
        }
    }

    /// Sets the keyspace of every connection, blocking until done.
    fn set_keyspace(&self, py: Python<'_>, keyspace: String) -> PyResult<()> {
        self.reject_if_shutdown()?;

        let request = self.core.clone().use_keyspace(keyspace, true);
        RUNTIME.block_on(py, catch_unwind(request))??;
        Ok(())
    }

    /// The keyspace in use, if any.
    #[getter]
    fn keyspace(&self) -> Option<String> {
        self.core.inner.get_keyspace().map(|k| k.to_string())
    }

    /// The cluster state, in place of the legacy `Cluster` object.
    #[getter]
    fn cluster(&self, py: Python<'_>) -> PyResult<Py<PyClusterState>> {
        self.core.cluster_state(py)
    }

    /// Row factory of requests that do not set one; `None` for the driver default.
    #[getter]
    fn get_row_factory(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.legacy_default(py, |s| {
            s.row_factory.as_ref().map(|f| f.original.clone_ref(py))
        })
        .flatten()
    }

    #[setter]
    fn set_row_factory(
        &self,
        py: Python<'_>,
        row_factory: Option<WithOriginalPyObject<PyRowFactory>>,
    ) -> PyResult<()> {
        // Dropped once the lock is released: dropping it may run Python code.
        let _previous = {
            let mut settings = self.settings.lock_py_attached(py).unwrap();
            let legacy = settings.mode.legacy_defaults_mut("row_factory")?;
            std::mem::replace(&mut legacy.row_factory, row_factory)
        };
        Ok(())
    }

    /// Page size of requests whose statement sets none: the driver default until
    /// set, `None` once paging was disabled for them.
    #[getter]
    fn get_default_fetch_size(&self, py: Python<'_>) -> Option<i32> {
        /// The Rust driver does not expose its default page size; a fresh statement carries it.
        static DRIVER_PAGE_SIZE: LazyLock<i32> =
            LazyLock::new(|| Statement::new("").get_page_size());
        match self.settings.lock_py_attached(py).unwrap().fetch_size {
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
        self.settings.lock_py_attached(py).unwrap().fetch_size = fetch_size;
        Ok(())
    }

    /// Timeout in seconds of a request given none whose statement has none of
    /// its own: the one set here, else the default profile's. `None` for no timeout.
    #[getter]
    fn get_default_timeout(&self, py: Python<'_>) -> Option<f64> {
        let timeout = match self.legacy_default(py, |s| s.timeout).unwrap_or_default() {
            RequestTimeout::Default => self.default_profile().get_request_timeout(),
            RequestTimeout::Never => None,
            RequestTimeout::After(d) => Some(d),
        };
        timeout.map(|d| d.as_secs_f64())
    }

    /// Seconds, `None` for no timeout, or `UNSET` to fall back to the default profile's.
    #[setter]
    fn set_default_timeout(&self, py: Python<'_>, timeout: RequestTimeout) -> PyResult<()> {
        let mut settings = self.settings.lock_py_attached(py).unwrap();
        let legacy = settings.mode.legacy_defaults_mut("default_timeout")?;
        legacy.timeout = timeout;
        Ok(())
    }

    /// Consistency of a request whose statement sets none: the one set here, else
    /// the default profile's.
    #[getter]
    fn get_default_consistency_level(&self, py: Python<'_>) -> PyConsistency {
        self.legacy_default(py, |s| s.consistency)
            .flatten()
            .unwrap_or_else(|| self.default_profile().get_consistency())
            .into()
    }

    /// A consistency, or `UNSET` to fall back to the default profile's.
    #[setter]
    fn set_default_consistency_level(
        &self,
        py: Python<'_>,
        consistency: MaybeUnset<PyConsistency>,
    ) -> PyResult<()> {
        let mut settings = self.settings.lock_py_attached(py).unwrap();
        let legacy = settings
            .mode
            .legacy_defaults_mut("default_consistency_level")?;
        legacy.consistency = match consistency {
            MaybeUnset::Set(consistency) => Some(consistency.into()),
            MaybeUnset::Unset => None,
        };
        Ok(())
    }

    /// Serial consistency of a request whose statement sets none: the one set
    /// here, else the default profile's. `None` for no serial consistency.
    #[getter]
    fn get_default_serial_consistency_level(&self, py: Python<'_>) -> Option<PySerialConsistency> {
        self.legacy_default(py, |s| s.serial_consistency)
            .flatten()
            .unwrap_or_else(|| self.default_profile().get_serial_consistency())
            .map(PySerialConsistency::from)
    }

    /// A serial consistency, `None` for none, or `UNSET` to fall back to the default profile's.
    #[setter]
    fn set_default_serial_consistency_level(
        &self,
        py: Python<'_>,
        serial: MaybeUnset<Option<PySerialConsistency>>,
    ) -> PyResult<()> {
        let mut settings = self.settings.lock_py_attached(py).unwrap();
        let legacy = settings
            .mode
            .legacy_defaults_mut("default_serial_consistency_level")?;
        legacy.serial_consistency = match serial {
            MaybeUnset::Set(serial) => Some(serial.map(SerialConsistency::from)),
            MaybeUnset::Unset => None,
        };
        Ok(())
    }

    // TODO: implement with named execution profiles (#33).
    fn get_execution_profile(&self, name: Bound<'_, PyAny>) -> PyResult<()> {
        let _ = name;
        Err(PyNotImplementedError::new_err(
            "Named execution profiles are not implemented.",
        ))
    }

    // TODO: implement with named execution profiles (#33).
    #[pyo3(signature = (ep, **kwargs))]
    fn execution_profile_clone_update(
        &self,
        ep: Bound<'_, PyAny>,
        kwargs: Option<Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        let _ = (ep, kwargs);
        Err(PyNotImplementedError::new_err(
            "Named execution profiles are not implemented.",
        ))
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

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    /// Shuts the session down when a `with` block exits; an exception raised in it propagates.
    #[pyo3(signature = (*_args))]
    fn __exit__(&self, _args: &Bound<'_, PyTuple>) {
        self.shutdown();
    }
}

/// What `execute_async` accepts: a batch, or anything executable as one statement.
#[derive(Clone)]
pub(crate) enum LegacyQuery {
    Batch(BatchState),
    Statement(ExecutableStatement),
}

/// Per-execution overrides, applied to whichever kind of request this is.
impl LegacyQuery {
    fn set_request_timeout(&mut self, timeout: Duration) {
        match self {
            Self::Batch(batch) => batch.options.inner.set_request_timeout(Some(timeout)),
            Self::Statement(statement) => statement.set_request_timeout(timeout),
        }
    }

    fn set_tracing(&mut self, tracing: bool) {
        match self {
            Self::Batch(batch) => batch.options.inner.set_tracing(tracing),
            Self::Statement(statement) => statement.set_tracing(tracing),
        }
    }

    fn set_execution_profile_handle(&mut self, handle: ExecutionProfileHandle) {
        match self {
            Self::Batch(batch) => batch
                .options
                .inner
                .set_execution_profile_handle(Some(handle)),
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
            Self::Batch(batch) => batch.options.inner.get_request_timeout(),
            Self::Statement(statement) => statement.request_timeout(),
        }
    }

    fn set_consistency(&mut self, consistency: Consistency) {
        match self {
            Self::Batch(batch) => batch.options.inner.set_consistency(consistency),
            Self::Statement(statement) => statement.set_consistency(consistency),
        }
    }

    /// The request's own consistency; `None` defers to its execution profile.
    fn consistency(&self) -> Option<Consistency> {
        match self {
            Self::Batch(batch) => batch.options.inner.get_consistency(),
            Self::Statement(statement) => statement.consistency(),
        }
    }

    fn set_serial_consistency(&mut self, serial: Option<SerialConsistency>) {
        match self {
            Self::Batch(batch) => {
                batch.options.inner.set_serial_consistency(serial);
                batch.options.is_serial_consistency_set = true;
            }
            Self::Statement(statement) => statement.set_serial_consistency(serial),
        }
    }

    /// Whether the request sets its own serial consistency, `None` included.
    fn is_serial_consistency_set(&self) -> bool {
        match self {
            Self::Batch(batch) => batch.options.is_serial_consistency_set,
            Self::Statement(statement) => statement.is_serial_consistency_set(),
        }
    }

    /// The request's own execution profile; `None` defers to the session's default.
    fn execution_profile_handle(&self) -> Option<&ExecutionProfileHandle> {
        match self {
            Self::Batch(batch) => batch.options.inner.get_execution_profile_handle(),
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

    pub(crate) fn is_batch(&self) -> bool {
        matches!(self, Self::Batch(_))
    }
}

impl<'py> FromPyObject<'_, 'py> for LegacyQuery {
    type Error = DriverStatementConversionError;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(batch) = obj.cast::<PyBatch>() {
            return Ok(Self::Batch(batch.get().snapshot(obj.py())));
        }

        Ok(Self::Statement(ExecutableStatement::extract(obj)?))
    }
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

/// The `timeout` argument of `execute()`; the legacy `_NOT_SET` sentinel is `UNSET`.
#[derive(Clone, Copy, Default)]
enum RequestTimeout {
    /// Not given: the statement's, else the profile's, timeout applies.
    #[default]
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
        Ok(match obj.extract::<MaybeUnset<Option<PyDuration>>>()? {
            MaybeUnset::Unset => Self::Default,
            MaybeUnset::Set(None) => Self::Never,
            MaybeUnset::Set(Some(duration)) => Self::After(duration.0),
        })
    }
}

/// The `paging_state` argument: a `PagingState`, or its raw bytes as the legacy driver kept them.
struct LegacyPagingState(PagingState);

impl<'py> FromPyObject<'_, 'py> for LegacyPagingState {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
        if let Ok(state) = obj.cast::<PyPagingState>() {
            return Ok(Self(state.get().inner().clone()));
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
fn error_if_present(name: &str, value: Option<Bound<'_, PyAny>>) -> PyResult<()> {
    match value {
        None => Ok(()),
        Some(_) => Err(PyNotImplementedError::new_err(format!(
            "{name} is not supported by this driver"
        ))),
    }
}
