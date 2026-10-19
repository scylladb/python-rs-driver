//! `LegacySession`: the blocking, callback-driven session of the legacy driver.

use std::sync::Arc;
use std::time::Duration;

use pyo3::exceptions::{PyNotImplementedError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use scylla::client::execution_profile::ExecutionProfileHandle;
use scylla::client::session::Session;
use scylla_cql::frame::request::query::PagingState;

use crate::batch::{BatchState, PyBatch};
use crate::core::session::{ExecutableStatement, SessionCore};
use crate::deserialize::row_factory::PyRowFactory;
use crate::errors::execution::DriverStatementConversionError;
use crate::execution_profile::PyExecutionProfile;
use crate::future::{LegacyRequestFuture, PyResponseFuture};
use crate::legacy::PyResultSet;
use crate::policies::load_balancing::PyTargetPolicy;
use crate::results::PyPagingState;
use crate::serialize::value_list::PyValueList;
use crate::types::MaybeUnset;
use crate::utils::{PyDuration, WithOriginalPyObject};

/// The legacy (`cassandra-driver` compatible) session: blocking `execute()`,
/// `execute_async()` returning a `ResponseFuture`.
///
/// A facade over [`SessionCore`] like `Session`.
#[pyclass(module = "scylla.legacy", name = "LegacySession", frozen)]
pub(crate) struct PyLegacySession {
    core: SessionCore,
}

impl PyLegacySession {
    pub(crate) fn new(
        inner: Arc<Session>,
        default_row_factory: Option<PyRowFactory>,
    ) -> Result<Self, PyErr> {
        Ok(Self {
            core: SessionCore::new(inner, default_row_factory)?,
        })
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

        // The factory itself is for the request; the object as set is for the future to report.
        let mut request_factory = None;
        let mut row_factory = None;

        // The future keeps `query` as extracted; the overrides go on a copy.
        let mut executable = query.extracted.clone();
        if let Some(d) = timeout.duration() {
            executable.set_request_timeout(d);
        }
        if let Some(trace) = trace {
            executable.set_tracing(trace);
        }
        if let Some(profile) = execution_profile {
            let profile = profile.get();
            executable.set_execution_profile_handle(profile.inner.clone().into_handle());
            if let Some(factory) = &profile.row_factory {
                request_factory = Some(factory.extracted.clone());
                row_factory = Some(factory.original.clone_ref(py));
            }
        }
        if let Some(target) = host {
            executable.set_target(target);
        }
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
                true,
            )?),
        };

        PyResponseFuture::spawn(py, request, query, timeout_secs, row_factory)
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

    /// The request's own timeout; `None` defers to its execution profile.
    fn request_timeout(&self) -> Option<Duration> {
        match self {
            Self::Batch(batch) => batch.options.inner.get_request_timeout(),
            Self::Statement(statement) => statement.request_timeout(),
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

/// The `timeout` argument of `execute()`; the legacy `_NOT_SET` sentinel is `UNSET`.
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
