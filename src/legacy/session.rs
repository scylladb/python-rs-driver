//! `LegacySession`: the blocking, callback-driven session of the legacy driver.

use std::sync::Arc;
use std::time::Duration;

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use scylla::client::execution_profile::ExecutionProfileHandle;
use scylla::client::session::Session;
use scylla_cql::frame::request::query::PagingState;

use crate::batch::{BatchState, PyBatch};
use crate::core::session::{ExecutableStatement, SessionCore};
use crate::deserialize::row_factory::PyRowFactory;
use crate::errors::execution::DriverStatementConversionError;
use crate::future::{LegacyRequestFuture, PyResponseFuture};
use crate::legacy::PyResultSet;
use crate::results::PyPagingState;
use crate::serialize::value_list::PyValueList;
use crate::utils::WithOriginalPyObject;

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
    #[pyo3(signature = (query, parameters=None, paging_state=None))]
    fn execute_async(
        &self,
        py: Python<'_>,
        query: WithOriginalPyObject<LegacyQuery>,
        parameters: Option<PyValueList>,
        paging_state: Option<LegacyPagingState>,
    ) -> PyResult<Py<PyResponseFuture>> {
        // The future keeps `query` as extracted; the request takes a copy.
        let executable = query.extracted.clone();
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

                Box::pin(self.core.clone().batch(batch, None))
            }

            LegacyQuery::Statement(statement) => Box::pin(self.core.clone().execute(
                statement,
                parameters.unwrap_or_default(),
                None,
                paging_state.map(|state| state.0),
                true,
            )?),
        };

        PyResponseFuture::spawn(py, request, query, timeout_secs, None)
    }

    /// Sends `query` and blocks for its `ResultSet`.
    #[pyo3(signature = (query, parameters=None, paging_state=None))]
    fn execute(
        &self,
        py: Python<'_>,
        query: WithOriginalPyObject<LegacyQuery>,
        parameters: Option<PyValueList>,
        paging_state: Option<LegacyPagingState>,
    ) -> PyResult<Py<PyResultSet>> {
        let future = self.execute_async(py, query, parameters, paging_state)?;
        PyResponseFuture::result(future, py)
    }
}

/// What `execute_async` accepts: a batch, or anything executable as one statement.
#[derive(Clone)]
pub(crate) enum LegacyQuery {
    Batch(BatchState),
    Statement(ExecutableStatement),
}

impl LegacyQuery {
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
