use std::sync::{Arc, Mutex};
use std::time::Duration;

use pyo3::prelude::*;
use pyo3::sync::MutexExt;
use pyo3::types::PyString;
use scylla::client::execution_profile::ExecutionProfileHandle;
use scylla::client::session::Session;
use scylla::response::query_result::QueryResult;
use scylla::statement::batch::BatchStatement;
use scylla::statement::prepared::PreparedStatement;
use scylla::statement::unprepared::Statement;
use scylla_cql::frame::request::query::{PagingState, PagingStateResponse};
use scylla_cql::serialize::row::SerializedValues;
use uuid::Uuid;

use crate::RUNTIME;
use crate::batch::PyBatch;
use crate::cluster::state::PyClusterState;
use crate::core::results::{Pager, RequestResultCore};
use crate::deserialize::results::RowFactory;
use crate::errors::execution::{
    DriverExecuteError, DriverPrepareError, DriverSchemaAgreementError,
    DriverStatementConversionError, DriverUseKeyspaceError,
};
use crate::future::{BoxedFuture, boxed_py_future};
use crate::policies::load_balancing::PyTargetPolicy;
use crate::serialize::value_list::PyValueList;
use crate::statement::{PyPreparedStatement, PyStatement, PyStatementSettings};

/// Where a page fetch runs relative to the future that awaits it.
#[derive(Clone, Copy)]
pub(crate) enum PageFetch {
    /// On a runtime worker, joined by the awaiting future. For futures polled
    /// from a Python thread, so the request never runs there.
    SpawnOnRuntime,
    /// Inline in the awaiting future. For futures that already run on the runtime.
    Inline,
}

/// Helper performing the core logic of executing queries.
#[derive(Clone)]
pub(crate) struct SessionCore {
    pub(crate) inner: Arc<Session>,
    /// Cached Python snapshot of the cluster state. Shared by every facade
    /// wrapping this core, so one underlying session has exactly one cache.
    cluster_state: Arc<Mutex<Py<PyClusterState>>>,
    page_fetch: PageFetch,
}

impl TryFrom<Arc<Session>> for SessionCore {
    type Error = PyErr;

    fn try_from(inner: Arc<Session>) -> Result<Self, Self::Error> {
        let cluster_state =
            Python::attach(|py| Py::new(py, PyClusterState::try_from(inner.get_cluster_state())?))?;
        Ok(Self {
            cluster_state: Arc::new(Mutex::new(cluster_state)),
            inner,
            page_fetch: PageFetch::SpawnOnRuntime,
        })
    }
}

/// Every request method returns the boxed future performing it, resolving to a
/// core type; the facade decides how to drive it and whether to convert the
/// output to Python.
impl SessionCore {
    /// The same session, fetching pages the given way.
    pub(crate) fn with_page_fetch(mut self, page_fetch: PageFetch) -> Self {
        self.page_fetch = page_fetch;
        self
    }

    pub(crate) fn use_keyspace(
        self,
        keyspace: String,
        case_sensitive: bool,
    ) -> BoxedFuture<(), DriverUseKeyspaceError> {
        boxed_py_future(async move {
            self.inner
                .use_keyspace(keyspace, case_sensitive)
                .await
                .map_err(DriverUseKeyspaceError::from)
        })
    }

    /// Executes `statement`, returning the future that performs the request.
    pub(crate) fn execute(
        self,
        statement: ExecutableStatement,
        values: PyValueList,
        factory: Option<Py<RowFactory>>,
        paging_state: Option<PagingState>,
        paged: bool,
    ) -> Result<BoxedFuture<RequestResultCore, DriverExecuteError>, DriverExecuteError> {
        let request = if paged {
            ExecutionParams::Paged {
                prepared: Arc::new(BoundStatement::new(statement, values)?),
                paging_state: paging_state.unwrap_or_else(PagingState::start),
            }
        } else {
            if paging_state.is_some() {
                return Err(DriverExecuteError::paging_state_must_be_none_for_unpaged_execution());
            }

            ExecutionParams::Unpaged {
                prepared: BoundStatement::new(statement, values)?,
            }
        };

        Ok(boxed_py_future(async move {
            match request {
                ExecutionParams::Unpaged { prepared } => {
                    self.execute_unpaged(prepared, factory).await
                }
                ExecutionParams::Paged {
                    prepared,
                    paging_state,
                } => self.execute_paged(prepared, paging_state, factory).await,
            }
        }))
    }

    pub(crate) fn prepare(
        self,
        statement: PreparableStatement,
    ) -> BoxedFuture<PyPreparedStatement, DriverPrepareError> {
        let PreparableStatement(py_statement) = statement;

        boxed_py_future(async move {
            let is_page_size_set = py_statement.is_page_size_set();
            match self.inner.prepare(py_statement.inner).await {
                Ok(prepared) => {
                    let is_serial_consistency_set = prepared.get_serial_consistency().is_some();
                    Ok(PyPreparedStatement::new(
                        prepared,
                        is_serial_consistency_set,
                        is_page_size_set,
                        py_statement.settings,
                    ))
                }
                Err(err) => Err(DriverPrepareError::rust_driver_prepare_error(err)),
            }
        })
    }

    pub(crate) fn batch(
        self,
        batch: PyBatch,
        factory: Option<Py<RowFactory>>,
    ) -> BoxedFuture<RequestResultCore, DriverExecuteError> {
        boxed_py_future(async move {
            let result = self
                .inner
                .batch(&batch.inner, batch.values)
                .await
                .map_err(DriverExecuteError::rust_driver_execution_error)?;

            Ok(RequestResultCore::new(result, Pager::unpaged(), factory))
        })
    }

    pub(crate) fn await_schema_agreement(self) -> BoxedFuture<Uuid, DriverSchemaAgreementError> {
        boxed_py_future(async move {
            self.inner
                .await_schema_agreement()
                .await
                .map_err(DriverSchemaAgreementError::rust_driver_schema_agreement_error)
        })
    }

    pub(crate) fn check_schema_agreement(
        self,
    ) -> BoxedFuture<Option<Uuid>, DriverSchemaAgreementError> {
        boxed_py_future(async move {
            self.inner
                .check_schema_agreement()
                .await
                .map_err(DriverSchemaAgreementError::rust_driver_schema_agreement_error)
        })
    }

    /// Returns the cached Python cluster state snapshot, refreshing it first if
    /// the Rust driver has since replaced its own.
    pub(crate) fn cluster_state(&self, py: Python<'_>) -> PyResult<Py<PyClusterState>> {
        // PyClusterState holds `Arc<ClusterState>` preventing Rust driver from replacing
        // inner Rust `Session`'s `ClusterState` with a new object in the same memory.
        //
        // This means by comparing current Rust `Session` `ClusterState` pointer
        // and `PyClusterState`'s internal `ClusterState` pointer
        // we can determine if the `PyClusterState`'s snapshot is stale
        // and needs to be replaced with a fresh snapshot.
        let mut py_cluster_state = self.cluster_state.lock_py_attached(py).unwrap();
        let rust_current_cluster_state = self.inner.get_cluster_state();
        let python_snapshot_cluster_state = &py_cluster_state.get().inner;
        if !Arc::ptr_eq(&rust_current_cluster_state, python_snapshot_cluster_state) {
            *py_cluster_state = Py::new(
                py,
                PyClusterState::try_from(self.inner.get_cluster_state())?,
            )?;
        }

        Ok(py_cluster_state.clone_ref(py))
    }

    async fn execute_unpaged(
        self,
        prepared: BoundStatement,
        factory: Option<Py<RowFactory>>,
    ) -> Result<RequestResultCore, DriverExecuteError> {
        let result = match prepared {
            BoundStatement::Prepared(p, serialized_values) => self
                .inner
                .execute_unstable(&p, &serialized_values, false, PagingState::start())
                .await
                .map(|(result, _paging_response)| result)
                .map_err(DriverExecuteError::rust_driver_execution_error),
            BoundStatement::Unprepared(q, values) => self
                .inner
                .query_unpaged(q, values)
                .await
                .map_err(DriverExecuteError::rust_driver_execution_error),
        }?;

        Ok(RequestResultCore::new(result, Pager::unpaged(), factory))
    }

    async fn execute_paged(
        self,
        prepared: Arc<BoundStatement>,
        paging_state: PagingState,
        factory: Option<Py<RowFactory>>,
    ) -> Result<RequestResultCore, DriverExecuteError> {
        let (result, paging_response) =
            fetch_page(Arc::clone(&self.inner), paging_state, Arc::clone(&prepared)).await?;

        Ok(RequestResultCore::new(
            result,
            Pager::paged(paging_response, self, prepared),
            factory,
        ))
    }

    /// Fetches one page, running it where [`PageFetch`] says.
    pub(crate) async fn execute_single_page(
        &self,
        paging_state: PagingState,
        prepared: Arc<BoundStatement>,
    ) -> Result<(QueryResult, PagingStateResponse), DriverExecuteError> {
        let page = fetch_page(Arc::clone(&self.inner), paging_state, prepared);
        match self.page_fetch {
            PageFetch::SpawnOnRuntime => RUNTIME.spawn(page).await?,
            PageFetch::Inline => page.await,
        }
    }
}

/// Requests one page of `prepared`. Owns its arguments so it can be spawned or awaited alike.
async fn fetch_page(
    session: Arc<Session>,
    paging_state: PagingState,
    prepared: Arc<BoundStatement>,
) -> Result<(QueryResult, PagingStateResponse), DriverExecuteError> {
    match &*prepared {
        BoundStatement::Prepared(p, serialized_values) => session
            .execute_unstable(p, serialized_values, true, paging_state)
            .await
            .map_err(DriverExecuteError::rust_driver_execution_error),
        BoundStatement::Unprepared(q, values) => session
            .query_single_page(q.clone(), values, paging_state)
            .await
            .map_err(DriverExecuteError::rust_driver_execution_error),
    }
}

/// A request with everything the future needs already gathered: values
/// serialized and paging mode decided, all while the calling thread still
/// holds the GIL.
enum ExecutionParams {
    Unpaged {
        prepared: BoundStatement,
    },
    Paged {
        prepared: Arc<BoundStatement>,
        paging_state: PagingState,
    },
}

/// An [`ExecutableStatement`] with its bind values already serialized.
///
/// Serialization needs the GIL  and is pure CPU work,
/// so it is done up front on the calling thread
pub(crate) enum BoundStatement {
    Prepared(PreparedStatement, SerializedValues),
    Unprepared(Statement, PyValueList),
}

impl BoundStatement {
    pub(crate) fn new(
        statement: ExecutableStatement,
        values: PyValueList,
    ) -> Result<Self, DriverExecuteError> {
        Ok(match statement.kind {
            StatementKind::Prepared(p) => {
                let serialized_values = p
                    .serialize_values_unstable(&values)
                    .map_err(DriverExecuteError::serialization_failed)?;
                BoundStatement::Prepared(p, serialized_values)
            }
            StatementKind::Unprepared(q) => BoundStatement::Unprepared(q, values),
        })
    }
}

#[derive(Clone)]
pub(crate) struct ExecutableStatement {
    pub(crate) kind: StatementKind,
    /// The Rust driver cannot tell an explicit page size from its default. The
    /// legacy session applies its own default only to a statement without one.
    is_page_size_set: bool,
}

#[derive(Clone)]
pub(crate) enum StatementKind {
    Prepared(PreparedStatement),
    Unprepared(Statement),
}

/// Per-execution overrides of the statement's own settings.
impl ExecutableStatement {
    /// Pins this statement to a single target for one execution.
    pub(crate) fn set_target(&mut self, target: PyTargetPolicy) {
        let policy = target.into_inner();
        match &mut self.kind {
            StatementKind::Prepared(prepared) => prepared.set_load_balancing_policy(Some(policy)),
            StatementKind::Unprepared(statement) => {
                statement.set_load_balancing_policy(Some(policy))
            }
        }
    }

    pub(crate) fn set_request_timeout(&mut self, timeout: Duration) {
        match &mut self.kind {
            StatementKind::Prepared(prepared) => prepared.set_request_timeout(Some(timeout)),
            StatementKind::Unprepared(statement) => statement.set_request_timeout(Some(timeout)),
        }
    }

    pub(crate) fn set_tracing(&mut self, tracing: bool) {
        match &mut self.kind {
            StatementKind::Prepared(prepared) => prepared.set_tracing(tracing),
            StatementKind::Unprepared(statement) => statement.set_tracing(tracing),
        }
    }

    pub(crate) fn set_page_size(&mut self, page_size: i32) {
        match &mut self.kind {
            StatementKind::Prepared(prepared) => prepared.set_page_size(page_size),
            StatementKind::Unprepared(statement) => statement.set_page_size(page_size),
        }
    }

    pub(crate) fn set_execution_profile_handle(&mut self, handle: ExecutionProfileHandle) {
        match &mut self.kind {
            StatementKind::Prepared(prepared) => {
                prepared.set_execution_profile_handle(Some(handle))
            }
            StatementKind::Unprepared(statement) => {
                statement.set_execution_profile_handle(Some(handle))
            }
        }
    }

    /// The statement's own timeout; `None` defers to its execution profile.
    pub(crate) fn request_timeout(&self) -> Option<Duration> {
        match &self.kind {
            StatementKind::Prepared(prepared) => prepared.get_request_timeout(),
            StatementKind::Unprepared(statement) => statement.get_request_timeout(),
        }
    }

    /// The statement's own execution profile; `None` defers to the session's default.
    pub(crate) fn execution_profile_handle(&self) -> Option<&ExecutionProfileHandle> {
        match &self.kind {
            StatementKind::Prepared(prepared) => prepared.get_execution_profile_handle(),
            StatementKind::Unprepared(statement) => statement.get_execution_profile_handle(),
        }
    }

    /// Whether the statement sets its own page size.
    pub(crate) fn is_page_size_set(&self) -> bool {
        self.is_page_size_set
    }

    /// The CQL text of the statement.
    pub(crate) fn contents(&self) -> &str {
        match &self.kind {
            StatementKind::Prepared(prepared) => prepared.get_statement(),
            StatementKind::Unprepared(statement) => &statement.contents,
        }
    }
}

impl<'py> FromPyObject<'_, 'py> for ExecutableStatement {
    type Error = DriverStatementConversionError;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(prepared) = obj.cast::<PyPreparedStatement>() {
            let prepared = prepared.get();
            return Ok(ExecutableStatement {
                kind: StatementKind::Prepared(prepared.inner.clone()),
                is_page_size_set: prepared.is_page_size_set(),
            });
        }

        if let Ok(text) = obj.cast::<PyString>() {
            let text = text
                .to_str()
                .map_err(DriverStatementConversionError::statement_string_conversion_failed)?;
            return Ok(ExecutableStatement {
                kind: StatementKind::Unprepared(text.into()),
                is_page_size_set: false,
            });
        }

        if let Ok(statement) = obj.cast::<PyStatement>() {
            let statement = statement.get();
            return Ok(ExecutableStatement {
                kind: StatementKind::Unprepared(statement.inner.clone()),
                is_page_size_set: statement.is_page_size_set(),
            });
        }

        Err(DriverStatementConversionError::invalid_statement_type(obj))
    }
}

/// The input to `Session.prepare`: a query string or a `Statement`.
pub(crate) struct PreparableStatement(PyStatement);

impl<'py> FromPyObject<'_, 'py> for PreparableStatement {
    type Error = DriverStatementConversionError;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if obj.cast::<PyPreparedStatement>().is_ok() {
            return Err(DriverStatementConversionError::cannot_prepare_prepared_statement());
        }

        if let Ok(text) = obj.cast::<PyString>() {
            let text = text
                .to_str()
                .map_err(DriverStatementConversionError::statement_string_conversion_failed)?;
            return Ok(PreparableStatement(PyStatement::new(
                text.into(),
                false,
                false,
                PyStatementSettings::default(),
            )));
        }

        if let Ok(statement) = obj.cast::<PyStatement>() {
            return Ok(PreparableStatement(statement.get().clone()));
        }

        Err(DriverStatementConversionError::invalid_statement_type(obj))
    }
}

impl From<ExecutableStatement> for BatchStatement {
    fn from(s: ExecutableStatement) -> Self {
        match s.kind {
            StatementKind::Prepared(p) => BatchStatement::PreparedStatement(p),
            StatementKind::Unprepared(q) => BatchStatement::Query(q),
        }
    }
}
