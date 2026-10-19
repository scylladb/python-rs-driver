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

use crate::batch::BatchState;
use crate::cluster::state::PyClusterState;
use crate::core::results::{Pager, PendingRequestResult};
use crate::core::traces::TraceLog;
use crate::deserialize::row_factory::PyRowFactory;
use crate::errors::execution::{
    DriverExecuteError, DriverMetadataRefreshError, DriverPrepareError, DriverSchemaAgreementError,
    DriverStatementConversionError, DriverUseKeyspaceError,
};
use crate::policies::load_balancing::PyTargetPolicy;
use crate::serialize::value_list::PyValueList;
use crate::statement::{
    PyPreparedStatement, PyStatement, PyStatementSettings, StatementClass, StatementOptions,
};
use crate::{RUNTIME, TaskExecutionMode};

/// Helper performing the core logic of executing queries.
#[derive(Clone)]
pub(crate) struct SessionCore {
    pub(crate) inner: Arc<Session>,
    /// Cached Python snapshot of the cluster state. Shared by every facade
    /// wrapping this core, so one underlying session has exactly one cache.
    cluster_state: Arc<Mutex<Py<PyClusterState>>>,
    /// Row factory of the default execution profile, taken at connect time.
    default_row_factory: Option<PyRowFactory>,
}

impl SessionCore {
    pub(crate) fn new(
        inner: Arc<Session>,
        default_row_factory: Option<PyRowFactory>,
    ) -> Result<Self, PyErr> {
        let cluster_state =
            Python::attach(|py| Py::new(py, PyClusterState::try_from(inner.get_cluster_state())?))?;
        Ok(Self {
            cluster_state: Arc::new(Mutex::new(cluster_state)),
            inner,
            default_row_factory,
        })
    }
}

impl SessionCore {
    pub(crate) async fn use_keyspace(
        self,
        keyspace: String,
        case_sensitive: bool,
    ) -> Result<(), DriverUseKeyspaceError> {
        self.inner
            .use_keyspace(keyspace, case_sensitive)
            .await
            .map_err(DriverUseKeyspaceError::from)
    }

    /// Picks the row factory for one request, in order of priority:
    /// - the `factory` argument to `execute` or `batch`,
    /// - what the statement or batch asks for (its own, else its execution
    ///   profile's),
    /// - the session's default profile,
    /// - the built-in namedtuple factory.
    fn choose_row_factory(
        &self,
        explicit: Option<PyRowFactory>,
        statement: Option<PyRowFactory>,
    ) -> PyRowFactory {
        explicit
            .or(statement)
            .or_else(|| self.default_row_factory.clone())
            .unwrap_or(PyRowFactory::NamedTuple)
    }

    /// Executes `statement`, returning the future that performs the request.
    pub(crate) fn execute(
        self,
        statement: ExecutableStatement,
        values: PyValueList,
        explicit_factory: Option<PyRowFactory>,
        paging_state: Option<PagingState>,
        paged: bool,
    ) -> Result<
        impl Future<Output = Result<PendingRequestResult, DriverExecuteError>> + Send + 'static,
        DriverExecuteError,
    > {
        let (kind, statement_factory) = statement.into_parts();
        let effective_factory = self.choose_row_factory(explicit_factory, statement_factory);

        let request = if paged {
            ExecutionParams::Paged {
                prepared: Arc::new(BoundStatement::new(kind, values)?),
                paging_state: paging_state.unwrap_or_else(PagingState::start),
            }
        } else {
            if paging_state.is_some() {
                return Err(DriverExecuteError::paging_state_must_be_none_for_unpaged_execution());
            }

            ExecutionParams::Unpaged {
                prepared: BoundStatement::new(kind, values)?,
            }
        };

        Ok(async move {
            match request {
                ExecutionParams::Unpaged { prepared } => {
                    self.execute_unpaged(prepared, effective_factory).await
                }
                ExecutionParams::Paged {
                    prepared,
                    paging_state,
                } => {
                    self.execute_paged(prepared, paging_state, effective_factory)
                        .await
                }
            }
        })
    }

    pub(crate) fn prepare(
        self,
        statement: PreparableStatement,
    ) -> impl Future<Output = Result<PyPreparedStatement, DriverPrepareError>> + Send + 'static
    {
        let PreparableStatement(StatementOptions {
            inner,
            is_serial_consistency_set,
            is_page_size_set,
            settings,
        }) = statement;

        async move {
            match self.inner.prepare(inner).await {
                Ok(prepared) => Ok(PyPreparedStatement::new(StatementOptions {
                    is_page_size_set,
                    ..StatementOptions::new(prepared, is_serial_consistency_set, settings)
                })),
                Err(err) => Err(DriverPrepareError::rust_driver_prepare_error(err)),
            }
        }
    }

    pub(crate) fn batch(
        self,
        batch: BatchState,
        explicit_factory: Option<PyRowFactory>,
    ) -> impl Future<Output = Result<PendingRequestResult, DriverExecuteError>> + Send + 'static
    {
        let effective_factory =
            self.choose_row_factory(explicit_factory, batch.options.settings.row_factory());

        async move {
            let result = self
                .inner
                .batch(&batch.options.inner, batch.values)
                .await
                .map_err(DriverExecuteError::rust_driver_execution_error)?;

            let traces = TraceLog::new(result.tracing_id());
            Ok(PendingRequestResult::new(
                result,
                Pager::unpaged(self.inner, traces),
                effective_factory,
            ))
        }
    }

    pub(crate) async fn await_schema_agreement(self) -> Result<Uuid, DriverSchemaAgreementError> {
        self.inner
            .await_schema_agreement()
            .await
            .map_err(DriverSchemaAgreementError::rust_driver_schema_agreement_error)
    }

    pub(crate) async fn check_schema_agreement(
        self,
    ) -> Result<Option<Uuid>, DriverSchemaAgreementError> {
        self.inner
            .check_schema_agreement()
            .await
            .map_err(DriverSchemaAgreementError::rust_driver_schema_agreement_error)
    }

    pub(crate) async fn refresh_metadata(self) -> Result<(), DriverMetadataRefreshError> {
        self.inner
            .refresh_metadata()
            .await
            .map_err(DriverMetadataRefreshError::rust_driver_metadata_error)
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
        factory: PyRowFactory,
    ) -> Result<PendingRequestResult, DriverExecuteError> {
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

        let traces = TraceLog::new(result.tracing_id());
        Ok(PendingRequestResult::new(
            result,
            Pager::unpaged(self.inner, traces),
            factory,
        ))
    }

    async fn execute_paged(
        self,
        prepared: Arc<BoundStatement>,
        paging_state: PagingState,
        factory: PyRowFactory,
    ) -> Result<PendingRequestResult, DriverExecuteError> {
        // Already on the runtime, as `Session.execute` spawns its future there.
        let (result, paging_response) = fetch_page(
            Arc::clone(&self.inner),
            paging_state,
            Arc::clone(&prepared),
            TaskExecutionMode::Inline,
        )
        .await?;

        let traces = TraceLog::new(result.tracing_id());
        Ok(PendingRequestResult::new(
            result,
            Pager::paged(paging_response, self.inner, prepared, traces),
            factory,
        ))
    }
}

/// Requests one page of `prepared`, running it where `mode` says.
pub(crate) async fn fetch_page(
    session: Arc<Session>,
    paging_state: PagingState,
    prepared: Arc<BoundStatement>,
    mode: TaskExecutionMode,
) -> Result<(QueryResult, PagingStateResponse), DriverExecuteError> {
    let page = async move {
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
    };

    match mode {
        TaskExecutionMode::SpawnOnRuntime => RUNTIME.spawn(page).await?,
        TaskExecutionMode::Inline => page.await,
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

/// A [`StatementKind`] with its bind values already serialized.
///
/// Serialization needs the GIL  and is pure CPU work,
/// so it is done up front on the calling thread
pub(crate) enum BoundStatement {
    Prepared(PreparedStatement, SerializedValues),
    Unprepared(Statement, PyValueList),
}

impl BoundStatement {
    pub(crate) fn new(
        statement: StatementKind,
        values: PyValueList,
    ) -> Result<Self, DriverExecuteError> {
        Ok(match statement {
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

/// A statement ready to run: the Rust statement, plus the row factory it asks
/// for.
#[derive(Clone)]
pub(crate) struct ExecutableStatement {
    kind: StatementKind,
    /// What the statement asks for: its own row factory, else its execution
    /// profile's. `None` when it asks for neither.
    row_factory: Option<PyRowFactory>,
    /// The Rust driver cannot tell an explicit page size from its default. The
    /// legacy session applies its own default only to a statement without one.
    is_page_size_set: bool,
}

#[derive(Clone)]
pub(crate) enum StatementKind {
    Prepared(PreparedStatement),
    Unprepared(Statement),
}

impl StatementKind {
    /// Pins this statement to a single target for one execution.
    pub(crate) fn set_target(&mut self, target: PyTargetPolicy) {
        let policy = target.into_inner();
        match self {
            Self::Prepared(prepared) => prepared.set_load_balancing_policy(Some(policy)),
            Self::Unprepared(statement) => statement.set_load_balancing_policy(Some(policy)),
        }
    }

    /// Turns tracing on or off for one execution.
    pub(crate) fn set_tracing(&mut self, should_trace: bool) {
        match self {
            Self::Prepared(prepared) => prepared.set_tracing(should_trace),
            Self::Unprepared(statement) => statement.set_tracing(should_trace),
        }
    }

    /// The statement's own timeout; `None` defers to its execution profile.
    pub(crate) fn request_timeout(&self) -> Option<Duration> {
        match self {
            Self::Prepared(prepared) => prepared.get_request_timeout(),
            Self::Unprepared(statement) => statement.get_request_timeout(),
        }
    }

    /// The statement's own execution profile; `None` defers to the session's default.
    pub(crate) fn execution_profile_handle(&self) -> Option<&ExecutionProfileHandle> {
        match self {
            Self::Prepared(prepared) => prepared.get_execution_profile_handle(),
            Self::Unprepared(statement) => statement.get_execution_profile_handle(),
        }
    }
}

impl ExecutableStatement {
    pub(crate) fn set_target(&mut self, target: PyTargetPolicy) {
        self.kind.set_target(target);
    }

    pub(crate) fn set_tracing(&mut self, should_trace: bool) {
        self.kind.set_tracing(should_trace);
    }

    pub(crate) fn into_parts(self) -> (StatementKind, Option<PyRowFactory>) {
        (self.kind, self.row_factory)
    }

    pub(crate) fn request_timeout(&self) -> Option<Duration> {
        self.kind.request_timeout()
    }

    pub(crate) fn execution_profile_handle(&self) -> Option<&ExecutionProfileHandle> {
        self.kind.execution_profile_handle()
    }

    /// Whether the statement sets its own page size.
    #[expect(dead_code, reason = "LegacySession reads it; it lands in a later PR")]
    pub(crate) fn is_page_size_set(&self) -> bool {
        self.is_page_size_set
    }
}

impl<'py> FromPyObject<'_, 'py> for ExecutableStatement {
    type Error = DriverStatementConversionError;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(prepared) = obj.cast::<PyPreparedStatement>() {
            return Ok(prepared
                .get()
                .with_options(obj.py(), |o| ExecutableStatement {
                    kind: StatementKind::Prepared(o.inner.clone()),
                    row_factory: o.settings.row_factory(),
                    is_page_size_set: o.is_page_size_set,
                }));
        }

        if let Ok(text) = obj.cast::<PyString>() {
            let text = text
                .to_str()
                .map_err(DriverStatementConversionError::statement_string_conversion_failed)?;
            return Ok(ExecutableStatement {
                kind: StatementKind::Unprepared(text.into()),
                row_factory: None,
                is_page_size_set: false,
            });
        }

        if let Ok(statement) = obj.cast::<PyStatement>() {
            return Ok(statement
                .get()
                .with_options(obj.py(), |o| ExecutableStatement {
                    kind: StatementKind::Unprepared(o.inner.clone()),
                    row_factory: o.settings.row_factory(),
                    is_page_size_set: o.is_page_size_set,
                }));
        }

        Err(DriverStatementConversionError::invalid_statement_type(obj))
    }
}

/// The input to `Session.prepare`: a query string or a `Statement`.
pub(crate) struct PreparableStatement(StatementOptions<Statement>);

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
            return Ok(PreparableStatement(StatementOptions::new(
                text.into(),
                false,
                PyStatementSettings::default(),
            )));
        }

        if let Ok(statement) = obj.cast::<PyStatement>() {
            return Ok(PreparableStatement(
                statement.get().with_options(obj.py(), |o| o.clone()),
            ));
        }

        Err(DriverStatementConversionError::invalid_statement_type(obj))
    }
}

impl From<ExecutableStatement> for BatchStatement {
    fn from(s: ExecutableStatement) -> Self {
        match s.into_parts().0 {
            StatementKind::Prepared(p) => BatchStatement::PreparedStatement(p),
            StatementKind::Unprepared(q) => BatchStatement::Query(q),
        }
    }
}
