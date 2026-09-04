use std::future::Future;
use std::sync::{Arc, Mutex};

use pyo3::prelude::*;
use pyo3::sync::MutexExt;
use pyo3::types::PyString;
use scylla::client::session::Session;
use scylla::response::query_result::QueryResult;
use scylla::statement::batch::BatchStatement;
use scylla::statement::prepared::PreparedStatement;
use scylla_cql::frame::request::query::{PagingState, PagingStateResponse};
use scylla_cql::serialize::row::SerializedValues;
use uuid::Uuid;

use crate::RUNTIME;
use crate::batch::PyBatch;
use crate::cluster::state::PyClusterState;
use crate::core::results::{Pager, RequestResultCore};
use crate::deserialize::results::{RequestResult, RowFactory};
use crate::errors::{
    DriverExecuteError, DriverPrepareError, DriverSchemaAgreementError,
    DriverStatementConversionError, DriverUseKeyspaceError,
};
use crate::future::{BoxedFuture, boxed_py_future};
use crate::serialize::value_list::PyValueList;
use crate::statement::{PyPreparedStatement, PyStatement};

/// Helper performing the core logic of executing queries.
#[derive(Clone)]
pub(crate) struct SessionCore {
    pub(crate) inner: Arc<Session>,
    /// Cached Python snapshot of the cluster state. Shared by every facade
    /// wrapping this core, so one underlying session has exactly one cache.
    cluster_state: Arc<Mutex<Py<PyClusterState>>>,
}

impl TryFrom<Arc<Session>> for SessionCore {
    type Error = PyErr;

    fn try_from(inner: Arc<Session>) -> Result<Self, Self::Error> {
        let cluster_state =
            Python::attach(|py| Py::new(py, PyClusterState::try_from(inner.get_cluster_state())?))?;
        Ok(Self {
            cluster_state: Arc::new(Mutex::new(cluster_state)),
            inner,
        })
    }
}

impl SessionCore {
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
    ) -> Result<BoxedFuture<RequestResult, DriverExecuteError>, DriverExecuteError> {
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
            .map(RequestResult::from)
        }))
    }

    pub(crate) fn prepare(
        self,
        statement: ExecutableStatement,
    ) -> BoxedFuture<PyPreparedStatement, DriverPrepareError> {
        boxed_py_future(async move {
            match statement {
                ExecutableStatement::Unprepared(py_statement) => {
                    match self.inner.prepare(py_statement.inner).await {
                        Ok(prepared) => {
                            let is_serial_consistency_set =
                                prepared.get_serial_consistency().is_some();
                            Ok(PyPreparedStatement::new(
                                prepared,
                                is_serial_consistency_set,
                                py_statement.execution_profile,
                                py_statement.load_balancing_policy,
                                py_statement.retry_policy,
                            ))
                        }
                        Err(err) => Err(DriverPrepareError::rust_driver_prepare_error(err)),
                    }
                }
                ExecutableStatement::Prepared(_) => {
                    Err(DriverPrepareError::cannot_prepare_prepared_statement())
                }
            }
        })
    }

    pub(crate) fn batch(
        self,
        batch: PyBatch,
        factory: Option<Py<RowFactory>>,
    ) -> BoxedFuture<RequestResult, DriverExecuteError> {
        boxed_py_future(async move {
            let result = self
                .inner
                .batch(&batch.inner, batch.values)
                .await
                .map_err(DriverExecuteError::rust_driver_execution_error)?;

            Ok(RequestResult::from(RequestResultCore::new(
                result,
                Pager::unpaged(),
                factory,
            )))
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
                .query_unpaged(q.inner, values)
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
        let (result, paging_response) = match &*prepared {
            BoundStatement::Prepared(p, serialized_values) => self
                .inner
                .execute_unstable(p, serialized_values, true, paging_state)
                .await
                .map_err(DriverExecuteError::rust_driver_execution_error)?,
            BoundStatement::Unprepared(q, values) => self
                .inner
                .query_single_page(q.inner.clone(), values, paging_state)
                .await
                .map_err(DriverExecuteError::rust_driver_execution_error)?,
        };

        Ok(RequestResultCore::new(
            result,
            Pager::paged(paging_response, self, prepared),
            factory,
        ))
    }

    async fn spawn_on_runtime<F, Fut, R, E>(&self, f: F) -> Result<R, E>
    where
        // closure: takes Arc<ScyllaSession> and returns a future
        F: FnOnce(Arc<Session>) -> Fut + Send + 'static,
        // for spawn we need Send + 'static
        Fut: Future<Output = Result<R, E>> + Send + 'static,
        R: Send + 'static,
        // Error: Send + 'static, and also convertible from JoinError for better error handling
        E: From<tokio::task::JoinError> + Send + 'static,
    {
        let session_clone = Arc::clone(&self.inner);

        RUNTIME.spawn(async move { f(session_clone).await }).await?
    }

    pub(crate) async fn execute_single_page(
        &self,
        paging_state: PagingState,
        prepared: Arc<BoundStatement>,
    ) -> Result<(QueryResult, PagingStateResponse), DriverExecuteError> {
        self.spawn_on_runtime(async move |s| match &*prepared {
            BoundStatement::Prepared(p, serialized_values) => s
                .execute_unstable(p, serialized_values, true, paging_state)
                .await
                .map_err(DriverExecuteError::rust_driver_execution_error),
            BoundStatement::Unprepared(q, values) => s
                .query_single_page(q.inner.clone(), values, paging_state)
                .await
                .map_err(DriverExecuteError::rust_driver_execution_error),
        })
        .await
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
    Unprepared(PyStatement, PyValueList),
}

impl BoundStatement {
    pub(crate) fn new(
        statement: ExecutableStatement,
        values: PyValueList,
    ) -> Result<Self, DriverExecuteError> {
        Ok(match statement {
            ExecutableStatement::Prepared(p) => {
                let serialized_values = p
                    .serialize_values_unstable(&values)
                    .map_err(DriverExecuteError::serialization_failed)?;
                BoundStatement::Prepared(p, serialized_values)
            }
            ExecutableStatement::Unprepared(q) => BoundStatement::Unprepared(q, values),
        })
    }
}

#[derive(Clone)]
pub(crate) enum ExecutableStatement {
    Prepared(PreparedStatement),
    Unprepared(PyStatement),
}

impl<'py> FromPyObject<'_, 'py> for ExecutableStatement {
    type Error = DriverStatementConversionError;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(prepared) = obj.cast::<PyPreparedStatement>() {
            let prepared = prepared.get();
            return Ok(ExecutableStatement::Prepared(prepared.inner.clone()));
        }

        if let Ok(text) = obj.cast::<PyString>() {
            let text = text
                .to_str()
                .map_err(DriverStatementConversionError::statement_string_conversion_failed)?;
            return Ok(ExecutableStatement::Unprepared(PyStatement::new(
                text.into(),
                false,
                None,
                None,
                None,
            )));
        }

        if let Ok(statement) = obj.cast::<PyStatement>() {
            return Ok(ExecutableStatement::Unprepared(statement.get().clone()));
        }

        Err(DriverStatementConversionError::invalid_statement_type(obj))
    }
}

impl From<ExecutableStatement> for BatchStatement {
    fn from(s: ExecutableStatement) -> Self {
        match s {
            ExecutableStatement::Prepared(p) => BatchStatement::PreparedStatement(p),
            ExecutableStatement::Unprepared(q) => BatchStatement::Query(q.inner),
        }
    }
}
