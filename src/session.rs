use std::sync::{Arc, Mutex};

use crate::RUNTIME;
use crate::batch::PyBatch;
use crate::cluster::state::PyClusterState;
use crate::deserialize::results::{Pager, PyPagingState, RequestResult, RowFactory};
use crate::errors::{
    DriverExecuteError, DriverPrepareError, DriverSchemaAgreementError,
    DriverStatementConversionError, DriverUseKeyspaceError,
};
use crate::future::PyResponseFuture;
use crate::serialize::value_list::PyValueList;
use crate::statement::PyPreparedStatement;
use crate::statement::PyStatement;
use pyo3::prelude::*;
use pyo3::sync::MutexExt;
use pyo3::types::PyString;
use scylla::client::session::Session;
use scylla::response::query_result::QueryResult;
use scylla::statement::batch::BatchStatement;
use scylla::statement::prepared::PreparedStatement;
use scylla::statement::unprepared::Statement;
use scylla_cql::frame::request::query::{PagingState, PagingStateResponse};
use std::future::Future;

#[pyclass(name = "Session", frozen, skip_from_py_object)]
#[derive(Clone)]
pub(crate) struct PySession {
    pub(crate) _inner: Arc<Session>,
    pub(crate) cluster_state: Arc<Mutex<Py<PyClusterState>>>,
}

impl TryFrom<Arc<Session>> for PySession {
    type Error = PyErr;
    fn try_from(_inner: Arc<Session>) -> Result<Self, Self::Error> {
        let cluster_state = Python::attach(|py| {
            Py::new(py, PyClusterState::try_from(_inner.get_cluster_state())?)
        })?;
        Ok(Self {
            cluster_state: Arc::new(Mutex::new(cluster_state)),
            _inner,
        })
    }
}

#[pymethods]
impl PySession {
    #[pyo3(signature = (keyspace, case_sensitive=false))]
    fn use_keyspace(
        &self,
        py: Python<'_>,
        keyspace: String,
        case_sensitive: bool,
    ) -> PyResult<Py<PyResponseFuture>> {
        let session = self.clone();
        PyResponseFuture::spawn(py, async move {
            let result: Result<_, DriverUseKeyspaceError> = session
                .session_spawn_on_runtime(async move |s| {
                    s.use_keyspace(keyspace, case_sensitive)
                        .await
                        .map_err(DriverUseKeyspaceError::from)
                })
                .await;
            result
        })
    }

    #[pyo3(signature = (statement, values=None, /, *, factory=None, paging_state=None, paged=true))]
    fn execute(
        &self,
        py: Python<'_>,
        statement: ExecutableStatement,
        values: Option<PyValueList>,
        factory: Option<Py<RowFactory>>,
        paging_state: Option<Py<PyPagingState>>,
        paged: bool,
    ) -> PyResult<Py<PyResponseFuture>> {
        // Why not accept PyValueList instead of Option<PyValueList>?
        // It would require us to use `Default::default` as default value in
        // `pyo3(signature = ...)`, and thus use `text_signature` as well
        // to keep signature usable for Python users. I think it is cleaner
        // to `unwrap_or_default()` here.
        let values = values.unwrap_or_default();
        let session = self.clone();

        PyResponseFuture::spawn(py, async move {
            if paged {
                session
                    .execute_paged(statement, paging_state, values, factory)
                    .await
            } else {
                if paging_state.is_some() {
                    return Err(
                        DriverExecuteError::paging_state_must_be_none_for_unpaged_execution(),
                    );
                }
                session.execute_unpaged(statement, values, factory).await
            }
        })
    }

    fn prepare(
        &self,
        py: Python<'_>,
        statement: ExecutableStatement,
    ) -> PyResult<Py<PyResponseFuture>> {
        let session = self.clone();
        PyResponseFuture::spawn(py, async move {
            match statement {
                ExecutableStatement::Unprepared(s) => session.scylla_prepare(s).await,
                ExecutableStatement::Prepared(_) => {
                    Err(DriverPrepareError::cannot_prepare_prepared_statement())
                }
            }
        })
    }

    #[pyo3(signature = (batch, /, *,  factory=None))]
    fn batch(
        &self,
        py: Python<'_>,
        batch: PyBatch,
        factory: Option<Py<RowFactory>>,
    ) -> PyResult<Py<PyResponseFuture>> {
        let session = self.clone();
        PyResponseFuture::spawn(py, async move {
            let result: Result<_, DriverExecuteError> = session
                .session_spawn_on_runtime(async move |s| {
                    s.batch(&batch._inner, batch.values)
                        .await
                        .map_err(DriverExecuteError::rust_driver_execution_error)
                })
                .await;

            result.map(|r| RequestResult::new(r, Pager::unpaged(), factory))
        })
    }

    fn await_schema_agreement(&self, py: Python<'_>) -> PyResult<Py<PyResponseFuture>> {
        let session = self.clone();
        PyResponseFuture::spawn(py, async move {
            let result: Result<_, DriverSchemaAgreementError> = session
                .session_spawn_on_runtime(async move |s| {
                    s.await_schema_agreement()
                        .await
                        .map_err(DriverSchemaAgreementError::rust_driver_schema_agreement_error)
                })
                .await;
            result
        })
    }

    fn check_schema_agreement(&self, py: Python<'_>) -> PyResult<Py<PyResponseFuture>> {
        let session = self.clone();
        PyResponseFuture::spawn(py, async move {
            let result: Result<_, DriverSchemaAgreementError> = session
                .session_spawn_on_runtime(async move |s| {
                    s.check_schema_agreement()
                        .await
                        .map_err(DriverSchemaAgreementError::rust_driver_schema_agreement_error)
                })
                .await;
            result
        })
    }

    #[getter]
    fn get_cluster_state<'py>(&self, py: Python<'py>) -> PyResult<Py<PyClusterState>> {
        // PyClusterState holds `Arc<ClusterState>` preventing Rust driver from replacing
        // inner Rust `Session`'s `ClusterState` with a new object in the same memory.
        //
        // This means by comparing current Rust `Session` `ClusterState` pointer
        // and `PyClusterState`'s internal `ClusterState` pointer
        // we can determine if the `PyClusterState`'s snapshot is stale
        // and needs to be replaced with a fresh snapshot.
        let mut py_cluster_state = self.cluster_state.lock_py_attached(py).unwrap();
        let rust_current_cluster_state = self._inner.get_cluster_state();
        let python_snapshot_cluster_state = &py_cluster_state.get()._inner;
        if !Arc::ptr_eq(&rust_current_cluster_state, python_snapshot_cluster_state) {
            *py_cluster_state = Py::new(
                py,
                PyClusterState::try_from(self._inner.get_cluster_state())?,
            )?;
        }

        Ok(py_cluster_state.clone_ref(py))
    }
}

impl PySession {
    async fn execute_unpaged(
        &self,
        statement: ExecutableStatement,
        values: PyValueList,
        factory: Option<Py<RowFactory>>,
    ) -> Result<RequestResult, DriverExecuteError> {
        let result = match statement {
            ExecutableStatement::Prepared(p) => {
                let serialized_values = p
                    .serialize_values_unstable(&values)
                    .map_err(DriverExecuteError::serialization_failed)?;
                self.session_spawn_on_runtime(async move |s| {
                    s.execute_unstable(&p, &serialized_values, false, PagingState::start())
                        .await
                        .map(|(result, _paging_response)| result)
                        .map_err(DriverExecuteError::rust_driver_execution_error)
                })
                .await?
            }
            ExecutableStatement::Unprepared(q) => {
                self.session_spawn_on_runtime(async move |s| {
                    s.query_unpaged(q, values)
                        .await
                        .map_err(DriverExecuteError::rust_driver_execution_error)
                })
                .await?
            }
        };

        Ok(RequestResult::new(result, Pager::unpaged(), factory))
    }

    async fn execute_paged(
        &self,
        statement: ExecutableStatement,
        paging_state: Option<Py<PyPagingState>>,
        values: PyValueList,
        factory: Option<Py<RowFactory>>,
    ) -> Result<RequestResult, DriverExecuteError> {
        let paging_state = if let Some(state) = paging_state {
            Python::attach(|py| state.borrow(py).inner.clone())
        } else {
            PagingState::start()
        };

        let (result, paging_response) = self
            .execute_single_page(paging_state, statement.clone(), values.clone())
            .await?;

        Ok(RequestResult::new(
            result,
            Pager::paged(paging_response, self.clone(), statement, values),
            factory,
        ))
    }

    async fn session_spawn_on_runtime<F, Fut, R, E>(&self, f: F) -> Result<R, E>
    where
        // closure: takes Arc<ScyllaSession> and returns a future
        F: FnOnce(Arc<Session>) -> Fut + Send + 'static,
        // for spawn we need Send + 'static
        Fut: Future<Output = Result<R, E>> + Send + 'static,
        R: Send + 'static,
        // Error: Send + 'static, and also convertible from JoinError for better error handling
        E: From<tokio::task::JoinError> + Send + 'static,
    {
        let session_clone = Arc::clone(&self._inner);

        RUNTIME.spawn(async move { f(session_clone).await }).await?
    }

    async fn scylla_prepare(
        &self,
        statement: impl Into<Statement>,
    ) -> Result<PyPreparedStatement, DriverPrepareError> {
        match self._inner.prepare(statement).await {
            Ok(prepared) => Ok(PyPreparedStatement::new(prepared, false)),
            Err(err) => Err(DriverPrepareError::rust_driver_prepare_error(err)),
        }
    }

    pub(crate) async fn execute_single_page(
        &self,
        paging_state: PagingState,
        query_request: ExecutableStatement,
        values: PyValueList,
    ) -> Result<(QueryResult, PagingStateResponse), DriverExecuteError> {
        match query_request {
            ExecutableStatement::Prepared(p) => {
                let serialized_values = p
                    .serialize_values_unstable(&values)
                    .map_err(DriverExecuteError::serialization_failed)?;
                self.session_spawn_on_runtime(async move |s| {
                    s.execute_unstable(&p, &serialized_values, true, paging_state)
                        .await
                        .map_err(DriverExecuteError::rust_driver_execution_error)
                })
                .await
            }
            ExecutableStatement::Unprepared(q) => {
                self.session_spawn_on_runtime(async move |s| {
                    s.query_single_page(q, values, paging_state)
                        .await
                        .map_err(DriverExecuteError::rust_driver_execution_error)
                })
                .await
            }
        }
    }
}

#[derive(Clone)]
pub(crate) enum ExecutableStatement {
    Prepared(PreparedStatement),
    Unprepared(Statement),
}

impl<'py> FromPyObject<'_, 'py> for ExecutableStatement {
    type Error = DriverStatementConversionError;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(prepared) = obj.cast::<PyPreparedStatement>() {
            return Ok(ExecutableStatement::Prepared(prepared.get()._inner.clone()));
        }

        if let Ok(text) = obj.cast::<PyString>() {
            let text = text
                .to_str()
                .map_err(DriverStatementConversionError::statement_string_conversion_failed)?;
            return Ok(ExecutableStatement::Unprepared(text.into()));
        }

        if let Ok(statement) = obj.cast::<PyStatement>() {
            return Ok(ExecutableStatement::Unprepared(
                statement.get()._inner.clone(),
            ));
        }

        let got = obj
            .get_type()
            .name()
            .map(|name| name.to_string())
            .unwrap_or_else(|_| "<unknown type>".to_string());

        Err(DriverStatementConversionError::invalid_statement_type(got))
    }
}

impl From<ExecutableStatement> for BatchStatement {
    fn from(s: ExecutableStatement) -> Self {
        match s {
            ExecutableStatement::Prepared(p) => BatchStatement::PreparedStatement(p),
            ExecutableStatement::Unprepared(u) => BatchStatement::Query(u),
        }
    }
}

#[pymodule]
pub(crate) fn session(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PySession>()?;

    Ok(())
}
