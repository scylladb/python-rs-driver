use std::sync::Arc;

use pyo3::prelude::*;
use scylla::client::session::Session;
use scylla_cql::frame::request::query::PagingState;
use uuid::Uuid;

use crate::batch::PyBatch;
use crate::cluster::state::PyClusterState;
use crate::core::session::{ExecutableStatement, SessionCore};
use crate::deserialize::results::{PyPagingState, RequestResult, RowFactory};
use crate::errors::{
    DriverExecuteError, DriverPrepareError, DriverSchemaAgreementError, DriverUseKeyspaceError,
};
use crate::future::DriverFuture;
use crate::serialize::value_list::PyValueList;
use crate::statement::PyPreparedStatement;

/// Python-facing asynchronous session.
///
/// A thin facade over [`SessionCore`]: every method here converts its Python
/// arguments, hands the work to the core, and returns a [`DriverFuture`]
/// driving the resulting future on the tokio runtime.
#[pyclass(name = "Session", frozen)]
pub(crate) struct PySession {
    pub(crate) core: SessionCore,
}

impl TryFrom<Arc<Session>> for PySession {
    type Error = PyErr;

    fn try_from(inner: Arc<Session>) -> Result<Self, Self::Error> {
        Ok(Self {
            core: SessionCore::try_from(inner)?,
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
    ) -> PyResult<DriverFuture<(), DriverUseKeyspaceError>> {
        DriverFuture::spawn_on_tokio(py, self.core.clone().use_keyspace(keyspace, case_sensitive))
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
    ) -> PyResult<DriverFuture<RequestResult, DriverExecuteError>> {
        // Why not accept PyValueList instead of Option<PyValueList>?
        // It would require us to use `Default::default` as default value in
        // `pyo3(signature = ...)`, and thus use `text_signature` as well
        // to keep signature usable for Python users. I think it is cleaner
        // to `unwrap_or_default()` here.
        let values = values.unwrap_or_default();
        let paging_state: Option<PagingState> =
            paging_state.map(|state| state.borrow(py).inner.clone());

        let request = self
            .core
            .clone()
            .execute(statement, values, factory, paging_state, paged)?;

        DriverFuture::spawn_on_tokio(py, request)
    }

    fn prepare(
        &self,
        py: Python<'_>,
        statement: ExecutableStatement,
    ) -> PyResult<DriverFuture<PyPreparedStatement, DriverPrepareError>> {
        DriverFuture::spawn_on_tokio(py, self.core.clone().prepare(statement))
    }

    #[pyo3(signature = (batch, /, *,  factory=None))]
    fn batch(
        &self,
        py: Python<'_>,
        batch: PyBatch,
        factory: Option<Py<RowFactory>>,
    ) -> PyResult<DriverFuture<RequestResult, DriverExecuteError>> {
        DriverFuture::spawn_on_tokio(py, self.core.clone().batch(batch, factory))
    }

    fn await_schema_agreement(
        &self,
        py: Python<'_>,
    ) -> PyResult<DriverFuture<Uuid, DriverSchemaAgreementError>> {
        DriverFuture::spawn_on_tokio(py, self.core.clone().await_schema_agreement())
    }

    fn check_schema_agreement(
        &self,
        py: Python<'_>,
    ) -> PyResult<DriverFuture<Option<Uuid>, DriverSchemaAgreementError>> {
        DriverFuture::spawn_on_tokio(py, self.core.clone().check_schema_agreement())
    }

    #[getter]
    fn get_cluster_state(&self, py: Python<'_>) -> PyResult<Py<PyClusterState>> {
        self.core.cluster_state(py)
    }
}

#[pymodule]
pub(crate) fn session(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PySession>()?;

    Ok(())
}
