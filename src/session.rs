use std::sync::Arc;

use pyo3::prelude::*;
use scylla::client::session::Session;
use scylla_cql::frame::request::query::PagingState;
use uuid::Uuid;

use crate::batch::PyBatch;
use crate::cluster::state::PyClusterState;
use crate::core::results::PendingRequestResult;
use crate::core::session::{ExecutableStatement, PreparableStatement, SessionCore};
use crate::deserialize::results::PyPagingState;
use crate::deserialize::row_factory::PyRowFactory;
use crate::errors::{
    DriverExecuteError, DriverPrepareError, DriverSchemaAgreementError, DriverUseKeyspaceError,
};
use crate::future::DriverFuture;
use crate::policies::load_balancing::PyTargetPolicy;
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

impl PySession {
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

    #[allow(clippy::too_many_arguments)]
    #[pyo3(
        signature = (statement, values=None, /, *, factory=None, paging_state=None, paged=true, target=None),
        text_signature = "(statement, values=None, /, *, factory=None, paging_state=None, paged=True, target=None)"
    )]
    fn execute(
        &self,
        py: Python<'_>,
        mut statement: ExecutableStatement,
        values: Option<PyValueList>,
        factory: Option<PyRowFactory>,
        paging_state: Option<Py<PyPagingState>>,
        paged: bool,
        target: Option<PyTargetPolicy>,
    ) -> PyResult<DriverFuture<PendingRequestResult, DriverExecuteError>> {
        // Why not accept PyValueList instead of Option<PyValueList>?
        // It would require us to use `Default::default` as default value in
        // `pyo3(signature = ...)`, and thus use `text_signature` as well
        // to keep signature usable for Python users. I think it is cleaner
        // to `unwrap_or_default()` here.
        let values = values.unwrap_or_default();
        let paging_state: Option<PagingState> =
            paging_state.map(|state| state.borrow(py).inner.clone());

        if let Some(target) = target {
            statement.set_target(target);
        }

        let request = self
            .core
            .clone()
            .execute(statement, values, factory, paging_state, paged)?;

        DriverFuture::spawn_on_tokio(py, request)
    }

    fn prepare(
        &self,
        py: Python<'_>,
        statement: PreparableStatement,
    ) -> PyResult<DriverFuture<PyPreparedStatement, DriverPrepareError>> {
        DriverFuture::spawn_on_tokio(py, self.core.clone().prepare(statement))
    }

    #[pyo3(
        signature = (batch, /, *, factory=None, target=None),
        text_signature = "(batch, /, *, factory=None, target=None)"
    )]
    fn batch(
        &self,
        py: Python<'_>,
        mut batch: PyBatch,
        factory: Option<PyRowFactory>,
        target: Option<PyTargetPolicy>,
    ) -> PyResult<DriverFuture<PendingRequestResult, DriverExecuteError>> {
        if let Some(target) = target {
            batch.set_target(target);
        }

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
