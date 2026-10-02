use std::sync::Arc;

use pyo3::{
    prelude::*,
    types::{PyDict, PyList, PyMappingProxy, PyString},
};
use scylla::cluster::ClusterState;
use scylla::errors::ClusterStateTokenError as RustClusterStateTokenError;

use crate::{
    cache::Cache,
    cluster::metadata::PyKeyspace,
    cluster::node::PyNode,
    errors::ClusterStateTokenError,
    routing::{PyReplicaLocator, PyToken},
    serialize::error::serialization_error_to_pyerr,
    serialize::value_list::PyValueList,
};

#[pyclass(
    module = "scylla.cluster",
    name = "ClusterState",
    frozen,
    skip_from_py_object
)]
pub(crate) struct PyClusterState {
    pub(crate) inner: Arc<ClusterState>,
    /// Invariant: Always contains all known nodes by the Rust Driver
    pub(crate) known_nodes: Py<PyDict>,
    pub(crate) keyspaces: Cache<String, PyKeyspace>,
}

impl TryFrom<Arc<ClusterState>> for PyClusterState {
    type Error = PyErr;

    fn try_from(inner: Arc<ClusterState>) -> Result<Self, Self::Error> {
        let known_nodes = Python::attach(|py| {
            let dict = PyDict::new(py);
            for node in inner.get_nodes_info().iter() {
                dict.set_item(node.host_id, PyNode::from(Arc::clone(node)))?
            }
            Ok::<Py<PyDict>, PyErr>(dict.unbind())
        })?;
        Ok(Self {
            inner,
            known_nodes,
            keyspaces: Cache::new(),
        })
    }
}

#[pymethods]
impl PyClusterState {
    fn get_keyspace<'py>(
        &self,
        py: Python<'py>,
        keyspace: Py<PyString>,
    ) -> PyResult<Option<Py<PyKeyspace>>> {
        self.keyspaces.get_or_init(py, keyspace.to_str(py)?, |key| {
            self.inner
                .get_keyspace(key)
                .map(|ks| Py::new(py, PyKeyspace::from(ks.clone())))
                .transpose()
        })
    }

    #[getter]
    fn get_keyspaces<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyMappingProxy>> {
        self.keyspaces.get_or_init_python_mapping(py, || {
            self.inner
                .keyspaces_iter()
                .map(|(name, keyspace)| {
                    (
                        name.to_string(),
                        Py::new(py, PyKeyspace::from(keyspace.clone())),
                    )
                })
                .collect()
        })
    }

    #[getter]
    fn get_nodes_info<'py>(&self, py: Python<'py>) -> Bound<'py, PyMappingProxy> {
        PyMappingProxy::new(py, self.known_nodes.bind(py).as_mapping())
    }

    fn compute_token(
        &self,
        keyspace: &str,
        table: &str,
        partition_key: PyValueList,
    ) -> Result<PyToken, DriverClusterStateTokenError> {
        let token = self.inner.compute_token(keyspace, table, &partition_key)?;
        Ok(PyToken::from(token))
    }

    fn get_token_endpoints<'py>(
        &self,
        keyspace: &str,
        table: &str,
        token: &PyToken,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyList>> {
        let token_endpoints_sequence = self
            .inner
            .get_token_endpoints(keyspace, table, token.inner)
            .into_iter()
            .map(|(node, shard)| -> PyResult<(Bound<'_, PyAny>, u32)> {
                let py_node = self.known_nodes.bind(py).get_item(node.host_id)?;
                let py_node =
                    py_node.expect("node can't be known by Rust Driver and simultaneously None");
                Ok((py_node, shard))
            });
        let list = PyList::empty(py);
        for token_endpoint in token_endpoints_sequence {
            let (py_node, shard) = token_endpoint?;
            list.append((py_node, shard))?;
        }
        Ok(list)
    }

    fn get_endpoints<'py>(
        &self,
        keyspace: &str,
        table: &str,
        partition_key: PyValueList,
        py: Python<'py>,
    ) -> Result<Bound<'py, PyList>, DriverClusterStateTokenError> {
        let endpoints_sequence = self
            .inner
            .get_endpoints(keyspace, table, &partition_key)?
            .into_iter()
            .map(
                |(node, shard)| -> Result<(Bound<'_, PyAny>, u32), DriverClusterStateTokenError> {
                    let py_node = self
                        .known_nodes
                        .bind(py)
                        .get_item(node.host_id)
                        .map_err(DriverClusterStateTokenError::python_conversion_failed)?;
                    let py_node = py_node
                        .expect("node can't be known by Rust Driver and simultaneously None");
                    Ok((py_node, shard))
                },
            );
        let list = PyList::empty(py);
        for endpoint in endpoints_sequence {
            let (py_node, shard) = endpoint?;
            list.append((py_node, shard))
                .map_err(DriverClusterStateTokenError::python_conversion_failed)?;
        }
        Ok(list)
    }

    #[getter]
    fn get_replica_locator<'py>(slf: PyRef<'py, Self>) -> PyResult<PyReplicaLocator> {
        Ok(PyReplicaLocator::from(slf))
    }

    fn __repr__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyString>> {
        PyString::from_fmt(
            py,
            format_args!(
                "ClusterState(nodes={:?}, keyspaces={:?})",
                self.inner.get_nodes_info().iter().collect::<Vec<_>>(),
                self.inner.keyspaces_iter().collect::<Vec<_>>()
            ),
        )
    }
}

/// Errors that can occur during cluster state operations.
#[derive(Debug, thiserror::Error)]
pub(crate) enum DriverClusterStateTokenError {
    /// The Rust driver failed to compute the token (calculation, serialization or unknown table).
    #[error(transparent)]
    RustDriverTokenError(#[from] RustClusterStateTokenError),
    /// An FFI-related error occurred (e.g., Python conversion, node creation).
    #[error(transparent)]
    PythonConversionFailed(PyErr),
}

impl DriverClusterStateTokenError {
    pub(crate) fn python_conversion_failed(err: PyErr) -> Self {
        Self::PythonConversionFailed(err)
    }
}

impl From<DriverClusterStateTokenError> for PyErr {
    fn from(e: DriverClusterStateTokenError) -> PyErr {
        let message = e.to_string();
        match e {
            DriverClusterStateTokenError::PythonConversionFailed(err) => err,
            DriverClusterStateTokenError::RustDriverTokenError(
                RustClusterStateTokenError::Serialization(err),
            ) => serialization_error_to_pyerr(&err, message),
            _ => ClusterStateTokenError::new_err(message),
        }
    }
}
