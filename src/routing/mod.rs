use pyo3::{
    prelude::*,
    types::{PyList, PyString},
};
use scylla::{
    frame::response::result::TableSpec,
    routing::{Shard, Token},
};

use crate::cluster::{metadata::PyStrategy, node::PyNode, state::PyClusterState};

#[pyclass(module = "scylla.routing", name = "Token", frozen, from_py_object)]
#[derive(Clone)]
pub(crate) struct PyToken {
    pub(crate) inner: Token,
}

impl From<Token> for PyToken {
    fn from(token: Token) -> Self {
        Self { inner: token }
    }
}

#[pymethods]
impl PyToken {
    #[new]
    fn new(value: i64) -> Self {
        Self {
            inner: Token::new(value),
        }
    }

    #[getter]
    fn value(&self) -> i64 {
        self.inner.value()
    }

    fn __repr__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyString>> {
        PyString::from_fmt(py, format_args!("Token({})", self.inner.value()))
    }

    fn __eq__(&self, other: &PyToken) -> bool {
        self.inner == other.inner
    }

    fn __hash__(&self) -> i64 {
        self.inner.value()
    }
}

#[pyclass(module = "scylla.routing", name = "ReplicaLocator", frozen)]
pub(crate) struct PyReplicaLocator {
    inner: Py<PyClusterState>,
}

impl From<PyRef<'_, PyClusterState>> for PyReplicaLocator {
    fn from(inner: PyRef<'_, PyClusterState>) -> Self {
        PyReplicaLocator {
            inner: inner.into(),
        }
    }
}

#[pymethods]
impl PyReplicaLocator {
    #[pyo3(
        signature = (token, strategy, keyspace, table, datacenter=None,)
    )]
    fn primary_replica_for_token<'py>(
        &self,
        token: &PyToken,
        strategy: &PyStrategy,
        keyspace: &str,
        table: &str,
        datacenter: Option<&str>,
        py: Python<'py>,
    ) -> PyResult<Option<(Bound<'py, PyNode>, Shard)>> {
        let table_spec = TableSpec::borrowed(keyspace, table);
        let replica_set = self
            .inner
            .bind(py)
            .get()
            .inner
            .replica_locator()
            .replicas_for_token(token.inner, &strategy.inner, datacenter, &table_spec);
        let Some((node, shard)) = replica_set.into_iter().next() else {
            return Ok(None);
        };
        let py_cs = self.inner.bind(py).get();
        let py_node = py_cs.known_nodes.bind(py).get_item(node.host_id)?;
        let py_node = py_node.expect("node can't be known by Rust Driver and simultaneously None");
        Ok(Some((
            py_node
                .cast_into_exact::<PyNode>()
                .expect("Invariant for known_nodes prevents cast error"),
            shard,
        )))
    }

    #[pyo3(
        signature = (token, strategy, keyspace, table, datacenter=None,)
    )]
    fn all_replicas_for_token<'py>(
        &self,
        token: &PyToken,
        strategy: &PyStrategy,
        keyspace: &str,
        table: &str,
        datacenter: Option<&str>,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyList>> {
        let table_spec = TableSpec::borrowed(keyspace, table);
        let replica_set = self
            .inner
            .bind(py)
            .get()
            .inner
            .replica_locator()
            .replicas_for_token(token.inner, &strategy.inner, datacenter, &table_spec);

        let list = PyList::empty(py);
        let py_cs = self.inner.bind(py).get();

        for (node, shard) in replica_set {
            let py_node = py_cs.known_nodes.bind(py).get_item(node.host_id)?;
            let py_node =
                py_node.expect("node can't be known by Rust Driver and simultaneously None");
            list.append((py_node, shard))?;
        }
        Ok(list)
    }

    fn unique_token_owning_nodes_in_cluster<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyList>> {
        let py_cs = self.inner.bind(py).get();
        let nodes_iter = py_cs
            .inner
            .replica_locator()
            .unique_nodes_in_global_ring()
            .iter();
        let list = PyList::empty(py);
        for node in nodes_iter {
            let py_node = py_cs.known_nodes.bind(py).get_item(node.host_id)?;
            let py_node =
                py_node.expect("node can't be known by Rust Driver and simultaneously None");
            list.append(py_node)?;
        }
        Ok(list)
    }

    fn unique_token_owning_nodes_in_datacenter<'py>(
        &self,
        datacenter: &str,
        py: Python<'py>,
    ) -> PyResult<Option<Bound<'py, PyList>>> {
        let py_cs = self.inner.bind(py).get();
        let Some(unique_nodes_iter) = py_cs
            .inner
            .replica_locator()
            .unique_nodes_in_datacenter_ring(datacenter)
            .map(IntoIterator::into_iter)
        else {
            return Ok(None);
        };
        let list = PyList::empty(py);
        for node in unique_nodes_iter {
            let py_node = py_cs.known_nodes.bind(py).get_item(node.host_id)?;
            let py_node =
                py_node.expect("node can't be known by Rust Driver and simultaneously None");
            list.append(py_node)?;
        }
        Ok(Some(list))
    }

    #[getter]
    fn datacenter_names<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        PyList::new(
            py,
            self.inner
                .bind(py)
                .get()
                .inner
                .replica_locator()
                .datacenter_names(),
        )
    }

    fn __repr__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyString>> {
        PyString::from_fmt(
            py,
            format_args!(
                "ReplicaLocator(ring_len={}, datacenters={:?})",
                self.inner
                    .bind(py)
                    .get()
                    .inner
                    .replica_locator()
                    .ring()
                    .len(),
                self.inner
                    .bind(py)
                    .get()
                    .inner
                    .replica_locator()
                    .datacenter_names(),
            ),
        )
    }
}

#[pymodule]
pub(crate) fn routing(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyToken>()?;
    module.add_class::<PyReplicaLocator>()?;
    Ok(())
}
