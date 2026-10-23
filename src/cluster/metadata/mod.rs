use std::sync::OnceLock;

use pyo3::{
    IntoPyObjectExt,
    prelude::*,
    sync::OnceLockExt,
    types::{IntoPyDict, PyDict, PyMappingProxy, PyString},
};
use scylla::cluster::metadata::{Column, ColumnKind, Keyspace, MaterializedView, Strategy, Table};

use crate::{
    cache::Cache,
    cluster::metadata::{column_type::*, query_metadata::PyColumnSpec},
};

pub(crate) mod column_type;
pub(crate) mod query_metadata;

#[pyclass(
    module = "scylla.cluster",
    name = "StrategyKind",
    eq,
    eq_int,
    frozen,
    skip_from_py_object
)]
#[derive(Clone, Copy, PartialEq)]
pub(crate) enum PyStrategyKind {
    Simple,
    NetworkTopology,
    Local,
    Other,
}

#[pyclass(
    module = "scylla.cluster",
    name = "Strategy",
    frozen,
    skip_from_py_object
)]
#[derive(Clone)]
pub(crate) struct PyStrategy {
    pub(crate) inner: Strategy,
}

impl From<Strategy> for PyStrategy {
    fn from(inner: Strategy) -> Self {
        PyStrategy { inner }
    }
}

impl From<&Strategy> for PyStrategyKind {
    fn from(strategy: &Strategy) -> Self {
        #[deny(clippy::wildcard_enum_match_arm)]
        match strategy {
            Strategy::SimpleStrategy { .. } => PyStrategyKind::Simple,
            Strategy::NetworkTopologyStrategy { .. } => PyStrategyKind::NetworkTopology,
            Strategy::LocalStrategy => PyStrategyKind::Local,
            Strategy::Other { .. } => PyStrategyKind::Other,
            _ => unreachable!("clippy testifies that the match is exhaustive"),
        }
    }
}

#[pymethods]
impl PyStrategy {
    #[getter]
    fn kind(&self) -> PyStrategyKind {
        PyStrategyKind::from(&self.inner)
    }

    #[getter]
    fn replication_factor<'py>(&self, py: Python<'py>) -> PyResult<Py<PyAny>> {
        match &self.inner {
            Strategy::SimpleStrategy {
                replication_factor, ..
            } => replication_factor.into_py_any(py),
            Strategy::LocalStrategy => 1.into_py_any(py),
            Strategy::NetworkTopologyStrategy {
                datacenter_repfactors,
            } => PyMappingProxy::new(
                py,
                datacenter_repfactors.iter().into_py_dict(py)?.as_mapping(),
            )
            .into_py_any(py),
            _ => Ok(py.None()),
        }
    }

    #[getter]
    fn other_name(&self) -> Option<&str> {
        match &self.inner {
            Strategy::Other { name, .. } => Some(name),
            _ => None,
        }
    }

    #[getter]
    fn other_data<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyMappingProxy>>> {
        match &self.inner {
            Strategy::Other { data, .. } => Ok(Some(PyMappingProxy::new(
                py,
                data.iter().into_py_dict(py)?.as_mapping(),
            ))),
            _ => Ok(None),
        }
    }

    fn __repr__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyString>> {
        PyString::from_fmt(py, format_args!("{:?}", self.inner))
    }
}

#[pyclass(
    module = "scylla.cluster",
    name = "ColumnKind",
    frozen,
    eq,
    eq_int,
    skip_from_py_object
)]
#[derive(Clone, PartialEq)]
pub(crate) enum PyColumnKind {
    Regular,
    Static,
    Clustering,
    PartitionKey,
}

impl From<&ColumnKind> for PyColumnKind {
    fn from(kind: &ColumnKind) -> Self {
        #[deny(clippy::wildcard_enum_match_arm)]
        match kind {
            ColumnKind::Regular => PyColumnKind::Regular,
            ColumnKind::Static => PyColumnKind::Static,
            ColumnKind::Clustering => PyColumnKind::Clustering,
            ColumnKind::PartitionKey => PyColumnKind::PartitionKey,
            _ => unreachable!("clippy testifies that the match is exhaustive"),
        }
    }
}

#[pyclass(
    module = "scylla.cluster",
    name = "Column",
    frozen,
    skip_from_py_object
)]
#[derive(Clone)]
pub(crate) struct PyColumn {
    inner: Column,
    #[pyo3(get)]
    typ: Py<PyCqlColumnType>,
    #[pyo3(get)]
    kind: Py<PyColumnKind>,
}

impl TryFrom<&Column> for PyColumn {
    type Error = PyErr;
    fn try_from(value: &Column) -> Result<Self, Self::Error> {
        Python::attach(|py| {
            Ok(Self {
                typ: extract_column_type(py, &value.typ)?,
                kind: Py::new(py, PyColumnKind::from(&value.kind))?,
                inner: value.clone(),
            })
        })
    }
}

#[pymethods]
impl PyColumn {
    fn __repr__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyString>> {
        PyString::from_fmt(
            py,
            format_args!(
                "Column(typ='{:?}', kind={:?})",
                self.inner.typ, self.inner.kind
            ),
        )
    }
}

#[pyclass(module = "scylla.cluster", name = "Table", frozen, skip_from_py_object)]
pub(crate) struct PyTable {
    inner: Table,
    partitioner: OnceLock<Option<Py<PyString>>>,
    /// Invariant: Holds all known `PyColumn`s of the table.
    columns: Py<PyDict>,
    /// Partition key columns of this table.
    ///
    /// Invariant: Always has partition key `PyColumn`s.
    partition_key: Py<PyDict>,
    /// Clustering key columns of this table.
    ///
    /// Invariant: Always has clustering key `PyColumn`s.
    clustering_key: Py<PyDict>,
}

impl TryFrom<Table> for PyTable {
    type Error = PyErr;

    fn try_from(inner: Table) -> Result<Self, Self::Error> {
        Python::attach(|py| {
            let py_cols = PyDict::new(py);
            let py_partition_key = PyDict::new(py);
            let py_clustering_key = PyDict::new(py);

            // Initialize columns dictionary
            for (name, column) in inner.columns.iter() {
                py_cols.set_item(name, PyColumn::try_from(column)?)?;
            }

            // Reuse the same columns for partition and clustering keys
            for name in &inner.partition_key {
                py_partition_key.set_item(name, py_cols.get_item(name)?)?;
            }

            for name in &inner.clustering_key {
                py_clustering_key.set_item(name, py_cols.get_item(name)?)?;
            }

            Ok(Self {
                inner,
                partitioner: OnceLock::new(),
                columns: py_cols.unbind(),
                partition_key: py_partition_key.unbind(),
                clustering_key: py_clustering_key.unbind(),
            })
        })
    }
}

#[pymethods]
impl PyTable {
    #[getter]
    fn columns<'py>(&self, py: Python<'py>) -> Bound<'py, PyMappingProxy> {
        PyMappingProxy::new(py, self.columns.bind(py).as_mapping())
    }

    #[getter]
    fn partition_key<'py>(&self, py: Python<'py>) -> Bound<'py, PyMappingProxy> {
        PyMappingProxy::new(py, self.partition_key.bind(py).as_mapping())
    }

    #[getter]
    fn clustering_key<'py>(&self, py: Python<'py>) -> Bound<'py, PyMappingProxy> {
        PyMappingProxy::new(py, self.clustering_key.bind(py).as_mapping())
    }

    #[getter]
    fn partitioner<'py>(&self, py: Python<'py>) -> &Option<Py<PyString>> {
        self.partitioner.get_or_init_py_attached(py, || {
            self.inner
                .partitioner
                .as_ref()
                .map(|p| PyString::new(py, p).unbind())
        })
    }

    fn __repr__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyString>> {
        PyString::from_fmt(
            py,
            format_args!(
                "Table(columns={}, partition_key={:?}, clustering_key={:?})",
                self.inner.columns.len(),
                self.inner.partition_key,
                self.inner.clustering_key,
            ),
        )
    }
}

#[pyclass(
    module = "scylla.cluster",
    name = "MaterializedView",
    frozen,
    skip_from_py_object
)]
pub(crate) struct PyMaterializedView {
    inner: MaterializedView,
    base_table_name: OnceLock<Py<PyString>>,
    partitioner: OnceLock<Option<Py<PyString>>>,
    /// Invariant: Holds all known `PyColumn`s of the table.
    columns: Py<PyDict>,
    /// Partition key columns of this table.
    ///
    /// Invariant: Always has partition key `PyColumn`s.
    partition_key: Py<PyDict>,
    /// Clustering key columns of this table.
    ///
    /// Invariant: Always has clustering key `PyColumn`s.
    clustering_key: Py<PyDict>,
}

impl TryFrom<MaterializedView> for PyMaterializedView {
    type Error = PyErr;

    fn try_from(inner: MaterializedView) -> Result<Self, Self::Error> {
        Python::attach(|py| {
            let py_cols = PyDict::new(py);
            let py_partition_key = PyDict::new(py);
            let py_clustering_key = PyDict::new(py);

            // Initialize columns dictionary
            for (name, column) in inner.view_metadata.columns.iter() {
                py_cols.set_item(name, PyColumn::try_from(column)?)?;
            }

            // Reuse the same columns for partition and clustering keys
            for name in &inner.view_metadata.partition_key {
                py_partition_key.set_item(name, py_cols.get_item(name)?)?;
            }

            for name in &inner.view_metadata.clustering_key {
                py_clustering_key.set_item(name, py_cols.get_item(name)?)?;
            }

            Ok(Self {
                inner,
                partitioner: OnceLock::new(),
                base_table_name: OnceLock::new(),
                columns: py_cols.unbind(),
                partition_key: py_partition_key.unbind(),
                clustering_key: py_clustering_key.unbind(),
            })
        })
    }
}

#[pymethods]
impl PyMaterializedView {
    #[getter]
    fn columns<'py>(&self, py: Python<'py>) -> Bound<'py, PyMappingProxy> {
        PyMappingProxy::new(py, self.columns.bind(py).as_mapping())
    }

    #[getter]
    fn partition_key<'py>(&self, py: Python<'py>) -> Bound<'py, PyMappingProxy> {
        PyMappingProxy::new(py, self.partition_key.bind(py).as_mapping())
    }

    #[getter]
    fn clustering_key<'py>(&self, py: Python<'py>) -> Bound<'py, PyMappingProxy> {
        PyMappingProxy::new(py, self.clustering_key.bind(py).as_mapping())
    }

    #[getter]
    fn partitioner<'py>(&self, py: Python<'py>) -> &Option<Py<PyString>> {
        self.partitioner.get_or_init_py_attached(py, || {
            self.inner
                .view_metadata
                .partitioner
                .as_ref()
                .map(|p| PyString::new(py, p).unbind())
        })
    }

    #[getter]
    fn base_table_name<'py>(&self, py: Python<'py>) -> &Py<PyString> {
        self.base_table_name.get_or_init_py_attached(py, || {
            PyString::new(py, self.inner.base_table_name.as_str()).unbind()
        })
    }

    fn __repr__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyString>> {
        PyString::from_fmt(
            py,
            format_args!(
                "MaterializedView(base_table='{}', columns={})",
                self.inner.base_table_name,
                self.inner.view_metadata.columns.len(),
            ),
        )
    }
}

#[pyclass(module = "scylla.cluster", name = "Keyspace", frozen)]
pub(crate) struct PyKeyspace {
    pub(crate) inner: Keyspace,
    pub(crate) strategy: OnceLock<Py<PyStrategy>>,
    /// Tables in this keyspace.
    pub(crate) tables: Cache<String, PyTable>,
    /// Materialized views in this keyspace.
    pub(crate) views: Cache<String, PyMaterializedView>,
}

impl From<Keyspace> for PyKeyspace {
    fn from(inner: Keyspace) -> Self {
        Self {
            inner,
            strategy: OnceLock::new(),
            tables: Cache::new(),
            views: Cache::new(),
        }
    }
}

#[pymethods]
impl PyKeyspace {
    #[getter]
    fn strategy<'py>(&self, py: Python<'py>) -> PyResult<Py<PyStrategy>> {
        match self.strategy.get() {
            Some(s) => Ok(s.clone_ref(py)),
            None => {
                let py_ref = Py::new(py, PyStrategy::from(self.inner.strategy.clone()))?;
                let strategy = self.strategy.get_or_init_py_attached(py, || py_ref);
                Ok(strategy.clone_ref(py))
            }
        }
    }

    #[getter]
    fn tables<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyMappingProxy>> {
        self.tables.get_or_init_python_mapping(py, || {
            self.inner
                .tables
                .iter()
                .map(|(name, table)| {
                    let py_table = PyTable::try_from(table.clone());
                    (name.clone(), py_table.and_then(|t| Py::new(py, t)))
                })
                .collect()
        })
    }

    #[getter]
    fn views<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyMappingProxy>> {
        self.views.get_or_init_python_mapping(py, || {
            self.inner
                .views
                .iter()
                .map(|(name, view)| {
                    let py_view = PyMaterializedView::try_from(view.clone());
                    (name.clone(), py_view.and_then(|t| Py::new(py, t)))
                })
                .collect()
        })
    }

    fn __repr__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyString>> {
        PyString::from_fmt(
            py,
            format_args!(
                "Keyspace(strategy={:?}, tables={}, views={}, udts={})",
                self.inner.strategy,
                self.inner.tables.len(),
                self.inner.views.len(),
                self.inner.user_defined_types.len(),
            ),
        )
    }
}

#[pymodule]
pub(crate) fn metadata(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    // Base column type classes
    module.add_class::<PyCqlColumnType>()?;
    module.add_class::<PyCqlNativeType>()?;
    module.add_class::<PyCqlCollectionType>()?;

    // Native types (20 total)
    module.add_class::<PyCqlAsciiType>()?;
    module.add_class::<PyCqlBigIntType>()?;
    module.add_class::<PyCqlBlobType>()?;
    module.add_class::<PyCqlBooleanType>()?;
    module.add_class::<PyCqlCounterType>()?;
    module.add_class::<PyCqlDateType>()?;
    module.add_class::<PyCqlDecimalType>()?;
    module.add_class::<PyCqlDoubleType>()?;
    module.add_class::<PyCqlDurationType>()?;
    module.add_class::<PyCqlFloatType>()?;
    module.add_class::<PyCqlInetType>()?;
    module.add_class::<PyCqlIntType>()?;
    module.add_class::<PyCqlSmallIntType>()?;
    module.add_class::<PyCqlTextType>()?;
    module.add_class::<PyCqlTimeType>()?;
    module.add_class::<PyCqlTimestampType>()?;
    module.add_class::<PyCqlTimeuuidType>()?;
    module.add_class::<PyCqlTinyIntType>()?;
    module.add_class::<PyCqlUuidType>()?;
    module.add_class::<PyCqlVarintType>()?;

    // Collection types
    module.add_class::<PyCqlListType>()?;
    module.add_class::<PyCqlMapType>()?;
    module.add_class::<PyCqlSetType>()?;

    // Complex types
    module.add_class::<PyCqlTupleType>()?;
    module.add_class::<PyCqlVectorType>()?;
    module.add_class::<PyCqlUserDefinedType>()?;

    // Metadata classes
    module.add_class::<PyColumn>()?;
    module.add_class::<PyColumnKind>()?;
    module.add_class::<PyTable>()?;
    module.add_class::<PyMaterializedView>()?;
    module.add_class::<PyKeyspace>()?;
    module.add_class::<PyStrategy>()?;
    module.add_class::<PyStrategyKind>()?;

    // Query / prepared statement metadata classes
    module.add_class::<PyColumnSpec>()?;

    Ok(())
}
