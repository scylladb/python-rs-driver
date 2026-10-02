use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyList, PyString, PyTuple};
use scylla::frame::response::result::{ColumnSpec, PartitionKeyIndex};

use crate::cluster::metadata::column_type::{PyCqlColumnType, extract_column_type};
use crate::errors::{QueryMetadataError, with_cause};

/// Specification of a column in a result set, used for both prepared statement metadata and query result metadata.
#[pyclass(name = "ColumnSpec", skip_from_py_object, frozen)]
pub(crate) struct PyColumnSpec {
    inner: ColumnSpec<'static>,

    // Cached Python-side representations used by the getters.
    py_name: PyOnceLock<Py<PyString>>,
    py_table_name: PyOnceLock<Py<PyString>>,
    py_keyspace_name: PyOnceLock<Py<PyString>>,
    py_cql_type: PyOnceLock<Py<PyCqlColumnType>>,
}

impl From<&ColumnSpec<'_>> for PyColumnSpec {
    fn from(spec: &ColumnSpec<'_>) -> Self {
        Self {
            inner: spec.clone().into_owned(),

            py_name: PyOnceLock::new(),
            py_table_name: PyOnceLock::new(),
            py_keyspace_name: PyOnceLock::new(),
            py_cql_type: PyOnceLock::new(),
        }
    }
}

#[pymethods]
impl PyColumnSpec {
    /// The name of the column.
    #[getter]
    fn name(&self, py: Python<'_>) -> Py<PyString> {
        let name = self
            .py_name
            .get_or_init(py, || PyString::new(py, self.inner.name()).unbind());
        name.clone_ref(py)
    }

    /// The name of the table containing the column.
    #[getter]
    fn table_name(&self, py: Python<'_>) -> Py<PyString> {
        let table_name = self.py_table_name.get_or_init(py, || {
            PyString::new(py, self.inner.table_spec().table_name()).unbind()
        });
        table_name.clone_ref(py)
    }

    /// The name of the keyspace containing the column.
    #[getter]
    fn keyspace_name(&self, py: Python<'_>) -> Py<PyString> {
        let keyspace_name = self.py_keyspace_name.get_or_init(py, || {
            PyString::new(py, self.inner.table_spec().ks_name()).unbind()
        });
        keyspace_name.clone_ref(py)
    }

    /// The CQL type of the column.
    #[getter]
    fn cql_type(&self, py: Python<'_>) -> PyResult<Py<PyCqlColumnType>> {
        let cql_type = self.py_cql_type.get_or_try_init(py, || {
            extract_column_type(py, self.inner.typ())
                .map_err(DriverQueryMetadataError::column_type_extraction_failed)
        })?;
        Ok(cql_type.clone_ref(py))
    }

    fn __repr__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyString>> {
        let cql_type = self.cql_type(py)?;
        let cql_type_name = cql_type.bind(py).get_type().name()?;

        PyString::from_fmt(
            py,
            format_args!(
                "ColumnSpec(keyspace_name='{}', table_name='{}', name='{}', cql_type={})",
                self.inner.table_spec().ks_name(),
                self.inner.table_spec().table_name(),
                self.inner.name(),
                cql_type_name,
            ),
        )
    }
}

/// Builds a Python tuple of lazily-initialized `ColumnSpec`s from column specifications.
pub(crate) fn column_spec_tuple(py: Python<'_>, specs: &[ColumnSpec<'_>]) -> PyResult<Py<PyTuple>> {
    PyTuple::new(py, specs.iter().map(PyColumnSpec::from)).map(Bound::unbind)
}

/// Builds a Python list of the CQL types of `specs`.
pub(crate) fn column_type_list(py: Python<'_>, specs: &[ColumnSpec<'_>]) -> PyResult<Py<PyList>> {
    let types = PyList::empty(py);
    for spec in specs {
        let cql_type = extract_column_type(py, spec.typ())
            .map_err(DriverQueryMetadataError::column_type_extraction_failed)?;
        types.append(cql_type)?;
    }

    Ok(types.unbind())
}

/// Builds a Python tuple of bind variable indexes of the partition key columns,
/// in partition key order.
///
/// `get_variable_pk_indexes` is sorted by `index`. Undo it here: `sequence` is assigned as the loop counter
/// over the list, so it is always dense over `0..len`, and placing each `index` at its `sequence` reconstructs
/// the order the server sent.
pub(crate) fn partition_key_index_tuple(
    py: Python<'_>,
    pk_indexes: &[PartitionKeyIndex],
) -> PyResult<Py<PyTuple>> {
    let mut ordered = vec![0u16; pk_indexes.len()];
    for pk_index in pk_indexes {
        ordered[pk_index.sequence as usize] = pk_index.index;
    }

    PyTuple::new(py, ordered).map(Bound::unbind)
}

/// Errors that can occur during query or prepared statement metadata extraction.
#[derive(Debug, thiserror::Error)]
pub(crate) enum DriverQueryMetadataError {
    /// An error occurred in Python code or during PyO3 conversion while extracting a column type.
    #[error("Failed to extract column type from metadata")]
    ColumnTypeExtractionFailed { source: Box<PyErr> },
}

impl DriverQueryMetadataError {
    /* Constructors */
    pub(crate) fn column_type_extraction_failed(source: PyErr) -> Self {
        Self::ColumnTypeExtractionFailed {
            source: Box::new(source),
        }
    }
}

impl From<DriverQueryMetadataError> for PyErr {
    fn from(e: DriverQueryMetadataError) -> PyErr {
        let err = QueryMetadataError::new_err(e.to_string());
        match e {
            DriverQueryMetadataError::ColumnTypeExtractionFailed { source } => {
                with_cause(err, *source)
            }
        }
    }
}
