use crate::cluster::metadata::query_metadata::column_spec_tuple;
use crate::core::results::PageCore;
use crate::future::{DriverFuture, boxed_py_future};
use crate::results::iterators::{AsyncPagesIterator, AsyncRowsIterator};
use crate::results::page::Page;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyList, PyTuple};
use pyo3::{Py, PyAny, PyErr, PyResult, Python, pyclass, pymethods};

/// Result of a whole query, across all of its pages.
///
/// Returned only by `execute()` and `batch()`, so it always starts at the
/// query's first page. Every way of consuming it starts from that page.
///
/// Python-facing facade over the [`PageCore`] of the first page: each method
/// clones the core, hands the work over, and awaits it.
#[pyclass(module = "scylla.results", frozen)]
pub(crate) struct RequestResult {
    core: PageCore,

    /// Cached Python-side column specifications of the first page.
    first_page_columns: PyOnceLock<Py<PyTuple>>,
}

impl From<PageCore> for RequestResult {
    fn from(core: PageCore) -> Self {
        Self {
            core,
            first_page_columns: PyOnceLock::new(),
        }
    }
}

#[pymethods]
impl RequestResult {
    /// Returns an async iterator over all rows with automatic paging.
    ///
    /// Creates an `AsyncRowsIterator` that transparently fetches
    /// subsequent pages as iteration progresses.
    ///
    /// # Returns
    ///
    /// Async iterator over all rows across all pages.
    pub fn __aiter__(&self) -> AsyncRowsIterator {
        AsyncRowsIterator::new(self.core.clone())
    }

    /// Returns the first row of the result.
    ///
    /// Fetches further pages as needed when the leading pages are empty.
    /// Returns `None` if the result has no rows.
    ///
    /// # Errors
    ///
    /// Returns an error if fetching or deserialization fails.
    pub fn first_row(&self, py: Python<'_>) -> PyResult<DriverFuture<Py<PyAny>, PyErr>> {
        let core = self.core.clone();

        DriverFuture::spawn(py, boxed_py_future(async move { core.first_row().await }))
    }

    /// Returns all rows from all pages with automatic paging.
    ///
    /// Fetches and returns all available rows across all pages as a Python list,
    /// automatically retrieving additional pages as needed.
    ///
    /// # Returns
    ///
    /// A list containing all rows as Python objects.
    ///
    /// # Errors
    ///
    /// Returns an error if fetching or deserialization fails.
    pub fn all(&self, py: Python<'_>) -> PyResult<DriverFuture<Py<PyList>, PyErr>> {
        let core = self.core.clone();

        DriverFuture::spawn(py, boxed_py_future(async move { core.all().await }))
    }

    /// The first page of the result, already fetched by `execute()`.
    #[getter]
    fn first_page(&self) -> Page {
        Page::from(self.core.clone())
    }

    /// Returns an async iterator over the pages of the result, starting with
    /// the first page.
    fn pages(&self) -> AsyncPagesIterator {
        AsyncPagesIterator::new(self.core.clone())
    }

    /// Specifications of the columns of the first page.
    ///
    /// Empty for a result that carries no rows, such as an `INSERT`.
    #[getter]
    fn get_first_page_columns(&self, py: Python<'_>) -> PyResult<Py<PyTuple>> {
        let columns = self.first_page_columns.get_or_try_init(py, || {
            column_spec_tuple(py, self.core.columns().unwrap_or_default())
        })?;
        Ok(columns.clone_ref(py))
    }
}
