use crate::TaskExecutionMode;
use crate::cluster::metadata::query_metadata::column_spec_tuple;
use crate::core::results::{PageCore, PendingRequestResult};
use crate::future::{DriverFuture, boxed_py_future};
use crate::results::iterators::{AsyncPagesIterator, AsyncRowsIterator, SinglePageIterator};
use crate::results::page::Page;
use crate::results::paging_state::PyPagingState;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyList, PyTuple};
use pyo3::{Py, PyAny, PyErr, PyResult, Python, pyclass, pymethods};

/// Database query result with paging support.
///
/// Represents a result frame from the database, providing access to rows
/// and support for fetching additional pages.
///
/// Python-facing facade over [`PageCore`]: each method clones the core,
/// hands the work over, and awaits it.
#[pyclass(module = "scylla.results", frozen)]
pub(crate) struct RequestResult {
    core: PageCore,

    /// Cached Python-side result column specifications.
    columns: PyOnceLock<Py<PyTuple>>,
}

impl From<PageCore> for RequestResult {
    fn from(core: PageCore) -> Self {
        Self {
            core,
            columns: PyOnceLock::new(),
        }
    }
}

#[pymethods]
impl RequestResult {
    /// Returns `true` if more pages are available.
    ///
    /// # Returns
    ///
    /// `true` if additional pages can be fetched, `false` otherwise.
    fn has_more_pages(&self) -> bool {
        self.core.has_more_pages()
    }

    /// Returns the current paging state.
    ///
    /// Can be `None` if there are no more pages available.
    /// The paging state can be passed to `execute()` to resume paging
    /// from a specific position.
    ///
    /// # Returns
    ///
    /// Current paging state or `None` if no more pages are available.
    fn paging_state(&self) -> Option<PyPagingState> {
        self.core.paging_state().map(PyPagingState::from)
    }

    /// Fetches the next page if available.
    ///
    /// Returns a new `RequestResult` with the next page's data if more pages
    /// are available. Returns `None` if no more pages exist.
    ///
    /// # Returns
    ///
    /// `Some(RequestResult)` with the next page data, or `None` if no more pages.
    ///
    /// # Errors
    ///
    /// Returns an error if the fetch operation fails.
    fn fetch_next_page(
        &self,
        py: Python<'_>,
    ) -> PyResult<DriverFuture<Option<PendingRequestResult>, PyErr>> {
        let (query_pager, row_factory) = self.core.clone_pager_and_factory();

        DriverFuture::spawn(
            py,
            boxed_py_future(async move {
                query_pager
                    .fetch_next_pending_page(row_factory, TaskExecutionMode::SpawnOnRuntime)
                    .await
            }),
        )
    }

    /// Returns an iterator over rows in the current page.
    ///
    /// Creates a `SinglePageIterator` that yields deserialized rows
    /// from the current page only, without fetching additional pages.
    ///
    /// # Returns
    ///
    /// Iterator over rows in the current page.
    fn iter_current_page(&self) -> SinglePageIterator {
        SinglePageIterator::new(self.core.page().clone())
    }

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

    /// Returns the first row starting from the current state.
    ///
    /// Fetches the first available row from the current page onwards,
    /// automatically retrieving additional pages as needed.
    /// Returns `None` if no more rows are available.
    ///
    /// # Returns
    ///
    /// The first row as a Python object from current state, or `None` if no more rows exist.
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

    /// Specifications of the columns in this result.
    ///
    /// Empty for a result that carries no rows, such as an `INSERT`.
    #[getter]
    fn get_columns(&self, py: Python<'_>) -> PyResult<Py<PyTuple>> {
        let columns = self.columns.get_or_try_init(py, || {
            column_spec_tuple(py, self.core.columns().unwrap_or_default())
        })?;
        Ok(columns.clone_ref(py))
    }
}
