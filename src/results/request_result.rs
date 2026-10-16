use crate::core::results::PageCore;
use crate::core::traces::{PendingQueryTrace, fetch_traces};
use crate::errors::execution::DriverTraceError;
use crate::future::{DriverFuture, boxed_py_future};
use crate::results::iterators::{AsyncPagesIterator, AsyncRowsIterator};
use crate::results::page::Page;
use pyo3::types::{PyList, PyTuple};
use pyo3::{Py, PyAny, PyErr, PyResult, Python, pyclass, pymethods};
use std::sync::Arc;
use uuid::Uuid;

/// Result of a whole query, across all of its pages.
///
/// Returned only by `execute()` and `batch()`, so it always starts at the
/// query's first page. Every way of consuming it starts from that page.
///
/// Python-facing facade over the first [`Page`]: each method clones its core,
/// hands the work over, and awaits it.
#[pyclass(module = "scylla.results", frozen)]
pub(crate) struct RequestResult {
    first_page: Py<Page>,
}

impl RequestResult {
    pub(crate) fn new(py: Python<'_>, core: PageCore) -> PyResult<Self> {
        Ok(Self {
            first_page: Py::new(py, Page::from(core))?,
        })
    }

    fn core(&self) -> &PageCore {
        self.first_page.get().core()
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
        AsyncRowsIterator::new(self.core().clone())
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
        let core = self.core().clone();

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
        let core = self.core().clone();

        DriverFuture::spawn(py, boxed_py_future(async move { core.all().await }))
    }

    /// The first page of the result, already fetched by `execute()`.
    #[getter]
    fn first_page(&self, py: Python<'_>) -> Py<Page> {
        self.first_page.clone_ref(py)
    }

    /// Returns an async iterator over the pages of the result, starting with
    /// the first page.
    fn pages(&self, py: Python<'_>) -> AsyncPagesIterator {
        AsyncPagesIterator::new(self.first_page.clone_ref(py))
    }

    /// Specifications of the columns of the first page.
    ///
    /// Empty for a result that carries no rows, such as an `INSERT`.
    #[getter]
    fn get_first_page_columns(&self, py: Python<'_>) -> PyResult<Py<PyTuple>> {
        self.first_page.get().columns(py)
    }

    /// Warnings the server sent with the first page.
    #[getter]
    fn get_first_page_warnings(&self, py: Python<'_>) -> PyResult<Py<PyTuple>> {
        self.first_page.get().warnings(py)
    }

    /// The tracing ids of all traced requests made for this result, in the
    /// order their responses arrived.
    ///
    /// The list starts with the id of the first page. Every page that a read
    /// of this result fetches adds its id, also when the same page is fetched
    /// again by a later read.
    #[getter]
    fn trace_ids(&self) -> Vec<Uuid> {
        self.core().traces().ids()
    }

    /// Fetches the trace of the request that fetched the first page.
    ///
    /// Resolves to `None` if the request was not traced.
    ///
    /// # Errors
    ///
    /// Raises `TraceUnavailable` if the driver cannot fetch the trace.
    fn fetch_first_page_query_trace(
        &self,
        py: Python<'_>,
    ) -> PyResult<DriverFuture<Option<PendingQueryTrace>, DriverTraceError>> {
        self.first_page.get().fetch_query_trace(py)
    }

    /// Fetches the traces of all ids in `trace_ids`, in the same order.
    ///
    /// # Errors
    ///
    /// Raises `TraceUnavailable` if the driver cannot fetch one of the traces.
    fn fetch_all_query_traces(
        &self,
        py: Python<'_>,
    ) -> PyResult<DriverFuture<Vec<PendingQueryTrace>, DriverTraceError>> {
        let trace_ids = self.core().traces().ids();
        if trace_ids.is_empty() {
            return DriverFuture::ready(py, Ok(PyList::empty(py).into_any().unbind()));
        }
        let session = Arc::clone(self.core().session());

        DriverFuture::spawn_on_tokio(py, boxed_py_future(fetch_traces(session, trace_ids)))
    }
}
