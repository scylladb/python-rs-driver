use crate::TaskExecutionMode;
use crate::cluster::metadata::query_metadata::column_spec_tuple;
use crate::core::results::{PageCore, PendingRequestResult};
use crate::core::traces::PendingQueryTrace;
use crate::errors::execution::DriverTraceError;
use crate::future::{DriverFuture, boxed_py_future};
use crate::results::iterators::SinglePageIterator;
use crate::results::paging_state::PyPagingState;
use crate::utils::cached_option;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyTuple;
use pyo3::{Bound, IntoPyObject, Py, PyAny, PyErr, PyResult, Python, pyclass, pymethods};

/// A single page of a query result.
///
/// Immutable: iterating it yields only this page's rows and never fetches,
/// and `fetch_next_page()` returns a new `Page`.
///
/// Python-facing facade over [`PageCore`].
#[pyclass(module = "scylla.results", frozen)]
pub(crate) struct Page {
    core: PageCore,

    /// Cached Python-side column specifications.
    columns: PyOnceLock<Py<PyTuple>>,

    /// Cached Python-side server warnings.
    warnings: PyOnceLock<Py<PyTuple>>,

    py_trace_id: PyOnceLock<Option<Py<PyAny>>>,
}

impl From<PageCore> for Page {
    fn from(core: PageCore) -> Self {
        Self {
            core,
            columns: PyOnceLock::new(),
            warnings: PyOnceLock::new(),
            py_trace_id: PyOnceLock::new(),
        }
    }
}

impl Page {
    pub(crate) fn core(&self) -> &PageCore {
        &self.core
    }
}

#[pymethods]
impl Page {
    /// Returns an iterator over the rows of this page.
    fn __iter__(&self) -> SinglePageIterator {
        SinglePageIterator::new(self.core.page().clone())
    }

    /// Paging state that resumes the query after this page.
    ///
    /// `None` if this is the last page.
    #[getter]
    fn paging_state(&self) -> Option<PyPagingState> {
        self.core.paging_state().map(PyPagingState::from)
    }

    /// `True` if there is a page after this one.
    #[getter]
    fn has_more_pages(&self) -> bool {
        self.core.has_more_pages()
    }

    /// Specifications of the columns of this page.
    ///
    /// Empty for a result that carries no rows, such as an `INSERT`.
    #[getter]
    pub(crate) fn columns(&self, py: Python<'_>) -> PyResult<Py<PyTuple>> {
        let columns = self.columns.get_or_try_init(py, || {
            column_spec_tuple(py, self.core.columns().unwrap_or_default())
        })?;
        Ok(columns.clone_ref(py))
    }

    /// Warnings the server sent with this page.
    #[getter]
    pub(crate) fn warnings(&self, py: Python<'_>) -> PyResult<Py<PyTuple>> {
        let warnings = self.warnings.get_or_try_init(py, || {
            PyTuple::new(py, self.core.warnings().collect::<Vec<_>>()).map(Bound::unbind)
        })?;
        Ok(warnings.clone_ref(py))
    }

    /// Fetches the page after this one, or `None` if this is the last page.
    ///
    /// # Errors
    ///
    /// Returns an error if the fetch operation fails.
    fn fetch_next_page(
        &self,
        py: Python<'_>,
    ) -> PyResult<DriverFuture<Option<PendingPage>, PyErr>> {
        let (query_pager, row_factory) = self.core.clone_pager_and_factory();

        DriverFuture::spawn_on_tokio(
            py,
            boxed_py_future(async move {
                Ok(query_pager
                    .fetch_next_pending_page(row_factory, TaskExecutionMode::Inline)
                    .await?
                    .map(PendingPage))
            }),
        )
    }

    /// The tracing id of the request that fetched this page.
    ///
    /// `None` if the request was not traced.
    #[getter]
    fn trace_id(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        cached_option(py, &self.py_trace_id, || self.core.trace_id())
    }

    /// Fetches the trace of the request that fetched this page.
    ///
    /// Resolves to `None` if the request was not traced.
    ///
    /// # Errors
    ///
    /// Raises `TraceUnavailable` if the driver cannot fetch the trace.
    pub(crate) fn fetch_query_trace(
        &self,
        py: Python<'_>,
    ) -> PyResult<DriverFuture<Option<PendingQueryTrace>, DriverTraceError>> {
        match self.core.trace_id() {
            Some(trace_id) => DriverFuture::spawn_on_tokio(
                py,
                boxed_py_future(self.core.fetch_query_trace(trace_id)),
            ),
            None => DriverFuture::ready(py, Ok(py.None())),
        }
    }
}

/// A fetched page that becomes a [`Page`] when handed to Python.
pub(crate) struct PendingPage(PendingRequestResult);

impl From<PendingRequestResult> for PendingPage {
    fn from(result: PendingRequestResult) -> Self {
        Self(result)
    }
}

impl<'py> IntoPyObject<'py> for PendingPage {
    type Target = Page;
    type Output = Bound<'py, Page>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Bound::new(py, Page::from(self.0.resolve(py)?))
    }
}
