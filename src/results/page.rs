use crate::TaskExecutionMode;
use crate::core::results::{PageCore, PendingRequestResult};
use crate::future::{DriverFuture, boxed_py_future};
use crate::results::iterators::SinglePageIterator;
use crate::results::paging_state::PyPagingState;
use pyo3::{Bound, IntoPyObject, PyErr, PyResult, Python, pyclass, pymethods};

/// A single page of a query result.
///
/// Immutable: iterating it yields only this page's rows and never fetches,
/// and `fetch_next_page()` returns a new `Page`.
///
/// Python-facing facade over [`PageCore`].
#[pyclass(module = "scylla.results", frozen)]
pub(crate) struct Page {
    core: PageCore,
}

impl From<PageCore> for Page {
    fn from(core: PageCore) -> Self {
        Self { core }
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
