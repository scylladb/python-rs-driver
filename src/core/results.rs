use std::sync::Arc;

use pyo3::prelude::{IntoPyObject, PyListMethods, Python};
use pyo3::types::PyList;
use pyo3::{Bound, Py, PyAny, PyErr, PyResult};
use scylla::response::query_result::QueryResult;
use scylla_cql::frame::request::query::{PagingState, PagingStateResponse};
use scylla_cql::frame::response::result::ColumnSpec;

use crate::core::session::{BoundStatement, SessionCore};
use crate::deserialize::error::DriverRowIterationError;
use crate::deserialize::results::{RequestResult, ResolvedPage, RowsIteratorKind};
use crate::deserialize::row_factory::PyRowFactory;
use crate::errors::execution::DriverExecuteError;

/// Helper performing the core logic of handling query results.
#[derive(Clone)]
pub(crate) struct RequestResultCore {
    /// Kept to resolve a builder for every following page.
    row_factory: PyRowFactory,
    query_pager: Pager,
    page: ResolvedPage,
}

impl RequestResultCore {
    pub(crate) fn new(
        py: Python<'_>,
        query_result: QueryResult,
        query_pager: Pager,
        row_factory: PyRowFactory,
    ) -> Result<Self, DriverExecuteError> {
        let page = ResolvedPage::new(py, Arc::new(query_result), &row_factory)
            .map_err(DriverExecuteError::row_factory_failed)?;

        Ok(Self {
            query_pager,
            page,
            row_factory,
        })
    }

    pub(crate) fn page(&self) -> &ResolvedPage {
        &self.page
    }

    /// Specifications of the page's columns, or `None` for a result without rows.
    pub(crate) fn columns(&self) -> Option<&[ColumnSpec<'_>]> {
        self.page
            .query_result()
            .deserialized_metadata_and_rows()
            .map(|rows| rows.metadata().col_specs())
    }

    pub(crate) fn into_parts(self) -> (ResolvedPage, Pager, PyRowFactory) {
        (self.page, self.query_pager, self.row_factory)
    }

    /// Returns `true` if more pages are available.
    pub(crate) fn has_more_pages(&self) -> bool {
        self.query_pager.has_more_pages()
    }

    /// Returns the current paging state, or `None` if no more pages are
    /// available.
    pub(crate) fn paging_state(&self) -> Option<PagingState> {
        self.query_pager.paging_state()
    }

    /// Fetches the next page, or returns `None` if no more pages exist.
    ///
    /// The page is bound to the row factory when it is handed to Python.
    pub(crate) async fn fetch_next_page(self) -> PyResult<Option<PendingRequestResult>> {
        let Self {
            row_factory,
            mut query_pager,
            ..
        } = self;

        let Some(query_result) = query_pager.fetch_next_page().await else {
            return Ok(None);
        };

        Ok(Some(PendingRequestResult::new(
            query_result?,
            query_pager,
            row_factory,
        )))
    }

    /// Returns the first row from the current position onwards, fetching
    /// further pages as needed, or `None` if no more rows exist.
    pub(crate) async fn first_row(self) -> PyResult<Py<PyAny>> {
        let Self {
            row_factory,
            mut query_pager,
            page,
        } = self;

        let mut rows_iterator = RowsIteratorKind::new(page);

        match next_row_with_paging(&mut rows_iterator, &mut query_pager, &row_factory).await {
            Some(res) => res.map_err(Into::into),
            None => Ok(Python::attach(|py| py.None())),
        }
    }

    /// Returns every remaining row across every remaining page as a list.
    pub(crate) async fn all(self) -> PyResult<Py<PyList>> {
        let Self {
            row_factory,
            mut query_pager,
            page,
        } = self;

        let mut rows_iterator = RowsIteratorKind::new(page);
        let list: Py<PyList> = Python::attach(|py| PyList::empty(py).into());

        // Drain all rows from the current page, then fetch the next page.
        // This is done to hold the GIL for longer and avoid frequent reacquisition.
        let mut next_page: Option<QueryResult> = None;
        loop {
            Python::attach(|py| -> PyResult<()> {
                if let Some(next_page) = next_page.take() {
                    rows_iterator
                        .update(py, Arc::new(next_page), &row_factory)
                        .map_err(DriverRowIterationError::PythonError)?;
                }

                while let Some(res_row) = rows_iterator.next(py) {
                    list.bind(py).append(res_row?)?;
                }

                Ok(())
            })?;

            let Some(page) = query_pager.fetch_next_page().await else {
                break;
            };

            next_page = Some(page?);
        }

        Ok(list)
    }
}

/// A finished request whose rows are not bound to a row factory yet.
pub(crate) struct PendingRequestResult {
    query_result: QueryResult,
    query_pager: Pager,
    factory: PyRowFactory,
}

impl PendingRequestResult {
    pub(crate) fn new(
        query_result: QueryResult,
        query_pager: Pager,
        factory: PyRowFactory,
    ) -> Self {
        Self {
            query_result,
            query_pager,
            factory,
        }
    }
}

impl<'py> IntoPyObject<'py> for PendingRequestResult {
    type Target = RequestResult;
    type Output = Bound<'py, RequestResult>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let core = RequestResultCore::new(py, self.query_result, self.query_pager, self.factory)?;

        Bound::new(py, RequestResult::from(core))
    }
}

/// Loop until a row is produced, all pages are exhausted,
/// or an error occurs while fetching or updating pages.
pub(crate) async fn next_row_with_paging(
    rows_iterator: &mut RowsIteratorKind,
    query_pager: &mut Pager,
    factory: &PyRowFactory,
) -> Option<Result<Py<PyAny>, DriverRowIterationError>> {
    // Switched to under the same GIL acquisition that reads its first row.
    let mut next_page: Option<QueryResult> = None;
    loop {
        let row = Python::attach(|py| {
            if let Some(next_page) = next_page.take()
                && let Err(err) = rows_iterator.update(py, Arc::new(next_page), factory)
            {
                return Some(Err(DriverRowIterationError::PythonError(err)));
            }

            rows_iterator.next(py)
        });

        if row.is_some() {
            return row;
        }

        next_page = match query_pager.fetch_next_page().await? {
            Ok(p) => Some(p),
            Err(e) => return Some(Err(DriverRowIterationError::FailedToFetchNextPage(e))),
        };
    }
}

/// Manages fetching next pages and encapsulates paging logic.
///
/// Responsible for handling pagination state transitions and retrieving
/// subsequent pages from paginated query results.
#[derive(Clone)]
pub(crate) enum Pager {
    Unpaged,
    Paged {
        paging_response: PagingStateResponse,
        session: SessionCore,
        prepared: Arc<BoundStatement>,
    },
}

impl Pager {
    pub(crate) fn unpaged() -> Self {
        Pager::Unpaged
    }

    pub(crate) fn paged(
        paging_response: PagingStateResponse,
        session: SessionCore,
        prepared: Arc<BoundStatement>,
    ) -> Self {
        Pager::Paged {
            paging_response,
            session,
            prepared,
        }
    }

    pub(crate) fn has_more_pages(&self) -> bool {
        matches!(
            self,
            Pager::Paged {
                paging_response: PagingStateResponse::HasMorePages { .. },
                ..
            }
        )
    }

    pub(crate) fn paging_state(&self) -> Option<PagingState> {
        match self {
            Pager::Paged {
                paging_response: PagingStateResponse::HasMorePages { state },
                ..
            } => Some(state.clone()),
            Pager::Paged {
                paging_response: PagingStateResponse::NoMorePages,
                ..
            } => None,
            Pager::Unpaged => None,
        }
    }

    pub(crate) async fn fetch_next_page(
        &mut self,
    ) -> Option<Result<QueryResult, DriverExecuteError>> {
        let Pager::Paged {
            paging_response,
            session,
            prepared,
        } = self
        else {
            return None;
        };

        let state = match paging_response {
            PagingStateResponse::HasMorePages { state } => state.clone(),
            PagingStateResponse::NoMorePages => return None,
        };

        let result = session
            .execute_single_page(state, Arc::clone(prepared))
            .await;

        let (query_result, new_paging_response) = match result {
            Ok(v) => v,
            Err(e) => return Some(Err(e)),
        };

        *paging_response = new_paging_response;

        Some(Ok(query_result))
    }
}
