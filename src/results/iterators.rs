use crate::core::results::{PageCore, Pager, next_row_with_paging};
use crate::deserialize::row_factory::PyRowFactory;
use crate::deserialize::rows::{ResolvedPage, RowsIteratorKind};
use crate::future::{DriverFuture, boxed_py_future};
use pyo3::exceptions::{PyRuntimeError, PyStopAsyncIteration, PyStopIteration};
use pyo3::sync::MutexExt;
use pyo3::{Py, PyAny, PyErr, PyRef, PyResult, Python, pyclass, pymethods};
use std::sync::Arc;
use tokio::sync::Mutex;

/// Iterator over a single page of query results.
///
/// Yields rows materialized by the request's row factory.
#[pyclass(module = "scylla.results", frozen)]
pub(crate) struct SinglePageIterator {
    kind: std::sync::Mutex<RowsIteratorKind>,
}

impl SinglePageIterator {
    pub(super) fn new(page: ResolvedPage) -> Self {
        SinglePageIterator {
            kind: std::sync::Mutex::new(RowsIteratorKind::new(page)),
        }
    }
}

#[pymethods]
impl SinglePageIterator {
    pub fn __next__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let mut guard = self.kind.lock_py_attached(py).map_err(|_| {
            PyErr::new::<PyRuntimeError, _>("SinglePageIterator mutex was poisoned")
        })?;

        match guard.next(py) {
            Some(res) => res.map_err(Into::into),
            None => Err(PyErr::new::<PyStopIteration, _>("")),
        }
    }

    pub fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
}

/// Async iterator over all rows with automatic paging.
///
/// Fetches subsequent pages transparently as iteration progresses.
#[pyclass(module = "scylla.results", frozen)]
pub struct AsyncRowsIterator {
    state: Arc<Mutex<AsyncIteratorState>>,
}

impl AsyncRowsIterator {
    pub(super) fn new(core: PageCore) -> Self {
        let (page, query_pager, factory) = core.into_parts();

        AsyncRowsIterator {
            state: Arc::new(Mutex::new(AsyncIteratorState {
                rows_iterator: RowsIteratorKind::new(page),
                query_pager,
                factory,
            })),
        }
    }
}

#[pymethods]
impl AsyncRowsIterator {
    pub(crate) fn __anext__(&self, py: Python<'_>) -> PyResult<DriverFuture<Py<PyAny>, PyErr>> {
        if let Ok(mut state) = self.state.try_lock()
            && let Some(row_result) = state.rows_iterator.next(py)
        {
            let result = row_result.map_err(Into::into);
            return DriverFuture::ready(py, result);
        }

        let state_clone = self.state.clone();

        let future = boxed_py_future(async move {
            let mut state = state_clone.lock().await;

            let AsyncIteratorState {
                rows_iterator,
                query_pager,
                factory,
            } = &mut *state;

            match next_row_with_paging(rows_iterator, query_pager, factory).await {
                Some(res) => res.map_err(Into::into),
                None => Err(PyErr::new::<PyStopAsyncIteration, _>("")),
            }
        });

        DriverFuture::spawn(py, future)
    }

    pub fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
}

/// Mutable state for async row iteration.
///
/// Holds current row iterator and pagination state.
struct AsyncIteratorState {
    rows_iterator: RowsIteratorKind,
    query_pager: Pager,
    factory: PyRowFactory,
}
