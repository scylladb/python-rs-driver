//! `ResultSet`: the synchronous, transparently paging row iterator of the legacy driver.

use std::sync::Mutex;

use pyo3::PyTypeInfo;
use pyo3::exceptions::{PyDeprecationWarning, PyNotImplementedError, PyRuntimeError};
use pyo3::prelude::*;
use pyo3::sync::MutexExt;
use pyo3::types::PyList;

use crate::deserialize::results::{PyPagingState, RowFactory};
use crate::future::PyResponseFuture;

struct IterState {
    current_rows: Py<PyList>,
    /// Index of the next row of `current_rows` to yield; `None` until iteration
    /// starts, after which list mode is refused.
    position: Option<usize>,
    /// Every remaining row has been materialized into `current_rows`.
    list_mode: bool,
}

/// Rows of a request, iterated page by page with further pages fetched
/// transparently. Returned by `ResponseFuture.result()`.
#[pyclass(name = "ResultSet", frozen)]
pub(crate) struct PyResultSet {
    response_future: Py<PyResponseFuture>,
    state: Mutex<IterState>,
}

impl PyResultSet {
    /// `rows` are the first page's, `None` for a result without rows.
    pub(crate) fn new(
        py: Python<'_>,
        response_future: Py<PyResponseFuture>,
        rows: Option<Py<PyList>>,
    ) -> Self {
        Self {
            response_future,
            state: Mutex::new(IterState {
                current_rows: rows_or_empty(py, rows),
                position: None,
                list_mode: false,
            }),
        }
    }

    fn future(&self) -> &PyResponseFuture {
        self.response_future.get()
    }

    /// Yields the next row, fetching further pages as needed; `None` once exhausted.
    fn next_row(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        loop {
            {
                let mut state = self.state.lock_py_attached(py).unwrap();
                let IterState {
                    current_rows,
                    position,
                    list_mode,
                } = &mut *state;

                let position = position.get_or_insert(0);
                let rows = current_rows.bind(py);
                if *position < rows.len() {
                    let row = rows.get_item(*position)?;
                    *position += 1;
                    return Ok(Some(row.unbind()));
                }

                if !self.future().has_more_pages(py) {
                    if !*list_mode && !rows.is_empty() {
                        *current_rows = PyList::empty(py).unbind();
                        *position = 0;
                    }
                    return Ok(None);
                }
            }

            let rows = self.fetch_page(py)?;
            let mut state = self.state.lock_py_attached(py).unwrap();
            state.current_rows = rows;
            state.position = Some(0);
        }
    }

    /// Requests the next page and blocks for its rows.
    fn fetch_page(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        self.future().start_fetching_next_page(py)?;
        let rows = self.future().wait_rows(py)?;
        Ok(rows_or_empty(py, rows))
    }

    /// Every row from the start of the current page onwards, as a new list.
    fn drain_from_start(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        self.restart_page(py);

        let all = PyList::empty(py);
        while let Some(row) = self.next_row(py)? {
            all.append(row)?;
        }
        Ok(all.unbind())
    }

    /// Materializes every remaining row so `operator` can work on the whole result.
    fn enter_list_mode(&self, py: Python<'_>, operator: &str) -> PyResult<()> {
        {
            let state = self.state.lock_py_attached(py).unwrap();
            if state.list_mode {
                return Ok(());
            }
            if state.position.is_some() {
                return Err(PyRuntimeError::new_err(format!(
                    "Cannot use {operator} when results have been iterated."
                )));
            }
        }

        if self.future().has_more_pages(py) {
            log::warn!(
                "Using {operator} on paged results causes entire result set to be materialized."
            );
        }

        let all = self.drain_from_start(py)?;
        let mut state = self.state.lock_py_attached(py).unwrap();
        state.current_rows = all;
        state.position = None;
        state.list_mode = true;
        Ok(())
    }

    /// Starts iterating the current page from its first row.
    fn restart_page(&self, py: Python<'_>) {
        self.state.lock_py_attached(py).unwrap().position = Some(0);
    }

    /// The rows list itself, to run Python code on without holding the state lock.
    fn current_rows_ref(&self, py: Python<'_>) -> Py<PyList> {
        self.state
            .lock_py_attached(py)
            .unwrap()
            .current_rows
            .clone_ref(py)
    }
}

#[pymethods]
impl PyResultSet {
    /// Whether the last page received says more follow.
    #[getter]
    fn has_more_pages(&self, py: Python<'_>) -> bool {
        self.future().has_more_pages(py)
    }

    /// Rows of the current page. Empty does not mean exhausted; see `has_more_pages`.
    #[getter]
    fn current_rows(&self, py: Python<'_>) -> Py<PyList> {
        self.current_rows_ref(py)
    }

    /// Every remaining row as a list; `list(result_set)`.
    fn all(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        self.drain_from_start(py)
    }

    /// The first row of the current page, `None` if it is empty.
    fn one(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let state = self.state.lock_py_attached(py).unwrap();
        let rows = state.current_rows.bind(py);
        if rows.is_empty() {
            return Ok(None);
        }
        Ok(Some(rows.get_item(0)?.unbind()))
    }

    fn __iter__(slf: Py<Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let this = slf.get();
        {
            let state = this.state.lock_py_attached(py).unwrap();
            if state.list_mode {
                return Ok(state.current_rows.bind(py).try_iter()?.unbind().into_any());
            }
        }
        this.restart_page(py);
        Ok(slf.into_any())
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        self.next_row(py)
    }

    /// Fetches the next page synchronously into `current_rows`; not needed when iterating.
    fn fetch_next_page(&self, py: Python<'_>) -> PyResult<()> {
        let rows = if self.future().has_more_pages(py) {
            self.fetch_page(py)?
        } else {
            PyList::empty(py).unbind()
        };
        let mut state = self.state.lock_py_attached(py).unwrap();
        state.current_rows = rows;

        state.position = state.position.map(|_| 0);
        Ok(())
    }

    fn __eq__(&self, py: Python<'_>, other: Bound<'_, PyAny>) -> PyResult<bool> {
        self.enter_list_mode(py, "equality operator")?;
        self.current_rows_ref(py).bind(py).eq(other)
    }

    fn __getitem__(&self, py: Python<'_>, index: Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        if index.eq(0)? {
            PyErr::warn(
                py,
                PyDeprecationWarning::type_object(py).as_any(),
                c"ResultSet indexing support will be removed in 4.0. Consider using ResultSet.one() to get a single row.",
                1,
            )?;
        }
        self.enter_list_mode(py, "index operator")?;
        Ok(self
            .current_rows_ref(py)
            .bind(py)
            .as_any()
            .get_item(index)?
            .unbind())
    }

    fn __bool__(&self, py: Python<'_>) -> bool {
        let state = self.state.lock_py_attached(py).unwrap();
        !state.current_rows.bind(py).is_empty()
    }

    /// Paging state of the last page, `None` if the query is not paged or exhausted.
    #[getter]
    fn paging_state(&self, py: Python<'_>) -> Option<PyPagingState> {
        self.future().paging_state(py)
    }

    /// Names of the result columns, `None` for a result without rows.
    #[getter]
    fn column_names(&self, py: Python<'_>) -> PyResult<Option<Py<PyList>>> {
        self.future().column_names(py)
    }

    /// CQL types of the result columns, `None` for a result without rows.
    #[getter]
    fn column_types(&self, py: Python<'_>) -> PyResult<Option<Py<PyList>>> {
        self.future().column_types(py)
    }

    /// For an LWT result, whether the transaction was applied. Like the legacy
    /// driver, only the driver's own row factory is supported: a user factory
    /// may build rows that have no `[applied]` to look up.
    #[getter]
    fn was_applied(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if let Some(factory) = self.future().row_factory_ref()
            && !factory.bind(py).is_exact_instance_of::<RowFactory>()
        {
            return Err(PyRuntimeError::new_err(format!(
                "Cannot determine LWT result with row factory {}",
                factory.bind(py).repr()?
            )));
        }

        let is_batch = self.future().query_ref().is_batch();
        let rows = self.current_rows_ref(py);
        let rows = rows.bind(py);

        if is_batch {
            let names = self.future().column_names(py)?;
            let applied_first = match &names {
                Some(names) => names.bind(py).get_item(0)?.eq("[applied]")?,
                None => false,
            };
            if !applied_first {
                return Err(PyRuntimeError::new_err(
                    "No LWT were present in the BatchStatement",
                ));
            }
        } else if rows.len() != 1 {
            return Err(PyRuntimeError::new_err(format!(
                "LWT result should have exactly one row. This has {}.",
                rows.len()
            )));
        }

        Ok(rows.get_item(0)?.get_item("[applied]")?.unbind())
    }

    #[pyo3(signature = (max_wait_sec=None))]
    fn get_query_trace(&self, max_wait_sec: Option<Bound<'_, PyAny>>) -> PyResult<()> {
        let _ = max_wait_sec;
        Err(PyNotImplementedError::new_err(
            "fetching query traces is not supported",
        ))
    }

    #[pyo3(signature = (max_wait_sec_per=None))]
    fn get_all_query_traces(&self, max_wait_sec_per: Option<Bound<'_, PyAny>>) -> PyResult<()> {
        let _ = max_wait_sec_per;
        Err(PyNotImplementedError::new_err(
            "fetching query traces is not supported",
        ))
    }
}

/// A result without rows iterates as an empty page.
fn rows_or_empty(py: Python<'_>, rows: Option<Py<PyList>>) -> Py<PyList> {
    rows.unwrap_or_else(|| PyList::empty(py).unbind())
}
