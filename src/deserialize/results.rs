use crate::deserialize::value::{PyDeserializeValue, PyDeserializedValue};
use crate::errors::{DriverDeserializationError, DriverExecuteError, DriverRowIterationError};
use crate::future::PyResponseFuture;
use crate::serialize::value_list::PyValueList;
use crate::session::{ExecutableStatement, PySession};
use pyo3::exceptions::{PyRuntimeError, PyStopAsyncIteration, PyStopIteration};
use pyo3::prelude::{PyDictMethods, PyListMethods, PyModule, PyModuleMethods};
use pyo3::types::{PyDict, PyList, PyString, PyTuple};
use pyo3::{
    Bound, Py, PyAny, PyErr, PyRef, PyRefMut, PyResult, Python, pyclass, pymethods, pymodule,
};
use scylla::deserialize::DeserializationError as ScyllaDeserializationError;
use scylla::response::query_result::QueryResult;
use scylla_cql::deserialize::FrameSlice;
use scylla_cql::deserialize::result::RawRowIterator;
use scylla_cql::deserialize::row::{ColumnIterator, RawColumn};
use scylla_cql::frame::request::query::{PagingState, PagingStateResponse};
use stable_deref_trait::StableDeref;
use std::iter::Enumerate;
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::Mutex;
use yoke::{Yoke, Yokeable};

/// Database query result with paging support.
///
/// Represents a result frame from the database, providing access to rows
/// and support for fetching additional pages.
#[pyclass(frozen)]
pub(crate) struct RequestResult {
    row_factory: Option<Py<RowFactory>>,
    query_pager: Pager,
    query_result: Arc<QueryResult>,
}

impl RequestResult {
    pub(crate) fn new(
        query_result: QueryResult,
        query_pager: Pager,
        row_factory: Option<Py<RowFactory>>,
    ) -> Self {
        Self {
            query_pager,
            query_result: Arc::new(query_result),
            row_factory,
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
        self.query_pager.has_more_pages()
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
        self.query_pager.paging_state()
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
    fn fetch_next_page(&self, py: Python<'_>) -> PyResult<Py<PyResponseFuture>> {
        let mut query_pager = self.query_pager.clone();
        let row_factory = self.row_factory.clone();

        PyResponseFuture::spawn(py, async move {
            if let Some(query_result) = query_pager.fetch_next_page().await {
                Ok(Some(RequestResult {
                    query_result: Arc::new(query_result?),
                    query_pager,
                    row_factory,
                }))
            } else {
                Ok::<_, DriverExecuteError>(None)
            }
        })
    }

    /// Returns an iterator over rows in the current page.
    ///
    /// Creates a `SinglePageIterator` that yields deserialized rows
    /// from the current page only, without fetching additional pages.
    ///
    /// # Returns
    ///
    /// Iterator over rows in the current page.
    fn iter_current_page<'py>(&self, py: Python<'py>) -> PyResult<SinglePageIterator> {
        SinglePageIterator::new(py, self.query_result.clone(), self.row_factory.clone())
    }

    /// Returns an async iterator over all rows with automatic paging.
    ///
    /// Creates an `AsyncRowsIterator` that transparently fetches
    /// subsequent pages as iteration progresses.
    ///
    /// # Returns
    ///
    /// Async iterator over all rows across all pages.
    pub fn __aiter__(&self, py: Python<'_>) -> PyResult<AsyncRowsIterator> {
        AsyncRowsIterator::new(
            py,
            self.query_pager.clone(),
            self.query_result.clone(),
            self.row_factory.clone(),
        )
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
    pub fn first_row(&self, py: Python<'_>) -> PyResult<Py<PyResponseFuture>> {
        let mut query_pager_clone = self.query_pager.clone();
        let mut rows_iterator =
            RowsIteratorKind::new(py, self.query_result.clone(), self.row_factory.clone())?;

        PyResponseFuture::spawn(py, async move {
            match next_row_with_paging(&mut rows_iterator, &mut query_pager_clone).await {
                Some(res) => res.map(Some).map_err(Into::<PyErr>::into),
                None => Ok(None::<Py<PyAny>>),
            }
        })
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
    pub fn all(&self, py: Python<'_>) -> PyResult<Py<PyResponseFuture>> {
        let mut query_pager_clone = self.query_pager.clone();

        let (mut rows_iterator, list) =
            Python::attach(|py| -> PyResult<(RowsIteratorKind, Py<PyList>)> {
                Ok((
                    RowsIteratorKind::new(py, self.query_result.clone(), self.row_factory.clone())?,
                    PyList::empty(py).into(),
                ))
            })?;

        PyResponseFuture::spawn(py, async move {
            // Drain all rows from the current page, then fetch the next page.
            // This is done to hold the GIL for longer and avoid frequent reacquisition.
            let mut next_page: Option<QueryResult> = None;
            loop {
                Python::attach(|py| -> PyResult<()> {
                    if let Some(next_page) = next_page.take() {
                        rows_iterator.update(py, Arc::new(next_page))?;
                    }

                    while let Some(res_row) = rows_iterator.next(py) {
                        list.bind(py).append(res_row?)?;
                    }

                    Ok(())
                })?;

                if let Some(res) = query_pager_clone.fetch_next_page().await {
                    next_page = Some(res?);
                } else {
                    break;
                }
            }

            Ok::<_, PyErr>(list)
        })
    }
}

/// Iterator over a single page of query results.
///
/// Yields deserialized rows materialized using a `RowFactory`.
/// Each iteration returns one row as a Python object (default: dict).
#[pyclass(frozen)]
struct SinglePageIterator {
    kind: std::sync::Mutex<RowsIteratorKind>,
}

impl SinglePageIterator {
    fn new(
        py: Python<'_>,
        query_result: Arc<QueryResult>,
        factory: Option<Py<RowFactory>>,
    ) -> PyResult<Self> {
        Ok(SinglePageIterator {
            kind: std::sync::Mutex::new(RowsIteratorKind::new(py, query_result, factory)?),
        })
    }
}

#[pymethods]
impl SinglePageIterator {
    pub fn __next__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let guard = self.kind.lock().map_err(|_| {
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

/// Represents paging state for paged queries.
///
/// Used to continue a query from where the previous page ended.
/// Can be passed to execute() to resume paging from a specific position.
#[pyclass(name = "PagingState", frozen)]
pub struct PyPagingState {
    pub(crate) inner: PagingState,
}

#[pymethods]
impl PyPagingState {
    /// Creates a new paging state starting from the first page.
    #[new]
    fn new() -> Self {
        PyPagingState {
            inner: PagingState::start(),
        }
    }

    /// Returns the inner representation of `PagingState` as bytes.
    ///
    /// Use this to store paging state for a longer time, and later restore it
    /// using `from_bytes()`. Returns `None` if this represents the start state.
    ///
    /// # Returns
    ///
    /// Raw paging state bytes, or `None` for the start state.
    pub fn as_bytes<'py>(&self, py: Python<'py>) -> Option<Bound<'py, pyo3::types::PyBytes>> {
        self.inner
            .as_bytes_slice()
            .map(|arc_slice| pyo3::types::PyBytes::new(py, arc_slice))
    }

    /// Creates `PagingState` from raw bytes.
    ///
    /// Use this to restore paging state after longer time, having previously
    /// stored it using `as_bytes()`.
    ///
    /// # Parameters
    ///
    /// raw_bytes : Raw paging state bytes previously obtained from `as_bytes()`.
    ///
    /// # Returns
    ///
    /// A new `PagingState` restored from the raw bytes.
    #[staticmethod]
    pub fn from_bytes(raw_bytes: &[u8]) -> Self {
        Self {
            inner: PagingState::new_from_raw_bytes(raw_bytes),
        }
    }

    fn __eq__(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

/// Async iterator over all rows with automatic paging.
///
/// Fetches subsequent pages transparently as iteration progresses.
#[pyclass(frozen)]
pub struct AsyncRowsIterator {
    state: Arc<Mutex<AsyncIteratorState>>,
}

impl AsyncRowsIterator {
    fn new(
        py: Python<'_>,
        paging_api: Pager,
        query_result: Arc<QueryResult>,
        factory: Option<Py<RowFactory>>,
    ) -> PyResult<Self> {
        Ok(AsyncRowsIterator {
            state: Arc::new(Mutex::new(AsyncIteratorState {
                rows_iterator: RowsIteratorKind::new(py, query_result, factory)?,
                query_pager: paging_api,
            })),
        })
    }
}

#[pymethods]
impl AsyncRowsIterator {
    pub fn __anext__(&self, py: Python<'_>) -> PyResult<Py<PyResponseFuture>> {
        if let Ok(state) = self.state.try_lock()
            && let Some(row_result) = state.rows_iterator.next(py)
        {
            let result = row_result.map_err(Into::into);
            return PyResponseFuture::ready(py, result);
        }

        let state_clone = self.state.clone();

        PyResponseFuture::spawn(py, async move {
            let mut state = state_clone.lock().await;

            let AsyncIteratorState {
                rows_iterator,
                query_pager,
            } = &mut *state;

            match next_row_with_paging(rows_iterator, query_pager).await {
                Some(res) => res.map_err(Into::into),
                None => Err(PyErr::new::<PyStopAsyncIteration, _>("")),
            }
        })
    }

    pub fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
}

/// Mutable state for async row iteration.
///
/// Holds current row iterator and pagination state.
#[derive(Clone)]
struct AsyncIteratorState {
    rows_iterator: RowsIteratorKind,
    query_pager: Pager,
}

/// Loop until a row is produced, all pages are exhausted,
/// or an error occurs while fetching or updating pages.
async fn next_row_with_paging(
    rows_iterator: &mut RowsIteratorKind,
    query_pager: &mut Pager,
) -> Option<Result<Py<PyAny>, DriverRowIterationError>> {
    loop {
        if let Some(row) = Python::attach(|py| rows_iterator.next(py)) {
            return Some(row);
        }

        let query_result = match query_pager.fetch_next_page().await? {
            Ok(p) => p,
            Err(e) => return Some(Err(DriverRowIterationError::FailedToFetchNextPage(e))),
        };

        if let Err(err) = Python::attach(|py| rows_iterator.update(py, Arc::new(query_result))) {
            return Some(Err(DriverRowIterationError::PythonError(err)));
        }
    }
}

/// Iterator over columns of the current row.
///
/// This object is passed to `RowFactory.build` and allows iterating over
/// column values of a single row. Each iteration yields a `Column` object
/// containing the column name and its deserialized value.
///
/// This iterator is only intended to be consumed while building a row and
/// should not be stored or reused outside of that context.
#[pyclass(name = "ColumnIterator")]
pub struct RowColumnCursor {
    // Yoke-backed container holding both row and column iterators.
    //
    // The yoke ensures that iterators can borrow directly from the
    // underlying query result frame without cloning buffers or allocating
    // intermediate representations.
    //
    // `Cursor` holds:
    // - a `RawRowIterator` to advance between rows
    // - a `ColumnIterator` for iterating columns of the current row
    yoked: Yoke<Cursor<'static>, QueryResultCart>,

    // Cached Python strings for column names. Column names are identical
    // across all rows, so we create them once and reuse via clone_ref.
    column_names: Vec<Py<PyString>>,
}

impl RowColumnCursor {
    fn new(py: Python<'_>, query_result: Arc<QueryResult>) -> Self {
        let cart = QueryResultCart(query_result);

        // Pre-create Python strings for column names — they are
        // identical for every row and can be reused via clone_ref.
        let column_names: Vec<Py<PyString>> = {
            let raw_rows_with_metadata = cart.deserialized_metadata_and_rows().expect(
                "deserialized_metadata_and_rows can't be None after is_rows() returned true",
            );
            raw_rows_with_metadata
                .metadata()
                .col_specs()
                .iter()
                .map(|spec| PyString::new(py, spec.name()).unbind())
                .collect()
        };

        let yoked = Yoke::attach_to_cart(cart, |cart| {
            let raw_rows_with_metadata = cart.deserialized_metadata_and_rows().expect(
                "deserialized_metadata_and_rows can't be None after is_rows() returned true",
            );
            let frame_slice = FrameSlice::new(raw_rows_with_metadata.raw_rows());
            let col_specs = raw_rows_with_metadata.metadata().col_specs();
            let row_iterator =
                RawRowIterator::new(raw_rows_with_metadata.rows_count(), col_specs, frame_slice);

            let column_iterator = ColumnIterator::new(col_specs, frame_slice).enumerate();

            Cursor {
                row_iterator,
                column_iterator,
                current_raw_column: None,
            }
        });

        Self {
            yoked,
            column_names,
        }
    }

    fn next_column(
        &mut self,
        py: Python<'_>,
    ) -> Option<Result<Column, DriverDeserializationError>> {
        if let Err(err) = self
            .yoked
            .with_mut_return(|view: &mut Cursor<'_>| view.next_column())
            .map_err(DriverDeserializationError::scylla_decode_failed)
        {
            return Some(Err(err));
        }

        let cursor = self.yoked.get();

        // If `current_raw_column` is None, it means all columns of the current row have been exhausted.
        let (column_index, raw_col) = cursor.current_raw_column.as_ref()?;

        let value = match PyDeserializedValue::deserialize_py(raw_col.spec.typ(), raw_col.slice, py)
        {
            Ok(value) => value,
            Err(err) => {
                return Some(Err(err
                    .at_column_name(raw_col.spec.name())
                    .at_column_index(*column_index)));
            }
        };

        let column_name = Py::clone_ref(&self.column_names[*column_index], py);

        Some(Ok(Column { column_name, value }))
    }
}

#[pymethods]
impl RowColumnCursor {
    pub fn __next__(&mut self, py: Python<'_>) -> PyResult<Column> {
        match self.next_column(py) {
            Some(res) => res.map_err(Into::into),
            None => Err(PyErr::new::<PyStopIteration, _>("")),
        }
    }
    pub fn __iter__(slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf
    }
}

/// A single column value within a row.
///
/// `Column` represents one column of a row returned by a query. It contains
/// the column name and the corresponding deserialized Python value.
#[pyclass(frozen)]
pub struct Column {
    #[pyo3(get)]
    column_name: Py<PyString>,
    #[pyo3(get)]
    value: PyDeserializedValue,
}

/// Factory responsible for constructing Python row objects.
///
/// `RowFactory` defines how a row is materialized from a column iterator.
/// The default implementation consumes all columns of the current row and
/// returns a Python dictionary mapping column names to values.
///
/// Users may subclass this type to implement custom row mappings.
#[pyclass(subclass, frozen)]
pub struct RowFactory {}

#[pymethods]
impl RowFactory {
    /// Create a new `RowFactory`.
    ///
    /// The default row factory builds each row as a Python `dict`
    /// mapping column names to deserialized Python values.
    ///
    /// The constructor accepts arbitrary positional and keyword arguments.
    /// This allows Python subclasses to define their own `__init__`
    /// signatures and store custom configuration or state.
    #[expect(unused_variables)]
    #[new]
    #[pyo3(signature = (*args, **kwargs))]
    pub fn new(args: &Bound<'_, PyTuple>, kwargs: Option<&Bound<'_, PyDict>>) -> Self {
        RowFactory {}
    }

    /// Build a Python object representing a single row.
    ///
    /// This method consumes all columns from the provided column iterator
    /// and returns a Python `dict` mapping column names to values.
    ///
    /// Parameters
    /// ----------
    /// column_iterator : RowColumnCursor
    ///     Iterator over columns of the current row.
    ///
    /// Returns
    /// -------
    /// dict
    ///     A dictionary mapping column names (`str`) to deserialized
    ///     Python values.
    ///
    /// Raises
    /// ------
    /// DeserializationError
    ///     If any column cannot be deserialized into a Python object.
    /// RowIterationError
    ///     If building the Python row object fails (with original error attached).
    pub fn build<'py>(
        &self,
        py: Python<'py>,
        column_iterator: &Bound<'py, RowColumnCursor>,
    ) -> Result<Py<PyDict>, DriverRowIterationError> {
        let mut columns = column_iterator.borrow_mut();

        let dict = PyDict::new(py);
        while let Some(next) = columns.next_column(py) {
            let column = next.map_err(DriverRowIterationError::Deserialization)?;
            dict.set_item(column.column_name, column.value)
                .map_err(DriverRowIterationError::PythonError)?;
        }

        Ok(dict.into())
    }
}

impl RowFactory {
    fn default_instance() -> &'static Self {
        static DEFAULT_FACTORY: RowFactory = RowFactory {};
        &DEFAULT_FACTORY
    }
}

/// Determines how to iterate over query results based on result type.
///
/// Dispatches to either row iteration or handles non-row results.
#[derive(Clone)]
enum RowsIteratorKind {
    Rows {
        row_col_cursor: Py<RowColumnCursor>,
        factory: Option<Py<RowFactory>>,
    },
    NonRows,
}

impl RowsIteratorKind {
    fn new(
        py: Python<'_>,
        query_result: Arc<QueryResult>,
        factory: Option<Py<RowFactory>>,
    ) -> PyResult<Self> {
        if !query_result.is_rows() {
            return Ok(RowsIteratorKind::NonRows);
        }

        let row_col_cursor = Py::new(py, RowColumnCursor::new(py, query_result))?;

        Ok(RowsIteratorKind::Rows {
            row_col_cursor,
            factory,
        })
    }

    fn update(&mut self, py: Python, query_result: Arc<QueryResult>) -> PyResult<()> {
        if let RowsIteratorKind::Rows { row_col_cursor, .. } = self {
            *row_col_cursor = Py::new(py, RowColumnCursor::new(py, query_result))?;
        }
        Ok(())
    }

    fn next(&self, py: Python) -> Option<Result<Py<PyAny>, DriverRowIterationError>> {
        match self {
            RowsIteratorKind::Rows {
                row_col_cursor,
                factory,
            } => {
                let res = row_col_cursor
                    .borrow_mut(py)
                    .yoked
                    .with_mut_return(|cursor| cursor.next_row())?;

                let cursor_bound = row_col_cursor.bind(py);

                match res {
                    Ok(()) => {
                        let out: Result<Py<PyAny>, DriverRowIterationError> = match factory {
                            None => RowFactory::default_instance()
                                .build(py, cursor_bound)
                                .map(|d| d.into_any()),
                            Some(f) => f
                                .call_method1(py, "build", (&cursor_bound,))
                                .map_err(DriverRowIterationError::PythonError),
                        };

                        Some(out)
                    }
                    Err(err) => Some(Err(DriverRowIterationError::Deserialization(
                        DriverDeserializationError::scylla_decode_failed(err),
                    ))),
                }
            }
            RowsIteratorKind::NonRows => None,
        }
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
        session: PySession,
        query_request: ExecutableStatement,
        value_list: PyValueList,
    },
}

impl Pager {
    pub(crate) fn unpaged() -> Self {
        Pager::Unpaged
    }

    pub(crate) fn paged(
        paging_response: PagingStateResponse,
        session: PySession,
        query_request: ExecutableStatement,
        value_list: PyValueList,
    ) -> Self {
        Pager::Paged {
            paging_response,
            session,
            query_request,
            value_list,
        }
    }

    fn has_more_pages(&self) -> bool {
        matches!(
            self,
            Pager::Paged {
                paging_response: PagingStateResponse::HasMorePages { .. },
                ..
            }
        )
    }

    fn paging_state(&self) -> Option<PyPagingState> {
        match self {
            Pager::Paged {
                paging_response: PagingStateResponse::HasMorePages { state },
                ..
            } => Some(PyPagingState {
                inner: state.clone(),
            }),
            Pager::Paged {
                paging_response: PagingStateResponse::NoMorePages,
                ..
            } => None,
            Pager::Unpaged => None,
        }
    }

    async fn fetch_next_page(&mut self) -> Option<Result<QueryResult, DriverExecuteError>> {
        let Pager::Paged {
            paging_response,
            session,
            query_request,
            value_list,
        } = self
        else {
            return None;
        };

        let state = match paging_response {
            PagingStateResponse::HasMorePages { state } => state.clone(),
            PagingStateResponse::NoMorePages => return None,
        };

        let result = session
            .execute_single_page(state, query_request.clone(), value_list.clone())
            .await;

        let (query_result, new_paging_response) = match result {
            Ok(v) => v,
            Err(e) => return Some(Err(e)),
        };

        *paging_response = new_paging_response;

        Some(Ok(query_result))
    }
}

/// Stable cart holding deserialized metadata and raw row data.
///
/// This type exists solely to serve as a `StableDeref` cart for `Yoke`.
struct QueryResultCart(Arc<QueryResult>);

impl Deref for QueryResultCart {
    type Target = QueryResult;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

unsafe impl StableDeref for QueryResultCart {}

/// Yoke-backed wrapper holding row and column iterators.
///
/// `Cursor` is stored inside a `Yoke` so that both the row iterator
///  and the column iterator can borrow from the same data without cloning.
///
/// - `next_row` advances the row iterator and switches the active column
///   iterator to the value received from row iterator.
/// - `next_column` advances the column iterator and caches the current raw
///   column; Python deserialization is performed by `RowColumnCursor::next_column`.
#[derive(Yokeable)]
struct Cursor<'a> {
    row_iterator: RawRowIterator<'a, 'a>,
    column_iterator: Enumerate<ColumnIterator<'a, 'a>>,
    current_raw_column: Option<(usize, RawColumn<'a, 'a>)>,
}

impl<'a> Cursor<'a> {
    fn next_column(&mut self) -> Result<(), ScyllaDeserializationError> {
        self.current_raw_column = self
            .column_iterator
            .next()
            .map(|(column_index, raw_column_result)| {
                raw_column_result.map(|raw_col| (column_index, raw_col))
            })
            .transpose()?;

        Ok(())
    }

    fn next_row(&mut self) -> Option<Result<(), ScyllaDeserializationError>> {
        let column_iterator = self.row_iterator.next()?;

        match column_iterator {
            Ok(column_iterator) => {
                self.column_iterator = column_iterator.enumerate();
                Some(Ok(()))
            }
            Err(err) => Some(Err(err)),
        }
    }
}

#[pymodule]
pub(crate) fn results(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<RowFactory>()?;
    module.add_class::<Column>()?;
    module.add_class::<RowColumnCursor>()?;
    module.add_class::<SinglePageIterator>()?;
    module.add_class::<PyPagingState>()?;
    module.add_class::<RequestResult>()?;
    module.add_class::<AsyncRowsIterator>()?;

    Ok(())
}
