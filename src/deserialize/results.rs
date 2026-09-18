use crate::cluster::metadata::query_metadata::column_spec_tuple;
use crate::core::results::{Pager, RequestResultCore, next_row_with_paging};
use crate::deserialize::row_factory::{
    PyClassRowFactory, PyDictRowFactory, PyNamedTupleRowFactory, PyTupleRowFactory, RowBuilder,
};
use crate::errors::{DriverDeserializationError, DriverRowIterationError};
use crate::future::{DriverFuture, boxed_py_future};
use crate::value::{PyDeserializeValue, PyDeserializedValue};
use pyo3::exceptions::{PyRuntimeError, PyStopAsyncIteration, PyStopIteration};
use pyo3::prelude::{PyModule, PyModuleMethods};
use pyo3::sync::{MutexExt, PyOnceLock};
use pyo3::types::{PyList, PyTuple};
use pyo3::{Bound, Py, PyAny, PyErr, PyRef, PyResult, Python, pyclass, pymethods, pymodule};
use scylla::response::query_result::QueryResult;
use scylla_cql::deserialize::FrameSlice;
use scylla_cql::deserialize::result::RawRowIterator;
use scylla_cql::deserialize::row::ColumnIterator;
use scylla_cql::frame::request::query::PagingState;
use stable_deref_trait::StableDeref;
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::Mutex;
use yoke::{Yoke, Yokeable};

/// Database query result with paging support.
///
/// Represents a result frame from the database, providing access to rows
/// and support for fetching additional pages.
///
/// Python-facing facade over [`RequestResultCore`]: each method clones the core,
/// hands the work over, and awaits it.
#[pyclass(frozen)]
pub(crate) struct RequestResult {
    core: RequestResultCore,

    /// Cached Python-side result column specifications.
    columns: PyOnceLock<Py<PyTuple>>,
}

impl From<RequestResultCore> for RequestResult {
    fn from(core: RequestResultCore) -> Self {
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
        self.core
            .paging_state()
            .map(|inner| PyPagingState { inner })
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
    ) -> PyResult<DriverFuture<Option<RequestResult>, PyErr>> {
        let core = self.core.clone();

        DriverFuture::spawn(
            py,
            boxed_py_future(async move {
                core.fetch_next_page()
                    .await
                    .map(|next| next.map(RequestResult::from))
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
        SinglePageIterator::new(
            self.core.query_result.clone(),
            self.core.row_builder.clone(),
        )
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
        AsyncRowsIterator::new(
            self.core.query_pager.clone(),
            self.core.query_result.clone(),
            self.core.row_builder.clone(),
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

    /// Specifications of the columns in this result.
    ///
    /// Empty for a result that carries no rows, such as an `INSERT`.
    #[getter]
    fn get_columns(&self, py: Python<'_>) -> PyResult<Py<PyTuple>> {
        let columns = self.columns.get_or_try_init(py, || {
            match self.core.query_result.deserialized_metadata_and_rows() {
                None => column_spec_tuple(py, &[]),
                Some(rows) => column_spec_tuple(py, rows.metadata().col_specs()),
            }
        })?;
        Ok(columns.clone_ref(py))
    }
}

/// Iterator over a single page of query results.
///
/// Yields rows materialized by the request's row factory.
#[pyclass(frozen)]
struct SinglePageIterator {
    kind: std::sync::Mutex<RowsIteratorKind>,
}

impl SinglePageIterator {
    fn new(query_result: Arc<QueryResult>, builder: Option<RowBuilder>) -> Self {
        SinglePageIterator {
            kind: std::sync::Mutex::new(RowsIteratorKind::new(query_result, builder)),
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
    fn new(paging_api: Pager, query_result: Arc<QueryResult>, builder: Option<RowBuilder>) -> Self {
        AsyncRowsIterator {
            state: Arc::new(Mutex::new(AsyncIteratorState {
                rows_iterator: RowsIteratorKind::new(query_result, builder),
                query_pager: paging_api,
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
            } = &mut *state;

            match next_row_with_paging(rows_iterator, query_pager).await {
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
}

/// Determines how to iterate over query results based on result type.
///
/// Dispatches to either row iteration or handles non-row results.
pub(crate) enum RowsIteratorKind {
    Rows {
        rows: PageRowIterator,
        builder: RowBuilder,
    },
    NonRows,
}

impl RowsIteratorKind {
    pub(crate) fn new(query_result: Arc<QueryResult>, builder: Option<RowBuilder>) -> Self {
        match builder {
            None => RowsIteratorKind::NonRows,
            Some(builder) => RowsIteratorKind::Rows {
                rows: PageRowIterator::new(query_result),
                builder,
            },
        }
    }

    pub(crate) fn update(&mut self, query_result: Arc<QueryResult>) {
        if let RowsIteratorKind::Rows { rows, .. } = self {
            *rows = PageRowIterator::new(query_result);
        }
    }

    pub(crate) fn next(
        &mut self,
        py: Python<'_>,
    ) -> Option<Result<Py<PyAny>, DriverRowIterationError>> {
        let RowsIteratorKind::Rows { rows, builder } = self else {
            return None;
        };

        rows.next_row(py, builder)
    }
}

/// Iterator over the rows of a single page.
///
/// The iterators borrow directly from the underlying frame, so they are held in
/// a yoke together with the buffer they point into.
pub(crate) struct PageRowIterator {
    yoked: Yoke<PageRows<'static>, QueryResultCart>,
}

impl PageRowIterator {
    fn new(query_result: Arc<QueryResult>) -> Self {
        let yoked = Yoke::attach_to_cart(QueryResultCart(query_result), |cart| {
            let raw_rows_with_metadata = cart.deserialized_metadata_and_rows().expect(
                "deserialized_metadata_and_rows can't be None after is_rows() returned true",
            );
            let frame_slice = FrameSlice::new(raw_rows_with_metadata.raw_rows());

            PageRows {
                rows: RawRowIterator::new(
                    raw_rows_with_metadata.rows_count(),
                    raw_rows_with_metadata.metadata().col_specs(),
                    frame_slice,
                ),
                columns: None,
            }
        });

        Self { yoked }
    }

    fn next_row(
        &mut self,
        py: Python<'_>,
        builder: &RowBuilder,
    ) -> Option<Result<Py<PyAny>, DriverRowIterationError>> {
        if let Err(err) = self.advance()? {
            return Some(Err(DriverRowIterationError::Deserialization(err)));
        }

        let columns = self
            .yoked
            .get()
            .columns
            .clone()
            .expect("advance() stores the column iterator whenever it reports a row");

        Some(builder.build(DeserializedColumns::new(py, columns)))
    }

    fn advance(&mut self) -> Option<Result<(), DriverDeserializationError>> {
        self.yoked.with_mut_return(|page| match page.rows.next()? {
            Ok(columns) => {
                page.columns = Some(columns);
                Some(Ok(()))
            }
            Err(err) => Some(Err(DriverDeserializationError::scylla_decode_failed(err))),
        })
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

#[derive(Yokeable)]
pub(crate) struct PageRows<'a> {
    rows: RawRowIterator<'a, 'a>,
    columns: Option<ColumnIterator<'a, 'a>>,
}

/// The columns of a single row, deserialized to Python objects as they are
/// consumed.
pub(crate) struct DeserializedColumns<'a, 'py> {
    columns: ColumnIterator<'a, 'a>,
    py: Python<'py>,
}

impl<'a, 'py> DeserializedColumns<'a, 'py> {
    pub(crate) fn new(py: Python<'py>, columns: ColumnIterator<'a, 'a>) -> Self {
        Self { columns, py }
    }

    pub(crate) fn py(&self) -> Python<'py> {
        self.py
    }
}

impl Iterator for DeserializedColumns<'_, '_> {
    type Item = Result<PyDeserializedValue, DriverRowIterationError>;

    fn next(&mut self) -> Option<Self::Item> {
        let column = match self.columns.next()? {
            Ok(column) => column,
            Err(err) => {
                return Some(Err(
                    DriverDeserializationError::scylla_decode_failed(err).into()
                ));
            }
        };

        Some(
            PyDeserializedValue::deserialize_py(column.spec.typ(), column.slice, self.py).map_err(
                |err| {
                    err.at_column_name(column.spec.name())
                        .at_column_index(column.index)
                        .into()
                },
            ),
        )
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.columns.size_hint()
    }
}

impl ExactSizeIterator for DeserializedColumns<'_, '_> {}

#[pymodule]
pub(crate) fn results(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyNamedTupleRowFactory>()?;
    module.add_class::<PyDictRowFactory>()?;
    module.add_class::<PyTupleRowFactory>()?;
    module.add_class::<PyClassRowFactory>()?;
    module.add_class::<SinglePageIterator>()?;
    module.add_class::<PyPagingState>()?;
    module.add_class::<RequestResult>()?;
    module.add_class::<AsyncRowsIterator>()?;

    Ok(())
}
