use pyo3::{Bound, Python, pyclass, pymethods};
use scylla_cql::frame::request::query::PagingState;

/// Represents paging state for paged queries.
///
/// Used to continue a query from where the previous page ended.
/// Can be passed to execute() to resume paging from a specific position.
#[pyclass(module = "scylla.results", name = "PagingState", frozen)]
pub struct PyPagingState {
    inner: PagingState,
}

impl From<PagingState> for PyPagingState {
    fn from(inner: PagingState) -> Self {
        Self { inner }
    }
}

impl PyPagingState {
    pub(crate) fn inner(&self) -> &PagingState {
        &self.inner
    }
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
