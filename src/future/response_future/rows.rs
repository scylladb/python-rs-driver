//! The rows of a page of a `ResponseFuture`, as a reader takes them out of the lock.

use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::{PyList, PyNone};
use scylla::response::query_result::QueryResult;

use crate::core::results::page_rows;
use crate::deserialize::rows::{ResolvedPage, RowReader};
use crate::errors::execution::DriverExecuteError;

/// The rows of a settled page: a list, `None` for a result without rows, or the error.
pub(super) type Rows = PyResult<Option<Py<PyList>>>;

/// The rows of a settled page, taken out of the lock.
pub(super) enum PageRows {
    Built(Rows),
    /// Not built yet: the response and the reader to build them with.
    ToBuild(Arc<QueryResult>, RowReader),
}

/// Builds the rows of a page. Runs the row factory, which is user Python, so
/// the caller must not hold the lock.
pub(super) fn build_rows(
    py: Python<'_>,
    query_result: Arc<QueryResult>,
    reader: &RowReader,
) -> Rows {
    ResolvedPage::new(py, query_result, reader)
        .map_err(|err| DriverExecuteError::row_factory_failed(err).into())
        .and_then(|page| page_rows(py, page))
}

pub(super) fn clone_rows(py: Python<'_>, rows: &Rows) -> Rows {
    match rows {
        Ok(rows) => Ok(rows.as_ref().map(|list| list.clone_ref(py))),
        Err(err) => Err(err.clone_ref(py)),
    }
}

/// What a callback receives for `rows`: the list, `None`, or the exception instance.
pub(super) fn callback_value<'py>(py: Python<'py>, rows: &Rows) -> Bound<'py, PyAny> {
    match rows {
        Ok(Some(list)) => list.bind(py).clone().into_any(),
        Ok(None) => PyNone::get(py).to_owned().into_any(),
        Err(err) => err.value(py).clone().into_any(),
    }
}
