//! The legacy (`cassandra-driver` compatible) API.

use pyo3::prelude::*;

use crate::future::PyResponseFuture;

pub(crate) mod errors;
pub(crate) mod pending_response_page;
pub(crate) mod result_set;
pub(crate) mod session;

use errors::QueryExhausted;
pub(crate) use result_set::PyResultSet;
pub(crate) use session::PyLegacySession;

#[pymodule]
pub(crate) fn legacy(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyResponseFuture>()?;
    module.add("QueryExhausted", py.get_type::<QueryExhausted>())?;
    module.add_class::<PyResultSet>()?;
    module.add_class::<PyLegacySession>()?;
    Ok(())
}
