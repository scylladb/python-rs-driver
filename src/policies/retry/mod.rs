use pyo3::prelude::*;

pub mod decision;
pub mod policies;
pub mod request;

#[pymodule]
pub(crate) fn retry(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<request::PyRequestInfo>()?;
    module.add_class::<decision::PyRetryDecision>()?;
    module.add_class::<policies::PyDefaultRetrySession>()?;
    module.add_class::<policies::PyDowngradingConsistencyRetrySession>()?;
    module.add_class::<policies::PyFallthroughRetrySession>()?;
    module.add_class::<policies::PyDefaultRetryPolicy>()?;
    module.add_class::<policies::PyDowngradingConsistencyRetryPolicy>()?;
    module.add_class::<policies::PyFallthroughRetryPolicy>()?;

    Ok(())
}
