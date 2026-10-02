use pyo3::prelude::*;

pub mod decision;
pub mod errors;
pub mod policies;
pub mod request;
pub mod types;

#[pymodule]
pub(crate) fn retry(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<errors::PyDbError>()?;
    module.add_class::<errors::PyRequestAttemptError>()?;
    module.add_class::<request::PyRequestInfo>()?;
    module.add_class::<decision::PyRetryDecision>()?;
    module.add_class::<policies::PyDefaultRetrySession>()?;
    module.add_class::<policies::PyDowngradingConsistencyRetrySession>()?;
    module.add_class::<policies::PyFallthroughRetrySession>()?;
    module.add_class::<policies::PyDefaultRetryPolicy>()?;
    module.add_class::<policies::PyDowngradingConsistencyRetryPolicy>()?;
    module.add_class::<policies::PyFallthroughRetryPolicy>()?;
    module.add_class::<types::PyCqlResponseKind>()?;
    module.add_class::<types::PyCqlRequestKind>()?;
    module.add_class::<types::PyOperationType>()?;
    module.add_class::<types::PyWriteType>()?;

    Ok(())
}
