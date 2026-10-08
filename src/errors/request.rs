//! Maps Rust driver request errors onto the Python exception hierarchy.
//!
//! The Python class is picked from the innermost cause, so the same failure raises the same
//! class whichever operation hit it; the operation is only part of the message.

use pyo3::prelude::*;
use scylla::errors::ExecutionError as RustExecutionError;

use crate::errors::execution::{DriverPrepareError, DriverUseKeyspaceError};
use crate::errors::{ExecutionError, MetadataError, SchemaAgreementError};

/// Maps an `ExecutionError`; `message` is the full description including the failed operation.
#[deny(clippy::wildcard_enum_match_arm)]
pub(crate) fn execution_error_to_pyerr(err: &RustExecutionError, message: String) -> PyErr {
    match err {
        RustExecutionError::PrepareError(e) => {
            DriverPrepareError::rust_driver_prepare_error(e.clone()).into()
        }
        RustExecutionError::BadQuery(_)
        | RustExecutionError::EmptyPlan
        | RustExecutionError::ConnectionPoolError(_)
        | RustExecutionError::LastAttemptError(_)
        | RustExecutionError::RequestTimeout(_) => py_err!(ExecutionError, message),
        RustExecutionError::UseKeyspaceError(e) => DriverUseKeyspaceError::from(e.clone()).into(),
        RustExecutionError::SchemaAgreementError(_) => py_err!(SchemaAgreementError, message),
        RustExecutionError::MetadataError(_) => py_err!(MetadataError, message),
        _ => unreachable!("clippy testifies that the match is exhaustive"),
    }
}
