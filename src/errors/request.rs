//! Maps Rust driver request errors onto the Python exception hierarchy.
//!
//! The Python class is picked from the innermost cause, so the same failure raises the same
//! class whichever operation hit it; the operation is only part of the message.

use pyo3::prelude::*;
use scylla::errors::{BadQuery as RustBadQuery, ExecutionError as RustExecutionError};

use crate::errors::execution::{DriverPrepareError, DriverUseKeyspaceError};
use crate::errors::{
    ExecutionError, MetadataError, NoHostAvailable, OperationTimedOut,
    PartitionKeyExtractionFailed, SchemaAgreementError, SerializationError,
    TooManyStatementsInBatch, ValuesTooLongForKey,
};

/// Maps an `ExecutionError`; `message` is the full description including the failed operation.
#[deny(clippy::wildcard_enum_match_arm)]
pub(crate) fn execution_error_to_pyerr(
    py: Python<'_>,
    err: &RustExecutionError,
    message: String,
) -> PyErr {
    match err {
        RustExecutionError::BadQuery(e) => bad_query_to_pyerr(py, e, message),
        RustExecutionError::EmptyPlan => py_err!(NoHostAvailable, message),
        RustExecutionError::PrepareError(e) => {
            DriverPrepareError::rust_driver_prepare_error(e.clone()).into()
        }
        RustExecutionError::ConnectionPoolError(_) | RustExecutionError::LastAttemptError(_) => {
            py_err!(ExecutionError, message)
        }
        RustExecutionError::RequestTimeout(timeout) => {
            py_err!(py, OperationTimedOut, message; timeout)
        }
        RustExecutionError::UseKeyspaceError(e) => DriverUseKeyspaceError::from(e.clone()).into(),
        RustExecutionError::SchemaAgreementError(_) => py_err!(SchemaAgreementError, message),
        RustExecutionError::MetadataError(_) => py_err!(MetadataError, message),
        _ => unreachable!("clippy testifies that the match is exhaustive"),
    }
}

#[deny(clippy::wildcard_enum_match_arm)]
fn bad_query_to_pyerr(py: Python<'_>, err: &RustBadQuery, message: String) -> PyErr {
    match err {
        RustBadQuery::PartitionKeyExtraction => py_err!(PartitionKeyExtractionFailed, message),
        RustBadQuery::SerializationError(_) => py_err!(SerializationError, message),
        RustBadQuery::ValuesTooLongForKey(length, max_length) => {
            py_err!(py, ValuesTooLongForKey, message; length, max_length)
        }
        RustBadQuery::TooManyQueriesInBatchStatement(count) => {
            py_err!(py, TooManyStatementsInBatch, message; count)
        }
        _ => unreachable!("clippy testifies that the match is exhaustive"),
    }
}
