//! Maps Rust driver request errors onto the Python exception hierarchy.
//!
//! The Python class is picked from the innermost cause, so the same failure raises the same
//! class whichever operation hit it; the operation is only part of the message.

use pyo3::prelude::*;
use scylla::errors::{
    BadQuery as RustBadQuery, ConnectionPoolError as RustConnectionPoolError,
    ExecutionError as RustExecutionError, RequestAttemptError,
};

use crate::errors::execution::{DriverPrepareError, DriverUseKeyspaceError};
use crate::errors::{
    BrokenConnection, ConnectionBusy, ConnectionPoolBroken, ConnectionPoolInitializing,
    MetadataError, NoHostAvailable, NodeDisabledByHostFilter, NonfinishedPagingState,
    OperationTimedOut, PartitionKeyExtractionFailed, RepreparedIdChanged,
    RepreparedIdMissingInBatch, RequestFailedError, RequestSerializationError, ResponseParseError,
    SchemaAgreementError, SerializationError, TooManyStatementsInBatch, UnexpectedResponse,
    ValuesTooLongForKey,
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
        RustExecutionError::ConnectionPoolError(e) => connection_pool_error_to_pyerr(e, message),
        RustExecutionError::LastAttemptError(e) => request_attempt_error_to_pyerr(py, e, message),
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

#[deny(clippy::wildcard_enum_match_arm)]
fn connection_pool_error_to_pyerr(err: &RustConnectionPoolError, message: String) -> PyErr {
    match err {
        RustConnectionPoolError::Broken { .. } => py_err!(ConnectionPoolBroken, message),
        RustConnectionPoolError::Initializing => py_err!(ConnectionPoolInitializing, message),
        RustConnectionPoolError::NodeDisabledByHostFilter => {
            py_err!(NodeDisabledByHostFilter, message)
        }
        _ => unreachable!("clippy testifies that the match is exhaustive"),
    }
}

#[deny(clippy::wildcard_enum_match_arm)]
pub(crate) fn request_attempt_error_to_pyerr(
    py: Python<'_>,
    err: &RequestAttemptError,
    message: String,
) -> PyErr {
    match err {
        RequestAttemptError::SerializationError(_) => py_err!(SerializationError, message),
        RequestAttemptError::CqlRequestSerialization(_) => {
            py_err!(RequestSerializationError, message)
        }
        RequestAttemptError::UnableToAllocStreamId => py_err!(ConnectionBusy, message),
        RequestAttemptError::BrokenConnectionError(_) => py_err!(BrokenConnection, message),
        RequestAttemptError::BodyExtensionsParseError(_)
        | RequestAttemptError::CqlResultParseError(_)
        | RequestAttemptError::CqlErrorParseError(_) => py_err!(ResponseParseError, message),
        RequestAttemptError::DbError(..) => py_err!(RequestFailedError, message),
        RequestAttemptError::UnexpectedResponse(response_kind) => {
            py_err!(py, UnexpectedResponse, message; response_kind)
        }
        RequestAttemptError::RepreparedIdChanged {
            statement,
            expected_id,
            reprepared_id,
        } => py_err!(py, RepreparedIdChanged, message; statement, expected_id, reprepared_id),
        RequestAttemptError::RepreparedIdMissingInBatch => {
            py_err!(RepreparedIdMissingInBatch, message)
        }
        RequestAttemptError::NonfinishedPagingState => py_err!(NonfinishedPagingState, message),
        _ => unreachable!("clippy testifies that the match is exhaustive"),
    }
}
