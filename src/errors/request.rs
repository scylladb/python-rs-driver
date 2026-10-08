//! Maps Rust driver request errors onto the Python exception hierarchy.
//!
//! The Python class is picked from the innermost cause, so the same failure raises the same
//! class whichever operation hit it; the operation is only part of the message.

use pyo3::prelude::*;
use scylla::errors::{
    BadQuery as RustBadQuery, ConnectionPoolError as RustConnectionPoolError, DbError,
    ExecutionError as RustExecutionError, RequestAttemptError,
};

use crate::errors::execution::{DriverPrepareError, DriverUseKeyspaceError};
use crate::errors::{
    AlreadyExists, AuthenticationFailed, BrokenConnection, ConnectionBusy, ConnectionPoolBroken,
    CqlSyntaxError, FunctionFailure, InvalidRequest, IsBootstrapping, MetadataError,
    NoHostAvailable, NodeDisabledByHostFilter, NonfinishedPagingState, OperationTimedOut,
    Overloaded, PartitionKeyExtractionFailed, PoolInitializing, RateLimitReached, ReadFailure,
    ReadTimeout, RepreparedIdChanged, RepreparedIdMissingInBatch, RequestSerializationError,
    ResponseParseError, SchemaAgreementError, ServerConfigError, ServerError, ServerProtocolError,
    TooManyStatementsInBatch, TruncateError, Unauthorized, Unavailable, UnexpectedResponse,
    UnknownDatabaseError, Unprepared, ValuesTooLongForKey, WriteFailure, WriteTimeout, with_attrs,
};
use crate::serialize::error::serialization_error_to_pyerr;

/// Maps an `ExecutionError`; `message` is the full description including the failed operation.
#[deny(clippy::wildcard_enum_match_arm)]
pub(crate) fn execution_error_to_pyerr(err: &RustExecutionError, message: String) -> PyErr {
    match err {
        RustExecutionError::BadQuery(e) => bad_query_to_pyerr(e, message),
        RustExecutionError::EmptyPlan => py_err!(NoHostAvailable, message),
        RustExecutionError::PrepareError(e) => {
            DriverPrepareError::rust_driver_prepare_error(e.clone()).into()
        }
        RustExecutionError::ConnectionPoolError(e) => connection_pool_error_to_pyerr(e, message),
        RustExecutionError::LastAttemptError(e) => request_attempt_error_to_pyerr(e, message),
        RustExecutionError::RequestTimeout(timeout) => {
            py_err!(OperationTimedOut, message; timeout)
        }
        RustExecutionError::UseKeyspaceError(e) => DriverUseKeyspaceError::from(e.clone()).into(),
        RustExecutionError::SchemaAgreementError(_) => py_err!(SchemaAgreementError, message),
        RustExecutionError::MetadataError(_) => py_err!(MetadataError, message),
        _ => unreachable!("clippy testifies that the match is exhaustive"),
    }
}

#[deny(clippy::wildcard_enum_match_arm)]
fn bad_query_to_pyerr(err: &RustBadQuery, message: String) -> PyErr {
    match err {
        RustBadQuery::PartitionKeyExtraction => py_err!(PartitionKeyExtractionFailed, message),
        RustBadQuery::SerializationError(e) => serialization_error_to_pyerr(e, message),
        RustBadQuery::ValuesTooLongForKey(length, max_length) => {
            py_err!(ValuesTooLongForKey, message; length, max_length)
        }
        RustBadQuery::TooManyQueriesInBatchStatement(count) => {
            py_err!(TooManyStatementsInBatch, message; count)
        }
        _ => unreachable!("clippy testifies that the match is exhaustive"),
    }
}

#[deny(clippy::wildcard_enum_match_arm)]
fn connection_pool_error_to_pyerr(err: &RustConnectionPoolError, message: String) -> PyErr {
    match err {
        RustConnectionPoolError::Broken { .. } => py_err!(ConnectionPoolBroken, message),
        RustConnectionPoolError::Initializing => py_err!(PoolInitializing, message),
        RustConnectionPoolError::NodeDisabledByHostFilter => {
            py_err!(NodeDisabledByHostFilter, message)
        }
        _ => unreachable!("clippy testifies that the match is exhaustive"),
    }
}

#[deny(clippy::wildcard_enum_match_arm)]
pub(crate) fn request_attempt_error_to_pyerr(err: &RequestAttemptError, message: String) -> PyErr {
    match err {
        RequestAttemptError::SerializationError(e) => serialization_error_to_pyerr(e, message),
        RequestAttemptError::CqlRequestSerialization(_) => {
            py_err!(RequestSerializationError, message)
        }
        RequestAttemptError::UnableToAllocStreamId => py_err!(ConnectionBusy, message),
        RequestAttemptError::BrokenConnectionError(_) => py_err!(BrokenConnection, message),
        RequestAttemptError::BodyExtensionsParseError(_)
        | RequestAttemptError::CqlResultParseError(_)
        | RequestAttemptError::CqlErrorParseError(_) => py_err!(ResponseParseError, message),
        RequestAttemptError::DbError(error, reason) => db_error_to_pyerr(error, reason, message),
        RequestAttemptError::UnexpectedResponse(response_kind) => {
            py_err!(UnexpectedResponse, message; response_kind)
        }
        RequestAttemptError::RepreparedIdChanged {
            statement,
            expected_id,
            reprepared_id,
        } => py_err!(RepreparedIdChanged, message; statement, expected_id, reprepared_id),
        RequestAttemptError::RepreparedIdMissingInBatch => {
            py_err!(RepreparedIdMissingInBatch, message)
        }
        RequestAttemptError::NonfinishedPagingState => py_err!(NonfinishedPagingState, message),
        _ => unreachable!("clippy testifies that the match is exhaustive"),
    }
}

/// Maps a `DbError`; `reason` is the error message sent by the server.
#[deny(clippy::wildcard_enum_match_arm)]
fn db_error_to_pyerr(error: &DbError, reason: &str, message: String) -> PyErr {
    let err = match error {
        DbError::SyntaxError => py_err!(CqlSyntaxError, message),
        DbError::Invalid => py_err!(InvalidRequest, message),
        DbError::AlreadyExists { keyspace, table } => {
            py_err!(AlreadyExists, message; keyspace, table)
        }
        DbError::FunctionFailure {
            keyspace,
            function,
            arg_types,
        } => py_err!(FunctionFailure, message; keyspace, function, arg_types),
        DbError::AuthenticationError => py_err!(AuthenticationFailed, message),
        DbError::Unauthorized => py_err!(Unauthorized, message),
        DbError::ConfigError => py_err!(ServerConfigError, message),
        DbError::Unavailable {
            consistency,
            required,
            alive,
        } => py_err!(Unavailable, message; consistency, required, alive),
        DbError::Overloaded => py_err!(Overloaded, message),
        DbError::IsBootstrapping => py_err!(IsBootstrapping, message),
        DbError::TruncateError => py_err!(TruncateError, message),
        DbError::ReadTimeout {
            consistency,
            received,
            required,
            data_present,
        } => py_err!(ReadTimeout, message; consistency, received, required, data_present),
        DbError::WriteTimeout {
            consistency,
            received,
            required,
            write_type,
        } => py_err!(WriteTimeout, message; consistency, received, required, write_type),
        DbError::ReadFailure {
            consistency,
            received,
            required,
            numfailures,
            data_present,
        } => py_err!(ReadFailure, message;
            consistency, received, required, numfailures, data_present),
        DbError::WriteFailure {
            consistency,
            received,
            required,
            numfailures,
            write_type,
        } => py_err!(WriteFailure, message;
            consistency, received, required, numfailures, write_type),
        DbError::Unprepared { statement_id } => py_err!(Unprepared, message; statement_id),
        DbError::ServerError => py_err!(ServerError, message),
        DbError::ProtocolError => py_err!(ServerProtocolError, message),
        DbError::RateLimitReached {
            op_type,
            rejected_by_coordinator,
        } => py_err!(RateLimitReached, message; op_type, rejected_by_coordinator),
        DbError::Other(code) => py_err!(UnknownDatabaseError, message; code),
        _ => unreachable!("clippy testifies that the match is exhaustive"),
    };

    with_attrs(err, |exc| exc.setattr("reason", reason))
}
