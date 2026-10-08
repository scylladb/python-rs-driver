//! Maps Rust driver request errors onto the Python exception hierarchy.
//!
//! The Python class is picked from the innermost cause, so the same failure raises the same
//! class whichever operation hit it; the operation is only part of the message.

use pyo3::prelude::*;
use scylla::errors::{
    BadQuery as RustBadQuery, ConnectionPoolError as RustConnectionPoolError, DbError,
    ExecutionError as RustExecutionError, MetadataError as RustMetadataError,
    MetadataFetchError as RustMetadataFetchError, MetadataFetchErrorKind,
    NewSessionError as RustNewSessionError, NextPageError, NextRowError,
    PrepareError as RustPrepareError, RequestAttemptError, RequestError as RustRequestError,
    SchemaAgreementError as RustSchemaAgreementError, TracingError as RustTracingError,
    UseKeyspaceError as RustUseKeyspaceError,
};

use crate::errors::connection::{broken_connection_to_pyerr, pool_broken_to_pyerr};
use crate::errors::{
    AlreadyExists, AuthenticationFailed, BadKeyspaceName, ConnectionBusy,
    ConnectionPoolInitializing, CqlSyntaxError, FunctionFailure, HostnameResolutionFailed,
    InvalidClusterMetadata, InvalidRequest, IsBootstrapping, KeyspaceNameMismatch,
    MetadataFetchFailed, NoHostAvailable, NoKnownNodes, NodeDisabledByHostFilter,
    NonfinishedPagingState, OperationTimedOut, Overloaded, PartitionKeyExtractionFailed,
    PreparedStatementIdsMismatch, RateLimitReached, ReadFailure, ReadTimeout, RepreparedIdChanged,
    RepreparedIdMissingInBatch, RequestSerializationError, RequiredHostAbsent, ResponseParseError,
    SchemaAgreementTimeout, ServerConfigError, ServerError, ServerProtocolError,
    SessionConfigError, TooManyStatementsInBatch, TraceUnavailable, TruncateError, Unauthorized,
    Unavailable, UnexpectedResponse, UnknownDatabaseError, Unprepared, ValuesTooLongForKey,
    WriteFailure, WriteTimeout, with_attrs,
};
use crate::serialize::error::serialization_error_to_pyerr;

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
        RustExecutionError::PrepareError(e) => prepare_error_to_pyerr(py, e, message),
        RustExecutionError::ConnectionPoolError(e) => {
            connection_pool_error_to_pyerr(py, e, message)
        }
        RustExecutionError::LastAttemptError(e) => request_attempt_error_to_pyerr(py, e, message),
        RustExecutionError::RequestTimeout(timeout) => {
            py_err!(py, OperationTimedOut, message; timeout)
        }
        RustExecutionError::UseKeyspaceError(e) => use_keyspace_error_to_pyerr(py, e, message),
        RustExecutionError::SchemaAgreementError(e) => {
            schema_agreement_error_to_pyerr(py, e, message)
        }
        RustExecutionError::MetadataError(e) => metadata_error_to_pyerr(py, e, message),
        _ => unreachable!("clippy testifies that the match is exhaustive"),
    }
}

/// Maps a failure to create a session; failures of the initial metadata fetch or `USE` keep their own class.
#[deny(clippy::wildcard_enum_match_arm)]
pub(crate) fn new_session_error_to_pyerr(
    py: Python<'_>,
    err: &RustNewSessionError,
    message: String,
) -> PyErr {
    match err {
        RustNewSessionError::FailedToResolveAnyHostname(hostnames) => {
            py_err!(py, HostnameResolutionFailed, message; hostnames)
        }
        RustNewSessionError::EmptyKnownNodesList => py_err!(NoKnownNodes, message),
        RustNewSessionError::MetadataError(e) => metadata_error_to_pyerr(py, e, message),
        RustNewSessionError::UseKeyspaceError(e) => use_keyspace_error_to_pyerr(py, e, message),
        RustNewSessionError::IllegalConfig(_) => py_err!(SessionConfigError, message),
        _ => unreachable!("clippy testifies that the match is exhaustive"),
    }
}

/// Maps a query trace fetch error; failures of the `system_traces` queries keep their own class.
#[deny(clippy::wildcard_enum_match_arm)]
pub(crate) fn tracing_error_to_pyerr(
    py: Python<'_>,
    err: &RustTracingError,
    message: String,
) -> PyErr {
    match err {
        RustTracingError::ExecutionError(e) => execution_error_to_pyerr(py, e, message),
        RustTracingError::PrepareError(e) => prepare_error_to_pyerr(py, e, message),
        RustTracingError::TracesSessionIntoRowsResultError(_)
        | RustTracingError::TracesSessionInvalidColumnType(_)
        | RustTracingError::TracesSessionDeserializationFailed(_)
        | RustTracingError::TracesEventsIntoRowsResultError(_)
        | RustTracingError::TracesEventsInvalidColumnType(_)
        | RustTracingError::TracesEventsDeserializationFailed(_) => {
            py_err!(ResponseParseError, message)
        }
        RustTracingError::EmptyResults => py_err!(TraceUnavailable, message),
        _ => unreachable!("clippy testifies that the match is exhaustive"),
    }
}

/// Maps a cluster metadata fetch error; request failures inside the fetch keep their own class.
#[deny(clippy::wildcard_enum_match_arm)]
pub(crate) fn metadata_error_to_pyerr(
    py: Python<'_>,
    err: &RustMetadataError,
    message: String,
) -> PyErr {
    match err {
        RustMetadataError::ConnectionPoolError(e) => connection_pool_error_to_pyerr(py, e, message),
        RustMetadataError::FetchError(e) => metadata_fetch_error_to_pyerr(py, e, message),
        RustMetadataError::Peers(_)
        | RustMetadataError::Keyspaces(_)
        | RustMetadataError::Udts(_)
        | RustMetadataError::Tables(_)
        | RustMetadataError::ClientRoutes(_) => py_err!(InvalidClusterMetadata, message),
        _ => unreachable!("clippy testifies that the match is exhaustive"),
    }
}

#[deny(clippy::wildcard_enum_match_arm)]
fn metadata_fetch_error_to_pyerr(
    py: Python<'_>,
    err: &RustMetadataFetchError,
    message: String,
) -> PyErr {
    let table = err.table;
    match &err.error {
        MetadataFetchErrorKind::PrepareError(e) => request_attempt_error_to_pyerr(py, e, message),
        MetadataFetchErrorKind::SerializationError(e) => {
            serialization_error_to_pyerr(py, e, message)
        }
        MetadataFetchErrorKind::NextRowError(NextRowError::NextPageError(
            NextPageError::RequestFailure(e),
        )) => request_error_to_pyerr(py, e, message),
        MetadataFetchErrorKind::NextRowError(_) | MetadataFetchErrorKind::InvalidColumnType(_) => {
            py_err!(py, MetadataFetchFailed, message; table)
        }
        _ => unreachable!("clippy testifies that the match is exhaustive"),
    }
}

/// Maps a `RequestError`, the failure of a single request sent through the pager.
#[deny(clippy::wildcard_enum_match_arm)]
fn request_error_to_pyerr(py: Python<'_>, err: &RustRequestError, message: String) -> PyErr {
    match err {
        RustRequestError::EmptyPlan => py_err!(NoHostAvailable, message),
        RustRequestError::ConnectionPoolError(e) => connection_pool_error_to_pyerr(py, e, message),
        RustRequestError::RequestTimeout(timeout) => {
            py_err!(py, OperationTimedOut, message; timeout)
        }
        RustRequestError::LastAttemptError(e) => request_attempt_error_to_pyerr(py, e, message),
        _ => unreachable!("clippy testifies that the match is exhaustive"),
    }
}

#[deny(clippy::wildcard_enum_match_arm)]
pub(crate) fn schema_agreement_error_to_pyerr(
    py: Python<'_>,
    err: &RustSchemaAgreementError,
    message: String,
) -> PyErr {
    match err {
        RustSchemaAgreementError::ConnectionPoolError(e) => {
            connection_pool_error_to_pyerr(py, e, message)
        }
        RustSchemaAgreementError::PrepareError(e) => prepare_error_to_pyerr(py, e, message),
        RustSchemaAgreementError::RequestError(e) => request_attempt_error_to_pyerr(py, e, message),
        RustSchemaAgreementError::TracesEventsIntoRowsResultError(_)
        | RustSchemaAgreementError::SingleRowError(_) => py_err!(ResponseParseError, message),
        RustSchemaAgreementError::Timeout(timeout) => {
            py_err!(py, SchemaAgreementTimeout, message; timeout)
        }
        RustSchemaAgreementError::RequiredHostAbsent(host_id) => {
            py_err!(py, RequiredHostAbsent, message; host_id)
        }
        _ => unreachable!("clippy testifies that the match is exhaustive"),
    }
}

#[deny(clippy::wildcard_enum_match_arm)]
pub(crate) fn prepare_error_to_pyerr(
    py: Python<'_>,
    err: &RustPrepareError,
    message: String,
) -> PyErr {
    match err {
        RustPrepareError::ConnectionPoolError(e) => connection_pool_error_to_pyerr(py, e, message),
        RustPrepareError::AllAttemptsFailed { first_attempt } => {
            request_attempt_error_to_pyerr(py, first_attempt, message)
        }
        RustPrepareError::PreparedStatementIdsMismatch => {
            py_err!(PreparedStatementIdsMismatch, message)
        }
        _ => unreachable!("clippy testifies that the match is exhaustive"),
    }
}

#[deny(clippy::wildcard_enum_match_arm)]
pub(crate) fn use_keyspace_error_to_pyerr(
    py: Python<'_>,
    err: &RustUseKeyspaceError,
    message: String,
) -> PyErr {
    match err {
        RustUseKeyspaceError::BadKeyspaceName(_) => py_err!(BadKeyspaceName, message),
        RustUseKeyspaceError::RequestError(e) => request_attempt_error_to_pyerr(py, e, message),
        RustUseKeyspaceError::KeyspaceNameMismatch {
            expected_keyspace_name_lowercase: expected,
            result_keyspace_name_lowercase: received,
        } => py_err!(py, KeyspaceNameMismatch, message; expected, received),
        RustUseKeyspaceError::RequestTimeout(timeout) => {
            py_err!(py, OperationTimedOut, message; timeout)
        }
        _ => unreachable!("clippy testifies that the match is exhaustive"),
    }
}

#[deny(clippy::wildcard_enum_match_arm)]
fn bad_query_to_pyerr(py: Python<'_>, err: &RustBadQuery, message: String) -> PyErr {
    match err {
        RustBadQuery::PartitionKeyExtraction => py_err!(PartitionKeyExtractionFailed, message),
        RustBadQuery::SerializationError(e) => serialization_error_to_pyerr(py, e, message),
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
fn connection_pool_error_to_pyerr(
    py: Python<'_>,
    err: &RustConnectionPoolError,
    message: String,
) -> PyErr {
    match err {
        RustConnectionPoolError::Broken {
            last_connection_error,
        } => pool_broken_to_pyerr(py, last_connection_error, message),
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
        RequestAttemptError::SerializationError(e) => serialization_error_to_pyerr(py, e, message),
        RequestAttemptError::CqlRequestSerialization(_) => {
            py_err!(RequestSerializationError, message)
        }
        RequestAttemptError::UnableToAllocStreamId => py_err!(ConnectionBusy, message),
        RequestAttemptError::BrokenConnectionError(e) => broken_connection_to_pyerr(py, e, message),
        RequestAttemptError::BodyExtensionsParseError(_)
        | RequestAttemptError::CqlResultParseError(_)
        | RequestAttemptError::CqlErrorParseError(_) => py_err!(ResponseParseError, message),
        RequestAttemptError::DbError(error, reason) => {
            db_error_to_pyerr(py, error, reason, message)
        }
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

/// Maps a `DbError`; `reason` is the error message sent by the server.
#[deny(clippy::wildcard_enum_match_arm)]
pub(crate) fn db_error_to_pyerr(
    py: Python<'_>,
    error: &DbError,
    reason: &str,
    message: String,
) -> PyErr {
    let err = match error {
        DbError::SyntaxError => py_err!(CqlSyntaxError, message),
        DbError::Invalid => py_err!(InvalidRequest, message),
        DbError::AlreadyExists { keyspace, table } => {
            py_err!(py, AlreadyExists, message; keyspace, table)
        }
        DbError::FunctionFailure {
            keyspace,
            function,
            arg_types,
        } => py_err!(py, FunctionFailure, message; keyspace, function, arg_types),
        DbError::AuthenticationError => py_err!(AuthenticationFailed, message),
        DbError::Unauthorized => py_err!(Unauthorized, message),
        DbError::ConfigError => py_err!(ServerConfigError, message),
        DbError::Unavailable {
            consistency,
            required,
            alive,
        } => py_err!(py, Unavailable, message; consistency, required, alive),
        DbError::Overloaded => py_err!(Overloaded, message),
        DbError::IsBootstrapping => py_err!(IsBootstrapping, message),
        DbError::TruncateError => py_err!(TruncateError, message),
        DbError::ReadTimeout {
            consistency,
            received,
            required,
            data_present,
        } => py_err!(py, ReadTimeout, message; consistency, received, required, data_present),
        DbError::WriteTimeout {
            consistency,
            received,
            required,
            write_type,
        } => py_err!(py, WriteTimeout, message; consistency, received, required, write_type),
        DbError::ReadFailure {
            consistency,
            received,
            required,
            numfailures,
            data_present,
        } => py_err!(py, ReadFailure, message;
            consistency, received, required, numfailures, data_present),
        DbError::WriteFailure {
            consistency,
            received,
            required,
            numfailures,
            write_type,
        } => py_err!(py, WriteFailure, message;
            consistency, received, required, numfailures, write_type),
        DbError::Unprepared { statement_id } => py_err!(py, Unprepared, message; statement_id),
        DbError::ServerError => py_err!(ServerError, message),
        DbError::ProtocolError => py_err!(ServerProtocolError, message),
        DbError::RateLimitReached {
            op_type,
            rejected_by_coordinator,
        } => py_err!(py, RateLimitReached, message; op_type, rejected_by_coordinator),
        DbError::Other(code) => py_err!(py, UnknownDatabaseError, message; code),
        _ => unreachable!("clippy testifies that the match is exhaustive"),
    };

    with_attrs(py, err, &[("reason", &reason)])
}
