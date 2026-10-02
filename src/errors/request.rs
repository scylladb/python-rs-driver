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
    SchemaAgreementError as RustSchemaAgreementError, UseKeyspaceError as RustUseKeyspaceError,
};

use crate::errors::connection::pool_broken_to_pyerr;
use crate::errors::{
    AlreadyExists, AuthenticationFailed, BadKeyspaceName, BrokenConnection, ConnectionBusy,
    CqlSyntaxError, FunctionFailure, HostnameResolutionFailed, InvalidClusterMetadata,
    InvalidRequest, IsBootstrapping, KeyspaceNameMismatch, MetadataFetchFailed, NoHostAvailable,
    NoKnownNodes, NodeDisabledByHostFilter, NonfinishedPagingState, OperationTimedOut, Overloaded,
    PartitionKeyExtractionFailed, PoolInitializing, PreparedStatementIdsMismatch, RateLimitReached,
    ReadFailure, ReadTimeout, RepreparedIdChanged, RepreparedIdMissingInBatch,
    RequestSerializationError, RequiredHostAbsent, ResponseParseError, SchemaAgreementError,
    SchemaAgreementTimeout, ServerConfigError, ServerError, ServerProtocolError,
    SessionConfigError, TooManyStatementsInBatch, TruncateError, Unauthorized, Unavailable,
    UnexpectedResponse, UnknownDatabaseError, Unprepared, ValuesTooLongForKey, WriteFailure,
    WriteTimeout, with_attrs,
};
use crate::serialize::error::serialization_error_to_pyerr;

/// Maps an `ExecutionError`; `message` is the full description including the failed operation.
#[deny(clippy::wildcard_enum_match_arm)]
pub(crate) fn execution_error_to_pyerr(err: &RustExecutionError, message: String) -> PyErr {
    match err {
        RustExecutionError::BadQuery(e) => bad_query_to_pyerr(e, message),
        RustExecutionError::EmptyPlan => py_err!(NoHostAvailable, message),
        RustExecutionError::PrepareError(e) => prepare_error_to_pyerr(e, message),
        RustExecutionError::ConnectionPoolError(e) => connection_pool_error_to_pyerr(e, message),
        RustExecutionError::LastAttemptError(e) => request_attempt_error_to_pyerr(e, message),
        RustExecutionError::RequestTimeout(timeout) => {
            py_err!(OperationTimedOut, message; timeout)
        }
        RustExecutionError::UseKeyspaceError(e) => use_keyspace_error_to_pyerr(e, message),
        RustExecutionError::SchemaAgreementError(e) => schema_agreement_error_to_pyerr(e, message),
        RustExecutionError::MetadataError(e) => metadata_error_to_pyerr(e, message),
        _ => unreachable!("clippy testifies that the match is exhaustive"),
    }
}

/// Maps a failure to create a session; failures of the initial metadata fetch or `USE` keep their own class.
#[deny(clippy::wildcard_enum_match_arm)]
pub(crate) fn new_session_error_to_pyerr(err: &RustNewSessionError, message: String) -> PyErr {
    match err {
        RustNewSessionError::FailedToResolveAnyHostname(hostnames) => {
            py_err!(HostnameResolutionFailed, message; hostnames)
        }
        RustNewSessionError::EmptyKnownNodesList => py_err!(NoKnownNodes, message),
        RustNewSessionError::MetadataError(e) => metadata_error_to_pyerr(e, message),
        RustNewSessionError::UseKeyspaceError(e) => use_keyspace_error_to_pyerr(e, message),
        RustNewSessionError::IllegalConfig(_) => py_err!(SessionConfigError, message),
        _ => unreachable!("clippy testifies that the match is exhaustive"),
    }
}

/// Maps a cluster metadata fetch error; request failures inside the fetch keep their own class.
#[deny(clippy::wildcard_enum_match_arm)]
pub(crate) fn metadata_error_to_pyerr(err: &RustMetadataError, message: String) -> PyErr {
    match err {
        RustMetadataError::ConnectionPoolError(e) => connection_pool_error_to_pyerr(e, message),
        RustMetadataError::FetchError(e) => metadata_fetch_error_to_pyerr(e, message),
        RustMetadataError::Peers(_)
        | RustMetadataError::Keyspaces(_)
        | RustMetadataError::Udts(_)
        | RustMetadataError::Tables(_)
        | RustMetadataError::ClientRoutes(_) => py_err!(InvalidClusterMetadata, message),
        _ => unreachable!("clippy testifies that the match is exhaustive"),
    }
}

#[deny(clippy::wildcard_enum_match_arm)]
fn metadata_fetch_error_to_pyerr(err: &RustMetadataFetchError, message: String) -> PyErr {
    let table = err.table;
    match &err.error {
        MetadataFetchErrorKind::PrepareError(e) => request_attempt_error_to_pyerr(e, message),
        MetadataFetchErrorKind::SerializationError(e) => serialization_error_to_pyerr(e, message),
        MetadataFetchErrorKind::NextRowError(NextRowError::NextPageError(
            NextPageError::RequestFailure(e),
        )) => request_error_to_pyerr(e, message),
        MetadataFetchErrorKind::NextRowError(_) | MetadataFetchErrorKind::InvalidColumnType(_) => {
            py_err!(MetadataFetchFailed, message; table)
        }
        _ => unreachable!("clippy testifies that the match is exhaustive"),
    }
}

/// Maps a `RequestError`, the failure of a single request sent through the pager.
#[deny(clippy::wildcard_enum_match_arm)]
fn request_error_to_pyerr(err: &RustRequestError, message: String) -> PyErr {
    match err {
        RustRequestError::EmptyPlan => py_err!(NoHostAvailable, message),
        RustRequestError::ConnectionPoolError(e) => connection_pool_error_to_pyerr(e, message),
        RustRequestError::RequestTimeout(timeout) => py_err!(OperationTimedOut, message; timeout),
        RustRequestError::LastAttemptError(e) => request_attempt_error_to_pyerr(e, message),
        _ => unreachable!("clippy testifies that the match is exhaustive"),
    }
}

#[deny(clippy::wildcard_enum_match_arm)]
pub(crate) fn schema_agreement_error_to_pyerr(
    err: &RustSchemaAgreementError,
    message: String,
) -> PyErr {
    match err {
        RustSchemaAgreementError::ConnectionPoolError(e) => {
            connection_pool_error_to_pyerr(e, message)
        }
        RustSchemaAgreementError::PrepareError(e) => prepare_error_to_pyerr(e, message),
        RustSchemaAgreementError::RequestError(e) => request_attempt_error_to_pyerr(e, message),
        RustSchemaAgreementError::TracesEventsIntoRowsResultError(_)
        | RustSchemaAgreementError::SingleRowError(_) => py_err!(SchemaAgreementError, message),
        RustSchemaAgreementError::Timeout(timeout) => {
            py_err!(SchemaAgreementTimeout, message; timeout)
        }
        RustSchemaAgreementError::RequiredHostAbsent(host_id) => {
            py_err!(RequiredHostAbsent, message; host_id)
        }
        _ => unreachable!("clippy testifies that the match is exhaustive"),
    }
}

#[deny(clippy::wildcard_enum_match_arm)]
pub(crate) fn prepare_error_to_pyerr(err: &RustPrepareError, message: String) -> PyErr {
    match err {
        RustPrepareError::ConnectionPoolError(e) => connection_pool_error_to_pyerr(e, message),
        RustPrepareError::AllAttemptsFailed { first_attempt } => {
            request_attempt_error_to_pyerr(first_attempt, message)
        }
        RustPrepareError::PreparedStatementIdsMismatch => {
            py_err!(PreparedStatementIdsMismatch, message)
        }
        _ => unreachable!("clippy testifies that the match is exhaustive"),
    }
}

#[deny(clippy::wildcard_enum_match_arm)]
pub(crate) fn use_keyspace_error_to_pyerr(err: &RustUseKeyspaceError, message: String) -> PyErr {
    match err {
        RustUseKeyspaceError::BadKeyspaceName(_) => py_err!(BadKeyspaceName, message),
        RustUseKeyspaceError::RequestError(e) => request_attempt_error_to_pyerr(e, message),
        RustUseKeyspaceError::KeyspaceNameMismatch {
            expected_keyspace_name_lowercase: expected,
            result_keyspace_name_lowercase: received,
        } => py_err!(KeyspaceNameMismatch, message; expected, received),
        RustUseKeyspaceError::RequestTimeout(timeout) => {
            py_err!(OperationTimedOut, message; timeout)
        }
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
        RustConnectionPoolError::Broken {
            last_connection_error,
        } => pool_broken_to_pyerr(last_connection_error, message),
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
