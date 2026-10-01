//! Maps why a connection pool is broken, i.e. why the last connection to the node failed.

use pyo3::prelude::*;
use scylla::errors::{
    ConnectionError, ConnectionSetupRequestError, ConnectionSetupRequestErrorKind, DbError,
};

use crate::errors::{
    AddressTranslationFailed, ConnectTimeout, ConnectionAuthenticationFailed, ConnectionLost,
    ConnectionPoolBroken, ConnectionSetupFailed,
};

/// Maps `last_connection_error` of a broken pool.
#[deny(clippy::wildcard_enum_match_arm)]
pub(crate) fn pool_broken_to_pyerr(
    py: Python<'_>,
    err: &ConnectionError,
    message: String,
) -> PyErr {
    match err {
        ConnectionError::ConnectTimeout => py_err!(ConnectTimeout, message),
        ConnectionError::TranslationError(_) => py_err!(AddressTranslationFailed, message),
        ConnectionError::BrokenConnection(_) => py_err!(ConnectionLost, message),
        ConnectionError::ConnectionSetupRequestError(e) => {
            setup_request_error_to_pyerr(py, e, message)
        }
        ConnectionError::IoError(_) | ConnectionError::NoSourcePortForShard(_) => {
            py_err!(ConnectionPoolBroken, message)
        }
        _ => unreachable!("clippy testifies that the match is exhaustive"),
    }
}

/// A request the driver sends while opening a connection (STARTUP, AUTH_RESPONSE, ...) failed.
#[deny(clippy::wildcard_enum_match_arm)]
fn setup_request_error_to_pyerr(
    py: Python<'_>,
    err: &ConnectionSetupRequestError,
    message: String,
) -> PyErr {
    let request_kind = err.request_kind;
    match &err.error {
        ConnectionSetupRequestErrorKind::DbError(DbError::AuthenticationError, reason) => {
            let reason = Some(reason);
            py_err!(py, ConnectionAuthenticationFailed, message; request_kind, reason)
        }
        ConnectionSetupRequestErrorKind::MissingAuthentication
        | ConnectionSetupRequestErrorKind::StartAuthSessionError(_)
        | ConnectionSetupRequestErrorKind::AuthChallengeEvaluationError(_)
        | ConnectionSetupRequestErrorKind::AuthFinishError(_) => {
            let reason: Option<&String> = None;
            py_err!(py, ConnectionAuthenticationFailed, message; request_kind, reason)
        }
        ConnectionSetupRequestErrorKind::DbError(_, reason) => {
            let reason = Some(reason);
            py_err!(py, ConnectionSetupFailed, message; request_kind, reason)
        }
        ConnectionSetupRequestErrorKind::BrokenConnection(_) => py_err!(ConnectionLost, message),
        ConnectionSetupRequestErrorKind::CqlRequestSerialization(_)
        | ConnectionSetupRequestErrorKind::BodyExtensionsParseError(_)
        | ConnectionSetupRequestErrorKind::UnableToAllocStreamId
        | ConnectionSetupRequestErrorKind::UnexpectedResponse(_)
        | ConnectionSetupRequestErrorKind::CqlSupportedParseError(_)
        | ConnectionSetupRequestErrorKind::CqlAuthenticateParseError(_)
        | ConnectionSetupRequestErrorKind::CqlAuthSuccessParseError(_)
        | ConnectionSetupRequestErrorKind::CqlAuthChallengeParseError(_)
        | ConnectionSetupRequestErrorKind::CqlErrorParseError(_) => {
            let reason: Option<&String> = None;
            py_err!(py, ConnectionSetupFailed, message; request_kind, reason)
        }
        _ => unreachable!("clippy testifies that the match is exhaustive"),
    }
}
