//! Maps why a connection pool is broken, i.e. why the last connection to the node failed.

use pyo3::prelude::*;
use scylla::errors::{
    BrokenConnectionError, BrokenConnectionErrorKind, ConnectionError, ConnectionSetupRequestError,
    ConnectionSetupRequestErrorKind, DbError,
};

use crate::errors::{
    AddressTranslationFailed, ConnectTimeout, ConnectionAuthenticationFailed, ConnectionLost,
    ConnectionPoolBroken, ConnectionSetupFailed, KeepaliveTimeout,
};

/// Maps `last_connection_error` of a broken pool.
#[deny(clippy::wildcard_enum_match_arm)]
pub(crate) fn pool_broken_to_pyerr(err: &ConnectionError, message: String) -> PyErr {
    match err {
        ConnectionError::ConnectTimeout => py_err!(ConnectTimeout, message),
        ConnectionError::TranslationError(_) => py_err!(AddressTranslationFailed, message),
        ConnectionError::BrokenConnection(e) => broken_connection_to_pyerr(e, message),
        ConnectionError::ConnectionSetupRequestError(e) => setup_request_error_to_pyerr(e, message),
        ConnectionError::IoError(_) | ConnectionError::NoSourcePortForShard(_) => {
            py_err!(ConnectionPoolBroken, message)
        }
        _ => unreachable!("clippy testifies that the match is exhaustive"),
    }
}

/// An established connection died; the reason is reachable only by downcasting.
fn broken_connection_to_pyerr(err: &BrokenConnectionError, message: String) -> PyErr {
    match err.downcast_ref::<BrokenConnectionErrorKind>() {
        Some(kind) => broken_connection_kind_to_pyerr(kind, message),
        None => py_err!(ConnectionLost, message),
    }
}

#[deny(clippy::wildcard_enum_match_arm)]
fn broken_connection_kind_to_pyerr(kind: &BrokenConnectionErrorKind, message: String) -> PyErr {
    match kind {
        BrokenConnectionErrorKind::KeepaliveTimeout(address) => {
            py_err!(KeepaliveTimeout, message; address)
        }
        BrokenConnectionErrorKind::KeepaliveRequestError(_)
        | BrokenConnectionErrorKind::FrameHeaderParseError(_)
        | BrokenConnectionErrorKind::CqlEventHandlingError(_)
        | BrokenConnectionErrorKind::UnexpectedStreamId(_)
        | BrokenConnectionErrorKind::WriteError(_)
        | BrokenConnectionErrorKind::TooManyOrphanedStreamIds(_)
        | BrokenConnectionErrorKind::ChannelError => py_err!(ConnectionLost, message),
        _ => unreachable!("clippy testifies that the match is exhaustive"),
    }
}

/// A request the driver sends while opening a connection (STARTUP, AUTH_RESPONSE, ...) failed.
#[deny(clippy::wildcard_enum_match_arm)]
fn setup_request_error_to_pyerr(err: &ConnectionSetupRequestError, message: String) -> PyErr {
    let request_kind = err.request_kind;
    match &err.error {
        ConnectionSetupRequestErrorKind::DbError(DbError::AuthenticationError, reason) => {
            let reason = Some(reason);
            py_err!(ConnectionAuthenticationFailed, message; request_kind, reason)
        }
        ConnectionSetupRequestErrorKind::MissingAuthentication
        | ConnectionSetupRequestErrorKind::StartAuthSessionError(_)
        | ConnectionSetupRequestErrorKind::AuthChallengeEvaluationError(_)
        | ConnectionSetupRequestErrorKind::AuthFinishError(_) => {
            let reason: Option<&String> = None;
            py_err!(ConnectionAuthenticationFailed, message; request_kind, reason)
        }
        ConnectionSetupRequestErrorKind::DbError(_, reason) => {
            let reason = Some(reason);
            py_err!(ConnectionSetupFailed, message; request_kind, reason)
        }
        ConnectionSetupRequestErrorKind::BrokenConnection(e) => {
            broken_connection_to_pyerr(e, message)
        }
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
            py_err!(ConnectionSetupFailed, message; request_kind, reason)
        }
        _ => unreachable!("clippy testifies that the match is exhaustive"),
    }
}
