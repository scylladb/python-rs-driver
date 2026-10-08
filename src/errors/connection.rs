//! Maps why a connection pool is broken, i.e. why the last connection to the node failed.
// TODO: drop once PyO3 ships PyO3/pyo3#6309 (from_py_object clones Copy types)
#![allow(clippy::clone_on_copy)]

use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;
use scylla::errors::{
    BrokenConnectionError, BrokenConnectionErrorKind, ConnectionError, ConnectionSetupRequestError,
    ConnectionSetupRequestErrorKind, DbError, TranslationError,
};

use crate::errors::{
    AddressTranslationFailed, BrokenConnection, ConnectTimeout, ConnectionAuthenticationFailed,
    ConnectionLost, ConnectionPoolBroken, ConnectionSetupFailed, ToPyAttr,
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
        ConnectionError::TranslationError(e) => translation_error_to_pyerr(py, e, message),
        ConnectionError::BrokenConnection(e) => connection_lost_to_pyerr(py, e, message),
        ConnectionError::ConnectionSetupRequestError(e) => {
            setup_request_error_to_pyerr(py, e, message)
        }
        ConnectionError::IoError(e) => {
            let exc = py_err!(ConnectionPoolBroken, message);
            exc.set_cause(py, Some(io_error_to_pyerr(e)));
            exc
        }
        ConnectionError::NoSourcePortForShard(_) | ConnectionError::UseKeyspaceError(_) => {
            py_err!(ConnectionPoolBroken, message)
        }
        _ => unreachable!("clippy testifies that the match is exhaustive"),
    }
}

/// The `OSError` subclass for the error kind, e.g. `ConnectionRefusedError`; the driver shares the error, so it is rebuilt.
fn io_error_to_pyerr(err: &std::io::Error) -> PyErr {
    PyErr::from(std::io::Error::new(err.kind(), err.to_string()))
}

/// Keeps the exception raised by a Python `AddressTranslator`, or the I/O error, as the cause.
fn translation_error_to_pyerr(py: Python<'_>, err: &TranslationError, message: String) -> PyErr {
    let exc = py_err!(AddressTranslationFailed, message);
    let cause = match err {
        TranslationError::Custom(custom) => custom
            .downcast_ref::<PyErr>()
            .map(|cause| cause.clone_ref(py)),
        TranslationError::IoError(e) => Some(io_error_to_pyerr(e)),
        _ => None,
    };
    exc.set_cause(py, cause);
    exc
}

/// Why an established connection died.
#[pyclass(
    module = "scylla.errors",
    name = "BrokenConnectionKind",
    frozen,
    from_py_object,
    eq,
    eq_int
)]
#[derive(Debug, Copy, Clone, PartialEq)]
#[non_exhaustive]
pub enum PyBrokenConnectionKind {
    KeepaliveTimeout,
    KeepaliveFailed,
    WriteFailed,
    FrameParseFailed,
    UnexpectedStreamId,
    EventHandlingFailed,
    TooManyOrphanedStreams,
    ChannelClosed,
}

impl From<&BrokenConnectionErrorKind> for PyBrokenConnectionKind {
    #[deny(clippy::wildcard_enum_match_arm)]
    fn from(kind: &BrokenConnectionErrorKind) -> Self {
        match kind {
            BrokenConnectionErrorKind::KeepaliveTimeout(_) => Self::KeepaliveTimeout,
            BrokenConnectionErrorKind::KeepaliveRequestError(_) => Self::KeepaliveFailed,
            BrokenConnectionErrorKind::WriteError(_) => Self::WriteFailed,
            BrokenConnectionErrorKind::FrameHeaderParseError(_) => Self::FrameParseFailed,
            BrokenConnectionErrorKind::UnexpectedStreamId(_) => Self::UnexpectedStreamId,
            BrokenConnectionErrorKind::CqlEventHandlingError(_) => Self::EventHandlingFailed,
            BrokenConnectionErrorKind::TooManyOrphanedStreamIds(_) => Self::TooManyOrphanedStreams,
            BrokenConnectionErrorKind::ChannelError => Self::ChannelClosed,
            _ => unreachable!("clippy testifies that the match is exhaustive"),
        }
    }
}

impl ToPyAttr for PyBrokenConnectionKind {
    fn to_py_attr<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        (*self).into_bound_py_any(py)
    }
}

/// A connection died with a request on it, so the server possibly executed the request.
pub(crate) fn broken_connection_to_pyerr(
    py: Python<'_>,
    err: &BrokenConnectionError,
    message: String,
) -> PyErr {
    let kind = broken_connection_kind(err);
    py_err!(py, BrokenConnection, message; kind)
}

/// The last connection of a broken pool died, so nothing was sent.
fn connection_lost_to_pyerr(py: Python<'_>, err: &BrokenConnectionError, message: String) -> PyErr {
    let kind = broken_connection_kind(err);
    py_err!(py, ConnectionLost, message; kind)
}

/// Why an established connection died; the reason is reachable only by downcasting.
fn broken_connection_kind(err: &BrokenConnectionError) -> Option<PyBrokenConnectionKind> {
    err.downcast_ref::<BrokenConnectionErrorKind>()
        .map(PyBrokenConnectionKind::from)
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
        ConnectionSetupRequestErrorKind::BrokenConnection(e) => {
            connection_lost_to_pyerr(py, e, message)
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
            py_err!(py, ConnectionSetupFailed, message; request_kind, reason)
        }
        _ => unreachable!("clippy testifies that the match is exhaustive"),
    }
}
