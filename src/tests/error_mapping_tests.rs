use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::Duration;

use pyo3::exceptions::{PyConnectionRefusedError, PyKeyError, PyTimeoutError};
use pyo3::prelude::*;
use scylla::deserialize::TypeCheckError;
use scylla::errors::{
    BrokenConnectionErrorKind, ConnectionError, ConnectionPoolError,
    ConnectionSetupRequestErrorKind, CqlEventHandlingError, CqlRequestKind,
    CqlRequestSerializationError, CqlResponseKind, CustomTranslationError, DbError, ExecutionError,
    FrameHeaderParseError, RequestAttemptError, TracingError, TranslationError, UseKeyspaceError,
};
use scylla::serialize::SerializationError;
use scylla::statement::Consistency;
use scylla_cql_core::frame::frame_errors::PrepareSerializationError;

use crate::errors::connection::setup_request_error_to_pyerr;
use crate::errors::request::{execution_error_to_pyerr, tracing_error_to_pyerr};
use crate::errors::types::PyCqlRequestKind;
use crate::errors::{
    AddressTranslationFailed, AlreadyExists, AuthenticationFailed, BadQuery, BrokenConnection,
    BrokenConnectionInFlight, BrokenConnectionInPool, ConnectTimeout,
    ConnectionAuthenticationFailed, ConnectionBusy, ConnectionPoolBroken,
    ConnectionPoolError as PyConnectionPoolError, ConnectionSetupFailed, DatabaseError,
    OperationTimedOut, PyBrokenConnectionKind, PyConnectionSetupErrorKind, ReadTimeout,
    RequestEncodingFailed, RequestFailedError, ResponseParseError, SerializeFailedError,
    TraceUnavailable, Unavailable,
};
use crate::serialize::error::serialization_error_to_pyerr;

fn db_error(py: Python<'_>, error: DbError) -> PyErr {
    let err = ExecutionError::LastAttemptError(RequestAttemptError::DbError(
        error,
        "server reason".to_string(),
    ));
    execution_error_to_pyerr(py, &err, err.to_string())
}

fn attr<'py, T: FromPyObjectOwned<'py>>(py: Python<'py>, err: &PyErr, name: &str) -> T {
    let value = err.value(py).getattr(name).unwrap();
    value.extract().map_err(Into::<PyErr>::into).unwrap()
}

#[test]
fn unavailable_keeps_counts_and_reason() {
    Python::initialize();
    Python::attach(|py| {
        let err = db_error(
            py,
            DbError::Unavailable {
                consistency: Consistency::Three,
                required: 3,
                alive: 1,
            },
        );
        assert!(err.is_instance_of::<Unavailable>(py));
        assert!(err.is_instance_of::<DatabaseError>(py));
        assert_eq!(attr::<i32>(py, &err, "required"), 3);
        assert_eq!(attr::<i32>(py, &err, "alive"), 1);
        assert_eq!(attr::<String>(py, &err, "reason"), "server reason");
    });
}

#[test]
fn read_timeout_keeps_received_and_required_apart() {
    Python::initialize();
    Python::attach(|py| {
        let err = db_error(
            py,
            DbError::ReadTimeout {
                consistency: Consistency::Quorum,
                received: 1,
                required: 2,
                data_present: true,
            },
        );
        assert!(err.is_instance_of::<ReadTimeout>(py));
        assert_eq!(attr::<i32>(py, &err, "received"), 1);
        assert_eq!(attr::<i32>(py, &err, "required"), 2);
        assert!(attr::<bool>(py, &err, "data_present"));
        // Server timeouts are not client timers.
        assert!(!err.is_instance_of::<PyTimeoutError>(py));
    });
}

#[test]
fn already_exists_keeps_keyspace_and_table() {
    Python::initialize();
    Python::attach(|py| {
        let err = db_error(
            py,
            DbError::AlreadyExists {
                keyspace: "ks".to_string(),
                table: "tbl".to_string(),
            },
        );
        assert!(err.is_instance_of::<AlreadyExists>(py));
        assert_eq!(attr::<String>(py, &err, "keyspace"), "ks");
        assert_eq!(attr::<String>(py, &err, "table"), "tbl");
    });
}

#[test]
fn request_that_cannot_be_encoded_is_bad_query() {
    Python::initialize();
    let too_long = u16::try_from(usize::MAX).unwrap_err();
    let err = ExecutionError::LastAttemptError(RequestAttemptError::CqlRequestSerialization(
        CqlRequestSerializationError::PrepareSerialization(
            PrepareSerializationError::StatementStringSerialization(too_long),
        ),
    ));
    Python::attach(|py| {
        let err = execution_error_to_pyerr(py, &err, err.to_string());
        assert!(err.is_instance_of::<RequestEncodingFailed>(py));
        assert!(err.is_instance_of::<BadQuery>(py));
        // Nothing was sent, so it is not a `RequestFailedError`.
        assert!(!err.is_instance_of::<RequestFailedError>(py));
    });
}

#[test]
fn request_timeout_is_a_timedelta() {
    Python::initialize();
    let err = ExecutionError::RequestTimeout(Duration::from_millis(1500));
    Python::attach(|py| {
        let err = execution_error_to_pyerr(py, &err, err.to_string());
        assert!(err.is_instance_of::<OperationTimedOut>(py));
        assert!(err.is_instance_of::<PyTimeoutError>(py));
        assert_eq!(
            attr::<Duration>(py, &err, "timeout"),
            Duration::from_millis(1500)
        );
    });
}

#[test]
fn failed_trace_query_raises_the_execute_class() {
    Python::initialize();
    let err = TracingError::ExecutionError(ExecutionError::RequestTimeout(Duration::from_secs(2)));
    Python::attach(|py| {
        let err = tracing_error_to_pyerr(py, &err, err.to_string());
        assert!(err.is_instance_of::<OperationTimedOut>(py));
        assert!(!err.is_instance_of::<TraceUnavailable>(py));
    });
}

#[test]
fn missing_trace_raises_trace_unavailable() {
    Python::initialize();
    let err = TracingError::EmptyResults;
    Python::attach(|py| {
        let err = tracing_error_to_pyerr(py, &err, err.to_string());
        assert!(err.is_instance_of::<TraceUnavailable>(py));
    });
}

#[test]
fn unreadable_trace_rows_raise_response_parse_error() {
    Python::initialize();
    let err = TracingError::TracesSessionInvalidColumnType(TypeCheckError::new(std::fmt::Error));
    Python::attach(|py| {
        let err = tracing_error_to_pyerr(py, &err, err.to_string());
        assert!(err.is_instance_of::<ResponseParseError>(py));
        // Retrying does not help, so it must not look like a trace that is not there yet.
        assert!(!err.is_instance_of::<TraceUnavailable>(py));
    });
}

#[test]
fn broken_pool_keepalive_timeout_is_broken_connection_in_pool() {
    Python::initialize();
    let address = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2));
    let err = ExecutionError::ConnectionPoolError(ConnectionPoolError::Broken {
        last_connection_error: ConnectionError::BrokenConnection(
            BrokenConnectionErrorKind::KeepaliveTimeout(address).into(),
        ),
    });
    Python::attach(|py| {
        let err = execution_error_to_pyerr(py, &err, err.to_string());
        assert!(err.is_instance_of::<BrokenConnectionInPool>(py));
        assert!(err.is_instance_of::<BrokenConnection>(py));
        assert!(!err.is_instance_of::<RequestFailedError>(py));
        // A dead connection is not a client timer.
        assert!(!err.is_instance_of::<PyTimeoutError>(py));
        assert_eq!(
            attr::<PyBrokenConnectionKind>(py, &err, "kind"),
            PyBrokenConnectionKind::KeepaliveTimeout
        );
    });
}

#[test]
fn request_keepalive_timeout_is_broken_connection() {
    Python::initialize();
    let address = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2));
    let err = ExecutionError::LastAttemptError(RequestAttemptError::BrokenConnectionError(
        BrokenConnectionErrorKind::KeepaliveTimeout(address).into(),
    ));
    Python::attach(|py| {
        let err = execution_error_to_pyerr(py, &err, err.to_string());
        assert!(err.is_instance_of::<BrokenConnectionInFlight>(py));
        assert!(err.is_instance_of::<BrokenConnection>(py));
        assert!(err.is_instance_of::<RequestFailedError>(py));
        assert!(!err.is_instance_of::<PyConnectionPoolError>(py));
        assert_eq!(
            attr::<PyBrokenConnectionKind>(py, &err, "kind"),
            PyBrokenConnectionKind::KeepaliveTimeout
        );
    });
}

#[test]
fn request_broken_connection_is_not_broken_pool() {
    Python::initialize();
    let err = ExecutionError::LastAttemptError(RequestAttemptError::BrokenConnectionError(
        BrokenConnectionErrorKind::ChannelError.into(),
    ));
    Python::attach(|py| {
        let err = execution_error_to_pyerr(py, &err, err.to_string());
        assert!(err.is_instance_of::<BrokenConnection>(py));
        assert!(!err.is_instance_of::<ConnectionPoolBroken>(py));
    });
}

#[test]
fn translator_exception_is_the_cause() {
    Python::initialize();
    let raised = PyKeyError::new_err("10.0.0.5");
    let err = ExecutionError::ConnectionPoolError(ConnectionPoolError::Broken {
        last_connection_error: ConnectionError::TranslationError(
            CustomTranslationError::new(raised).into(),
        ),
    });
    Python::attach(|py| {
        let err = execution_error_to_pyerr(py, &err, err.to_string());
        assert!(err.is_instance_of::<AddressTranslationFailed>(py));
        let cause = err.cause(py).expect("translator exception is the cause");
        assert!(cause.is_instance_of::<PyKeyError>(py));
    });
}

#[test]
fn refused_connection_is_the_cause_of_broken_pool() {
    Python::initialize();
    let refused = std::io::Error::from(std::io::ErrorKind::ConnectionRefused);
    let err = ExecutionError::ConnectionPoolError(ConnectionPoolError::Broken {
        last_connection_error: ConnectionError::IoError(Arc::new(refused)),
    });
    Python::attach(|py| {
        let err = execution_error_to_pyerr(py, &err, err.to_string());
        assert!(err.is_instance_of::<ConnectionPoolBroken>(py));
        let cause = err.cause(py).expect("I/O error is the cause");
        assert!(cause.is_instance_of::<PyConnectionRefusedError>(py));
    });
}

#[test]
fn os_error_cause_keeps_its_errno() {
    Python::initialize();
    Python::attach(|py| {
        let errno: i32 = py
            .import("errno")
            .and_then(|m| m.getattr("ECONNREFUSED"))
            .and_then(|v| v.extract())
            .unwrap();
        let refused = std::io::Error::from_raw_os_error(errno);
        let err = ExecutionError::ConnectionPoolError(ConnectionPoolError::Broken {
            last_connection_error: ConnectionError::IoError(Arc::new(refused)),
        });
        let err = execution_error_to_pyerr(py, &err, err.to_string());
        let cause = err.cause(py).expect("I/O error is the cause");
        assert!(cause.is_instance_of::<PyConnectionRefusedError>(py));
        assert_eq!(attr::<i32>(py, &cause, "errno"), errno);
    });
}

#[test]
fn failed_use_keyspace_is_the_cause_of_broken_pool() {
    Python::initialize();
    let timeout = UseKeyspaceError::RequestTimeout(Duration::from_secs(5));
    let err = ExecutionError::ConnectionPoolError(ConnectionPoolError::Broken {
        last_connection_error: ConnectionError::UseKeyspaceError(timeout),
    });
    Python::attach(|py| {
        let err = execution_error_to_pyerr(py, &err, err.to_string());
        assert!(err.is_instance_of::<ConnectionPoolBroken>(py));
        let cause = err.cause(py).expect("use_keyspace error is the cause");
        assert!(cause.is_instance_of::<OperationTimedOut>(py));
    });
}

#[test]
fn dropped_connection_keeps_its_kind_on_both_paths() {
    Python::initialize();
    let kinds: Vec<(fn() -> BrokenConnectionErrorKind, PyBrokenConnectionKind)> = vec![
        (
            || BrokenConnectionErrorKind::KeepaliveTimeout(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            PyBrokenConnectionKind::KeepaliveTimeout,
        ),
        (
            || BrokenConnectionErrorKind::KeepaliveRequestError(Arc::new(std::fmt::Error)),
            PyBrokenConnectionKind::KeepaliveFailed,
        ),
        (
            || BrokenConnectionErrorKind::WriteError(std::io::ErrorKind::BrokenPipe.into()),
            PyBrokenConnectionKind::WriteFailed,
        ),
        (
            || {
                BrokenConnectionErrorKind::FrameHeaderParseError(
                    FrameHeaderParseError::FrameFromClient,
                )
            },
            PyBrokenConnectionKind::FrameParseFailed,
        ),
        (
            || BrokenConnectionErrorKind::UnexpectedStreamId(7),
            PyBrokenConnectionKind::UnexpectedStreamId,
        ),
        (
            || {
                BrokenConnectionErrorKind::CqlEventHandlingError(
                    CqlEventHandlingError::UnexpectedResponse(CqlResponseKind::Ready),
                )
            },
            PyBrokenConnectionKind::EventHandlingFailed,
        ),
        (
            || BrokenConnectionErrorKind::TooManyOrphanedStreamIds(3),
            PyBrokenConnectionKind::TooManyOrphanedStreams,
        ),
        (
            || BrokenConnectionErrorKind::ChannelError,
            PyBrokenConnectionKind::ChannelClosed,
        ),
    ];
    for (make, expected) in kinds {
        let on_request = ExecutionError::LastAttemptError(
            RequestAttemptError::BrokenConnectionError(make().into()),
        );
        let in_pool = ExecutionError::ConnectionPoolError(ConnectionPoolError::Broken {
            last_connection_error: ConnectionError::BrokenConnection(make().into()),
        });
        Python::attach(|py| {
            let on_request = execution_error_to_pyerr(py, &on_request, on_request.to_string());
            let in_pool = execution_error_to_pyerr(py, &in_pool, in_pool.to_string());
            assert!(on_request.is_instance_of::<BrokenConnectionInFlight>(py));
            assert!(in_pool.is_instance_of::<BrokenConnectionInPool>(py));
            for err in [&on_request, &in_pool] {
                assert!(err.is_instance_of::<BrokenConnection>(py));
                assert_eq!(attr::<PyBrokenConnectionKind>(py, err, "kind"), expected);
            }
        });
    }
}

#[test]
fn unlocated_serialization_failure_has_none_bind_marker() {
    Python::initialize();
    let err = SerializationError::new(std::fmt::Error);
    Python::attach(|py| {
        let err = serialization_error_to_pyerr(py, &err, err.to_string());
        assert!(err.is_instance_of::<SerializeFailedError>(py));
        assert!(err.value(py).getattr("bind_marker").unwrap().is_none());
    });
}

fn broken_pool(py: Python<'_>, last_connection_error: ConnectionError) -> PyErr {
    let err = ExecutionError::ConnectionPoolError(ConnectionPoolError::Broken {
        last_connection_error,
    });
    execution_error_to_pyerr(py, &err, err.to_string())
}

#[test]
fn connect_timeout_is_a_timeout_error() {
    Python::initialize();
    Python::attach(|py| {
        let err = broken_pool(py, ConnectionError::ConnectTimeout);
        assert!(err.is_instance_of::<ConnectTimeout>(py));
        assert!(err.is_instance_of::<ConnectionPoolBroken>(py));
        assert!(err.is_instance_of::<PyTimeoutError>(py));
    });
}

#[test]
fn translator_io_error_is_the_cause() {
    Python::initialize();
    let refused = std::io::Error::from(std::io::ErrorKind::ConnectionRefused);
    let translation = TranslationError::IoError(Arc::new(refused));
    Python::attach(|py| {
        let err = broken_pool(py, ConnectionError::TranslationError(translation));
        assert!(err.is_instance_of::<AddressTranslationFailed>(py));
        let cause = err.cause(py).expect("I/O error is the cause");
        assert!(cause.is_instance_of::<PyConnectionRefusedError>(py));
    });
}

#[test]
fn no_free_stream_on_a_request_is_connection_busy() {
    Python::initialize();
    let err = ExecutionError::LastAttemptError(RequestAttemptError::UnableToAllocStreamId);
    Python::attach(|py| {
        let err = execution_error_to_pyerr(py, &err, err.to_string());
        assert!(err.is_instance_of::<ConnectionBusy>(py));
    });
}

fn setup_error(
    py: Python<'_>,
    request_kind: CqlRequestKind,
    err: ConnectionSetupRequestErrorKind,
) -> PyErr {
    let message = err.to_string();
    setup_request_error_to_pyerr(py, request_kind, &err, message)
}

#[test]
fn rejected_credentials_are_setup_failure_with_database_cause() {
    Python::initialize();
    let rejected = ConnectionSetupRequestErrorKind::DbError(
        DbError::AuthenticationError,
        "bad credentials".to_string(),
    );
    Python::attach(|py| {
        let err = setup_error(py, CqlRequestKind::AuthResponse, rejected);
        assert!(err.is_instance_of::<ConnectionSetupFailed>(py));
        assert!(!err.is_instance_of::<RequestFailedError>(py));
        assert_eq!(
            attr::<PyCqlRequestKind>(py, &err, "request_kind"),
            PyCqlRequestKind::AuthResponse
        );
        assert_eq!(
            attr::<PyConnectionSetupErrorKind>(py, &err, "kind"),
            PyConnectionSetupErrorKind::DatabaseError
        );
        let cause = err.cause(py).expect("database error is the cause");
        assert!(cause.is_instance_of::<AuthenticationFailed>(py));
        assert_eq!(attr::<String>(py, &cause, "reason"), "bad credentials");
    });
}

#[test]
fn missing_authenticator_is_connection_authentication_failed() {
    Python::initialize();
    let missing = ConnectionSetupRequestErrorKind::MissingAuthentication;
    Python::attach(|py| {
        let err = setup_error(py, CqlRequestKind::Startup, missing);
        assert!(err.is_instance_of::<ConnectionAuthenticationFailed>(py));
        assert!(!err.is_instance_of::<ConnectionSetupFailed>(py));
    });
}

#[test]
fn dead_connection_during_setup_is_broken_connection_in_pool() {
    Python::initialize();
    let dead = ConnectionSetupRequestErrorKind::BrokenConnection(
        BrokenConnectionErrorKind::ChannelError.into(),
    );
    Python::attach(|py| {
        let err = setup_error(py, CqlRequestKind::Options, dead);
        assert!(err.is_instance_of::<BrokenConnectionInPool>(py));
    });
}

#[test]
fn setup_failure_keeps_its_kind() {
    Python::initialize();
    let too_long = u16::try_from(usize::MAX).unwrap_err();
    let kinds: Vec<(ConnectionSetupRequestErrorKind, PyConnectionSetupErrorKind)> = vec![
        (
            ConnectionSetupRequestErrorKind::CqlRequestSerialization(
                CqlRequestSerializationError::PrepareSerialization(
                    PrepareSerializationError::StatementStringSerialization(too_long),
                ),
            ),
            PyConnectionSetupErrorKind::RequestSerializationError,
        ),
        (
            ConnectionSetupRequestErrorKind::UnableToAllocStreamId,
            PyConnectionSetupErrorKind::ConnectionBusy,
        ),
        (
            ConnectionSetupRequestErrorKind::UnexpectedResponse(CqlResponseKind::Result),
            PyConnectionSetupErrorKind::UnexpectedResponse,
        ),
    ];
    for (err, expected) in kinds {
        Python::attach(|py| {
            let err = setup_error(py, CqlRequestKind::Startup, err);
            assert!(err.is_instance_of::<ConnectionSetupFailed>(py));
            assert_eq!(
                attr::<PyConnectionSetupErrorKind>(py, &err, "kind"),
                expected
            );
        });
    }
}

#[test]
fn every_request_kind_has_its_own_value() {
    let kinds = [
        (CqlRequestKind::Startup, PyCqlRequestKind::Startup),
        (CqlRequestKind::AuthResponse, PyCqlRequestKind::AuthResponse),
        (CqlRequestKind::Options, PyCqlRequestKind::Options),
        (CqlRequestKind::Query, PyCqlRequestKind::Query),
        (CqlRequestKind::Prepare, PyCqlRequestKind::Prepare),
        (CqlRequestKind::Execute, PyCqlRequestKind::Execute),
        (CqlRequestKind::Batch, PyCqlRequestKind::Batch),
        (CqlRequestKind::Register, PyCqlRequestKind::Register),
    ];
    for (rust, python) in kinds {
        assert_eq!(PyCqlRequestKind::from(rust), python);
    }
}
