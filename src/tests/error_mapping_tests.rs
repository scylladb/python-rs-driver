use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::Duration;

use pyo3::exceptions::{PyConnectionRefusedError, PyKeyError, PyTimeoutError};
use pyo3::prelude::*;
use scylla::deserialize::TypeCheckError;
use scylla::errors::{
    BrokenConnectionErrorKind, ConnectionError, ConnectionPoolError, CustomTranslationError,
    DbError, ExecutionError, RequestAttemptError, TracingError,
};
use scylla::serialize::SerializationError;
use scylla::statement::Consistency;

use crate::errors::request::{execution_error_to_pyerr, tracing_error_to_pyerr};
use crate::errors::{
    AddressTranslationFailed, AlreadyExists, BrokenConnection, ConnectionLost,
    ConnectionPoolBroken, ConnectionPoolError as PyConnectionPoolError, DatabaseError,
    OperationTimedOut, PyBrokenConnectionKind, ReadTimeout, RequestFailedError, ResponseParseError,
    SerializeFailedError, TraceUnavailable, Unavailable,
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
fn broken_pool_keepalive_timeout_is_connection_lost() {
    Python::initialize();
    let address = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2));
    let err = ExecutionError::ConnectionPoolError(ConnectionPoolError::Broken {
        last_connection_error: ConnectionError::BrokenConnection(
            BrokenConnectionErrorKind::KeepaliveTimeout(address).into(),
        ),
    });
    Python::attach(|py| {
        let err = execution_error_to_pyerr(py, &err, err.to_string());
        assert!(err.is_instance_of::<ConnectionLost>(py));
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
        assert!(err.is_instance_of::<BrokenConnection>(py));
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
            || BrokenConnectionErrorKind::UnexpectedStreamId(7),
            PyBrokenConnectionKind::UnexpectedStreamId,
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
            assert!(on_request.is_instance_of::<BrokenConnection>(py));
            assert!(in_pool.is_instance_of::<ConnectionLost>(py));
            for err in [&on_request, &in_pool] {
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
