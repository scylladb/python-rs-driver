use std::time::Duration;

use pyo3::exceptions::PyTimeoutError;
use pyo3::prelude::*;
use scylla::errors::{DbError, ExecutionError, RequestAttemptError};
use scylla::serialize::SerializationError;
use scylla::statement::Consistency;

use crate::errors::request::execution_error_to_pyerr;
use crate::errors::{
    AlreadyExists, DatabaseError, OperationTimedOut, ReadTimeout, SerializeFailedError, Unavailable,
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
fn unlocated_serialization_failure_has_none_parameter() {
    Python::initialize();
    let err = SerializationError::new(std::fmt::Error);
    Python::attach(|py| {
        let err = serialization_error_to_pyerr(py, &err, err.to_string());
        assert!(err.is_instance_of::<SerializeFailedError>(py));
        assert!(err.value(py).getattr("parameter").unwrap().is_none());
    });
}
