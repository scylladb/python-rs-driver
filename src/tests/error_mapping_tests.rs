use std::time::Duration;

use pyo3::exceptions::PyTimeoutError;
use pyo3::prelude::*;
use scylla::errors::ExecutionError;

use crate::errors::OperationTimedOut;
use crate::errors::request::execution_error_to_pyerr;

fn attr<'py, T: FromPyObjectOwned<'py>>(py: Python<'py>, err: &PyErr, name: &str) -> T {
    let value = err.value(py).getattr(name).unwrap();
    value.extract().map_err(Into::<PyErr>::into).unwrap()
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
