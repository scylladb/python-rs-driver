use crate::errors::{RetryPolicyError, get_type_name};
use crate::policies::retry::decision::PyRetryDecision;
use crate::policies::retry::request::PyRequestInfo;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::sync::MutexExt;
use scylla::policies::retry::{
    DefaultRetryPolicy, DefaultRetrySession, DowngradingConsistencyRetryPolicy,
    FallthroughRetryPolicy, RequestInfo, RetryDecision, RetryPolicy, RetrySession,
};
use scylla::policies::retry::{DowngradingConsistencyRetrySession, FallthroughRetrySession};
use std::sync::Arc;
use std::sync::Mutex;
use tracing::error;

#[derive(Clone)]
pub(crate) struct SharedRetrySession<T: RetrySession>(pub(crate) Arc<Mutex<T>>);

impl<T: RetrySession> RetrySession for SharedRetrySession<T> {
    fn decide_should_retry(&mut self, request_info: RequestInfo) -> RetryDecision {
        #[expect(
            clippy::disallowed_methods,
            reason = "the session under this lock is pure Rust, except a DEBUG log in the downgrading \
                      session that may run Python handlers. Only a handler calling back into this \
                      same session could deadlock, which is extremely unlikely, so it is accepted"
        )]
        let mut inner = self.0.lock().unwrap();
        inner.decide_should_retry(request_info)
    }

    fn reset(&mut self) {
        #[expect(
            clippy::disallowed_methods,
            reason = "the session under this lock is pure Rust, except a DEBUG log in the downgrading \
                      session that may run Python handlers. Only a handler calling back into this \
                      same session could deadlock, which is extremely unlikely, so it is accepted"
        )]
        let mut inner = self.0.lock().unwrap();
        inner.reset();
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PyCustomRetrySession {
    pub(crate) inner: Py<PyAny>,
}

impl RetrySession for PyCustomRetrySession {
    fn decide_should_retry(&mut self, request_info: RequestInfo) -> RetryDecision {
        Python::attach(|py| {
            let py_retry_session = self.inner.bind(py);
            let py_request_info = PyRequestInfo::from(&request_info);

            let result = py_retry_session
                .call_method1(intern!(py, "decide_should_retry"), (py_request_info,));

            match result {
                Ok(res) => match res.cast::<PyRetryDecision>() {
                    Ok(py_retry_decision) => RetryDecision::from(py_retry_decision.get()),
                    Err(err) => {
                        error!(
                            "Failed to extract 'PyRetryDecision'. \
                            Fallback action: 'DontRetry'. Reason: {}",
                            err
                        );
                        RetryDecision::DontRetry
                    }
                },
                Err(err) => {
                    error!(
                        "Failed to call decide_should_retry() on custom retry session. \
                        Fallback action: 'DontRetry'. Reason: {}",
                        err
                    );
                    RetryDecision::DontRetry
                }
            }
        })
    }

    fn reset(&mut self) {
        Python::attach(|py| {
            let obj = self.inner.bind(py);

            if let Err(err) = obj.call_method0(intern!(py, "reset")) {
                error!(
                    "Failed to call reset() on custom retry session. \
                    Reason: {}",
                    err
                );
            }
        })
    }
}

#[pyclass(module = "scylla.policies.retry", name = "DefaultRetrySession", frozen)]
pub(crate) struct PyDefaultRetrySession {
    pub(crate) inner: Arc<Mutex<DefaultRetrySession>>,
}

#[pymethods]
impl PyDefaultRetrySession {
    #[new]
    fn py_new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(DefaultRetrySession::new())),
        }
    }

    fn decide_should_retry(&self, request_info: PyRequestInfo, py: Python<'_>) -> PyRetryDecision {
        let mut inner = self.inner.lock_py_attached(py).unwrap();
        inner
            .decide_should_retry(request_info.to_request_info())
            .into()
    }

    fn reset(&self, py: Python<'_>) {
        let mut inner = self.inner.lock_py_attached(py).unwrap();
        inner.reset();
    }
}

#[pyclass(
    module = "scylla.policies.retry",
    name = "DowngradingConsistencyRetrySession",
    frozen
)]
pub(crate) struct PyDowngradingConsistencyRetrySession {
    pub(crate) inner: Arc<Mutex<DowngradingConsistencyRetrySession>>,
}

#[pymethods]
impl PyDowngradingConsistencyRetrySession {
    #[new]
    fn py_new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(DowngradingConsistencyRetrySession::new())),
        }
    }

    fn decide_should_retry(&self, request_info: PyRequestInfo, py: Python<'_>) -> PyRetryDecision {
        let mut inner = self.inner.lock_py_attached(py).unwrap();
        inner
            .decide_should_retry(request_info.to_request_info())
            .into()
    }

    fn reset(&self, py: Python<'_>) {
        let mut inner = self.inner.lock_py_attached(py).unwrap();
        inner.reset();
    }
}

#[pyclass(
    module = "scylla.policies.retry",
    name = "FallthroughRetrySession",
    frozen
)]
pub(crate) struct PyFallthroughRetrySession {}

#[pymethods]
impl PyFallthroughRetrySession {
    #[new]
    fn py_new() -> Self {
        Self {}
    }

    #[allow(unused_variables)]
    fn decide_should_retry(&self, request_info: PyRequestInfo) -> PyRetryDecision {
        PyRetryDecision::DontRetry()
    }

    fn reset(&self) {}
}

#[derive(Debug)]
pub(crate) struct PyCustomRetryPolicy {
    pub(crate) inner: Py<PyAny>,
}

impl RetryPolicy for PyCustomRetryPolicy {
    fn new_session(&self) -> Box<dyn RetrySession> {
        Python::attach(|py| -> Box<dyn RetrySession> {
            let policy = self.inner.bind(py);

            match policy.call_method0(intern!(py, "new_session")) {
                Ok(session) => {
                    if let Ok(s) = session.cast::<PyDefaultRetrySession>() {
                        return Box::new(SharedRetrySession(s.get().inner.clone()));
                    }
                    if let Ok(s) = session.cast::<PyDowngradingConsistencyRetrySession>() {
                        return Box::new(SharedRetrySession(s.get().inner.clone()));
                    }
                    if session.cast::<PyFallthroughRetrySession>().is_ok() {
                        return Box::new(FallthroughRetrySession);
                    }
                    Box::new(PyCustomRetrySession {
                        inner: session.unbind(),
                    })
                }
                Err(err) => {
                    error!(
                        "Failed to call new_session() on custom retry policy. \
                        Fallback action: 'DefaultRetrySession'. Reason: {}",
                        err
                    );
                    Box::new(DefaultRetrySession::new())
                }
            }
        })
    }
}

#[pyclass(module = "scylla.policies.retry", name = "DefaultRetryPolicy", frozen)]
#[derive(Debug)]
pub(crate) struct PyDefaultRetryPolicy {
    pub(crate) inner: Arc<DefaultRetryPolicy>,
}

#[pymethods]
impl PyDefaultRetryPolicy {
    #[new]
    fn py_new() -> Self {
        Self {
            inner: Arc::new(DefaultRetryPolicy::new()),
        }
    }

    fn new_session(&self) -> PyDefaultRetrySession {
        PyDefaultRetrySession {
            inner: Arc::new(Mutex::new(DefaultRetrySession::new())),
        }
    }
}

#[pyclass(
    module = "scylla.policies.retry",
    name = "DowngradingConsistencyRetryPolicy",
    frozen
)]
#[derive(Debug)]
pub(crate) struct PyDowngradingConsistencyRetryPolicy {
    pub(crate) inner: Arc<DowngradingConsistencyRetryPolicy>,
}

#[pymethods]
impl PyDowngradingConsistencyRetryPolicy {
    #[new]
    fn py_new() -> Self {
        Self {
            inner: Arc::new(DowngradingConsistencyRetryPolicy::new()),
        }
    }

    fn new_session(&self) -> PyDowngradingConsistencyRetrySession {
        PyDowngradingConsistencyRetrySession {
            inner: Arc::new(Mutex::new(DowngradingConsistencyRetrySession::new())),
        }
    }
}

#[pyclass(
    module = "scylla.policies.retry",
    name = "FallthroughRetryPolicy",
    frozen
)]
#[derive(Debug)]
pub(crate) struct PyFallthroughRetryPolicy {
    pub(crate) inner: Arc<FallthroughRetryPolicy>,
}

#[pymethods]
impl PyFallthroughRetryPolicy {
    #[new]
    fn py_new() -> Self {
        Self {
            inner: Arc::new(FallthroughRetryPolicy::new()),
        }
    }

    fn new_session(&self) -> PyFallthroughRetrySession {
        PyFallthroughRetrySession {}
    }
}

pub(crate) struct PyRetryPolicy {
    pub(crate) inner: Arc<dyn RetryPolicy>,
}

impl PyRetryPolicy {
    pub(crate) fn into_inner(self) -> Arc<dyn RetryPolicy> {
        self.inner
    }
}

impl<'py> FromPyObject<'_, 'py> for PyRetryPolicy {
    type Error = DriverRetryPolicyError;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(policy) = obj.cast::<PyDefaultRetryPolicy>() {
            return Ok(Self {
                inner: policy.get().inner.clone(),
            });
        }

        if let Ok(policy) = obj.cast::<PyDowngradingConsistencyRetryPolicy>() {
            return Ok(Self {
                inner: policy.get().inner.clone(),
            });
        }

        if let Ok(policy) = obj.cast::<PyFallthroughRetryPolicy>() {
            return Ok(Self {
                inner: policy.get().inner.clone(),
            });
        }

        if obj
            .hasattr(intern!(obj.py(), "new_session"))
            .unwrap_or(false)
        {
            return Ok(Self {
                inner: Arc::new(PyCustomRetryPolicy {
                    inner: obj.to_owned().unbind(),
                }),
            });
        }

        Err(DriverRetryPolicyError::invalid_policy(obj))
    }
}

/* Retry policy errors */

#[derive(Debug, thiserror::Error)]
#[must_use]
pub(crate) enum DriverRetryPolicyError {
    #[error(
        "Invalid retry policy '{type_name}': Object does not implement the \
         RetryPolicy protocol (missing required 'new_session' method)."
    )]
    InvalidPolicy { type_name: String },
}

impl DriverRetryPolicyError {
    pub(crate) fn invalid_policy(obj: Borrowed<PyAny>) -> Self {
        let type_name = get_type_name(obj);
        Self::InvalidPolicy { type_name }
    }
}

impl From<DriverRetryPolicyError> for PyErr {
    fn from(e: DriverRetryPolicyError) -> PyErr {
        RetryPolicyError::new_err(e.to_string())
    }
}
