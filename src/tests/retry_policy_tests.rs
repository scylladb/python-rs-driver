//! Tests of the retry policy glue between the Rust driver and Python, for
//! what the Python tests cannot reach.
//!
//! `RequestInfo` is built only by the driver, so only Rust can hand one to a
//! built-in session and compare its decision with the Rust driver's own. The
//! Rust driver never calls `reset()` on a retry session, so `reset()` runs
//! only here. The rest is in `python/tests/test_retry_policy.py`.

use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;
use pyo3::types::PyType;
use scylla::errors::{DbError, RequestAttemptError, WriteType};
use scylla::policies::retry::{
    DefaultRetrySession, DowngradingConsistencyRetrySession, FallthroughRetrySession, RequestInfo,
    RetryDecision, RetryPolicy, RetrySession,
};
use scylla::statement::Consistency;

use super::attr;
use crate::policies::retry::decision::PyRetryDecision;
use crate::policies::retry::policies::{
    PyCustomRetryPolicy, PyCustomRetrySession, PyDefaultRetryPolicy,
    PyDowngradingConsistencyRetryPolicy, PyFallthroughRetryPolicy, PyFallthroughRetrySession,
};
use crate::policies::retry::request::PyRequestInfo;

/* Helpers */

fn to_py<'py, T: IntoPyObjectExt<'py>>(py: Python<'py>, value: T) -> Bound<'py, PyAny> {
    value.into_bound_py_any(py).unwrap()
}

fn unavailable() -> RequestAttemptError {
    RequestAttemptError::DbError(
        DbError::Unavailable {
            consistency: Consistency::All,
            required: 3,
            alive: 2,
        },
        "unavailable".to_string(),
    )
}

/* Built-in sessions called from Python */

/// Errors on which the three built-in sessions decide differently. What each
/// one decides is the Rust driver's logic, which the Rust driver tests. These
/// only have to show which Rust session a Python session calls.
fn sample_errors() -> Vec<RequestAttemptError> {
    let db = |error| RequestAttemptError::DbError(error, String::new());
    vec![
        db(DbError::IsBootstrapping),
        db(DbError::Overloaded),
        db(DbError::Unavailable {
            consistency: Consistency::Quorum,
            required: 2,
            alive: 1,
        }),
        db(DbError::ReadTimeout {
            consistency: Consistency::Quorum,
            received: 2,
            required: 2,
            data_present: false,
        }),
        db(DbError::WriteTimeout {
            consistency: Consistency::Quorum,
            received: 1,
            required: 2,
            write_type: WriteType::Simple,
        }),
    ]
}

const SAMPLE_CONSISTENCIES: [Consistency; 2] = [Consistency::Quorum, Consistency::Serial];

type NewSession = fn() -> Box<dyn RetrySession>;

fn new_default_session() -> Box<dyn RetrySession> {
    Box::new(DefaultRetrySession::new())
}

fn new_downgrading_session() -> Box<dyn RetrySession> {
    Box::new(DowngradingConsistencyRetrySession::new())
}

fn new_fallthrough_session() -> Box<dyn RetrySession> {
    Box::new(FallthroughRetrySession)
}

fn decide(session: &Bound<'_, PyAny>, info: &RequestInfo<'_>) -> RetryDecision {
    let decision = session
        .call_method1(
            "decide_should_retry",
            (PyRequestInfo::new(session.py(), info),),
        )
        .unwrap();
    let decision: PyRetryDecision = decision.extract().unwrap();
    RetryDecision::from(&decision)
}

/// Feeds the same requests to a Python session object and to the Rust
/// session it should wrap, and checks they decide the same. Each error is
/// decided twice, since the built-in sessions only retry once, and a reset in
/// between checks that `reset()` reaches the Rust session as well.
fn assert_session_delegates(py_session: &Bound<'_, PyAny>, new_rust_session: NewSession) {
    let mut decided = std::collections::HashSet::new();
    for error in sample_errors() {
        for is_idempotent in [true, false] {
            for cl in SAMPLE_CONSISTENCIES {
                let info = || RequestInfo::new(&error, is_idempotent, cl);
                let mut rust_session = new_rust_session();
                let reset = py_session.call_method0("reset").unwrap();
                assert!(reset.is_none());

                let context = format!("{error:?}, idempotent={is_idempotent}, {cl:?}");
                let first = rust_session.decide_should_retry(info());
                assert_eq!(decide(py_session, &info()), first, "first try: {context}");
                let second = rust_session.decide_should_retry(info());
                assert_eq!(decide(py_session, &info()), second, "second try: {context}");

                rust_session.reset();
                py_session.call_method0("reset").unwrap();
                assert_eq!(
                    decide(py_session, &info()),
                    rust_session.decide_should_retry(info()),
                    "after reset: {context}"
                );
                decided.insert(format!("{first:?}"));
            }
        }
    }
    // Keep the sample honest: the comparison is only meaningful if the session
    // under test gives different answers for different inputs.
    if py_session.is_instance_of::<PyFallthroughRetrySession>() {
        assert_eq!(decided.len(), 1);
    } else {
        assert!(decided.len() >= 3, "sample too narrow: {decided:?}");
    }
}

#[test]
fn builtin_sessions_delegate_to_rust() {
    Python::initialize();
    Python::attach(|py| {
        let builtins: [(Bound<'_, PyType>, NewSession); 3] = [
            (py.get_type::<PyDefaultRetryPolicy>(), new_default_session),
            (
                py.get_type::<PyDowngradingConsistencyRetryPolicy>(),
                new_downgrading_session,
            ),
            (
                py.get_type::<PyFallthroughRetryPolicy>(),
                new_fallthrough_session,
            ),
        ];

        for (policy_type, new_rust_session) in builtins {
            let policy = policy_type.call0().unwrap();
            let session = policy.call_method0("new_session").unwrap();
            assert_session_delegates(&session, new_rust_session);

            // To the driver, any object with `new_session()` is a custom policy.
            // Given a built-in policy, it uses the Rust session of the Python
            // session, and `reset()` must reach that one too. The Rust driver
            // never calls `reset()`, so nothing else runs this.
            let custom = PyCustomRetryPolicy {
                inner: policy.unbind(),
            };
            let mut shared = custom.new_session();
            let mut rust_session = new_rust_session();
            let error = unavailable();
            let info = || RequestInfo::new(&error, true, Consistency::All);
            for attempt in 0..2 {
                assert_eq!(
                    shared.decide_should_retry(info()),
                    rust_session.decide_should_retry(info()),
                    "attempt {attempt}"
                );
            }
            shared.reset();
            rust_session.reset();
            assert_eq!(
                shared.decide_should_retry(info()),
                rust_session.decide_should_retry(info()),
                "after reset"
            );
        }
    });
}

/* Custom Python sessions and policies, as the driver calls them */

const CUSTOM_SESSIONS: &std::ffi::CStr = cr#"
class FailingReset:
    def __init__(self, decision):
        self.decision = decision
        self.resets = 0

    def decide_should_retry(self, info):
        return self.decision

    def reset(self):
        self.resets += 1
        raise RuntimeError("reset failed")
"#;

fn custom_sessions(py: Python<'_>) -> Bound<'_, PyModule> {
    PyModule::from_code(
        py,
        CUSTOM_SESSIONS,
        c"retry_custom_sessions.py",
        c"retry_custom_sessions",
    )
    .unwrap()
}

/// A custom session whose `reset()` raises must stay usable. The Rust driver
/// never calls `reset()` on a retry session (it makes a new one per request),
/// so this is the only place it runs.
#[test]
fn custom_session_keeps_working_after_its_reset_raises() {
    Python::initialize();
    let error = unavailable();
    let (py_session, mut session) = Python::attach(|py| {
        let decision = to_py(py, PyRetryDecision::IgnoreWriteError());
        let py_session = custom_sessions(py)
            .getattr("FailingReset")
            .unwrap()
            .call1((decision,))
            .unwrap();
        let session = PyCustomRetrySession {
            inner: py_session.clone().unbind(),
        };
        (py_session.unbind(), session)
    });

    session.reset();

    let decision = session.decide_should_retry(RequestInfo::new(&error, false, Consistency::One));
    assert_eq!(decision, RetryDecision::IgnoreWriteError);
    Python::attach(|py| {
        assert_eq!(attr::<i32>(py_session.bind(py), "resets"), 1);
    });
}
