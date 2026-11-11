//! Tests of the retry policy glue between the Rust driver and Python.
//!
//! `RetryDecision` crosses the boundary both ways, `RequestInfo` is built only
//! by the driver, and the built-in sessions must decide exactly like the Rust
//! driver's own. Custom Python sessions are driven the way the driver drives
//! them, including the failure paths that only log.

use pyo3::prelude::*;
use pyo3::{IntoPyObjectExt, PyTypeInfo};
use scylla::errors::{BrokenConnectionErrorKind, DbError, RequestAttemptError, WriteType};
use scylla::policies::retry::{
    DefaultRetrySession, DowngradingConsistencyRetrySession, FallthroughRetrySession, RequestInfo,
    RetryDecision, RetryPolicy, RetrySession,
};
use scylla::statement::Consistency;

use crate::enums::PyConsistency;
use crate::errors::Unavailable;
use crate::policies::retry::decision::PyRetryDecision;
use crate::policies::retry::policies::{
    PyCustomRetryPolicy, PyCustomRetrySession, PyDefaultRetryPolicy, PyDefaultRetrySession,
    PyDowngradingConsistencyRetryPolicy, PyDowngradingConsistencyRetrySession,
    PyFallthroughRetryPolicy, PyFallthroughRetrySession,
};
use crate::policies::retry::request::PyRequestInfo;

/* Helpers */

fn to_py<'py, T: IntoPyObjectExt<'py>>(py: Python<'py>, value: T) -> Bound<'py, PyAny> {
    value.into_bound_py_any(py).unwrap()
}

/// Asserts that `obj` is an instance of the variant class `T.<variant>`.
fn assert_variant<T: PyTypeInfo>(obj: &Bound<'_, PyAny>, variant: &str) {
    let base = obj.py().get_type::<T>();
    let class = base.getattr(variant).unwrap();
    assert!(
        obj.is_instance(&class).unwrap(),
        "expected {}.{variant}, got {}",
        base.name().unwrap(),
        obj.get_type().name().unwrap(),
    );
    assert!(obj.is_instance(&base).unwrap());
}

fn field<'py, T: FromPyObjectOwned<'py>>(obj: &Bound<'py, PyAny>, name: &str) -> T {
    let value = obj.getattr(name).unwrap();
    value.extract().map_err(Into::<PyErr>::into).unwrap()
}

fn consistency(obj: &Bound<'_, PyAny>) -> PyConsistency {
    field(obj, "consistency")
}

/* RetryDecision */

#[test]
fn retry_decision_round_trips() {
    Python::initialize();
    Python::attach(|py| {
        let cases = [
            (RetryDecision::RetrySameTarget(None), "RetrySameTarget"),
            (
                RetryDecision::RetrySameTarget(Some(Consistency::Two)),
                "RetrySameTarget",
            ),
            (RetryDecision::RetryNextTarget(None), "RetryNextTarget"),
            (
                RetryDecision::RetryNextTarget(Some(Consistency::LocalOne)),
                "RetryNextTarget",
            ),
            (RetryDecision::DontRetry, "DontRetry"),
            (RetryDecision::IgnoreWriteError, "IgnoreWriteError"),
        ];

        for (decision, class) in cases {
            let obj = to_py(py, PyRetryDecision::from(decision.clone()));
            assert_variant::<PyRetryDecision>(&obj, class);

            if let RetryDecision::RetrySameTarget(cl) | RetryDecision::RetryNextTarget(cl) =
                &decision
            {
                let py_cl: Option<PyConsistency> = field(&obj, "consistency");
                assert_eq!(py_cl, cl.map(PyConsistency::from));
            }

            let back: PyRetryDecision = obj.extract().unwrap();
            assert_eq!(RetryDecision::from(&back), decision);
        }
    });
}

/* RequestInfo */

#[test]
fn request_info_exposes_its_fields_and_formats_them() {
    Python::initialize();
    Python::attach(|py| {
        let error = RequestAttemptError::DbError(
            DbError::Unavailable {
                consistency: Consistency::Quorum,
                required: 3,
                alive: 1,
            },
            "not enough replicas".to_string(),
        );
        for (is_idempotent, cl, py_cl) in [
            (true, Consistency::LocalQuorum, PyConsistency::LocalQuorum),
            (false, Consistency::One, PyConsistency::One),
        ] {
            let info = PyRequestInfo::new(py, &RequestInfo::new(&error, is_idempotent, cl));
            let obj = to_py(py, info.clone());

            // The error is the exception `execute` would raise for it.
            let py_error = obj.getattr("error").unwrap();
            assert!(py_error.is_instance_of::<Unavailable>());
            assert_eq!(consistency(&py_error), PyConsistency::Quorum);
            assert_eq!(field::<i32>(&py_error, "required"), 3);
            assert_eq!(field::<i32>(&py_error, "alive"), 1);
            assert_eq!(field::<String>(&py_error, "reason"), "not enough replicas");
            assert_eq!(field::<bool>(&obj, "is_idempotent"), is_idempotent);
            assert_eq!(consistency(&obj), py_cl);

            let expected = format!(
                "RequestInfo(error={error}, is_idempotent={is_idempotent}, consistency={py_cl:?})"
            );
            let text = obj.str().unwrap();
            assert_eq!(text.to_str().unwrap(), expected);
            let repr = obj.repr().unwrap();
            assert_eq!(repr.to_str().unwrap(), expected);

            // And back to the Rust driver's type.
            let back = info.to_request_info();
            assert_eq!(format!("{:?}", back.error), format!("{error:?}"));
            assert_eq!(back.is_idempotent, is_idempotent);
            assert_eq!(back.consistency, cl);
        }
    });
}

/* Built-in policies called from Python */

fn grid_errors() -> Vec<RequestAttemptError> {
    let db = |error| RequestAttemptError::DbError(error, String::new());
    vec![
        RequestAttemptError::BrokenConnectionError(
            BrokenConnectionErrorKind::UnexpectedStreamId(1).into(),
        ),
        RequestAttemptError::UnableToAllocStreamId,
        RequestAttemptError::NonfinishedPagingState,
        db(DbError::Overloaded),
        db(DbError::ServerError),
        db(DbError::TruncateError),
        db(DbError::IsBootstrapping),
        db(DbError::SyntaxError),
        db(DbError::Unavailable {
            consistency: Consistency::Three,
            required: 3,
            alive: 2,
        }),
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
        db(DbError::ReadTimeout {
            consistency: Consistency::Quorum,
            received: 1,
            required: 2,
            data_present: true,
        }),
        db(DbError::WriteTimeout {
            consistency: Consistency::Quorum,
            received: 1,
            required: 2,
            write_type: WriteType::BatchLog,
        }),
        db(DbError::WriteTimeout {
            consistency: Consistency::Quorum,
            received: 1,
            required: 2,
            write_type: WriteType::UnloggedBatch,
        }),
        db(DbError::WriteTimeout {
            consistency: Consistency::Quorum,
            received: 0,
            required: 2,
            write_type: WriteType::Simple,
        }),
        db(DbError::ReadFailure {
            consistency: Consistency::Quorum,
            received: 1,
            required: 2,
            numfailures: 1,
            data_present: false,
        }),
    ]
}

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

const GRID_CONSISTENCIES: [Consistency; 3] = [
    Consistency::Quorum,
    Consistency::Serial,
    Consistency::EachQuorum,
];

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
    for error in grid_errors() {
        for is_idempotent in [true, false] {
            for cl in GRID_CONSISTENCIES {
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
    // Keep the grid honest: the comparison is only meaningful if the policy
    // under test gives different answers for different inputs.
    if py_session.is_instance_of::<PyFallthroughRetrySession>() {
        assert_eq!(decided.len(), 1);
    } else {
        assert!(decided.len() >= 3, "grid too narrow: {decided:?}");
    }
}

#[test]
fn default_retry_session_delegates_to_rust() {
    Python::initialize();
    Python::attach(|py| {
        let policy = py.get_type::<PyDefaultRetryPolicy>().call0().unwrap();
        let session = policy.call_method0("new_session").unwrap();
        assert!(session.is_exact_instance_of::<PyDefaultRetrySession>());
        assert_session_delegates(&session, new_default_session);

        let session = py.get_type::<PyDefaultRetrySession>().call0().unwrap();
        assert_session_delegates(&session, new_default_session);
    });
}

#[test]
fn downgrading_consistency_retry_session_delegates_to_rust() {
    Python::initialize();
    Python::attach(|py| {
        let policy = py
            .get_type::<PyDowngradingConsistencyRetryPolicy>()
            .call0()
            .unwrap();
        let session = policy.call_method0("new_session").unwrap();
        assert!(session.is_exact_instance_of::<PyDowngradingConsistencyRetrySession>());
        assert_session_delegates(&session, new_downgrading_session);

        let session = py
            .get_type::<PyDowngradingConsistencyRetrySession>()
            .call0()
            .unwrap();
        assert_session_delegates(&session, new_downgrading_session);
    });
}

#[test]
fn fallthrough_retry_session_delegates_to_rust() {
    Python::initialize();
    Python::attach(|py| {
        let policy = py.get_type::<PyFallthroughRetryPolicy>().call0().unwrap();
        let session = policy.call_method0("new_session").unwrap();
        assert!(session.is_exact_instance_of::<PyFallthroughRetrySession>());
        assert_session_delegates(&session, new_fallthrough_session);

        let session = py.get_type::<PyFallthroughRetrySession>().call0().unwrap();
        assert_session_delegates(&session, new_fallthrough_session);
    });
}

/// A Python policy whose `new_session()` returns a built-in session is
/// unwrapped by the driver into the Rust session, which must still decide
/// exactly like the Rust driver's own session.
#[test]
fn custom_policy_returning_builtin_sessions_uses_them() {
    Python::initialize();
    let policies: [(Py<PyAny>, NewSession); 3] = Python::attach(|py| {
        let policy = |ty: Bound<'_, pyo3::types::PyType>| ty.call0().unwrap().unbind();
        [
            (
                policy(py.get_type::<PyDefaultRetryPolicy>()),
                new_default_session as NewSession,
            ),
            (
                policy(py.get_type::<PyDowngradingConsistencyRetryPolicy>()),
                new_downgrading_session,
            ),
            (
                policy(py.get_type::<PyFallthroughRetryPolicy>()),
                new_fallthrough_session,
            ),
        ]
    });

    for (py_policy, new_rust_session) in policies {
        // The driver treats any object with `new_session()` as a custom
        // policy; pass the built-in one through that path.
        let policy = PyCustomRetryPolicy { inner: py_policy };
        for error in grid_errors() {
            for is_idempotent in [true, false] {
                for cl in GRID_CONSISTENCIES {
                    let info = || RequestInfo::new(&error, is_idempotent, cl);
                    let mut wrapped = policy.new_session();
                    let mut rust_session = new_rust_session();
                    let context = format!("{error:?}, idempotent={is_idempotent}, {cl:?}");
                    for attempt in 0..2 {
                        assert_eq!(
                            wrapped.decide_should_retry(info()),
                            rust_session.decide_should_retry(info()),
                            "attempt {attempt}: {context}"
                        );
                    }
                    wrapped.reset();
                    rust_session.reset();
                    assert_eq!(
                        wrapped.decide_should_retry(info()),
                        rust_session.decide_should_retry(info()),
                        "after reset: {context}"
                    );
                }
            }
        }
    }
}

/* Custom Python sessions and policies, as the driver calls them */

/// Collects the driver's `tracing` events, which reach the `log` crate when
/// no `tracing` subscriber is installed. Tests share it, so each one looks
/// only for messages carrying its own unique marker.
struct CapturingLogger {
    records: std::sync::Mutex<Vec<String>>,
}

impl log::Log for CapturingLogger {
    fn enabled(&self, _metadata: &log::Metadata<'_>) -> bool {
        true
    }

    fn log(&self, record: &log::Record<'_>) {
        #[expect(
            clippy::disallowed_methods,
            reason = "test-only logger; no Python code runs under this lock"
        )]
        let mut records = self.records.lock().unwrap();
        records.push(format!("{} {}", record.level(), record.args()));
    }

    fn flush(&self) {}
}

static LOGGER: CapturingLogger = CapturingLogger {
    records: std::sync::Mutex::new(Vec::new()),
};

fn capture_logs() {
    // Fails when another test of this module installed it first, which is fine.
    let _ = log::set_logger(&LOGGER);
    // A different logger installed first would leave `LOGGER` empty and fail
    // every log assertion without saying why.
    assert!(
        std::ptr::addr_eq(log::logger(), &LOGGER),
        "another logger is installed, so these tests cannot capture the driver's logs"
    );
    log::set_max_level(log::LevelFilter::Trace);
}

fn logged(marker: &str) -> Vec<String> {
    #[expect(
        clippy::disallowed_methods,
        reason = "test-only logger; no Python code runs under this lock"
    )]
    let records = LOGGER.records.lock().unwrap();
    records
        .iter()
        .filter(|r| r.contains(marker))
        .cloned()
        .collect()
}

const CUSTOM_SESSIONS: &std::ffi::CStr = cr#"
class Recording:
    def __init__(self, decision=None, decide_error=None, reset_error=None):
        self.decision = decision
        self.decide_error = decide_error
        self.reset_error = reset_error
        self.infos = []
        self.resets = 0

    def decide_should_retry(self, info):
        self.infos.append(info)
        if self.decide_error is not None:
            raise RuntimeError(self.decide_error)
        return self.decision

    def reset(self):
        self.resets += 1
        if self.reset_error is not None:
            raise RuntimeError(self.reset_error)

class NotADecision9b8e:
    pass

class FailingPolicy:
    def __init__(self):
        self.calls = 0

    def new_session(self):
        self.calls += 1
        raise RuntimeError("new_session marker-7f3a")
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

fn custom_session(inner: &Bound<'_, PyAny>) -> PyCustomRetrySession {
    PyCustomRetrySession {
        inner: inner.clone().unbind(),
    }
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

#[test]
fn custom_session_decision_and_request_info_reach_the_driver() {
    Python::initialize();
    let error = unavailable();
    let (py_session, mut session) = Python::attach(|py| {
        let decision = to_py(
            py,
            PyRetryDecision::RetryNextTarget {
                consistency: Some(PyConsistency::Two),
            },
        );
        let py_session = custom_sessions(py)
            .getattr("Recording")
            .unwrap()
            .call1((decision,))
            .unwrap();
        let session = custom_session(&py_session);
        (py_session.unbind(), session)
    });

    let decision = session.decide_should_retry(RequestInfo::new(&error, true, Consistency::All));
    assert_eq!(
        decision,
        RetryDecision::RetryNextTarget(Some(Consistency::Two))
    );

    Python::attach(|py| {
        let infos = py_session.bind(py).getattr("infos").unwrap();
        assert_eq!(infos.len().unwrap(), 1);
        let info = infos.get_item(0).unwrap();
        assert!(field::<bool>(&info, "is_idempotent"));
        assert_eq!(consistency(&info), PyConsistency::All);
        let error = info.getattr("error").unwrap();
        assert!(error.is_instance_of::<Unavailable>());
        assert_eq!(consistency(&error), PyConsistency::All);
        assert_eq!(field::<i32>(&error, "required"), 3);
        assert_eq!(field::<i32>(&error, "alive"), 2);
        assert_eq!(field::<String>(&error, "reason"), "unavailable");
    });
}

#[test]
fn custom_session_that_raises_does_not_retry_and_is_logged() {
    Python::initialize();
    capture_logs();
    let error = unavailable();
    let mut session = Python::attach(|py| {
        let decision = to_py(py, PyRetryDecision::RetrySameTarget { consistency: None });
        let kwargs = pyo3::types::PyDict::new(py);
        kwargs.set_item("decision", decision).unwrap();
        kwargs
            .set_item("decide_error", "decide marker-1c2d")
            .unwrap();
        let py_session = custom_sessions(py)
            .getattr("Recording")
            .unwrap()
            .call((), Some(&kwargs))
            .unwrap();
        custom_session(&py_session)
    });

    let decision = session.decide_should_retry(RequestInfo::new(&error, true, Consistency::All));
    assert_eq!(decision, RetryDecision::DontRetry);

    let records = logged("decide marker-1c2d");
    assert_eq!(records.len(), 1, "{records:?}");
    assert!(records[0].starts_with("ERROR "), "{records:?}");
    assert!(
        records[0].contains("Failed to call decide_should_retry() on custom retry session"),
        "{records:?}"
    );
}

#[test]
fn custom_session_returning_a_non_decision_does_not_retry_and_is_logged() {
    Python::initialize();
    capture_logs();
    let error = unavailable();
    let mut session = Python::attach(|py| {
        let module = custom_sessions(py);
        // The log names the type of what the session returned: that is this test's marker.
        let not_a_decision = module.getattr("NotADecision9b8e").unwrap().call0().unwrap();
        let py_session = module
            .getattr("Recording")
            .unwrap()
            .call1((not_a_decision,))
            .unwrap();
        custom_session(&py_session)
    });

    let decision = session.decide_should_retry(RequestInfo::new(&error, true, Consistency::All));
    assert_eq!(decision, RetryDecision::DontRetry);

    let records = logged("NotADecision9b8e");
    assert_eq!(records.len(), 1, "{records:?}");
    assert!(records[0].starts_with("ERROR "), "{records:?}");
    assert!(
        records[0].contains("Failed to extract 'PyRetryDecision'"),
        "{records:?}"
    );
}

/// The Rust driver never calls `reset()` on a retry session (it makes a new
/// one per request), so this is the only place it runs.
#[test]
fn custom_session_reset_failure_is_logged_and_session_keeps_working() {
    Python::initialize();
    capture_logs();
    let error = unavailable();
    let (py_session, mut session) = Python::attach(|py| {
        let kwargs = pyo3::types::PyDict::new(py);
        kwargs
            .set_item("decision", to_py(py, PyRetryDecision::IgnoreWriteError()))
            .unwrap();
        kwargs.set_item("reset_error", "reset marker-4e5f").unwrap();
        let py_session = custom_sessions(py)
            .getattr("Recording")
            .unwrap()
            .call((), Some(&kwargs))
            .unwrap();
        let session = custom_session(&py_session);
        (py_session.unbind(), session)
    });

    session.reset();

    let records = logged("reset marker-4e5f");
    assert_eq!(records.len(), 1, "{records:?}");
    assert!(records[0].starts_with("ERROR "), "{records:?}");
    assert!(
        records[0].contains("Failed to call reset() on custom retry session"),
        "{records:?}"
    );

    let decision = session.decide_should_retry(RequestInfo::new(&error, false, Consistency::One));
    assert_eq!(decision, RetryDecision::IgnoreWriteError);
    Python::attach(|py| {
        assert_eq!(field::<i32>(py_session.bind(py), "resets"), 1);
    });
}

#[test]
fn custom_policy_whose_new_session_raises_falls_back_to_default_session() {
    Python::initialize();
    capture_logs();
    let py_policy = Python::attach(|py| {
        custom_sessions(py)
            .getattr("FailingPolicy")
            .unwrap()
            .call0()
            .unwrap()
            .unbind()
    });
    let policy = PyCustomRetryPolicy {
        inner: Python::attach(|py| py_policy.clone_ref(py)),
    };

    for error in grid_errors() {
        for is_idempotent in [true, false] {
            for cl in GRID_CONSISTENCIES {
                let info = || RequestInfo::new(&error, is_idempotent, cl);
                let mut session = policy.new_session();
                let mut default = DefaultRetrySession::new();
                for attempt in 0..2 {
                    assert_eq!(
                        session.decide_should_retry(info()),
                        default.decide_should_retry(info()),
                        "attempt {attempt}: {error:?}, idempotent={is_idempotent}, {cl:?}"
                    );
                }
            }
        }
    }

    let records = logged("new_session marker-7f3a");
    assert!(!records.is_empty());
    assert!(records.iter().all(|r| r.starts_with("ERROR ")
        && r.contains("Failed to call new_session() on custom retry policy")));
    Python::attach(|py| {
        let calls: usize = field(py_policy.bind(py), "calls");
        assert_eq!(calls, grid_errors().len() * 2 * GRID_CONSISTENCIES.len());
    });
}
