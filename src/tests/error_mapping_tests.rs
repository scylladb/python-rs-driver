//! The mapping from Rust driver errors to Python exceptions is public API: it decides which
//! `except` clause catches a failure and which attributes the handler can read. These tests pin
//! the class (exactly, plus the base classes users may catch instead), the message and every
//! attribute with its Python type, for every branch of the mapping.

use std::io;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use bytes::Bytes;
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::{PyTimeoutError, PyTypeError, PyUnicodeEncodeError, PyValueError};
use pyo3::prelude::*;
use pyo3::type_object::PyTypeInfo;
use pyo3::types::{PyBytes, PyString};
use scylla::deserialize::TypeCheckError;
use scylla::errors::{
    BadKeyspaceName as RustBadKeyspaceName, BadQuery as RustBadQuery, BrokenConnectionErrorKind,
    ClientRoutesMetadataError, ConnectionError, ConnectionPoolError as RustConnectionPoolError,
    CqlErrorParseError, CqlRequestSerializationError, CqlResponseKind, CqlResultParseError,
    DbError, ExecutionError, FrameBodyExtensionsParseError, KeyspaceStrategyError,
    KeyspacesMetadataError, MetadataError as RustMetadataError, NewSessionError, OperationType,
    PeersMetadataError, PrepareError as RustPrepareError, RequestAttemptError,
    SchemaAgreementError as RustSchemaAgreementError, SingleRowError, TablesMetadataError,
    TracingError, UdtMetadataError, UseKeyspaceError as RustUseKeyspaceError, WriteType,
};
use scylla::frame::frame_errors::LowLevelDeserializationError;
use scylla::serialize::SerializationError as RustSerializationError;
use scylla::statement::Consistency;
use uuid::Uuid;

use crate::enums::PyConsistency;
use crate::errors::execution::{
    DriverExecuteError, DriverMetadataRefreshError, DriverPrepareError, DriverSchemaAgreementError,
    DriverSessionConnectionError, DriverStatementConversionError, DriverTraceError,
    DriverUseKeyspaceError,
};
use crate::errors::request::{execution_error_to_pyerr, tracing_error_to_pyerr};
use crate::errors::types::{PyCqlResponseKind, PyOperationType, PyWriteType};
use crate::errors::{
    AlreadyExists, AlreadyPrepared, AuthenticationFailed, BadKeyspaceName, BadQuery,
    BrokenConnection, ConnectionBusy, ConnectionPoolBroken, ConnectionPoolError,
    ConnectionPoolInitializing, ContactPointError, CqlSyntaxError, DatabaseError,
    ExecutionError as PyExecutionError, FunctionFailure, HostnameResolutionFailed,
    InternalDriverError, InvalidClusterMetadata, InvalidRequest, InvalidStatementType,
    IsBootstrapping, KeyspaceNameMismatch, MetadataError, NoHostAvailable, NoKnownNodes,
    NodeDisabledByHostFilter, NonfinishedPagingState, OperationTimedOut, Overloaded,
    PagingStateNotAllowed, PartitionKeyExtractionFailed, PrepareError,
    PreparedStatementIdsMismatch, ProtocolError, RateLimitReached, ReadFailure, ReadTimeout,
    RepreparedIdChanged, RepreparedIdMissingInBatch, RequestFailedError, RequestSerializationError,
    RequiredHostAbsent, ResponseParseError, SchemaAgreementError, SchemaAgreementTimeout,
    ScyllaError, SerializationError, SerializeFailedError, ServerConfigError, ServerError,
    ServerProtocolError, SessionConfigError, StatementConversionError, TooManyStatementsInBatch,
    TraceUnavailable, TruncateError, Unauthorized, Unavailable, UnexpectedResponse,
    UnknownDatabaseError, Unprepared, UseKeyspaceError, ValueSerializationError,
    ValuesTooLongForKey, WriteFailure, WriteTimeout,
};
use crate::serialize::error::serialization_error_to_pyerr;

/// The message passed to the mapping: it stands for the full description of the failed operation.
const MESSAGE: &str = "Failed to execute statement: test failure";
/// The error message sent by the server along with a `DbError`.
const REASON: &str = "server reason";

/* Helpers */

fn map(py: Python<'_>, err: ExecutionError) -> PyErr {
    execution_error_to_pyerr(py, &err, MESSAGE.to_string())
}

fn attempt_error(py: Python<'_>, err: RequestAttemptError) -> PyErr {
    map(py, ExecutionError::LastAttemptError(err))
}

fn db_error(py: Python<'_>, error: DbError) -> PyErr {
    attempt_error(py, RequestAttemptError::DbError(error, REASON.to_string()))
}

/// Asserts that `err` is exactly of class `T`, not one of its subclasses or bases.
fn assert_class<T: PyTypeInfo>(py: Python<'_>, err: &PyErr) {
    let actual = err.get_type(py);
    let expected = py.get_type::<T>();
    assert!(
        actual.is(&expected),
        "expected {expected}, got {actual}: {err}"
    );
}

/// Asserts that `err` is an instance of every listed class: the bases a user may catch it as.
macro_rules! assert_bases {
    ($py: expr, $err: expr, $($base: ty),+ $(,)?) => {
        $(
            assert!(
                $err.is_instance_of::<$base>($py),
                "{} is not a {}",
                $err.get_type($py),
                stringify!($base),
            );
        )+
    };
}

fn message(py: Python<'_>, err: &PyErr) -> String {
    err.value(py).str().unwrap().to_string()
}

fn attr<'py>(py: Python<'py>, err: &PyErr, name: &str) -> Bound<'py, PyAny> {
    err.value(py)
        .getattr(name)
        .unwrap_or_else(|e| panic!("{} has no attribute {name}: {e}", err.get_type(py)))
}

fn type_name(value: &Bound<'_, PyAny>) -> String {
    value.get_type().name().unwrap().to_string()
}

/// Asserts that attribute `name` is a Python `int` (not a `bool`) equal to `expected`.
fn assert_int(py: Python<'_>, err: &PyErr, name: &str, expected: i64) {
    let value = attr(py, err, name);
    assert_eq!(type_name(&value), "int", "type of {name}");
    assert_eq!(value.extract::<i64>().unwrap(), expected, "value of {name}");
}

fn assert_bool(py: Python<'_>, err: &PyErr, name: &str, expected: bool) {
    let value = attr(py, err, name);
    assert_eq!(type_name(&value), "bool", "type of {name}");
    assert_eq!(
        value.extract::<bool>().unwrap(),
        expected,
        "value of {name}"
    );
}

fn assert_str(py: Python<'_>, err: &PyErr, name: &str, expected: &str) {
    let value = attr(py, err, name);
    assert_eq!(type_name(&value), "str", "type of {name}");
    assert_eq!(
        value.extract::<String>().unwrap(),
        expected,
        "value of {name}"
    );
}

fn assert_bytes(py: Python<'_>, err: &PyErr, name: &str, expected: &[u8]) {
    let value = attr(py, err, name);
    let bytes = value
        .cast::<PyBytes>()
        .unwrap_or_else(|_| panic!("{name} is a {}, not bytes", type_name(&value)));
    assert_eq!(bytes.as_bytes(), expected, "value of {name}");
}

fn assert_timedelta(py: Python<'_>, err: &PyErr, name: &str, expected: Duration) {
    let value = attr(py, err, name);
    assert_eq!(type_name(&value), "timedelta", "type of {name}");
    assert_eq!(
        value.extract::<Duration>().unwrap(),
        expected,
        "value of {name}"
    );
}

fn assert_consistency(py: Python<'_>, err: &PyErr, expected: PyConsistency) {
    let value = attr(py, err, "consistency");
    assert!(
        value.is_instance_of::<PyConsistency>(),
        "type of consistency"
    );
    assert_eq!(value.extract::<PyConsistency>().unwrap(), expected);
}

fn assert_write_type(py: Python<'_>, err: &PyErr, expected: PyWriteType) {
    let value = attr(py, err, "write_type");
    assert!(value.is_instance_of::<PyWriteType>(), "type of write_type");
    assert_eq!(value.extract::<PyWriteType>().unwrap(), expected);
}

/// Asserts the `reason` attribute every `DatabaseError` carries: the server's message.
fn assert_reason(py: Python<'_>, err: &PyErr) {
    assert_str(py, err, "reason", REASON);
}

static JOIN_ERROR_RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
});

/// A `JoinError` as returned for a runtime task that did not complete, and its description.
///
/// The description is tokio's wording, so tests compare against it instead of spelling it out.
/// Call it while not attached to the interpreter: it blocks on a runtime.
fn join_error() -> (tokio::task::JoinError, String) {
    #[expect(clippy::disallowed_methods, reason = "called outside Python::attach")]
    let error = JOIN_ERROR_RUNTIME.block_on(async {
        let handle = tokio::spawn(std::future::pending::<()>());
        handle.abort();
        handle.await.unwrap_err()
    });
    let description = error.to_string();
    (error, description)
}

/* ExecutionError */

#[test]
fn empty_plan_is_no_host_available() {
    Python::initialize();
    Python::attach(|py| {
        let err = map(py, ExecutionError::EmptyPlan);
        assert_class::<NoHostAvailable>(py, &err);
        assert_bases!(py, err, PyExecutionError, ScyllaError);
        assert_eq!(message(py, &err), MESSAGE);
    });
}

#[test]
fn request_timeout_is_a_timedelta() {
    Python::initialize();
    Python::attach(|py| {
        let err = map(
            py,
            ExecutionError::RequestTimeout(Duration::from_millis(1500)),
        );
        assert_class::<OperationTimedOut>(py, &err);
        assert_bases!(py, err, PyExecutionError, PyTimeoutError, ScyllaError);
        assert_eq!(message(py, &err), MESSAGE);
        assert_timedelta(py, &err, "timeout", Duration::from_millis(1500));
    });
}

#[test]
fn execution_keeps_the_class_of_the_cause() {
    Python::initialize();
    Python::attach(|py| {
        // Each nested driver error maps to the class of its innermost cause, with the message
        // of the operation that failed.
        let err = map(
            py,
            ExecutionError::PrepareError(RustPrepareError::PreparedStatementIdsMismatch),
        );
        assert_class::<PreparedStatementIdsMismatch>(py, &err);
        assert_bases!(py, err, PrepareError, PyExecutionError);
        assert_eq!(message(py, &err), MESSAGE);

        let err = map(
            py,
            ExecutionError::UseKeyspaceError(RustUseKeyspaceError::BadKeyspaceName(
                RustBadKeyspaceName::Empty,
            )),
        );
        assert_class::<BadKeyspaceName>(py, &err);
        assert_bases!(py, err, UseKeyspaceError, PyExecutionError, PyValueError);
        assert_eq!(message(py, &err), MESSAGE);

        let err = map(
            py,
            ExecutionError::SchemaAgreementError(RustSchemaAgreementError::Timeout(
                Duration::from_secs(1),
            )),
        );
        assert_class::<SchemaAgreementTimeout>(py, &err);
        assert_eq!(message(py, &err), MESSAGE);

        let err = map(
            py,
            ExecutionError::MetadataError(RustMetadataError::Peers(PeersMetadataError::EmptyPeers)),
        );
        assert_class::<InvalidClusterMetadata>(py, &err);
        assert_bases!(py, err, MetadataError, PyExecutionError);
        assert_eq!(message(py, &err), MESSAGE);
    });
}

/* BadQuery */

#[test]
fn partition_key_extraction_is_a_bad_query() {
    Python::initialize();
    Python::attach(|py| {
        let err = map(
            py,
            ExecutionError::BadQuery(RustBadQuery::PartitionKeyExtraction),
        );
        assert_class::<PartitionKeyExtractionFailed>(py, &err);
        assert_bases!(py, err, BadQuery, PyValueError, ScyllaError);
        assert!(!err.is_instance_of::<PyExecutionError>(py));
        assert_eq!(message(py, &err), MESSAGE);
    });
}

#[test]
fn bad_query_serialization_error_is_a_serialization_error() {
    Python::initialize();
    Python::attach(|py| {
        let err = map(
            py,
            ExecutionError::BadQuery(RustBadQuery::SerializationError(
                RustSerializationError::new(io::Error::other("cannot serialize")),
            )),
        );
        // A failure that is neither ours nor a known Rust driver one.
        assert_class::<SerializeFailedError>(py, &err);
        assert_bases!(
            py,
            err,
            ValueSerializationError,
            SerializationError,
            BadQuery
        );
        assert_eq!(message(py, &err), MESSAGE);
        assert!(attr(py, &err, "bind_marker").is_none());
    });
}

#[test]
fn values_too_long_for_key_keeps_length_and_max_length_apart() {
    Python::initialize();
    Python::attach(|py| {
        let err = map(
            py,
            ExecutionError::BadQuery(RustBadQuery::ValuesTooLongForKey(70000, 65535)),
        );
        assert_class::<ValuesTooLongForKey>(py, &err);
        assert_bases!(py, err, BadQuery, PyValueError, ScyllaError);
        assert_eq!(message(py, &err), MESSAGE);
        assert_int(py, &err, "length", 70000);
        assert_int(py, &err, "max_length", 65535);
    });
}

#[test]
fn too_many_statements_in_batch_keeps_count() {
    Python::initialize();
    Python::attach(|py| {
        let err = map(
            py,
            ExecutionError::BadQuery(RustBadQuery::TooManyQueriesInBatchStatement(70001)),
        );
        assert_class::<TooManyStatementsInBatch>(py, &err);
        assert_bases!(py, err, BadQuery, PyValueError, ScyllaError);
        assert_eq!(message(py, &err), MESSAGE);
        assert_int(py, &err, "count", 70001);
    });
}

/* ConnectionPoolError */

#[test]
fn connection_pool_errors_map_to_their_classes() {
    Python::initialize();
    Python::attach(|py| {
        let broken = map(
            py,
            ExecutionError::ConnectionPoolError(RustConnectionPoolError::Broken {
                last_connection_error: ConnectionError::ConnectTimeout,
            }),
        );
        assert_class::<ConnectionPoolBroken>(py, &broken);
        assert_bases!(
            py,
            broken,
            ConnectionPoolError,
            PyExecutionError,
            ScyllaError
        );
        assert_eq!(message(py, &broken), MESSAGE);

        let initializing = map(
            py,
            ExecutionError::ConnectionPoolError(RustConnectionPoolError::Initializing),
        );
        assert_class::<ConnectionPoolInitializing>(py, &initializing);
        assert_bases!(py, initializing, ConnectionPoolError, PyExecutionError);
        assert_eq!(message(py, &initializing), MESSAGE);

        let disabled = map(
            py,
            ExecutionError::ConnectionPoolError(RustConnectionPoolError::NodeDisabledByHostFilter),
        );
        assert_class::<NodeDisabledByHostFilter>(py, &disabled);
        assert_bases!(py, disabled, ConnectionPoolError, PyExecutionError);
        assert_eq!(message(py, &disabled), MESSAGE);
    });
}

/* RequestAttemptError */

#[test]
fn attempt_serialization_error_is_a_serialization_error() {
    Python::initialize();
    Python::attach(|py| {
        let err = attempt_error(
            py,
            RequestAttemptError::SerializationError(RustSerializationError::new(io::Error::other(
                "cannot serialize",
            ))),
        );
        assert_class::<SerializeFailedError>(py, &err);
        assert_eq!(message(py, &err), MESSAGE);
    });
}

#[test]
fn request_serialization_error_is_a_protocol_error() {
    Python::initialize();
    Python::attach(|py| {
        let err = attempt_error(
            py,
            RequestAttemptError::CqlRequestSerialization(
                CqlRequestSerializationError::SnapCompressError(Arc::new(io::Error::other(
                    "compression failed",
                ))),
            ),
        );
        assert_class::<RequestSerializationError>(py, &err);
        assert_bases!(py, err, ProtocolError, RequestFailedError, PyExecutionError);
        assert_eq!(message(py, &err), MESSAGE);
    });
}

#[test]
fn unable_to_alloc_stream_id_is_connection_busy() {
    Python::initialize();
    Python::attach(|py| {
        let err = attempt_error(py, RequestAttemptError::UnableToAllocStreamId);
        assert_class::<ConnectionBusy>(py, &err);
        assert_bases!(py, err, RequestFailedError, PyExecutionError);
        assert_eq!(message(py, &err), MESSAGE);
    });
}

#[test]
fn broken_connection_is_a_request_failure() {
    Python::initialize();
    Python::attach(|py| {
        let err = attempt_error(
            py,
            RequestAttemptError::BrokenConnectionError(
                BrokenConnectionErrorKind::ChannelError.into(),
            ),
        );
        assert_class::<BrokenConnection>(py, &err);
        assert_bases!(py, err, RequestFailedError, PyExecutionError);
        assert_eq!(message(py, &err), MESSAGE);
    });
}

#[test]
fn every_parse_error_is_a_response_parse_error() {
    Python::initialize();
    Python::attach(|py| {
        let errors = [
            RequestAttemptError::BodyExtensionsParseError(
                FrameBodyExtensionsParseError::NoCompressionNegotiated,
            ),
            RequestAttemptError::CqlResultParseError(CqlResultParseError::UnknownResultId(17)),
            RequestAttemptError::CqlErrorParseError(CqlErrorParseError::ErrorCodeParseError(
                LowLevelDeserializationError::InvalidValueLength(-5),
            )),
        ];
        for error in errors {
            let err = attempt_error(py, error);
            assert_class::<ResponseParseError>(py, &err);
            assert_bases!(py, err, ProtocolError, RequestFailedError, PyExecutionError);
            assert_eq!(message(py, &err), MESSAGE);
        }
    });
}

#[test]
fn unexpected_response_keeps_response_kind() {
    Python::initialize();
    Python::attach(|py| {
        let err = attempt_error(
            py,
            RequestAttemptError::UnexpectedResponse(CqlResponseKind::AuthChallenge),
        );
        assert_class::<UnexpectedResponse>(py, &err);
        assert_bases!(py, err, ProtocolError, RequestFailedError, PyExecutionError);
        assert_eq!(message(py, &err), MESSAGE);
        let kind = attr(py, &err, "response_kind");
        assert!(kind.is_instance_of::<PyCqlResponseKind>());
        assert_eq!(
            kind.extract::<PyCqlResponseKind>().unwrap(),
            PyCqlResponseKind::AuthChallenge
        );
    });
}

#[test]
fn reprepared_id_changed_keeps_statement_and_both_ids() {
    Python::initialize();
    Python::attach(|py| {
        let err = attempt_error(
            py,
            RequestAttemptError::RepreparedIdChanged {
                statement: "SELECT * FROM ks.t".to_string(),
                expected_id: vec![1, 2, 3],
                reprepared_id: vec![4, 5, 6, 7],
            },
        );
        assert_class::<RepreparedIdChanged>(py, &err);
        assert_bases!(py, err, PrepareError, RequestFailedError, PyExecutionError);
        assert_eq!(message(py, &err), MESSAGE);
        assert_str(py, &err, "statement", "SELECT * FROM ks.t");
        assert_bytes(py, &err, "expected_id", &[1, 2, 3]);
        assert_bytes(py, &err, "reprepared_id", &[4, 5, 6, 7]);
    });
}

#[test]
fn reprepared_id_missing_in_batch_is_a_prepare_error() {
    Python::initialize();
    Python::attach(|py| {
        let err = attempt_error(py, RequestAttemptError::RepreparedIdMissingInBatch);
        assert_class::<RepreparedIdMissingInBatch>(py, &err);
        assert_bases!(py, err, PrepareError, RequestFailedError, PyExecutionError);
        assert_eq!(message(py, &err), MESSAGE);
    });
}

#[test]
fn nonfinished_paging_state_is_a_request_failure() {
    Python::initialize();
    Python::attach(|py| {
        let err = attempt_error(py, RequestAttemptError::NonfinishedPagingState);
        assert_class::<NonfinishedPagingState>(py, &err);
        assert_bases!(py, err, RequestFailedError, PyExecutionError);
        assert_eq!(message(py, &err), MESSAGE);
    });
}

/* DbError */

/// Asserts what every `DatabaseError` shares: the bases, the message and the server's reason.
fn assert_database_error(py: Python<'_>, err: &PyErr) {
    assert_bases!(
        py,
        err,
        DatabaseError,
        RequestFailedError,
        PyExecutionError,
        ScyllaError
    );
    assert_eq!(message(py, err), MESSAGE);
    assert_reason(py, err);
}

#[test]
fn db_errors_without_fields_map_to_their_classes() {
    Python::initialize();
    Python::attach(|py| {
        fn check<T: PyTypeInfo>(py: Python<'_>, error: DbError) {
            let err = db_error(py, error);
            assert_class::<T>(py, &err);
            assert_database_error(py, &err);
        }
        check::<CqlSyntaxError>(py, DbError::SyntaxError);
        check::<InvalidRequest>(py, DbError::Invalid);
        check::<AuthenticationFailed>(py, DbError::AuthenticationError);
        check::<Unauthorized>(py, DbError::Unauthorized);
        check::<ServerConfigError>(py, DbError::ConfigError);
        check::<Overloaded>(py, DbError::Overloaded);
        check::<IsBootstrapping>(py, DbError::IsBootstrapping);
        check::<TruncateError>(py, DbError::TruncateError);
        check::<ServerError>(py, DbError::ServerError);
        check::<ServerProtocolError>(py, DbError::ProtocolError);
    });
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
        assert_class::<Unavailable>(py, &err);
        assert_database_error(py, &err);
        assert_consistency(py, &err, PyConsistency::Three);
        assert_int(py, &err, "required", 3);
        assert_int(py, &err, "alive", 1);
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
        assert_class::<ReadTimeout>(py, &err);
        assert_database_error(py, &err);
        assert_consistency(py, &err, PyConsistency::Quorum);
        assert_int(py, &err, "received", 1);
        assert_int(py, &err, "required", 2);
        assert_bool(py, &err, "data_present", true);
        // Server timeouts are not client timers.
        assert!(!err.is_instance_of::<PyTimeoutError>(py));
    });
}

#[test]
fn write_timeout_keeps_write_type() {
    Python::initialize();
    Python::attach(|py| {
        let err = db_error(
            py,
            DbError::WriteTimeout {
                consistency: Consistency::LocalQuorum,
                received: 2,
                required: 3,
                write_type: WriteType::Cas,
            },
        );
        assert_class::<WriteTimeout>(py, &err);
        assert_database_error(py, &err);
        assert_consistency(py, &err, PyConsistency::LocalQuorum);
        assert_int(py, &err, "received", 2);
        assert_int(py, &err, "required", 3);
        assert_write_type(py, &err, PyWriteType::Cas());
        assert!(!err.is_instance_of::<PyTimeoutError>(py));
    });
}

#[test]
fn read_failure_keeps_every_count() {
    Python::initialize();
    Python::attach(|py| {
        let err = db_error(
            py,
            DbError::ReadFailure {
                consistency: Consistency::All,
                received: 4,
                required: 5,
                numfailures: 6,
                data_present: false,
            },
        );
        assert_class::<ReadFailure>(py, &err);
        assert_database_error(py, &err);
        assert_consistency(py, &err, PyConsistency::All);
        assert_int(py, &err, "received", 4);
        assert_int(py, &err, "required", 5);
        assert_int(py, &err, "numfailures", 6);
        assert_bool(py, &err, "data_present", false);
    });
}

#[test]
fn write_failure_keeps_every_count_and_write_type() {
    Python::initialize();
    Python::attach(|py| {
        let err = db_error(
            py,
            DbError::WriteFailure {
                consistency: Consistency::EachQuorum,
                received: 7,
                required: 8,
                numfailures: 9,
                write_type: WriteType::Other("CUSTOM_WRITE".to_string()),
            },
        );
        assert_class::<WriteFailure>(py, &err);
        assert_database_error(py, &err);
        assert_consistency(py, &err, PyConsistency::EachQuorum);
        assert_int(py, &err, "received", 7);
        assert_int(py, &err, "required", 8);
        assert_int(py, &err, "numfailures", 9);
        assert_write_type(
            py,
            &err,
            PyWriteType::Other {
                value: "CUSTOM_WRITE".to_string(),
            },
        );
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
        assert_class::<AlreadyExists>(py, &err);
        assert_database_error(py, &err);
        assert_str(py, &err, "keyspace", "ks");
        assert_str(py, &err, "table", "tbl");
    });
}

#[test]
fn function_failure_keeps_keyspace_function_and_arg_types() {
    Python::initialize();
    Python::attach(|py| {
        let err = db_error(
            py,
            DbError::FunctionFailure {
                keyspace: "fks".to_string(),
                function: "my_fn".to_string(),
                arg_types: vec!["int".to_string(), "text".to_string()],
            },
        );
        assert_class::<FunctionFailure>(py, &err);
        assert_database_error(py, &err);
        assert_str(py, &err, "keyspace", "fks");
        assert_str(py, &err, "function", "my_fn");
        let arg_types = attr(py, &err, "arg_types");
        assert_eq!(type_name(&arg_types), "list");
        assert_eq!(arg_types.extract::<Vec<String>>().unwrap(), ["int", "text"]);
    });
}

#[test]
fn unprepared_keeps_statement_id_as_bytes() {
    Python::initialize();
    Python::attach(|py| {
        let err = db_error(
            py,
            DbError::Unprepared {
                statement_id: Bytes::from_static(b"\xde\xad\xbe\xef"),
            },
        );
        assert_class::<Unprepared>(py, &err);
        assert_database_error(py, &err);
        assert_bytes(py, &err, "statement_id", b"\xde\xad\xbe\xef");
    });
}

#[test]
fn rate_limit_reached_keeps_op_type_and_coordinator_flag() {
    Python::initialize();
    Python::attach(|py| {
        for (op_type, expected, rejected_by_coordinator) in [
            (OperationType::Read, PyOperationType::Read(), true),
            (OperationType::Write, PyOperationType::Write(), false),
            (
                OperationType::Other(42),
                PyOperationType::Other { code: 42 },
                true,
            ),
        ] {
            let err = db_error(
                py,
                DbError::RateLimitReached {
                    op_type,
                    rejected_by_coordinator,
                },
            );
            assert_class::<RateLimitReached>(py, &err);
            assert_database_error(py, &err);
            let value = attr(py, &err, "op_type");
            assert!(value.is_instance_of::<PyOperationType>());
            assert_eq!(value.extract::<PyOperationType>().unwrap(), expected);
            assert_bool(py, &err, "rejected_by_coordinator", rejected_by_coordinator);
        }
    });
}

#[test]
fn unknown_db_error_keeps_code() {
    Python::initialize();
    Python::attach(|py| {
        let err = db_error(py, DbError::Other(0x7777));
        assert_class::<UnknownDatabaseError>(py, &err);
        assert_database_error(py, &err);
        assert_int(py, &err, "code", 0x7777);
    });
}

/* Errors of operations other than execute, mapped by cause */

#[test]
fn prepare_errors_map_by_cause() {
    Python::initialize();
    Python::attach(|py| {
        fn prepare(py: Python<'_>, error: RustPrepareError) -> PyErr {
            let err: PyErr = DriverPrepareError::from(error.clone()).into();
            assert_eq!(
                message(py, &err),
                format!("Failed to prepare statement: {error}")
            );
            err
        }

        let err = prepare(py, RustPrepareError::PreparedStatementIdsMismatch);
        assert_class::<PreparedStatementIdsMismatch>(py, &err);
        assert_bases!(py, err, PrepareError, PyExecutionError, ScyllaError);

        let err = prepare(
            py,
            RustPrepareError::ConnectionPoolError(RustConnectionPoolError::Initializing),
        );
        assert_class::<ConnectionPoolInitializing>(py, &err);

        // The first attempt's failure decides the class, so server errors keep their fields.
        let err = prepare(
            py,
            RustPrepareError::AllAttemptsFailed {
                first_attempt: RequestAttemptError::DbError(
                    DbError::SyntaxError,
                    REASON.to_string(),
                ),
            },
        );
        assert_class::<CqlSyntaxError>(py, &err);
        assert_reason(py, &err);
    });
}

#[test]
fn use_keyspace_errors_map_by_cause() {
    Python::initialize();
    let (join_error, join_description) = join_error();
    let join_failed: PyErr = DriverUseKeyspaceError::RuntimeTaskJoinFailed(join_error).into();
    Python::attach(|py| {
        fn use_keyspace(py: Python<'_>, error: RustUseKeyspaceError) -> PyErr {
            let expected = error.to_string();
            let err: PyErr = DriverUseKeyspaceError::from(error).into();
            // Transparent: the Rust driver's message.
            assert_eq!(message(py, &err), expected);
            err
        }

        let err = use_keyspace(
            py,
            RustUseKeyspaceError::BadKeyspaceName(RustBadKeyspaceName::IllegalCharacter(
                "bad-ks".to_string(),
                '-',
            )),
        );
        assert_class::<BadKeyspaceName>(py, &err);
        assert_bases!(py, err, UseKeyspaceError, PyExecutionError, PyValueError);

        let err = use_keyspace(
            py,
            RustUseKeyspaceError::RequestError(RequestAttemptError::UnableToAllocStreamId),
        );
        assert_class::<ConnectionBusy>(py, &err);

        let err = use_keyspace(
            py,
            RustUseKeyspaceError::KeyspaceNameMismatch {
                expected_keyspace_name_lowercase: "expected_ks".to_string(),
                result_keyspace_name_lowercase: "received_ks".to_string(),
            },
        );
        assert_class::<KeyspaceNameMismatch>(py, &err);
        assert_bases!(py, err, UseKeyspaceError, PyExecutionError);
        assert_str(py, &err, "expected", "expected_ks");
        assert_str(py, &err, "received", "received_ks");

        let err = use_keyspace(
            py,
            RustUseKeyspaceError::RequestTimeout(Duration::from_millis(750)),
        );
        assert_class::<OperationTimedOut>(py, &err);
        assert_bases!(py, err, PyTimeoutError);
        assert_timedelta(py, &err, "timeout", Duration::from_millis(750));

        assert_class::<InternalDriverError>(py, &join_failed);
        assert_eq!(
            message(py, &join_failed),
            format!("runtime error while using keyspace: {join_description}")
        );
    });
}

#[test]
fn schema_agreement_errors_map_by_cause() {
    Python::initialize();
    let (join_error, join_description) = join_error();
    let join_failed: PyErr = DriverSchemaAgreementError::RuntimeTaskJoinFailed(join_error).into();
    Python::attach(|py| {
        fn agreement(py: Python<'_>, error: RustSchemaAgreementError) -> PyErr {
            let err: PyErr =
                DriverSchemaAgreementError::rust_driver_schema_agreement_error(error.clone())
                    .into();
            assert_eq!(
                message(py, &err),
                format!("Failed to check schema agreement: {error}")
            );
            err
        }

        let err = agreement(
            py,
            RustSchemaAgreementError::Timeout(Duration::from_millis(2500)),
        );
        assert_class::<SchemaAgreementTimeout>(py, &err);
        assert_bases!(
            py,
            err,
            SchemaAgreementError,
            OperationTimedOut,
            PyTimeoutError,
            PyExecutionError
        );
        assert_timedelta(py, &err, "timeout", Duration::from_millis(2500));

        let host_id = Uuid::from_u128(0x1234_5678_9abc_def0_1122_3344_5566_7788);
        let err = agreement(py, RustSchemaAgreementError::RequiredHostAbsent(host_id));
        assert_class::<RequiredHostAbsent>(py, &err);
        assert_bases!(py, err, SchemaAgreementError, PyExecutionError);
        let value = attr(py, &err, "host_id");
        assert_eq!(type_name(&value), "UUID");
        assert_eq!(value.extract::<Uuid>().unwrap(), host_id);

        let err = agreement(
            py,
            RustSchemaAgreementError::ConnectionPoolError(
                RustConnectionPoolError::NodeDisabledByHostFilter,
            ),
        );
        assert_class::<NodeDisabledByHostFilter>(py, &err);

        let err = agreement(
            py,
            RustSchemaAgreementError::PrepareError(RustPrepareError::PreparedStatementIdsMismatch),
        );
        assert_class::<PreparedStatementIdsMismatch>(py, &err);

        let err = agreement(
            py,
            RustSchemaAgreementError::RequestError(RequestAttemptError::NonfinishedPagingState),
        );
        assert_class::<NonfinishedPagingState>(py, &err);

        let err = agreement(
            py,
            RustSchemaAgreementError::SingleRowError(SingleRowError::UnexpectedRowCount(2)),
        );
        assert_class::<ResponseParseError>(py, &err);

        assert_class::<InternalDriverError>(py, &join_failed);
        assert_eq!(
            message(py, &join_failed),
            format!("runtime error while checking schema agreement: {join_description}")
        );
    });
}

#[test]
fn metadata_refresh_errors_map_by_cause() {
    Python::initialize();
    let (join_error, join_description) = join_error();
    let join_failed: PyErr = DriverMetadataRefreshError::RuntimeTaskJoinFailed(join_error).into();
    Python::attach(|py| {
        fn refresh(py: Python<'_>, error: RustMetadataError) -> PyErr {
            let err: PyErr =
                DriverMetadataRefreshError::rust_driver_metadata_error(error.clone()).into();
            assert_eq!(
                message(py, &err),
                format!("Failed to refresh metadata: {error}")
            );
            err
        }

        // Metadata the driver cannot make sense of.
        for error in [
            RustMetadataError::Peers(PeersMetadataError::EmptyTokenLists),
            RustMetadataError::Keyspaces(KeyspacesMetadataError::Strategy {
                keyspace: "ks".to_string(),
                error: KeyspaceStrategyError::MissingClassForStrategyDefinition,
            }),
            RustMetadataError::Udts(UdtMetadataError::CircularTypeDependency),
            RustMetadataError::Tables(TablesMetadataError::UnknownColumnKind {
                keyspace_name: "ks".to_string(),
                table_name: "t".to_string(),
                column_name: "c".to_string(),
                column_kind: "weird".to_string(),
            }),
            RustMetadataError::ClientRoutes(ClientRoutesMetadataError::InvalidPortValue {
                port: -1,
                is_tls: true,
            }),
        ] {
            let err = refresh(py, error);
            assert_class::<InvalidClusterMetadata>(py, &err);
            assert_bases!(py, err, MetadataError, PyExecutionError, ScyllaError);
        }

        let err = refresh(
            py,
            RustMetadataError::ConnectionPoolError(RustConnectionPoolError::Initializing),
        );
        assert_class::<ConnectionPoolInitializing>(py, &err);

        assert_class::<InternalDriverError>(py, &join_failed);
        assert_eq!(
            message(py, &join_failed),
            format!("runtime error while refreshing metadata: {join_description}")
        );
    });
}

#[test]
fn new_session_errors_map_by_cause() {
    Python::initialize();
    let (join_error, join_description) = join_error();
    let join_failed: PyErr = DriverSessionConnectionError::RuntimeTaskJoinFailed(join_error).into();
    Python::attach(|py| {
        fn connect(py: Python<'_>, error: NewSessionError) -> PyErr {
            let expected = format!("failed to establish session: {error}");
            let err: PyErr = DriverSessionConnectionError::new_session_error(error).into();
            assert_eq!(message(py, &err), expected);
            err
        }

        let err = connect(
            py,
            NewSessionError::FailedToResolveAnyHostname(vec![
                "first.invalid".to_string(),
                "second.invalid".to_string(),
            ]),
        );
        assert_class::<HostnameResolutionFailed>(py, &err);
        assert_bases!(py, err, ContactPointError, ScyllaError);
        let hostnames = attr(py, &err, "hostnames");
        assert_eq!(type_name(&hostnames), "list");
        assert_eq!(
            hostnames.extract::<Vec<String>>().unwrap(),
            ["first.invalid", "second.invalid"]
        );

        let err = connect(py, NewSessionError::EmptyKnownNodesList);
        assert_class::<NoKnownNodes>(py, &err);
        assert_bases!(py, err, ContactPointError, ScyllaError);

        let err = connect(
            py,
            NewSessionError::MetadataError(RustMetadataError::Peers(
                PeersMetadataError::EmptyPeers,
            )),
        );
        assert_class::<InvalidClusterMetadata>(py, &err);

        let err = connect(
            py,
            NewSessionError::UseKeyspaceError(RustUseKeyspaceError::BadKeyspaceName(
                RustBadKeyspaceName::Empty,
            )),
        );
        assert_class::<BadKeyspaceName>(py, &err);

        let err = connect(
            py,
            NewSessionError::IllegalConfig("conflicting options".into()),
        );
        assert_class::<SessionConfigError>(py, &err);
        assert_bases!(py, err, ScyllaError);

        assert_class::<InternalDriverError>(py, &join_failed);
        assert_eq!(
            message(py, &join_failed),
            format!("runtime error while creating session: {join_description}")
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
fn failed_trace_prepare_raises_the_prepare_class() {
    Python::initialize();
    let err = TracingError::PrepareError(RustPrepareError::PreparedStatementIdsMismatch);
    Python::attach(|py| {
        let err = tracing_error_to_pyerr(py, &err, MESSAGE.to_string());
        assert_class::<PreparedStatementIdsMismatch>(py, &err);
        assert!(!err.is_instance_of::<TraceUnavailable>(py));
        assert_eq!(message(py, &err), MESSAGE);
    });
}

#[test]
fn missing_trace_raises_trace_unavailable() {
    Python::initialize();
    let err = TracingError::EmptyResults;
    Python::attach(|py| {
        let err = tracing_error_to_pyerr(py, &err, err.to_string());
        assert_class::<TraceUnavailable>(py, &err);
        assert_bases!(py, err, PyExecutionError, ScyllaError);
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
fn trace_fetch_errors_keep_their_description() {
    Python::initialize();
    let (join_error, join_description) = join_error();
    let join_failed: PyErr = DriverTraceError::RuntimeTaskJoinFailed(join_error).into();
    let failed: PyErr =
        DriverTraceError::rust_driver_tracing_error(TracingError::EmptyResults).into();
    Python::attach(|py| {
        assert_class::<InternalDriverError>(py, &join_failed);
        assert_eq!(
            message(py, &join_failed),
            format!("runtime error while fetching a query trace: {join_description}")
        );
        assert_class::<TraceUnavailable>(py, &failed);
        assert_eq!(
            message(py, &failed),
            format!(
                "Failed to fetch the query trace: {}",
                TracingError::EmptyResults
            )
        );
    });
}

#[test]
fn unlocated_serialization_failure_has_none_bind_marker() {
    Python::initialize();
    let err = RustSerializationError::new(std::fmt::Error);
    Python::attach(|py| {
        let err = serialization_error_to_pyerr(py, &err, err.to_string());
        assert!(err.is_instance_of::<SerializeFailedError>(py));
        assert!(err.value(py).getattr("bind_marker").unwrap().is_none());
    });
}

/* Errors of `execution.rs` that a cluster cannot produce on demand */

#[test]
fn execute_paging_state_on_unpaged_execution_is_not_allowed() {
    Python::initialize();
    let err: PyErr = DriverExecuteError::paging_state_must_be_none_for_unpaged_execution().into();
    Python::attach(|py| {
        assert_class::<PagingStateNotAllowed>(py, &err);
        assert_bases!(py, err, BadQuery, PyValueError, ScyllaError);
        assert_eq!(
            message(py, &err),
            "Paging state must be None for unpaged execution"
        );
    });
}

#[test]
fn execute_serialization_failure_is_a_serialization_error() {
    Python::initialize();
    let source = RustSerializationError::new(io::Error::other("bad value"));
    // The source's description is the Rust driver's wording.
    let expected = format!("Failed to serialize values: {source}");
    let err: PyErr = DriverExecuteError::serialization_failed(source).into();
    Python::attach(|py| {
        assert_class::<SerializeFailedError>(py, &err);
        assert_bases!(py, err, SerializationError, BadQuery, ScyllaError);
        assert!(!err.is_instance_of::<PyExecutionError>(py));
        assert_eq!(message(py, &err), expected);
    });
}

#[test]
fn execute_driver_error_is_described_as_failed_execution() {
    Python::initialize();
    let err: PyErr =
        DriverExecuteError::rust_driver_execution_error(ExecutionError::EmptyPlan).into();
    Python::attach(|py| {
        assert_class::<NoHostAvailable>(py, &err);
        assert_eq!(
            message(py, &err),
            format!("Failed to execute statement: {}", ExecutionError::EmptyPlan)
        );
    });
}

#[test]
fn execute_runtime_task_join_failure_is_internal() {
    Python::initialize();
    let (join_error, join_description) = join_error();
    let err: PyErr = DriverExecuteError::RuntimeTaskJoinFailed(join_error).into();
    Python::attach(|py| {
        assert_class::<InternalDriverError>(py, &err);
        assert_bases!(py, err, ScyllaError);
        assert_eq!(
            message(py, &err),
            format!("runtime error while executing query: {join_description}")
        );
    });
}

#[test]
fn statement_string_conversion_failure_chains_the_unicode_error() {
    Python::initialize();
    Python::attach(|py| {
        // A lone surrogate is a valid Python `str` that has no UTF-8 encoding.
        let text = py.eval(c"'SELECT \\ud800'", None, None).unwrap();
        let source = text.cast::<PyString>().unwrap().to_str().unwrap_err();
        let source_value = source.value(py).clone();

        let err: PyErr =
            DriverStatementConversionError::statement_string_conversion_failed(source).into();
        assert_class::<StatementConversionError>(py, &err);
        assert_bases!(py, err, BadQuery, PyValueError, ScyllaError);
        assert_eq!(
            message(py, &err),
            "Failed to convert statement string to Rust string"
        );
        let cause = err.cause(py).expect("the UnicodeEncodeError is chained");
        assert!(cause.is_instance_of::<PyUnicodeEncodeError>(py));
        assert!(cause.value(py).is(&source_value));
    });
}

#[test]
fn statement_conversion_type_errors_are_type_errors() {
    Python::initialize();
    Python::attach(|py| {
        let err: PyErr = DriverStatementConversionError::cannot_prepare_prepared_statement().into();
        assert_class::<AlreadyPrepared>(py, &err);
        assert_bases!(py, err, StatementConversionError, PyTypeError, BadQuery);
        assert_eq!(
            message(py, &err),
            "Cannot prepare a PreparedStatement; expected a str or Statement"
        );

        let number = 42_i32.into_pyobject(py).unwrap().into_any();
        let err: PyErr =
            DriverStatementConversionError::invalid_statement_type(number.as_borrowed()).into();
        assert_class::<InvalidStatementType>(py, &err);
        assert_bases!(py, err, StatementConversionError, PyTypeError, BadQuery);
        assert_eq!(
            message(py, &err),
            "Invalid statement type: expected a str, Statement, or PreparedStatement, got int"
        );
    });
}

#[test]
fn python_conversion_error_is_raised_unchanged() {
    Python::initialize();
    Python::attach(|py| {
        let source = PyValueError::new_err("row factory rejected the session");
        let source_value = source.value(py).clone();

        let err: PyErr = DriverSessionConnectionError::python_conversion_error(source).into();
        // Transparent: the Python exception itself, not wrapped in a driver exception.
        assert_class::<PyValueError>(py, &err);
        assert!(!err.is_instance_of::<ScyllaError>(py));
        assert!(err.value(py).is(&source_value));
        assert_eq!(message(py, &err), "row factory rejected the session");
    });
}

/* Error attribute types (`errors/types.rs`) */

/// Moves `value` into a Python object, checks it compares equal in Python to `expected` (built
/// from Python), and extracts it back.
fn round_trip<'py, P>(py: Python<'py>, value: P, expected: &Bound<'py, PyAny>) -> P
where
    P: IntoPyObjectExt<'py> + FromPyObjectOwned<'py>,
{
    // As the driver converts it: `Bound::new` would build the base class, not the variant's.
    let object = value.into_bound_py_any(py).unwrap();
    assert!(object.eq(expected).unwrap(), "{object} != {expected}");
    assert!(!object.ne(expected).unwrap());
    object.extract::<P>().map_err(Into::<PyErr>::into).unwrap()
}

#[test]
fn every_write_type_round_trips() {
    Python::initialize();
    Python::attach(|py| {
        let write_type = py.get_type::<PyWriteType>();
        let cases = [
            (WriteType::Simple, "Simple"),
            (WriteType::Batch, "Batch"),
            (WriteType::UnloggedBatch, "UnloggedBatch"),
            (WriteType::Counter, "Counter"),
            (WriteType::BatchLog, "BatchLog"),
            (WriteType::Cas, "Cas"),
            (WriteType::View, "View"),
            (WriteType::Cdc, "Cdc"),
        ];
        for (rust, name) in &cases {
            // Built from Python, as a user would.
            let class = write_type.getattr(*name).unwrap();
            let expected = class.call0().unwrap();
            let converted = PyWriteType::from(rust.clone())
                .into_bound_py_any(py)
                .unwrap();
            assert!(
                converted.is_instance(&class).unwrap(),
                "{rust:?} is not a WriteType.{name}"
            );
            let back = round_trip(py, PyWriteType::from(rust.clone()), &expected);
            assert_eq!(&WriteType::from(back), rust);
            for (other, other_name) in &cases {
                if other != rust {
                    let other = PyWriteType::from(other.clone())
                        .into_bound_py_any(py)
                        .unwrap();
                    assert!(!expected.eq(other).unwrap(), "{name} == {other_name}");
                }
            }
        }

        let custom = WriteType::Other("CUSTOM".to_string());
        let other_class = write_type.getattr("Other").unwrap();
        let expected = other_class.call1(("CUSTOM",)).unwrap();
        let back = round_trip(py, PyWriteType::from(custom.clone()), &expected);
        assert_eq!(WriteType::from(back), custom);
        assert_eq!(
            expected
                .getattr("value")
                .unwrap()
                .extract::<String>()
                .unwrap(),
            "CUSTOM"
        );
        // The payload takes part in equality.
        let different = other_class.call1(("DIFFERENT",)).unwrap();
        assert!(!expected.eq(different).unwrap());
    });
}

#[test]
fn every_operation_type_round_trips() {
    Python::initialize();
    Python::attach(|py| {
        let operation_type = py.get_type::<PyOperationType>();
        let other_class = operation_type.getattr("Other").unwrap();
        let cases = [
            (
                OperationType::Read,
                operation_type.getattr("Read").unwrap().call0().unwrap(),
            ),
            (
                OperationType::Write,
                operation_type.getattr("Write").unwrap().call0().unwrap(),
            ),
            (OperationType::Other(7), other_class.call1((7,)).unwrap()),
        ];
        for (rust, expected) in &cases {
            let converted = PyOperationType::from(rust.clone())
                .into_bound_py_any(py)
                .unwrap();
            assert!(
                converted.is_instance(&expected.get_type()).unwrap(),
                "{rust:?} is not a {}",
                expected.get_type()
            );
            let back = round_trip(py, PyOperationType::from(rust.clone()), expected);
            assert_eq!(&OperationType::from(back), rust);
        }
        assert!(!cases[0].1.eq(&cases[1].1).unwrap());
        assert!(!cases[1].1.eq(&cases[2].1).unwrap());
        let other = &cases[2].1;
        assert_eq!(other.getattr("code").unwrap().extract::<u8>().unwrap(), 7);
        // The code takes part in equality.
        assert!(!other.eq(other_class.call1((8,)).unwrap()).unwrap());
    });
}

#[test]
fn every_cql_response_kind_round_trips() {
    Python::initialize();
    Python::attach(|py| {
        let response_kind = py.get_type::<PyCqlResponseKind>();
        let cases = [
            (CqlResponseKind::Error, "Error"),
            (CqlResponseKind::Ready, "Ready"),
            (CqlResponseKind::Authenticate, "Authenticate"),
            (CqlResponseKind::Supported, "Supported"),
            (CqlResponseKind::Result, "Result"),
            (CqlResponseKind::Event, "Event"),
            (CqlResponseKind::AuthChallenge, "AuthChallenge"),
            (CqlResponseKind::AuthSuccess, "AuthSuccess"),
        ];
        for (index, (rust, name)) in cases.iter().enumerate() {
            let expected = response_kind.getattr(*name).unwrap();
            let back = round_trip(py, PyCqlResponseKind::from(*rust), &expected);
            // `CqlResponseKind` has no `PartialEq`; its `Debug` output is the variant name.
            assert_eq!(format!("{:?}", CqlResponseKind::from(back)), *name);

            let object = PyCqlResponseKind::from(*rust)
                .into_bound_py_any(py)
                .unwrap();
            // `eq_int`: equal to its position in declaration order.
            assert!(object.eq(index).unwrap(), "{name} != {index}");
            assert!(!object.eq(index + 1).unwrap());
            assert_eq!(
                object.repr().unwrap().to_string(),
                format!("CqlResponseKind.{name}")
            );
        }
    });
}
