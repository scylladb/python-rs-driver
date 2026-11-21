//! The mapping from Rust driver errors to Python exceptions is public API: it decides which
//! `except` clause catches a failure and which attributes the handler can read. These tests pin
//! the exact class, the message and every attribute with its Python type, for every branch of
//! the mapping that the Python tests cannot reach. The one exception is the metadata fetch
//! failure path (`metadata_fetch_error_to_pyerr` and the `request_error_to_pyerr` it calls):
//! only the Rust driver can build its input. The base classes a user may catch instead are
//! checked once per class, in `exception_classes_have_the_bases_users_catch`.

use std::io;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use bytes::Bytes;
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::{PyTimeoutError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::type_object::PyTypeInfo;
use scylla::deserialize::{DeserializationError, TypeCheckError};
use scylla::errors::{
    BadKeyspaceName as RustBadKeyspaceName, BadQuery as RustBadQuery, BrokenConnectionErrorKind,
    ClientRoutesMetadataError, ConnectionError, ConnectionPoolError as RustConnectionPoolError,
    CqlErrorParseError, CqlRequestSerializationError, CqlResponseKind, CqlResultParseError,
    DbError, ExecutionError, FrameBodyExtensionsParseError, IntoRowsResultError,
    KeyspaceStrategyError, KeyspacesMetadataError, MetadataError as RustMetadataError,
    NewSessionError, OperationType, PeersMetadataError, PrepareError as RustPrepareError,
    RequestAttemptError, SchemaAgreementError as RustSchemaAgreementError, SingleRowError,
    TablesMetadataError, TracingError, UdtMetadataError, UseKeyspaceError as RustUseKeyspaceError,
    WriteType,
};
use scylla::frame::frame_errors::{
    LowLevelDeserializationError, ResultMetadataAndRowsCountParseError,
};
use scylla::serialize::SerializationError as RustSerializationError;
use scylla::statement::Consistency;
use uuid::Uuid;

use crate::enums::PyConsistency;
use crate::errors::execution::{
    DriverExecuteError, DriverMetadataRefreshError, DriverPrepareError, DriverSchemaAgreementError,
    DriverSessionConnectionError, DriverTraceError, DriverUseKeyspaceError,
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

fn message(py: Python<'_>, err: &PyErr) -> String {
    err.value(py).str().unwrap().to_string()
}

/// Asserts that `err` is exactly of class `T` and carries `expected_message`.
fn assert_mapped<T: PyTypeInfo>(py: Python<'_>, err: &PyErr, expected_message: &str) {
    assert_class::<T>(py, err);
    assert_eq!(message(py, err), expected_message);
}

fn attr<'py>(py: Python<'py>, err: &PyErr, name: &str) -> Bound<'py, PyAny> {
    err.value(py)
        .getattr(name)
        .unwrap_or_else(|e| panic!("{} has no attribute {name}: {e}", err.get_type(py)))
}

/// Asserts that attribute `name` is of the Python type called `type_name` and equals `expected`.
///
/// The type is checked by name, so an `int` is not accepted where a `bool` is expected.
fn assert_attr<'py, T>(py: Python<'py>, err: &PyErr, name: &str, type_name: &str, expected: T)
where
    T: FromPyObjectOwned<'py> + PartialEq + std::fmt::Debug,
{
    let value = attr(py, err, name);
    let actual_type = value.get_type().name().unwrap().to_string();
    assert_eq!(actual_type, type_name, "type of {name}");
    let value: T = value.extract().map_err(Into::<PyErr>::into).unwrap();
    assert_eq!(value, expected, "value of {name}");
}

/// Asserts that attribute `name` is an instance of the Python class of `T` and equals `expected`.
fn assert_class_attr<'py, T>(py: Python<'py>, err: &PyErr, name: &str, expected: T)
where
    T: PyTypeInfo + FromPyObjectOwned<'py> + PartialEq + std::fmt::Debug,
{
    let value = attr(py, err, name);
    assert!(value.is_instance_of::<T>(), "type of {name}");
    let value: T = value.extract().map_err(Into::<PyErr>::into).unwrap();
    assert_eq!(value, expected, "value of {name}");
}

/// Asserts that `err` is exactly the `DatabaseError` subclass `T`, with the message and the
/// `reason` attribute every `DatabaseError` carries: the server's message.
fn assert_database_error<T: PyTypeInfo>(py: Python<'_>, err: &PyErr) {
    assert_mapped::<T>(py, err, MESSAGE);
    assert_attr(py, err, "reason", "str", REASON.to_string());
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

/* The exception hierarchy */

/// Asserts that the class before the colon is a subclass of every class after it.
macro_rules! assert_subclass {
    ($py: expr, $class: ty: $($base: ty),+ $(,)?) => {
        $(
            assert!(
                $py.get_type::<$class>().is_subclass_of::<$base>().unwrap(),
                "{} is not a subclass of {}",
                stringify!($class),
                stringify!($base),
            );
        )+
    };
}

/// Asserts that the class before the colon is a subclass of none of the classes after it.
macro_rules! assert_not_subclass {
    ($py: expr, $class: ty: $($base: ty),+ $(,)?) => {
        $(
            assert!(
                !$py.get_type::<$class>().is_subclass_of::<$base>().unwrap(),
                "{} is a subclass of {}",
                stringify!($class),
                stringify!($base),
            );
        )+
    };
}

/// The bases a user may catch each exception as. They are fixed by the class definitions, so
/// they are checked here once and the mapping tests check only the exact class.
#[test]
fn exception_classes_have_the_bases_users_catch() {
    Python::initialize();
    Python::attach(|py| {
        assert_subclass!(py, PyExecutionError: ScyllaError);
        assert_subclass!(py, NoHostAvailable: PyExecutionError);
        assert_subclass!(py, OperationTimedOut: PyExecutionError, PyTimeoutError);
        assert_subclass!(py, TraceUnavailable: PyExecutionError);
        assert_subclass!(py, InternalDriverError: ScyllaError);
        assert_subclass!(py, SessionConfigError: ScyllaError);

        // Invalid input: not execution errors, and catchable as the built-in they resemble.
        assert_subclass!(py, BadQuery: ScyllaError);
        assert_not_subclass!(py, BadQuery: PyExecutionError);
        assert_subclass!(py, PartitionKeyExtractionFailed: BadQuery, PyValueError);
        assert_subclass!(py, ValuesTooLongForKey: BadQuery, PyValueError);
        assert_subclass!(py, TooManyStatementsInBatch: BadQuery, PyValueError);
        assert_subclass!(py, PagingStateNotAllowed: BadQuery, PyValueError);
        assert_subclass!(py, SerializationError: BadQuery);
        assert_subclass!(py, ValueSerializationError: SerializationError);
        assert_subclass!(py, SerializeFailedError: ValueSerializationError);
        assert_subclass!(py, StatementConversionError: BadQuery, PyValueError);
        assert_subclass!(py, AlreadyPrepared: StatementConversionError, PyTypeError);
        assert_subclass!(py, InvalidStatementType: StatementConversionError, PyTypeError);

        assert_subclass!(py, ConnectionPoolError: PyExecutionError);
        assert_subclass!(py, ConnectionPoolBroken: ConnectionPoolError);
        assert_subclass!(py, ConnectionPoolInitializing: ConnectionPoolError);
        assert_subclass!(py, NodeDisabledByHostFilter: ConnectionPoolError);

        assert_subclass!(py, PrepareError: PyExecutionError);
        assert_subclass!(py, PreparedStatementIdsMismatch: PrepareError);
        assert_subclass!(py, UseKeyspaceError: PyExecutionError);
        assert_subclass!(py, BadKeyspaceName: UseKeyspaceError, PyValueError);
        assert_subclass!(py, KeyspaceNameMismatch: UseKeyspaceError);
        assert_subclass!(py, SchemaAgreementError: PyExecutionError);
        assert_subclass!(py, SchemaAgreementTimeout: SchemaAgreementError, OperationTimedOut);
        assert_subclass!(py, RequiredHostAbsent: SchemaAgreementError);
        assert_subclass!(py, MetadataError: PyExecutionError);
        assert_subclass!(py, InvalidClusterMetadata: MetadataError);
        assert_subclass!(py, ContactPointError: ScyllaError);
        assert_subclass!(py, HostnameResolutionFailed: ContactPointError);
        assert_subclass!(py, NoKnownNodes: ContactPointError);

        assert_subclass!(py, RequestFailedError: PyExecutionError);
        assert_subclass!(py, ConnectionBusy: RequestFailedError);
        assert_subclass!(py, BrokenConnection: RequestFailedError);
        assert_subclass!(py, NonfinishedPagingState: RequestFailedError);
        assert_subclass!(py, RepreparedIdChanged: PrepareError, RequestFailedError);
        assert_subclass!(py, RepreparedIdMissingInBatch: PrepareError, RequestFailedError);
        assert_subclass!(py, ProtocolError: RequestFailedError);
        assert_subclass!(py, RequestSerializationError: ProtocolError);
        assert_subclass!(py, ResponseParseError: ProtocolError);
        assert_subclass!(py, UnexpectedResponse: ProtocolError);

        assert_subclass!(py, DatabaseError: RequestFailedError);
        assert_subclass!(py, CqlSyntaxError: DatabaseError);
        assert_subclass!(py, InvalidRequest: DatabaseError);
        assert_subclass!(py, AuthenticationFailed: DatabaseError);
        assert_subclass!(py, Unauthorized: DatabaseError);
        assert_subclass!(py, ServerConfigError: DatabaseError);
        assert_subclass!(py, Overloaded: DatabaseError);
        assert_subclass!(py, IsBootstrapping: DatabaseError);
        assert_subclass!(py, TruncateError: DatabaseError);
        assert_subclass!(py, ServerError: DatabaseError);
        assert_subclass!(py, ServerProtocolError: DatabaseError);
        assert_subclass!(py, Unavailable: DatabaseError);
        assert_subclass!(py, ReadTimeout: DatabaseError);
        assert_subclass!(py, WriteTimeout: DatabaseError);
        assert_subclass!(py, ReadFailure: DatabaseError);
        assert_subclass!(py, WriteFailure: DatabaseError);
        assert_subclass!(py, AlreadyExists: DatabaseError);
        assert_subclass!(py, FunctionFailure: DatabaseError);
        assert_subclass!(py, Unprepared: DatabaseError);
        assert_subclass!(py, RateLimitReached: DatabaseError);
        assert_subclass!(py, UnknownDatabaseError: DatabaseError);
        // Server timeouts are not client timers.
        assert_not_subclass!(py, ReadTimeout: PyTimeoutError);
        assert_not_subclass!(py, WriteTimeout: PyTimeoutError);
    });
}

/* RuntimeTaskJoinFailed */

/// Every operation maps a runtime task that did not complete to `InternalDriverError`, with a
/// message naming the operation.
#[test]
fn runtime_task_join_failure_is_an_internal_driver_error() {
    Python::initialize();
    type Convert = fn(tokio::task::JoinError) -> PyErr;
    let cases: [(Convert, &str); 6] = [
        (
            |e| DriverUseKeyspaceError::RuntimeTaskJoinFailed(e).into(),
            "using keyspace",
        ),
        (
            |e| DriverSchemaAgreementError::RuntimeTaskJoinFailed(e).into(),
            "checking schema agreement",
        ),
        (
            |e| DriverMetadataRefreshError::RuntimeTaskJoinFailed(e).into(),
            "refreshing metadata",
        ),
        (
            |e| DriverSessionConnectionError::RuntimeTaskJoinFailed(e).into(),
            "creating session",
        ),
        (
            |e| DriverTraceError::RuntimeTaskJoinFailed(e).into(),
            "fetching a query trace",
        ),
        (
            |e| DriverExecuteError::RuntimeTaskJoinFailed(e).into(),
            "executing query",
        ),
    ];
    for (convert, operation) in cases {
        let (join_error, description) = join_error();
        let err = convert(join_error);
        let expected = format!("runtime error while {operation}: {description}");
        Python::attach(|py| assert_mapped::<InternalDriverError>(py, &err, &expected));
    }
}

/* ExecutionError */

#[test]
fn empty_plan_is_no_host_available() {
    Python::initialize();
    Python::attach(|py| {
        let err = map(py, ExecutionError::EmptyPlan);
        assert_mapped::<NoHostAvailable>(py, &err, MESSAGE);
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
        assert_mapped::<OperationTimedOut>(py, &err, MESSAGE);
        assert_attr(
            py,
            &err,
            "timeout",
            "timedelta",
            Duration::from_millis(1500),
        );
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
        assert_mapped::<PreparedStatementIdsMismatch>(py, &err, MESSAGE);

        let err = map(
            py,
            ExecutionError::UseKeyspaceError(RustUseKeyspaceError::BadKeyspaceName(
                RustBadKeyspaceName::Empty,
            )),
        );
        assert_mapped::<BadKeyspaceName>(py, &err, MESSAGE);

        let err = map(
            py,
            ExecutionError::SchemaAgreementError(RustSchemaAgreementError::Timeout(
                Duration::from_secs(1),
            )),
        );
        assert_mapped::<SchemaAgreementTimeout>(py, &err, MESSAGE);

        let err = map(
            py,
            ExecutionError::MetadataError(RustMetadataError::Peers(PeersMetadataError::EmptyPeers)),
        );
        assert_mapped::<InvalidClusterMetadata>(py, &err, MESSAGE);
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
        assert_mapped::<PartitionKeyExtractionFailed>(py, &err, MESSAGE);
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
        assert_mapped::<SerializeFailedError>(py, &err, MESSAGE);
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
        assert_mapped::<ValuesTooLongForKey>(py, &err, MESSAGE);
        assert_attr(py, &err, "length", "int", 70000);
        assert_attr(py, &err, "max_length", "int", 65535);
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
        assert_mapped::<TooManyStatementsInBatch>(py, &err, MESSAGE);
        assert_attr(py, &err, "count", "int", 70001);
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
        assert_mapped::<ConnectionPoolBroken>(py, &broken, MESSAGE);

        let initializing = map(
            py,
            ExecutionError::ConnectionPoolError(RustConnectionPoolError::Initializing),
        );
        assert_mapped::<ConnectionPoolInitializing>(py, &initializing, MESSAGE);

        let disabled = map(
            py,
            ExecutionError::ConnectionPoolError(RustConnectionPoolError::NodeDisabledByHostFilter),
        );
        assert_mapped::<NodeDisabledByHostFilter>(py, &disabled, MESSAGE);
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
        assert_mapped::<SerializeFailedError>(py, &err, MESSAGE);
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
        assert_mapped::<RequestSerializationError>(py, &err, MESSAGE);
    });
}

#[test]
fn unable_to_alloc_stream_id_is_connection_busy() {
    Python::initialize();
    Python::attach(|py| {
        let err = attempt_error(py, RequestAttemptError::UnableToAllocStreamId);
        assert_mapped::<ConnectionBusy>(py, &err, MESSAGE);
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
        assert_mapped::<BrokenConnection>(py, &err, MESSAGE);
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
            assert_mapped::<ResponseParseError>(py, &err, MESSAGE);
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
        assert_mapped::<UnexpectedResponse>(py, &err, MESSAGE);
        assert_class_attr(py, &err, "response_kind", PyCqlResponseKind::AuthChallenge);
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
        assert_mapped::<RepreparedIdChanged>(py, &err, MESSAGE);
        assert_attr(
            py,
            &err,
            "statement",
            "str",
            "SELECT * FROM ks.t".to_string(),
        );
        assert_attr(py, &err, "expected_id", "bytes", vec![1_u8, 2, 3]);
        assert_attr(py, &err, "reprepared_id", "bytes", vec![4_u8, 5, 6, 7]);
    });
}

#[test]
fn reprepared_id_missing_in_batch_is_a_prepare_error() {
    Python::initialize();
    Python::attach(|py| {
        let err = attempt_error(py, RequestAttemptError::RepreparedIdMissingInBatch);
        assert_mapped::<RepreparedIdMissingInBatch>(py, &err, MESSAGE);
    });
}

#[test]
fn nonfinished_paging_state_is_a_request_failure() {
    Python::initialize();
    Python::attach(|py| {
        let err = attempt_error(py, RequestAttemptError::NonfinishedPagingState);
        assert_mapped::<NonfinishedPagingState>(py, &err, MESSAGE);
    });
}

/* DbError */

#[test]
fn db_errors_without_fields_map_to_their_classes() {
    Python::initialize();
    Python::attach(|py| {
        fn check<T: PyTypeInfo>(py: Python<'_>, error: DbError) {
            let err = db_error(py, error);
            assert_database_error::<T>(py, &err);
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
        assert_database_error::<Unavailable>(py, &err);
        assert_class_attr(py, &err, "consistency", PyConsistency::Three);
        assert_attr(py, &err, "required", "int", 3);
        assert_attr(py, &err, "alive", "int", 1);
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
        assert_database_error::<ReadTimeout>(py, &err);
        assert_class_attr(py, &err, "consistency", PyConsistency::Quorum);
        assert_attr(py, &err, "received", "int", 1);
        assert_attr(py, &err, "required", "int", 2);
        assert_attr(py, &err, "data_present", "bool", true);
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
        assert_database_error::<WriteTimeout>(py, &err);
        assert_class_attr(py, &err, "consistency", PyConsistency::LocalQuorum);
        assert_attr(py, &err, "received", "int", 2);
        assert_attr(py, &err, "required", "int", 3);
        assert_class_attr(py, &err, "write_type", PyWriteType::Cas());
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
        assert_database_error::<ReadFailure>(py, &err);
        assert_class_attr(py, &err, "consistency", PyConsistency::All);
        assert_attr(py, &err, "received", "int", 4);
        assert_attr(py, &err, "required", "int", 5);
        assert_attr(py, &err, "numfailures", "int", 6);
        assert_attr(py, &err, "data_present", "bool", false);
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
        assert_database_error::<WriteFailure>(py, &err);
        assert_class_attr(py, &err, "consistency", PyConsistency::EachQuorum);
        assert_attr(py, &err, "received", "int", 7);
        assert_attr(py, &err, "required", "int", 8);
        assert_attr(py, &err, "numfailures", "int", 9);
        assert_class_attr(
            py,
            &err,
            "write_type",
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
        assert_database_error::<AlreadyExists>(py, &err);
        assert_attr(py, &err, "keyspace", "str", "ks".to_string());
        assert_attr(py, &err, "table", "str", "tbl".to_string());
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
        assert_database_error::<FunctionFailure>(py, &err);
        assert_attr(py, &err, "keyspace", "str", "fks".to_string());
        assert_attr(py, &err, "function", "str", "my_fn".to_string());
        let arg_types = vec!["int".to_string(), "text".to_string()];
        assert_attr(py, &err, "arg_types", "list", arg_types);
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
        assert_database_error::<Unprepared>(py, &err);
        assert_attr(
            py,
            &err,
            "statement_id",
            "bytes",
            b"\xde\xad\xbe\xef".to_vec(),
        );
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
            assert_database_error::<RateLimitReached>(py, &err);
            assert_class_attr(py, &err, "op_type", expected);
            assert_attr(
                py,
                &err,
                "rejected_by_coordinator",
                "bool",
                rejected_by_coordinator,
            );
        }
    });
}

#[test]
fn unknown_db_error_keeps_code() {
    Python::initialize();
    Python::attach(|py| {
        let err = db_error(py, DbError::Other(0x7777));
        assert_database_error::<UnknownDatabaseError>(py, &err);
        assert_attr(py, &err, "code", "int", 0x7777);
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
        assert_attr(py, &err, "reason", "str", REASON.to_string());
    });
}

#[test]
fn use_keyspace_errors_map_by_cause() {
    Python::initialize();
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
        assert_attr(py, &err, "expected", "str", "expected_ks".to_string());
        assert_attr(py, &err, "received", "str", "received_ks".to_string());

        let err = use_keyspace(
            py,
            RustUseKeyspaceError::RequestTimeout(Duration::from_millis(750)),
        );
        assert_class::<OperationTimedOut>(py, &err);
        assert_attr(py, &err, "timeout", "timedelta", Duration::from_millis(750));
    });
}

#[test]
fn schema_agreement_errors_map_by_cause() {
    Python::initialize();
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
        assert_attr(
            py,
            &err,
            "timeout",
            "timedelta",
            Duration::from_millis(2500),
        );

        let host_id = Uuid::from_u128(0x1234_5678_9abc_def0_1122_3344_5566_7788);
        let err = agreement(py, RustSchemaAgreementError::RequiredHostAbsent(host_id));
        assert_class::<RequiredHostAbsent>(py, &err);
        assert_attr(py, &err, "host_id", "UUID", host_id);

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

        // Despite its name, this variant is for the result of the schema version query.
        let err = agreement(
            py,
            RustSchemaAgreementError::TracesEventsIntoRowsResultError(into_rows_error()),
        );
        assert_class::<ResponseParseError>(py, &err);
    });
}

#[test]
fn metadata_refresh_errors_map_by_cause() {
    Python::initialize();
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
        }

        let err = refresh(
            py,
            RustMetadataError::ConnectionPoolError(RustConnectionPoolError::Initializing),
        );
        assert_class::<ConnectionPoolInitializing>(py, &err);
    });
}

#[test]
fn new_session_errors_map_by_cause() {
    Python::initialize();
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
        let hostnames = vec!["first.invalid".to_string(), "second.invalid".to_string()];
        assert_attr(py, &err, "hostnames", "list", hostnames);

        let err = connect(py, NewSessionError::EmptyKnownNodesList);
        assert_class::<NoKnownNodes>(py, &err);

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
    });
}

#[test]
fn failed_trace_query_raises_the_execute_class() {
    Python::initialize();
    let err = TracingError::ExecutionError(ExecutionError::RequestTimeout(Duration::from_secs(2)));
    let expected = err.to_string();
    Python::attach(|py| {
        let err = tracing_error_to_pyerr(py, &err, expected.clone());
        assert_mapped::<OperationTimedOut>(py, &err, &expected);
        assert_attr(py, &err, "timeout", "timedelta", Duration::from_secs(2));
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
    let expected = err.to_string();
    Python::attach(|py| {
        let err = tracing_error_to_pyerr(py, &err, expected.clone());
        assert_mapped::<TraceUnavailable>(py, &err, &expected);
    });
}

/// A result that is not rows-shaped, as the Rust driver reports it.
fn into_rows_error() -> IntoRowsResultError {
    ResultMetadataAndRowsCountParseError::RowsCountParseError(
        LowLevelDeserializationError::InvalidValueLength(-5),
    )
    .into()
}

#[test]
fn unreadable_trace_rows_raise_response_parse_error() {
    Python::initialize();
    let errors = [
        TracingError::TracesSessionIntoRowsResultError(into_rows_error()),
        TracingError::TracesSessionInvalidColumnType(TypeCheckError::new(std::fmt::Error)),
        TracingError::TracesSessionDeserializationFailed(DeserializationError::new(
            std::fmt::Error,
        )),
        TracingError::TracesEventsIntoRowsResultError(into_rows_error()),
        TracingError::TracesEventsInvalidColumnType(TypeCheckError::new(std::fmt::Error)),
        TracingError::TracesEventsDeserializationFailed(DeserializationError::new(std::fmt::Error)),
    ];
    Python::attach(|py| {
        for err in &errors {
            let expected = err.to_string();
            let err = tracing_error_to_pyerr(py, err, expected.clone());
            // Retrying does not help, so it must not be a `TraceUnavailable`, which says the
            // trace is not there yet.
            assert_mapped::<ResponseParseError>(py, &err, &expected);
        }
    });
}

#[test]
fn trace_fetch_errors_keep_their_description() {
    Python::initialize();
    let failed: PyErr =
        DriverTraceError::rust_driver_tracing_error(TracingError::EmptyResults).into();
    Python::attach(|py| {
        let expected = format!(
            "Failed to fetch the query trace: {}",
            TracingError::EmptyResults
        );
        assert_mapped::<TraceUnavailable>(py, &failed, &expected);
    });
}

#[test]
fn unlocated_serialization_failure_has_none_bind_marker() {
    Python::initialize();
    let err = RustSerializationError::new(std::fmt::Error);
    let expected = err.to_string();
    Python::attach(|py| {
        let err = serialization_error_to_pyerr(py, &err, expected.clone());
        assert_mapped::<SerializeFailedError>(py, &err, &expected);
        assert!(err.value(py).getattr("bind_marker").unwrap().is_none());
    });
}

/* Errors of `execution.rs` that a cluster cannot produce on demand */

#[test]
fn execute_serialization_failure_is_a_serialization_error() {
    Python::initialize();
    let source = RustSerializationError::new(io::Error::other("bad value"));
    // The source's description is the Rust driver's wording.
    let expected = format!("Failed to serialize values: {source}");
    let err: PyErr = DriverExecuteError::serialization_failed(source).into();
    Python::attach(|py| {
        assert_mapped::<SerializeFailedError>(py, &err, &expected);
    });
}

#[test]
fn execute_driver_error_is_described_as_failed_execution() {
    Python::initialize();
    let err: PyErr =
        DriverExecuteError::rust_driver_execution_error(ExecutionError::EmptyPlan).into();
    Python::attach(|py| {
        let expected = format!("Failed to execute statement: {}", ExecutionError::EmptyPlan);
        assert_mapped::<NoHostAvailable>(py, &err, &expected);
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

/// Converts `value` the way the driver does: `Bound::new` would build the base class, not the
/// variant's.
fn to_py<'py, P: IntoPyObjectExt<'py>>(py: Python<'py>, value: P) -> Bound<'py, PyAny> {
    value.into_bound_py_any(py).unwrap()
}

/// Asserts that `object` compares equal in Python to `expected`, which was built from Python.
fn assert_py_eq(object: &Bound<'_, PyAny>, expected: &Bound<'_, PyAny>) {
    assert!(object.eq(expected).unwrap(), "{object} != {expected}");
    assert!(!object.ne(expected).unwrap());
}

#[test]
fn every_write_type_converts_to_python() {
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
            let converted = to_py(py, PyWriteType::from(rust.clone()));
            assert!(
                converted.is_instance(&class).unwrap(),
                "{rust:?} is not a WriteType.{name}"
            );
            assert_py_eq(&converted, &expected);
            for (other, other_name) in &cases {
                if other != rust {
                    let other = to_py(py, PyWriteType::from(other.clone()));
                    assert!(!expected.eq(other).unwrap(), "{name} == {other_name}");
                }
            }
        }

        let other_class = write_type.getattr("Other").unwrap();
        let expected = other_class.call1(("CUSTOM",)).unwrap();
        let converted = to_py(
            py,
            PyWriteType::from(WriteType::Other("CUSTOM".to_string())),
        );
        assert!(converted.is_instance(&other_class).unwrap());
        assert_py_eq(&converted, &expected);
        let value = converted.getattr("value").unwrap();
        assert_eq!(value.extract::<String>().unwrap(), "CUSTOM");
        // The payload takes part in equality.
        let different = other_class.call1(("DIFFERENT",)).unwrap();
        assert!(!converted.eq(different).unwrap());
    });
}

#[test]
fn every_operation_type_converts_to_python() {
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
        let converted = cases
            .iter()
            .map(|(rust, expected)| {
                let converted = to_py(py, PyOperationType::from(rust.clone()));
                assert!(
                    converted.is_instance(&expected.get_type()).unwrap(),
                    "{rust:?} is not a {}",
                    expected.get_type()
                );
                assert_py_eq(&converted, expected);
                converted
            })
            .collect::<Vec<_>>();
        assert!(!converted[0].eq(&cases[1].1).unwrap());
        assert!(!converted[1].eq(&cases[2].1).unwrap());
        let other = &converted[2];
        assert_eq!(other.getattr("code").unwrap().extract::<u8>().unwrap(), 7);
        // The code takes part in equality.
        assert!(!other.eq(other_class.call1((8,)).unwrap()).unwrap());
    });
}

#[test]
fn every_cql_response_kind_converts_to_python() {
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
            let converted = to_py(py, PyCqlResponseKind::from(*rust));
            assert_py_eq(&converted, &expected);
            // `eq_int`: equal to its position in declaration order.
            assert!(converted.eq(index).unwrap(), "{name} != {index}");
            assert!(!converted.eq(index + 1).unwrap());
            assert_eq!(
                converted.repr().unwrap().to_string(),
                format!("CqlResponseKind.{name}")
            );
        }
    });
}
