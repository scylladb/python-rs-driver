// TODO: `PyDbError` and `PyRequestAttemptError` duplicate the exception classes in `crate::errors`;
// pass those to retry policies instead and remove these.

use crate::enums::PyConsistency;
use crate::policies::retry::types::{PyCqlResponseKind, PyOperationType, PyWriteType};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use scylla::errors::DbError;
use scylla::errors::RequestAttemptError;

#[pyclass(
    module = "scylla.policies.retry",
    name = "DbError",
    frozen,
    from_py_object
)]
#[derive(Debug, Clone)]
#[non_exhaustive]
pub(crate) enum PyDbError {
    SyntaxError(),
    Invalid(),
    AlreadyExists {
        keyspace: String,
        table: String,
    },
    FunctionFailure {
        keyspace: String,
        function: String,
        arg_types: Vec<String>,
    },
    AuthenticationError(),
    Unauthorized(),
    ConfigError(),
    Unavailable {
        consistency: PyConsistency,
        required: i32,
        alive: i32,
    },
    Overloaded(),
    IsBootstrapping(),
    TruncateError(),
    ReadTimeout {
        consistency: PyConsistency,
        received: i32,
        required: i32,
        data_present: bool,
    },
    WriteTimeout {
        consistency: PyConsistency,
        received: i32,
        required: i32,
        write_type: PyWriteType,
    },
    ReadFailure {
        consistency: PyConsistency,
        received: i32,
        required: i32,
        numfailures: i32,
        data_present: bool,
    },
    WriteFailure {
        consistency: PyConsistency,
        received: i32,
        required: i32,
        numfailures: i32,
        write_type: PyWriteType,
    },
    Unprepared {
        statement_id: Py<PyBytes>,
    },
    ServerError(),
    ProtocolError(),
    RateLimitReached {
        op_type: PyOperationType,
        rejected_by_coordinator: bool,
    },
    Other {
        code: i32,
    },
}

impl From<DbError> for PyDbError {
    #[deny(clippy::wildcard_enum_match_arm)]
    fn from(value: DbError) -> Self {
        match value {
            DbError::SyntaxError => PyDbError::SyntaxError(),
            DbError::Invalid => PyDbError::Invalid(),
            DbError::AlreadyExists { keyspace, table } => {
                PyDbError::AlreadyExists { keyspace, table }
            }
            DbError::FunctionFailure {
                keyspace,
                function,
                arg_types,
            } => PyDbError::FunctionFailure {
                keyspace,
                function,
                arg_types,
            },
            DbError::AuthenticationError => PyDbError::AuthenticationError(),
            DbError::Unauthorized => PyDbError::Unauthorized(),
            DbError::ConfigError => PyDbError::ConfigError(),
            DbError::Unavailable {
                consistency,
                required,
                alive,
            } => PyDbError::Unavailable {
                consistency: consistency.into(),
                required,
                alive,
            },
            DbError::Overloaded => PyDbError::Overloaded(),
            DbError::IsBootstrapping => PyDbError::IsBootstrapping(),
            DbError::TruncateError => PyDbError::TruncateError(),
            DbError::ReadTimeout {
                consistency,
                received,
                required,
                data_present,
            } => PyDbError::ReadTimeout {
                consistency: consistency.into(),
                received,
                required,
                data_present,
            },
            DbError::WriteTimeout {
                consistency,
                received,
                required,
                write_type,
            } => PyDbError::WriteTimeout {
                consistency: consistency.into(),
                received,
                required,
                write_type: write_type.into(),
            },
            DbError::ReadFailure {
                consistency,
                received,
                required,
                numfailures,
                data_present,
            } => PyDbError::ReadFailure {
                consistency: consistency.into(),
                received,
                required,
                numfailures,
                data_present,
            },
            DbError::WriteFailure {
                consistency,
                received,
                required,
                numfailures,
                write_type,
            } => PyDbError::WriteFailure {
                consistency: consistency.into(),
                received,
                required,
                numfailures,
                write_type: write_type.into(),
            },
            DbError::Unprepared { statement_id } => Python::attach(|py| {
                let py_bytes = PyBytes::new_with(py, statement_id.len(), |buf| {
                    buf.copy_from_slice(&statement_id);
                    Ok(())
                })
                .expect("Failed to allocate `PyBytes` for DbError::Unprepared");
                PyDbError::Unprepared {
                    statement_id: py_bytes.into(),
                }
            }),
            DbError::ServerError => PyDbError::ServerError(),
            DbError::ProtocolError => PyDbError::ProtocolError(),
            DbError::RateLimitReached {
                op_type,
                rejected_by_coordinator,
            } => PyDbError::RateLimitReached {
                op_type: op_type.into(),
                rejected_by_coordinator,
            },
            DbError::Other(code) => PyDbError::Other { code },
            _ => unreachable!("Unhandled `DbError` variant"),
        }
    }
}

#[pyclass(
    module = "scylla.policies.retry",
    name = "RequestAttemptError",
    frozen,
    from_py_object
)]
#[derive(Debug, Clone)]
#[non_exhaustive]
pub(crate) enum PyRequestAttemptError {
    SerializationError(),
    CqlRequestSerialization(),
    UnableToAllocStreamId(),
    BrokenConnectionError(),
    BodyExtensionsParseError(),
    CqlResultParseError(),
    CqlErrorParseError(),
    DbError {
        error: PyDbError,
        message: String,
    },
    UnexpectedResponse {
        kind: PyCqlResponseKind,
    },
    RepreparedIdChanged {
        statement: String,
        expected_id: Vec<u8>,
        reprepared_id: Vec<u8>,
    },
    RepreparedIdMissingInBatch(),
    NonfinishedPagingState(),
}

impl From<RequestAttemptError> for PyRequestAttemptError {
    #[deny(clippy::wildcard_enum_match_arm)]
    fn from(value: RequestAttemptError) -> Self {
        match value {
            RequestAttemptError::SerializationError(_) => {
                PyRequestAttemptError::SerializationError()
            }
            RequestAttemptError::CqlRequestSerialization(_) => {
                PyRequestAttemptError::CqlRequestSerialization()
            }
            RequestAttemptError::UnableToAllocStreamId => {
                PyRequestAttemptError::UnableToAllocStreamId()
            }
            RequestAttemptError::BrokenConnectionError(_) => {
                PyRequestAttemptError::BrokenConnectionError()
            }
            RequestAttemptError::BodyExtensionsParseError(_) => {
                PyRequestAttemptError::BodyExtensionsParseError()
            }
            RequestAttemptError::CqlResultParseError(_) => {
                PyRequestAttemptError::CqlResultParseError()
            }
            RequestAttemptError::CqlErrorParseError(_) => {
                PyRequestAttemptError::CqlErrorParseError()
            }
            RequestAttemptError::DbError(error, message) => PyRequestAttemptError::DbError {
                error: error.into(),
                message,
            },
            RequestAttemptError::UnexpectedResponse(kind) => {
                PyRequestAttemptError::UnexpectedResponse { kind: kind.into() }
            }
            RequestAttemptError::RepreparedIdChanged {
                statement,
                expected_id,
                reprepared_id,
            } => PyRequestAttemptError::RepreparedIdChanged {
                statement,
                expected_id,
                reprepared_id,
            },
            RequestAttemptError::RepreparedIdMissingInBatch => {
                PyRequestAttemptError::RepreparedIdMissingInBatch()
            }
            RequestAttemptError::NonfinishedPagingState => {
                PyRequestAttemptError::NonfinishedPagingState()
            }
            _ => unreachable!("Unhandled `RequestAttemptError` variant"),
        }
    }
}
