use crate::errors::request::db_error_to_pyerr;
use crate::policies::retry::types::PyCqlResponseKind;
use pyo3::exceptions::PyBaseException;
use pyo3::prelude::*;
use scylla::errors::RequestAttemptError;

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
        error: Py<PyBaseException>,
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
        let description = value.to_string();
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
            RequestAttemptError::DbError(error, message) => Python::attach(|py| {
                let error = db_error_to_pyerr(py, &error, &message, description).into_value(py);
                PyRequestAttemptError::DbError { error, message }
            }),
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
