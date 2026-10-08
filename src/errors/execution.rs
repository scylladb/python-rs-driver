use pyo3::prelude::*;
use scylla::errors::UseKeyspaceError as RustUseKeyspaceError;

use crate::errors::request::execution_error_to_pyerr;
use crate::errors::{
    BadKeyspaceNameError, InternalDriverError, KeyspaceNameMismatchError, PagingStateNotAllowed,
    PrepareError, RequestError, RequestTimeoutError, RowFactoryError, RuntimeTaskJoinFailedError,
    SchemaAgreementError, SessionConnectionError, StatementConversionError, get_type_name,
    with_cause,
};
use crate::serialize::error::serialization_error_to_pyerr;

/* Connection errors */

/// Errors that can occur during session creation and connection establishment.
#[derive(Debug, thiserror::Error)]
#[must_use]
pub(crate) enum DriverSessionConnectionError {
    /// The Tokio task running session creation failed to join.
    #[error("runtime error while creating session: {0}")]
    RuntimeTaskJoinFailed(#[from] tokio::task::JoinError),
    /// The Rust driver failed to establish a new session.
    #[error("failed to establish session: {source}")]
    NewSessionError {
        source: Box<scylla::errors::NewSessionError>,
    },

    #[error(transparent)]
    PythonConversionError { source: PyErr },
}

impl DriverSessionConnectionError {
    /* Constructors */

    pub(crate) fn new_session_error(source: scylla::errors::NewSessionError) -> Self {
        Self::NewSessionError {
            source: Box::new(source),
        }
    }

    pub(crate) fn python_conversion_error(source: PyErr) -> Self {
        Self::PythonConversionError { source }
    }
}

impl From<DriverSessionConnectionError> for PyErr {
    fn from(e: DriverSessionConnectionError) -> PyErr {
        match e {
            DriverSessionConnectionError::PythonConversionError { source } => source,
            _ => SessionConnectionError::new_err(e.to_string()),
        }
    }
}

/// Errors that can occur during conversion of Python objects into statements for execution.
#[derive(Debug, thiserror::Error)]
#[must_use]
pub(crate) enum DriverStatementConversionError {
    /// The provided statement argument is of an unsupported type.
    #[error(
        "Invalid statement type: expected a str, Statement, or PreparedStatement, got {type_name}"
    )]
    InvalidStatementType { type_name: String },
    /// Failed to convert a Python string object into a Rust string when extracting a statement.
    #[error("Failed to convert statement string to Rust string")]
    StatementStringConversionFailed { source: Box<PyErr> },
    /// Attempted to prepare an already prepared statement.
    #[error("Cannot prepare a PreparedStatement; expected a str or Statement")]
    CannotPreparePreparedStatement,
}

impl DriverStatementConversionError {
    /* Constructors */

    pub(crate) fn invalid_statement_type(obj: Borrowed<PyAny>) -> Self {
        let type_name = get_type_name(obj);
        Self::InvalidStatementType { type_name }
    }

    pub(crate) fn cannot_prepare_prepared_statement() -> Self {
        Self::CannotPreparePreparedStatement
    }

    pub(crate) fn statement_string_conversion_failed(source: PyErr) -> Self {
        Self::StatementStringConversionFailed {
            source: Box::new(source),
        }
    }
}

impl From<DriverStatementConversionError> for PyErr {
    fn from(e: DriverStatementConversionError) -> PyErr {
        let message = e.to_string();
        match e {
            DriverStatementConversionError::InvalidStatementType { .. } => {
                StatementConversionError::new_err(message)
            }
            DriverStatementConversionError::StatementStringConversionFailed { source } => {
                with_cause(StatementConversionError::new_err(message), *source)
            }
            // Raised as a `PrepareError` rather than a `StatementConversionError`:
            // the type is a valid statement, it just cannot be prepared again.
            DriverStatementConversionError::CannotPreparePreparedStatement => {
                PrepareError::new_err(message)
            }
        }
    }
}

/// Errors that can occur during execution of a query (session.execute),
/// excluding deserialization errors which are represented separately in RowIterationError.
#[derive(Debug, thiserror::Error)]
#[must_use]
pub(crate) enum DriverExecuteError {
    /// paging_state parameter in session.execute must be None.
    #[error("Paging state must be None for unpaged execution")]
    PagingStateMustBeNoneForUnpagedExecution,
    /// The Rust driver failed while executing a query.
    #[error("Failed to execute statement: {source}")]
    RustDriverExecutionError {
        source: Box<scylla::errors::ExecutionError>,
    },
    /// Serialization of values failed before execution.
    #[error("Failed to serialize values: {source}")]
    SerializationFailed {
        source: scylla::serialize::SerializationError,
    },
    /// The Tokio runtime task responsible for executing the query failed to join.
    #[error("runtime error while executing query: {0}")]
    RuntimeTaskJoinFailed(#[from] tokio::task::JoinError),
    /// Resolving the row factory against the result metadata failed.
    #[error("Failed to prepare the row factory for the result metadata")]
    RowFactoryFailed { source: PyErr },
}

impl DriverExecuteError {
    /* Constructors */

    pub(crate) fn paging_state_must_be_none_for_unpaged_execution() -> Self {
        Self::PagingStateMustBeNoneForUnpagedExecution
    }

    pub(crate) fn rust_driver_execution_error(source: scylla::errors::ExecutionError) -> Self {
        Self::RustDriverExecutionError {
            source: Box::new(source),
        }
    }

    pub(crate) fn serialization_failed(source: scylla::serialize::SerializationError) -> Self {
        Self::SerializationFailed { source }
    }

    pub(crate) fn row_factory_failed(source: PyErr) -> Self {
        Self::RowFactoryFailed { source }
    }
}

impl From<DriverExecuteError> for PyErr {
    fn from(e: DriverExecuteError) -> PyErr {
        let message = e.to_string();
        match e {
            DriverExecuteError::PagingStateMustBeNoneForUnpagedExecution => {
                py_err!(PagingStateNotAllowed, message)
            }
            DriverExecuteError::RustDriverExecutionError { source } => {
                execution_error_to_pyerr(&source, message)
            }
            DriverExecuteError::SerializationFailed { source } => {
                serialization_error_to_pyerr(&source, message)
            }
            DriverExecuteError::RuntimeTaskJoinFailed(_) => py_err!(InternalDriverError, message),
            DriverExecuteError::RowFactoryFailed { source } => {
                with_cause(py_err!(RowFactoryError, message), source)
            }
        }
    }
}

/// Errors that can occur during preparation of a statement.
#[derive(Debug, thiserror::Error)]
#[must_use]
pub(crate) enum DriverPrepareError {
    /// The Rust driver failed while preparing a statement.
    #[allow(clippy::enum_variant_names)]
    #[error("Failed to prepare statement: {source}")]
    RustDriverPrepareError {
        source: Box<scylla::errors::PrepareError>,
    },
}

impl DriverPrepareError {
    /* Constructors */

    pub(crate) fn rust_driver_prepare_error(source: scylla::errors::PrepareError) -> Self {
        Self::RustDriverPrepareError {
            source: Box::new(source),
        }
    }
}

impl From<DriverPrepareError> for PyErr {
    fn from(e: DriverPrepareError) -> PyErr {
        PrepareError::new_err(e.to_string())
    }
}

/// Errors that can occur during schema agreement checks.
#[derive(Debug, thiserror::Error)]
#[must_use]
pub(crate) enum DriverSchemaAgreementError {
    /// The Rust driver failed to check for schema agreement.
    #[error("Failed to check schema agreement: {source}")]
    RustDriverSchemaAgreementError {
        source: Box<scylla::errors::SchemaAgreementError>,
    },
    /// The Tokio runtime task responsible for checking schema agreement failed to join.
    #[error("runtime error while checking schema agreement: {0}")]
    RuntimeTaskJoinFailed(#[from] tokio::task::JoinError),
}

impl DriverSchemaAgreementError {
    /* Constructors */

    pub(crate) fn rust_driver_schema_agreement_error(
        source: scylla::errors::SchemaAgreementError,
    ) -> Self {
        Self::RustDriverSchemaAgreementError {
            source: Box::new(source),
        }
    }
}

impl From<DriverSchemaAgreementError> for PyErr {
    fn from(e: DriverSchemaAgreementError) -> PyErr {
        SchemaAgreementError::new_err(e.to_string())
    }
}

/// Errors that can occur during use_keyspace operation on a session object.
#[derive(Debug, thiserror::Error)]
pub(crate) enum DriverUseKeyspaceError {
    /// The Rust driver failed to switch the keyspace.
    #[error(transparent)]
    RustDriverUseKeyspaceError(#[from] RustUseKeyspaceError),
    /// The Tokio runtime task responsible for switching the keyspace failed to join.
    #[error("runtime error while using keyspace: {0}")]
    RuntimeTaskJoinFailed(#[from] tokio::task::JoinError),
}

impl From<DriverUseKeyspaceError> for PyErr {
    fn from(e: DriverUseKeyspaceError) -> Self {
        let message = e.to_string();
        match e {
            DriverUseKeyspaceError::RustDriverUseKeyspaceError(source) =>
            {
                #[deny(clippy::wildcard_enum_match_arm)]
                match source {
                    RustUseKeyspaceError::BadKeyspaceName(_) => {
                        BadKeyspaceNameError::new_err(message)
                    }
                    RustUseKeyspaceError::RequestError(_) => RequestError::new_err(message),
                    RustUseKeyspaceError::KeyspaceNameMismatch { .. } => {
                        KeyspaceNameMismatchError::new_err(message)
                    }
                    RustUseKeyspaceError::RequestTimeout(_) => {
                        RequestTimeoutError::new_err(message)
                    }
                    _ => unreachable!("clippy testifies that the match is exhaustive"),
                }
            }
            DriverUseKeyspaceError::RuntimeTaskJoinFailed(_) => {
                RuntimeTaskJoinFailedError::new_err(message)
            }
        }
    }
}
