use pyo3::prelude::*;

use crate::errors::{SessionConfigError, StatementConfigError, get_type_name, with_cause};
use crate::tls::TlsConfigError;
use crate::utils::AddressParseError;

/// Errors related to invalid session configuration.
#[derive(Debug, thiserror::Error)]
#[must_use]
pub enum DriverSessionConfigError {
    #[error(
        "Invalid port range: start port must be less than or equal to end port, and both ports must be greater than or equal to 1024"
    )]
    InvalidPortRange,

    #[error("Duration must be greater than zero.")]
    ZeroDurationNotAllowed,

    /// The address object is not a valid type (str, tuple, or IpAddr tuple).
    #[error("Failed to parse address")]
    InvalidAddress { source: AddressParseError },

    /// The object does not have a `translate` method and is not a dict-based address translator.
    #[error("Expected a class implementing AddressTranslator protocol, got {type_name}")]
    InvalidAddressTranslator { type_name: String },

    /// The object is not AuthenticatorProvider subclass.
    #[error("Expected an AuthenticatorProvider subclass, got {type_name}")]
    InvalidAuthenticatorProvider { type_name: String },

    /// The object does not have a `next_timestamp` method and is not a built-in timestamp generator.
    #[error("Expected a class implementing TimestampGenerator protocol, got {type_name}")]
    InvalidTimestampGenerator { type_name: String },

    /// The object does not have an `accept` method and is not a built-in host filter class.
    #[error("Expected a class implementing HostFilter protocol, got {type_name}")]
    InvalidHostFilter { type_name: String },

    /// An OpenSSL operation failed while building the TLS context.
    #[error("TLS configuration error")]
    InvalidTlsConfig { source: TlsConfigError },
}

impl DriverSessionConfigError {
    /* Constructors */
    pub(crate) fn invalid_authenticator_provider(obj: Borrowed<PyAny>) -> Self {
        Self::InvalidAuthenticatorProvider {
            type_name: get_type_name(obj),
        }
    }

    pub(crate) fn invalid_address_translator(obj: Borrowed<PyAny>) -> Self {
        Self::InvalidAddressTranslator {
            type_name: get_type_name(obj),
        }
    }

    pub(crate) fn invalid_timestamp_generator(obj: Borrowed<PyAny>) -> Self {
        Self::InvalidTimestampGenerator {
            type_name: get_type_name(obj),
        }
    }

    pub(crate) fn invalid_host_filter(obj: Borrowed<PyAny>) -> Self {
        Self::InvalidHostFilter {
            type_name: get_type_name(obj),
        }
    }
}

impl From<DriverSessionConfigError> for PyErr {
    fn from(e: DriverSessionConfigError) -> PyErr {
        let err = SessionConfigError::new_err(e.to_string());
        match e {
            DriverSessionConfigError::InvalidAddress { source } => with_cause(err, source.into()),
            DriverSessionConfigError::InvalidTlsConfig { source } => with_cause(err, source.into()),
            _ => err,
        }
    }
}

/// Errors related to invalid statement configuration.
#[derive(Debug, thiserror::Error)]
#[must_use]
pub enum DriverStatementConfigError {
    /// The provided request timeout is not a non-negative finite number of seconds.
    #[error("timeout must be a non-negative, finite number (in seconds), got {value}")]
    InvalidRequestTimeout { value: f64 },
    /// The provided page size is not a positive number.
    #[error("page size must be positive, got {value}")]
    NonPositivePageSize { value: i32 },
}

impl DriverStatementConfigError {
    /* Constructors */

    pub(crate) fn invalid_request_timeout(value: f64) -> Self {
        Self::InvalidRequestTimeout { value }
    }

    pub(crate) fn non_positive_page_size(value: i32) -> Self {
        Self::NonPositivePageSize { value }
    }
}

impl From<DriverStatementConfigError> for PyErr {
    fn from(e: DriverStatementConfigError) -> PyErr {
        StatementConfigError::new_err(e.to_string())
    }
}
