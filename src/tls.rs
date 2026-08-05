use openssl::ssl::{SslConnector, SslContext, SslFiletype, SslMethod, SslVerifyMode};
use openssl::x509::X509;
use pyo3::prelude::*;
use pyo3::sync::MutexExt;
use std::path::PathBuf;
use std::sync::Mutex;

pub use crate::errors::TlsConfigError;
use crate::utils::WithOriginalPyObject;

/// Selects the peer certificate verification mode for [`SslConfig`].
///
/// Mirrors the `ssl.CERT_NONE` and `ssl.CERT_REQUIRED` constants
/// from Python's standard library.
#[pyclass(frozen, eq, eq_int, from_py_object, name = "VerifyMode")]
#[derive(Clone, PartialEq, Eq, Copy, Debug)]
pub enum PyVerifyMode {
    /// Peer certificates are ignored and validation is disabled.
    /// Equivalent to ``ssl.CERT_NONE``.
    #[pyo3(name = "CERT_NONE")]
    None,

    /// Peer certificates are required and strictly validated. If no certificate
    /// is presented or validation fails, the handshake is aborted.
    /// Equivalent to ``ssl.CERT_REQUIRED``.
    #[pyo3(name = "CERT_REQUIRED")]
    Required,
}

#[pymethods]
impl PyVerifyMode {
    fn __repr__(&self) -> &'static str {
        match self {
            PyVerifyMode::None => "VerifyMode.CERT_NONE",
            PyVerifyMode::Required => "VerifyMode.CERT_REQUIRED",
        }
    }
}

impl From<PyVerifyMode> for SslVerifyMode {
    fn from(mode: PyVerifyMode) -> Self {
        match mode {
            PyVerifyMode::None => SslVerifyMode::NONE,
            PyVerifyMode::Required => SslVerifyMode::PEER,
        }
    }
}

impl From<SslVerifyMode> for PyVerifyMode {
    fn from(mode: SslVerifyMode) -> Self {
        if mode.contains(SslVerifyMode::FAIL_IF_NO_PEER_CERT) {
            PyVerifyMode::Required
        } else {
            PyVerifyMode::None
        }
    }
}

/// Immutable snapshot of a [`PyTlsContext`] at the time it is assigned to a session builder.
#[pyclass(frozen, name = "TlsConfig")]
pub(crate) struct PyTlsConfig {
    #[pyo3(get, name = "cafile")]
    ca_file: Option<PathBuf>,
    #[pyo3(get, name = "capath")]
    ca_path: Option<PathBuf>,
    #[pyo3(get, name = "cadata")]
    ca_data: Option<Py<PyAny>>,
    #[pyo3(get, name = "certfile")]
    cert_file: Option<PathBuf>,
    #[pyo3(get, name = "keyfile")]
    key_file: Option<PathBuf>,
    #[pyo3(get)]
    verify_mode: PyVerifyMode,
}

/// Internal config data stored inside [`PySslConfig`].
struct SslConfigInner {
    ca_file: Option<PathBuf>,
    ca_path: Option<PathBuf>,
    ca_data: Option<WithOriginalPyObject<CaData>>,
    cert_file: Option<PathBuf>,
    key_file: Option<PathBuf>,
    verify_mode: SslVerifyMode,
}

impl SslConfigInner {
    fn new() -> Self {
        Self {
            ca_file: None,
            ca_path: None,
            ca_data: None,
            cert_file: None,
            key_file: None,
            verify_mode: SslVerifyMode::PEER,
        }
    }
}

/// TLS configuration for a ScyllaDB session.
///
/// Mirrors the interface of Python's `ssl.SSLContext`. Pass an instance of this class to
/// ``SessionBuilder.tls_context()`` — a snapshot of the configuration is taken
/// at that moment, and the actual OpenSSL context is built internally when needed.
#[pyclass(frozen, name = "TlsContext")]
pub(crate) struct PyTlsContext {
    inner: Mutex<SslConfigInner>,
}

#[pymethods]
impl PyTlsContext {
    #[new]
    fn new() -> Self {
        Self {
            inner: Mutex::new(SslConfigInner::new()),
        }
    }

    /// Load CA certificates used to verify the server's certificate.
    ///
    /// Equivalent to ``ssl.SSLContext.load_verify_locations()``.
    #[pyo3(signature = (cafile = None, capath = None, cadata = None))]
    fn load_verify_locations<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        cafile: Option<PathBuf>,
        capath: Option<PathBuf>,
        cadata: Option<WithOriginalPyObject<CaData>>,
    ) -> PyResult<()> {
        if cafile.is_none() && capath.is_none() && cadata.is_none() {
            return Err(TlsConfigError::NoCaLocationsSpecified.into());
        }

        let mut inner = slf.inner.lock_py_attached(py).unwrap();
        inner.ca_file = cafile;
        inner.ca_path = capath;
        inner.ca_data = cadata;
        Ok(())
    }

    /// Load the client certificate and optional private key for mutual TLS (mTLS).
    ///
    /// Equivalent to ``ssl.SSLContext.load_cert_chain()``.
    #[pyo3(signature = (certfile, keyfile = None))]
    fn load_cert_chain<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        certfile: PathBuf,
        keyfile: Option<PathBuf>,
    ) {
        let mut inner = slf.inner.lock_py_attached(py).unwrap();
        inner.cert_file = Some(certfile);
        inner.key_file = keyfile;
    }

    #[setter]
    fn set_verify_mode(slf: PyRef<'_, Self>, py: Python<'_>, mode: PyVerifyMode) -> PyResult<()> {
        let verify: SslVerifyMode = mode.into();

        slf.inner.lock_py_attached(py).unwrap().verify_mode = verify;
        Ok(())
    }

    #[getter]
    fn get_verify_mode(&self, py: Python<'_>) -> PyVerifyMode {
        let mode = self.inner.lock_py_attached(py).unwrap().verify_mode;
        mode.into()
    }
}

impl PyTlsContext {
    /// Build an [`SslContext`] and immutable configuration snapshot from the stored state.
    ///
    /// Called internally by `SessionBuilder`.
    pub(crate) fn build_with_snapshot(
        &self,
        py: Python<'_>,
    ) -> Result<(SslContext, PyTlsConfig), TlsConfigError> {
        let (ca_file, ca_path, ca_data, cert_file, key_file, verify_mode) = {
            let inner = self.inner.lock_py_attached(py).unwrap();
            (
                inner.ca_file.clone(),
                inner.ca_path.clone(),
                inner.ca_data.clone(),
                inner.cert_file.clone(),
                inner.key_file.clone(),
                inner.verify_mode,
            )
        };

        let mut builder = SslConnector::builder(SslMethod::tls_client())
            .map_err(|e| TlsConfigError::ContextCreationFailed(e.to_string()))?;

        builder
            .set_default_verify_paths()
            .map_err(|e| TlsConfigError::DefaultVerifyPathsLoadFailed(e.to_string()))?;

        match (&ca_file, &ca_path) {
            (None, None) => {}
            (cafile, capath) => {
                builder
                    .load_verify_locations(cafile.as_deref(), capath.as_deref())
                    .map_err(|e| TlsConfigError::CaLocationsLoadFailed {
                        cafile: cafile.clone(),
                        capath: capath.clone(),
                        cause: e.to_string(),
                    })?;
            }
        }

        let ca_data_original = match ca_data {
            Some(ca_data) => {
                for certificate in ca_data.extracted.0 {
                    builder
                        .cert_store_mut()
                        .add_cert(certificate)
                        .map_err(|e| TlsConfigError::CaDataLoadFailed(e.to_string()))?;
                }

                Some(ca_data.original)
            }
            None => None,
        };

        if let Some(cert_file) = &cert_file {
            builder.set_certificate_chain_file(cert_file).map_err(|e| {
                TlsConfigError::CertFileLoadFailed {
                    path: cert_file.clone(),
                    cause: e.to_string(),
                }
            })?;

            // If no separate keyfile was given, OpenSSL reads the key from the
            // cert file itself. Same behaviour as Python's ssl when keyfile=None.
            let key_path = key_file.as_ref().unwrap_or(cert_file);
            builder
                .set_private_key_file(key_path, SslFiletype::PEM)
                .map_err(|e| TlsConfigError::KeyFileLoadFailed {
                    path: key_path.clone(),
                    cause: e.to_string(),
                })?;
        }

        builder.set_verify(verify_mode);
        let snapshot = PyTlsConfig {
            ca_file,
            ca_path,
            ca_data: ca_data_original,
            cert_file,
            key_file,
            verify_mode: verify_mode.into(),
        };
        Ok((builder.build().into_context(), snapshot))
    }
}

#[derive(Clone)]
struct CaData(Vec<X509>);

impl<'py> FromPyObject<'_, 'py> for CaData {
    type Error = TlsConfigError;

    fn extract(ca_data: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        let certificates = if let Ok(pem) = ca_data.extract::<String>() {
            X509::stack_from_pem(pem.as_bytes())
        } else if let Ok(der) = ca_data.extract::<Vec<u8>>() {
            X509::from_der(&der).map(|certificate| vec![certificate])
        } else {
            return Err(TlsConfigError::CaDataLoadFailed(
                "cadata must be a PEM string or DER bytes".to_string(),
            ));
        }
        .map_err(|e| TlsConfigError::CaDataLoadFailed(e.to_string()))?;
        Ok(Self(certificates))
    }
}

#[pymodule]
pub(crate) fn tls(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyTlsContext>()?;
    module.add_class::<PyTlsConfig>()?;
    module.add_class::<PyVerifyMode>()?;
    Ok(())
}
