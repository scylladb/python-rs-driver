use crate::errors::{
    DriverAddressTranslationError, DriverHostFilterError, DriverSessionConfigError,
};
use crate::routing::PyToken;
use crate::session_builder::PyDuration;
use crate::utils::{ParsedAddress, ParsedAddressList, PyValueOrError};
use async_trait::async_trait;
use pyo3::IntoPyObject;
use pyo3::PyErr;
use pyo3::exceptions::PyNotImplementedError;
use pyo3::prelude::{PyAnyMethods, PyDictMethods, PyModule, PyModuleMethods};
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyDict, PyString, PyTuple};
use pyo3::{
    Borrowed, Bound, BoundObject, FromPyObject, Py, PyAny, PyResult, Python, intern, pyclass,
    pymethods, pymodule,
};
use scylla::authentication::{AuthError, AuthenticatorProvider, AuthenticatorSession};
use scylla::cluster::metadata::Peer;
use scylla::errors::{CustomTranslationError, TranslationError};
use scylla::policies::address_translator::{AddressTranslator, UntranslatedPeer};
use scylla::policies::host_filter::{
    AcceptAllHostFilter, AllowListHostFilter, DcHostFilter, HostFilter,
};
use scylla::policies::timestamp_generator::{
    MonotonicTimestampGenerator, SimpleTimestampGenerator, TimestampGenerator,
};
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Python-side base class that users subclass to provide a custom authenticator provider.
/// Exposed to Python as `AuthenticatorProvider`.
#[pyclass(subclass, skip_from_py_object, name = "AuthenticatorProvider", frozen)]
pub(crate) struct PyAuthenticatorProviderClass {}

#[pymethods]
impl PyAuthenticatorProviderClass {
    #[expect(unused_variables)]
    #[new]
    #[pyo3(signature = (*args, **kwargs))]
    pub fn new(args: &Bound<'_, PyTuple>, kwargs: Option<&Bound<'_, PyDict>>) -> Self {
        PyAuthenticatorProviderClass {}
    }

    fn new_authenticator(&self, _authenticator_name: &str) -> PyResult<Py<PyAuthenticatorClass>> {
        Err(PyNotImplementedError::new_err("Method is not implemented"))
    }
}

/// Holds a Python object (subclass of `AuthenticatorProvider`) and implements the Rust
/// `AuthenticatorProvider` trait by delegating to the Python object's methods.
struct CustomAuthenticatorProvider {
    python_authenticator: Py<PyAuthenticatorProviderClass>,
}

#[async_trait]
impl AuthenticatorProvider for CustomAuthenticatorProvider {
    async fn start_authentication_session(
        &self,
        authenticator_name: &str,
    ) -> Result<(Option<Vec<u8>>, Box<dyn AuthenticatorSession>), AuthError> {
        let (result, py_auth) = Python::attach(
            |py| -> PyResult<(Option<Vec<u8>>, Box<CustomAuthenticator>)> {
                let py_auth_provider = self.python_authenticator.bind(py);

                let py_auth_any = py_auth_provider
                    .call_method1(intern!(py, "new_authenticator"), (authenticator_name,))?;

                let py_auth = py_auth_any.cast::<PyAuthenticatorClass>()?;

                let response = py_auth
                    .call_method0(intern!(py, "initial_response"))?
                    .extract::<Option<Vec<u8>>>()?;

                Ok((
                    response,
                    Box::new(CustomAuthenticator {
                        python_authenticator: py_auth.to_owned().unbind(),
                    }),
                ))
            },
        )
        .map_err(|e| format!("Python new_authenticator failed: {:?}", e))?;

        Ok((result, py_auth))
    }
}

/// Python-side base class that users subclass to implement authentication logic for a single session.
/// Exposed to Python as `Authenticator`.
#[pyclass(subclass, name = "Authenticator", frozen)]
pub(crate) struct PyAuthenticatorClass {}

#[pymethods]
impl PyAuthenticatorClass {
    #[expect(unused_variables)]
    #[new]
    #[pyo3(signature = (*args, **kwargs))]
    pub fn new(args: &Bound<'_, PyTuple>, kwargs: Option<&Bound<'_, PyDict>>) -> Self {
        PyAuthenticatorClass {}
    }

    fn initial_response(&self) -> PyResult<Option<Vec<u8>>> {
        Ok(None)
    }

    fn evaluate_challenge(&self, _challenge: Option<&[u8]>) -> PyResult<Option<Vec<u8>>> {
        Err(PyNotImplementedError::new_err("Method is not implemented"))
    }

    fn success(&self, _token: Option<&[u8]>) -> PyResult<()> {
        Ok(())
    }
}

/// Holds a Python object (subclass of `Authenticator`) and implements the Rust
/// `AuthenticatorSession` trait by delegating to the Python object's methods.
struct CustomAuthenticator {
    python_authenticator: Py<PyAuthenticatorClass>,
}

#[async_trait]
impl AuthenticatorSession for CustomAuthenticator {
    async fn evaluate_challenge(
        &mut self,
        token: Option<&[u8]>,
    ) -> Result<Option<Vec<u8>>, AuthError> {
        let result = Python::attach(|py| -> PyResult<Option<Vec<u8>>> {
            let py_auth = self.python_authenticator.bind(py);

            py_auth
                .call_method1(intern!(py, "evaluate_challenge"), (token,))?
                .extract::<Option<Vec<u8>>>()
        })
        .map_err(|e| format!("Python evaluate_challenge failed: {:?}", e))?;

        Ok(result)
    }

    async fn success(&mut self, token: Option<&[u8]>) -> Result<(), AuthError> {
        let result = Python::attach(|py| -> PyResult<()> {
            let py_auth = self.python_authenticator.bind(py);

            py_auth.call_method1(intern!(py, "success"), (token,))?;

            Ok(())
        })
        .map_err(|e| format!("Python success failed: {:?}", e))?;

        Ok(result)
    }
}

/// Python-facing input type that extracts an `Arc<dyn AuthenticatorProvider>` from a Python object.
pub(crate) struct PyAuthenticatorProvider {
    inner: Arc<dyn AuthenticatorProvider>,
}

impl PyAuthenticatorProvider {
    pub(crate) fn into_inner(self) -> Arc<dyn AuthenticatorProvider> {
        self.inner
    }
}

impl<'py> FromPyObject<'_, 'py> for PyAuthenticatorProvider {
    type Error = DriverSessionConfigError;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(python_authenticator) = obj.extract::<Py<PyAuthenticatorProviderClass>>() {
            return Ok(Self {
                inner: Arc::new(CustomAuthenticatorProvider {
                    python_authenticator,
                }),
            });
        }

        Err(DriverSessionConfigError::invalid_authenticator_provider(
            obj,
        ))
    }
}

/// Stores a Python object with a `translate` method (user's custom implementation)
/// and implements the Rust `AddressTranslator` trait by delegating to that Python object.
struct CustomAddressTranslator {
    inner: Py<PyAny>,
}

#[async_trait]
impl AddressTranslator for CustomAddressTranslator {
    async fn translate_address(
        &self,
        untranslated_peer: &UntranslatedPeer,
    ) -> Result<SocketAddr, TranslationError> {
        Python::attach(|py| -> PyResult<SocketAddr> {
            let py_trans = self.inner.bind(py);
            let peer_info = PyUntranslatedPeer::from(untranslated_peer);

            let translated = py_trans
                .call_method1(intern!(py, "translate"), (peer_info,))?
                .extract::<ParsedAddress>()?;

            SocketAddr::try_from(translated).map_err(|e| e.into())
        })
        .map_err(|e| CustomTranslationError::new(e).into())
    }
}

/// Python-facing input type for address translator. Extracts from a built-in `PyDictAddressTranslator`
/// or wraps any Python object with a `translate` method as a `CustomAddressTranslator`.
pub(crate) struct PyAddressTranslator {
    inner: Arc<dyn AddressTranslator>,
}

impl PyAddressTranslator {
    pub(crate) fn into_inner(self) -> Arc<dyn AddressTranslator> {
        self.inner
    }
}

impl<'py> FromPyObject<'_, 'py> for PyAddressTranslator {
    type Error = DriverSessionConfigError;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(dict) = obj.cast::<PyDictAddressTranslator>() {
            return Ok(Self {
                inner: Arc::clone(&dict.get().inner) as Arc<dyn AddressTranslator>,
            });
        }

        if !obj.hasattr(intern!(obj.py(), "translate")).unwrap_or(false) {
            return Err(DriverSessionConfigError::invalid_address_translator(obj));
        }

        Ok(Self {
            inner: Arc::new(CustomAddressTranslator {
                inner: obj.unbind(),
            }),
        })
    }
}

/// Built-in address translator that uses a dict-based address mapping.
/// Exposed to Python as `DictAddressTranslator`.
#[pyclass(name = "DictAddressTranslator", frozen)]
struct PyDictAddressTranslator {
    inner: Arc<HashMap<SocketAddr, SocketAddr>>,
}

#[pymethods]
impl PyDictAddressTranslator {
    #[new]
    pub fn new<'py>(dict: Bound<'py, PyDict>) -> Result<Self, DriverAddressTranslationError> {
        let map = dict
            .iter()
            .enumerate()
            .map(|(idx, (k, v))| {
                let from = k
                    .extract::<ParsedAddress>()
                    .and_then(SocketAddr::try_from)
                    .map_err(|e| DriverAddressTranslationError::invalid_address(idx, e))?;

                let to = v
                    .extract::<ParsedAddress>()
                    .and_then(SocketAddr::try_from)
                    .map_err(|e| DriverAddressTranslationError::invalid_address(idx, e))?;

                Ok((from, to))
            })
            .collect::<Result<HashMap<SocketAddr, SocketAddr>, DriverAddressTranslationError>>()?;

        Ok(PyDictAddressTranslator {
            inner: Arc::new(map),
        })
    }

    pub fn translate(
        &self,
        peer: Py<PyUntranslatedPeer>,
    ) -> Result<(IpAddr, u16), DriverAddressTranslationError> {
        let untranslated_peer = peer.get();
        let addr = SocketAddr::new(
            untranslated_peer.untranslated_address.0,
            untranslated_peer.untranslated_address.1,
        );
        match self.inner.get(&addr) {
            Some(&translated) => Ok((translated.ip(), translated.port())),
            None => Err(DriverAddressTranslationError::from(
                TranslationError::NoRuleForAddress(addr),
            )),
        }
    }
}

/// Python representation of an untranslated peer address, exposing host_id, untranslated_address,
/// datacenter, and rack. Exposed to Python as `UntranslatedPeer`.
#[pyclass(name = "UntranslatedPeer", frozen)]
pub struct PyUntranslatedPeer {
    host_id: uuid::Uuid,
    untranslated_address: (IpAddr, u16),
    datacenter: Option<String>,
    rack: Option<String>,

    // Cached Python-side representations used by the getters.
    pub py_host_id: PyOnceLock<Py<PyAny>>,
    pub py_untranslated_address: PyOnceLock<Py<PyTuple>>,
    pub py_datacenter: PyOnceLock<Py<PyString>>,
    pub py_rack: PyOnceLock<Py<PyString>>,
}

#[pymethods]
impl PyUntranslatedPeer {
    #[getter]
    fn host_id(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(self
            .py_host_id
            .get_or_try_init(py, || {
                Ok::<_, PyErr>(self.host_id.into_pyobject(py)?.unbind())
            })?
            .clone_ref(py))
    }

    #[getter]
    fn untranslated_address(&self, py: Python<'_>) -> PyResult<Py<PyTuple>> {
        Ok(self
            .py_untranslated_address
            .get_or_try_init(py, || {
                let (ip, port) = self.untranslated_address;

                Ok::<_, PyErr>(
                    (ip, port)
                        .into_pyobject(py)?
                        .cast_into::<PyTuple>()?
                        .unbind(),
                )
            })?
            .clone_ref(py))
    }

    #[getter]
    fn datacenter(&self, py: Python<'_>) -> Py<PyAny> {
        match &self.datacenter {
            None => py.None(),
            Some(datacenter) => self
                .py_datacenter
                .get_or_init(py, || PyString::new(py, datacenter).unbind())
                .clone_ref(py)
                .into_any(),
        }
    }

    #[getter]
    fn rack(&self, py: Python<'_>) -> Py<PyAny> {
        match &self.rack {
            None => py.None(),
            Some(rack) => self
                .py_rack
                .get_or_init(py, || PyString::new(py, rack).unbind())
                .clone_ref(py)
                .into_any(),
        }
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<Py<PyString>> {
        let (ip, port) = self.untranslated_address;

        let repr_str = PyString::from_fmt(
            py,
            format_args!(
                "UntranslatedPeer(host_id='{}', untranslated_address=('{}', {}), datacenter={:?}, rack={:?})",
                self.host_id, ip, port, self.datacenter, self.rack
            ),
        )?;

        Ok(repr_str.into())
    }
}

impl From<&UntranslatedPeer<'_>> for PyUntranslatedPeer {
    fn from(peer: &UntranslatedPeer) -> Self {
        Self {
            host_id: peer.host_id(),
            untranslated_address: (
                peer.untranslated_address().ip(),
                peer.untranslated_address().port(),
            ),
            datacenter: peer.datacenter().map(|s| s.to_string()),
            rack: peer.rack().map(|s| s.to_string()),

            py_host_id: PyOnceLock::new(),
            py_untranslated_address: PyOnceLock::new(),
            py_datacenter: PyOnceLock::new(),
            py_rack: PyOnceLock::new(),
        }
    }
}

impl<'a> From<&'a PyUntranslatedPeer> for UntranslatedPeer<'a> {
    fn from(peer: &'a PyUntranslatedPeer) -> UntranslatedPeer<'a> {
        let (ip, port) = peer.untranslated_address;

        UntranslatedPeer::from_fields(
            peer.host_id,
            SocketAddr::new(ip, port),
            peer.datacenter.as_deref(),
            peer.rack.as_deref(),
        )
    }
}

/// Stores a Python object with a `next_timestamp` method (user's custom implementation)
/// and implements the Rust `TimestampGenerator` trait by delegating to that Python object.
pub struct CustomTimestampGenerator {
    pub py_timestamp_generator: Py<PyAny>,
}

impl TimestampGenerator for CustomTimestampGenerator {
    fn next_timestamp(&self) -> i64 {
        Python::attach(|py| {
            let py_generator = self.py_timestamp_generator.bind(py);

            py_generator
                .call_method0(intern!(py, "next_timestamp"))
                .and_then(|res| res.extract::<i64>())
                .unwrap_or_else(|err| {
                    log::error!("Failed to generate custom timestamp from Python: {}", err);

                    // Returns current system time in microseconds as a fallback
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_micros() as i64)
                        .unwrap_or(0)
                })
        })
    }
}

/// Python-facing input type for timestamp generator. Extracts from built-in `PyMonotonicTimestampGenerator`,
/// `PySimpleTimestampGenerator`, or wraps any Python object with a `next_timestamp` method
/// as a `CustomTimestampGenerator`.
pub(crate) struct PyTimestampGenerator {
    inner: Arc<dyn TimestampGenerator>,
}

impl PyTimestampGenerator {
    pub(crate) fn into_inner(self) -> Arc<dyn TimestampGenerator> {
        self.inner
    }
}

impl<'py> FromPyObject<'_, 'py> for PyTimestampGenerator {
    type Error = DriverSessionConfigError;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(monotonic) = obj.cast::<PyMonotonicTimestampGenerator>() {
            return Ok(Self {
                inner: Arc::clone(&monotonic.get().inner) as Arc<dyn TimestampGenerator>,
            });
        }

        if let Ok(simple) = obj.cast::<PySimpleTimestampGenerator>() {
            return Ok(Self {
                inner: Arc::clone(&simple.get().inner) as Arc<dyn TimestampGenerator>,
            });
        }

        if !obj
            .hasattr(intern!(obj.py(), "next_timestamp"))
            .unwrap_or(false)
        {
            return Err(DriverSessionConfigError::invalid_timestamp_generator(obj));
        }

        Ok(Self {
            inner: Arc::new(CustomTimestampGenerator {
                py_timestamp_generator: obj.unbind(),
            }),
        })
    }
}

/// Built-in timestamp generator that guarantees monotonically increasing timestamps.
/// Exposed to Python as `MonotonicTimestampGenerator`.
#[pyclass(name = "MonotonicTimestampGenerator", frozen)]
struct PyMonotonicTimestampGenerator {
    inner: Arc<MonotonicTimestampGenerator>,
}

#[pymethods]
impl PyMonotonicTimestampGenerator {
    #[new]
    #[pyo3(signature = (warn_on_drift=true, warning_threshold=PyDuration(Duration::from_secs(1)), warning_interval=PyDuration(Duration::from_secs(1))))]
    pub fn new(
        warn_on_drift: bool,
        warning_threshold: PyDuration,
        warning_interval: PyDuration,
    ) -> Self {
        let mut monotonic_timestamp_generator = MonotonicTimestampGenerator::new()
            .with_warning_times(warning_threshold.0, warning_interval.0);

        if !warn_on_drift {
            monotonic_timestamp_generator = monotonic_timestamp_generator.without_warnings();
        }

        PyMonotonicTimestampGenerator {
            inner: Arc::new(monotonic_timestamp_generator),
        }
    }

    pub fn next_timestamp(&self) -> i64 {
        self.inner.next_timestamp()
    }
}

/// Built-in timestamp generator returning `SystemTime`-based microsecond timestamps.
/// Exposed to Python as `SimpleTimestampGenerator`.
#[pyclass(name = "SimpleTimestampGenerator", frozen)]
struct PySimpleTimestampGenerator {
    inner: Arc<SimpleTimestampGenerator>,
}

#[pymethods]
impl PySimpleTimestampGenerator {
    #[new]
    pub fn new() -> Self {
        PySimpleTimestampGenerator {
            inner: Arc::new(SimpleTimestampGenerator {}),
        }
    }

    pub fn next_timestamp(&self) -> i64 {
        self.inner.next_timestamp()
    }
}

/// Stores a Python object with an `accept` method (user's custom implementation)
/// and implements the Rust `HostFilter` trait by delegating to that Python object.
struct CustomHostFilter {
    py_host_filter: Py<PyAny>,
}
impl HostFilter for CustomHostFilter {
    fn accept(&self, peer: &Peer) -> bool {
        Python::attach(|py| {
            let py_filter = self.py_host_filter.bind(py);
            let py_peer = PyPeer::from(peer.clone());

            py_filter
                .call_method1(intern!(py, "accept"), (py_peer,))
                .and_then(|res| res.extract::<bool>())
                .unwrap_or_else(|err| {
                    log::error!("Failed to evaluate custom host filter from Python: {}", err);
                    true
                })
        })
    }
}

/// Python-facing input type for host filter. Extracts from built-in `PyAcceptAllHostFilter`,
/// `PyDcHostFilter`, `PyAllowListHostFilter`, or wraps any Python object with an `accept` method
/// as a `CustomHostFilter`.
pub(crate) struct PyHostFilter {
    inner: Arc<dyn HostFilter>,
}

impl PyHostFilter {
    pub(crate) fn into_inner(self) -> Arc<dyn HostFilter> {
        self.inner
    }
}

impl<'py> FromPyObject<'_, 'py> for PyHostFilter {
    type Error = DriverSessionConfigError;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(filter) = obj.cast::<PyAllowListHostFilter>() {
            return Ok(Self {
                inner: Arc::clone(&filter.get().inner) as Arc<dyn HostFilter>,
            });
        }

        if let Ok(filter) = obj.cast::<PyAcceptAllHostFilter>() {
            return Ok(Self {
                inner: Arc::clone(&filter.get().inner) as Arc<dyn HostFilter>,
            });
        }

        if let Ok(filter) = obj.cast::<PyDcHostFilter>() {
            return Ok(Self {
                inner: Arc::clone(&filter.get().inner) as Arc<dyn HostFilter>,
            });
        }

        if !obj.hasattr(intern!(obj.py(), "accept")).unwrap_or(false) {
            return Err(DriverSessionConfigError::invalid_host_filter(obj));
        }

        Ok(Self {
            inner: Arc::new(CustomHostFilter {
                py_host_filter: obj.to_owned().unbind(),
            }),
        })
    }
}

/// Built-in host filter that accepts all peers. Exposed to Python as `AcceptAllHostFilter`.
#[pyclass(name = "AcceptAllHostFilter", frozen)]
struct PyAcceptAllHostFilter {
    inner: Arc<AcceptAllHostFilter>,
}

#[pymethods]
impl PyAcceptAllHostFilter {
    #[new]
    pub fn new() -> Self {
        PyAcceptAllHostFilter {
            inner: Arc::new(AcceptAllHostFilter {}),
        }
    }

    pub fn accept(&self, _peer: Py<PyPeer>) -> bool {
        true
    }
}

/// Built-in host filter that accepts only peers in a given datacenter.
/// Exposed to Python as `DcHostFilter`.
#[pyclass(name = "DcHostFilter", frozen)]
struct PyDcHostFilter {
    inner: Arc<DcHostFilter>,
}

#[pymethods]
impl PyDcHostFilter {
    #[new]
    pub fn new(local_dc: String) -> Self {
        PyDcHostFilter {
            inner: Arc::new(DcHostFilter::new(local_dc)),
        }
    }

    pub fn accept(&self, peer: Py<PyPeer>) -> bool {
        self.inner.accept(&peer.get().inner)
    }
}

/// Built-in host filter that accepts only peers whose address matches a given allow list.
/// Exposed to Python as `AllowListHostFilter`.
#[pyclass(name = "AllowListHostFilter", frozen)]
struct PyAllowListHostFilter {
    inner: Arc<AllowListHostFilter>,
}

#[pymethods]
impl PyAllowListHostFilter {
    #[new]
    pub fn new(list: ParsedAddressList) -> Result<Self, DriverHostFilterError> {
        let filter =
            AllowListHostFilter::new(list.inner).map_err(DriverHostFilterError::invalid_address)?;

        Ok(PyAllowListHostFilter {
            inner: Arc::new(filter),
        })
    }

    pub fn accept(&self, peer: Py<PyPeer>) -> bool {
        self.inner.accept(&peer.get().inner)
    }
}

/// Python representation of a cluster peer node, exposing host_id, address, tokens, datacenter, and rack.
/// Exposed to Python as `Peer`.
#[pyclass(frozen, name = "Peer")]
pub struct PyPeer {
    inner: Peer,
    py_host_id: PyOnceLock<Py<PyAny>>,
    py_address: PyOnceLock<Py<PyTuple>>,
    py_tokens: PyOnceLock<Py<PyTuple>>,
    py_datacenter: PyOnceLock<Py<PyString>>,
    py_rack: PyOnceLock<Py<PyString>>,
}

#[pymethods]
impl PyPeer {
    #[getter]
    fn host_id(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(self
            .py_host_id
            .get_or_try_init(py, || {
                Ok::<_, PyErr>(self.inner.host_id.into_pyobject(py)?.unbind())
            })?
            .clone_ref(py))
    }

    #[getter]
    fn address(&self, py: Python<'_>) -> PyResult<Py<PyTuple>> {
        Ok(self
            .py_address
            .get_or_try_init(py, || {
                let ip = self.inner.address.ip();
                let port = self.inner.address.port();
                Ok::<_, PyErr>(
                    (ip, port)
                        .into_pyobject(py)?
                        .cast_into::<PyTuple>()?
                        .unbind(),
                )
            })?
            .clone_ref(py))
    }

    #[getter]
    fn tokens(&self, py: Python<'_>) -> PyResult<Py<PyTuple>> {
        Ok(self
            .py_tokens
            .get_or_try_init(py, || {
                let mapped_tokens = self
                    .inner
                    .tokens
                    .iter()
                    .map(|token| PyValueOrError::new(Py::new(py, PyToken::from(*token))));

                PyTuple::new(py, mapped_tokens).map(|t| t.unbind())
            })?
            .clone_ref(py))
    }

    #[getter]
    fn datacenter(&self, py: Python<'_>) -> Py<PyAny> {
        match &self.inner.datacenter {
            None => py.None(),
            Some(datacenter) => self
                .py_datacenter
                .get_or_init(py, || PyString::new(py, datacenter).unbind())
                .clone_ref(py)
                .into_any(),
        }
    }

    #[getter]
    fn rack(&self, py: Python<'_>) -> Py<PyAny> {
        match &self.inner.rack {
            None => py.None(),
            Some(rack) => self
                .py_rack
                .get_or_init(py, || PyString::new(py, rack).unbind())
                .clone_ref(py)
                .into_any(),
        }
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<Py<PyString>> {
        let ip = self.inner.address.ip();
        let port = self.inner.address.port();

        let repr_str = PyString::from_fmt(
            py,
            format_args!(
                "Peer(host_id='{}', address=('{}', {}), tokens={}, datacenter={:?}, rack={:?})",
                self.inner.host_id,
                ip,
                port,
                self.tokens(py)?,
                self.inner.datacenter,
                self.inner.rack
            ),
        )?;

        Ok(repr_str.into())
    }
}

impl From<Peer> for PyPeer {
    fn from(peer: Peer) -> Self {
        Self {
            inner: peer,
            py_host_id: PyOnceLock::new(),
            py_address: PyOnceLock::new(),
            py_tokens: PyOnceLock::new(),
            py_datacenter: PyOnceLock::new(),
            py_rack: PyOnceLock::new(),
        }
    }
}

#[pymodule]
pub(crate) fn policies(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyAuthenticatorProviderClass>()?;
    module.add_class::<PyAuthenticatorClass>()?;
    module.add_class::<PyDictAddressTranslator>()?;
    module.add_class::<PyUntranslatedPeer>()?;
    module.add_class::<PyMonotonicTimestampGenerator>()?;
    module.add_class::<PySimpleTimestampGenerator>()?;
    module.add_class::<PyAcceptAllHostFilter>()?;
    module.add_class::<PyDcHostFilter>()?;
    module.add_class::<PyAllowListHostFilter>()?;
    module.add_class::<PyPeer>()?;
    Ok(())
}
