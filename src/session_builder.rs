use crate::RUNTIME;
use crate::enums::{PyCompression, PyPoolSize, PySelfIdentity, PyWriteCoalescingDelay};
use crate::errors::{DriverSessionConfigError, DriverSessionConnectionError};
use crate::execution_profile::PyExecutionProfile;
use crate::policies::address_translator::PyAddressTranslator;
use crate::policies::authenticator_provider::PyAuthenticatorProvider;
use crate::policies::host_filter::PyHostFilter;
use crate::policies::timestamp_generator::PyTimestampGenerator;
use crate::session::PySession;
use crate::tls::{PyTlsConfig, PyTlsContext};
use crate::utils::{ParsedAddress, ParsedAddressList, WithOriginalPyObject};
use pyo3::prelude::*;
use pyo3::sync::MutexExt;
use scylla::authentication::PlainTextAuthenticator;
use scylla::client::session::{SessionConfig, TlsContext};
use scylla::routing::ShardAwarePortRange;
use std::net::IpAddr;
use std::ops::RangeInclusive;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[pyclass(frozen)]
struct SessionBuilder {
    inner: Mutex<PySessionBuilderConfig>,
}

#[pymethods]
impl SessionBuilder {
    #[new]
    fn new(py: Python) -> PyResult<Self> {
        Ok(Self {
            inner: Mutex::new(PySessionBuilderConfig::new(py)?),
        })
    }
    fn contact_points<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        contact_points: ContactPoints,
    ) -> PyRef<'py, Self> {
        {
            let mut inner = slf.inner.lock_py_attached(py).unwrap();

            contact_points.add_known_nodes(&mut inner.config);
            inner.contact_points.extend(contact_points.inner);
        }

        slf
    }

    fn execution_profile<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        execution_profile: Py<PyExecutionProfile>,
    ) -> PyRef<'py, Self> {
        {
            let mut inner = slf.inner.lock_py_attached(py).unwrap();
            inner.execution_profile = execution_profile.clone();
            inner.config.default_execution_profile_handle =
                execution_profile.get().inner.clone().into_handle();
        }
        slf
    }

    fn user<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        username: String,
        password: String,
    ) -> PyRef<'py, Self> {
        {
            let mut inner = slf.inner.lock_py_attached(py).unwrap();
            inner.config.authenticator =
                Some(Arc::new(PlainTextAuthenticator::new(username, password)));
        }
        slf
    }

    fn authenticator_provider<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        authenticator: WithOriginalPyObject<PyAuthenticatorProvider>,
    ) -> Result<PyRef<'py, Self>, DriverSessionConfigError> {
        {
            let mut inner = slf.inner.lock_py_attached(py).unwrap();
            inner.authenticator = Some(authenticator.original.clone());
            inner.config.authenticator = Some(authenticator.extracted.into_inner());
        }

        Ok(slf)
    }

    fn address_translator<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        translator: WithOriginalPyObject<PyAddressTranslator>,
    ) -> Result<PyRef<'py, Self>, DriverSessionConfigError> {
        {
            let mut inner = slf.inner.lock_py_attached(py).unwrap();
            inner.address_translator = Some(translator.original.clone());
            inner.config.address_translator = Some(translator.extracted.into_inner());
        }

        Ok(slf)
    }

    fn timestamp_generator<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        generator: WithOriginalPyObject<PyTimestampGenerator>,
    ) -> Result<PyRef<'py, Self>, DriverSessionConfigError> {
        {
            let mut inner = slf.inner.lock_py_attached(py).unwrap();
            inner.timestamp_generator = Some(generator.original.clone());
            inner.config.timestamp_generator = Some(generator.extracted.into_inner());
        }

        Ok(slf)
    }
    fn host_filter<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        host_filter: WithOriginalPyObject<PyHostFilter>,
    ) -> Result<PyRef<'py, Self>, DriverSessionConfigError> {
        {
            let mut inner = slf.inner.lock_py_attached(py).unwrap();
            inner.host_filter = Some(host_filter.original.clone());
            inner.config.host_filter = Some(host_filter.extracted.into_inner());
        }

        Ok(slf)
    }

    fn local_ip_address<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        ip: Option<IpAddr>,
    ) -> PyRef<'py, Self> {
        {
            let mut inner = slf.inner.lock_py_attached(py).unwrap();
            inner.config.local_ip_address = ip;
        }
        slf
    }

    fn shard_aware_local_port_range<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        port_range: (u16, u16),
    ) -> Result<PyRef<'py, Self>, DriverSessionConfigError> {
        {
            let mut inner = slf.inner.lock_py_attached(py).unwrap();
            inner.shard_aware_local_port_range = port_range;
            inner.config.shard_aware_local_port_range =
                ShardAwarePortRange::new(RangeInclusive::new(port_range.0, port_range.1))
                    .map_err(|_| DriverSessionConfigError::InvalidPortRange)?;
        }
        Ok(slf)
    }

    fn compression<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        compression: Option<PyCompression>,
    ) -> PyRef<'py, Self> {
        {
            let mut inner = slf.inner.lock_py_attached(py).unwrap();
            inner.config.compression = compression.map(|c| c.into());
        }
        slf
    }

    fn schema_agreement_interval<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        timeout: PyDuration,
    ) -> PyRef<'py, Self> {
        {
            let mut inner = slf.inner.lock_py_attached(py).unwrap();
            inner.config.schema_agreement_interval = timeout.0;
        }

        slf
    }

    pub fn tcp_nodelay<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        nodelay: bool,
    ) -> PyRef<'py, Self> {
        {
            let mut inner = slf.inner.lock_py_attached(py).unwrap();
            inner.config.tcp_nodelay = nodelay;
        }
        slf
    }

    fn tcp_keepalive_interval<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        interval: Option<PyDuration>,
    ) -> PyRef<'py, Self> {
        if let Some(ref dur) = interval
            && dur.0 <= Duration::from_secs(1)
        {
            log::warn!(
                "Setting the TCP keepalive interval to low values ({:?}) is not recommended as it can have a negative impact on performance. Consider setting it above 1 second.",
                dur.0
            );
        }

        {
            let mut inner = slf.inner.lock_py_attached(py).unwrap();
            inner.config.tcp_keepalive_interval = interval.map(|delta| delta.0);
        }

        slf
    }

    fn use_keyspace<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        keyspace_name: String,
        case_sensitive: bool,
    ) -> PyRef<'py, Self> {
        {
            let mut inner = slf.inner.lock_py_attached(py).unwrap();
            inner.config.used_keyspace = Some(keyspace_name);
            inner.config.keyspace_case_sensitive = case_sensitive;
        }
        slf
    }

    fn connection_timeout<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        timeout: PyDuration,
    ) -> PyRef<'py, Self> {
        {
            let mut inner = slf.inner.lock_py_attached(py).unwrap();
            inner.config.connect_timeout = timeout.0;
        }
        slf
    }

    fn pool_size<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        size: PyPoolSize,
    ) -> PyRef<'py, Self> {
        {
            let mut inner = slf.inner.lock_py_attached(py).unwrap();
            inner.config.connection_pool_size = size.inner;
        }
        slf
    }

    fn disallow_shard_aware_port<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        disallow: bool,
    ) -> PyRef<'py, Self> {
        {
            let mut inner = slf.inner.lock_py_attached(py).unwrap();
            inner.config.disallow_shard_aware_port = disallow;
        }
        slf
    }

    fn keyspaces_to_fetch<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        keyspaces: Vec<String>,
    ) -> PyRef<'py, Self> {
        {
            let mut inner = slf.inner.lock_py_attached(py).unwrap();
            inner.config.keyspaces_to_fetch = keyspaces;
        }
        slf
    }

    fn fetch_schema_metadata<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        fetch: bool,
    ) -> PyRef<'py, Self> {
        {
            let mut inner = slf.inner.lock_py_attached(py).unwrap();
            inner.config.fetch_schema_metadata = fetch;
        }
        slf
    }

    fn metadata_request_serverside_timeout<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        timeout: PyDuration,
    ) -> PyRef<'py, Self> {
        {
            let mut inner = slf.inner.lock_py_attached(py).unwrap();
            inner.config.metadata_request_serverside_timeout = Some(timeout.0);
        }
        slf
    }

    fn keepalive_interval<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        interval: Option<PyDuration>,
    ) -> PyResult<PyRef<'py, Self>> {
        if let Some(ref interval) = interval
            && interval.0 <= Duration::from_secs(1)
        {
            log::warn!(
                "Setting the keepalive interval to low values ({:?}) is not recommended as it can have a negative impact on performance. Consider setting it above 5 second.",
                interval.0
            );
        }

        {
            let mut inner = slf.inner.lock_py_attached(py).unwrap();
            inner.config.keepalive_interval = interval.map(|value| value.0);
        }
        Ok(slf)
    }

    fn keepalive_timeout<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        timeout: Option<PyDuration>,
    ) -> PyRef<'py, Self> {
        if let Some(ref timeout) = timeout
            && timeout.0 <= Duration::from_secs(1)
        {
            log::warn!(
                "Setting the keepalive timeout to low values ({:?}) is not recommended as it can have a negative impact on performance. Consider setting it above 5 second.",
                timeout.0
            );
        }

        {
            let mut inner = slf.inner.lock_py_attached(py).unwrap();
            inner.config.keepalive_timeout = timeout.map(|value| value.0);
        }
        slf
    }

    fn schema_agreement_timeout<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        timeout: PyDuration,
    ) -> PyRef<'py, Self> {
        {
            let mut inner = slf.inner.lock_py_attached(py).unwrap();
            inner.config.schema_agreement_timeout = timeout.0;
        }
        slf
    }

    fn auto_await_schema_agreement<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        enabled: bool,
    ) -> PyRef<'py, Self> {
        {
            let mut inner = slf.inner.lock_py_attached(py).unwrap();
            inner.config.schema_agreement_automatic_waiting = enabled;
        }
        slf
    }

    fn hostname_resolution_timeout<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        duration: Option<PyDuration>,
    ) -> PyRef<'py, Self> {
        {
            let mut inner = slf.inner.lock_py_attached(py).unwrap();
            inner.config.hostname_resolution_timeout = duration.map(|d| d.0);
        }
        slf
    }

    fn refresh_metadata_on_auto_schema_agreement<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        refresh_metadata: bool,
    ) -> PyRef<'py, Self> {
        {
            let mut inner = slf.inner.lock_py_attached(py).unwrap();
            inner.config.refresh_metadata_on_auto_schema_agreement = refresh_metadata;
        }
        slf
    }

    fn write_coalescing<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        delay: Option<PyWriteCoalescingDelay>,
    ) -> PyRef<'py, Self> {
        {
            let mut inner = slf.inner.lock_py_attached(py).unwrap();
            if let Some(delay) = delay {
                inner.config.write_coalescing_delay = delay.inner;
                inner.config.enable_write_coalescing = true;
            } else {
                inner.config.enable_write_coalescing = false;
            }
        }
        slf
    }

    pub fn custom_identity<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        identity: PySelfIdentity,
    ) -> PyRef<'py, Self> {
        {
            let mut inner = slf.inner.lock_py_attached(py).unwrap();
            inner.config.identity = identity.inner;
        }
        slf
    }

    fn tls_context<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        tls_context: Option<Py<PyTlsContext>>,
    ) -> PyResult<PyRef<'py, Self>> {
        {
            let mut inner = slf.inner.lock_py_attached(py).unwrap();
            match tls_context {
                Some(ctx) => {
                    let (context, snapshot) = ctx
                        .get()
                        .build_with_snapshot(py)
                        .map_err(DriverSessionConfigError::from)?;

                    inner.config.tls_context = Some(TlsContext::from(context));
                    inner.tls_context = Some(Py::new(py, snapshot)?);
                }
                None => {
                    inner.config.tls_context = None;
                    inner.tls_context = None;
                }
            }
        }
        Ok(slf)
    }

    fn get_config<'py>(&self, py: Python<'py>) -> PyResult<Py<PySessionBuilderConfig>> {
        let inner = self.inner.lock_py_attached(py).unwrap();
        Py::new(py, inner.clone())
    }

    async fn connect(&self) -> Result<PySession, DriverSessionConnectionError> {
        let config = Python::attach(|py| {
            let inner = self.inner.lock_py_attached(py).unwrap();
            inner.config.clone()
        });

        let session_result = RUNTIME
            .spawn(async move { scylla::client::session::Session::connect(config).await })
            .await?;
        match session_result {
            Ok(session) => PySession::try_from(Arc::new(session))
                .map_err(DriverSessionConnectionError::python_conversion_error),
            Err(err) => Err(DriverSessionConnectionError::new_session_error(err)),
        }
    }
}

#[derive(Clone)]
#[pyclass(name = "SessionBuilderConfig", frozen, skip_from_py_object)]
struct PySessionBuilderConfig {
    config: SessionConfig,
    #[pyo3(get)]
    pub execution_profile: Py<PyExecutionProfile>,
    #[pyo3(get)]
    pub contact_points: Vec<ParsedAddress>,
    #[pyo3(get)]
    pub host_filter: Option<Py<PyAny>>,
    #[pyo3(get)]
    pub authenticator: Option<Py<PyAny>>,
    #[pyo3(get)]
    pub address_translator: Option<Py<PyAny>>,
    #[pyo3(get)]
    pub timestamp_generator: Option<Py<PyAny>>,
    #[pyo3(get)]
    pub shard_aware_local_port_range: (u16, u16),
    #[pyo3(get)]
    pub tls_context: Option<Py<PyTlsConfig>>,
}

impl PySessionBuilderConfig {
    fn new(py: Python) -> PyResult<Self> {
        let config = SessionConfig::new();

        let execution_profile = Py::new(
            py,
            PyExecutionProfile {
                inner: config.default_execution_profile_handle.to_profile(),
                load_balancing_policy: None,
                retry_policy: None,
            },
        )?;

        Ok(Self {
            config,
            execution_profile,
            shard_aware_local_port_range: (49152, 65535),
            contact_points: Vec::new(),
            host_filter: None,
            authenticator: None,
            address_translator: None,
            timestamp_generator: None,
            tls_context: None,
        })
    }
}

#[pymethods]
impl PySessionBuilderConfig {
    #[getter]
    fn local_ip_address(&self) -> Option<IpAddr> {
        self.config.local_ip_address
    }

    #[getter]
    fn compression(&self) -> Option<PyCompression> {
        self.config.compression.map(PyCompression::from)
    }

    #[getter]
    fn tcp_nodelay(&self) -> bool {
        self.config.tcp_nodelay
    }

    #[getter]
    fn tcp_keepalive_interval(&self) -> Option<Duration> {
        self.config.tcp_keepalive_interval
    }

    #[getter]
    fn connect_timeout(&self) -> Duration {
        self.config.connect_timeout
    }

    #[getter]
    fn connection_pool_size(&self) -> PyPoolSize {
        PyPoolSize {
            inner: self.config.connection_pool_size,
        }
    }

    #[getter]
    fn disallow_shard_aware_port(&self) -> bool {
        self.config.disallow_shard_aware_port
    }

    #[getter]
    fn used_keyspace(&self) -> Option<String> {
        self.config.used_keyspace.clone()
    }

    #[getter]
    fn keyspace_case_sensitive(&self) -> bool {
        self.config.keyspace_case_sensitive
    }

    #[getter]
    fn keyspaces_to_fetch(&self) -> Vec<String> {
        self.config.keyspaces_to_fetch.clone()
    }

    #[getter]
    fn fetch_schema_metadata(&self) -> bool {
        self.config.fetch_schema_metadata
    }

    #[getter]
    fn metadata_request_serverside_timeout(&self) -> Option<Duration> {
        self.config.metadata_request_serverside_timeout
    }

    #[getter]
    fn schema_agreement_interval(&self) -> Duration {
        self.config.schema_agreement_interval
    }

    #[getter]
    fn schema_agreement_timeout(&self) -> Duration {
        self.config.schema_agreement_timeout
    }

    #[getter]
    fn schema_agreement_automatic_waiting(&self) -> bool {
        self.config.schema_agreement_automatic_waiting
    }

    #[getter]
    fn refresh_metadata_on_auto_schema_agreement(&self) -> bool {
        self.config.refresh_metadata_on_auto_schema_agreement
    }

    #[getter]
    fn cluster_metadata_refresh_interval(&self) -> Duration {
        self.config.cluster_metadata_refresh_interval
    }

    #[getter]
    fn keepalive_interval(&self) -> Option<Duration> {
        self.config.keepalive_interval
    }

    #[getter]
    fn keepalive_timeout(&self) -> Option<Duration> {
        self.config.keepalive_timeout
    }

    #[getter]
    fn hostname_resolution_timeout(&self) -> Option<Duration> {
        self.config.hostname_resolution_timeout
    }

    #[getter]
    fn enable_write_coalescing(&self) -> bool {
        self.config.enable_write_coalescing
    }

    #[getter]
    fn write_coalescing(&self) -> Option<PyWriteCoalescingDelay> {
        if self.config.enable_write_coalescing {
            Some(PyWriteCoalescingDelay {
                inner: self.config.write_coalescing_delay.clone(),
            })
        } else {
            None
        }
    }

    #[getter]
    fn identity(&self) -> PySelfIdentity {
        PySelfIdentity {
            inner: self.config.identity.clone(),
        }
    }
}

pub(crate) struct PyDuration(pub(crate) Duration);

impl<'py> FromPyObject<'_, 'py> for PyDuration {
    type Error = DriverSessionConfigError;
    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(duration) = obj.extract::<Duration>() {
            return Ok(PyDuration(duration));
        }

        if let Ok(secs) = obj.extract::<f64>() {
            let duration = Duration::try_from_secs_f64(secs)
                .map_err(|_| DriverSessionConfigError::invalid_duration(obj))?;
            return Ok(PyDuration(duration));
        }

        Err(DriverSessionConfigError::invalid_duration(obj))
    }
}

#[derive(Clone, Default)]
struct ContactPoints {
    inner: Vec<ParsedAddress>,
}

impl ContactPoints {
    fn add_known_nodes(&self, config: &mut SessionConfig) {
        for addr in &self.inner {
            match addr {
                ParsedAddress::Resolved(socket_addr) => config.add_known_node_addr(*socket_addr),
                ParsedAddress::Unresolved(host) => config.add_known_node(host),
            }
        }
    }
}

impl<'py> FromPyObject<'_, 'py> for ContactPoints {
    type Error = DriverSessionConfigError;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        let list = obj
            .extract::<ParsedAddressList>()
            .map_err(|e| DriverSessionConfigError::InvalidAddress { source: e })?;
        Ok(ContactPoints { inner: list.inner })
    }
}

#[pymodule]
pub(crate) fn session_builder(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<SessionBuilder>()?;
    module.add_class::<PySessionBuilderConfig>()?;
    Ok(())
}
