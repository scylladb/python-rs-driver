use crate::errors::HostFilterError;
use crate::errors::config::DriverSessionConfigError;
use crate::routing::PyToken;
use crate::utils::{ParsedAddressList, PyValueOrError};
use pyo3::IntoPyObject;
use pyo3::PyErr;
use pyo3::prelude::{PyAnyMethods, PyModule, PyModuleMethods};
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyString, PyTuple};
use pyo3::{
    Borrowed, Bound, FromPyObject, Py, PyAny, PyResult, Python, intern, pyclass, pymethods,
    pymodule,
};
use scylla::cluster::metadata::Peer;
use scylla::policies::host_filter::{
    AcceptAllHostFilter, AllowListHostFilter, DcHostFilter, HostFilter,
};
use std::sync::Arc;

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
#[pyclass(
    module = "scylla.policies.host_filter",
    name = "AcceptAllHostFilter",
    frozen
)]
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
#[pyclass(module = "scylla.policies.host_filter", name = "DcHostFilter", frozen)]
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
#[pyclass(
    module = "scylla.policies.host_filter",
    name = "AllowListHostFilter",
    frozen
)]
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
#[pyclass(module = "scylla.policies.host_filter", frozen, name = "Peer")]
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
pub(crate) fn host_filter(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyAcceptAllHostFilter>()?;
    module.add_class::<PyDcHostFilter>()?;
    module.add_class::<PyAllowListHostFilter>()?;
    module.add_class::<PyPeer>()?;
    Ok(())
}

#[derive(Debug, thiserror::Error)]
#[must_use]
pub(crate) enum DriverHostFilterError {
    #[error("Invalid address in host filter allow list: {source}")]
    InvalidAddress { source: std::io::Error },
}

impl DriverHostFilterError {
    pub(crate) fn invalid_address(source: std::io::Error) -> Self {
        Self::InvalidAddress { source }
    }
}

impl From<DriverHostFilterError> for PyErr {
    fn from(e: DriverHostFilterError) -> PyErr {
        HostFilterError::new_err(e.to_string())
    }
}
