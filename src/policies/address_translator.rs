use crate::errors::{DriverAddressTranslationError, DriverSessionConfigError};
use crate::utils::ParsedAddress;
use async_trait::async_trait;
use pyo3::IntoPyObject;
use pyo3::PyErr;
use pyo3::prelude::{PyAnyMethods, PyDictMethods, PyModule, PyModuleMethods};
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyDict, PyString, PyTuple};
use pyo3::{
    Borrowed, Bound, BoundObject, FromPyObject, Py, PyAny, PyResult, Python, intern, pyclass,
    pymethods, pymodule,
};
use scylla::errors::{CustomTranslationError, TranslationError};
use scylla::policies::address_translator::{AddressTranslator, UntranslatedPeer};
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

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

    //TODO
    // Investigate how make AddressTranslator methods async for python users
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

#[pymodule]
pub(crate) fn address_translator(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyDictAddressTranslator>()?;
    module.add_class::<PyUntranslatedPeer>()?;
    Ok(())
}
