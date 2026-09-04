use crate::errors::DriverSessionConfigError;
use crate::utils::PyDuration;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::PyString;
use scylla::client::{PoolSize, SelfIdentity, WriteCoalescingDelay};
use scylla::statement::{Consistency, SerialConsistency};
use scylla_cql::frame::Compression;
use std::num::{NonZeroU64, NonZeroUsize};

#[pyclass(name = "Consistency", eq, eq_int, frozen, from_py_object)]
#[derive(Clone, Copy, PartialEq, Debug)]
pub(crate) enum PyConsistency {
    Any,
    One,
    Two,
    Three,
    Quorum,
    All,
    LocalQuorum,
    EachQuorum,
    LocalOne,
    Serial,
    LocalSerial,
}

impl From<PyConsistency> for Consistency {
    fn from(value: PyConsistency) -> Self {
        match value {
            PyConsistency::Any => Self::Any,
            PyConsistency::One => Self::One,
            PyConsistency::Two => Self::Two,
            PyConsistency::Three => Self::Three,
            PyConsistency::Quorum => Self::Quorum,
            PyConsistency::All => Self::All,
            PyConsistency::LocalQuorum => Self::LocalQuorum,
            PyConsistency::EachQuorum => Self::EachQuorum,
            PyConsistency::LocalOne => Self::LocalOne,
            PyConsistency::Serial => Self::Serial,
            PyConsistency::LocalSerial => Self::LocalSerial,
        }
    }
}

impl From<Consistency> for PyConsistency {
    fn from(value: Consistency) -> Self {
        match value {
            Consistency::Any => Self::Any,
            Consistency::One => Self::One,
            Consistency::Two => Self::Two,
            Consistency::Three => Self::Three,
            Consistency::Quorum => Self::Quorum,
            Consistency::All => Self::All,
            Consistency::LocalQuorum => Self::LocalQuorum,
            Consistency::EachQuorum => Self::EachQuorum,
            Consistency::LocalOne => Self::LocalOne,
            Consistency::Serial => Self::Serial,
            Consistency::LocalSerial => Self::LocalSerial,
        }
    }
}

#[pyclass(name = "SerialConsistency", eq, eq_int, frozen, from_py_object)]
#[derive(Clone, Copy, PartialEq, Debug)]
pub(crate) enum PySerialConsistency {
    Serial,
    LocalSerial,
}

impl From<PySerialConsistency> for SerialConsistency {
    fn from(value: PySerialConsistency) -> Self {
        match value {
            PySerialConsistency::Serial => Self::Serial,
            PySerialConsistency::LocalSerial => Self::LocalSerial,
        }
    }
}

impl From<SerialConsistency> for PySerialConsistency {
    fn from(value: SerialConsistency) -> Self {
        match value {
            SerialConsistency::Serial => Self::Serial,
            SerialConsistency::LocalSerial => Self::LocalSerial,
        }
    }
}

#[pyclass(eq, eq_int, frozen, from_py_object, name = "Compression")]
#[derive(Clone, Copy, PartialEq, Debug)]
pub(crate) enum PyCompression {
    Lz4,
    Snappy,
}

impl From<PyCompression> for Compression {
    fn from(value: PyCompression) -> Self {
        match value {
            PyCompression::Lz4 => Self::Lz4,
            PyCompression::Snappy => Self::Snappy,
        }
    }
}

impl From<Compression> for PyCompression {
    fn from(value: Compression) -> Self {
        match value {
            Compression::Lz4 => Self::Lz4,
            Compression::Snappy => Self::Snappy,
        }
    }
}

#[pyclass(name = "PoolSize", from_py_object, frozen)]
#[derive(Clone, Copy, Debug)]
pub struct PyPoolSize {
    pub(crate) inner: PoolSize,
}

#[pymethods]
impl PyPoolSize {
    #[staticmethod]
    fn per_host(connections: NonZeroUsize) -> PyResult<Self> {
        Ok(Self {
            inner: PoolSize::PerHost(connections),
        })
    }

    #[staticmethod]
    fn per_shard(connections: NonZeroUsize) -> Self {
        Self {
            inner: PoolSize::PerShard(connections),
        }
    }

    #[getter]
    fn kind<'py>(&self, py: Python<'py>) -> &Bound<'py, PyString> {
        match self.inner {
            PoolSize::PerHost(_) => intern!(py, "per_host"),
            PoolSize::PerShard(_) => intern!(py, "per_shard"),
        }
    }

    #[getter]
    fn connections(&self) -> usize {
        match self.inner {
            PoolSize::PerHost(v) | PoolSize::PerShard(v) => v.get(),
        }
    }

    fn __repr__(&self, py: Python) -> PyResult<Py<PyString>> {
        let repr_str = PyString::from_fmt(
            py,
            format_args!(
                "PoolSize(kind={}, connections={})",
                self.kind(py),
                self.connections()
            ),
        )?;

        Ok(repr_str.into())
    }
}

#[pyclass(name = "WriteCoalescingDelay", from_py_object, frozen)]
#[derive(Clone, Debug)]
pub struct PyWriteCoalescingDelay {
    pub(crate) inner: WriteCoalescingDelay,
}

#[pymethods]
impl PyWriteCoalescingDelay {
    #[staticmethod]
    fn small_nondeterministic() -> Self {
        Self {
            inner: WriteCoalescingDelay::SmallNondeterministic,
        }
    }

    #[staticmethod]
    fn from_seconds(py_duration: PyDuration) -> Result<Self, DriverSessionConfigError> {
        let millis = py_duration.0.as_millis();

        let delay_ms = u64::try_from(millis)
            .ok()
            .and_then(NonZeroU64::new)
            .ok_or_else(|| DriverSessionConfigError::ZeroDurationNotAllowed)?;

        Ok(Self {
            inner: WriteCoalescingDelay::Milliseconds(delay_ms),
        })
    }

    #[getter]
    fn kind<'py>(&self, py: Python<'py>) -> &Bound<'py, PyString> {
        #[deny(clippy::wildcard_enum_match_arm)]
        match self.inner {
            WriteCoalescingDelay::SmallNondeterministic => intern!(py, "small_nondeterministic"),
            WriteCoalescingDelay::Milliseconds(_) => intern!(py, "milliseconds"),
            _ => unreachable!("clippy testifies that the match is exhaustive"),
        }
    }

    #[getter]
    fn milliseconds(&self) -> Option<u64> {
        match self.inner {
            WriteCoalescingDelay::Milliseconds(ms) => Some(ms.get()),
            _ => None,
        }
    }

    fn __repr__(&self, py: Python) -> PyResult<Py<PyString>> {
        #[deny(clippy::wildcard_enum_match_arm)]
        let repr_str = match self.inner {
            WriteCoalescingDelay::SmallNondeterministic => PyString::from_fmt(
                py,
                format_args!("WriteCoalescingDelay(kind={})", self.kind(py)),
            )?,
            WriteCoalescingDelay::Milliseconds(ms) => PyString::from_fmt(
                py,
                format_args!(
                    "WriteCoalescingDelay(kind={}, milliseconds={})",
                    self.kind(py),
                    ms.get()
                ),
            )?,
            _ => unreachable!("clippy testifies that the match is exhaustive"),
        };

        Ok(repr_str.into())
    }
}

#[pyclass(name = "SelfIdentity", from_py_object, frozen)]
#[derive(Clone, Debug, Default)]
pub struct PySelfIdentity {
    pub(crate) inner: SelfIdentity<'static>,
}

#[pymethods]
impl PySelfIdentity {
    #[new]
    #[pyo3(signature = (
        *,
        custom_driver_name = None,
        custom_driver_version = None,
        application_name = None,
        application_version = None,
        client_id = None,
    ))]
    fn new(
        custom_driver_name: Option<String>,
        custom_driver_version: Option<String>,
        application_name: Option<String>,
        application_version: Option<String>,
        client_id: Option<String>,
    ) -> Self {
        let mut inner = SelfIdentity::new();

        if let Some(v) = custom_driver_name {
            inner.set_custom_driver_name(v);
        } else {
            inner.set_custom_driver_name("ScyllaDB Python RS Driver");
        }

        if let Some(v) = custom_driver_version {
            inner.set_custom_driver_version(v);
        } else {
            inner.set_custom_driver_version(env!("CARGO_PKG_VERSION"));
        }

        if let Some(v) = application_name {
            inner.set_application_name(v);
        }
        if let Some(v) = application_version {
            inner.set_application_version(v);
        }
        if let Some(v) = client_id {
            inner.set_client_id(v);
        }

        Self { inner }
    }

    #[getter]
    fn custom_driver_name(&self) -> Option<&str> {
        self.inner.get_custom_driver_name()
    }

    #[getter]
    fn custom_driver_version(&self) -> Option<&str> {
        self.inner.get_custom_driver_version()
    }

    #[getter]
    fn application_name(&self) -> Option<&str> {
        self.inner.get_application_name()
    }

    #[getter]
    fn application_version(&self) -> Option<&str> {
        self.inner.get_application_version()
    }

    #[getter]
    fn client_id(&self) -> Option<&str> {
        self.inner.get_client_id()
    }

    fn __repr__(&self, py: Python) -> PyResult<Py<PyString>> {
        let repr_str = PyString::from_fmt(
            py,
            format_args!(
                "SelfIdentity(custom_driver_name={}, custom_driver_version={}, application_name={}, application_version={}, client_id={})",
                self.custom_driver_name().unwrap_or("None"),
                self.custom_driver_version().unwrap_or("None"),
                self.application_name().unwrap_or("None"),
                self.application_version().unwrap_or("None"),
                self.client_id().unwrap_or("None"),
            ),
        )?;

        Ok(repr_str.into())
    }
}

#[pymodule]
pub(crate) fn enums(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyConsistency>()?;
    module.add_class::<PySerialConsistency>()?;
    module.add_class::<PyCompression>()?;
    module.add_class::<PyPoolSize>()?;
    module.add_class::<PyWriteCoalescingDelay>()?;
    module.add_class::<PySelfIdentity>()?;
    Ok(())
}
