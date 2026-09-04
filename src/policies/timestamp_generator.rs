use crate::errors::DriverSessionConfigError;
use crate::utils::PyDuration;
use pyo3::prelude::{PyAnyMethods, PyModule, PyModuleMethods};
use pyo3::{
    Borrowed, Bound, BoundObject, FromPyObject, Py, PyAny, PyResult, Python, intern, pyclass,
    pymethods, pymodule,
};
use scylla::policies::timestamp_generator::{
    MonotonicTimestampGenerator, SimpleTimestampGenerator, TimestampGenerator,
};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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

#[pymodule]
pub(crate) fn timestamp_generator(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyMonotonicTimestampGenerator>()?;
    module.add_class::<PySimpleTimestampGenerator>()?;
    Ok(())
}
