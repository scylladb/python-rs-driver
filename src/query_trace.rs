use chrono::{DateTime, TimeDelta, Utc};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyDateTime, PyDelta, PyDict, PyString, PyTuple};
use scylla::observability::tracing::{TracingEvent, TracingInfo};
use std::sync::Mutex;
use uuid::Uuid;

use crate::core::traces::PendingQueryTrace;
use crate::utils::{cached, cached_option};

/// The trace of one request: how the server ran it, step by step.
#[pyclass(module = "scylla.tracing", frozen)]
pub(crate) struct QueryTrace {
    trace_id: Uuid,
    /// Without its events, which moved into `events`.
    info: TracingInfo,
    /// Moved into `py_events` by the first call to the `events` getter.
    events: Mutex<Vec<TracingEvent>>,

    // Cached Python-side representations used by the getters.
    // `uuid.UUID` and the `ipaddress` classes have no pyo3 type, so they are `PyAny`.
    py_trace_id: PyOnceLock<Py<PyAny>>,
    py_request_type: PyOnceLock<Option<Py<PyString>>>,
    py_command: PyOnceLock<Option<Py<PyString>>>,
    py_duration: PyOnceLock<Option<Py<PyDelta>>>,
    py_coordinator: PyOnceLock<Option<Py<PyAny>>>,
    py_client: PyOnceLock<Option<Py<PyAny>>>,
    py_parameters: PyOnceLock<Option<Py<PyDict>>>,
    py_started_at: PyOnceLock<Option<Py<PyDateTime>>>,
    py_events: PyOnceLock<Py<PyTuple>>,
}

impl From<PendingQueryTrace> for QueryTrace {
    fn from(trace: PendingQueryTrace) -> Self {
        let mut info = trace.info;
        let events = std::mem::take(&mut info.events);

        Self {
            trace_id: trace.trace_id,
            info,
            events: Mutex::new(events),

            py_trace_id: PyOnceLock::new(),
            py_request_type: PyOnceLock::new(),
            py_command: PyOnceLock::new(),
            py_duration: PyOnceLock::new(),
            py_coordinator: PyOnceLock::new(),
            py_client: PyOnceLock::new(),
            py_parameters: PyOnceLock::new(),
            py_started_at: PyOnceLock::new(),
            py_events: PyOnceLock::new(),
        }
    }
}

#[pymethods]
impl QueryTrace {
    #[getter]
    fn trace_id(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        cached(py, &self.py_trace_id, || self.trace_id)
    }

    #[getter]
    fn request_type(&self, py: Python<'_>) -> PyResult<Option<Py<PyString>>> {
        cached_option(py, &self.py_request_type, || self.info.request.as_deref())
    }

    #[getter]
    fn command(&self, py: Python<'_>) -> PyResult<Option<Py<PyString>>> {
        cached_option(py, &self.py_command, || self.info.command.as_deref())
    }

    #[getter]
    fn duration(&self, py: Python<'_>) -> PyResult<Option<Py<PyDelta>>> {
        cached_option(py, &self.py_duration, || self.info.duration.map(micros))
    }

    #[getter]
    fn coordinator(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        cached_option(py, &self.py_coordinator, || self.info.coordinator)
    }

    #[getter]
    fn client(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        cached_option(py, &self.py_client, || self.info.client)
    }

    #[getter]
    fn parameters(&self, py: Python<'_>) -> PyResult<Option<Py<PyDict>>> {
        cached_option(py, &self.py_parameters, || self.info.parameters.as_ref())
    }

    #[getter]
    fn started_at(&self, py: Python<'_>) -> PyResult<Option<Py<PyDateTime>>> {
        cached_option(py, &self.py_started_at, || {
            self.info
                .started_at
                .and_then(|t| -> Option<DateTime<Utc>> { t.try_into().ok() })
        })
    }

    #[getter]
    fn events(&self, py: Python<'_>) -> PyResult<Py<PyTuple>> {
        let events = self.py_events.get_or_try_init(py, || {
            #[expect(
                clippy::disallowed_methods,
                reason = "the only holder moves the Rust events out, so it releases the lock without needing the GIL"
            )]
            let events = std::mem::take(&mut *self.events.lock().unwrap());
            PyTuple::new(py, events.into_iter().map(TraceEvent::from)).map(Bound::unbind)
        })?;
        Ok(events.clone_ref(py))
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!(
            "QueryTrace(trace_id={}, request_type={}, coordinator={}, started_at={}, duration={})",
            self.trace_id(py)?.into_pyobject(py)?.repr()?,
            self.request_type(py)?.into_pyobject(py)?.repr()?,
            self.coordinator(py)?.into_pyobject(py)?.repr()?,
            self.started_at(py)?.into_pyobject(py)?.repr()?,
            self.duration(py)?.into_pyobject(py)?.repr()?,
        ))
    }
}

/// One step of a query trace: what occurred, on which node, and when.
#[pyclass(module = "scylla.tracing", frozen)]
pub(crate) struct TraceEvent {
    inner: TracingEvent,

    // Cached Python-side representations used by the getters.
    // `uuid.UUID` and the `ipaddress` classes have no pyo3 type, so they are `PyAny`.
    py_event_id: PyOnceLock<Py<PyAny>>,
    py_description: PyOnceLock<Option<Py<PyString>>>,
    py_datetime: PyOnceLock<Option<Py<PyDateTime>>>,
    py_source: PyOnceLock<Option<Py<PyAny>>>,
    py_source_elapsed: PyOnceLock<Option<Py<PyDelta>>>,
    py_thread_name: PyOnceLock<Option<Py<PyString>>>,
}

impl From<TracingEvent> for TraceEvent {
    fn from(inner: TracingEvent) -> Self {
        Self {
            inner,

            py_event_id: PyOnceLock::new(),
            py_description: PyOnceLock::new(),
            py_datetime: PyOnceLock::new(),
            py_source: PyOnceLock::new(),
            py_source_elapsed: PyOnceLock::new(),
            py_thread_name: PyOnceLock::new(),
        }
    }
}

#[pymethods]
impl TraceEvent {
    #[getter]
    fn event_id(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        cached(py, &self.py_event_id, || Uuid::from(self.inner.event_id))
    }

    #[getter]
    fn description(&self, py: Python<'_>) -> PyResult<Option<Py<PyString>>> {
        cached_option(py, &self.py_description, || self.inner.activity.as_deref())
    }

    #[getter]
    fn datetime(&self, py: Python<'_>) -> PyResult<Option<Py<PyDateTime>>> {
        cached_option(py, &self.py_datetime, || {
            let (secs, nanos) = Uuid::from(self.inner.event_id).get_timestamp()?.to_unix();
            DateTime::<Utc>::from_timestamp(secs.try_into().ok()?, nanos)
        })
    }

    #[getter]
    fn source(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        cached_option(py, &self.py_source, || self.inner.source)
    }

    #[getter]
    fn source_elapsed(&self, py: Python<'_>) -> PyResult<Option<Py<PyDelta>>> {
        cached_option(py, &self.py_source_elapsed, || {
            self.inner.source_elapsed.map(micros)
        })
    }

    #[getter]
    fn thread_name(&self, py: Python<'_>) -> PyResult<Option<Py<PyString>>> {
        cached_option(py, &self.py_thread_name, || self.inner.thread.as_deref())
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!(
            "TraceEvent(description={}, source={}, thread_name={}, source_elapsed={})",
            self.description(py)?.into_pyobject(py)?.repr()?,
            self.source(py)?.into_pyobject(py)?.repr()?,
            self.thread_name(py)?.into_pyobject(py)?.repr()?,
            self.source_elapsed(py)?.into_pyobject(py)?.repr()?,
        ))
    }
}

fn micros(value: i32) -> TimeDelta {
    TimeDelta::microseconds(value.into())
}

impl<'py> IntoPyObject<'py> for PendingQueryTrace {
    type Target = QueryTrace;
    type Output = Bound<'py, QueryTrace>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Bound::new(py, QueryTrace::from(self))
    }
}

#[pymodule]
pub(crate) fn query_trace(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<QueryTrace>()?;
    module.add_class::<TraceEvent>()?;
    Ok(())
}
