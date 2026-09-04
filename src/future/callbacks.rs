use pyo3::prelude::*;
use pyo3::{Py, PyAny, PyResult};

use super::PyDriverFuture;

/// A registered callback, invoked with the future's outcome as its sole argument.
pub(super) struct Callback {
    callable: Py<PyAny>,
}

impl Callback {
    fn new(callable: Py<PyAny>) -> Self {
        Self { callable }
    }

    /// Invoke this callback with `value` as its only argument.
    /// Errors are logged and swallowed.
    fn invoke(&self, py: Python<'_>, value: &Py<PyAny>) {
        if let Err(err) = self.callable.call1(py, (value.clone_ref(py),)) {
            log::error!("DriverFuture callback raised an exception: {}", err);
        }
    }
}

/// Discriminates whether a [`Callback`] fires on success, on error, or either way.
pub(super) enum CallbackKind {
    /// Fired when the future resolves successfully. Passes the result value.
    Success(Callback),
    /// Fired when the future resolves with an error. Passes the exception instance.
    Error(Callback),
    /// Fired whichever way the future resolves. Passes the future itself, so the
    /// callback reads the outcome off `future.result()` and handles both cases in
    /// one `try`/`except`.
    Done {
        callback: Callback,
        future: Py<PyDriverFuture>,
    },
}

impl CallbackKind {
    pub(super) fn on_success(callable: Py<PyAny>) -> Self {
        Self::Success(Callback::new(callable))
    }

    pub(super) fn on_error(callable: Py<PyAny>) -> Self {
        Self::Error(Callback::new(callable))
    }

    pub(super) fn on_done(callable: Py<PyAny>, future: Py<PyDriverFuture>) -> Self {
        Self::Done {
            callback: Callback::new(callable),
            future,
        }
    }

    /// Invoke this callback if its variant matches the outcome of `result`.
    pub(super) fn invoke(&self, py: Python<'_>, result: &PyResult<Py<PyAny>>) {
        match (self, result) {
            (CallbackKind::Success(cb), Ok(value)) => {
                cb.invoke(py, value);
            }
            (CallbackKind::Error(cb), Err(err)) => {
                let exc_obj = err.value(py);
                cb.invoke(py, exc_obj.as_any().as_unbound());
            }
            (CallbackKind::Done { callback, future }, _) => {
                callback.invoke(py, future.as_any());
            }
            _ => {}
        }
    }

    /// Fire every callback in `callbacks` that matches the outcome of `result`.
    pub(super) fn fire_all(
        py: Python<'_>,
        callbacks: Vec<CallbackKind>,
        result: &PyResult<Py<PyAny>>,
    ) {
        for cb in &callbacks {
            cb.invoke(py, result);
        }
    }
}
