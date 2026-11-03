use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};
use pyo3::{Py, PyAny, PyResult};

use super::PyDriverFuture;
use crate::utils::Prepended;

/// A legacy-driver callback, invoked as `fn(value, *args, **kwargs)`.
pub(super) struct LegacyCallback {
    callable: Py<PyAny>,
    args: Py<PyTuple>,
    kwargs: Option<Py<PyDict>>,
}

impl LegacyCallback {
    pub(super) fn new(callable: Py<PyAny>, args: Py<PyTuple>, kwargs: Option<Py<PyDict>>) -> Self {
        Self {
            callable,
            args,
            kwargs,
        }
    }

    /// Invoke this callback with `value` prepended to its arguments.
    /// Errors are logged and swallowed, as the legacy driver did.
    pub(super) fn invoke(&self, py: Python<'_>, value: &Bound<'_, PyAny>) {
        let extra = self.args.bind(py);
        let kwargs = self.kwargs.as_ref().map(|kwargs| kwargs.bind(py));

        let args = Prepended::new(value.as_borrowed(), extra.iter_borrowed());
        let called = match PyTuple::new(py, args) {
            Ok(args) => self.callable.call(py, args, kwargs),
            Err(err) => Err(err),
        };

        if let Err(err) = called {
            log::error!("ResponseFuture callback raised an exception: {}", err);
        }
    }
}

/// Which list a legacy callback goes on, and which outcome it fires for.
#[derive(Clone, Copy, PartialEq)]
pub(super) enum Delivery {
    Callbacks,
    Errbacks,
}

impl Delivery {
    /// The list `result` is delivered to.
    pub(super) fn of<T>(result: &PyResult<T>) -> Self {
        match result {
            Ok(_) => Self::Callbacks,
            Err(_) => Self::Errbacks,
        }
    }
}

/// The callbacks and errbacks registered on a `ResponseFuture`, or the ones
/// still to fire for one page. Shared through an `Arc`: a copy for a page is a
/// clone of it, and a change copies the lists only while such a clone exists.
#[derive(Clone, Default)]
pub(super) struct Callbacks {
    callbacks: Vec<Arc<LegacyCallback>>,
    errbacks: Vec<Arc<LegacyCallback>>,
}

impl Callbacks {
    fn list(&self, delivery: Delivery) -> &[Arc<LegacyCallback>] {
        match delivery {
            Delivery::Callbacks => &self.callbacks,
            Delivery::Errbacks => &self.errbacks,
        }
    }

    /// Whether nothing fires for `delivery`.
    pub(super) fn is_empty_for(&self, delivery: Delivery) -> bool {
        self.list(delivery).is_empty()
    }

    /// Adds `callback` to the list for `delivery`, copying the lists first if
    /// a clone shares them.
    pub(super) fn push(self: &mut Arc<Self>, delivery: Delivery, callback: Arc<LegacyCallback>) {
        let callbacks = Arc::make_mut(self);
        match delivery {
            Delivery::Callbacks => callbacks.callbacks.push(callback),
            Delivery::Errbacks => callbacks.errbacks.push(callback),
        }
    }

    /// Empties both lists. A clone that shares them keeps its own.
    pub(super) fn clear(self: &mut Arc<Self>) {
        match Arc::get_mut(self) {
            Some(callbacks) => {
                callbacks.callbacks.clear();
                callbacks.errbacks.clear();
            }
            None => *self = Arc::default(),
        }
    }

    /// The callbacks to fire for a page settled with `outcome`, `None` if none
    /// would fire. An outcome not known yet can still go either way, so then
    /// either list counts.
    pub(super) fn for_outcome(self: &Arc<Self>, outcome: Option<Delivery>) -> Option<Arc<Self>> {
        let can_fire = match outcome {
            Some(delivery) => !self.is_empty_for(delivery),
            None => {
                !self.is_empty_for(Delivery::Callbacks) || !self.is_empty_for(Delivery::Errbacks)
            }
        };
        can_fire.then(|| Arc::clone(self))
    }

    /// Invokes every callback for `delivery` with `value`.
    pub(super) fn fire(&self, py: Python<'_>, delivery: Delivery, value: &Bound<'_, PyAny>) {
        for callback in self.list(delivery) {
            callback.invoke(py, value);
        }
    }
}

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
        if let Err(err) = self.callable.call1(py, (value,)) {
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
