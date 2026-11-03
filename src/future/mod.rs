//! # PyDriverFuture — hybrid design
//!
//! ## Four states
//!
//! `PendingAsyncio { coroutine }`
//!     The future is driven by the asyncio event loop.
//!     This is the default starting state.
//!
//! `PendingTokio { callbacks, abort_handle, waker }`
//!     The future has been spawned on the tokio runtime. `__next__` just
//!     yields the asyncio future from the waker. The spawned task transitions
//!     to `Ready` on completion without touching Python: it stores the raw
//!     output and wakes the waker, which hands the parked asyncio future to the
//!     loop's completion batcher.
//!
//! `Ready { result }`
//!     Terminal state. Result stored permanently. A result produced on a tokio
//!     worker is kept unconverted until something on a GIL-holding thread asks
//!     for it (`__next__`, `result()`, a callback), so the worker never takes
//!     the GIL to finish a future.
//!
//! `Panicked`
//!     Terminal state. A panic unwound out of a state transition, taking the coroutine
//!     with it; every entry point reports `panicked_err()`.
//!
//! ## Transitions
//!
//! - `PendingAsyncio` → `PendingTokio`: when callbacks are registered, `result()` is
//!   called, or `start()` is called explicitly. The inner future is taken from the
//!   coroutine, spawned on tokio.
//! - `PendingAsyncio` → `Ready`: when `poll` completes, or `close()`/`cancel()` is called.
//! - `PendingTokio` → `Ready`: when the spawned task completes, or `close()`/`cancel()` aborts it.
//! - any state → `Panicked`: when a panic unwinds out of a transition (see below).
//! - `Ready` / `Panicked` → (no transitions)

use crate::RUNTIME;
use crate::errors::FutureCancelledError;
use crate::future::asyncio::waker::AsyncioWaker;
use crate::future::asyncio::{Coroutine, PollResult};
pub(crate) use crate::future::boxed_future::boxed_py_future;
use crate::future::boxed_future::{PyBoxedFuture, ResolvedResult};
use crate::future::callbacks::CallbackKind;
pub(crate) use crate::future::driver_future::DriverFuture;
use crate::future::panics::{catch_panics, resolve_catch_panics};
use crate::future::state_lock::{StateGuard, StateLock};
use crate::future::task::{TaskDropped, spawn_guarded};
use crate::utils::PyDuration;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyStopIteration;
use pyo3::exceptions::PyTimeoutError;
use pyo3::prelude::*;
use pyo3::types::{PyGenericAlias, PyType};
use pyo3::{Py, PyAny, PyResult};
use std::sync::Arc;
use std::time::Duration;

use tokio::task::AbortHandle;

mod asyncio;
mod boxed_future;
mod callbacks;
mod driver_future;
mod panics;
mod response_future;
mod state_lock;
mod task;

pub(crate) use crate::future::panics::catch_unwind;
pub(crate) use crate::future::response_future::LegacyRequestFuture;
pub(crate) use crate::future::response_future::PyResponseFuture;

/// Internal state of a PyDriverFuture.
enum FutureState {
    /// Future is driven by the asyncio executor.
    PendingAsyncio { coroutine: Coroutine },
    /// Future has been spawned on the tokio runtime.
    PendingTokio {
        callbacks: Vec<CallbackKind>,
        abort_handle: AbortHandle,
        waker: Arc<AsyncioWaker>,
    },
    /// Future has completed. Result is stored permanently.
    Ready { result: ReadyResult },
    /// A transition that consumes the previous state is in progress, or panicked
    /// halfway through one.
    Panicked,
}

impl FutureState {
    /// Whether the future can still make progress. Both terminal states —
    /// [`FutureState::Ready`] and [`FutureState::Panicked`]
    fn is_terminal(&self) -> bool {
        matches!(self, FutureState::Ready { .. } | FutureState::Panicked)
    }
}

/// The outcome of a finished future, converted to Python on first access.
enum ReadyResult {
    /// Produced on a tokio worker; the Python conversion is still pending.
    Unconverted(ResolvedResult),
    /// Converted, or produced as a Python value in the first place.
    Converted(PyResult<Py<PyAny>>),
}

impl ReadyResult {
    /// The result as a Python value, converting and caching it on first call.
    fn get_or_convert(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self {
            ReadyResult::Unconverted(resolved) => {
                let placeholder = ResolvedResult::Err(panicked_err());
                let owned_resolved = std::mem::replace(resolved, placeholder);
                let py_result = resolve_catch_panics(owned_resolved, py);
                *self = ReadyResult::Converted(clone_result(py, &py_result));
                py_result
            }
            ReadyResult::Converted(result) => clone_result(py, result),
        }
    }
}

/// The error every entry point reports for a future left [`FutureState::Panicked`].
fn panicked_err() -> PyErr {
    PyRuntimeError::new_err(
        "internal driver error: a panic left this future unusable; \
         this is a bug in the scylla driver, please report it",
    )
}

/// A Python awaitable wrapping a Rust future.
#[pyclass(module = "scylla.future", name = "DriverFuture", frozen)]
pub struct PyDriverFuture {
    // We need Arc here to be able to transition from asyncio to tokio.
    /// Waiters are notified when the state transitions to a terminal state.
    state: Arc<StateLock<FutureState>>,
}

impl PyDriverFuture {
    /// Create a PyDriverFuture starting in PendingAsyncio (default).
    fn new(future: PyBoxedFuture) -> Self {
        Self {
            state: Arc::new(StateLock::new(FutureState::PendingAsyncio {
                coroutine: Coroutine::new(future),
            })),
        }
    }

    /// Create a `Py<PyDriverFuture>` from an already-boxed future.
    /// Starts in PendingAsyncio.
    pub(in crate::future) fn spawn(
        py: Python<'_>,
        future: PyBoxedFuture,
    ) -> PyResult<Py<PyDriverFuture>> {
        Py::new(py, PyDriverFuture::new(future))
    }

    /// Create a `Py<PyDriverFuture>` from an already-boxed future, spawning it on
    /// the tokio runtime immediately: the future starts in `PendingTokio`.
    pub(in crate::future) fn spawn_on_tokio(
        py: Python<'_>,
        future: PyBoxedFuture,
    ) -> PyResult<Py<PyDriverFuture>> {
        let waker = Arc::new(AsyncioWaker::new());
        let shared = Arc::new(StateLock::new(FutureState::Panicked));

        {
            let mut state = shared.lock_py_attached(py);
            let abort_handle = Self::spawn_future_on_tokio(future, &shared, &waker);
            *state = FutureState::PendingTokio {
                callbacks: Vec::new(),
                abort_handle,
                waker,
            };
        }

        Py::new(py, PyDriverFuture { state: shared })
    }

    /// Create an already-resolved PyDriverFuture.
    pub(in crate::future) fn ready(
        py: Python,
        result: PyResult<Py<PyAny>>,
    ) -> PyResult<Py<PyDriverFuture>> {
        Py::new(
            py,
            PyDriverFuture {
                state: Arc::new(StateLock::new(FutureState::Ready {
                    result: ReadyResult::Converted(result),
                })),
            },
        )
    }

    /// Spawn a future on tokio, returning the abort handle.
    /// On completion the spawned task transitions `state` to `Ready`,
    /// fires any registered callbacks, wakes the asyncio waker, and notifies
    /// the condvar.
    fn spawn_future_on_tokio(
        future: PyBoxedFuture,
        shared: &Arc<StateLock<FutureState>>,
        waker: &Arc<AsyncioWaker>,
    ) -> AbortHandle {
        let (shared, waker) = (Arc::clone(shared), Arc::clone(waker));

        spawn_guarded(catch_panics(future), move |outcome| {
            Self::resolve_on_tokio(outcome, shared, waker)
        })
    }

    /// The spawned task completed or was dropped: `PendingTokio` → `Ready`, then
    /// fire callbacks and notify.
    ///
    /// A task is dropped before completing likely because the runtime shut down.
    /// Without the `Err` result the future would stay `PendingTokio` forever and
    /// its waiters would hit the `atexit` timeout.
    fn resolve_on_tokio(
        outcome: Result<ResolvedResult, TaskDropped>,
        shared: Arc<StateLock<FutureState>>,
        waker: Arc<AsyncioWaker>,
    ) {
        let dropped = outcome.is_err();
        let result = match outcome {
            Ok(resolved) => ReadyResult::Unconverted(resolved),
            Err(TaskDropped) => ReadyResult::Converted(Err(dropped_err())),
        };

        let (callbacks, waiters) = {
            // A runtime thread, or shutdown inside py.detach: not attached.
            let mut state = shared.lock_detached();
            match &mut *state {
                FutureState::PendingTokio { callbacks, .. } => {
                    let taken = std::mem::take(callbacks);
                    *state = FutureState::Ready { result };
                    (taken, state.waiters())
                }
                // The future was already closed/cancelled/thrown-into. There is nothing left to notify.
                _ => return,
            }
        };

        // A runtime that shuts down runs no new blocking tasks.
        if !dropped && !callbacks.is_empty() {
            RUNTIME.spawn_blocking(move || {
                Self::fire_and_notify(callbacks, &shared, &waker, waiters);
            });
        } else {
            Self::fire_and_notify(callbacks, &shared, &waker, waiters);
        }
    }

    /// Fire `callbacks` with the `Ready` result, then wake the asyncio waker and
    /// the blocked waiters.
    fn fire_and_notify(
        callbacks: Vec<CallbackKind>,
        shared: &StateLock<FutureState>,
        waker: &AsyncioWaker,
        waiters: usize,
    ) {
        if !callbacks.is_empty() {
            Python::attach(|py| {
                let result = {
                    let mut state = shared.lock_py_attached(py);
                    match &mut *state {
                        FutureState::Ready { result } => result.get_or_convert(py),
                        _ => unreachable!(
                            "This is unreachable if no panic happened during the transitions"
                        ),
                    }
                };
                CallbackKind::fire_all(py, callbacks, &result);
            });
        }

        waker.wake_detached();
        shared.notify(waiters);
    }

    /// Transition from PendingAsyncio to PendingTokio by spawning the given
    /// future on the tokio runtime.
    /// Must be called while holding the state lock.
    fn transition_to_tokio(
        future: PyBoxedFuture,
        waker: Arc<AsyncioWaker>,
        shared: &Arc<StateLock<FutureState>>,
        state_guard: &mut StateGuard<'_, FutureState>,
    ) {
        let abort_handle = Self::spawn_future_on_tokio(future, shared, &waker);

        **state_guard = FutureState::PendingTokio {
            callbacks: Vec::new(),
            abort_handle,
            waker,
        };
    }

    /// If `state_guard` is `PendingAsyncio`, take its future/waker and
    /// transition to `PendingTokio`. No-op otherwise.
    /// Must be called while holding the state lock.
    fn ensure_started(
        shared: &Arc<StateLock<FutureState>>,
        state_guard: &mut StateGuard<'_, FutureState>,
    ) {
        let coroutine = match std::mem::replace(&mut **state_guard, FutureState::Panicked) {
            FutureState::PendingAsyncio { coroutine } => coroutine,
            // Already started, or finished — put the state back untouched.
            other => {
                **state_guard = other;
                return;
            }
        };

        let (future, waker) = coroutine.into_future_and_waker();
        Self::transition_to_tokio(future, waker, shared, state_guard);
    }

    /// Poll the coroutine (__next__).
    fn poll_coroutine(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let mut state = self.state.lock_py_attached(py);
        match std::mem::replace(&mut *state, FutureState::Panicked) {
            FutureState::Ready { mut result } => {
                let err = raise_stop_iteration(py, &result.get_or_convert(py));
                *state = FutureState::Ready { result };
                Err(err)
            }

            // Future is running on tokio — just yield the asyncio future.
            FutureState::PendingTokio {
                callbacks,
                abort_handle,
                waker,
            } => {
                let asyncio_waker = Arc::clone(&waker);
                *state = FutureState::PendingTokio {
                    callbacks,
                    abort_handle,
                    waker,
                };
                drop(state);
                asyncio_waker.yield_asyncio_future(py)
            }

            // Drive the future via the coroutine.
            FutureState::PendingAsyncio { coroutine } => match coroutine.poll(py, None) {
                PollResult::Pending { coroutine, value } => {
                    *state = FutureState::PendingAsyncio { coroutine };
                    value
                }
                PollResult::Ready(result) => {
                    *state = FutureState::Ready {
                        result: ReadyResult::Converted(clone_result(py, &result)),
                    };
                    drop(state);
                    Err(raise_stop_iteration(py, &result))
                }
            },

            // Report the panic to the awaiter.
            FutureState::Panicked => Err(panicked_err()),
        }
    }

    /// Close the future. Transitions to Ready with `exc` as the error.
    fn close_future(&self, py: Python<'_>, exc: PyErr) {
        let err_result: PyResult<Py<PyAny>> = Err(exc);

        let (callbacks, waker, waiters) = {
            let mut state = self.state.lock_py_attached(py);

            let closed = FutureState::Ready {
                result: ReadyResult::Converted(clone_result(py, &err_result)),
            };
            match std::mem::replace(&mut *state, closed) {
                terminal @ (FutureState::Panicked | FutureState::Ready { .. }) => {
                    *state = terminal;
                    return;
                }

                FutureState::PendingTokio {
                    callbacks,
                    abort_handle,
                    waker,
                } => {
                    abort_handle.abort();
                    (Some(callbacks), Some(waker), state.waiters())
                }

                FutureState::PendingAsyncio { coroutine } => (None, coroutine.into_waker(), 0),
            }
        };

        self.state.notify(waiters);

        if let Some(waker) = waker {
            waker.wake_py_attached(py);
        }

        if let Some(callbacks) = callbacks {
            CallbackKind::fire_all(py, callbacks, &err_result);
        }
    }

    /// Release the GIL, wait on the condvar until state is Ready or `timeout`
    /// elapses, then return the result. Raises `TimeoutError` on timeout.
    fn wait_for_ready(&self, py: Python<'_>, timeout: Option<Duration>) -> PyResult<Py<PyAny>> {
        let timed_out = self.state.wait_while(py, timeout, |state| match state {
            FutureState::PendingTokio { .. } => true,
            FutureState::Panicked | FutureState::Ready { .. } => false,
            FutureState::PendingAsyncio { .. } => {
                unreachable!("We cannot wait for ready if future is not spawned on tokio")
            }
        });

        if timed_out {
            return Err(PyTimeoutError::new_err("DriverFuture.result() timed out"));
        }

        let mut state = self.state.lock_py_attached(py);
        match &mut *state {
            FutureState::Ready { result } => result.get_or_convert(py),
            // The condvar only releases on a terminal state, and the only other
            // terminal state is `Panicked`.
            _ => Err(panicked_err()),
        }
    }

    /// Block until the future is ready, returning the result.
    /// If `timeout` elapses first, raises `TimeoutError`.
    fn block_until_ready(&self, py: Python<'_>, timeout: Option<Duration>) -> PyResult<Py<PyAny>> {
        let mut state = self.state.lock_py_attached(py);
        match &mut *state {
            FutureState::Ready { result } => result.get_or_convert(py),

            FutureState::PendingTokio { .. } => {
                drop(state);
                self.wait_for_ready(py, timeout)
            }

            FutureState::PendingAsyncio { .. } => {
                Self::ensure_started(&self.state, &mut state);
                drop(state);
                self.wait_for_ready(py, timeout)
            }

            FutureState::Panicked => Err(panicked_err()),
        }
    }

    /// Register a [`CallbackKind`] on this future.
    ///
    /// - If already `Ready`, invokes the callback immediately.
    /// - If `PendingTokio`, queues it.
    /// - If `PendingAsyncio`, transitions to `PendingTokio` first, then queues it.
    fn register_callback(&self, py: Python<'_>, cb: CallbackKind) {
        let mut state = self.state.lock_py_attached(py);
        match &mut *state {
            FutureState::Ready { result } => {
                let result = result.get_or_convert(py);
                drop(state);
                cb.invoke(py, &result);
            }

            FutureState::PendingTokio { callbacks, .. } => {
                callbacks.push(cb);
            }

            FutureState::PendingAsyncio { .. } => {
                Self::ensure_started(&self.state, &mut state);
                let FutureState::PendingTokio { callbacks, .. } = &mut *state else {
                    unreachable!("ensure_started transitions PendingAsyncio to PendingTokio")
                };
                callbacks.push(cb);
            }

            // The future will never complete, so a queued callback would never fire:
            // report the panic to the callback right away instead.
            FutureState::Panicked => {
                drop(state);
                cb.invoke(py, &Err(panicked_err()));
            }
        }
    }

    /// Throw an exception into the future.
    /// - Ready: re-raises the exception (coroutine is exhausted).
    /// - PendingAsyncio: delegates to `coroutine.poll(py, Some(exc))`.
    /// - PendingTokio: aborts the tokio task, fires the error callbacks,
    ///   transitions to Ready, and re-raises the exception.
    fn throw_into(&self, py: Python<'_>, exc: Py<PyAny>) -> PyResult<Py<PyAny>> {
        let mut state = self.state.lock_py_attached(py);
        match std::mem::replace(&mut *state, FutureState::Panicked) {
            terminal @ (FutureState::Panicked | FutureState::Ready { .. }) => {
                *state = terminal;
                Err(PyErr::from_value(exc.into_bound(py)))
            }

            FutureState::PendingAsyncio { coroutine } => match coroutine.poll(py, Some(exc)) {
                PollResult::Pending { coroutine, value } => {
                    *state = FutureState::PendingAsyncio { coroutine };
                    value
                }
                PollResult::Ready(result) => {
                    *state = FutureState::Ready {
                        result: ReadyResult::Converted(clone_result(py, &result)),
                    };
                    drop(state);
                    Err(raise_stop_iteration(py, &result))
                }
            },

            FutureState::PendingTokio {
                callbacks,
                abort_handle,
                waker,
            } => {
                abort_handle.abort();

                let err_result: PyResult<Py<PyAny>> = Err(PyErr::from_value(exc.into_bound(py)));
                *state = FutureState::Ready {
                    result: ReadyResult::Converted(clone_result(py, &err_result)),
                };
                let waiters = state.waiters();
                drop(state);

                waker.wake_py_attached(py);
                self.state.notify(waiters);
                CallbackKind::fire_all(py, callbacks, &err_result);

                // Re-raise the thrown exception.
                err_result
            }
        }
    }
}

fn dropped_err() -> PyErr {
    FutureCancelledError::new_err(
        "future was dropped before completing, likely because the driver runtime shut down",
    )
}

fn clone_result(py: Python<'_>, result: &PyResult<Py<PyAny>>) -> PyResult<Py<PyAny>> {
    match result {
        Ok(value) => Ok(value.clone_ref(py)),
        Err(err) => Err(err.clone_ref(py)),
    }
}

fn raise_stop_iteration(py: Python<'_>, result: &PyResult<Py<PyAny>>) -> PyErr {
    match result {
        Ok(value) => PyStopIteration::new_err((value.clone_ref(py),)),
        Err(err) => err.clone_ref(py),
    }
}

#[pymethods]
impl PyDriverFuture {
    fn __await__(self_: Py<Self>) -> Py<Self> {
        self_
    }

    fn __iter__(self_: Py<Self>) -> Py<Self> {
        self_
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.poll_coroutine(py)
    }

    fn send(&self, py: Python<'_>, _value: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.__next__(py)
    }

    fn throw(&self, py: Python<'_>, exc: Py<PyAny>) -> PyResult<Py<PyAny>> {
        self.throw_into(py, exc)
    }

    fn close(&self, py: Python<'_>) {
        self.close_future(py, PyRuntimeError::new_err("future was closed"));
    }

    /// Force the transition from `PendingAsyncio` to `PendingTokio`.
    ///
    /// Spawns the inner future onto the tokio runtime immediately, without
    /// waiting for a callback registration or a `result()` call. No-op if
    /// the future is already `PendingTokio` or `Ready`. Returns `self` so
    /// calls can be chained, e.g. `future = session.execute(...).start()`.
    fn start(self_: Py<Self>, py: Python<'_>) -> Py<Self> {
        {
            let this = self_.borrow(py);
            let mut state = this.state.lock_py_attached(py);
            Self::ensure_started(&this.state, &mut state);
        }
        self_
    }

    /// Register a callback to be invoked when the future completes successfully.
    ///
    /// The callback is called as `callback(result)`.
    /// If the future is already done with a success, the callback is invoked immediately.
    /// If the future is pending on asyncio, it is moved to tokio to support callbacks.
    fn on_success(&self, py: Python<'_>, callback: Py<PyAny>) {
        let cb = CallbackKind::on_success(callback);
        self.register_callback(py, cb);
    }

    /// Register a callback to be invoked when the future completes with an error.
    ///
    /// The callback is called as `callback(exception)`.
    /// If the future is already done with an error, the callback is invoked immediately.
    /// If the future is pending on asyncio, it is moved to tokio to support callbacks.
    fn on_error(&self, py: Python<'_>, callback: Py<PyAny>) {
        let cb = CallbackKind::on_error(callback);
        self.register_callback(py, cb);
    }

    /// Register a callback to be invoked when the future completes, whichever way
    /// it goes.
    ///
    /// The callback is called as `callback(future)` with the very future it was
    /// registered on.
    ///
    /// If the future is already done, the callback is invoked immediately.
    /// If the future is pending on asyncio, it is moved to tokio to support callbacks.
    fn on_done(self_: Py<Self>, py: Python<'_>, callback: Py<PyAny>) {
        let cb = CallbackKind::on_done(callback, self_.clone_ref(py));
        self_.borrow(py).register_callback(py, cb);
    }

    /// Get the result of this future.
    ///
    /// If the future is still pending, this blocks the calling thread until
    /// it completes (releasing the GIL while waiting). If `timeout` is
    /// given and elapses before the future completes, raises `TimeoutError`.
    #[pyo3(signature = (timeout=None))]
    fn result(&self, py: Python<'_>, timeout: Option<PyDuration>) -> PyResult<Py<PyAny>> {
        self.block_until_ready(py, timeout.map(|d| d.0))
    }

    /// Cancel the future. Unlike `close()`, this raises `FutureCancelledError`
    /// from `result()`/`__next__()`/callbacks, distinguishing a deliberate
    /// cancellation from the future being torn down.
    fn cancel(&self, py: Python<'_>) {
        self.close_future(py, FutureCancelledError::new_err("future was cancelled"));
    }

    /// Returns True if the future completed because `cancel()` was called.
    fn cancelled(&self, py: Python<'_>) -> bool {
        let mut state = self.state.lock_py_attached(py);
        match &mut *state {
            FutureState::Ready { result } => match result.get_or_convert(py) {
                Err(err) => err.is_instance_of::<FutureCancelledError>(py),
                Ok(_) => false,
            },
            _ => false,
        }
    }

    /// Returns True if the future has completed (successfully or with an error).
    fn done(&self, py: Python<'_>) -> bool {
        let state = self.state.lock_py_attached(py);
        state.is_terminal()
    }

    fn __repr__(&self, py: Python<'_>) -> String {
        let mut state = self.state.lock_py_attached(py);
        match &mut *state {
            FutureState::PendingAsyncio { .. } | FutureState::PendingTokio { .. } => {
                "<DriverFuture pending>".to_string()
            }
            FutureState::Ready { result } => match result.get_or_convert(py) {
                Ok(_) => "<DriverFuture finished>".to_string(),
                Err(e) => format!("<DriverFuture finished exception={}>", e),
            },
            FutureState::Panicked => {
                format!("<DriverFuture finished exception={}>", panicked_err())
            }
        }
    }

    /// Make `DriverFuture[T]` legal at runtime, not just in a stub.
    #[classmethod]
    fn __class_getitem__<'py>(
        cls: &Bound<'py, PyType>,
        item: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyGenericAlias>> {
        PyGenericAlias::new(cls.py(), cls.as_any(), item)
    }
}

#[pymodule]
pub(crate) fn future(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyDriverFuture>()?;
    Ok(())
}
