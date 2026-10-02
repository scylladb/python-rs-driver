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
pub(crate) use crate::future::boxed_future::{BoxedFuture, boxed_py_future};
use crate::future::boxed_future::{PyBoxedFuture, ResolvedResult};
use crate::future::callbacks::CallbackKind;
pub(crate) use crate::future::driver_future::DriverFuture;
use crate::future::panics::{catch_panics, resolve_catch_panics};
use crate::future::task::spawn_guarded;
use crate::utils::PyDuration;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyStopIteration;
use pyo3::exceptions::PyTimeoutError;
use pyo3::prelude::*;
use pyo3::sync::MutexExt;
use pyo3::types::{PyGenericAlias, PyType};
use pyo3::{Py, PyAny, PyResult};
use std::sync::{Arc, Condvar, Mutex};
use std::task::Wake;
use std::time::Duration;

use tokio::task::AbortHandle;

mod asyncio;
mod boxed_future;
mod callbacks;
mod driver_future;
mod panics;
mod response_future;
mod task;

pub(crate) use crate::future::panics::{catch_panics_typed, catch_unwind};
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
        waiters: usize,
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
pub(in crate::future) fn panicked_err() -> PyErr {
    PyRuntimeError::new_err(
        "internal driver error: a panic left this future unusable; \
         this is a bug in the scylla driver, please report it",
    )
}

struct FutureInner {
    state: Mutex<FutureState>,
    /// Notified when the state transitions to a terminal state.
    ready: Condvar,
}

impl FutureInner {
    /// Wake the threads that were blocked in `wait_for_ready`.
    fn notify_waiters(&self, waiters: usize) {
        if waiters > 0 {
            self.ready.notify_all();
        }
    }
}

/// A Python awaitable wrapping a Rust future.
#[pyclass(name = "DriverFuture", frozen)]
pub struct PyDriverFuture {
    // We need Arc here to be able to transition from asyncio to tokio
    inner: Arc<FutureInner>,
}

impl PyDriverFuture {
    /// Create a PyDriverFuture starting in PendingAsyncio (default).
    fn new(future: PyBoxedFuture) -> Self {
        Self {
            inner: Arc::new(FutureInner {
                state: Mutex::new(FutureState::PendingAsyncio {
                    coroutine: Coroutine::new(future),
                }),
                ready: Condvar::new(),
            }),
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
        let inner = Arc::new(FutureInner {
            state: Mutex::new(FutureState::Panicked),
            ready: Condvar::new(),
        });

        {
            let mut state = inner.state.lock_py_attached(py).unwrap();
            let abort_handle = Self::spawn_future_on_tokio(future, &inner, &waker);
            *state = FutureState::PendingTokio {
                callbacks: Vec::new(),
                abort_handle,
                waker,
                waiters: 0,
            };
        }

        Py::new(py, PyDriverFuture { inner })
    }

    /// Create an already-resolved PyDriverFuture.
    pub(in crate::future) fn ready(
        py: Python,
        result: PyResult<Py<PyAny>>,
    ) -> PyResult<Py<PyDriverFuture>> {
        Py::new(
            py,
            PyDriverFuture {
                inner: Arc::new(FutureInner {
                    state: Mutex::new(FutureState::Ready {
                        result: ReadyResult::Converted(result),
                    }),
                    ready: Condvar::new(),
                }),
            },
        )
    }

    /// Spawn a future on tokio, returning the abort handle.
    /// On completion the spawned task transitions `state` to `Ready`,
    /// fires any registered callbacks, wakes the asyncio waker, and notifies
    /// the condvar.
    fn spawn_future_on_tokio(
        future: PyBoxedFuture,
        inner: &Arc<FutureInner>,
        waker: &Arc<AsyncioWaker>,
    ) -> AbortHandle {
        let (inner_done, waker_done) = (Arc::clone(inner), Arc::clone(waker));
        let (inner_dropped, waker_dropped) = (Arc::clone(inner), Arc::clone(waker));

        spawn_guarded(
            catch_panics(future),
            move |resolved| Self::resolve_on_tokio(resolved, inner_done, waker_done),
            move || Self::resolve_dropped(&inner_dropped, &waker_dropped),
        )
    }

    /// The spawned task completed: `PendingTokio` → `Ready`, then notify and fire callbacks.
    fn resolve_on_tokio(
        resolved: ResolvedResult,
        inner: Arc<FutureInner>,
        waker: Arc<AsyncioWaker>,
    ) {
        let finished = {
            let mut state = inner.state.lock().unwrap();
            match &mut *state {
                FutureState::PendingTokio {
                    callbacks, waiters, ..
                } => {
                    let taken = std::mem::take(callbacks);
                    let waiters = *waiters;
                    *state = FutureState::Ready {
                        result: ReadyResult::Unconverted(resolved),
                    };
                    Some((taken, waiters))
                }
                _ => None,
            }
        };

        // `None` means the future was already closed/cancelled/thrown-into
        // by the time this task completed. There is nothing left to notify.
        let Some((callbacks, waiters)) = finished else {
            return;
        };

        if callbacks.is_empty() {
            waker.wake();
            inner.notify_waiters(waiters);
            return;
        }

        RUNTIME.spawn_blocking(move || {
            Python::attach(|py| {
                let result = {
                    let mut state = inner.state.lock_py_attached(py).unwrap();
                    match &mut *state {
                        FutureState::Ready { result } => result.get_or_convert(py),
                        _ => unreachable!(
                            "This is unreachable if no panic happened during the transitions"
                        ),
                    }
                };
                CallbackKind::fire_all(py, callbacks, &result);

                waker.wake();
                inner.notify_waiters(waiters);
            });
        });
    }

    /// The spawned task was dropped before completing, likely because the
    /// runtime shut down. Without this the future would stay `PendingTokio`
    /// forever and its waiters would hit the `atexit` timeout.
    fn resolve_dropped(inner: &Arc<FutureInner>, waker: &Arc<AsyncioWaker>) {
        let (callbacks, waiters) = {
            let mut state = inner.state.lock().unwrap();
            match &mut *state {
                FutureState::PendingTokio {
                    callbacks, waiters, ..
                } => {
                    let taken = std::mem::take(callbacks);
                    let waiters = *waiters;
                    *state = FutureState::Ready {
                        result: ReadyResult::Converted(Err(dropped_err())),
                    };
                    (taken, waiters)
                }
                _ => return,
            }
        };

        if !callbacks.is_empty() {
            Python::attach(|py| {
                CallbackKind::fire_all(py, callbacks, &Err(dropped_err()));
            });
        }

        waker.wake_by_ref();
        inner.notify_waiters(waiters);
    }

    /// Transition from PendingAsyncio to PendingTokio by spawning the given
    /// future on the tokio runtime.
    /// Must be called while holding the state lock.
    fn transition_to_tokio(
        future: PyBoxedFuture,
        waker: Arc<AsyncioWaker>,
        inner: &Arc<FutureInner>,
        state_guard: &mut std::sync::MutexGuard<'_, FutureState>,
    ) {
        let abort_handle = Self::spawn_future_on_tokio(future, inner, &waker);

        **state_guard = FutureState::PendingTokio {
            callbacks: Vec::new(),
            abort_handle,
            waker,
            waiters: 0,
        };
    }

    /// If `state_guard` is `PendingAsyncio`, take its future/waker and
    /// transition to `PendingTokio`. No-op otherwise.
    /// Must be called while holding the state lock.
    fn ensure_started(
        inner: &Arc<FutureInner>,
        state_guard: &mut std::sync::MutexGuard<'_, FutureState>,
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
        Self::transition_to_tokio(future, waker, inner, state_guard);
    }

    /// Poll the coroutine (__next__).
    fn poll_coroutine(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let mut state = self.inner.state.lock_py_attached(py).unwrap();
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
                waiters,
            } => {
                let asyncio_waker = Arc::clone(&waker);
                *state = FutureState::PendingTokio {
                    callbacks,
                    abort_handle,
                    waker,
                    waiters,
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
            let mut state = self.inner.state.lock_py_attached(py).unwrap();

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
                    waiters,
                } => {
                    abort_handle.abort();
                    (Some(callbacks), Some(waker), waiters)
                }

                FutureState::PendingAsyncio { coroutine } => (None, coroutine.into_waker(), 0),
            }
        };

        self.inner.notify_waiters(waiters);

        if let Some(waker) = waker {
            waker.wake();
        }

        if let Some(callbacks) = callbacks {
            CallbackKind::fire_all(py, callbacks, &err_result);
        }
    }

    /// Release the GIL, wait on the condvar until state is Ready or `timeout`
    /// elapses, then return the result. Raises `TimeoutError` on timeout.
    fn wait_for_ready(&self, py: Python<'_>, timeout: Option<Duration>) -> PyResult<Py<PyAny>> {
        let timed_out = py.detach(|| {
            let mut state = self.inner.state.lock().unwrap();

            match &mut *state {
                FutureState::PendingTokio { waiters, .. } => *waiters += 1,
                FutureState::Panicked | FutureState::Ready { .. } => return false,
                FutureState::PendingAsyncio { .. } => {
                    unreachable!("We cannot wait for ready if future is not spawned on tokio")
                }
            }

            let (mut guard, timed_out) = match timeout {
                None => (
                    self.inner
                        .ready
                        .wait_while(state, |s| !s.is_terminal())
                        .unwrap(),
                    false,
                ),
                Some(timeout) => {
                    let (guard, result) = self
                        .inner
                        .ready
                        .wait_timeout_while(state, timeout, |s| !s.is_terminal())
                        .unwrap();
                    let timed_out = result.timed_out() && !guard.is_terminal();
                    (guard, timed_out)
                }
            };

            // Only a still-pending future has a count left to decrement; a
            // completed one dropped it along with the `PendingTokio` variant.
            if let FutureState::PendingTokio { waiters, .. } = &mut *guard {
                *waiters -= 1;
            }
            timed_out
        });

        if timed_out {
            return Err(PyTimeoutError::new_err("DriverFuture.result() timed out"));
        }

        let mut state = self.inner.state.lock_py_attached(py).unwrap();
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
        let mut state = self.inner.state.lock_py_attached(py).unwrap();
        match &mut *state {
            FutureState::Ready { result } => result.get_or_convert(py),

            FutureState::PendingTokio { .. } => {
                drop(state);
                self.wait_for_ready(py, timeout)
            }

            FutureState::PendingAsyncio { .. } => {
                Self::ensure_started(&self.inner, &mut state);
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
        let mut state = self.inner.state.lock_py_attached(py).unwrap();
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
                Self::ensure_started(&self.inner, &mut state);
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
        let mut state = self.inner.state.lock_py_attached(py).unwrap();
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
                waiters,
            } => {
                abort_handle.abort();

                let err_result: PyResult<Py<PyAny>> = Err(PyErr::from_value(exc.into_bound(py)));
                *state = FutureState::Ready {
                    result: ReadyResult::Converted(clone_result(py, &err_result)),
                };
                drop(state);

                waker.wake();
                self.inner.notify_waiters(waiters);
                CallbackKind::fire_all(py, callbacks, &err_result);

                // Re-raise the thrown exception.
                err_result
            }
        }
    }
}

pub(in crate::future) fn dropped_err() -> PyErr {
    FutureCancelledError::new_err(
        "future was dropped before completing, likely because the driver runtime shut down",
    )
}

pub(in crate::future) fn clone_result(
    py: Python<'_>,
    result: &PyResult<Py<PyAny>>,
) -> PyResult<Py<PyAny>> {
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
            let mut state = this.inner.state.lock_py_attached(py).unwrap();
            Self::ensure_started(&this.inner, &mut state);
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
        let mut state = self.inner.state.lock_py_attached(py).unwrap();
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
        let state = self.inner.state.lock_py_attached(py).unwrap();
        state.is_terminal()
    }

    fn __repr__(&self, py: Python<'_>) -> String {
        let mut state = self.inner.state.lock_py_attached(py).unwrap();
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
    module.add_class::<PyResponseFuture>()?;
    Ok(())
}
