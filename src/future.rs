use std::future::Future;
use std::sync::{Arc, Condvar, Mutex};
use std::task::Wake;

use crate::RUNTIME;

use crate::coroutine::waker::AsyncioWaker;
use crate::coroutine::{BoxedFuture, Coroutine, PollResult};
use crate::utils::PrependedIterator;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyStopIteration;
use pyo3::prelude::*;
use pyo3::sync::MutexExt;
use pyo3::types::{PyDict, PyTuple};
use pyo3::{BoundObject, Py, PyAny, PyResult};

use tokio::task::AbortHandle;

// # PyResponseFuture — hybrid design
//
// ## Three states
//
// `PendingAsyncio { coroutine }`
//     The future is driven by the asyncio event loop.
//     This is the default starting state.
//
// `PendingTokio { on_success, on_error, abort_handle, waker }`
//     The future has been spawned on the tokio runtime. `__next__` just
//     yields the asyncio future from the waker. The spawned task transitions
//     to `Ready` on completion.
//
// `Ready { result }`
//     Terminal state. Result stored permanently.
//
// ## Transitions
//
// - `PendingAsyncio` → `PendingTokio`: when callbacks are registered or `result()` is called.
//   The inner future is taken from the coroutine, spawned on tokio.
// - `PendingAsyncio` → `Ready`: when `poll` completes or `close()` is called.
// - `PendingTokio` → `Ready`: when the spawned task completes or `close()` aborts it.
// - `Ready` → (no transitions)

/// A registered callback with optional positional and keyword arguments.
struct Callback {
    callable: Py<PyAny>,
    args: Py<PyTuple>,
    kwargs: Option<Py<PyDict>>,
}

impl Callback {
    fn new(
        callable: Py<PyAny>,
        args: &Bound<'_, PyTuple>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> Self {
        Self {
            callable,
            args: args.clone().unbind(),
            kwargs: kwargs.map(|k| k.clone().unbind()),
        }
    }

    /// Invoke this callback, passing `value` as the first argument
    /// followed by any extra args/kwargs. Errors are logged and swallowed.
    fn invoke(&self, py: Python<'_>, value: &Py<PyAny>) {
        let extra = self.args.bind(py);
        let first = value.clone_ref(py).into_any();
        let rest = extra.iter().map(|item| item.unbind());
        let exact_size_wrapper = PrependedIterator::new(first, rest);
        let args = PyTuple::new(py, exact_size_wrapper)
            .expect("failed to allocate PyTuple for callback args");

        let kwargs = self.kwargs.as_ref().map(|k| Bound::clone(k.bind(py)));
        if let Err(err) = self.callable.call(py, args, kwargs.as_ref()) {
            log::error!("ResponseFuture callback raised an exception: {}", err);
        }
    }
}

/// Discriminates whether a [`Callback`] fires on success or on error.
enum CallbackKind {
    /// Fired when the future resolves successfully. Passes the result value.
    OnSuccess(Callback),
    /// Fired when the future resolves with an error. Passes the exception instance.
    OnError(Callback),
}

impl CallbackKind {
    /// Invoke this callback if its variant matches the outcome of `result`.
    fn invoke(&self, py: Python<'_>, result: &PyResult<Py<PyAny>>) {
        match (self, result) {
            (CallbackKind::OnSuccess(cb), Ok(value)) => {
                cb.invoke(py, value);
            }
            (CallbackKind::OnError(cb), Err(err)) => {
                let exc_obj = err.value(py);
                cb.invoke(py, exc_obj.as_any().as_unbound());
            }
            _ => {}
        }
    }

    /// Fire every callback in `callbacks` that matches the outcome of `result`.
    fn fire_all(py: Python<'_>, callbacks: Vec<CallbackKind>, result: &PyResult<Py<PyAny>>) {
        for cb in &callbacks {
            cb.invoke(py, result);
        }
    }
}

/// Internal state of a PyResponseFuture.
enum FutureState {
    /// Future is driven by the asyncio executor.
    PendingAsyncio { coroutine: Coroutine },
    /// Future has been spawned on the tokio runtime.
    PendingTokio {
        callbacks: Vec<CallbackKind>,
        abort_handle: Option<AbortHandle>,
        waker: Arc<AsyncioWaker>,
    },
    /// Future has completed. Result is stored permanently.
    Ready { result: PyResult<Py<PyAny>> },
}

struct FutureInner {
    state: Mutex<FutureState>,
    /// Notified when state transitions to Ready.
    ready: Condvar,
}

/// A Python awaitable wrapping a Rust future.
#[pyclass(name = "ResponseFuture", frozen)]
pub struct PyResponseFuture {
    inner: Arc<FutureInner>,
}

impl PyResponseFuture {
    /// Create a PyResponseFuture starting in PendingAsyncio (default).
    fn new<F>(future: F) -> Self
    where
        F: Future<Output = PyResult<Py<PyAny>>> + Send + 'static,
    {
        Self {
            inner: Arc::new(FutureInner {
                state: Mutex::new(FutureState::PendingAsyncio {
                    coroutine: Coroutine::new(future),
                }),
                ready: Condvar::new(),
            }),
        }
    }

    /// Create a `Py<PyResponseFuture>` from a future returning `Result<T, E>`.
    /// Starts in PendingAsyncio.
    pub(crate) fn spawn<Fut, T, E>(py: Python<'_>, future: Fut) -> PyResult<Py<PyResponseFuture>>
    where
        Fut: Future<Output = Result<T, E>> + Send + 'static,
        T: for<'py> IntoPyObject<'py>,
        E: Into<PyErr>,
    {
        Py::new(
            py,
            PyResponseFuture::new(async move {
                let result = future.await;
                Python::attach(|py| {
                    result.map_err(Into::into).and_then(|v| {
                        v.into_pyobject(py)
                            .map(|b| b.into_any().unbind())
                            .map_err(Into::into)
                    })
                })
            }),
        )
    }
    /// Create an already-resolved PyResponseFuture.
    pub(crate) fn ready(py: Python, result: PyResult<Py<PyAny>>) -> PyResult<Py<PyResponseFuture>> {
        Py::new(
            py,
            PyResponseFuture {
                inner: Arc::new(FutureInner {
                    state: Mutex::new(FutureState::Ready { result }),
                    ready: Condvar::new(),
                }),
            },
        )
    }

    /// Spawn a future on tokio, returning the abort handle.
    /// On completion the spawned task transitions `state` to `Ready`,
    /// fires callbacks, wakes the asyncio waker, and notifies the condvar.
    fn spawn_future_on_tokio<F>(
        future: F,
        inner: &Arc<FutureInner>,
        waker: &Arc<AsyncioWaker>,
    ) -> AbortHandle
    where
        F: Future<Output = PyResult<Py<PyAny>>> + Send + 'static,
    {
        let inner_clone = Arc::clone(inner);
        let waker_clone = Arc::clone(waker);

        let handle = RUNTIME.spawn(async move {
            let result = future.await;

            Python::attach(|py| {
                let callbacks = {
                    let mut state = inner_clone.state.lock_py_attached(py).unwrap();
                    match &mut *state {
                        FutureState::PendingTokio { callbacks, .. } => {
                            let taken = std::mem::take(callbacks);
                            *state = FutureState::Ready {
                                result: clone_result(py, &result),
                            };
                            Some(taken)
                        }
                        _ => None,
                    }
                };

                if let Some(cbs) = callbacks {
                    let result_for_cbs = clone_result(py, &result);
                    RUNTIME.spawn_blocking(move || {
                        Python::attach(|py| {
                            CallbackKind::fire_all(py, cbs, &result_for_cbs);
                        });

                        waker_clone.wake();
                        inner_clone.ready.notify_all();
                    });
                }
            });
        });

        handle.abort_handle()
    }

    /// Transition from PendingAsyncio to PendingTokio by spawning the given
    /// future on the tokio runtime.
    /// Must be called while holding the state lock.
    fn transition_to_tokio(
        future: BoxedFuture,
        waker: Arc<AsyncioWaker>,
        inner: &Arc<FutureInner>,
        state_guard: &mut std::sync::MutexGuard<'_, FutureState>,
    ) {
        let abort_handle = Self::spawn_future_on_tokio(future, inner, &waker);

        **state_guard = FutureState::PendingTokio {
            callbacks: Vec::new(),
            abort_handle: Some(abort_handle),
            waker,
        };
    }

    /// Poll the coroutine (__next__).
    fn poll_coroutine(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let mut state = self.inner.state.lock_py_attached(py).unwrap();
        match &mut *state {
            FutureState::Ready { result } => Err(raise_stop_iteration(py, result)),

            FutureState::PendingTokio { waker, .. } => {
                // Future is running on tokio — just yield the asyncio future.
                let waker = Arc::clone(waker);
                drop(state);
                waker.yield_asyncio_future(py)
            }

            FutureState::PendingAsyncio { coroutine } => {
                // Drive the future via the coroutine.
                match coroutine.poll(py, None)? {
                    PollResult::Pending(maybe_future) => Ok(maybe_future),
                    PollResult::Ready(result) => {
                        *state = FutureState::Ready {
                            result: clone_result(py, &result),
                        };
                        drop(state);
                        self.inner.ready.notify_all();
                        Err(raise_stop_iteration(py, &result))
                    }
                }
            }
        }
    }

    /// Close the future. Transitions to Ready with an error.
    fn close_future(&self, py: Python<'_>) {
        let err_result: PyResult<Py<PyAny>> = Err(PyRuntimeError::new_err("future was closed"));

        let (callbacks, waker) = {
            let mut state = self.inner.state.lock_py_attached(py).unwrap();

            let (callbacks, waker) = match &mut *state {
                FutureState::Ready { .. } => return,

                FutureState::PendingTokio {
                    abort_handle,
                    waker,
                    callbacks,
                    ..
                } => {
                    if let Some(ah) = abort_handle {
                        ah.abort();
                    }
                    (Some(std::mem::take(callbacks)), Some(Arc::clone(waker)))
                }

                FutureState::PendingAsyncio { coroutine } => {
                    (None, coroutine.close_and_get_waker())
                }
            };

            *state = FutureState::Ready {
                result: clone_result(py, &err_result),
            };

            (callbacks, waker)
        };

        self.inner.ready.notify_all();

        if let Some(waker) = waker {
            waker.wake();
        }

        if let Some(callbacks) = callbacks {
            CallbackKind::fire_all(py, callbacks, &err_result);
        }
    }

    /// Release the GIL, wait on the condvar until state is Ready, then return the result.
    fn wait_for_ready(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        py.detach(|| {
            let state = self.inner.state.lock().unwrap();
            let _state = self
                .inner
                .ready
                .wait_while(state, |s| !matches!(s, FutureState::Ready { .. }))
                .unwrap();
        });

        let state = self.inner.state.lock_py_attached(py).unwrap();
        match &*state {
            FutureState::Ready { result } => clone_result(py, result),
            _ => unreachable!("condvar woke but state is not Ready"),
        }
    }

    /// Block until the future is ready, returning the result.
    fn block_until_ready(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let mut state = self.inner.state.lock_py_attached(py).unwrap();
        match &mut *state {
            FutureState::Ready { result } => clone_result(py, result),

            FutureState::PendingTokio { .. } => {
                drop(state);
                self.wait_for_ready(py)
            }

            FutureState::PendingAsyncio { coroutine } => {
                let (future, waker) = coroutine
                    .take_future_and_waker()
                    .expect("PendingAsyncio coroutine has no future");
                Self::transition_to_tokio(future, waker, &self.inner, &mut state);
                drop(state);
                self.wait_for_ready(py)
            }
        }
    }

    /// Register a [`CallbackKind`] on this future.
    ///
    /// - If already `Ready`, invokes the callback immediately.
    /// - If `PendingTokio`, queues it.
    /// - If `PendingAsyncio`, transitions to `PendingTokio` first, then queues it.
    fn add_callback_impl(&self, py: Python<'_>, cb: CallbackKind) {
        let mut state = self.inner.state.lock_py_attached(py).unwrap();
        match &mut *state {
            FutureState::Ready { result } => {
                let result = clone_result(py, result);
                drop(state);
                cb.invoke(py, &result);
            }

            FutureState::PendingTokio { callbacks, .. } => {
                callbacks.push(cb);
            }

            FutureState::PendingAsyncio { coroutine } => {
                let (future, waker) = coroutine
                    .take_future_and_waker()
                    .expect("PendingAsyncio coroutine has no future");
                Self::transition_to_tokio(future, waker, &self.inner, &mut state);
                if let FutureState::PendingTokio { callbacks, .. } = &mut *state {
                    callbacks.push(cb);
                }
            }
        }
    }

    /// Throw an exception into the future.
    /// - Ready: re-raises the exception (coroutine is exhausted).
    /// - PendingAsyncio: delegates to `coroutine.poll(py, Some(exc))`.
    /// - PendingTokio: aborts the tokio task, fires on_error callbacks,
    ///   transitions to Ready, and re-raises the exception.
    fn throw_into(&self, py: Python<'_>, exc: Py<PyAny>) -> PyResult<Py<PyAny>> {
        let mut state = self.inner.state.lock_py_attached(py).unwrap();
        match &mut *state {
            FutureState::Ready { .. } => Err(PyErr::from_value(exc.into_bound(py))),

            FutureState::PendingAsyncio { coroutine } => match coroutine.poll(py, Some(exc))? {
                PollResult::Pending(value) => Ok(value),
                PollResult::Ready(result) => {
                    *state = FutureState::Ready {
                        result: clone_result(py, &result),
                    };
                    drop(state);
                    self.inner.ready.notify_all();
                    Err(raise_stop_iteration(py, &result))
                }
            },

            FutureState::PendingTokio {
                abort_handle,
                waker,
                callbacks,
                ..
            } => {
                if let Some(ah) = abort_handle {
                    ah.abort();
                }
                let waker = Arc::clone(waker);
                let taken = std::mem::take(callbacks);
                let err_result: PyResult<Py<PyAny>> =
                    Err(PyErr::from_value(exc.clone_ref(py).into_bound(py)));
                *state = FutureState::Ready {
                    result: clone_result(py, &err_result),
                };
                drop(state);

                waker.wake();
                self.inner.ready.notify_all();
                CallbackKind::fire_all(py, taken, &err_result);

                // Re-raise the thrown exception.
                err_result
            }
        }
    }
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
impl PyResponseFuture {
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
        self.close_future(py);
    }

    fn cancel(&self, py: Python<'_>) {
        self.close_future(py);
    }

    /// Get the result of this future.
    ///
    /// If the future is still pending, this blocks the calling thread until
    /// it completes (releasing the GIL while waiting).
    fn result(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.block_until_ready(py)
    }

    /// Register a callback to be invoked when the future completes successfully.
    ///
    /// The callback is called as `callback(result, *args, **kwargs)`.
    /// If the future is already done with a success, the callback is invoked immediately.
    /// If the future is pending on asyncio, it is moved to tokio to support callbacks.
    #[pyo3(signature = (callback, *args, **kwargs))]
    fn add_callback(
        &self,
        py: Python<'_>,
        callback: Py<PyAny>,
        args: &Bound<'_, PyTuple>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) {
        let cb = CallbackKind::OnSuccess(Callback::new(callback, args, kwargs));
        self.add_callback_impl(py, cb);
    }

    /// Register a callback to be invoked when the future completes with an error.
    ///
    /// The callback is called as `callback(exception, *args, **kwargs)`.
    /// If the future is already done with an error, the callback is invoked immediately.
    /// If the future is pending on asyncio, it is moved to tokio to support callbacks.
    #[pyo3(signature = (callback, *args, **kwargs))]
    fn add_errback(
        &self,
        py: Python<'_>,
        callback: Py<PyAny>,
        args: &Bound<'_, PyTuple>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) {
        let cb = CallbackKind::OnError(Callback::new(callback, args, kwargs));
        self.add_callback_impl(py, cb);
    }

    /// Register both a success and an error callback in a single call.
    ///
    /// Equivalent to calling `add_callback` and `add_errback` separately.
    /// The success callback is called as `callback(result, *callback_args, **callback_kwargs)`
    /// and the error callback as `errback(exception, *errback_args, **errback_kwargs)`.
    ///
    /// If the future is already resolved, the appropriate callback is invoked immediately.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (callback, errback, /, callback_args=None, callback_kwargs=None, errback_args=None, errback_kwargs=None))]
    fn add_callbacks(
        &self,
        py: Python<'_>,
        callback: Py<PyAny>,
        errback: Py<PyAny>,
        callback_args: Option<&Bound<'_, PyTuple>>,
        callback_kwargs: Option<&Bound<'_, PyDict>>,
        errback_args: Option<&Bound<'_, PyTuple>>,
        errback_kwargs: Option<&Bound<'_, PyDict>>,
    ) {
        let empty = PyTuple::empty(py);
        let cb_args = callback_args.unwrap_or(&empty);
        let eb_args = errback_args.unwrap_or(&empty);
        self.add_callback(py, callback, cb_args, callback_kwargs);
        self.add_errback(py, errback, eb_args, errback_kwargs);
    }

    /// Returns True if the future has completed (successfully or with an error).
    fn done(&self, py: Python<'_>) -> bool {
        let state = self.inner.state.lock_py_attached(py).unwrap();
        matches!(*state, FutureState::Ready { .. })
    }

    fn __repr__(&self, py: Python<'_>) -> String {
        let state = self.inner.state.lock_py_attached(py).unwrap();
        match &*state {
            FutureState::PendingAsyncio { .. } | FutureState::PendingTokio { .. } => {
                "<ResponseFuture pending>".to_string()
            }
            FutureState::Ready { result } => match result {
                Ok(_) => "<ResponseFuture finished>".to_string(),
                Err(e) => format!("<ResponseFuture finished exception={}>", e),
            },
        }
    }
}

#[pymodule]
pub(crate) fn future(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyResponseFuture>()?;
    Ok(())
}
