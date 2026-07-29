// Portions of this file were copied from the PyO3 project (https://github.com/PyO3/pyo3),
// version 0.28.x (git commit: 8fcf8fc63), licensed under either of Apache-2.0 or MIT at your option.
//
// Copyright (c) 2023-present PyO3 Project and Contributors. https://github.com/PyO3
//
// Modifications Copyright 2025 ScyllaDB, licensed under Apache-2.0 OR MIT.
//
// Changes from the original pyo3 source:
// - Removed `ThrowCallback` and the `throw_callback` field from `Coroutine`.
//   In upstream pyo3, `ThrowCallback` is used to deliver exceptions thrown into
//   the coroutine to a `CancelHandle` (the `#[pyo3(cancel_handle)]` annotation).
//   Since we don't use `CancelHandle` in this project, the throw callback is
//   unnecessary. Now, `throw()` always drops the future and reraises the
//   exception directly (the simple path that upstream uses when no callback is set).
//
// - `Coroutine` is no longer a `#[pyclass]`. It is used purely as internal Rust state,
//   not exposed to Python directly. `poll` returns a `PollResult` enum (`Pending` / `Ready`)
//   instead of a Python object, keeping the result in the Rust type system. This avoids
//   the overhead and error-prone nature of converting to Python objects before the caller
//   is ready to use them, and allows building higher-level abstractions on top using
//   full Rust type guarantees.
//
// - Imports updated from pyo3-internal paths (`alloc`, `core`, `pyo3_macros`, `crate::platform`)
//   to standard `std` and public `pyo3::` re-exports, since this code lives outside the pyo3
//   crate itself.
//
// - A `None` inner future in `poll` is now unreachable. The future is only `None` after
//   `close()` (which transitions `FutureState` to `Ready`) or `take_future_and_waker()`
//   (which transitions to `PendingTokio`). In neither case will `poll` be called on the
//   coroutine again, so the `None` branch is marked `unreachable!()`.

// - `take_future_and_waker` extracts the inner future so it can be spawned on Tokio,
//   transitioning the `FutureState` to `PendingTokio`. It returns an `Arc<AsyncioWaker>`
//   that is shared between the coroutine and the Tokio task. The waker is reset (its
//   internal asyncio future cleared) so that a fresh one can be created when needed.

// - `close_and_get_waker` drops the future and returns the waker so that the caller
//   (`PyResponseFuture::close`) can fire `waker.wake()` after writing `Ready`, ensuring any
//   Python coroutine suspended on this future gets rescheduled and sees the closed state.

// - Removed `unsafe impl Sync for Coroutine`. It is no longer needed because `Coroutine`
//   is not a `#[pyclass]` and lives behind a `Mutex`.

use std::future::Future;
use std::panic;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use crate::coroutine::waker::AsyncioWaker;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

pub(crate) mod waker;

pub(crate) type BoxedFuture = Pin<Box<dyn Future<Output = PyResult<Py<PyAny>>> + Send>>;

/// Result of polling a coroutine. In contrast to Rust Poll enum it stores Py<PyAny>
/// in Pending variant.
pub enum PollResult {
    /// The future is not ready. Yield this value to the Python event loop.
    /// Contains either an asyncio.Future object or py.None().
    Pending(Py<PyAny>),
    /// The future completed with this result.
    Ready(PyResult<Py<PyAny>>),
}

/// Rust-side coroutine wrapping a [`Future`].
pub(crate) struct Coroutine {
    future: Option<BoxedFuture>,
    waker: Option<Arc<AsyncioWaker>>,
}

impl Coroutine {
    ///  Wrap a future into a Python coroutine.
    ///
    /// Coroutine `send` polls the wrapped future, ignoring the value passed
    /// (should always be `None` anyway).
    ///
    /// `Coroutine `throw` drop the wrapped future and reraise the exception passed
    pub(crate) fn new<F>(future: F) -> Self
    where
        F: Future<Output = PyResult<Py<PyAny>>> + Send + 'static,
    {
        Self {
            future: Some(Box::pin(future)),
            waker: None,
        }
    }

    /// Takes the inner future and returns it together with the waker.
    /// Returns `None` if the future was already taken.
    pub(crate) fn take_future_and_waker(&mut self) -> Option<(BoxedFuture, Arc<AsyncioWaker>)> {
        let future = self.future.take()?;

        let waker = if let Some(existing) = &self.waker {
            Arc::clone(existing)
        } else {
            let new_waker = Arc::new(AsyncioWaker::new());
            self.waker = Some(Arc::clone(&new_waker));
            new_waker
        };
        Some((future, waker))
    }

    /// Poll the underlying future.
    pub(crate) fn poll(
        &mut self,
        py: Python<'_>,
        throw: Option<Py<PyAny>>,
    ) -> PyResult<PollResult> {
        // raise if the coroutine has already been run to completion
        let Some(ref mut future_rs) = self.future else {
            // The future is `None` only after `close()` (which sets `FutureState::Ready`)
            // or `take_future_and_waker()` (which moves to `FutureState::PendingTokio`).
            // In both cases the `FutureState` is no longer `PendingAsyncio`, so `poll`
            // on the coroutine will never be called again.
            unreachable!();
        };
        // reraise thrown exception
        if let Some(exc) = throw {
            self.close();
            return Ok(PollResult::Ready(Err(PyErr::from_value(
                exc.into_bound(py),
            ))));
        }
        // create a new waker, or try to reset it in place
        if let Some(waker) = self.waker.as_mut().and_then(Arc::get_mut) {
            waker.reset();
        } else {
            self.waker = Some(Arc::new(AsyncioWaker::new()));
        }
        let waker = Waker::from(self.waker.clone().unwrap());
        // poll the Rust future and forward its results if ready
        // polling is UnwindSafe because the future is dropped in case of panic
        let poll = || future_rs.as_mut().poll(&mut Context::from_waker(&waker));
        match std::panic::catch_unwind(panic::AssertUnwindSafe(poll)) {
            Ok(Poll::Ready(res)) => {
                self.close();
                return Ok(PollResult::Ready(res));
            }
            Err(err) => {
                self.close();
                let msg = if let Some(s) = err.downcast_ref::<&str>() {
                    s.to_string()
                } else if let Some(s) = err.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "Rust future panicked".to_string()
                };
                return Ok(PollResult::Ready(Err(PyRuntimeError::new_err(msg))));
            }
            _ => {}
        }

        // unwrap() is safe as waker is always Some() when we reach here
        // To reach here We need to either reset the waker or create it. In each case waker is Some.
        let value = self.waker.as_ref().unwrap().yield_asyncio_future(py)?;
        Ok(PollResult::Pending(value))
    }

    /// Close the coroutine, dropping the underlying future.
    /// Used when the future completed via `poll` — no waker needed since the
    /// state transition to `Ready` happens in the same call.
    fn close(&mut self) {
        drop(self.future.take());
    }

    /// Close the coroutine, dropping the underlying future, and return the waker.
    /// Used by `PyResponseFuture::close` so the caller can fire `waker.wake()` after
    /// writing `Ready`, waking any Python coroutine suspended on this future.
    pub(crate) fn close_and_get_waker(&mut self) -> Option<Arc<AsyncioWaker>> {
        drop(self.future.take());
        self.waker.take()
    }
}
