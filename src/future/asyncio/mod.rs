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
// - Upstream's `future: Option<BoxedFuture>`, emptied by `close()`, is replaced by an
//   unconditionally owned `PyBoxedFuture`: a `Coroutine` value always has a future to poll.
//   Every operation that gets rid of the future consumes the whole `Coroutine` instead of
//   emptying it — `poll` takes `self` and hands the coroutine back only in
//   `PollResult::Pending`, and `into_future_and_waker` extracts the future so it can be
//   spawned on tokio. A spent coroutine is therefore not representable, so upstream's
//   "poll after completion" check is gone: the compiler rules that case out. The inner
//   future is likewise dropped before `poll` returns, since converting its stashed output
//   consumes it.
//
// - `into_future_and_waker` extracts the inner future so it can be spawned on Tokio,
//   transitioning the `FutureState` to `PendingTokio`. It returns an `Arc<AsyncioWaker>`
//   that is shared between the coroutine and the Tokio task, so a Python coroutine already
//   suspended on this future is woken by the spawned task.
//
// - The waker is reset in place before every poll instead of being replaced whenever the
//   event loop still holds a reference to it. Our `AsyncioWaker` keeps its state behind a
//   mutex (see `waker.rs`), so it needs no unique access to reset, and a stale wake from a
//   previous poll is harmless: it only makes the coroutine poll once more.
//
// - `into_waker` drops the future and returns the waker.
//
// - The boxed future no longer lives here, and is no longer a `dyn Future`. Upstream boxes a
//   `dyn Future<Output = PyResult<PyObject>>`, which forces the Python conversion to happen
//   wherever the future completes — for a future spawned on tokio, a worker thread holding
//   no GIL, where every completing request would contend for it. Ours is a `PyBoxedFuture`
//   (see `crate::future::boxed_future`): it stashes its output inside its own allocation
//   and converts it on request, so the conversion can wait for a thread that already holds
//   the GIL. That is why the future here is driven through `poll_catch_panics` rather than
//   `Future::poll`.
//
// - Catching a panic escaping the polled future, and turning it into a `PyErr`, is
//   delegated to `poll_catch_panics`. The deferred result conversion is guarded the same
//   way, by `resolve_catch_panics`.
//
// - Removed `unsafe impl Sync for Coroutine`. It is no longer needed because `Coroutine`
//   is not a `#[pyclass]` and lives behind a `Mutex`.

use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use crate::future::asyncio::waker::AsyncioWaker;
use crate::future::boxed_future::{PyBoxedFuture, ResolvedResult};
use crate::future::panics::{poll_catch_panics, resolve_catch_panics};
use pyo3::prelude::*;

pub(crate) mod batcher;
pub(crate) mod waker;

/// Result of polling a coroutine. In contrast to the Rust `Poll` enum, the pending
/// variant carries the value to yield to the Python event loop — and the coroutine
/// itself, since [`Coroutine::poll`] consumes it.
pub(crate) enum PollResult {
    /// The future is not ready, so the coroutine is handed back to be polled again.
    ///
    /// `value` is what should be yielded to the Python event loop: either an
    /// `asyncio.Future` object or `py.None()`. It is a `PyResult` because creating the
    /// `asyncio.Future` can fail.
    Pending {
        coroutine: Coroutine,
        value: PyResult<Py<PyAny>>,
    },
    /// The future completed with this result. The coroutine is consumed.
    Ready(PyResult<Py<PyAny>>),
}

/// Rust-side coroutine wrapping a [`Future`].
pub(crate) struct Coroutine {
    future: PyBoxedFuture,
    waker: Option<Arc<AsyncioWaker>>,
}

impl Coroutine {
    ///  Wrap a future into a Python coroutine.
    ///
    /// Coroutine `send` polls the wrapped future, ignoring the value passed
    /// (should always be `None` anyway).
    ///
    /// `Coroutine `throw` drop the wrapped future and reraise the exception passed
    pub(crate) fn new(future: PyBoxedFuture) -> Self {
        Self {
            future,
            waker: None,
        }
    }

    /// Consume the coroutine, dropping the inner future, and return the waker it was
    /// parked on, if any.
    pub(crate) fn into_waker(self) -> Option<Arc<AsyncioWaker>> {
        self.waker
    }

    /// Consume the coroutine, returning the inner future so it can be driven elsewhere
    /// together with the waker it was parked on.
    pub(crate) fn into_future_and_waker(self) -> (PyBoxedFuture, Arc<AsyncioWaker>) {
        let Coroutine { future, waker } = self;
        (
            future,
            waker.unwrap_or_else(|| Arc::new(AsyncioWaker::new())),
        )
    }

    /// Return the waker to poll with, reset so that only a wake arriving during
    /// this poll counts.
    fn poll_waker(&mut self, py: Python<'_>) -> Arc<AsyncioWaker> {
        let waker = self
            .waker
            .get_or_insert_with(|| Arc::new(AsyncioWaker::new()));
        waker.reset(py);
        Arc::clone(waker)
    }

    /// Poll the underlying future, consuming the coroutine.
    ///
    /// The coroutine is handed back in [`PollResult::Pending`] and only there, so a
    /// future that has completed — or been thrown into, or panicked — cannot be polled
    /// again.
    pub(crate) fn poll(mut self, py: Python<'_>, throw: Option<Py<PyAny>>) -> PollResult {
        // reraise thrown exception, dropping the future along with `self`
        if let Some(exc) = throw {
            return PollResult::Ready(Err(PyErr::from_value(exc.into_bound(py))));
        }
        let asyncio_waker = self.poll_waker(py);
        let waker = Waker::from(Arc::clone(&asyncio_waker));
        // poll the Rust future and forward its result if ready
        match poll_catch_panics(self.future.as_mut(), &mut Context::from_waker(&waker)) {
            // The output is stashed inside the future. Convert it here rather than
            // later as We already have GIL so it cost us nothing.
            Poll::Ready(Ok(())) => {
                let resolved = ResolvedResult::Stashed(self.future);
                return PollResult::Ready(resolve_catch_panics(resolved, py));
            }
            Poll::Ready(Err(err)) => return PollResult::Ready(Err(err)),
            Poll::Pending => {}
        }

        let value = asyncio_waker.yield_asyncio_future(py);
        PollResult::Pending {
            coroutine: self,
            value,
        }
    }
}
