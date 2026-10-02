// Portions of this file were copied from the PyO3 project (https://github.com/PyO3/pyo3),
// version 0.28.x (git commit: 8fcf8fc63), licensed under either of Apache-2.0 or MIT at your option.
//
// Copyright (c) 2023-present PyO3 Project and Contributors. https://github.com/PyO3
//
// Modifications Copyright 2025 ScyllaDB, licensed under Apache-2.0 OR MIT.

//! Changes from the original pyo3 source:
//!
//! - The `PyOnceLock<Option<LoopAndFuture>>` became a `Mutex<WakerSlot>`. Upstream's
//!   once-cell has two costs on the hot path: its initialisation detaches from the
//!   interpreter and reattaches (a GIL release per parked coroutine, and the moment a
//!   tokio worker steals the GIL), and it can only be reset through `&mut self`, which
//!   forced a fresh `Arc` whenever the event loop still held the old one. The mutex is
//!   uncontended in practice and resets in place.
//!
//! - `wake` no longer attaches to the interpreter per wake. Upstream did, and called
//!   `call_soon_threadsafe` every time. Here the parked `asyncio.Future` is handed to
//!   the loop's `Batcher` (see `batcher.rs`), which wakes a whole batch in one trip to
//!   the loop thread — and where the loop supports `add_reader`, without touching Python
//!   at all, since the wake is then a one-byte write. Upstream's per-wake path is gone:
//!   every loop carries a batcher.
//!
//! - Added `yield_asyncio_future` to encapsulate parking: it creates the asyncio future
//!   and yields it, or returns `py.None()` if the waker was already woken (the
//!   `sleep(0)` equivalent).

use std::sync::{Arc, Mutex, MutexGuard};
use std::task::Wake;

use pyo3::intern;
use pyo3::prelude::*;
use pyo3::sync::MutexExt;
use pyo3::types::PyIterator;

use crate::future::asyncio::batcher::{Batcher, batcher_for, running_loop};
use crate::utils::thread_is_attached;

/// Where the coroutine using this waker currently is.
enum WakerSlot {
    /// Not parked, no wake pending.
    Idle,
    /// Woken before it could park: the next `yield_asyncio_future` must yield None.
    Woken,
    /// Parked on an `asyncio.Future`.
    Parked(Parked),
}

/// A parked coroutine's `asyncio.Future` and the batcher that will wake it.
struct Parked {
    future: Py<PyAny>,
    batcher: Arc<Batcher>,
}

/// Lazy `asyncio.Future` wrapper, implementing [`Wake`] by arranging for
/// `Future.set_result` to run on the event loop thread.
///
/// The asyncio future is left uninitialized until [`yield_asyncio_future`] is called.
/// If [`wake`] is called before that (during Rust future polling),
/// [`yield_asyncio_future`] yields `None` instead (roughly `asyncio.sleep(0)`).
///
/// [`yield_asyncio_future`]: AsyncioWaker::yield_asyncio_future
/// [`wake`]: Wake::wake
pub(crate) struct AsyncioWaker {
    slot: Mutex<WakerSlot>,
}

impl AsyncioWaker {
    pub(crate) fn new() -> Self {
        Self {
            slot: Mutex::new(WakerSlot::Idle),
        }
    }

    /// Forget any pending wake or parked future. Called right before a poll, so a
    /// wake that arrives during the poll is the only one that counts.
    pub(crate) fn reset(&self, py: Python<'_>) {
        // A `Parked` dropped here releases its `Py`s, which needs the GIL we hold.
        *self.slot.lock_py_attached(py).unwrap() = WakerSlot::Idle;
    }

    /// Park the coroutine: create the asyncio future and yield it.
    /// Returns `py.None()` if the waker was already woken (sleep(0) equivalent).
    pub(crate) fn yield_asyncio_future(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let mut slot = self.slot.lock_py_attached(py).unwrap();

        match &*slot {
            WakerSlot::Woken => {
                *slot = WakerSlot::Idle;
                return Ok(py.None());
            }
            // Polled again while still parked (nothing woke us): keep waiting on
            // the same future, unless it is done, in which case park afresh below.
            WakerSlot::Parked(parked) => {
                if let Some(yielded) = yield_future(parked.future.bind(py))? {
                    return Ok(yielded);
                }
            }
            WakerSlot::Idle => {}
        }

        let event_loop = running_loop(py)?;
        let future = event_loop.call_method0(intern!(py, "create_future"))?;
        let yielded = yield_future(&future)?.expect("a fresh asyncio.Future is not done");

        *slot = WakerSlot::Parked(Parked {
            batcher: batcher_for(&event_loop)?,
            future: future.unbind(),
        });
        Ok(yielded)
    }

    /// Wake from a thread attached to the interpreter.
    pub(crate) fn wake_py_attached(&self, py: Python<'_>) {
        deliver(take_parked(self.slot.lock_py_attached(py).unwrap()));
    }

    /// Wake from a thread that is not attached to the interpreter.
    pub(crate) fn wake_detached(&self) {
        #[expect(clippy::disallowed_methods, reason = "caller is detached")]
        let slot = self.slot.lock().unwrap();
        deliver(take_parked(slot));
    }
}

/// Record a wake in the slot, returning the parked future to deliver it to, if any.
/// Idle becomes Woken, Woken stays Woken, Parked becomes Idle.
fn take_parked(mut slot: MutexGuard<'_, WakerSlot>) -> Option<Parked> {
    match std::mem::replace(&mut *slot, WakerSlot::Woken) {
        WakerSlot::Parked(parked) => {
            *slot = WakerSlot::Idle;
            Some(parked)
        }
        WakerSlot::Idle | WakerSlot::Woken => None,
    }
}

/// Hand a parked future to its loop's batcher. Called with the slot released.
fn deliver(parked: Option<Parked>) {
    if let Some(Parked { future, batcher }) = parked {
        batcher.push(future);
    }
}

/// What to yield to the event loop to park on `future`: the future itself, or
/// `None` if it is already done.
///
/// `asyncio.Future` must be awaited; fortunately, it implements `__iter__ = __await__`
/// and yields itself, flagged as blocking, if its result has not been set.
fn yield_future<'py>(future: &Bound<'py, PyAny>) -> PyResult<Option<Py<PyAny>>> {
    PyIterator::from_object(future)
        .expect("asyncio.Future implements __iter__ = __await__")
        .next()
        .map(|yielded| yielded.map(Bound::unbind))
        .transpose()
}

impl Wake for AsyncioWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref()
    }

    /// Called by the Rust future being polled, so the context is unknown: a future
    /// may wake itself during a poll, while attached, or later from a detached reactor.
    fn wake_by_ref(self: &Arc<Self>) {
        if thread_is_attached() {
            // SAFETY: we just checked this thread is attached.
            self.wake_py_attached(unsafe { Python::assume_attached() });
        } else {
            self.wake_detached();
        }
    }
}
