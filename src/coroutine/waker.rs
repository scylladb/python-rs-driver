// Portions of this file were copied from the PyO3 project (https://github.com/PyO3/pyo3),
// version 0.28.x (git commit: 8fcf8fc63), licensed under either of Apache-2.0 or MIT at your option.
//
// Copyright (c) 2023-present PyO3 Project and Contributors. https://github.com/PyO3
//
// Modifications Copyright 2025 ScyllaDB, licensed under Apache-2.0 OR MIT.
//
// Changes from the original pyo3 source:
//
// - Added `yield_asyncio_future` to encapsulate initializing the asyncio future and
//   yielding it to park the Python coroutine. Returns `py.None()` if the waker was
//   already woken (sleep(0) equivalent).

use std::sync::Arc;
use std::task::Wake;

use pyo3::sync::PyOnceLock;
use pyo3::types::{PyCFunction, PyIterator};
use pyo3::{Bound, Py, PyAny, PyResult, Python, intern, wrap_pyfunction};

use pyo3::prelude::PyAnyMethods;

/// Lazy `asyncio.Future` wrapper, implementing [`Wake`] by calling `Future.set_result`.
///
/// The asyncio future is left uninitialized until [`initialize_future`] is called.
/// If [`wake`] is called before future initialization (during Rust future polling),
/// [`initialize_future`] will return `None` (roughly equivalent to `asyncio.sleep(0)`).
pub(crate) struct AsyncioWaker {
    state: PyOnceLock<Option<LoopAndFuture>>,
}

impl AsyncioWaker {
    pub(crate) fn new() -> Self {
        Self {
            state: PyOnceLock::new(),
        }
    }

    pub(crate) fn reset(&mut self) {
        self.state.take();
    }

    pub(crate) fn initialize_future<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Option<&Bound<'py, PyAny>>> {
        let init = || LoopAndFuture::new(py).map(Some);
        let loop_and_future = self.state.get_or_try_init(py, init)?.as_ref();
        Ok(loop_and_future.map(|LoopAndFuture { future, .. }| future.bind(py)))
    }

    /// Initialize the asyncio future and yield it to park the Python coroutine.
    /// Returns `py.None()` if the waker was already woken (sleep(0) equivalent).
    pub(crate) fn yield_asyncio_future(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        // `asyncio.Future` must be awaited; fortunately, it implements `__iter__ = __await__`
        // and will yield itself if its result has not been set in polling above
        if let Some(future) = self.initialize_future(py)?
            && let Some(future) = PyIterator::from_object(future)
                .expect("asyncio.Future implements __iter__ = __await__")
                .next()
        {
            return Ok(future?.unbind());
        }
        Ok(py.None())
    }
}

impl Wake for AsyncioWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref()
    }

    fn wake_by_ref(self: &Arc<Self>) {
        Python::attach(|py| {
            let state = self.state.get_or_init(py, || None);
            if let Some(loop_and_future) = state {
                loop_and_future
                    .set_result(py)
                    .expect("unexpected error in coroutine waker");
            }
        });
    }
}

struct LoopAndFuture {
    event_loop: Py<PyAny>,
    future: Py<PyAny>,
}

impl LoopAndFuture {
    fn new(py: Python<'_>) -> PyResult<Self> {
        static GET_RUNNING_LOOP: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
        let get_running_loop = GET_RUNNING_LOOP.get_or_try_init(py, || -> PyResult<_> {
            let asyncio = py.import("asyncio")?;
            Ok(asyncio.getattr("get_running_loop")?.unbind())
        })?;
        let event_loop = get_running_loop.call0(py)?;
        let future = event_loop.call_method0(py, "create_future")?;
        Ok(Self { event_loop, future })
    }

    fn set_result(&self, py: Python<'_>) -> PyResult<()> {
        static RELEASE_WAITER: PyOnceLock<Py<PyCFunction>> = PyOnceLock::new();
        let release_waiter = RELEASE_WAITER.get_or_try_init(py, || -> PyResult<_> {
            Ok(wrap_pyfunction!(release_waiter, py)?.unbind())
        })?;
        // `Future.set_result` must be called in the event loop thread,
        // so it requires `call_soon_threadsafe`
        let call_soon_threadsafe = self.event_loop.call_method1(
            py,
            intern!(py, "call_soon_threadsafe"),
            (release_waiter, self.future.bind(py)),
        );
        if let Err(err) = call_soon_threadsafe {
            // `call_soon_threadsafe` will raise if the event loop is closed;
            // instead of catching an unspecific `RuntimeError`, check directly if it's closed.
            let is_closed = self.event_loop.call_method0(py, "is_closed")?;
            if !is_closed.extract(py)? {
                return Err(err);
            }
        }
        Ok(())
    }
}

/// Call `future.set_result` if the future is not done.
///
/// The future can be cancelled by the event loop before being woken.
/// See <https://github.com/python/cpython/blob/main/Lib/asyncio/tasks.py#L452C5-L452C5>
#[pyo3::pyfunction]
fn release_waiter(future: &Bound<'_, PyAny>) -> PyResult<()> {
    let done = future.call_method0(intern!(future.py(), "done"))?;
    if !done.extract::<bool>()? {
        future.call_method1(intern!(future.py(), "set_result"), (future.py().None(),))?;
    }
    Ok(())
}
