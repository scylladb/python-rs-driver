//! Batches the wakeups of parked coroutines, one event-loop trip per batch.
//!
//! Waking a coroutine parked on an `asyncio.Future` means calling `set_result` on
//! the event loop's thread. Doing that from a tokio worker costs one GIL
//! acquisition per completion (`Python::attach`, then `call_soon_threadsafe`,
//! which also writes to the loop's self-pipe) and stalls the loop thread for the
//! whole GIL section.
//!
//! A [`Batcher`] cuts that down to one GIL acquisition per loop iteration.
//! Completions push their parked future onto a queue without touching Python.
//! Only the first push after a drain schedules the drain callback; every later
//! push before the drain runs simply rides along. The drain runs on the loop
//! thread and calls `set_result` on everything queued, no `call_soon_threadsafe`
//! needed per future.
//!
//! Scheduling the drain itself needs no GIL either where the loop supports
//! `add_reader`: the batcher owns a socket pair whose read end is registered with
//! the loop, and arming is a one-byte `write(2)` from the worker. The loop's
//! selector wakes up and calls the drain. Loops without `add_reader`
//!  get `call_soon_threadsafe` instead, still once per batch.
//!
//! One batcher exists per event loop. They live in a process-wide map keyed by
//! the loop's address, which is identity — the only sensible key, since loops are
//! never equal to one another. An address alone would be unsound, because CPython
//! reuses freed ones, so each entry owns a `weakref.ref(loop, evict)` that removes
//! it while the loop is being deallocated, before its memory can be reused. The
//! map therefore never keeps a loop alive and never holds a stale entry. A loop
//! that cannot be weakly referenced has no safe key and is therefore rejected
//! outright; both asyncio and uvloop support weakrefs.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::io::{self, Read, Write};
#[cfg(unix)]
use std::os::fd::AsRawFd;
#[cfg(unix)]
use std::os::unix::net::UnixStream;
use std::sync::{Arc, LazyLock, Mutex};

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::sync::{MutexExt, PyOnceLock};
use pyo3::types::{PyCFunction, PyDict, PyTuple, PyWeakrefMethods, PyWeakrefReference};
use pyo3::{intern, wrap_pyfunction};

/// Every live loop's batcher, keyed by [`loop_key`].
static BATCHERS: LazyLock<Mutex<HashMap<usize, Registration>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// One loop's entry in [`BATCHERS`].
struct Registration {
    /// `weakref.ref(loop, evict)`, held only to keep the callback alive: a weakref
    /// nobody owns is collected immediately and then never fires.
    _weak: Py<PyWeakrefReference>,
    batcher: Arc<Batcher>,
}

/// A loop's identity. Loops are never equal to one another, so address *is* the
/// identity; [`Registration`]'s weakref is what keeps a reused address from
/// resolving to a dead entry.
fn loop_key(event_loop: &Bound<'_, PyAny>) -> usize {
    event_loop.as_ptr() as usize
}

/// `asyncio.get_running_loop`, resolved once.
pub(super) fn running_loop(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    static GET_RUNNING_LOOP: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
    let get_running_loop = GET_RUNNING_LOOP.get_or_try_init(py, || -> PyResult<_> {
        let asyncio = py.import("asyncio")?;
        Ok(asyncio.getattr("get_running_loop")?.unbind())
    })?;
    get_running_loop.bind(py).call0()
}

/// Call `future.set_result(None)` if the future is not done.
///
/// The future can be cancelled by the event loop before being woken.
/// See <https://github.com/python/cpython/blob/main/Lib/asyncio/tasks.py#L452C5-L452C5>
fn release_waiter(future: &Bound<'_, PyAny>) -> PyResult<()> {
    let py = future.py();
    let done = future.call_method0(intern!(py, "done"))?;
    if !done.extract::<bool>()? {
        future.call_method1(intern!(py, "set_result"), (py.None(),))?;
    }
    Ok(())
}

/// The completion queue of one event loop.
pub(crate) struct Batcher {
    /// Every critical section on this mutex (`push`, `drain`, `discard`) is pure
    /// Rust bookkeeping: no Python call, no Python allocation, no `Py` drop. A
    /// holder therefore never needs the GIL to release it, which is what makes
    /// a plain `lock()` safe.
    state: Mutex<State>,
    /// How a worker gets the drain onto the loop thread.
    notifier: Notifier,
}

struct State {
    /// Parked `asyncio.Future`s waiting for `set_result`.
    queue: Vec<Py<PyAny>>,
    /// Set from the first push after a drain until that drain runs.
    armed: bool,
}

/// The loop is gone or closed, so no drain can ever run.
struct LoopGone;

/// Notifies the loop about the drain.
struct Notifier {
    kind: NotifierKind,
    /// `weakref.ref(loop)`. Strong would pin the loop for as long as its entry
    /// lives, and the entry is only ever dropped because the loop died.
    event_loop: Py<PyWeakrefReference>,
}

/// How the first push of a batch schedules the drain.
enum NotifierKind {
    /// `loop.call_soon_threadsafe(drain, loop)`: one GIL acquisition per batch.
    CallSoon,
    /// A socket pair whose read end is registered with `loop.add_reader`:
    /// arming is a one-byte write, no GIL involved.
    #[cfg(unix)]
    Pipe {
        write_end: UnixStream,
        read_end: UnixStream,
    },
}

impl Notifier {
    fn new(event_loop: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self {
            kind: NotifierKind::new(event_loop),
            event_loop: PyWeakrefReference::new(event_loop)?.unbind(),
        })
    }

    /// Hand the drain to the event loop. Runs once per batch.
    ///
    /// Callable from any thread; takes the GIL only where the loop offers no
    /// `add_reader`.
    fn notify(&self) -> Result<(), LoopGone> {
        match &self.kind {
            #[cfg(unix)]
            NotifierKind::Pipe { write_end, .. } => loop {
                match (&*write_end).write(&[1]) {
                    Ok(_) => return Ok(()),
                    // The socket buffer is full, so the loop is already going to
                    // wake up and drain.
                    Err(err) if err.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                    Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                    Err(err) => {
                        log::error!(
                            "failed to wake the event loop via pipe, falling back to \
                             call_soon_threadsafe: {err}"
                        );
                        return self.schedule_drain_call_soon();
                    }
                }
            },
            NotifierKind::CallSoon => self.schedule_drain_call_soon(),
        }
    }

    fn schedule_drain_call_soon(&self) -> Result<(), LoopGone> {
        Python::attach(|py| {
            let Some(event_loop) = self.event_loop.bind(py).upgrade() else {
                return Err(LoopGone);
            };
            let scheduled = event_loop.call_method1(
                intern!(py, "call_soon_threadsafe"),
                (drain_fn(py), &event_loop),
            );
            if let Err(err) = scheduled {
                // `call_soon_threadsafe` raises once the loop is closed; anything
                // else is unexpected and worth hearing about.
                let closed = event_loop
                    .call_method0(intern!(py, "is_closed"))
                    .and_then(|c| c.extract::<bool>())
                    .unwrap_or(true);
                if !closed {
                    log::error!("failed to schedule the completion drain: {err}");
                }
                return Err(LoopGone);
            }
            Ok(())
        })
    }

    fn acknowledge(&self) {
        self.kind.acknowledge();
    }
}

impl NotifierKind {
    /// The pipe where the loop supports `add_reader`, `call_soon_threadsafe` otherwise.
    fn new(event_loop: &Bound<'_, PyAny>) -> Self {
        #[cfg(unix)]
        match Self::pipe(event_loop) {
            Ok(kind) => return kind,
            Err(err) => log::debug!(
                "event loop does not support add_reader, waking it with \
                 call_soon_threadsafe instead: {err}"
            ),
        }
        NotifierKind::CallSoon
    }

    #[cfg(unix)]
    fn pipe(event_loop: &Bound<'_, PyAny>) -> PyResult<Self> {
        let py = event_loop.py();
        let (write_end, read_end) = UnixStream::pair()?;
        write_end.set_nonblocking(true)?;
        read_end.set_nonblocking(true)?;
        event_loop.call_method1(
            intern!(py, "add_reader"),
            (read_end.as_raw_fd(), drain_fn(py), event_loop),
        )?;
        Ok(NotifierKind::Pipe {
            write_end,
            read_end,
        })
    }

    /// Empty the pipe so the loop stops reporting it readable.
    fn acknowledge(&self) {
        #[cfg(unix)]
        if let NotifierKind::Pipe { read_end, .. } = &self {
            let mut buf = [0u8; 64];
            loop {
                match (&*read_end).read(&mut buf) {
                    Ok(0) => break,
                    Ok(_) => continue,
                    Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                    Err(_) => break,
                }
            }
        }
    }
}

impl Drop for Notifier {
    /// Undo `add_reader` if it was ever registered.
    fn drop(&mut self) {
        #[cfg(unix)]
        if let NotifierKind::Pipe { read_end, .. } = &self.kind {
            let fd = read_end.as_raw_fd();
            Python::attach(|py| {
                let Some(event_loop) = self.event_loop.bind(py).upgrade() else {
                    return;
                };

                // Best effort: the loop may already be closing.
                let _ = event_loop.call_method1(intern!(py, "remove_reader"), (fd,));
            });
        }
    }
}

impl Batcher {
    fn new(event_loop: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self {
            state: Mutex::new(State {
                queue: Vec::new(),
                armed: false,
            }),
            notifier: Notifier::new(event_loop)?,
        })
    }

    /// Queue `future` for `set_result`, scheduling a drain if none is pending.
    /// Callers may hold the GIL, see `state`.
    pub(crate) fn push(&self, future: Py<PyAny>) {
        let first_of_batch = {
            #[expect(
                clippy::disallowed_methods,
                reason = "every critical section on `state` is pure Rust, see the field doc"
            )]
            let mut state = self.state.lock().unwrap();
            state.queue.push(future);
            !std::mem::replace(&mut state.armed, true)
        };
        if !first_of_batch {
            // A drain is already on its way.
            return;
        }

        if self.notifier.notify().is_err() {
            self.discard();
        }
    }

    /// The loop is gone or unusable: nothing queued can ever be woken.
    fn discard(&self) {
        #[expect(
            clippy::disallowed_methods,
            reason = "every critical section on `state` is pure Rust, see the field doc"
        )]
        let mut state = self.state.lock().unwrap();
        let dead = std::mem::take(&mut state.queue);
        // Let the next push try again, so a dead loop never accumulates a queue.
        state.armed = false;

        // We drop the lock before the futures.
        drop(state);
        drop(dead);
    }

    /// Wake everything queued. Runs on the loop thread.
    fn drain(&self, py: Python<'_>) -> PyResult<()> {
        self.notifier.acknowledge();
        let ready = {
            #[expect(
                clippy::disallowed_methods,
                reason = "nothing under `state` needs the GIL, so waiting for it with the GIL held cannot deadlock"
            )]
            let mut state = self.state.lock().unwrap();
            state.armed = false;
            std::mem::take(&mut state.queue)
        };

        let mut first_err = None;
        for future in ready {
            if let Err(err) = release_waiter(future.bind(py)) {
                first_err.get_or_insert(err);
            }
        }
        first_err.map_or(Ok(()), Err)
    }
}

/// Wake every future queued on `event_loop`'s batcher.
///
/// Scheduled with `call_soon_threadsafe`, or registered as the reader callback
/// of the batcher's pipe.
#[pyfunction]
fn drain(event_loop: &Bound<'_, PyAny>) -> PyResult<()> {
    let py = event_loop.py();

    // We need to drop the guard before calling some python functions
    // to avoid deadlock with `evict_fn`.
    let batcher = BATCHERS
        .lock_py_attached(py)
        .unwrap()
        .get(&loop_key(event_loop))
        .map(|registration| Arc::clone(&registration.batcher));

    match batcher {
        Some(batcher) => batcher.drain(py),
        None => Ok(()),
    }
}

fn drain_fn(py: Python<'_>) -> &Bound<'_, PyCFunction> {
    static DRAIN: PyOnceLock<Py<PyCFunction>> = PyOnceLock::new();
    DRAIN
        .get_or_init(py, || {
            wrap_pyfunction!(drain, py)
                .expect("wrapping a pyfunction cannot fail")
                .unbind()
        })
        .bind(py)
}

/// Remove `key`'s entry once its loop is being deallocated.
fn evict_fn(py: Python<'_>, key: usize) -> PyResult<Bound<'_, PyCFunction>> {
    PyCFunction::new_closure(
        py,
        None,
        None,
        move |args: &Bound<'_, PyTuple>, _kwargs: Option<&Bound<'_, PyDict>>| {
            BATCHERS.lock_py_attached(args.py()).unwrap().remove(&key);
        },
    )
}

/// The batcher of `event_loop`, created on first use.
///
/// # Panics
///
/// If the loop cannot be weakly referenced. Without a weakref we could never
/// evict its entry, and its address would outlive it.
pub(crate) fn batcher_for(event_loop: &Bound<'_, PyAny>) -> PyResult<Arc<Batcher>> {
    let py = event_loop.py();
    let key = loop_key(event_loop);

    if let Some(registration) = BATCHERS.lock_py_attached(py).unwrap().get(&key) {
        return Ok(Arc::clone(&registration.batcher));
    }

    let weak = match PyWeakrefReference::new_with(event_loop, evict_fn(py, key)?) {
        Ok(weak) => weak,
        Err(err) if err.is_instance_of::<PyTypeError>(py) => panic!(
            "ran on an asyncio-like event loop that cannot be weakly referenced: \
             {event_loop:?}"
        ),
        Err(err) => return Err(err),
    };

    let batcher = Arc::new(Batcher::new(event_loop)?);

    let batcher = {
        let mut batchers = BATCHERS.lock_py_attached(py).unwrap();
        match batchers.entry(key) {
            // Another thread registered this loop while we were building ours.
            Entry::Occupied(entry) => Arc::clone(&entry.get().batcher),
            Entry::Vacant(entry) => {
                entry.insert(Registration {
                    _weak: weak.unbind(),
                    batcher: Arc::clone(&batcher),
                });
                batcher
            }
        }
    };

    Ok(batcher)
}
