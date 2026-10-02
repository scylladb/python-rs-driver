//! `ResponseFuture`: the callback-driven future of the legacy driver.
//!
//! Always runs on the tokio runtime, so there is no asyncio state to hand over
//! from. Two states suffice: `Pending` while a request is in flight and `Ready`
//! once it settled, and `start_fetching_next_page()` moves `Ready` back to
//! `Pending`. Callbacks persist across pages and fire once per page, as the
//! legacy driver did.
//!
//! The worker completing a request never takes the GIL: it stores the page and,
//! if callbacks are registered, wakes the future's drainer, a blocking thread
//! that builds the rows and runs the callbacks page by page. A page paged past
//! before the drainer reached it is moved to a queue rather than dropped, so
//! every page's callbacks fire in page order with the rows it settled with.
//! Rows are built by whichever GIL-holding thread asks first: a `result()`
//! caller, the drainer, or `add_callback` on a settled future. The row factory
//! is user Python that may touch the future, so they are built with the lock
//! released; two threads asking at once both build, and the first to store wins.

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

use pyo3::PyTypeInfo;
use pyo3::exceptions::PyNotImplementedError;
use pyo3::prelude::*;
use pyo3::sync::{MutexExt, PyOnceLock};
use pyo3::types::{PyDict, PyList, PyString, PyTuple};
use scylla::frame::response::result::ColumnSpec;
use uuid::Uuid;

use crate::RUNTIME;
use crate::cluster::metadata::query_metadata::{column_spec_tuple, column_type_list};
use crate::core::results::{RequestResultCore, page_rows};
use crate::deserialize::results::{PyPagingState, RowFactory};
use crate::errors::execution::DriverExecuteError;
use crate::errors::{QueryExhausted, ScyllaError};
use crate::future::callbacks::LegacyCallback;
use crate::future::panics::catch_panics_typed;
use crate::future::task::spawn_guarded;
use crate::future::{BoxedFuture, boxed_py_future, dropped_err};
use crate::legacy::PyResultSet;
use crate::legacy::session::LegacyQuery;
use crate::utils::WithOriginalPyObject;

mod state;
use state::{Callbacks, Delivery, ReadyRows, ResponseState, Rows, Undelivered, callback_value};

/// Owned by the running drainer, else parked in `Shared::drain_permit`. There
/// is exactly one per future, so two drainers can never run at once.
struct DrainToken(());

/// Everything the one lock guards.
struct Shared {
    state: ResponseState,
    /// The last page received: the source of paging state, columns and the next
    /// page. Kept while the next one is in flight, as the legacy driver did.
    page: Option<RequestResultCore>,
    /// Dropped once no page can follow: a callback often holds the future
    /// itself, and this pyclass has no GC traversal to break that cycle.
    registered: Callbacks,
    /// Pages paged past before the drainer ran their callbacks, oldest first.
    unfired_pages: VecDeque<Undelivered>,
    drain_permit: Option<DrainToken>,
    /// Tracing id of every page received, in order.
    tracing_ids: Vec<Uuid>,
    /// Threads blocked in `wait_rows`. Lets a completion skip the condvar's
    /// syscall when nobody waits.
    waiters: usize,
}

impl Shared {
    /// The drainer's next page: the oldest paged past, else the current one.
    fn next_undelivered(&mut self, py: Python<'_>) -> Option<Undelivered> {
        self.unfired_pages
            .pop_front()
            .or_else(|| self.state.take_awaiting(py))
    }

    /// Whether nothing can fire again: the request settled and no page can be
    /// requested after the last one received.
    fn is_terminal(&self) -> bool {
        !self.state.is_pending()
            && !self
                .page
                .as_ref()
                .is_some_and(RequestResultCore::has_more_pages)
    }
}

struct ResponseInner {
    shared: Mutex<Shared>,
    /// Notified on every transition to `Ready`, if anyone waits.
    ready: Condvar,
    /// Row factory of the request, the same for every page.
    row_factory: Option<Py<RowFactory>>,
}

impl ResponseInner {
    fn new(row_factory: Option<Py<RowFactory>>) -> Arc<Self> {
        Arc::new(Self {
            shared: Mutex::new(Shared {
                state: ResponseState::pending(),
                page: None,
                registered: Callbacks::default(),
                unfired_pages: VecDeque::new(),
                drain_permit: Some(DrainToken(())),
                tracing_ids: Vec::new(),
                waiters: 0,
            }),
            ready: Condvar::new(),
            row_factory,
        })
    }

    fn row_factory(&self) -> Option<&Py<RowFactory>> {
        self.row_factory.as_ref()
    }

    /// Wake the threads that were blocked in `wait_rows`.
    fn notify_waiters(&self, waiters: usize) {
        if waiters > 0 {
            self.ready.notify_all();
        }
    }

    /// Spawns a request whose page, once received, becomes the current one.
    fn spawn<E>(self: &Arc<Self>, request: BoxedFuture<RequestResultCore, E>)
    where
        E: Into<PyErr> + 'static,
    {
        let done = Arc::clone(self);
        let dropped = Arc::clone(self);

        spawn_guarded(
            catch_panics_typed(request),
            move |outcome| done.complete(outcome.and_then(|page| page.map_err(Into::into))),
            move || dropped.complete_dropped(),
        );
    }

    /// The request finished on a runtime worker: settle without touching
    /// Python, and wake the drainer if anything is registered to fire.
    fn complete(self: &Arc<Self>, outcome: PyResult<RequestResultCore>) {
        let (page, rows) = match outcome {
            Ok(page) => {
                let rows = ReadyRows::Unconverted(Arc::clone(&page.query_result));
                (Some(page), rows)
            }
            Err(err) => (None, ReadyRows::Converted(Err(err))),
        };

        let waiters = {
            let mut shared = self.shared.lock().unwrap();
            if let Some(page) = page {
                shared.tracing_ids.extend(page.query_result.tracing_id());
                shared.page = Some(page);
            }

            let awaiting = shared.registered.snapshot_for(&rows);
            let wake_drainer = awaiting.is_some();
            shared.state.settle(rows, awaiting);
            if shared.is_terminal() {
                shared.registered.clear();
            }

            if wake_drainer && let Some(token) = shared.drain_permit.take() {
                self.drain(token);
            }
            shared.waiters
        };

        self.notify_waiters(waiters);
    }

    /// The rows of `ready`, built here if not yet. Must be called with the lock
    /// released: the row factory is user Python that may touch this future.
    /// A page built by two threads at once is stored by the first to finish;
    /// the other keeps its own copy.
    fn build(&self, py: Python<'_>, ready: ReadyRows) -> Rows {
        match ready {
            ReadyRows::Converted(rows) => rows,
            ReadyRows::Unconverted(query_result) => {
                let rows = page_rows(py, &query_result, self.row_factory());
                let mut shared = self.shared.lock_py_attached(py).unwrap();
                shared.state.cache(py, &query_result, rows)
            }
        }
    }

    /// Runs the callbacks of every undelivered page, oldest first, on a
    /// blocking thread, then parks the token again.
    fn drain(self: &Arc<Self>, token: DrainToken) {
        let inner = Arc::clone(self);
        RUNTIME.spawn_blocking(move || {
            Python::attach(|py| {
                loop {
                    let undelivered = {
                        let mut shared = inner.shared.lock_py_attached(py).unwrap();
                        match shared.next_undelivered(py) {
                            Some(undelivered) => undelivered,
                            None => {
                                shared.drain_permit = Some(token);
                                return;
                            }
                        }
                    };

                    let result = inner.build(py, undelivered.rows);
                    undelivered.to_fire.fire(py, &result);
                }
            });
        });
    }

    /// The request was torn down before completing, likely because the
    /// runtime shut down. Nothing will fire again, so the errbacks are consumed.
    fn complete_dropped(&self) {
        let (errbacks, waiters) = {
            let mut shared = self.shared.lock().unwrap();
            if !shared.state.is_pending() {
                return;
            }

            shared
                .state
                .settle(ReadyRows::Converted(Err(dropped_err())), None);
            (
                std::mem::take(&mut shared.registered).errbacks,
                shared.waiters,
            )
        };

        self.notify_waiters(waiters);

        if !errbacks.is_empty() {
            Python::attach(|py| {
                let err = dropped_err();
                LegacyCallback::fire_all(py, &errbacks, err.value(py).as_any());
            });
        }
    }
}

/// A request in flight, delivering its result synchronously via `result()` or
/// asynchronously via callbacks. Returned by `LegacySession.execute_async()`.
#[pyclass(name = "ResponseFuture", frozen)]
pub(crate) struct PyResponseFuture {
    inner: Arc<ResponseInner>,
    /// The query as passed in, and what it was extracted to.
    query: WithOriginalPyObject<LegacyQuery>,
    timeout: Option<f64>,
    /// Result columns, identical for every page.
    columns: PyOnceLock<Py<PyTuple>>,
    column_names: PyOnceLock<Py<PyList>>,
    column_types: PyOnceLock<Py<PyList>>,
}

impl PyResponseFuture {
    /// Spawns `request` on the runtime, returning the future that delivers its result.
    pub(crate) fn spawn(
        py: Python<'_>,
        request: BoxedFuture<RequestResultCore, DriverExecuteError>,
        query: WithOriginalPyObject<LegacyQuery>,
        timeout: Option<f64>,
        row_factory: Option<Py<RowFactory>>,
    ) -> PyResult<Py<Self>> {
        let inner = ResponseInner::new(row_factory);
        inner.spawn(request);

        Py::new(
            py,
            Self {
                inner,
                query,
                timeout,
                columns: PyOnceLock::new(),
                column_names: PyOnceLock::new(),
                column_types: PyOnceLock::new(),
            },
        )
    }

    /// Blocks, with the GIL released, until the request in flight settles;
    /// returns the rows of its page (a list or `None`) or raises its error.
    ///
    /// Another thread may call `start_fetching_next_page()` between the wakeup
    /// and the read, so the state can be `Pending` again here. Like the legacy
    /// driver, which does not support paging concurrently with `result()`, no
    /// attempt is made to return the page that was skipped: the loop waits for
    /// whichever page settles next.
    pub(crate) fn wait_rows(&self, py: Python<'_>) -> Rows {
        loop {
            let ready = self.shared(py).state.ready_rows(py);
            if let Some(ready) = ready {
                return self.inner.build(py, ready);
            }

            py.detach(|| {
                let mut shared = self.inner.shared.lock().unwrap();
                if !shared.state.is_pending() {
                    return;
                }

                shared.waiters += 1;
                let mut guard = self
                    .inner
                    .ready
                    .wait_while(shared, |shared| shared.state.is_pending())
                    .unwrap();
                guard.waiters -= 1;
            });
        }
    }

    /// The one lock, taken so that waiting for it releases the GIL.
    fn shared(&self, py: Python<'_>) -> MutexGuard<'_, Shared> {
        self.inner.shared.lock_py_attached(py).unwrap()
    }

    /// The query being executed, as extracted.
    pub(crate) fn query_ref(&self) -> &LegacyQuery {
        &self.query.extracted
    }

    /// Row factory of the request, `None` for the driver default.
    pub(crate) fn row_factory_ref(&self) -> Option<&Py<RowFactory>> {
        self.inner.row_factory()
    }

    /// Registers `callback` for `delivery`. If the matching outcome is already
    /// there it runs right away, unless the drainer still has this page to do:
    /// then it joins that run, so callbacks keep firing in page order. Once no
    /// page can follow, the callback is not kept.
    ///
    /// The rows of a settled page are built with the lock released, so a thread
    /// paging this future meanwhile can have the drainer fire `callback` for
    /// the next page before it runs here for this one. The legacy driver has
    /// the same window: its `add_callback` invokes the callback after dropping
    /// `_callback_lock`. It takes a second thread paging concurrently, so this
    /// should rarely happen; closing it is left for later.
    fn register(&self, py: Python<'_>, callback: LegacyCallback, delivery: Delivery) {
        let callback = Arc::new(callback);
        let ready = {
            let mut shared = self.shared(py);
            if !shared.is_terminal() {
                shared
                    .registered
                    .list_mut(delivery)
                    .push(Arc::clone(&callback));
            }

            if let Some(awaiting) = shared.state.awaiting_mut() {
                awaiting.list_mut(delivery).push(callback);
                return;
            }

            shared.state.ready_rows(py)
        };

        if let Some(ready) = ready {
            let rows = self.inner.build(py, ready);
            if Delivery::of(&rows) == delivery {
                callback.invoke(py, &callback_value(py, &rows));
            }
        }
    }

    /// Something built off the result columns, cached in `cell` from the first
    /// call that finds a page with rows; `None` before one arrived.
    fn cached_columns<T>(
        &self,
        py: Python<'_>,
        cell: &PyOnceLock<Py<T>>,
        build: impl FnOnce(&[ColumnSpec<'_>]) -> PyResult<Py<T>>,
    ) -> PyResult<Option<Py<T>>> {
        if let Some(cached) = cell.get(py) {
            return Ok(Some(cached.clone_ref(py)));
        }

        let built = {
            let shared = self.shared(py);
            let Some(specs) = shared.page.as_ref().and_then(RequestResultCore::col_specs) else {
                return Ok(None);
            };
            build(specs)?
        };

        Ok(Some(cell.get_or_init(py, || built).clone_ref(py)))
    }
}

#[pymethods]
impl PyResponseFuture {
    /// Blocks until the request settles; returns its `ResultSet` or raises.
    pub(crate) fn result(slf: Py<Self>, py: Python<'_>) -> PyResult<Py<PyResultSet>> {
        let rows = slf.get().wait_rows(py)?;
        Py::new(py, PyResultSet::new(py, slf, rows))
    }

    /// Registers `fn(rows, *args, **kwargs)` to run when a page arrives.
    /// Runs immediately if one already has. Returns `self`.
    #[pyo3(signature = (callback, *args, **kwargs))]
    fn add_callback(
        slf: Py<Self>,
        py: Python<'_>,
        callback: Py<PyAny>,
        args: Bound<'_, PyTuple>,
        kwargs: Option<Bound<'_, PyDict>>,
    ) -> Py<Self> {
        let callback = LegacyCallback::new(callback, args.unbind(), kwargs.map(Bound::unbind));
        slf.get().register(py, callback, Delivery::Callbacks);
        slf
    }

    /// Registers `fn(exception, *args, **kwargs)` to run when a request fails.
    /// Runs immediately if one already has. Returns `self`.
    #[pyo3(signature = (errback, *args, **kwargs))]
    fn add_errback(
        slf: Py<Self>,
        py: Python<'_>,
        errback: Py<PyAny>,
        args: Bound<'_, PyTuple>,
        kwargs: Option<Bound<'_, PyDict>>,
    ) -> Py<Self> {
        let errback = LegacyCallback::new(errback, args.unbind(), kwargs.map(Bound::unbind));
        slf.get().register(py, errback, Delivery::Errbacks);
        slf
    }

    /// `add_callback` and `add_errback` in one call.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (callback, errback, callback_args=None, callback_kwargs=None, errback_args=None, errback_kwargs=None))]
    fn add_callbacks(
        slf: Py<Self>,
        py: Python<'_>,
        callback: Py<PyAny>,
        errback: Py<PyAny>,
        callback_args: Option<Bound<'_, PyAny>>,
        callback_kwargs: Option<Bound<'_, PyDict>>,
        errback_args: Option<Bound<'_, PyAny>>,
        errback_kwargs: Option<Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        let callback_args = args_tuple(py, callback_args)?;
        let errback_args = args_tuple(py, errback_args)?;
        Self::add_callback(
            slf.clone_ref(py),
            py,
            callback,
            callback_args,
            callback_kwargs,
        );
        Self::add_errback(slf, py, errback, errback_args, errback_kwargs);
        Ok(())
    }

    /// Drops the registered callbacks, so none fire for a later page. A page that
    /// already settled still fires the snapshot taken when it did — `clear_callbacks`
    /// cannot reach it, exactly as in the legacy driver, where `_set_final_result`
    /// copies the callback list under `_callback_lock` and invokes the copy after
    /// releasing it.
    fn clear_callbacks(&self, py: Python<'_>) {
        self.shared(py).registered.clear();
    }

    /// Whether the last page received says more follow.
    #[getter]
    pub(crate) fn has_more_pages(&self, py: Python<'_>) -> bool {
        let shared = self.shared(py);
        shared
            .page
            .as_ref()
            .is_some_and(RequestResultCore::has_more_pages)
    }

    /// Starts fetching the next page; callbacks fire again when it arrives.
    /// Raises `QueryExhausted` if there is none.
    pub(crate) fn start_fetching_next_page(&self, py: Python<'_>) -> PyResult<()> {
        let page = {
            let mut shared = self.shared(py);
            if shared.state.is_pending() {
                return Err(ScyllaError::new_err("a request is already in flight"));
            }
            let page = match &shared.page {
                Some(page) if page.has_more_pages() => page.clone(),
                _ => return Err(QueryExhausted::new_err("no more pages")),
            };

            if let Some(undelivered) = shared.state.start_next() {
                shared.unfired_pages.push_back(undelivered);
            }
            page
        };

        let next_page = async move {
            page.fetch_next_page()
                .await?
                .ok_or_else(|| QueryExhausted::new_err("no more pages"))
        };

        self.inner.spawn(boxed_py_future(next_page));
        Ok(())
    }

    /// Paging state of the last page, `None` if the query is not paged or exhausted.
    #[getter]
    pub(crate) fn paging_state(&self, py: Python<'_>) -> Option<PyPagingState> {
        let shared = self.shared(py);
        let page = shared.page.as_ref()?;

        page.paging_state().map(|inner| PyPagingState { inner })
    }

    /// Specifications of the result columns, `None` until a page with rows arrived.
    #[getter]
    pub(crate) fn columns(&self, py: Python<'_>) -> PyResult<Option<Py<PyTuple>>> {
        self.cached_columns(py, &self.columns, |specs| column_spec_tuple(py, specs))
    }

    /// Names of the result columns, `None` until a page with rows arrived.
    #[getter]
    pub(crate) fn column_names(&self, py: Python<'_>) -> PyResult<Option<Py<PyList>>> {
        self.cached_columns(py, &self.column_names, |specs| {
            let names = specs.iter().map(|spec| PyString::new(py, spec.name()));
            Ok(PyList::new(py, names)?.unbind())
        })
    }

    /// CQL types of the result columns, `None` until a page with rows arrived.
    #[getter]
    pub(crate) fn column_types(&self, py: Python<'_>) -> PyResult<Option<Py<PyList>>> {
        self.cached_columns(py, &self.column_types, |specs| column_type_list(py, specs))
    }

    /// Warnings the server attached to the last page. Raises until the request settled.
    #[getter]
    fn warnings(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let shared = self.shared(py);
        if shared.state.is_pending() {
            return Err(ScyllaError::new_err(
                "warnings cannot be retrieved before ResponseFuture is finalized",
            ));
        }

        let Some(page) = shared.page.as_ref() else {
            return Ok(PyList::empty(py).unbind());
        };

        let warnings = page.query_result.warnings().map(|w| PyString::new(py, w));

        Ok(PyList::new(py, warnings)?.unbind())
    }

    /// Custom payload of the response; not exposed by the Rust driver, so always `None`.
    /// Raises until the request settled.
    #[getter]
    fn custom_payload(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        if self.shared(py).state.is_pending() {
            return Err(ScyllaError::new_err(
                "custom_payload cannot be retrieved before ResponseFuture is finalized",
            ));
        }
        Ok(None)
    }

    /// The statement being executed.
    #[getter]
    pub(crate) fn query(&self, py: Python<'_>) -> Py<PyAny> {
        self.query.original.clone_ref(py)
    }

    /// Client-side timeout of the request in seconds, `None` for no timeout.
    #[getter]
    fn timeout(&self) -> Option<f64> {
        self.timeout
    }

    /// Row factory of the request, `None` for the driver default.
    #[getter]
    fn row_factory(&self, py: Python<'_>) -> Option<Py<RowFactory>> {
        self.inner.row_factory().map(|f| f.clone_ref(py))
    }

    /// Not exposed by the Rust driver.
    #[getter]
    fn coordinator_host(&self) -> PyResult<()> {
        Err(PyNotImplementedError::new_err(
            "coordinator_host is not supported yet",
        ))
    }

    /// Not exposed by the Rust driver.
    #[getter]
    fn attempted_hosts(&self) -> PyResult<Vec<()>> {
        Err(PyNotImplementedError::new_err(
            "attempted_hosts is not supported yet",
        ))
    }

    /// The Rust driver awaits schema agreement itself; always `True`.
    #[getter]
    fn is_schema_agreed(&self) -> bool {
        true
    }

    /// Tracing id of every page received so far, if tracing was enabled.
    fn get_query_trace_ids(&self, py: Python<'_>) -> Vec<Uuid> {
        self.shared(py).tracing_ids.clone()
    }

    #[pyo3(signature = (max_wait=None, query_cl=None))]
    fn get_query_trace(
        &self,
        max_wait: Option<Bound<'_, PyAny>>,
        query_cl: Option<Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let _ = (max_wait, query_cl);
        Err(PyNotImplementedError::new_err(
            "fetching query traces is not supported; use get_query_trace_ids()",
        ))
    }

    #[pyo3(signature = (max_wait_per=None, query_cl=None))]
    fn get_all_query_traces(
        &self,
        max_wait_per: Option<Bound<'_, PyAny>>,
        query_cl: Option<Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let _ = (max_wait_per, query_cl);
        Err(PyNotImplementedError::new_err(
            "fetching query traces is not supported; use get_query_trace_ids()",
        ))
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let outcome = {
            let shared = self.shared(py);
            if shared.state.is_pending() {
                "(no result yet)".to_string()
            } else if let Some(err) = shared.state.error() {
                format!("exception={err}")
            } else {
                "result".to_string()
            }
        };

        Ok(format!(
            "<ResponseFuture: query={} {outcome}>",
            self.query.original.bind(py).repr()?
        ))
    }
}

/// `args` as passed to `add_callbacks`: any iterable, defaulting to none.
fn args_tuple<'py>(
    py: Python<'py>,
    args: Option<Bound<'py, PyAny>>,
) -> PyResult<Bound<'py, PyTuple>> {
    match args {
        None => Ok(PyTuple::empty(py)),
        Some(args) => match args.cast_into::<PyTuple>() {
            Ok(tuple) => Ok(tuple),
            Err(err) => PyTuple::type_object(py)
                .call1((err.into_inner(),))?
                .cast_into::<PyTuple>()
                .map_err(Into::into),
        },
    }
}
