//! `ResponseFuture`: the callback-driven future of the legacy driver.
//!
//! Always runs on the tokio runtime, so there is no asyncio state to hand over
//! from. A request is `Pending` while in flight, then `Received` or `Failed`,
//! and `start_fetching_next_page()` moves it back to `Pending`. Callbacks
//! persist across pages and fire once per page, as the legacy driver did.
//!
//! The worker completing a request never takes the GIL: it stores the page and,
//! if callbacks are registered, wakes the future's drainer, a blocking thread
//! that builds the rows and runs the callbacks page by page. A page paged past
//! before the drainer reached it is moved to a queue rather than dropped, so
//! every page's callbacks fire in page order with the rows it settled with.
//! Rows are built by whichever GIL-holding thread asks first: a `result()`
//! caller, the drainer, or `add_callback` on a settled future.
//!
//! - `rows`: the rows of a page, built or still to build.
//! - `state`: the request in flight or how it settled, and the last page received.
//! - `drainer`: the thread that runs the callbacks.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use futures::TryFutureExt;
use pyo3::PyTypeInfo;
use pyo3::exceptions::PyNotImplementedError;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyDict, PyList, PyString, PyTuple};
use scylla::frame::response::result::ColumnSpec;
use uuid::Uuid;

use crate::TaskExecutionMode;
use crate::cluster::metadata::query_metadata::{column_spec_tuple, column_type_list};
use crate::core::results::PendingRequestResult;
use crate::deserialize::row_factory::PyRowFactory;
use crate::errors::ScyllaError;
use crate::errors::execution::DriverExecuteError;
use crate::future::callbacks::{Callbacks, Delivery, LegacyCallback};
use crate::future::dropped_err;
use crate::future::panics::catch_unwind;
use crate::future::state_lock::StateLock;
use crate::future::task::{TaskDropped, spawn_guarded};
use crate::legacy::PyResultSet;
use crate::legacy::errors::QueryExhausted;
use crate::legacy::pending_response_page::PendingResponsePage;
use crate::legacy::session::LegacyQuery;
use crate::results::PyPagingState;
use crate::utils::WithOriginalPyObject;

mod drainer;
mod rows;
mod state;
use drainer::Drainer;
use rows::{PageRows, Rows, build_rows, callback_value};
use state::RequestState;

/// A request for one page, boxed where it is built so the large future is copied once.
pub(crate) type LegacyRequestFuture<E> =
    Pin<Box<dyn Future<Output = Result<PendingRequestResult, E>> + Send>>;

/// Everything the one lock guards.
struct Shared {
    /// The request in flight or how it settled, and the last page received.
    request: RequestState,
    /// Dropped once no page can follow: a callback often holds the future
    /// itself, and this pyclass has no GC traversal to break that cycle.
    registered: Arc<Callbacks>,
    drainer: Drainer,
    /// Tracing id of every page received, in order.
    tracing_ids: Vec<Uuid>,
}

impl Shared {
    /// Whether the last page received says more follow.
    fn has_more_pages(&self) -> bool {
        self.request
            .last_page()
            .is_some_and(|page| page.pager().has_more_pages())
    }

    /// Whether nothing can fire again: the request settled and no page can be
    /// requested after the last one received.
    fn is_terminal(&self) -> bool {
        !self.request.is_pending() && !self.has_more_pages()
    }
}

/// The part of a `ResponseFuture` that its requests and its drainer share.
struct ResponseInner {
    /// Waiters are notified on every transition to `Ready`.
    shared: StateLock<Shared>,
}

impl ResponseInner {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            shared: StateLock::new(Shared {
                request: RequestState::pending(),
                registered: Arc::default(),
                drainer: Drainer::new(),
                tracing_ids: Vec::new(),
            }),
        })
    }

    /// Spawns a request whose page, once received, becomes the current one.
    fn spawn<E>(self: &Arc<Self>, request: LegacyRequestFuture<E>)
    where
        E: Into<PyErr> + 'static,
    {
        let this = Arc::clone(self);
        let request = request.map_ok(PendingResponsePage::from);

        spawn_guarded(catch_unwind(request), move |outcome| match outcome {
            Ok(outcome) => this.complete(outcome.and_then(|page| page.map_err(Into::into))),
            Err(TaskDropped) => this.complete_dropped(),
        });
    }

    /// The request finished on a runtime worker: settle without touching
    /// Python, and wake the drainer if anything is registered to fire.
    fn complete(self: &Arc<Self>, outcome: PyResult<PendingResponsePage>) {
        let waiters = {
            // A tokio worker: not attached.
            let mut shared = self.shared.lock_detached();

            if let Ok(page) = &outcome {
                shared.tracing_ids.extend(page.query_result().tracing_id());
            }

            // The rows of a received page can still fail to build, so either kind may fire.
            let delivery = outcome.as_ref().err().map(|_| Delivery::Errbacks);
            let unfired = shared.registered.for_outcome(delivery);
            let wake_drainer = unfired.is_some();
            shared.request.settle(outcome, unfired);

            // A callback often holds the future itself, and this pyclass has
            // no GC traversal to break that cycle. That is why we drop it here.
            if shared.is_terminal() {
                shared.registered.clear();
            }

            if wake_drainer {
                shared.drainer.wake(self);
            }
            shared.waiters()
        };

        self.shared.notify(waiters);
    }

    /// The request was torn down before completing, likely because the
    /// runtime shut down. Nothing will fire again, so the errbacks are consumed.
    fn complete_dropped(&self) {
        let (registered, waiters) = {
            // The task is dropped on a runtime thread, or during shutdown
            // inside py.detach: not attached.
            let mut shared = self.shared.lock_detached();
            if !shared.request.is_pending() {
                return;
            }

            shared.request.settle(Err(dropped_err()), None);
            (std::mem::take(&mut shared.registered), shared.waiters())
        };

        self.shared.notify(waiters);

        if !registered.is_empty_for(Delivery::Errbacks) {
            Python::attach(|py| {
                let err = dropped_err();
                registered.fire(py, Delivery::Errbacks, err.value(py).as_any());
            });
        }
    }

    /// Blocks, with the GIL released, until the request in flight settles;
    /// returns the rows of its page (a list or `None`) or raises its error.
    ///
    /// Another thread may call `start_fetching_next_page()` between the wakeup
    /// and the read, so the state can be `Pending` again here. Like the legacy
    /// driver, which does not support paging concurrently with `result()`, no
    /// attempt is made to return the page that was skipped: the loop waits for
    /// whichever page settles next.
    fn wait_rows(&self, py: Python<'_>) -> Rows {
        loop {
            let rows = self.shared.lock_py_attached(py).request.rows(py);
            if let Some(rows) = rows {
                return self.build_rows(py, rows);
            }

            self.shared
                .wait_while(py, None, |shared| shared.request.is_pending());
        }
    }

    /// The rows of a page, built if needed. The row factory is user Python that
    /// may touch the future, so no lock is held while it runs: two threads
    /// asking at once both build, and the first to finish stores its rows.
    fn build_rows(&self, py: Python<'_>, rows: PageRows) -> Rows {
        let (query_result, factory) = match rows {
            PageRows::Built(rows) => return rows,
            PageRows::ToBuild(query_result, factory) => (query_result, factory),
        };

        let built = build_rows(py, Arc::clone(&query_result), &factory);
        self.shared
            .lock_py_attached(py)
            .request
            .store_rows(py, &query_result, built)
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
        let rows = {
            let mut shared = self.shared.lock_py_attached(py);
            if !shared.is_terminal() {
                shared.registered.push(delivery, Arc::clone(&callback));
            }

            if let Some(unfired) = shared.request.unfired_mut() {
                unfired.push(delivery, callback);
                return;
            }

            shared.request.rows(py)
        };

        if let Some(rows) = rows {
            let rows = self.build_rows(py, rows);
            if Delivery::of(&rows) == delivery {
                callback.invoke(py, &callback_value(py, &rows));
            }
        }
    }
}

/// A request in flight, delivering its result synchronously via `result()` or
/// asynchronously via callbacks. Returned by `LegacySession.execute_async()`.
#[pyclass(module = "scylla.legacy", name = "ResponseFuture", frozen)]
pub(crate) struct PyResponseFuture {
    inner: Arc<ResponseInner>,
    /// The query as passed in, and what it was extracted to.
    query: WithOriginalPyObject<LegacyQuery>,
    timeout: Option<f64>,
    /// The row factory as the session passed it, `None` for the driver default.
    row_factory: Option<Py<PyAny>>,
    /// Result columns, identical for every page.
    columns: PyOnceLock<Py<PyTuple>>,
    column_names: PyOnceLock<Py<PyList>>,
    column_types: PyOnceLock<Py<PyList>>,
}

impl PyResponseFuture {
    /// Spawns `request` on the runtime, returning the future that delivers its result.
    pub(crate) fn spawn(
        py: Python<'_>,
        request: LegacyRequestFuture<DriverExecuteError>,
        query: WithOriginalPyObject<LegacyQuery>,
        timeout: Option<f64>,
        row_factory: Option<Py<PyAny>>,
    ) -> PyResult<Py<Self>> {
        let inner = ResponseInner::new();
        inner.spawn(request);

        Py::new(
            py,
            Self {
                inner,
                query,
                timeout,
                row_factory,
                columns: PyOnceLock::new(),
                column_names: PyOnceLock::new(),
                column_types: PyOnceLock::new(),
            },
        )
    }

    /// Blocks until the request in flight settles; returns the rows of its
    /// page (a list or `None`) or raises its error.
    pub(crate) fn wait_rows(&self, py: Python<'_>) -> Rows {
        self.inner.wait_rows(py)
    }

    /// The query being executed, as extracted.
    pub(crate) fn query_ref(&self) -> &LegacyQuery {
        &self.query.extracted
    }

    /// The request's row factory unless it is a built-in tuple, namedtuple or
    /// dict one; `None` also before a page arrived.
    pub(crate) fn custom_row_factory(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        let shared = self.inner.shared.lock_py_attached(py);
        match shared.request.last_page()?.factory() {
            PyRowFactory::NamedTuple | PyRowFactory::Dict | PyRowFactory::Tuple => None,
            PyRowFactory::Class(factory)
            | PyRowFactory::Deferred(factory)
            | PyRowFactory::Builder(factory) => Some(factory.clone_ref(py)),
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
            let shared = self.inner.shared.lock_py_attached(py);
            let Some(specs) = shared.request.last_page().and_then(|page| page.columns()) else {
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
        slf.get().inner.register(py, callback, Delivery::Callbacks);
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
        slf.get().inner.register(py, errback, Delivery::Errbacks);
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
    /// already settled still fires the callbacks it settled with — `clear_callbacks`
    /// cannot reach them, exactly as in the legacy driver, where `_set_final_result`
    /// copies the callback list under `_callback_lock` and invokes the copy after
    /// releasing it.
    fn clear_callbacks(&self, py: Python<'_>) {
        self.inner.shared.lock_py_attached(py).registered.clear();
    }

    /// Whether the last page received says more follow.
    #[getter]
    pub(crate) fn has_more_pages(&self, py: Python<'_>) -> bool {
        self.inner.shared.lock_py_attached(py).has_more_pages()
    }

    /// Starts fetching the next page; callbacks fire again when it arrives.
    /// Raises `QueryExhausted` if there is none.
    pub(crate) fn start_fetching_next_page(&self, py: Python<'_>) -> PyResult<()> {
        let (query_pager, row_factory) = {
            let mut shared = self.inner.shared.lock_py_attached(py);
            if shared.request.is_pending() {
                return Err(ScyllaError::new_err("a request is already in flight"));
            }
            let pager_and_factory = match shared.request.last_page() {
                Some(page) if page.pager().has_more_pages() => {
                    (page.pager().clone(), page.factory().clone())
                }
                _ => return Err(QueryExhausted::new_err("no more pages")),
            };

            if let Some(undelivered) = shared.request.switch_to_pending(py) {
                shared.drainer.queue(undelivered);
            }
            pager_and_factory
        };

        let next_page = async move {
            // Already on the runtime, as the future runs there.
            query_pager
                .fetch_next_pending_page(row_factory, TaskExecutionMode::Inline)
                .await?
                .ok_or_else(|| QueryExhausted::new_err("no more pages"))
        };

        self.inner.spawn(Box::pin(next_page));
        Ok(())
    }

    /// Paging state of the last page, `None` if the query is not paged or exhausted.
    #[getter]
    pub(crate) fn paging_state(&self, py: Python<'_>) -> Option<PyPagingState> {
        let shared = self.inner.shared.lock_py_attached(py);
        let page = shared.request.last_page()?;

        page.pager().paging_state().map(PyPagingState::from)
    }

    /// Specifications of the result columns, `None` until a page with rows arrived.
    #[getter]
    fn columns(&self, py: Python<'_>) -> PyResult<Option<Py<PyTuple>>> {
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

    /// Warnings the server attached to the last page, `None` if it sent none.
    /// Raises until the request settled.
    #[getter]
    fn warnings(&self, py: Python<'_>) -> PyResult<Option<Py<PyList>>> {
        let shared = self.inner.shared.lock_py_attached(py);
        if shared.request.is_pending() {
            return Err(ScyllaError::new_err(
                "warnings cannot be retrieved before ResponseFuture is finalized",
            ));
        }

        let Some(page) = shared.request.last_page() else {
            return Ok(None);
        };

        let mut warnings = page.query_result().warnings().peekable();
        if warnings.peek().is_none() {
            return Ok(None);
        }

        let warnings = warnings.map(|w| PyString::new(py, w));
        Ok(Some(PyList::new(py, warnings)?.unbind()))
    }

    /// Custom payload of the response; not exposed by the Rust driver, so always `None`.
    /// Raises until the request settled.
    #[getter]
    fn custom_payload(&self) -> PyResult<Option<Py<PyAny>>> {
        Err(PyNotImplementedError::new_err(
            "custom_payload t is not supported yet",
        ))
    }

    /// The statement being executed.
    #[getter]
    fn query(&self, py: Python<'_>) -> Py<PyAny> {
        self.query.original.clone_ref(py)
    }

    /// Client-side timeout of the request in seconds, `None` for no timeout.
    #[getter]
    fn timeout(&self) -> Option<f64> {
        self.timeout
    }

    /// Row factory of the request, `None` for the driver default.
    #[getter]
    fn row_factory(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.row_factory.as_ref().map(|f| f.clone_ref(py))
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

    /// Not exposed by the Rust driver.
    #[getter]
    fn is_schema_agreed(&self) -> PyResult<bool> {
        Err(PyNotImplementedError::new_err(
            "is_schema_agreed is not supported yet",
        ))
    }

    /// Tracing id of every page received so far, if tracing was enabled.
    fn get_query_trace_ids(&self, py: Python<'_>) -> Vec<Uuid> {
        self.inner.shared.lock_py_attached(py).tracing_ids.clone()
    }

    // TODO: implement with legacy tracing support (#243).
    #[pyo3(signature = (max_wait=None, query_cl=None))]
    fn get_query_trace(
        &self,
        max_wait: Option<Bound<'_, PyAny>>,
        query_cl: Option<Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let _ = (max_wait, query_cl);
        Err(PyNotImplementedError::new_err(
            "Query traces are not implemented. Use get_query_trace_ids().",
        ))
    }

    // TODO: implement with legacy tracing support (#243).
    #[pyo3(signature = (max_wait_per=None, query_cl=None))]
    fn get_all_query_traces(
        &self,
        max_wait_per: Option<Bound<'_, PyAny>>,
        query_cl: Option<Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let _ = (max_wait_per, query_cl);
        Err(PyNotImplementedError::new_err(
            "Query traces are not implemented. Use get_query_trace_ids().",
        ))
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let outcome = {
            let shared = self.inner.shared.lock_py_attached(py);
            if shared.request.is_pending() {
                "(no result yet)".to_string()
            } else {
                match shared.request.error(py) {
                    Some(err) => format!("exception={err}"),
                    None => "result".to_string(),
                }
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
