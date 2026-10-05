//! The settled page of a `ResponseFuture`: its rows, the callbacks that fire
//! for it, and the request state, changed only through its transitions.
//! Leaving `Ready` hands back a page whose callbacks have not run, so it
//! cannot be dropped.

use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::{PyList, PyNone};
use scylla::response::query_result::QueryResult;

use crate::future::callbacks::LegacyCallback;

/// The rows of a settled page: a list, `None` for a result without rows, or the error.
pub(super) type Rows = PyResult<Option<Py<PyList>>>;

/// The outcome of a settled request, its rows built on first access.
pub(super) enum ReadyRows {
    /// The page as it arrived; its rows are not built yet.
    Unconverted(Arc<QueryResult>),
    Converted(Rows),
}

impl ReadyRows {
    pub(super) fn clone_ref(&self, py: Python<'_>) -> Self {
        match self {
            Self::Unconverted(query_result) => Self::Unconverted(Arc::clone(query_result)),
            Self::Converted(rows) => Self::Converted(clone_rows(py, rows)),
        }
    }
}

fn clone_rows(py: Python<'_>, rows: &Rows) -> Rows {
    match rows {
        Ok(rows) => Ok(rows.as_ref().map(|list| list.clone_ref(py))),
        Err(err) => Err(err.clone_ref(py)),
    }
}

/// What a callback receives for `rows`: the list, `None`, or the exception instance.
pub(super) fn callback_value<'py>(py: Python<'py>, rows: &Rows) -> Bound<'py, PyAny> {
    match rows {
        Ok(Some(list)) => list.bind(py).clone().into_any(),
        Ok(None) => PyNone::get(py).to_owned().into_any(),
        Err(err) => err.value(py).clone().into_any(),
    }
}

/// Which list a callback goes on, and which outcome it fires for.
#[derive(Clone, Copy, PartialEq)]
pub(super) enum Delivery {
    Callbacks,
    Errbacks,
}

impl Delivery {
    /// The list `result` is delivered to.
    pub(super) fn of<T>(result: &PyResult<T>) -> Self {
        match result {
            Ok(_) => Self::Callbacks,
            Err(_) => Self::Errbacks,
        }
    }
}

/// The callbacks and errbacks registered on a future, or the snapshot of them
/// one page fires. `Arc`, so a worker can snapshot them without the GIL.
#[derive(Clone, Default)]
pub(super) struct Callbacks {
    pub(super) callbacks: Vec<Arc<LegacyCallback>>,
    pub(super) errbacks: Vec<Arc<LegacyCallback>>,
}

impl Callbacks {
    fn list(&self, delivery: Delivery) -> &Vec<Arc<LegacyCallback>> {
        match delivery {
            Delivery::Callbacks => &self.callbacks,
            Delivery::Errbacks => &self.errbacks,
        }
    }

    pub(super) fn list_mut(&mut self, delivery: Delivery) -> &mut Vec<Arc<LegacyCallback>> {
        match delivery {
            Delivery::Callbacks => &mut self.callbacks,
            Delivery::Errbacks => &mut self.errbacks,
        }
    }

    fn is_empty(&self) -> bool {
        self.callbacks.is_empty() && self.errbacks.is_empty()
    }

    pub(super) fn clear(&mut self) {
        self.callbacks.clear();
        self.errbacks.clear();
    }

    /// What `ready` can fire, `None` if nothing. Rows not built yet may still
    /// fail to build, so both lists are kept for them.
    pub(super) fn snapshot_for(&self, ready: &ReadyRows) -> Option<Self> {
        let snapshot = match ready {
            ReadyRows::Unconverted(_) => self.clone(),
            ReadyRows::Converted(result) => {
                let delivery = Delivery::of(result);
                let mut snapshot = Self::default();
                snapshot.list_mut(delivery).clone_from(self.list(delivery));
                snapshot
            }
        };
        (!snapshot.is_empty()).then_some(snapshot)
    }

    /// Fires the list matching `rows`.
    pub(super) fn fire(&self, py: Python<'_>, rows: &Rows) {
        LegacyCallback::fire_all(py, self.list(Delivery::of(rows)), &callback_value(py, rows));
    }
}

pub(super) struct ResponseState(State);

enum State {
    /// A request is in flight.
    Pending,
    Ready(Settled),
}

struct Settled {
    rows: ReadyRows,
    /// Callbacks the drainer still has to run for this page; `None` once it
    /// took them, or if there were none to run.
    awaiting: Option<Callbacks>,
}

/// A settled page with the callbacks that have not run for it yet.
pub(super) struct Undelivered {
    pub(super) rows: ReadyRows,
    pub(super) to_fire: Callbacks,
}

impl ResponseState {
    pub(super) fn pending() -> Self {
        Self(State::Pending)
    }

    pub(super) fn is_pending(&self) -> bool {
        matches!(self.0, State::Pending)
    }

    /// `Pending` → `Ready`; `awaiting` are the callbacks the drainer runs for this page.
    pub(super) fn settle(&mut self, rows: ReadyRows, awaiting: Option<Callbacks>) {
        debug_assert!(self.is_pending(), "only a request in flight can settle");
        self.0 = State::Ready(Settled { rows, awaiting });
    }

    /// `Ready` → `Pending`. Hands back the page if its callbacks have not
    /// run, so it is queued for the drainer rather than dropped.
    #[must_use = "a page whose callbacks have not run must be queued for the drainer"]
    pub(super) fn start_next(&mut self) -> Option<Undelivered> {
        match std::mem::replace(&mut self.0, State::Pending) {
            State::Pending => None,
            State::Ready(Settled { rows, awaiting }) => {
                awaiting.map(|to_fire| Undelivered { rows, to_fire })
            }
        }
    }

    /// Rows of the settled request as they are, built or not; `None` while a
    /// request is in flight. Building happens with the lock released, see
    /// `ResponseInner::build`.
    pub(super) fn ready_rows(&self, py: Python<'_>) -> Option<ReadyRows> {
        match &self.0 {
            State::Pending => None,
            State::Ready(settled) => Some(settled.rows.clone_ref(py)),
        }
    }

    /// Stores `rows`, built from `query_result` with the lock released, if that
    /// page is still current and nobody stored theirs first. Returns the rows
    /// to hand out: a copy of the stored ones, else `rows` itself.
    pub(super) fn cache(
        &mut self,
        py: Python<'_>,
        query_result: &Arc<QueryResult>,
        rows: Rows,
    ) -> Rows {
        if let State::Ready(settled) = &mut self.0
            && let ReadyRows::Unconverted(current) = &settled.rows
            && Arc::ptr_eq(current, query_result)
        {
            let out = clone_rows(py, &rows);
            settled.rows = ReadyRows::Converted(rows);
            return out;
        }
        rows
    }

    /// Takes the current page's pending callbacks along with its rows as they
    /// are; `None` if none wait.
    pub(super) fn take_awaiting(&mut self, py: Python<'_>) -> Option<Undelivered> {
        match &mut self.0 {
            State::Pending => None,
            State::Ready(settled) => {
                let to_fire = settled.awaiting.take()?;
                let rows = settled.rows.clone_ref(py);
                Some(Undelivered { rows, to_fire })
            }
        }
    }

    /// The current page's pending callbacks, for a late registration to join.
    pub(super) fn awaiting_mut(&mut self) -> Option<&mut Callbacks> {
        match &mut self.0 {
            State::Ready(settled) => settled.awaiting.as_mut(),
            State::Pending => None,
        }
    }

    /// The error the request settled with, if it failed.
    pub(super) fn error(&self) -> Option<&PyErr> {
        match &self.0 {
            State::Ready(Settled {
                rows: ReadyRows::Converted(Err(err)),
                ..
            }) => Some(err),
            _ => None,
        }
    }
}
