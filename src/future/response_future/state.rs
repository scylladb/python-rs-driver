//! The request of a `ResponseFuture` and the last page received, changed only
//! through its transitions. Each state owns the last page received, so exactly
//! one copy of it exists. Leaving a settled state hands back a page whose
//! callbacks have not run, so it cannot be dropped.

use std::sync::Arc;

use pyo3::prelude::*;
use scylla::response::query_result::QueryResult;

use super::rows::{PageRows, Rows, clone_rows};
use crate::future::callbacks::Callbacks;
use crate::future::panicked_err;
use crate::legacy::pending_response_page::PendingResponsePage;

/// The request in flight or how it settled, and the last page received.
pub(super) struct RequestState(State);

enum State {
    /// A request is in flight. `last_page` is the page received before it, if any.
    Pending {
        last_page: Option<PendingResponsePage>,
    },
    Received {
        page: PendingResponsePage,
        /// `None` until the rows are built.
        rows: Option<Rows>,
        /// Callbacks the drainer still has to run for this page; `None` once
        /// it took them, or if there were none to run.
        unfired: Option<Arc<Callbacks>>,
    },
    Failed {
        err: PyErr,
        /// The last page received before the error, kept as the legacy driver did.
        last_page: Option<PendingResponsePage>,
        unfired: Option<Arc<Callbacks>>,
    },
}

/// The rows of a settled page with the callbacks that have not run for it yet.
pub(super) struct Undelivered {
    pub(super) rows: PageRows,
    pub(super) unfired: Arc<Callbacks>,
}

impl RequestState {
    pub(super) fn pending() -> Self {
        Self(State::Pending { last_page: None })
    }

    pub(super) fn is_pending(&self) -> bool {
        matches!(self.0, State::Pending { .. })
    }

    /// The last page received: the source of paging state, columns and the next page.
    pub(super) fn last_page(&self) -> Option<&PendingResponsePage> {
        match &self.0 {
            State::Pending { last_page } | State::Failed { last_page, .. } => last_page.as_ref(),
            State::Received { page, .. } => Some(page),
        }
    }

    /// `Pending` → `Received` or `Failed`; `unfired` are the callbacks the
    /// drainer runs for this page. A failure keeps the last page received.
    pub(super) fn settle(
        &mut self,
        outcome: PyResult<PendingResponsePage>,
        unfired: Option<Arc<Callbacks>>,
    ) {
        debug_assert!(self.is_pending(), "only a request in flight can settle");
        self.0 = match outcome {
            Ok(page) => State::Received {
                page,
                rows: None,
                unfired,
            },
            Err(err) => State::Failed {
                err,
                last_page: self.take().into_last_page(),
                unfired,
            },
        };
    }

    /// `Received` or `Failed` → `Pending`. Hands back the page if its callbacks
    /// have not run, so it is queued for the drainer rather than dropped.
    #[must_use = "a page whose callbacks have not run must be queued for the drainer"]
    pub(super) fn switch_to_pending(&mut self, py: Python<'_>) -> Option<Undelivered> {
        let undelivered = self.take_unfired(py);
        self.0 = State::Pending {
            last_page: self.take().into_last_page(),
        };
        undelivered
    }

    /// The rows the request settled with; `None` while a request is in flight.
    pub(super) fn rows(&self, py: Python<'_>) -> Option<PageRows> {
        match &self.0 {
            State::Pending { .. } => None,
            State::Received { page, rows, .. } => Some(match rows {
                Some(rows) => PageRows::Built(clone_rows(py, rows)),
                None => PageRows::ToBuild(Arc::clone(page.query_result()), page.reader().clone()),
            }),
            State::Failed { err, .. } => Some(PageRows::Built(Err(err.clone_ref(py)))),
        }
    }

    /// Stores `built` as the rows of the page `query_result` belongs to, if it
    /// is still the current one and nobody stored rows first. Returns the
    /// stored rows, so every reader of a page gets the same list.
    pub(super) fn store_rows(
        &mut self,
        py: Python<'_>,
        query_result: &Arc<QueryResult>,
        built: Rows,
    ) -> Rows {
        match &mut self.0 {
            State::Received { page, rows, .. }
                if Arc::ptr_eq(page.query_result(), query_result) =>
            {
                clone_rows(py, rows.get_or_insert(built))
            }
            _ => built,
        }
    }

    /// The error the request failed with, or its row factory raised.
    pub(super) fn error(&self, py: Python<'_>) -> Option<PyErr> {
        match &self.0 {
            State::Failed { err, .. } => Some(err.clone_ref(py)),
            State::Received {
                rows: Some(Err(err)),
                ..
            } => Some(err.clone_ref(py)),
            _ => None,
        }
    }

    /// Takes the current page with its callbacks that have not run; `None` if none wait.
    pub(super) fn take_unfired(&mut self, py: Python<'_>) -> Option<Undelivered> {
        let unfired = match &mut self.0 {
            State::Received { unfired, .. } | State::Failed { unfired, .. } => unfired.take()?,
            State::Pending { .. } => return None,
        };
        let rows = self.rows(py)?;
        Some(Undelivered { rows, unfired })
    }

    /// The current page's callbacks that have not run, for a late registration to join.
    pub(super) fn unfired_mut(&mut self) -> Option<&mut Arc<Callbacks>> {
        match &mut self.0 {
            State::Received { unfired, .. } | State::Failed { unfired, .. } => unfired.as_mut(),
            State::Pending { .. } => None,
        }
    }

    /// Takes the state out and leaves `Failed` with a panic error, so a
    /// transition that panics before it stores the next state is reported.
    fn take(&mut self) -> State {
        let panicked = State::Failed {
            err: panicked_err(),
            last_page: None,
            unfired: None,
        };
        std::mem::replace(&mut self.0, panicked)
    }
}

impl State {
    /// The last page received, consuming the state.
    fn into_last_page(self) -> Option<PendingResponsePage> {
        match self {
            State::Pending { last_page } | State::Failed { last_page, .. } => last_page,
            State::Received { page, .. } => Some(page),
        }
    }
}
