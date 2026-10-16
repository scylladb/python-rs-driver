use std::sync::{Arc, Mutex};

use futures::stream::{self, StreamExt, TryStreamExt};
use scylla::client::session::Session;
use scylla::observability::tracing::TracingInfo;
use uuid::Uuid;

use crate::errors::execution::DriverTraceError;

/// The tracing ids of every request made for one query, in the order the
/// responses arrived.
///
/// Shared by all pagers of the query, so every page fetched for the query
/// adds its id to the same list.
#[derive(Clone)]
pub(crate) enum TraceLog {
    /// The query is not traced, so there are no ids to record.
    Untraced,
    Traced {
        ids: Arc<Mutex<Vec<Uuid>>>,
    },
}

impl TraceLog {
    /// Every page of a query is traced alike, so the first page decides.
    pub(crate) fn new(first_page_id: Option<Uuid>) -> Self {
        match first_page_id {
            Some(id) => Self::Traced {
                ids: Arc::new(Mutex::new(vec![id])),
            },
            None => Self::Untraced,
        }
    }

    pub(crate) fn record(&self, id: Option<Uuid>) {
        let (Self::Traced { ids }, Some(id)) = (self, id) else {
            return;
        };
        #[expect(
            clippy::disallowed_methods,
            reason = "holders only push or copy Uuids, so they release the lock without needing the GIL"
        )]
        ids.lock().unwrap().push(id);
    }

    pub(crate) fn ids(&self) -> Vec<Uuid> {
        match self {
            Self::Untraced => Vec::new(),
            #[expect(
                clippy::disallowed_methods,
                reason = "holders only push or copy Uuids, so they release the lock without needing the GIL"
            )]
            Self::Traced { ids } => ids.lock().unwrap().clone(),
        }
    }
}

/// A fetched trace that becomes a [`QueryTrace`] when handed to Python.
pub(crate) struct PendingQueryTrace {
    pub(crate) trace_id: Uuid,
    pub(crate) info: TracingInfo,
}

/// Fetches the trace of the request with `trace_id`.
pub(crate) async fn fetch_trace(
    session: Arc<Session>,
    trace_id: Uuid,
) -> Result<PendingQueryTrace, DriverTraceError> {
    let info = session
        .get_tracing_info(&trace_id)
        .await
        .map_err(DriverTraceError::rust_driver_tracing_error)?;

    Ok(PendingQueryTrace { trace_id, info })
}

/// The maximum number of traces that [`fetch_traces`] fetches at the same time.
const MAX_CONCURRENT_TRACE_FETCHES: usize = 16;

/// Fetches the traces of `trace_ids` concurrently, and returns them in the same order.
pub(crate) async fn fetch_traces(
    session: Arc<Session>,
    trace_ids: Vec<Uuid>,
) -> Result<Vec<PendingQueryTrace>, DriverTraceError> {
    stream::iter(trace_ids)
        .map(|id| fetch_trace(Arc::clone(&session), id))
        .buffered(MAX_CONCURRENT_TRACE_FETCHES)
        .try_collect()
        .await
}
