//! Driver-facing core types.
//!
//! These types hold the functionality shared by every Python-facing session:
//! the asynchronous one and, later, the legacy (`cassandra-driver` compatible)
//! one. Each core method performs a single task and returns the future that
//! carries it out; how that future is driven is deliberately not its concern.
//! A facade may await it directly, spawn it onto the Tokio runtime behind a
//! `DriverFuture`, or park it in a callback-driven `ResponseFuture` — the core
//! is unchanged either way.

pub(crate) mod results;
pub(crate) mod session;
pub(crate) mod traces;
