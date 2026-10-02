//! Spawning futures on the runtime with a guaranteed completion signal.

use std::future::Future;

use tokio::task::AbortHandle;

use crate::RUNTIME;

/// Spawns `future` on the runtime and hands its output to `on_complete`.
///
/// If the task is torn down first — aborted, the runtime shutting down, or a panic
/// escaping `on_complete` — `on_dropped` runs instead, so whoever waits on the
/// outcome is always told.
pub(in crate::future) fn spawn_guarded<F, C, D>(
    future: F,
    on_complete: C,
    on_dropped: D,
) -> AbortHandle
where
    F: Future + Send + 'static,
    C: FnOnce(F::Output) + Send + 'static,
    D: FnOnce() + Send + 'static,
{
    RUNTIME
        .spawn(async move {
            let guard = DropGuard(Some(on_dropped));
            on_complete(future.await);
            guard.disarm();
        })
        .abort_handle()
}

/// Runs its callback when dropped, unless disarmed first.
struct DropGuard<D: FnOnce()>(Option<D>);

impl<D: FnOnce()> DropGuard<D> {
    fn disarm(mut self) {
        self.0.take();
    }
}

impl<D: FnOnce()> Drop for DropGuard<D> {
    fn drop(&mut self) {
        if let Some(on_dropped) = self.0.take() {
            on_dropped();
        }
    }
}
