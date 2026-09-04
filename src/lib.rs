use std::future::Future;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

#[cfg(test)]
mod tests;

use crate::deserialize::value;
use deserialize::results;
use pyo3::prelude::*;
use pyo3::sync::OnceExt;
use pyo3::wrap_pyfunction;
use std::sync::Once;
use tokio::runtime::{Handle, Runtime};
use tokio::task::JoinHandle;

mod batch;
mod cache;
mod cluster;
mod core;
mod deserialize;
mod enums;
mod errors;
mod execution_profile;
mod policies;
mod routing;
mod serialize;
mod session;
mod session_builder;
mod statement;
mod tls;
mod types;
mod utils;

use crate::utils::add_submodule;

/// How long the atexit hook waits for outstanding tokio work to finish
/// before giving up and letting the interpreter finalize anyway.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

/// The driver's tokio runtime.
///
/// `spawn`/`spawn_blocking` go through `handle`, a cheap clone that never
/// needs to touch the mutex. `runtime` holds the only owner of the
/// `Runtime` itself, so it can be taken and dropped from the atexit hook.
pub(crate) struct DriverRuntime {
    handle: Handle,
    runtime: Mutex<Option<Runtime>>,
}

pub(crate) static RUNTIME: LazyLock<DriverRuntime> = LazyLock::new(|| {
    let runtime = Runtime::new().unwrap();
    DriverRuntime {
        handle: runtime.handle().clone(),
        runtime: Mutex::new(Some(runtime)),
    }
});

impl DriverRuntime {
    pub(crate) fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.handle.spawn(future)
    }

    #[allow(dead_code)]
    pub(crate) fn spawn_blocking<F, R>(&self, f: F) -> JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.handle.spawn_blocking(f)
    }

    /// Drain the runtime. Called from the atexit hook on the main thread,
    /// with the GIL released: any worker stuck inside `Python::attach`
    /// needs it back to unwind.
    ///
    /// A no-op on a second call
    fn shutdown(&self, py: Python<'_>, timeout: Duration) {
        let Some(runtime) = self.runtime.lock().unwrap().take() else {
            return;
        };

        let start = Instant::now();
        py.detach(|| runtime.shutdown_timeout(timeout));

        // `shutdown_timeout` doesn't report whether it drained cleanly or
        // hit the wall, so infer it from how long it actually took.
        if start.elapsed() >= timeout {
            eprintln!(
                "scylla driver: runtime shutdown timed out after {timeout:?}; \
                 a blocking callback or background task is still running. \
                 The process may hang or abort on exit."
            );
        }
    }
}

#[pyfunction]
fn _shutdown_runtime(py: Python<'_>) {
    RUNTIME.shutdown(py, SHUTDOWN_TIMEOUT);
}

static INIT_LOG: Once = Once::new();

fn init_logging(py: Python<'_>) {
    INIT_LOG.call_once_py_attached(py, || {
        if let Err(e) = pyo3_log::try_init() {
            eprintln!("pyo3_log::try_init failed: {:?}", e);
        }
    });
}

/// A Python module implemented in Rust.
#[pymodule]
#[pyo3(name = "_rust")]
fn scylla(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    init_logging(py);

    py.import("atexit")?
        .call_method1("register", (wrap_pyfunction!(_shutdown_runtime, module)?,))?;

    add_submodule(
        py,
        module,
        "session_builder",
        session_builder::session_builder,
    )?;
    add_submodule(py, module, "session", session::session)?;
    add_submodule(py, module, "results", results::results)?;
    add_submodule(py, module, "statement", statement::statement)?;
    add_submodule(py, module, "enums", enums::enums)?;
    add_submodule(py, module, "errors", errors::errors)?;
    add_submodule(
        py,
        module,
        "execution_profile",
        execution_profile::execution_profile,
    )?;
    add_submodule(py, module, "types", types::types)?;
    add_submodule(py, module, "value", value::value)?;
    add_submodule(py, module, "batch", batch::batch)?;
    add_submodule(py, module, "policies", policies::policies)?;
    add_submodule(py, module, "cluster", cluster::cluster)?;
    add_submodule(py, module, "routing", routing::routing)?;
    add_submodule(py, module, "tls", tls::tls)?;
    Ok(())
}
