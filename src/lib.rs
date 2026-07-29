use std::sync::LazyLock;

#[cfg(test)]
mod tests;

use crate::deserialize::value;
use deserialize::results;
use pyo3::prelude::*;
use pyo3::sync::OnceExt;
use std::sync::Once;
use tokio::runtime::Runtime;

mod batch;
mod cache;
mod cluster;
mod coroutine;
mod deserialize;
mod enums;
mod errors;
mod execution_profile;
mod future;
mod policies;
mod routing;
mod serialize;
mod session;
mod session_builder;
mod statement;
mod types;
mod utils;

use crate::utils::add_submodule;

pub static RUNTIME: LazyLock<Runtime> = LazyLock::new(|| Runtime::new().unwrap());

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
    add_submodule(py, module, "future", future::future)?;
    Ok(())
}
