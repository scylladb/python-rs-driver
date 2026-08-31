use pyo3::prelude::*;

use crate::utils::add_submodule;

pub mod address_translator;
pub mod authenticator_provider;
pub mod host_filter;
pub mod load_balancing;
pub mod retry;
pub mod speculative_execution;
pub mod timestamp_generator;

#[pymodule]
pub(crate) fn policies(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    add_submodule(
        py,
        module,
        "address_translator",
        address_translator::address_translator,
    )?;
    add_submodule(
        py,
        module,
        "authenticator_provider",
        authenticator_provider::authenticator_provider,
    )?;
    add_submodule(py, module, "host_filter", host_filter::host_filter)?;
    add_submodule(py, module, "load_balancing", load_balancing::load_balancing)?;
    add_submodule(
        py,
        module,
        "timestamp_generator",
        timestamp_generator::timestamp_generator,
    )?;
    add_submodule(py, module, "retry_policy", retry::retry_policy)?;
    add_submodule(
        py,
        module,
        "speculative_execution",
        speculative_execution::speculative_execution,
    )?;
    Ok(())
}
