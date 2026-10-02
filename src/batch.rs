// TODO: drop once PyO3 ships PyO3/pyo3#6309 (from_py_object clones Copy types)
#![allow(clippy::clone_on_copy)]

use crate::core::session::ExecutableStatement;
use crate::errors::BatchError;
use crate::policies::load_balancing::PyTargetPolicy;
use crate::serialize::value_list::PyValueList;
use crate::statement::{PyStatementSettings, StatementOptions, statement_pymethods};
use pyo3::prelude::*;
use pyo3::sync::MutexExt;
use scylla::statement::batch::{Batch, BatchType};
use std::sync::Mutex;

#[pyclass(
    module = "scylla.statement",
    name = "BatchType",
    from_py_object,
    eq,
    eq_int,
    frozen
)]
#[derive(Clone, Copy, PartialEq)]
pub(crate) enum PyBatchType {
    Logged,
    Unlogged,
    Counter,
}

impl From<PyBatchType> for BatchType {
    fn from(value: PyBatchType) -> Self {
        match value {
            PyBatchType::Logged => Self::Logged,
            PyBatchType::Unlogged => Self::Unlogged,
            PyBatchType::Counter => Self::Counter,
        }
    }
}

impl From<BatchType> for PyBatchType {
    fn from(value: BatchType) -> Self {
        match value {
            BatchType::Logged => Self::Logged,
            BatchType::Unlogged => Self::Unlogged,
            BatchType::Counter => Self::Counter,
        }
    }
}

/// A batch's statements, their values and its configuration.
#[derive(Clone)]
pub(crate) struct BatchState {
    pub(crate) options: StatementOptions<Batch>,
    pub(crate) values: Vec<PyValueList>,
}

impl BatchState {
    /// Pins this batch to a single target for one execution.
    pub(crate) fn set_target(&mut self, target: PyTargetPolicy) {
        self.options
            .inner
            .set_load_balancing_policy(Some(target.into_inner()));
    }
}

#[pyclass(module = "scylla.statement", name = "Batch", frozen)]
pub(crate) struct PyBatch {
    state: Mutex<BatchState>,
}

impl PyBatch {
    fn with_state<R>(&self, py: Python<'_>, f: impl FnOnce(&mut BatchState) -> R) -> R {
        f(&mut self.state.lock_py_attached(py).unwrap())
    }

    fn with_options<R>(
        &self,
        py: Python<'_>,
        f: impl FnOnce(&mut StatementOptions<Batch>) -> R,
    ) -> R {
        self.with_state(py, |s| f(&mut s.options))
    }

    /// A snapshot of the batch with its current statements and configuration.
    pub(crate) fn snapshot(&self, py: Python<'_>) -> BatchState {
        self.with_state(py, |s| s.clone())
    }
}

statement_pymethods!(PyBatch, DriverBatchError, {
    #[new]
    #[pyo3(signature = (batch_type=PyBatchType::Logged))]
    fn py_new(batch_type: PyBatchType) -> Self {
        let options = StatementOptions::new(
            Batch::new(batch_type.into()),
            false,
            PyStatementSettings::default(),
        );
        Self {
            state: Mutex::new(BatchState {
                options,
                values: vec![],
            }),
        }
    }

    #[pyo3(signature = (statement, values=None))]
    fn add(&self, py: Python<'_>, statement: ExecutableStatement, values: Option<PyValueList>) {
        self.with_state(py, |s| {
            s.options.inner.append_statement(statement);
            s.values.push(values.unwrap_or(PyValueList::Empty));
        });
    }

    fn add_all(&self, py: Python<'_>, items: Vec<(ExecutableStatement, Option<PyValueList>)>) {
        self.with_state(py, |s| {
            s.values.reserve_exact(items.len());
            for (statement, values) in items {
                s.options.inner.append_statement(statement);
                s.values.push(values.unwrap_or(PyValueList::Empty));
            }
        });
    }

    #[getter]
    fn get_type(&self, py: Python<'_>) -> PyBatchType {
        self.with_options(py, |o| o.inner.get_type().into())
    }
});

#[pymodule]
pub(crate) fn batch(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyBatch>()?;
    module.add_class::<PyBatchType>()?;
    Ok(())
}

/// Errors related to batch execution and batch statement configuration.
#[derive(Debug, thiserror::Error)]
#[must_use]
pub enum DriverBatchError {
    /// The provided request timeout is not a non-negative finite number of seconds.
    #[error("timeout must be a non-negative, finite number (in seconds), got {value}")]
    InvalidRequestTimeout { value: f64 },
}

impl DriverBatchError {
    /* Constructors */

    pub(crate) fn invalid_request_timeout(value: f64) -> Self {
        Self::InvalidRequestTimeout { value }
    }
}

impl From<DriverBatchError> for PyErr {
    fn from(e: DriverBatchError) -> PyErr {
        BatchError::new_err(e.to_string())
    }
}
