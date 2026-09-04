//! Typed handle over a spawned [`PyDriverFuture`].

use std::convert::Infallible;
use std::marker::PhantomData;

use pyo3::prelude::*;
use pyo3::{IntoPyObject, Py, PyAny, PyErr, PyResult, Python};

use crate::future::PyDriverFuture;
use crate::future::boxed_future::BoxedFuture;

/// A spawned driver future that remembers what it resolves to.
pub(crate) struct DriverFuture<T, E> {
    inner: Py<PyDriverFuture>,
    _output: PhantomData<fn() -> Result<T, E>>,
}

impl<T, E> DriverFuture<T, E> {
    fn new(inner: Py<PyDriverFuture>) -> Self {
        Self {
            inner,
            _output: PhantomData,
        }
    }

    /// Let the asyncio event loop drive the future on first poll.
    pub(crate) fn spawn(py: Python<'_>, future: BoxedFuture<T, E>) -> PyResult<Self> {
        PyDriverFuture::spawn(py, future.into_erased()).map(Self::new)
    }

    /// Spawn the future on the tokio runtime immediately.
    pub(crate) fn spawn_on_tokio(py: Python<'_>, future: BoxedFuture<T, E>) -> PyResult<Self> {
        PyDriverFuture::spawn_on_tokio(py, future.into_erased()).map(Self::new)
    }
}

impl DriverFuture<Py<PyAny>, PyErr> {
    /// An already-resolved future, for a result available synchronously.
    pub(crate) fn ready(py: Python<'_>, result: PyResult<Py<PyAny>>) -> PyResult<Self> {
        PyDriverFuture::ready(py, result).map(Self::new)
    }
}

impl<'py, T, E> IntoPyObject<'py> for DriverFuture<T, E> {
    type Target = PyDriverFuture;
    type Output = Bound<'py, PyDriverFuture>;
    type Error = Infallible;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(self.inner.into_bound(py))
    }
}
