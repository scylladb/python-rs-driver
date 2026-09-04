//! The boxed driver future. It stashes its output rather than converting it, so the
//! Python conversion can be deferred to a thread that already holds the GIL.

use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::future::panicked_err;
use pin_project_lite::pin_project;
use pyo3::prelude::*;
use pyo3::{BoundObject, Py, PyAny, PyErr, PyResult, Python};

/// Drives a driver future and converts its result.
pub(in crate::future) trait PyFuture {
    /// Poll the inner future, stashing its output. Fused once stashed.
    fn poll_stash(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()>;

    /// Consume the future, converting its stashed output.
    fn into_py_result(self: Pin<Box<Self>>, py: Python<'_>) -> PyResult<Py<PyAny>>;
}

/// The future the pyclass internals store.
pub(in crate::future) type PyBoxedFuture = Pin<Box<dyn PyFuture + Send>>;

/// A [`PyBoxedFuture`] that remembers what it resolves to.
pub(crate) struct BoxedFuture<T, E> {
    inner: PyBoxedFuture,
    _output: PhantomData<fn() -> Result<T, E>>,
}

impl<T, E> BoxedFuture<T, E> {
    pub(in crate::future) fn into_erased(self) -> PyBoxedFuture {
        self.inner
    }
}

/// A finished future's result, still awaiting its Python conversion.
pub(in crate::future) enum ResolvedResult {
    /// Resolved; the output is stashed inside this future.
    Stashed(PyBoxedFuture),
    /// Panicked, or an exception was thrown in.
    Err(PyErr),
}

impl ResolvedResult {
    pub(in crate::future) fn into_py_result(self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self {
            ResolvedResult::Stashed(future) => future.into_py_result(py),
            ResolvedResult::Err(err) => Err(err),
        }
    }
}

pin_project! {
    /// A request future plus the slot its output is stashed in. `future` is the
    /// structurally pinned field.
    struct StashingFuture<Fut, T, E> {
        #[pin]
        future: Fut,
        output: Option<Result<T, E>>,
    }
}

impl<Fut, T, E> StashingFuture<Fut, T, E> {
    fn drain(self: Pin<&mut Self>) -> Option<Result<T, E>> {
        self.project().output.take()
    }
}

impl<Fut, T, E> PyFuture for StashingFuture<Fut, T, E>
where
    Fut: Future<Output = Result<T, E>>,
    T: for<'py> IntoPyObject<'py>,
    E: Into<PyErr>,
{
    fn poll_stash(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.project();

        // Fused, if is polled again return already stored result.
        if this.output.is_some() {
            return Poll::Ready(());
        }

        match this.future.poll(cx) {
            Poll::Ready(output) => {
                *this.output = Some(output);
                Poll::Ready(())
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn into_py_result(mut self: Pin<Box<Self>>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        // `None` means a future that did not resolved. We should throw panic exception in such a situation.
        let Some(output) = self.as_mut().drain() else {
            return Err(panicked_err());
        };
        output.map_err(Into::into).and_then(|value| {
            value
                .into_pyobject(py)
                .map(|bound| bound.into_any().unbind())
                .map_err(Into::into)
        })
    }
}

/// Box `future` at its construction site, deferring the Python conversion of its
/// output to a slot in the same allocation.
pub(crate) fn boxed_py_future<Fut, T, E>(future: Fut) -> BoxedFuture<T, E>
where
    Fut: Future<Output = Result<T, E>> + Send + 'static,
    T: for<'py> IntoPyObject<'py> + Send + 'static,
    E: Into<PyErr> + Send + 'static,
{
    BoxedFuture {
        inner: Box::pin(StashingFuture {
            future,
            output: None,
        }),
        _output: PhantomData,
    }
}
