//! The boxed driver future. It stashes its output rather than converting it, so the
//! Python conversion can be deferred to a thread that already holds the GIL.

use std::future::Future;
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

/// A [`PyFuture`] whose output is still typed, for a facade that consumes it as Rust.
pub(in crate::future) trait TypedFuture<T, E>: PyFuture {
    /// Consume the future, taking its stashed output. `None` if it never resolved.
    fn into_output(self: Pin<Box<Self>>) -> Option<Result<T, E>>;
}

/// The future the pyclass internals store.
pub(in crate::future) type PyBoxedFuture = Pin<Box<dyn PyFuture + Send>>;

/// A boxed driver future that remembers what it resolves to.
///
/// Boxed where it is built, so the large request future is copied once; every
/// hand-over after that moves a pointer.
pub(crate) struct BoxedFuture<T, E> {
    inner: Pin<Box<dyn TypedFuture<T, E> + Send>>,
}

impl<T, E> BoxedFuture<T, E> {
    /// Forget the output type; an upcast of the trait object, no allocation.
    pub(in crate::future) fn into_erased(self) -> PyBoxedFuture {
        self.inner
    }

    pub(in crate::future) fn as_erased_mut(&mut self) -> Pin<&mut (dyn PyFuture + Send)> {
        self.inner.as_mut()
    }

    pub(in crate::future) fn into_output(self) -> Option<Result<T, E>> {
        self.inner.into_output()
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

impl<Fut, T, E> TypedFuture<T, E> for StashingFuture<Fut, T, E>
where
    Fut: Future<Output = Result<T, E>>,
    T: for<'py> IntoPyObject<'py>,
    E: Into<PyErr>,
{
    fn into_output(mut self: Pin<Box<Self>>) -> Option<Result<T, E>> {
        self.as_mut().drain()
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
    }
}
