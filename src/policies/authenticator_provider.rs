use crate::errors::config::DriverSessionConfigError;
use async_trait::async_trait;
use pyo3::exceptions::PyNotImplementedError;
use pyo3::prelude::{PyAnyMethods, PyModule, PyModuleMethods};
use pyo3::types::{PyDict, PyTuple};
use pyo3::{
    Borrowed, Bound, FromPyObject, Py, PyAny, PyResult, Python, intern, pyclass, pymethods,
    pymodule,
};
use scylla::authentication::{AuthError, AuthenticatorProvider, AuthenticatorSession};
use std::sync::Arc;

// TODO: Split this file into separate parts and move them to the `policies` directory.

/// Python-side base class that users subclass to provide a custom authenticator provider.
/// Exposed to Python as `AuthenticatorProvider`.
#[pyclass(
    module = "scylla.auth",
    subclass,
    name = "AuthenticatorProvider",
    frozen
)]
pub(crate) struct PyAuthenticatorProviderClass {}

#[pymethods]
impl PyAuthenticatorProviderClass {
    #[expect(unused_variables)]
    #[new]
    #[pyo3(signature = (*args, **kwargs))]
    pub fn new(args: &Bound<'_, PyTuple>, kwargs: Option<&Bound<'_, PyDict>>) -> Self {
        PyAuthenticatorProviderClass {}
    }

    fn new_authenticator(&self, _authenticator_name: &str) -> PyResult<Py<PyAuthenticatorClass>> {
        Err(PyNotImplementedError::new_err("Method is not implemented"))
    }
}

/// Holds a Python object (subclass of `AuthenticatorProvider`) and implements the Rust
/// `AuthenticatorProvider` trait by delegating to the Python object's methods.
struct CustomAuthenticatorProvider {
    python_authenticator: Py<PyAuthenticatorProviderClass>,
}

#[async_trait]
impl AuthenticatorProvider for CustomAuthenticatorProvider {
    async fn start_authentication_session(
        &self,
        authenticator_name: &str,
    ) -> Result<(Option<Vec<u8>>, Box<dyn AuthenticatorSession>), AuthError> {
        let (result, py_auth) = Python::attach(
            |py| -> PyResult<(Option<Vec<u8>>, Box<CustomAuthenticator>)> {
                let py_auth_provider = self.python_authenticator.bind(py);

                let py_auth_any = py_auth_provider
                    .call_method1(intern!(py, "new_authenticator"), (authenticator_name,))?;

                let py_auth = py_auth_any.cast::<PyAuthenticatorClass>()?;

                let response = py_auth
                    .call_method0(intern!(py, "initial_response"))?
                    .extract::<Option<Vec<u8>>>()?;

                Ok((
                    response,
                    Box::new(CustomAuthenticator {
                        python_authenticator: py_auth.to_owned().unbind(),
                    }),
                ))
            },
        )
        .map_err(|e| format!("Python new_authenticator failed: {:?}", e))?;

        Ok((result, py_auth))
    }
}

/// Python-side base class that users subclass to implement authentication logic for a single session.
/// Exposed to Python as `Authenticator`.
#[pyclass(module = "scylla.auth", subclass, name = "Authenticator", frozen)]
pub(crate) struct PyAuthenticatorClass {}

#[pymethods]
impl PyAuthenticatorClass {
    #[expect(unused_variables)]
    #[new]
    #[pyo3(signature = (*args, **kwargs))]
    pub fn new(args: &Bound<'_, PyTuple>, kwargs: Option<&Bound<'_, PyDict>>) -> Self {
        PyAuthenticatorClass {}
    }

    fn initial_response(&self) -> PyResult<Option<Vec<u8>>> {
        Ok(None)
    }

    fn evaluate_challenge(&self, _challenge: Option<&[u8]>) -> PyResult<Option<Vec<u8>>> {
        Err(PyNotImplementedError::new_err("Method is not implemented"))
    }

    fn success(&self, _token: Option<&[u8]>) -> PyResult<()> {
        Ok(())
    }
}

/// Holds a Python object (subclass of `Authenticator`) and implements the Rust
/// `AuthenticatorSession` trait by delegating to the Python object's methods.
struct CustomAuthenticator {
    python_authenticator: Py<PyAuthenticatorClass>,
}

#[async_trait]
impl AuthenticatorSession for CustomAuthenticator {
    async fn evaluate_challenge(
        &mut self,
        token: Option<&[u8]>,
    ) -> Result<Option<Vec<u8>>, AuthError> {
        let result = Python::attach(|py| -> PyResult<Option<Vec<u8>>> {
            let py_auth = self.python_authenticator.bind(py);

            py_auth
                .call_method1(intern!(py, "evaluate_challenge"), (token,))?
                .extract::<Option<Vec<u8>>>()
        })
        .map_err(|e| format!("Python evaluate_challenge failed: {:?}", e))?;

        Ok(result)
    }

    async fn success(&mut self, token: Option<&[u8]>) -> Result<(), AuthError> {
        let result = Python::attach(|py| -> PyResult<()> {
            let py_auth = self.python_authenticator.bind(py);

            py_auth.call_method1(intern!(py, "success"), (token,))?;

            Ok(())
        })
        .map_err(|e| format!("Python success failed: {:?}", e))?;

        Ok(result)
    }
}

/// Python-facing input type that extracts an `Arc<dyn AuthenticatorProvider>` from a Python object.
pub(crate) struct PyAuthenticatorProvider {
    inner: Arc<dyn AuthenticatorProvider>,
}

impl PyAuthenticatorProvider {
    pub(crate) fn into_inner(self) -> Arc<dyn AuthenticatorProvider> {
        self.inner
    }
}

impl<'py> FromPyObject<'_, 'py> for PyAuthenticatorProvider {
    type Error = DriverSessionConfigError;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(python_authenticator) = obj.extract::<Py<PyAuthenticatorProviderClass>>() {
            return Ok(Self {
                inner: Arc::new(CustomAuthenticatorProvider {
                    python_authenticator,
                }),
            });
        }

        Err(DriverSessionConfigError::invalid_authenticator_provider(
            obj,
        ))
    }
}

#[pymodule]
pub(crate) fn authenticator_provider(
    _py: Python<'_>,
    module: &Bound<'_, PyModule>,
) -> PyResult<()> {
    module.add_class::<PyAuthenticatorProviderClass>()?;
    module.add_class::<PyAuthenticatorClass>()?;
    Ok(())
}
