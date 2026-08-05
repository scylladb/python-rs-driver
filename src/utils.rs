use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::str::FromStr;

use pyo3::prelude::*;
use pyo3::{
    Borrowed, Bound, IntoPyObject, PyErr, PyResult, Python,
    types::{PyAnyMethods, PyModule, PyModuleMethods, PyString},
};

use crate::errors::AddressParseError;

#[derive(Clone)]
pub(crate) struct WithOriginalPyObject<T> {
    pub(crate) original: Py<PyAny>,
    pub(crate) extracted: T,
}

impl<'a, 'py, T> FromPyObject<'a, 'py> for WithOriginalPyObject<T>
where
    T: FromPyObject<'a, 'py>,
{
    type Error = <T as FromPyObject<'a, 'py>>::Error;

    fn extract(obj: Borrowed<'a, 'py, PyAny>) -> Result<Self, Self::Error> {
        let original = obj.to_owned().unbind();
        Ok(Self {
            extracted: obj.extract::<T>()?,
            original,
        })
    }
}

/// A parsed network address extracted from a Python object.
/// Can be either a resolved SocketAddr or an unresolved string.
#[derive(Clone, Debug)]
pub(crate) enum ParsedAddress {
    Resolved(SocketAddr),
    Unresolved(String),
}

impl TryFrom<ParsedAddress> for SocketAddr {
    type Error = AddressParseError;

    fn try_from(value: ParsedAddress) -> Result<Self, Self::Error> {
        match value {
            ParsedAddress::Resolved(addr) => Ok(addr),
            ParsedAddress::Unresolved(s) => {
                SocketAddr::from_str(&s).map_err(|source| AddressParseError::InvalidSocketAddr {
                    addr: s.clone(),
                    source,
                })
            }
        }
    }
}

impl<'py> FromPyObject<'_, 'py> for ParsedAddress {
    type Error = AddressParseError;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(s) = obj.extract::<String>() {
            return Ok(ParsedAddress::Unresolved(s));
        }

        if let Ok((host_str, port)) = obj.extract::<(&str, u16)>() {
            return if let Ok(ip) = IpAddr::from_str(host_str) {
                Ok(ParsedAddress::Resolved(SocketAddr::new(ip, port)))
            } else {
                Ok(ParsedAddress::Unresolved(format!("{host_str}:{port}")))
            };
        }

        if let Ok((host, port)) = obj.extract::<(IpAddr, u16)>() {
            return Ok(ParsedAddress::Resolved(SocketAddr::new(host, port)));
        }

        Err(AddressParseError::invalid_type(obj))
    }
}

impl<'py> IntoPyObject<'py> for ParsedAddress {
    type Target = PyString;
    type Output = Bound<'py, PyString>;
    type Error = std::convert::Infallible;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            ParsedAddress::Unresolved(host) => Ok(PyString::new(py, &host)),
            ParsedAddress::Resolved(addr) => Ok(PyString::new(py, &addr.to_string())),
        }
    }
}

impl ToSocketAddrs for ParsedAddress {
    type Iter = std::vec::IntoIter<SocketAddr>;

    fn to_socket_addrs(&self) -> std::io::Result<Self::Iter> {
        match self {
            ParsedAddress::Resolved(addr) => Ok(vec![*addr].into_iter()),
            ParsedAddress::Unresolved(host) => host.to_socket_addrs(),
        }
    }
}

/// A list of parsed addresses extracted from a Python object.
/// Accepts a single address or a sequence of addresses.
pub(crate) struct ParsedAddressList {
    pub(crate) inner: Vec<ParsedAddress>,
}

impl<'py> FromPyObject<'_, 'py> for ParsedAddressList {
    type Error = AddressParseError;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        // Try single address first
        if let Ok(addr) = obj.extract::<ParsedAddress>() {
            return Ok(ParsedAddressList { inner: vec![addr] });
        }

        // Try as a sequence
        if let Ok(seq) = obj.cast::<pyo3::types::PySequence>() {
            let iter = seq
                .try_iter()
                .map_err(AddressParseError::iteration_failed)?;

            let mut addrs = Vec::new();
            for (index, item_result) in iter.enumerate() {
                let item = item_result.map_err(|e| AddressParseError::invalid_item(index, e))?;
                let addr = item
                    .extract::<ParsedAddress>()
                    .map_err(|e| AddressParseError::invalid_item(index, e.into()))?;
                addrs.push(addr);
            }
            return Ok(ParsedAddressList { inner: addrs });
        }

        Err(AddressParseError::invalid_type(obj))
    }
}

pub(crate) struct PyValueOrError<T, E = PyErr> {
    result: Result<T, E>,
}

impl<T, E> PyValueOrError<T, E> {
    pub(crate) fn new(result: Result<T, E>) -> Self {
        PyValueOrError { result }
    }
}

impl<'py, T, E> IntoPyObject<'py> for PyValueOrError<T, E>
where
    T: IntoPyObject<'py>,
    E: Into<PyErr>,
{
    type Target = T::Target;
    type Output = T::Output;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self.result {
            Ok(value) => value.into_pyobject(py).map_err(|e| e.into()),
            Err(e) => Err(e.into()),
        }
    }
}

/// Add submodule.
///
/// This function is required,
/// because by default for native libs python
/// adds module as an attribute and
/// doesn't add it's submodules in list
/// of all available modules.
///
/// To surpass this issue, we
/// manually update `sys.modules` attribute,
/// adding all submodules.
///
/// It's important to register submodules with
/// parent's full name in order to allow for
/// nested imports. Namely registering submodules
/// inside other submodules.
///
/// # Errors
///
/// May result in an error, if
/// cannot construct modules, or add it,
/// or modify `sys.modules` attr.
pub(crate) fn add_submodule(
    py: Python<'_>,
    parent_mod: &Bound<'_, PyModule>,
    name: &'static str,
    module_constructor: impl FnOnce(Python<'_>, &Bound<'_, PyModule>) -> PyResult<()>,
) -> PyResult<()> {
    let full_name = format!("{}.{name}", parent_mod.name()?);
    let sub_module = PyModule::new(py, &full_name)?;
    module_constructor(py, &sub_module)?;
    parent_mod.add_submodule(&sub_module)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item(&full_name, sub_module)?;
    Ok(())
}
