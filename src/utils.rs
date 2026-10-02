use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::str::FromStr;
use std::time::Duration;

use pyo3::prelude::*;
use pyo3::{
    Borrowed, Bound, IntoPyObject, PyErr, PyResult, Python,
    types::{PyAnyMethods, PyModule, PyModuleMethods, PyString},
};

use crate::errors::{get_type_name, with_cause};

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

pub(crate) struct PyDuration(pub(crate) Duration);

impl<'py> FromPyObject<'_, 'py> for PyDuration {
    type Error = DurationParseError;
    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(duration) = obj.extract::<Duration>() {
            return Ok(PyDuration(duration));
        }

        if let Ok(secs) = obj.extract::<f64>() {
            let duration = Duration::try_from_secs_f64(secs)
                .map_err(|_| DurationParseError::invalid_type(obj))?;
            return Ok(PyDuration(duration));
        }

        Err(DurationParseError::invalid_type(obj))
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

/// `first` followed by `rest`, keeping the exact size `PyTuple::new` needs.
pub(crate) struct Prepended<I: ExactSizeIterator> {
    first: Option<I::Item>,
    rest: I,
}

impl<I: ExactSizeIterator> Prepended<I> {
    pub(crate) fn new(first: I::Item, rest: I) -> Self {
        Self {
            first: Some(first),
            rest,
        }
    }
}

impl<I: ExactSizeIterator> Iterator for Prepended<I> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.first.take().or_else(|| self.rest.next())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.rest.len() + usize::from(self.first.is_some());
        (len, Some(len))
    }
}

impl<I: ExactSizeIterator> ExactSizeIterator for Prepended<I> {}

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

/* Address parsing errors */

/// Error type for address parsing failures.
#[derive(Debug, thiserror::Error)]
pub enum AddressParseError {
    /// The Python object is not a valid address type (str, tuple(str, int), tuple(IpAddr, int)).
    #[error(
        "Invalid address type: expected str | tuple(str, int) | tuple(ipaddress, int) or a sequence of these, got {type_name}"
    )]
    InvalidType { type_name: String },
    /// A string could not be parsed into a SocketAddr.
    #[error("Invalid socket address '{addr}': {source}")]
    InvalidSocketAddr {
        addr: String,
        source: std::net::AddrParseError,
    },
    /// Failed to iterate over a sequence of addresses.
    #[error("Failed to iterate over sequence of addresses")]
    IterationFailed { source: Box<PyErr> },
    /// An individual item in an address sequence failed to extract at the given index.
    #[error("Error processing address at index {index}")]
    InvalidItem { index: usize, source: Box<PyErr> },
}

impl AddressParseError {
    pub(crate) fn invalid_type(obj: Borrowed<PyAny>) -> Self {
        Self::InvalidType {
            type_name: get_type_name(obj),
        }
    }

    pub(crate) fn iteration_failed(source: PyErr) -> Self {
        Self::IterationFailed {
            source: Box::new(source),
        }
    }

    pub(crate) fn invalid_item(index: usize, source: PyErr) -> Self {
        Self::InvalidItem {
            index,
            source: Box::new(source),
        }
    }
}

impl From<AddressParseError> for PyErr {
    fn from(e: AddressParseError) -> PyErr {
        let err = pyo3::exceptions::PyValueError::new_err(e.to_string());
        match e {
            AddressParseError::IterationFailed { source }
            | AddressParseError::InvalidItem { source, .. } => with_cause(err, *source),
            _ => err,
        }
    }
}

/* Duration parsing errors */

/// Error type for duration parsing failures.
#[derive(Debug, thiserror::Error)]
pub enum DurationParseError {
    /// The Python object is neither a `datetime.timedelta` nor a non-negative finite float.
    #[error(
        "Expected a datetime.timedelta or a non-negative finite float (seconds), got: {type_name}"
    )]
    InvalidType { type_name: String },
}

impl DurationParseError {
    pub(crate) fn invalid_type(obj: Borrowed<PyAny>) -> Self {
        Self::InvalidType {
            type_name: get_type_name(obj),
        }
    }
}

impl From<DurationParseError> for PyErr {
    fn from(e: DurationParseError) -> PyErr {
        pyo3::exceptions::PyValueError::new_err(e.to_string())
    }
}
