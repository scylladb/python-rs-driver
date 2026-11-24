//! Maps why the cluster metadata that the driver fetched is not valid.
// TODO: drop once PyO3 ships PyO3/pyo3#6309 (from_py_object clones Copy types)
#![allow(clippy::clone_on_copy)]

use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;
use scylla::errors::{
    ClientRoutesMetadataError, KeyspacesMetadataError, PeersMetadataError, TablesMetadataError,
    UdtMetadataError,
};

use crate::errors::{InvalidClusterMetadata, ToPyAttr};

/// Why the cluster metadata is not valid.
#[pyclass(
    module = "scylla.errors",
    name = "InvalidClusterMetadataKind",
    frozen,
    from_py_object,
    eq,
    eq_int
)]
#[derive(Debug, Copy, Clone, PartialEq)]
#[non_exhaustive]
pub enum PyInvalidClusterMetadataKind {
    EmptyPeers,
    EmptyTokenLists,
    InvalidKeyspaceStrategy,
    InvalidUdtFieldType,
    CircularUdtDependency,
    InvalidColumnType,
    UnknownColumnKind,
    InvalidClientRoutesPort,
}

impl ToPyAttr for PyInvalidClusterMetadataKind {
    fn to_py_attr<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        (*self).into_bound_py_any(py)
    }
}

fn invalid_metadata(
    py: Python<'_>,
    message: String,
    kind: PyInvalidClusterMetadataKind,
    keyspace: Option<&str>,
    table: Option<&str>,
) -> PyErr {
    py_err!(py, InvalidClusterMetadata, message; kind, keyspace, table)
}

#[deny(clippy::wildcard_enum_match_arm)]
pub(crate) fn peers_error_to_pyerr(
    py: Python<'_>,
    err: &PeersMetadataError,
    message: String,
) -> PyErr {
    let kind = match err {
        PeersMetadataError::EmptyPeers => PyInvalidClusterMetadataKind::EmptyPeers,
        PeersMetadataError::EmptyTokenLists => PyInvalidClusterMetadataKind::EmptyTokenLists,
        _ => unreachable!("clippy testifies that the match is exhaustive"),
    };
    invalid_metadata(py, message, kind, None, None)
}

#[deny(clippy::wildcard_enum_match_arm)]
pub(crate) fn keyspaces_error_to_pyerr(
    py: Python<'_>,
    err: &KeyspacesMetadataError,
    message: String,
) -> PyErr {
    match err {
        KeyspacesMetadataError::Strategy { keyspace, .. } => {
            let kind = PyInvalidClusterMetadataKind::InvalidKeyspaceStrategy;
            invalid_metadata(py, message, kind, Some(keyspace), None)
        }
        _ => unreachable!("clippy testifies that the match is exhaustive"),
    }
}

#[deny(clippy::wildcard_enum_match_arm)]
pub(crate) fn udt_error_to_pyerr(py: Python<'_>, err: &UdtMetadataError, message: String) -> PyErr {
    let kind = match err {
        UdtMetadataError::InvalidCqlType { .. } => {
            PyInvalidClusterMetadataKind::InvalidUdtFieldType
        }
        UdtMetadataError::CircularTypeDependency => {
            PyInvalidClusterMetadataKind::CircularUdtDependency
        }
        _ => unreachable!("clippy testifies that the match is exhaustive"),
    };
    invalid_metadata(py, message, kind, None, None)
}

#[deny(clippy::wildcard_enum_match_arm)]
pub(crate) fn tables_error_to_pyerr(
    py: Python<'_>,
    err: &TablesMetadataError,
    message: String,
) -> PyErr {
    match err {
        TablesMetadataError::InvalidCqlType { .. } => {
            let kind = PyInvalidClusterMetadataKind::InvalidColumnType;
            invalid_metadata(py, message, kind, None, None)
        }
        TablesMetadataError::UnknownColumnKind {
            keyspace_name,
            table_name,
            ..
        } => {
            let kind = PyInvalidClusterMetadataKind::UnknownColumnKind;
            invalid_metadata(py, message, kind, Some(keyspace_name), Some(table_name))
        }
        _ => unreachable!("clippy testifies that the match is exhaustive"),
    }
}

#[deny(clippy::wildcard_enum_match_arm)]
pub(crate) fn client_routes_error_to_pyerr(
    py: Python<'_>,
    err: &ClientRoutesMetadataError,
    message: String,
) -> PyErr {
    let kind = match err {
        ClientRoutesMetadataError::InvalidPortValue { .. } => {
            PyInvalidClusterMetadataKind::InvalidClientRoutesPort
        }
        _ => unreachable!("clippy testifies that the match is exhaustive"),
    };
    invalid_metadata(py, message, kind, None, None)
}
