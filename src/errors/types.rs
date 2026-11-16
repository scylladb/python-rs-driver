// TODO: drop once PyO3 ships PyO3/pyo3#6309 (from_py_object clones Copy types)
#![allow(clippy::clone_on_copy)]

use pyo3::prelude::*;
use scylla::errors::{CqlResponseKind, OperationType, WriteType};

#[pyclass(
    module = "scylla.errors",
    name = "WriteType",
    frozen,
    from_py_object,
    eq
)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PyWriteType {
    Simple(),
    Batch(),
    UnloggedBatch(),
    Counter(),
    BatchLog(),
    Cas(),
    View(),
    Cdc(),
    Other { value: String },
}

impl From<WriteType> for PyWriteType {
    fn from(value: WriteType) -> Self {
        match value {
            WriteType::Simple => PyWriteType::Simple(),
            WriteType::Batch => PyWriteType::Batch(),
            WriteType::UnloggedBatch => PyWriteType::UnloggedBatch(),
            WriteType::Counter => PyWriteType::Counter(),
            WriteType::BatchLog => PyWriteType::BatchLog(),
            WriteType::Cas => PyWriteType::Cas(),
            WriteType::View => PyWriteType::View(),
            WriteType::Cdc => PyWriteType::Cdc(),
            WriteType::Other(str) => PyWriteType::Other { value: str },
        }
    }
}

#[pyclass(
    module = "scylla.errors",
    name = "OperationType",
    frozen,
    from_py_object,
    eq
)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PyOperationType {
    Read(),
    Write(),
    Other { code: u8 },
}

impl From<OperationType> for PyOperationType {
    fn from(value: OperationType) -> Self {
        match value {
            OperationType::Read => PyOperationType::Read(),
            OperationType::Write => PyOperationType::Write(),
            OperationType::Other(i) => PyOperationType::Other { code: i },
        }
    }
}

#[pyclass(
    module = "scylla.errors",
    name = "CqlResponseKind",
    frozen,
    from_py_object,
    eq,
    eq_int
)]
#[derive(Debug, Copy, Clone, PartialEq)]
#[non_exhaustive]
pub enum PyCqlResponseKind {
    Error,
    Ready,
    Authenticate,
    Supported,
    Result,
    Event,
    AuthChallenge,
    AuthSuccess,
}

impl From<CqlResponseKind> for PyCqlResponseKind {
    #[deny(clippy::wildcard_enum_match_arm)]
    fn from(value: CqlResponseKind) -> Self {
        match value {
            CqlResponseKind::Error => PyCqlResponseKind::Error,
            CqlResponseKind::Ready => PyCqlResponseKind::Ready,
            CqlResponseKind::Authenticate => PyCqlResponseKind::Authenticate,
            CqlResponseKind::Supported => PyCqlResponseKind::Supported,
            CqlResponseKind::Result => PyCqlResponseKind::Result,
            CqlResponseKind::Event => PyCqlResponseKind::Event,
            CqlResponseKind::AuthChallenge => PyCqlResponseKind::AuthChallenge,
            CqlResponseKind::AuthSuccess => PyCqlResponseKind::AuthSuccess,
            _ => unreachable!("Unhandled `CqlResponseKind` variant"),
        }
    }
}
