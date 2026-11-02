//! The read adapters of a page, resolved once against its column types.

use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyType};
use scylla_cql::frame::response::result::{CollectionType, ColumnSpec, ColumnType};

use crate::adapters::Adapters;
use crate::deserialize::error::DriverDeserializationError;
use crate::deserialize::value::PyDeserializedValue;

/// What to do with a value after its built-in deserialization.
pub(crate) enum ReadAdapter {
    /// `cls(**fields)`, for a UDT with a registered class.
    UdtClass(Py<PyType>),
}

impl ReadAdapter {
    fn apply<'py>(&self, value: Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            // A null UDT stays None: only a field dict goes to the class.
            Self::UdtClass(cls) => match value.cast::<PyDict>() {
                Ok(fields) => cls.bind(value.py()).call((), Some(fields)),
                Err(_) => Ok(value),
            },
        }
    }
}

/// The adapters of a value and below it. Exists only where at least one adapter applies.
pub(crate) struct AdapterTree {
    adapter: Option<ReadAdapter>,
    /// The item; the key and the value; or the fields by position.
    /// `None` where no adapter applies to that child or below it.
    children: Box<[Option<AdapterTree>]>,
}

impl AdapterTree {
    /// Walks `typ`. `None` if no adapter applies to it or below it.
    fn resolve(py: Python<'_>, typ: &ColumnType<'_>, adapters: &Adapters) -> Option<Self> {
        let child = |typ| Self::resolve(py, typ, adapters);
        let (adapter, children): (_, Box<[_]>) = match typ {
            ColumnType::Collection {
                typ: CollectionType::List(item) | CollectionType::Set(item),
                ..
            }
            | ColumnType::Vector { typ: item, .. } => (None, Box::new([child(item)])),
            ColumnType::Collection {
                typ: CollectionType::Map(key, value),
                ..
            } => (None, Box::new([child(key), child(value)])),
            ColumnType::Tuple(fields) => (None, fields.iter().map(child).collect()),
            ColumnType::UserDefinedType { definition, .. } => (
                adapters
                    .udt_class(&definition.keyspace, &definition.name)
                    .map(|cls| ReadAdapter::UdtClass(cls.clone_ref(py))),
                definition
                    .field_types
                    .iter()
                    .map(|(_, typ)| child(typ))
                    .collect(),
            ),
            _ => return None,
        };

        if adapter.is_none() && children.iter().all(Option::is_none) {
            return None;
        }
        Some(Self { adapter, children })
    }

    fn child(&self, index: usize) -> Option<&Self> {
        self.children.get(index)?.as_ref()
    }

    /// The element of a list, set or vector.
    pub(crate) fn item(&self) -> Option<&Self> {
        self.child(0)
    }

    pub(crate) fn key(&self) -> Option<&Self> {
        self.child(0)
    }

    pub(crate) fn value(&self) -> Option<&Self> {
        self.child(1)
    }

    /// A tuple element or UDT field, by position.
    pub(crate) fn field(&self, index: usize) -> Option<&Self> {
        self.child(index)
    }

    /// Applies the adapter of this value, if it has one, to its deserialized form.
    pub(crate) fn apply(
        &self,
        py: Python<'_>,
        value: PyDeserializedValue,
    ) -> Result<PyDeserializedValue, DriverDeserializationError> {
        let Some(adapter) = &self.adapter else {
            return Ok(value);
        };
        let Ok(value) = value.into_pyobject(py);
        adapter
            .apply(value)
            .map(PyDeserializedValue::new)
            .map_err(DriverDeserializationError::python_conversion_failed)
    }
}

/// The adapter trees of the columns of a page. Empty if no adapter applies to any column.
#[derive(Clone, Default)]
pub(crate) struct ResolvedAdapters(Option<Arc<[Option<AdapterTree>]>>);

impl ResolvedAdapters {
    pub(crate) fn resolve(py: Python<'_>, specs: &[ColumnSpec<'_>], adapters: &Adapters) -> Self {
        if !adapters.has_read_adapters() {
            return Self(None);
        }
        let trees: Arc<[Option<AdapterTree>]> = specs
            .iter()
            .map(|spec| AdapterTree::resolve(py, spec.typ(), adapters))
            .collect();
        if trees.iter().all(Option::is_none) {
            return Self(None);
        }
        Self(Some(trees))
    }

    pub(crate) fn get(&self, column: usize) -> Option<&AdapterTree> {
        self.0.as_deref()?.get(column)?.as_ref()
    }
}
