//! The read adapters of a page, resolved once against its column types.

use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyType;
use scylla::frame::response::result::{CollectionType, ColumnSpec, ColumnType};

use crate::adapters::Adapters;
use crate::cql_types::CqlEmpty;
use crate::deserialize::error::DriverDeserializationError;
use crate::deserialize::value::{PyDeserializedValue, fields_dict};

/// What the deserializer does with a value besides its built-in deserialization.
pub(crate) enum ReadAdapter {
    /// Builds a UDT value from its fields, while the UDT is read.
    Udt(UdtBuilder),
    /// `reader(value)`, for a type with a registered reader.
    Reader(Py<PyAny>),
}

impl ReadAdapter {
    /// The step after the built-in deserialization.
    fn apply<'py>(&self, value: Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            // The builder already made the value.
            Self::Udt(_) => Ok(value),
            Self::Reader(_) if value.is_none() || value.is_instance_of::<CqlEmpty>() => Ok(value),
            Self::Reader(reader) => reader.bind(value.py()).call1((value,)),
        }
    }
}

/// How the deserializer builds the value of a UDT from its fields.
pub(crate) enum UdtBuilder {
    /// `cls(name=value, ...)`, for a UDT with a registered class.
    Class(Py<PyType>),
}

impl UdtBuilder {
    /// Builds a UDT value from its fields, in schema order.
    pub(crate) fn build<'py, 'n>(
        &self,
        py: Python<'py>,
        fields: impl Iterator<Item = Result<(&'n str, PyDeserializedValue), DriverDeserializationError>>,
    ) -> Result<Bound<'py, PyAny>, DriverDeserializationError> {
        match self {
            Self::Class(cls) => {
                let kwargs = fields_dict(py, fields)?;
                cls.bind(py)
                    .call((), Some(&kwargs))
                    .map_err(DriverDeserializationError::python_conversion_failed)
            }
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
        let reader = || {
            adapters
                .reader_for(typ)
                .map(|reader| ReadAdapter::Reader(reader.clone_ref(py)))
        };
        let (adapter, children): (_, Box<[_]>) = match typ {
            ColumnType::Native(_) => (reader(), Box::new([])),
            ColumnType::Collection {
                typ: CollectionType::List(item) | CollectionType::Set(item),
                ..
            }
            | ColumnType::Vector { typ: item, .. } => (reader(), Box::new([child(item)])),
            ColumnType::Collection {
                typ: CollectionType::Map(key, value),
                ..
            } => (reader(), Box::new([child(key), child(value)])),
            ColumnType::Tuple(fields) => (reader(), fields.iter().map(child).collect()),
            ColumnType::UserDefinedType { definition, .. } => (
                adapters
                    .udt_class(&definition.keyspace, &definition.name)
                    .map(|cls| ReadAdapter::Udt(UdtBuilder::Class(cls.clone_ref(py)))),
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

    /// How to build this value, if it is a UDT with its own builder.
    pub(crate) fn udt(&self) -> Option<&UdtBuilder> {
        match &self.adapter {
            Some(ReadAdapter::Udt(builder)) => Some(builder),
            _ => None,
        }
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
