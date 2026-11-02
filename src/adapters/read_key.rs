//! The CQL types that readers are registered for.
//!
//! A key is a CQL type in which an element type can be "any":
//! - `CqlListType[CqlIntType]` is `List(Native(Int))`. It matches `list<int>`.
//! - `CqlListType` is `List(Any)`. It matches every list.
//! - `CqlListType[CqlListType]` is `List(List(Any))`. It matches every list of lists.

use std::cmp::Ordering;

use pyo3::PyTypeInfo;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyGenericAlias, PyTuple, PyType};
use scylla_cql::frame::response::result::{CollectionType, ColumnType, NativeType};

use super::error::DriverReadKeyError;
use crate::cluster::metadata::column_type::{
    PyCqlColumnType, PyCqlListType, PyCqlMapType, PyCqlSetType, PyCqlTupleType, PyCqlVectorType,
    native_type_of,
};

/// The CQL type that a reader is registered for: a `ColumnType` in which an element type can be
/// `Any`, and without `frozen` and the vector dimensions, which matching ignores.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ReadKey {
    /// Every type. The element types of a class without type arguments.
    Any,
    Native(NativeType),
    List(Box<ReadKey>),
    Set(Box<ReadKey>),
    Map(Box<ReadKey>, Box<ReadKey>),
    /// `None` for a tuple class without type arguments, which matches tuples of every length.
    Tuple(Option<Vec<ReadKey>>),
    Vector(Box<ReadKey>),
}

/// Converts the CQL type given to `register_reader`.
impl<'py> FromPyObject<'_, 'py> for ReadKey {
    type Error = DriverReadKeyError;

    fn extract(key: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        let (class, args) = class_and_args(&key)?;
        let py = key.py();

        if let Some(native) = native_type_of(&class) {
            // A native class is not generic, so only a hand-made GenericAlias gives it arguments.
            if args.is_some() {
                return Err(DriverReadKeyError::wrong_argument_count(&key));
            }
            return Ok(Self::Native(native));
        }
        if class.is(PyCqlListType::type_object(py)) {
            let [item] = elements(&key, args)?;
            return Ok(Self::List(Box::new(item)));
        }
        if class.is(PyCqlSetType::type_object(py)) {
            let [item] = elements(&key, args)?;
            return Ok(Self::Set(Box::new(item)));
        }
        if class.is(PyCqlVectorType::type_object(py)) {
            let [item] = elements(&key, args)?;
            return Ok(Self::Vector(Box::new(item)));
        }
        if class.is(PyCqlMapType::type_object(py)) {
            let [key_type, value_type] = elements(&key, args)?;
            return Ok(Self::Map(Box::new(key_type), Box::new(value_type)));
        }
        if class.is(PyCqlTupleType::type_object(py)) {
            let fields = args
                .map(|args| args.iter().map(|arg| arg.extract()).collect())
                .transpose()?;
            return Ok(Self::Tuple(fields));
        }
        // Also a UDT, which has its own `register_udt`.
        Err(DriverReadKeyError::not_a_cql_type(&key))
    }
}

impl ReadKey {
    /// Whether this key applies to a column of type `typ`. Frozen and vector dimensions do not matter.
    pub(super) fn matches(&self, typ: &ColumnType<'_>) -> bool {
        match (self, typ) {
            (Self::Any, _) => true,
            (Self::Native(key), ColumnType::Native(native)) => key == native,
            (
                Self::List(item),
                ColumnType::Collection {
                    typ: CollectionType::List(typ),
                    ..
                },
            )
            | (
                Self::Set(item),
                ColumnType::Collection {
                    typ: CollectionType::Set(typ),
                    ..
                },
            )
            | (Self::Vector(item), ColumnType::Vector { typ, .. }) => item.matches(typ),
            (
                Self::Map(key, value),
                ColumnType::Collection {
                    typ: CollectionType::Map(key_type, value_type),
                    ..
                },
            ) => key.matches(key_type) && value.matches(value_type),
            (Self::Tuple(None), ColumnType::Tuple(_)) => true,
            (Self::Tuple(Some(fields)), ColumnType::Tuple(types)) => {
                fields.len() == types.len()
                    && fields
                        .iter()
                        .zip(types)
                        .all(|(field, typ)| field.matches(typ))
            }
            _ => false,
        }
    }

    /// Orders two keys that match the same column type. The more specific key is greater.
    ///
    /// `Any` is less specific than every other key. Otherwise the first element type that
    /// differs decides, from the outside to the inside and from left to right.
    ///
    /// # Panics
    ///
    /// Panics if the keys are of different kinds, so they cannot match the same column type.
    pub(super) fn cmp_specificity(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Any, Self::Any)
            | (Self::Native(_), Self::Native(_))
            | (Self::Tuple(None), Self::Tuple(None)) => Ordering::Equal,
            (Self::Any, _) | (Self::Tuple(None), Self::Tuple(Some(_))) => Ordering::Less,
            (_, Self::Any) | (Self::Tuple(Some(_)), Self::Tuple(None)) => Ordering::Greater,
            (Self::List(mine), Self::List(theirs))
            | (Self::Set(mine), Self::Set(theirs))
            | (Self::Vector(mine), Self::Vector(theirs)) => mine.cmp_specificity(theirs),
            (Self::Map(my_key, my_value), Self::Map(their_key, their_value)) => my_key
                .cmp_specificity(their_key)
                .then_with(|| my_value.cmp_specificity(their_value)),
            (Self::Tuple(Some(mine)), Self::Tuple(Some(theirs))) => mine
                .iter()
                .zip(theirs)
                .map(|(mine, theirs)| mine.cmp_specificity(theirs))
                .find(|order| order.is_ne())
                .unwrap_or(Ordering::Equal),
            _ => unreachable!(
                "reader_for compares only keys that match the same column type, \
                 but got {self:?} and {other:?}"
            ),
        }
    }
}

/// The `N` element types of a collection or vector key. `Any` for each if the key has no type
/// arguments.
fn elements<const N: usize>(
    key: &Bound<'_, PyAny>,
    args: PyArgs<'_>,
) -> Result<[ReadKey; N], DriverReadKeyError> {
    let Some(args) = args else {
        return Ok(std::array::from_fn(|_| ReadKey::Any));
    };

    let wrong_count = || DriverReadKeyError::wrong_argument_count(key);
    if args.len() != N {
        return Err(wrong_count());
    }

    let keys: Vec<ReadKey> = args
        .iter()
        .map(|arg| arg.extract())
        .collect::<Result<_, _>>()?;
    keys.try_into().map_err(|_| wrong_count())
}

/// The type arguments of a key, as Python objects. `None` if the key has none.
type PyArgs<'py> = Option<Vec<Bound<'py, PyAny>>>;

/// Splits a key into its class and its type arguments:
/// - `CqlListType[CqlIntType]` gives `CqlListType` and `[CqlIntType]`.
/// - `CqlListType` gives `CqlListType` and no arguments.
/// - A type object from the schema gives its class and its element types.
fn class_and_args<'py>(
    key: &Bound<'py, PyAny>,
) -> Result<(Bound<'py, PyType>, PyArgs<'py>), DriverReadKeyError> {
    let py = key.py();
    if let Ok(alias) = key.cast::<PyGenericAlias>() {
        let class = alias
            .getattr(intern!(py, "__origin__"))?
            .cast_into::<PyType>()
            // A hand-made GenericAlias can have an origin that is not a class.
            .map_err(|_| DriverReadKeyError::not_a_cql_type(key))?;

        let args = alias.getattr(intern!(py, "__args__"))?;
        let args = args
            .cast::<PyTuple>()
            .map_err(PyErr::from)?
            .iter()
            .collect();
        return Ok((class, Some(args)));
    }

    if let Ok(class) = key.cast::<PyType>() {
        return Ok((Bound::clone(class), None));
    }

    Ok((key.get_type(), element_types(key)))
}

/// The element types of a type object from the schema. `None` if it has none.
fn element_types<'py>(typ: &Bound<'py, PyAny>) -> PyArgs<'py> {
    let py = typ.py();
    let types: Vec<&Py<PyCqlColumnType>> = if let Ok(list) = typ.cast::<PyCqlListType>() {
        vec![&list.get().column_type]
    } else if let Ok(set) = typ.cast::<PyCqlSetType>() {
        vec![&set.get().column_type]
    } else if let Ok(map) = typ.cast::<PyCqlMapType>() {
        vec![&map.get().key_type, &map.get().value_type]
    } else if let Ok(tuple) = typ.cast::<PyCqlTupleType>() {
        tuple.get().element_types.iter().collect()
    } else if let Ok(vector) = typ.cast::<PyCqlVectorType>() {
        vec![&vector.get().typ]
    } else {
        return None;
    };
    Some(
        types
            .into_iter()
            .map(|element| element.bind(py).clone().into_any())
            .collect(),
    )
}
