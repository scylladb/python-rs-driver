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
use scylla::frame::response::result::{CollectionType, ColumnType, NativeType};

use super::error::DriverReadKeyError;
use crate::cluster::metadata::column_type::{
    PyCqlColumnType, PyCqlListType, PyCqlMapType, PyCqlSetType, PyCqlTupleType,
    PyCqlUserDefinedType, PyCqlVectorType, native_type_of,
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
    /// Every user defined type. One UDT has `register_udt` instead.
    Udt,
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
        if class.is(PyCqlUserDefinedType::type_object(py)) {
            // Only the class itself: one UDT has `register_udt`.
            if !key.is(&class) {
                return Err(DriverReadKeyError::one_udt(&key));
            }
            return Ok(Self::Udt);
        }

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
            (Self::Tuple(None), ColumnType::Tuple(_))
            | (Self::Udt, ColumnType::UserDefinedType { .. }) => true,
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
            | (Self::Tuple(None), Self::Tuple(None))
            | (Self::Udt, Self::Udt) => Ordering::Equal,
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

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;
    use std::sync::Arc;

    use scylla::frame::response::result::{
        CollectionType, ColumnType, NativeType, UserDefinedType,
    };

    use super::ReadKey::{self, Any, List, Map, Native, Set, Tuple, Udt, Vector};

    fn int() -> ColumnType<'static> {
        ColumnType::Native(NativeType::Int)
    }

    fn text() -> ColumnType<'static> {
        ColumnType::Native(NativeType::Text)
    }

    fn list(item: ColumnType<'static>, frozen: bool) -> ColumnType<'static> {
        ColumnType::Collection {
            frozen,
            typ: CollectionType::List(Box::new(item)),
        }
    }

    fn set(item: ColumnType<'static>) -> ColumnType<'static> {
        ColumnType::Collection {
            frozen: false,
            typ: CollectionType::Set(Box::new(item)),
        }
    }

    fn map(key: ColumnType<'static>, value: ColumnType<'static>) -> ColumnType<'static> {
        ColumnType::Collection {
            frozen: false,
            typ: CollectionType::Map(Box::new(key), Box::new(value)),
        }
    }

    fn vector(item: ColumnType<'static>, dimensions: u16) -> ColumnType<'static> {
        ColumnType::Vector {
            typ: Box::new(item),
            dimensions,
        }
    }

    fn udt(name: &'static str) -> ColumnType<'static> {
        ColumnType::UserDefinedType {
            frozen: true,
            definition: Arc::new(UserDefinedType {
                name: name.into(),
                keyspace: "ks".into(),
                field_types: vec![("a".into(), int())],
            }),
        }
    }

    fn key_int() -> ReadKey {
        Native(NativeType::Int)
    }

    fn b(key: ReadKey) -> Box<ReadKey> {
        Box::new(key)
    }

    /// The key that `reader_for` picks: the most specific match, the last one on a tie.
    fn best<'k>(keys: &'k [ReadKey], typ: &ColumnType<'_>) -> Option<&'k ReadKey> {
        keys.iter()
            .filter(|key| key.matches(typ))
            .max_by(|a, b| a.cmp_specificity(b))
    }

    #[test]
    fn bare_class_matches_every_element_type() {
        let any_list = List(b(Any));
        assert!(any_list.matches(&list(int(), false)));
        assert!(any_list.matches(&list(text(), true)));
        assert!(any_list.matches(&list(list(int(), true), false)));
        assert!(!any_list.matches(&set(int())));
        assert!(Set(b(Any)).matches(&set(int())));
        assert!(Map(b(Any), b(Any)).matches(&map(text(), list(int(), true))));
    }

    #[test]
    fn type_arguments_select_one_type() {
        let int_list = List(b(key_int()));
        assert!(int_list.matches(&list(int(), false)));
        assert!(!int_list.matches(&list(text(), false)));
        assert!(
            Map(b(Native(NativeType::Text)), b(List(b(key_int()))))
                .matches(&map(text(), list(int(), true)))
        );
        assert!(!Map(b(key_int()), b(Any)).matches(&map(text(), int())));
    }

    #[test]
    fn frozen_and_vector_dimensions_do_not_matter() {
        assert!(List(b(List(b(key_int())))).matches(&list(list(int(), true), true)));
        let float_vector = Vector(b(Native(NativeType::Float)));
        assert!(float_vector.matches(&vector(ColumnType::Native(NativeType::Float), 2)));
        assert!(float_vector.matches(&vector(ColumnType::Native(NativeType::Float), 3)));
        assert!(!float_vector.matches(&vector(int(), 2)));
    }

    #[test]
    fn tuple_without_arguments_matches_every_length() {
        assert!(Tuple(None).matches(&ColumnType::Tuple(vec![int()])));
        assert!(Tuple(None).matches(&ColumnType::Tuple(vec![int(), text()])));
        let pair = Tuple(Some(vec![key_int(), Any]));
        assert!(pair.matches(&ColumnType::Tuple(vec![int(), text()])));
        assert!(!pair.matches(&ColumnType::Tuple(vec![int()])));
        assert!(!pair.matches(&ColumnType::Tuple(vec![text(), text()])));
    }

    #[test]
    fn udt_key_matches_every_udt() {
        assert!(Udt.matches(&udt("address")));
        assert!(Udt.matches(&udt("point")));
        assert!(!Udt.matches(&ColumnType::Tuple(vec![int()])));
        assert!(List(b(Udt)).matches(&list(udt("address"), false)));
        assert!(!List(b(Udt)).matches(&list(int(), false)));
    }

    #[test]
    fn type_arguments_beat_a_bare_class() {
        let keys = [List(b(Any)), List(b(key_int()))];
        assert_eq!(best(&keys, &list(int(), false)), Some(&keys[1]));
        assert_eq!(best(&keys, &list(text(), false)), Some(&keys[0]));
    }

    #[test]
    fn most_specific_nested_key_wins() {
        let keys = [
            List(b(Any)),
            List(b(List(b(Any)))),
            List(b(List(b(key_int())))),
        ];
        assert_eq!(best(&keys, &list(list(int(), true), true)), Some(&keys[2]));
        assert_eq!(best(&keys, &list(list(text(), true), true)), Some(&keys[1]));
        assert_eq!(best(&keys, &list(int(), false)), Some(&keys[0]));
    }

    #[test]
    fn tuple_elements_decide_from_left_to_right() {
        let right = Tuple(Some(vec![List(b(Any)), List(b(key_int()))]));
        let left = Tuple(Some(vec![List(b(key_int())), List(b(Any))]));
        let typ = ColumnType::Tuple(vec![list(int(), false), list(int(), false)]);
        let keys = [right, left];
        assert_eq!(best(&keys, &typ), Some(&keys[1]));
    }

    #[test]
    fn map_key_decides_before_value() {
        let typ = map(list(int(), true), list(int(), true));
        let value_specific = Map(b(List(b(Any))), b(List(b(key_int()))));
        let key_specific = Map(b(List(b(key_int()))), b(List(b(Any))));
        let keys = [value_specific, key_specific];
        assert_eq!(best(&keys, &typ), Some(&keys[1]));
    }

    #[test]
    fn specificity_of_each_kind_of_key() {
        use Ordering::{Equal, Greater, Less};

        let cases = [
            (Any, Any, Equal),
            (Any, key_int(), Less),
            (key_int(), Any, Greater),
            (key_int(), key_int(), Equal),
            (List(b(Any)), List(b(key_int())), Less),
            (Set(b(key_int())), Set(b(Any)), Greater),
            (Vector(b(Any)), Vector(b(key_int())), Less),
            // The keys are equal, so the values decide.
            (Map(b(Any), b(Any)), Map(b(Any), b(key_int())), Less),
            (
                Map(b(key_int()), b(Any)),
                Map(b(Any), b(key_int())),
                Greater,
            ),
            (Udt, Udt, Equal),
            (Any, Udt, Less),
            (List(b(Any)), List(b(Udt)), Less),
            (Tuple(None), Tuple(None), Equal),
            (Tuple(None), Tuple(Some(vec![key_int()])), Less),
            (Tuple(Some(vec![key_int()])), Tuple(None), Greater),
            // The equal first elements do not decide.
            (
                Tuple(Some(vec![key_int(), Any])),
                Tuple(Some(vec![key_int(), key_int()])),
                Less,
            ),
            (
                Tuple(Some(vec![key_int(), key_int()])),
                Tuple(Some(vec![key_int(), key_int()])),
                Equal,
            ),
        ];
        for (mine, theirs, expected) in cases {
            assert_eq!(
                mine.cmp_specificity(&theirs),
                expected,
                "{mine:?} against {theirs:?}"
            );
        }
    }

    #[test]
    #[should_panic(expected = "reader_for compares only keys that match the same column type")]
    fn keys_of_different_kinds_are_a_bug() {
        List(b(Any)).cmp_specificity(&Set(b(Any)));
    }
}
