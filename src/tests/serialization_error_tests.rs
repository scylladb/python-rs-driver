use std::borrow::Cow;
use std::ffi::CStr;
use std::sync::Arc;

use pyo3::exceptions::{PyOverflowError, PyValueError};
use pyo3::prelude::*;
use scylla::cluster::metadata::{ColumnType, NativeType, UserDefinedType};
use scylla::frame::response::result::{ColumnSpec, TableSpec};
use scylla::serialize::SerializationError;
use scylla::serialize::row::{RowSerializationContext, SerializeRow};
use scylla::serialize::value::{
    BuiltinSerializationError, BuiltinSerializationErrorKind, MapSerializationErrorKind,
    SetOrListSerializationErrorKind, VectorSerializationErrorKind,
};
use scylla::serialize::writers::RowWriter;

use crate::errors::{
    TypeMismatchSerializationError, ValueOverflowSerializationError, WrongNumberOfElements,
};
use crate::serialize::error::serialization_error_to_pyerr;
use crate::serialize::value_list::PyValueList;

fn attr<'py, T: FromPyObjectOwned<'py>>(py: Python<'py>, err: &PyErr, name: &str) -> T {
    let value = err.value(py).getattr(name).unwrap();
    value.extract().map_err(Into::<PyErr>::into).unwrap()
}

fn builtin(kind: impl Into<BuiltinSerializationErrorKind>) -> SerializationError {
    SerializationError::new(BuiltinSerializationError {
        rust_name: "test",
        got: ColumnType::Native(NativeType::Int),
        kind: kind.into(),
    })
}

fn to_pyerr(py: Python<'_>, err: &SerializationError) -> PyErr {
    serialization_error_to_pyerr(py, err, err.to_string())
}

#[test]
fn too_large_value_is_value_overflow() {
    Python::initialize();
    let errors = [
        builtin(SetOrListSerializationErrorKind::TooManyElements),
        builtin(MapSerializationErrorKind::TooManyElements),
    ];
    Python::attach(|py| {
        for err in &errors {
            let err = to_pyerr(py, err);
            assert!(err.is_instance_of::<ValueOverflowSerializationError>(py));
            assert!(err.is_instance_of::<PyOverflowError>(py));
        }
    });
}

#[test]
fn vector_of_wrong_length_is_wrong_number_of_elements() {
    Python::initialize();
    let err = builtin(VectorSerializationErrorKind::InvalidNumberOfElements(3, 4));
    Python::attach(|py| {
        let err = to_pyerr(py, &err);
        assert!(err.is_instance_of::<WrongNumberOfElements>(py));
        assert!(err.is_instance_of::<PyValueError>(py));
        assert_eq!(attr::<String>(py, &err, "container"), "vector");
        assert_eq!(attr::<usize>(py, &err, "expected"), 4);
        assert_eq!(attr::<usize>(py, &err, "received"), 3);
    });
}

#[test]
fn failed_element_raises_the_class_of_its_cause() {
    Python::initialize();
    let overflow = || builtin(BuiltinSerializationErrorKind::SizeOverflow);
    let errors = [
        builtin(SetOrListSerializationErrorKind::ElementSerializationFailed(
            overflow(),
        )),
        builtin(VectorSerializationErrorKind::ElementSerializationFailed(
            overflow(),
        )),
        builtin(MapSerializationErrorKind::KeySerializationFailed(overflow())),
        builtin(MapSerializationErrorKind::ValueSerializationFailed(
            overflow(),
        )),
    ];
    Python::attach(|py| {
        for err in &errors {
            let err = to_pyerr(py, err);
            assert!(err.is_instance_of::<ValueOverflowSerializationError>(py));
        }
    });
}

/// Serializes `row` (Python code) for one column of type `typ`, as `execute` does.
fn serialize_row(py: Python<'_>, row: &CStr, typ: ColumnType<'_>) -> PyErr {
    let values: PyValueList = py.eval(row, None, None).unwrap().extract().unwrap();
    let specs = [ColumnSpec::borrowed(
        "v",
        typ,
        TableSpec::borrowed("ks", "t"),
    )];
    let ctx = RowSerializationContext::from_specs(&specs);
    let mut buf = Vec::new();
    let err = values
        .serialize(&ctx, &mut RowWriter::new(&mut buf))
        .unwrap_err();
    serialization_error_to_pyerr(py, &err, err.to_string())
}

fn int() -> ColumnType<'static> {
    ColumnType::Native(NativeType::Int)
}

#[test]
fn failed_tuple_element_names_its_index() {
    Python::initialize();
    Python::attach(|py| {
        let err = serialize_row(py, c"((1, 'x'),)", ColumnType::Tuple(vec![int(), int()]));
        assert!(err.is_instance_of::<TypeMismatchSerializationError>(py));
        assert_eq!(attr::<usize>(py, &err, "bind_marker"), 0);
        assert!(err.to_string().contains("element no. 1"));
    });
}

#[test]
fn failed_udt_field_names_the_field() {
    Python::initialize();
    let udt = UserDefinedType {
        name: Cow::Borrowed("point"),
        keyspace: Cow::Borrowed("ks"),
        field_types: vec![(Cow::Borrowed("x"), int())],
    };
    let typ = ColumnType::UserDefinedType {
        frozen: true,
        definition: Arc::new(udt),
    };
    Python::attach(|py| {
        let err = serialize_row(py, c"({'x': 'not an int'},)", typ);
        assert!(err.is_instance_of::<TypeMismatchSerializationError>(py));
        assert!(err.to_string().contains("field x"));
    });
}

#[test]
fn row_with_vector_of_wrong_length_raises_wrong_number_of_elements() {
    Python::initialize();
    let typ = ColumnType::Vector {
        typ: Box::new(int()),
        dimensions: 4,
    };
    Python::attach(|py| {
        let err = serialize_row(py, c"([1, 2, 3],)", typ);
        assert!(err.is_instance_of::<WrongNumberOfElements>(py));
        assert_eq!(attr::<usize>(py, &err, "expected"), 4);
        assert_eq!(attr::<usize>(py, &err, "received"), 3);
    });
}

#[test]
fn row_with_too_long_tuple_raises_wrong_number_of_elements() {
    Python::initialize();
    Python::attach(|py| {
        let err = serialize_row(py, c"((1, 2, 3),)", ColumnType::Tuple(vec![int(), int()]));
        assert!(err.is_instance_of::<WrongNumberOfElements>(py));
        assert_eq!(attr::<String>(py, &err, "container"), "tuple");
    });
}
