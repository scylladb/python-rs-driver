use crate::serialize::error::{DriverSerializationError, ParameterReference};

use std::any::Any;

use pyo3::exceptions::{PyKeyError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyList, PyMapping, PySequence, PyTuple};
use pyo3::{Bound, BoundObject, Py, PyAny};

use scylla::frame::response::result::ColumnSpec;
use scylla::serialize::SerializationError;
use scylla::serialize::row::{
    BuiltinTypeCheckError, BuiltinTypeCheckErrorKind, RowSerializationContext, SerializeRow,
};
use scylla::serialize::value::SerializeValue;
use scylla::serialize::writers::{RowWriter, WrittenCellProof};

use crate::serialize::value::PyAnyWrapper;

#[derive(Default, Clone)]
pub(crate) enum PyValueList {
    Sequence(Py<PySequence>),
    Mapping(Py<PyMapping>),
    #[default]
    Empty,
}

impl SerializeRow for PyValueList {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        row_writer: &mut RowWriter,
    ) -> Result<(), SerializationError> {
        Python::attach(|py| match self {
            Self::Sequence(sequence) => serialize_sequence(sequence.bind(py), ctx, row_writer),
            Self::Mapping(mapping) => serialize_mapping(mapping.bind(py), ctx, row_writer),
            Self::Empty => {
                if ctx.columns().is_empty() {
                    Ok(())
                } else {
                    let expected = ctx.columns().len();
                    let got = 0usize;
                    let kind = BuiltinTypeCheckErrorKind::WrongColumnCount {
                        rust_cols: got,
                        cql_cols: expected,
                    };
                    Err(SerializationError::new(BuiltinTypeCheckError {
                        rust_name: "None or empty collection",
                        kind,
                    }))
                }
            }
        })
    }

    fn is_empty(&self) -> bool {
        matches!(self, Self::Empty)
    }
}

impl<'a, 'py> FromPyObject<'a, 'py> for PyValueList {
    type Error = PyErr;

    fn extract(val: Borrowed<'a, 'py, PyAny>) -> Result<Self, Self::Error> {
        if val.is_none() {
            return Ok(Self::Empty);
        }

        if let Ok(sequence) = val.cast::<PyList>() {
            if sequence.len() == 0 {
                return Ok(Self::Empty);
            }
            return Ok(Self::Sequence(sequence.as_sequence().to_owned().unbind()));
        }

        if let Ok(sequence) = val.cast::<PyTuple>() {
            if sequence.len() == 0 {
                return Ok(Self::Empty);
            }
            return Ok(Self::Sequence(sequence.as_sequence().to_owned().unbind()));
        }

        if let Ok(mapping) = val.cast::<PyMapping>() {
            // If any error was encountered, we should not treat this as empty.
            if mapping.len().map(|len| len == 0).unwrap_or(false) {
                return Ok(Self::Empty);
            }
            return Ok(Self::Mapping(mapping.unbind()));
        }

        let python_type_name = val.get_type().name()?;
        let python_type_name = python_type_name.extract::<&str>()?;

        Err(PyErr::new::<PyTypeError, _>(format!(
            "Invalid row type: got {}, expected Python tuple, list or Mapping (e.g. dict)",
            python_type_name
        )))
    }
}

fn length_equality_check<T: Any>(
    val_list_len: usize,
    cols_len: usize,
) -> Result<(), SerializationError> {
    if val_list_len != cols_len {
        return Err(mk_typck_err_val_list::<T>(
            BuiltinTypeCheckErrorKind::WrongColumnCount {
                rust_cols: val_list_len,
                cql_cols: cols_len,
            },
        ));
    }

    Ok(())
}

fn serialize_element<'a>(
    col: &ColumnSpec,
    val: &Bound<PyAny>,
    row_writer: &'a mut RowWriter<'_>,
) -> Result<WrittenCellProof<'a>, SerializationError> {
    let wrapper = PyAnyWrapper::new(val);
    let sub_writer = row_writer.make_cell_writer();
    SerializeValue::serialize(&wrapper, col.typ(), sub_writer)
}

fn serialize_sequence<'py>(
    value_list: &Bound<'py, PySequence>,
    ctx: &RowSerializationContext<'_>,
    row_writer: &mut RowWriter,
) -> Result<(), SerializationError> {
    let len = value_list
        .len()
        .map_err(DriverSerializationError::python_interop_failed)?;

    length_equality_check::<PySequence>(len, ctx.columns().len())?;

    let iter = value_list
        .try_iter()
        .map_err(DriverSerializationError::python_interop_failed)?;

    for (index, (col, val)) in ctx.columns().iter().zip(iter).enumerate() {
        let val = val.map_err(DriverSerializationError::python_interop_failed)?;
        serialize_element(col, &val, row_writer).map_err(|err| {
            DriverSerializationError::locate(err, ParameterReference::Index(index))
        })?;
    }

    Ok(())
}

fn serialize_mapping<'py>(
    value_list: &Bound<'py, PyMapping>,
    ctx: &RowSerializationContext<'_>,
    row_writer: &mut RowWriter,
) -> Result<(), SerializationError> {
    let py = value_list.py();
    let dict_len = value_list
        .len()
        .map_err(DriverSerializationError::python_interop_failed)?;
    length_equality_check::<PyMapping>(dict_len, ctx.columns().len())?;

    for col in ctx.columns().iter() {
        let item: Bound<PyAny> = value_list.get_item(col.name()).map_err(|e| {
            if e.is_instance_of::<PyKeyError>(py) {
                mk_typck_err_val_list::<PyMapping>(
                    BuiltinTypeCheckErrorKind::ValueMissingForColumn {
                        name: col.name().into(),
                    },
                )
            } else {
                SerializationError::new(DriverSerializationError::python_interop_failed(e))
            }
        })?;
        serialize_element(col, &item, row_writer).map_err(|err| {
            DriverSerializationError::locate(err, ParameterReference::Name(col.name().into()))
        })?;
    }

    Ok(())
}

fn mk_typck_err_val_list<T>(kind: impl Into<BuiltinTypeCheckErrorKind>) -> SerializationError {
    SerializationError::new(BuiltinTypeCheckError {
        rust_name: std::any::type_name::<T>(),
        kind: kind.into(),
    })
}
