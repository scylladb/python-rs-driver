use crate::serialize::adapters::Adapters;
use crate::serialize::error::DriverSerializationError;

use std::any::Any;
use std::sync::Arc;

use pyo3::exceptions::{PyKeyError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyList, PyMapping, PySequence, PyTuple};
use pyo3::{Bound, BoundObject, Py, PyAny};

use scylla::frame::response::result::ColumnSpec;
use scylla::serialize::SerializationError;
use scylla::serialize::batch::{BatchValues, BatchValuesIterator};
use scylla::serialize::row::{
    BuiltinTypeCheckError, BuiltinTypeCheckErrorKind, RowSerializationContext, SerializeRow,
};
use scylla::serialize::writers::{RowWriter, WrittenCellProof};

use crate::serialize::value::{AdapterContext, PyAnyWrapper, PySerializeValue};

#[derive(Default, Clone)]
pub(crate) enum PyValueList {
    Sequence(Py<PySequence>),
    Mapping(Py<PyMapping>),
    #[default]
    Empty,
}

/// Bind values together with the adapters of the session that sends them.
#[derive(Clone)]
pub(crate) struct BoundValues {
    values: PyValueList,
    adapters: Arc<Adapters>,
}

impl BoundValues {
    pub(crate) fn new(values: PyValueList, adapters: Arc<Adapters>) -> Self {
        Self { values, adapters }
    }
}

impl SerializeRow for BoundValues {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        row_writer: &mut RowWriter,
    ) -> Result<(), SerializationError> {
        BoundValuesRef::new(&self.values, &self.adapters).serialize(ctx, row_writer)
    }

    fn is_empty(&self) -> bool {
        matches!(self.values, PyValueList::Empty)
    }
}

/// [`BoundValues`] that borrow both parts, for values serialized while their owner lives.
pub(crate) struct BoundValuesRef<'a> {
    values: &'a PyValueList,
    adapters: &'a Adapters,
}

impl<'a> BoundValuesRef<'a> {
    pub(crate) fn new(values: &'a PyValueList, adapters: &'a Adapters) -> Self {
        Self { values, adapters }
    }
}

impl SerializeRow for BoundValuesRef<'_> {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        row_writer: &mut RowWriter,
    ) -> Result<(), SerializationError> {
        let context = AdapterContext {
            adapters: self.adapters,
        };
        Python::attach(|py| match self.values {
            PyValueList::Sequence(sequence) => {
                serialize_sequence(sequence.bind(py), &context, ctx, row_writer)
            }
            PyValueList::Mapping(mapping) => {
                serialize_mapping(mapping.bind(py), &context, ctx, row_writer)
            }
            PyValueList::Empty => {
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
        matches!(self.values, PyValueList::Empty)
    }
}

/// The values of all statements in a batch, with one borrow of the adapters for all of them.
pub(crate) struct BoundBatchValues<'a> {
    values: Vec<PyValueList>,
    adapters: &'a Adapters,
}

impl<'a> BoundBatchValues<'a> {
    pub(crate) fn new(values: Vec<PyValueList>, adapters: &'a Adapters) -> Self {
        Self { values, adapters }
    }
}

impl BatchValues for BoundBatchValues<'_> {
    type BatchValuesIter<'r>
        = BoundBatchValuesIter<'r>
    where
        Self: 'r;

    fn batch_values_iter(&self) -> Self::BatchValuesIter<'_> {
        BoundBatchValuesIter {
            values: self.values.iter(),
            adapters: self.adapters,
        }
    }
}

pub(crate) struct BoundBatchValuesIter<'r> {
    values: std::slice::Iter<'r, PyValueList>,
    adapters: &'r Adapters,
}

impl<'r> BatchValuesIterator<'r> for BoundBatchValuesIter<'r> {
    fn serialize_next(
        &mut self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter,
    ) -> Option<Result<(), SerializationError>> {
        self.values
            .next()
            .map(|values| BoundValuesRef::new(values, self.adapters).serialize(ctx, writer))
    }

    fn is_empty_next(&mut self) -> Option<bool> {
        self.values
            .next()
            .map(|values| matches!(values, PyValueList::Empty))
    }

    fn skip_next(&mut self) -> Option<()> {
        self.values.next().map(|_| ())
    }

    fn count(self) -> usize {
        self.values.len()
    }
}

// The cluster state does not pass its adapters yet.
impl SerializeRow for PyValueList {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        row_writer: &mut RowWriter,
    ) -> Result<(), SerializationError> {
        BoundValuesRef::new(self, &Adapters::default()).serialize(ctx, row_writer)
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
    context: &AdapterContext<'_>,
    row_writer: &'a mut RowWriter<'_>,
) -> Result<WrittenCellProof<'a>, SerializationError> {
    let wrapper = PyAnyWrapper::new(val);
    let sub_writer = row_writer.make_cell_writer();
    wrapper.serialize_py(col.typ(), sub_writer, context)
}

fn serialize_sequence<'py>(
    value_list: &Bound<'py, PySequence>,
    context: &AdapterContext<'_>,
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
        serialize_element(col, &val, context, row_writer).map_err(|err| {
            DriverSerializationError::scylla_serialize_failed(err).at_parameter_index(index)
        })?;
    }

    Ok(())
}

fn serialize_mapping<'py>(
    value_list: &Bound<'py, PyMapping>,
    context: &AdapterContext<'_>,
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
        serialize_element(col, &item, context, row_writer).map_err(|err| {
            DriverSerializationError::scylla_serialize_failed(err).at_parameter_name(col.name())
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
