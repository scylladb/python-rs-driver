use crate::cql_types::{CqlEmpty, PyCqlDate, PyCqlDuration, PyCqlTime, PyCqlTimestamp};
use crate::deserialize::conversion::CqlVarintWrapper;
use crate::deserialize::error::DriverDeserializationError;
use crate::utils::PyValueOrError;
use bigdecimal::BigDecimal;
use pyo3::prelude::{PyDictMethods, PyListMethods, PySetMethods};
use pyo3::sync::PyOnceLock;
use pyo3::types::{
    PyBool, PyBytes, PyDict, PyFloat, PyInt, PyList, PyNone, PySet, PyString, PyTuple,
};
use pyo3::{Bound, IntoPyObject, Py, PyAny, PyResult, Python};
use scylla::deserialize::value::FrameSliceWithMetadata;
use scylla::deserialize::value::VectorIterator;
use scylla::deserialize::value::{DeserializeValue, ListlikeIterator, MapIterator, UdtIterator};
use scylla_cql::deserialize::{DeserializationError, FrameSlice};
use scylla_cql::frame::response::result::ColumnType;
use scylla_cql::frame::response::result::ColumnType::Native;
use scylla_cql::frame::response::result::{CollectionType, NativeType};
use scylla_cql::value::{
    Counter, CqlDate, CqlDecimalBorrowed, CqlDuration, CqlTime, CqlTimestamp, CqlTimeuuid,
    CqlVarintBorrowed,
};
use std::convert::Infallible;
use std::marker::PhantomData;
use std::net::IpAddr;
// NOTE: I intentionally do NOT use Scylla's `DeserializeValue` trait here.
// The trait does not provide a `Python` argument, meaning that Python objects which
// would have to be constructed inside `deserialize()` or deeper in recursion
// would require acquiring the GIL separately for every column/element/UDT/List
// during nested decoding.
//
// Using our own `PyDeserializeValue` trait keeps all Python conversions inside
// a single `Python::with_gil(...)` boundary, allowing complex values (lists,
// sets, UDTs, nested collections) to be deserialized without repeated GIL
// acquisition. This avoids potential slowdown of acquiring GIL multiples times.
//
// If GIL-per-python object created would not be considered a problem and
// eliminating the need of rewriting DeserializeValue trait would be more beneficial
// We could switch back to using `DeserializeValue` trait.
pub(crate) trait PyDeserializeValue<'frame, 'metadata, 'py>: Sized {
    fn deserialize_py(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
        py: Python<'py>,
    ) -> Result<PyDeserializedValue, DriverDeserializationError>;
}

impl PyDeserializedValue {
    fn new(value: Bound<'_, PyAny>) -> Self {
        Self {
            value: value.into(),
        }
    }

    fn none(py: Python) -> Self {
        Self {
            value: PyNone::get(py).to_owned().into(),
        }
    }

    fn empty_value(py: Python<'_>) -> PyResult<Self> {
        static EMPTY_CQL_VALUE: PyOnceLock<Py<CqlEmpty>> = PyOnceLock::new();
        let empty: &Py<CqlEmpty> = EMPTY_CQL_VALUE.get_or_try_init(py, || Py::new(py, CqlEmpty))?;

        Ok(Self {
            value: Py::clone_ref(empty.as_any(), py),
        })
    }
}

impl<'py> IntoPyObject<'py> for PyDeserializedValue {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = Infallible;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(self.value.into_bound(py))
    }
}

impl<'py> IntoPyObject<'py> for &PyDeserializedValue {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = Infallible;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(self.value.clone_ref(py).into_bound(py))
    }
}

impl<'frame, 'metadata, 'py> PyDeserializeValue<'frame, 'metadata, 'py> for PyDeserializedValue {
    fn deserialize_py(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
        py: Python<'py>,
    ) -> Result<Self, DriverDeserializationError> {
        deser_cql_py_value(py, typ, v)
    }
}

pub(crate) struct PyDeserializedValue {
    value: Py<PyAny>,
}

struct List<T> {
    phantom_data: PhantomData<T>,
}

fn deserialize_sequence<'frame, 'metadata, 'py, T, FBuild>(
    typ: &'metadata ColumnType<'metadata>,
    v: FrameSlice<'frame>,
    py: Python<'py>,
    mut builder: FBuild,
) -> Result<(), DriverDeserializationError>
where
    T: PyDeserializeValue<'frame, 'metadata, 'py>,
    FBuild: FnMut(PyDeserializedValue) -> PyResult<()>,
{
    let list_iter =
        ListlikeIterator::<FrameSliceWithMetadata<'frame, 'metadata>>::deserialize(typ, Some(v))
            .map_err(DriverDeserializationError::scylla_decode_failed)?;

    for (i, raw_elem_with_metadata) in list_iter.enumerate() {
        let raw_elem_with_metadata = raw_elem_with_metadata
            .map_err(DriverDeserializationError::scylla_decode_failed)
            .map_err(|e| e.in_sequence_index(i))?;

        let item = T::deserialize_py(
            raw_elem_with_metadata.column_type,
            raw_elem_with_metadata.frame_slice,
            py,
        )
        .map_err(|e| e.in_sequence_index(i))?;

        builder(item)
            .map_err(DriverDeserializationError::python_conversion_failed)
            .map_err(|e| e.in_sequence_index(i))?;
    }

    Ok(())
}

impl<'frame, 'metadata, 'py, T> PyDeserializeValue<'frame, 'metadata, 'py> for List<T>
where
    T: PyDeserializeValue<'frame, 'metadata, 'py>,
{
    fn deserialize_py(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
        py: Python<'py>,
    ) -> Result<PyDeserializedValue, DriverDeserializationError> {
        let Some(v) = v else {
            return Ok(PyDeserializedValue::new(PyList::empty(py).into_any()));
        };

        let list = PyList::empty(py);

        deserialize_sequence::<T, _>(typ, v, py, |item| list.append(item))?;

        Ok(PyDeserializedValue::new(list.into_any()))
    }
}

struct Map<K, V> {
    phantom_data_k: PhantomData<K>,
    phantom_data_v: PhantomData<V>,
}

impl<'frame, 'metadata, 'py, K, V> PyDeserializeValue<'frame, 'metadata, 'py> for Map<K, V>
where
    K: PyDeserializeValue<'frame, 'metadata, 'py>,
    V: PyDeserializeValue<'frame, 'metadata, 'py>,
{
    fn deserialize_py(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
        py: Python<'py>,
    ) -> Result<PyDeserializedValue, DriverDeserializationError> {
        let Some(v) = v else {
            return Ok(PyDeserializedValue::new(PyDict::new(py).into_any()));
        };

        let map_iter = MapIterator::<
            FrameSliceWithMetadata<'frame, 'metadata>,
            FrameSliceWithMetadata<'frame, 'metadata>,
        >::deserialize(typ, Some(v))
        .map_err(DriverDeserializationError::scylla_decode_failed)?;

        let dict = PyDict::new(py);

        for (i, kv_result) in map_iter.enumerate() {
            let (raw_key_with_metadata, raw_value_with_metadata) = kv_result
                .map_err(DriverDeserializationError::scylla_decode_failed)
                .map_err(|e| e.in_map_index(i))?;

            let key = K::deserialize_py(
                raw_key_with_metadata.column_type,
                raw_key_with_metadata.frame_slice,
                py,
            )
            .map_err(|e| e.in_map_index(i))?;

            let value = V::deserialize_py(
                raw_value_with_metadata.column_type,
                raw_value_with_metadata.frame_slice,
                py,
            )
            .map_err(|e| e.in_map_index(i))?;

            dict.set_item(key, value)
                .map_err(DriverDeserializationError::python_conversion_failed)
                .map_err(|e| e.in_map_index(i))?;
        }

        Ok(PyDeserializedValue::new(dict.into_any()))
    }
}

struct Set<T> {
    phantom_data: PhantomData<T>,
}

impl<'frame, 'metadata, 'py, T> PyDeserializeValue<'frame, 'metadata, 'py> for Set<T>
where
    T: PyDeserializeValue<'frame, 'metadata, 'py>,
{
    fn deserialize_py(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
        py: Python<'py>,
    ) -> Result<PyDeserializedValue, DriverDeserializationError> {
        let Some(v) = v else {
            return Ok(PyDeserializedValue::new(
                PySet::empty(py)
                    .map_err(DriverDeserializationError::python_conversion_failed)?
                    .into_any(),
            ));
        };

        let set = PySet::empty(py).map_err(DriverDeserializationError::python_conversion_failed)?;

        deserialize_sequence::<T, _>(typ, v, py, |item| set.add(item))?;

        Ok(PyDeserializedValue::new(set.into_any()))
    }
}

struct Vector<T> {
    phantom_data: PhantomData<T>,
}

impl<'frame, 'metadata, 'py, T> PyDeserializeValue<'frame, 'metadata, 'py> for Vector<T>
where
    T: PyDeserializeValue<'frame, 'metadata, 'py>,
{
    fn deserialize_py(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
        py: Python<'py>,
    ) -> Result<PyDeserializedValue, DriverDeserializationError> {
        let Some(val) = v else {
            return Ok(PyDeserializedValue::none(py));
        };

        let vector_iterator =
            VectorIterator::<FrameSliceWithMetadata<'frame, 'metadata>>::deserialize(
                typ,
                Some(val),
            )
            .map_err(DriverDeserializationError::scylla_decode_failed)?;

        let list = PyList::empty(py);
        for (i, raw_value_with_metadata_result) in vector_iterator.enumerate() {
            let raw_value_with_metadata = raw_value_with_metadata_result
                .map_err(DriverDeserializationError::scylla_decode_failed)
                .map_err(|e| e.in_vector_index(i))?;

            let deserialized_value = T::deserialize_py(
                raw_value_with_metadata.column_type,
                raw_value_with_metadata.frame_slice,
                py,
            )
            .map_err(|e| e.in_vector_index(i))?;

            list.append(deserialized_value)
                .map_err(DriverDeserializationError::python_conversion_failed)
                .map_err(|e| e.in_vector_index(i))?;
        }

        Ok(PyDeserializedValue::new(list.into_any()))
    }
}

fn deser_cql_py_value<'py, 'metadata, 'frame>(
    py: Python<'py>,
    typ: &'metadata ColumnType<'metadata>,
    val: Option<FrameSlice<'frame>>,
) -> Result<PyDeserializedValue, DriverDeserializationError> {
    if let Some(v) = val
        && v.as_slice().is_empty()
    {
        match typ {
            Native(NativeType::Ascii) | Native(NativeType::Blob) | Native(NativeType::Text) => {
                // can't be empty
            }
            _ => {
                return PyDeserializedValue::empty_value(py)
                    .map_err(DriverDeserializationError::python_conversion_failed);
            }
        }
    }

    Ok(match typ {
        Native(native_type) => {
            let Some(v) = val else {
                return Ok(PyDeserializedValue::none(py));
            };

            PyDeserializedValue::new({
                match native_type {
                    // CQL Counter → Python int
                    NativeType::Counter => {
                        let v = Counter::deserialize(typ, Some(v))
                            .map_err(DriverDeserializationError::scylla_decode_failed)?;
                        PyInt::new(py, v.0).into_any()
                    }
                    // CQL Decimal → Python decimal.Decimal
                    NativeType::Decimal => {
                        let d: BigDecimal = CqlDecimalBorrowed::deserialize(typ, Some(v))
                            .map_err(DriverDeserializationError::scylla_decode_failed)?
                            .into();
                        d.into_pyobject(py)
                            .map_err(DriverDeserializationError::python_conversion_failed)?
                            .into_any()
                    }
                    // CQL TinyInt → Python int
                    NativeType::TinyInt => {
                        let v = i8::deserialize(typ, Some(v))
                            .map_err(DriverDeserializationError::scylla_decode_failed)?;
                        PyInt::new(py, v).into_any()
                    }
                    // CQL SmallInt → Python int
                    NativeType::SmallInt => {
                        let v = i16::deserialize(typ, Some(v))
                            .map_err(DriverDeserializationError::scylla_decode_failed)?;
                        PyInt::new(py, v).into_any()
                    }
                    // CQL Int → Python int
                    NativeType::Int => {
                        let v = i32::deserialize(typ, Some(v))
                            .map_err(DriverDeserializationError::scylla_decode_failed)?;
                        PyInt::new(py, v).into_any()
                    }
                    // CQL BigInt → Python int
                    NativeType::BigInt => {
                        let v = i64::deserialize(typ, Some(v))
                            .map_err(DriverDeserializationError::scylla_decode_failed)?;
                        PyInt::new(py, v).into_any()
                    }
                    // CQL Varint → Python int
                    NativeType::Varint => {
                        let varint: CqlVarintWrapper = CqlVarintBorrowed::deserialize(typ, Some(v))
                            .map_err(DriverDeserializationError::scylla_decode_failed)?
                            .into();
                        varint
                            .into_pyobject(py)
                            .map_err(DriverDeserializationError::python_conversion_failed)?
                            .into_any()
                    }
                    // CQL Double → Python float
                    NativeType::Double => {
                        let v = f64::deserialize(typ, Some(v))
                            .map_err(DriverDeserializationError::scylla_decode_failed)?;
                        PyFloat::new(py, v).into_any()
                    }
                    // CQL Float → Python float
                    NativeType::Float => {
                        let v = f32::deserialize(typ, Some(v))
                            .map_err(DriverDeserializationError::scylla_decode_failed)?;
                        PyFloat::new(py, v as f64).into_any()
                    }
                    // CQL Ascii → Python str
                    // CQL Text → Python str
                    NativeType::Ascii | NativeType::Text => {
                        let v = <&str as DeserializeValue<'frame, 'metadata>>::deserialize(
                            typ,
                            Some(v),
                        )
                        .map_err(DriverDeserializationError::scylla_decode_failed)?;
                        PyString::new(py, v).into_any()
                    }
                    // CQL Boolean → Python bool
                    NativeType::Boolean => {
                        let v = bool::deserialize(typ, Some(v))
                            .map_err(DriverDeserializationError::scylla_decode_failed)?;
                        PyBool::new(py, v).to_owned().into_any()
                    }
                    // CQL Date → Python scylla.cql_types.CqlDate
                    NativeType::Date => {
                        let d = CqlDate::deserialize(typ, Some(v))
                            .map_err(DriverDeserializationError::scylla_decode_failed)?;
                        Bound::new(py, PyCqlDate::from(d))
                            .map_err(DriverDeserializationError::python_conversion_failed)?
                            .into_any()
                    }
                    // CQL Timestamp → Python scylla.cql_types.CqlTimestamp
                    NativeType::Timestamp => {
                        let t = CqlTimestamp::deserialize(typ, Some(v))
                            .map_err(DriverDeserializationError::scylla_decode_failed)?;
                        Bound::new(py, PyCqlTimestamp::from(t))
                            .map_err(DriverDeserializationError::python_conversion_failed)?
                            .into_any()
                    }
                    // CQL Time → Python scylla.cql_types.CqlTime
                    NativeType::Time => {
                        let t = CqlTime::deserialize(typ, Some(v))
                            .map_err(DriverDeserializationError::scylla_decode_failed)?;
                        Bound::new(py, PyCqlTime::from(t))
                            .map_err(DriverDeserializationError::python_conversion_failed)?
                            .into_any()
                    }
                    // CQL Duration → Python scylla.cql_types.CqlDuration
                    NativeType::Duration => {
                        let d = CqlDuration::deserialize(typ, Some(v))
                            .map_err(DriverDeserializationError::scylla_decode_failed)?;
                        Bound::new(py, PyCqlDuration::from(d))
                            .map_err(DriverDeserializationError::python_conversion_failed)?
                            .into_any()
                    }
                    // CQL Blob → Python bytes
                    NativeType::Blob => PyBytes::new(py, v.as_slice()).into_any(),
                    // CQL Inet → Python ipaddress.IPv4Address / ipaddress.IPv6Address
                    NativeType::Inet => {
                        let v = IpAddr::deserialize(typ, Some(v))
                            .map_err(DriverDeserializationError::scylla_decode_failed)?;
                        v.into_pyobject(py)
                            .map_err(DriverDeserializationError::python_conversion_failed)?
                            .into_any()
                    }
                    // CQL Uuid → Python uuid.UUID
                    NativeType::Uuid => {
                        let v = uuid::Uuid::deserialize(typ, Some(v))
                            .map_err(DriverDeserializationError::scylla_decode_failed)?;
                        v.into_pyobject(py)
                            .map_err(DriverDeserializationError::python_conversion_failed)?
                            .into_any()
                    }
                    // CQL Uuid → Python uuid.UUID
                    NativeType::Timeuuid => {
                        let v: uuid::Uuid = CqlTimeuuid::deserialize(typ, Some(v))
                            .map_err(DriverDeserializationError::scylla_decode_failed)?
                            .into();
                        v.into_pyobject(py)
                            .map_err(DriverDeserializationError::python_conversion_failed)?
                            .into_any()
                    }
                    _ => {
                        return Err(DriverDeserializationError::unsupported_type(format!(
                            "{typ:?}"
                        )));
                    }
                }
            })
        }
        ColumnType::Collection {
            frozen: _frozen,
            typ: col_typ,
        } => match col_typ {
            // CQL List → Python list
            CollectionType::List(_type_name) => {
                List::<PyDeserializedValue>::deserialize_py(typ, val, py)?
            }
            // CQL Map → Python dict
            CollectionType::Map(_key_type, _value_type) => {
                Map::<PyDeserializedValue, PyDeserializedValue>::deserialize_py(typ, val, py)?
            }
            // CQL Set → Python set
            CollectionType::Set(_type_name) => {
                Set::<PyDeserializedValue>::deserialize_py(typ, val, py)?
            }
            _ => {
                return Err(DriverDeserializationError::unsupported_type(format!(
                    "{col_typ:?}"
                )));
            }
        },
        // CQL UserDefinedType (UDT) → Python dict[str, value]
        ColumnType::UserDefinedType {
            definition: _udt, ..
        } => {
            let Some(v) = val else {
                return Ok(PyDeserializedValue::none(py));
            };

            let iter = UdtIterator::deserialize(typ, Some(v))
                .map_err(DriverDeserializationError::scylla_decode_failed)?;

            let dict = PyDict::new(py);

            for ((col_name, col_type), res) in iter {
                let v = res
                    .map_err(DriverDeserializationError::scylla_decode_failed)
                    .map_err(|e| e.in_udt_field(col_name.clone()))?;

                let val = PyDeserializedValue::deserialize_py(col_type, v.flatten(), py)
                    .map_err(|e| e.in_udt_field(col_name.clone()))?;

                dict.set_item(col_name.clone(), val)
                    .map_err(DriverDeserializationError::python_conversion_failed)
                    .map_err(|e| e.in_udt_field(col_name.clone()))?;
            }

            PyDeserializedValue::new(dict.into_any())
        }
        // CQL Vector → Python list
        ColumnType::Vector { .. } => Vector::<PyDeserializedValue>::deserialize_py(typ, val, py)?,
        // CQL Tuple → Python tuple
        ColumnType::Tuple(type_names) => {
            let Some(mut v) = val else {
                return Ok(PyDeserializedValue::none(py));
            };

            let t = type_names.iter().enumerate().map(|(i, typ)| {
                let result: Result<PyDeserializedValue, DriverDeserializationError> = v
                    .read_cql_bytes()
                    // low-level → DeserializationError
                    .map_err(DeserializationError::new)
                    // DeserializationError → DriverDeserializationError
                    .map_err(DriverDeserializationError::scylla_decode_failed)
                    // Option<&[u8]> → PyDeserializedValue
                    .and_then(|raw| PyDeserializedValue::deserialize_py(typ, raw, py))
                    // Add context about which tuple index failed
                    .map_err(|e| e.in_tuple_index(i));
                PyValueOrError::new(result)
            });

            let tuple = PyTuple::new(py, t)
                .map_err(DriverDeserializationError::python_conversion_failed)?;
            PyDeserializedValue::new(tuple.into_any())
        }
        _ => {
            return Err(DriverDeserializationError::unsupported_type(format!(
                "{typ:?}"
            )));
        }
    })
}
