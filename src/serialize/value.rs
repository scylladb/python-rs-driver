use crate::cql_types::{
    PyCqlDate, PyCqlDuration, PyCqlTime, PyCqlTimestamp, aware_datetime_to_utc,
};
use crate::relativedelta::{Relativedelta, RelativedeltaError};
use crate::serialize::error::{DriverSerializationError, TypeExpected};

use std::any::Any;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::ops::Deref;
use std::sync::Arc;

use bigdecimal::BigDecimal;
use bigdecimal::num_bigint::BigInt;
use chrono::{NaiveDate, NaiveTime};
use uuid::Uuid;

use pyo3::Bound;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{
    PyAny, PyBytes, PyDateTime, PyDict, PyInt, PyList, PyMapping, PySet, PyString, PyTuple, PyType,
};

use scylla::cluster::metadata::{CollectionType, ColumnType, NativeType, UserDefinedType};
use scylla::serialize::SerializationError;
use scylla::serialize::value::{
    BuiltinSerializationError, BuiltinSerializationErrorKind, BuiltinTypeCheckError,
    BuiltinTypeCheckErrorKind, MapSerializationErrorKind, SerializeValue,
    SetOrListSerializationErrorKind, TupleTypeCheckErrorKind, UdtTypeCheckErrorKind,
};
use scylla::serialize::writers::{CellWriter, WrittenCellProof};
use scylla::value::{Counter, CqlDate, CqlDuration, CqlTime, CqlTimestamp, CqlTimeuuid, CqlValue};

use scylla_cql::serialize::value::{
    VectorSerializationErrorKind, serialize_next_constant_length_elem_unstable,
    serialize_next_variable_length_elem_unstable,
};

/// Wrapper around a Python value (`PyAny`) used for Python → CQL serialization.
///
/// This type performs runtime type inspection and dispatches the value to the
/// appropriate serializer based on the target CQL `ColumnType`.
#[derive(Debug)]
pub(super) struct PyAnyWrapper<'a, 'py>(&'a Bound<'py, PyAny>);

impl<'a, 'py> Deref for PyAnyWrapper<'a, 'py> {
    type Target = &'a Bound<'py, PyAny>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a, 'py> PyAnyWrapper<'a, 'py> {
    pub(super) fn new(inner: &'a Bound<'py, PyAny>) -> Self {
        Self(inner)
    }

    #[deny(clippy::wildcard_enum_match_arm)]
    /// Dispatcher that inspects ColumnType and, based on that, forwards the value
    /// to the appropriate serializer.
    fn serialize_arbitrary_value<'b>(
        &self,
        typ: &ColumnType,
        cell_writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        match typ {
            ColumnType::Native(native_type) => {
                self.serialize_natives(typ, native_type, cell_writer)
            }

            ColumnType::Collection {
                typ: collection_typ,
                ..
            } => match collection_typ {
                CollectionType::List(_) => {
                    let list = PyListWrapper::new(self).map_err(SerializationError::from)?;

                    list.serialize(typ, cell_writer)
                }

                CollectionType::Map(_, _) => {
                    let map = PyMapWrapper::new(self).map_err(SerializationError::from)?;

                    map.serialize(typ, cell_writer)
                }

                CollectionType::Set(_) => {
                    let set = PySetWrapper::new(self).map_err(SerializationError::from)?;

                    set.serialize(typ, cell_writer)
                }

                _ => {
                    let name = format!("{:?}", collection_typ);

                    Err(DriverSerializationError::unsupported_type(name).into())
                }
            },

            ColumnType::Vector {
                typ: element_typ,
                dimensions,
            } => {
                let vector = PyVectorWrapper::new(self, *dimensions, element_typ)
                    .map_err(SerializationError::from)?;

                vector.serialize(typ, cell_writer)
            }

            // Supports UDTs passed as Python dicts.
            // For Python dataclass instances, convert to dict first (e.g., using dataclasses.asdict()).
            ColumnType::UserDefinedType { definition, .. } => {
                let Ok(dict) = PyUdtWrapper::new(self, definition) else {
                    return Err(DriverSerializationError::type_mismatch(TypeExpected::Udt).into());
                };

                dict.serialize(typ, cell_writer)
            }

            ColumnType::Tuple(elements_types) => {
                let Ok(tuple) = PyTupleWrapper::new(self, elements_types) else {
                    return Err(DriverSerializationError::type_mismatch(TypeExpected::Tuple).into());
                };

                tuple.serialize(typ, cell_writer)
            }
            _ => Err(DriverSerializationError::unsupported_type(format!("{typ:?}")).into()),
        }
    }

    #[deny(clippy::wildcard_enum_match_arm)]
    fn serialize_natives<'b>(
        &self,
        typ: &ColumnType,
        native_type: &NativeType,
        cell_writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        match native_type {
            // Integer types.
            NativeType::TinyInt => self.serialize_int::<i8>(typ, cell_writer),
            NativeType::SmallInt => self.serialize_int::<i16>(typ, cell_writer),
            NativeType::Int => self.serialize_int::<i32>(typ, cell_writer),
            NativeType::BigInt => self.serialize_int::<i64>(typ, cell_writer),
            NativeType::Counter => {
                let value = self
                    .cast::<PyInt>()
                    .map_err(|_| self.mismatched_type_error::<Counter>(typ))?
                    .extract::<i64>()
                    .map_err(|_| DriverSerializationError::value_overflow())?;

                let counter = Counter(value);
                counter.serialize(typ, cell_writer)
            }
            NativeType::Varint => self.serialize_native::<BigInt>(typ, cell_writer),

            // Float types.
            NativeType::Float => self.serialize_native::<f32>(typ, cell_writer),
            NativeType::Double => self.serialize_native::<f64>(typ, cell_writer),
            NativeType::Decimal => self.serialize_native::<BigDecimal>(typ, cell_writer),

            // Boolean type.
            NativeType::Boolean => self.serialize_native::<bool>(typ, cell_writer),

            // Text types.
            // TODO: Python allows strings that are not valid in Rust, conversion to `&str` is fallible.
            // This case is currently ignored and should be handled in the future. See: #41
            NativeType::Ascii | NativeType::Text => self.serialize_native::<&str>(typ, cell_writer),

            // Binary data type.
            NativeType::Blob => {
                let value = self
                    .cast::<PyBytes>()
                    .map_err(|_| self.mismatched_type_error::<PyBytes>(typ))?;

                let bytes = value.as_bytes();

                bytes.serialize(typ, cell_writer)
            }

            // Datatime types.
            NativeType::Date => match self.cast::<PyCqlDate>() {
                Ok(date) => CqlDate::from(*date.get()).serialize(typ, cell_writer),
                Err(_) => self.serialize_native::<NaiveDate>(typ, cell_writer),
            },
            NativeType::Duration => {
                let duration = match self.cast::<PyCqlDuration>() {
                    Ok(duration) => *duration.get(),
                    Err(_) => self
                        .extract::<Relativedelta>()
                        .and_then(PyCqlDuration::try_from)
                        .map_err(|err| self.relativedelta_error(err, typ))?,
                };
                CqlDuration::from(duration).serialize(typ, cell_writer)
            }
            NativeType::Time => {
                let time = match self.cast::<PyCqlTime>() {
                    Ok(time) => CqlTime::from(*time.get()),
                    Err(_) => self
                        .extract::<NaiveTime>()
                        .map_err(|_| self.mismatched_type_error::<CqlTime>(typ))?
                        .try_into()
                        .map_err(|_| self.mismatched_type_error::<CqlTime>(typ))?,
                };

                time.serialize(typ, cell_writer)
            }
            NativeType::Timestamp => {
                let timestamp = match self.cast::<PyCqlTimestamp>() {
                    Ok(timestamp) => CqlTimestamp::from(*timestamp.get()),
                    Err(_) => self
                        .cast::<PyDateTime>()
                        .map_err(PyErr::from)
                        .and_then(aware_datetime_to_utc)
                        .map_err(|_| self.mismatched_type_error::<CqlTimestamp>(typ))?
                        .into(),
                };

                timestamp.serialize(typ, cell_writer)
            }

            // IP address type.
            NativeType::Inet => {
                let ip = self
                    .ip_address()
                    .map_err(DriverSerializationError::python_interop_failed)?
                    .ok_or_else(|| self.mismatched_type_error::<IpAddr>(typ))?;

                ip.serialize(typ, cell_writer)
            }

            // UUID types.
            NativeType::Timeuuid => {
                let uuid = self
                    .extract::<Uuid>()
                    .map_err(|_| self.mismatched_type_error::<CqlTimeuuid>(typ))?;
                CqlTimeuuid::from(uuid).serialize(typ, cell_writer)
            }
            NativeType::Uuid => self.serialize_native::<Uuid>(typ, cell_writer),

            _ => {
                let name = format!("{:?}", native_type);

                Err(DriverSerializationError::unsupported_type(name).into())
            }
        }
    }

    fn serialize_native<'b, T>(
        &self,
        typ: &ColumnType,
        cell_writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError>
    where
        T: pyo3::FromPyObject<'a, 'py> + SerializeValue,
    {
        self.extract::<T>()
            .map_err(|_| self.mismatched_type_error::<T>(typ))?
            .serialize(typ, cell_writer)
    }

    fn serialize_int<'b, T>(
        &self,
        typ: &ColumnType,
        cell_writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError>
    where
        T: pyo3::FromPyObject<'a, 'py> + SerializeValue,
    {
        // The cast to `PyInt` is performed to distinguish between two different error cases:
        // `MismatchedType` and `ValueOverflow`.
        self.cast::<PyInt>()
            .map_err(|_| self.mismatched_type_error::<T>(typ))?
            .extract::<T>()
            .map_err(|_| DriverSerializationError::value_overflow())?
            .serialize(typ, cell_writer)
    }

    /// Reads an `ipaddress.IPv4Address` or `IPv6Address`; `None` for any other type.
    fn ip_address(&self) -> PyResult<Option<IpAddr>> {
        static IPV4_ADDRESS: PyOnceLock<Py<PyType>> = PyOnceLock::new();
        static IPV6_ADDRESS: PyOnceLock<Py<PyType>> = PyOnceLock::new();

        let py = self.py();
        let ip = if self.is_instance(IPV4_ADDRESS.import(py, "ipaddress", "IPv4Address")?)? {
            let int = self.call_method0(intern!(py, "__int__"))?;
            IpAddr::V4(Ipv4Addr::from(int.extract::<u32>()?))
        } else if self.is_instance(IPV6_ADDRESS.import(py, "ipaddress", "IPv6Address")?)? {
            let int = self.call_method0(intern!(py, "__int__"))?;
            IpAddr::V6(Ipv6Addr::from(int.extract::<u128>()?))
        } else {
            return Ok(None);
        };
        Ok(Some(ip))
    }

    fn python_type_name(&self) -> Result<Bound<'py, PyString>, SerializationError> {
        self.get_type()
            .name()
            .map_err(|err| DriverSerializationError::python_interop_failed(err).into())
    }

    /// Maps Python type to the list of CQL types that this type can serialize as.
    ///
    /// Supports only Python types that serialize to native CQL types.
    /// Returns an error if the Python type does not correspond to a native CQL type.
    fn map_type_to_expected(&self) -> Result<&'static [ColumnType<'static>], SerializationError> {
        let name = self.python_type_name()?;

        let name = name
            .extract::<&str>()
            .map_err(DriverSerializationError::python_interop_failed)?;

        let columns = match name {
            "int" => INT_COLUMNS,
            "float" => FLOAT_COLUMNS,
            "bool" => BOOL_COLUMNS,
            "str" => STRING_COLUMNS,
            "bytes" => BYTES_COLUMNS,
            "Decimal" => DECIMAL_COLUMNS,
            "CqlDuration" | "relativedelta" => DURATION_COLUMNS,
            "CqlDate" | "date" => DATE_COLUMNS,
            "datetime" => DATETIME_COLUMNS,
            "CqlTimestamp" => TIMESTAMP_COLUMNS,
            "IPv4Address" | "IPv6Address" => IP_COLUMNS,
            "CqlTime" | "time" => TIME_COLUMNS,
            "UUID" => UUID_COLUMNS,

            _ => {
                return Err(DriverSerializationError::unsupported_type(name.to_string()).into());
            }
        };

        Ok(columns)
    }

    fn relativedelta_error(&self, err: RelativedeltaError, typ: &ColumnType) -> SerializationError {
        match err {
            RelativedeltaError::NotRelativedelta => self.mismatched_type_error::<CqlDuration>(typ),
            RelativedeltaError::Overflow => DriverSerializationError::value_overflow().into(),
            RelativedeltaError::MixedSign => DriverSerializationError::mixed_sign_duration().into(),
        }
    }

    fn mismatched_type_error<T: ?Sized>(&self, typ: &ColumnType) -> SerializationError {
        let expected = match self.map_type_to_expected() {
            Ok(expected) => expected,
            Err(e) => return e,
        };
        mk_typck_err::<T>(typ, BuiltinTypeCheckErrorKind::MismatchedType { expected })
    }
}

impl<'a, 'py> SerializeValue for PyAnyWrapper<'a, 'py> {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        cell_writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        if self.is_none() {
            return Ok(cell_writer.set_null());
        }

        self.serialize_arbitrary_value(typ, cell_writer)
    }
}

fn serialize_sequence<'t, 'b, 'py, T: Any>(
    len: usize,
    iter: impl Iterator<Item = Bound<'py, PyAny>>,
    typ: &ColumnType,
    writer: CellWriter<'b>,
) -> Result<WrittenCellProof<'b>, SerializationError> {
    let ColumnType::Collection {
        typ: CollectionType::List(elt) | CollectionType::Set(elt),
        ..
    } = typ
    else {
        unreachable!("typ is guaranteed to be Set or List");
    };

    let mut builder = writer.into_value_builder();

    let element_count: i32 = len
        .try_into()
        .map_err(|_| mk_ser_err::<T>(typ, SetOrListSerializationErrorKind::TooManyElements))?;
    builder.append_bytes(&element_count.to_be_bytes());

    for el in iter {
        PyAnyWrapper::serialize(&PyAnyWrapper::new(&el), elt, builder.make_sub_writer()).map_err(
            |err| {
                mk_ser_err::<T>(
                    typ,
                    SetOrListSerializationErrorKind::ElementSerializationFailed(err),
                )
            },
        )?;
    }

    builder
        .finish()
        .map_err(|_| mk_ser_err::<T>(typ, BuiltinSerializationErrorKind::SizeOverflow))
}

pub fn serialize_vector<'t, 'b, 'py, T: Any>(
    len: usize,
    iter: impl Iterator<Item = Bound<'py, PyAny>>,
    element_type: &ColumnType,
    dimensions: u16,
    typ: &ColumnType,
    writer: CellWriter<'b>,
) -> Result<WrittenCellProof<'b>, SerializationError> {
    if len != dimensions as usize {
        return Err(mk_ser_err::<T>(
            typ,
            VectorSerializationErrorKind::InvalidNumberOfElements(len, dimensions),
        ));
    }
    let mut builder = writer.into_value_builder();
    match element_type.type_size_for_vector() {
        Some(_) => {
            for element in iter {
                serialize_next_constant_length_elem_unstable(
                    std::any::type_name::<T>(),
                    element_type,
                    typ,
                    &mut builder,
                    &PyAnyWrapper::new(&element),
                )?;
            }
        }
        None => {
            for element in iter {
                serialize_next_variable_length_elem_unstable(
                    std::any::type_name::<T>(),
                    element_type,
                    typ,
                    &mut builder,
                    &PyAnyWrapper::new(&element),
                )?;
            }
        }
    }

    builder
        .finish()
        .map_err(|_| mk_ser_err::<T>(typ, BuiltinSerializationErrorKind::SizeOverflow))
}

#[derive(Debug)]
struct PyListWrapper<'a, 'py> {
    inner: &'a Bound<'py, PyList>,
}

impl<'a, 'py> Deref for PyListWrapper<'a, 'py> {
    type Target = Bound<'py, PyList>;

    fn deref(&self) -> &Self::Target {
        self.inner
    }
}

impl<'a, 'py> PyListWrapper<'a, 'py> {
    fn new(value: &PyAnyWrapper<'a, 'py>) -> Result<Self, DriverSerializationError> {
        let list: &Bound<PyList> = value
            .cast::<PyList>()
            .map_err(|_| DriverSerializationError::type_mismatch(TypeExpected::List))?;
        Ok(PyListWrapper { inner: list })
    }
}

impl<'a, 'py> SerializeValue for PyListWrapper<'a, 'py> {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        cell_writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        let items = self.iter();

        serialize_sequence::<PyList>(items.len(), items, typ, cell_writer)
    }
}

struct PySetWrapper<'py, 'a>(&'a Bound<'py, PySet>);

impl<'a, 'py> Deref for PySetWrapper<'a, 'py> {
    type Target = Bound<'py, PySet>;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl<'a, 'py> PySetWrapper<'a, 'py> {
    fn new(value: &PyAnyWrapper<'a, 'py>) -> Result<Self, DriverSerializationError> {
        let set: &Bound<PySet> = value
            .cast::<PySet>()
            .map_err(|_| DriverSerializationError::type_mismatch(TypeExpected::Set))?;
        Ok(PySetWrapper(set))
    }
}

impl<'a, 'py> SerializeValue for PySetWrapper<'a, 'py> {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        cell_writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        let items = self.iter();

        serialize_sequence::<PySet>(items.len(), items, typ, cell_writer)
    }
}

struct PyMapWrapper<'a, 'py>(&'a Bound<'py, PyMapping>);

impl<'a, 'py> Deref for PyMapWrapper<'a, 'py> {
    type Target = Bound<'py, PyMapping>;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl<'a, 'py> PyMapWrapper<'a, 'py> {
    fn new(value: &PyAnyWrapper<'a, 'py>) -> Result<Self, DriverSerializationError> {
        let map: &Bound<PyMapping> = value
            .cast::<PyMapping>()
            .map_err(|_| DriverSerializationError::type_mismatch(TypeExpected::Map))?;
        Ok(PyMapWrapper(map))
    }
}

impl<'a, 'py> SerializeValue for PyMapWrapper<'a, 'py> {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        cell_writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        let ColumnType::Collection {
            typ: CollectionType::Map(ktyp, vtyp),
            ..
        } = typ
        else {
            unreachable!("typ is guaranteed to be Map");
        };

        let mut builder = cell_writer.into_value_builder();

        let items = self
            .items()
            .map_err(DriverSerializationError::python_interop_failed)?;

        let element_count: i32 = items.len().try_into().map_err(|_| {
            mk_ser_err::<PyMapping>(typ, MapSerializationErrorKind::TooManyElements)
        })?;
        builder.append_bytes(&element_count.to_be_bytes());

        for pair in items {
            let (key, value) = pair
                .extract::<(Bound<'py, PyAny>, Bound<'py, PyAny>)>()
                .map_err(DriverSerializationError::python_interop_failed)?;
            PyAnyWrapper::serialize(&PyAnyWrapper::new(&key), ktyp, builder.make_sub_writer())
                .map_err(|err| {
                    mk_ser_err::<PyMapping>(
                        typ,
                        MapSerializationErrorKind::KeySerializationFailed(err),
                    )
                })?;
            PyAnyWrapper::serialize(&PyAnyWrapper::new(&value), vtyp, builder.make_sub_writer())
                .map_err(|err| {
                    mk_ser_err::<PyMapping>(
                        typ,
                        MapSerializationErrorKind::ValueSerializationFailed(err),
                    )
                })?;
        }

        builder
            .finish()
            .map_err(|_| mk_ser_err::<PyMapping>(typ, BuiltinSerializationErrorKind::SizeOverflow))
    }
}

/// The Python sequence of a vector. Each kind keeps its own iterator, which reads
/// the items directly.
#[derive(Debug)]
enum VectorItems<'a, 'py> {
    List(&'a Bound<'py, PyList>),
    Tuple(&'a Bound<'py, PyTuple>),
}

#[derive(Debug)]
struct PyVectorWrapper<'py, 'a> {
    items: VectorItems<'a, 'py>,
    dimension: u16,
    element_type: &'a ColumnType<'a>,
}

impl<'py, 'a> PyVectorWrapper<'py, 'a> {
    fn new(
        value: &PyAnyWrapper<'a, 'py>,
        dimension: u16,
        element_type: &'a ColumnType<'a>,
    ) -> Result<Self, DriverSerializationError> {
        let items = if let Ok(list) = value.cast::<PyList>() {
            VectorItems::List(list)
        } else {
            let tuple = value
                .cast::<PyTuple>()
                .map_err(|_| DriverSerializationError::type_mismatch(TypeExpected::Vector))?;
            VectorItems::Tuple(tuple)
        };
        Ok(PyVectorWrapper {
            items,
            dimension,
            element_type,
        })
    }
}

impl<'py, 'a> SerializeValue for PyVectorWrapper<'py, 'a> {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        cell_writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        match self.items {
            VectorItems::List(list) => {
                let items = list.iter();
                serialize_vector::<PyList>(
                    items.len(),
                    items,
                    self.element_type,
                    self.dimension,
                    typ,
                    cell_writer,
                )
            }
            VectorItems::Tuple(tuple) => {
                let items = tuple.iter();
                serialize_vector::<PyTuple>(
                    items.len(),
                    items,
                    self.element_type,
                    self.dimension,
                    typ,
                    cell_writer,
                )
            }
        }
    }
}

struct PyUdtWrapper<'py, 'a> {
    inner: &'a Bound<'py, PyDict>,
    definition: &'a Arc<UserDefinedType<'a>>,
}

impl<'py> Deref for PyUdtWrapper<'py, '_> {
    type Target = Bound<'py, PyDict>;
    fn deref(&self) -> &Self::Target {
        self.inner
    }
}

impl<'py, 'a> PyUdtWrapper<'py, 'a> {
    fn new(
        value: &PyAnyWrapper<'a, 'py>,
        definition: &'a Arc<UserDefinedType<'a>>,
    ) -> Result<Self, DriverSerializationError> {
        let dict: &Bound<PyDict> = value
            .cast::<PyDict>()
            .map_err(|_| DriverSerializationError::type_mismatch(TypeExpected::Udt))?;
        Ok(PyUdtWrapper {
            inner: dict,
            definition,
        })
    }
}

impl<'py> SerializeValue for PyUdtWrapper<'py, '_> {
    fn serialize<'a>(
        &self,
        typ: &ColumnType,
        cell_writer: CellWriter<'a>,
    ) -> Result<WrittenCellProof<'a>, SerializationError> {
        let mut builder = cell_writer.into_value_builder();

        for (field_name, field_type) in &self.definition.field_types {
            let item: Bound<PyAny> = self
                .inner
                .get_item(field_name)
                .map_err(DriverSerializationError::python_interop_failed)?
                .ok_or_else(|| {
                    mk_typck_err::<PyDict>(
                        typ,
                        UdtTypeCheckErrorKind::ValueMissingForUdtField {
                            field_name: field_name.to_string(),
                        },
                    )
                })?;

            PyAnyWrapper::new(&item)
                .serialize_arbitrary_value(field_type, builder.make_sub_writer())?;
        }

        builder
            .finish()
            .map_err(|_| mk_ser_err::<CqlValue>(typ, BuiltinSerializationErrorKind::SizeOverflow))
    }
}

#[derive(Debug)]
struct PyTupleWrapper<'py, 'a> {
    inner: &'a Bound<'py, PyTuple>,
    elements_types: &'a Vec<ColumnType<'a>>,
}

impl<'py> Deref for PyTupleWrapper<'py, '_> {
    type Target = Bound<'py, PyTuple>;

    fn deref(&self) -> &Self::Target {
        self.inner
    }
}

impl<'py, 'a> PyTupleWrapper<'py, 'a> {
    fn new(
        value: &PyAnyWrapper<'a, 'py>,
        elements_types: &'a Vec<ColumnType<'_>>,
    ) -> Result<Self, DriverSerializationError> {
        let tuple: &Bound<PyTuple> = value
            .cast::<PyTuple>()
            .map_err(|_| DriverSerializationError::type_mismatch(TypeExpected::Tuple))?;
        Ok(PyTupleWrapper {
            inner: tuple,
            elements_types,
        })
    }
}

impl<'py, 'a> SerializeValue for PyTupleWrapper<'py, 'a> {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        cell_writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        // A shorter tuple is allowed: the database reads the missing elements as null.
        if self.inner.len() > self.elements_types.len() {
            return Err(mk_typck_err::<PyTuple>(
                typ,
                TupleTypeCheckErrorKind::WrongElementCount {
                    rust_type_el_count: self.inner.len(),
                    cql_type_el_count: self.elements_types.len(),
                },
            ));
        }

        let mut builder = cell_writer.into_value_builder();

        for (val, element_type) in self.inner.iter().zip(self.elements_types) {
            PyAnyWrapper::new(&val)
                .serialize_arbitrary_value(element_type, builder.make_sub_writer())?;
        }

        builder
            .finish()
            .map_err(|_| mk_ser_err::<CqlValue>(typ, BuiltinSerializationErrorKind::SizeOverflow))
    }
}

// List of CQL column types used to provide clear error messages
// indicating which CQL types are compatible with Python `int` type.
static INT_COLUMNS: &[ColumnType<'static>] = &[
    ColumnType::Native(NativeType::TinyInt),
    ColumnType::Native(NativeType::SmallInt),
    ColumnType::Native(NativeType::Int),
    ColumnType::Native(NativeType::BigInt),
    ColumnType::Native(NativeType::Counter),
    ColumnType::Native(NativeType::Varint),
];

// List of CQL column types used to provide clear error messages
// indicating which CQL types are compatible with Python `float` type.
static FLOAT_COLUMNS: &[ColumnType<'static>] = &[
    ColumnType::Native(NativeType::Double),
    ColumnType::Native(NativeType::Float),
];

// List of CQL column types used to provide clear error messages
// indicating which CQL types are compatible with Python `Decimal` type.
static DECIMAL_COLUMNS: &[ColumnType<'static>] = &[ColumnType::Native(NativeType::Decimal)];

// List of CQL column types used to provide clear error messages
// indicating which CQL types are compatible with Python `str` type.
static STRING_COLUMNS: &[ColumnType<'static>] = &[
    ColumnType::Native(NativeType::Ascii),
    ColumnType::Native(NativeType::Text),
];

// List of CQL column types used to provide clear error messages
// indicating which CQL types are compatible with Python `bool` type.
static BOOL_COLUMNS: &[ColumnType<'static>] = &[ColumnType::Native(NativeType::Boolean)];

// List of CQL column types used to provide clear error messages
// indicating which CQL types are compatible with Python `bytes` type.
static BYTES_COLUMNS: &[ColumnType<'static>] = &[ColumnType::Native(NativeType::Blob)];

// List of CQL column types used to provide clear error messages
// indicating which CQL types are compatible with Python `CqlDuration` and `relativedelta` types.
static DURATION_COLUMNS: &[ColumnType<'static>] = &[ColumnType::Native(NativeType::Duration)];

// List of CQL column types used to provide clear error messages
// indicating which CQL types are compatible with Python `CqlDate` and `datetime.date` types.
static DATE_COLUMNS: &[ColumnType<'static>] = &[ColumnType::Native(NativeType::Date)];

// List of CQL column types used to provide clear error messages
// indicating which CQL types are compatible with Python `datetime.datetime` type.
static DATETIME_COLUMNS: &[ColumnType<'static>] = &[
    ColumnType::Native(NativeType::Date),
    ColumnType::Native(NativeType::Timestamp),
];

// List of CQL column types used to provide clear error messages
// indicating which CQL types are compatible with Python `CqlTimestamp` type.
static TIMESTAMP_COLUMNS: &[ColumnType<'static>] = &[ColumnType::Native(NativeType::Timestamp)];

// List of CQL column types used to provide clear error messages
// indicating which CQL types are compatible with Python `ipaddress.IPv4Address`
// and `ipaddress.IPv6Address` types.
static IP_COLUMNS: &[ColumnType<'static>] = &[ColumnType::Native(NativeType::Inet)];

// List of CQL column types used to provide clear error messages
// indicating which CQL types are compatible with Python `CqlTime` and `datetime.time` types.
static TIME_COLUMNS: &[ColumnType<'static>] = &[ColumnType::Native(NativeType::Time)];

// List of CQL column types used to provide clear error messages
// indicating which CQL types are compatible with Python `uuid.UUID` type.
static UUID_COLUMNS: &[ColumnType<'static>] = &[
    ColumnType::Native(NativeType::Uuid),
    ColumnType::Native(NativeType::Timeuuid),
];

fn mk_typck_err<T: ?Sized>(
    got: &ColumnType,
    kind: impl Into<BuiltinTypeCheckErrorKind>,
) -> SerializationError {
    SerializationError::new(BuiltinTypeCheckError {
        rust_name: std::any::type_name::<T>(),
        got: got.clone().into_owned(),
        kind: kind.into(),
    })
}

fn mk_ser_err<T: ?Sized>(
    got: &ColumnType,
    kind: impl Into<BuiltinSerializationErrorKind>,
) -> SerializationError {
    SerializationError::new(BuiltinSerializationError {
        rust_name: std::any::type_name::<T>(),
        got: got.clone().into_owned(),
        kind: kind.into(),
    })
}
