use crate::cluster::metadata::column_type::{PyCqlColumnType, extract_column_type};
use crate::cql_types::{
    PyCqlDate, PyCqlDuration, PyCqlTime, PyCqlTimestamp, aware_datetime_to_utc,
};
use crate::relativedelta::{Relativedelta, RelativedeltaError};
use crate::serialize::adapters::Adapters;
use crate::serialize::error::{DriverSerializationError, TypeExpected};

use std::any::{Any, type_name};
use std::cell::RefCell;
use std::collections::HashMap;
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

/// A cell writer that nothing has used yet.
///
/// Only the methods of [`Pending`] use it, so a rejected value is known to have written nothing.
pub(super) struct Pending<'b> {
    cell_writer: CellWriter<'b>,
    null: NullWrite,
}

/// Whether the slot behind a writer can hold a CQL null.
enum NullWrite {
    Allowed,
    /// A vector element has no way to hold a null.
    Rejected,
}

/// Why a value was not written.
pub(super) enum WriteError<'b> {
    /// The built-in path does not accept the Python type. The writer is unused.
    Rejected(Pending<'b>, Mismatch),
    Failed(SerializationError),
}

impl<'b> Pending<'b> {
    pub(super) fn new(cell_writer: CellWriter<'b>) -> Self {
        Self {
            cell_writer,
            null: NullWrite::Allowed,
        }
    }

    fn vector_element(cell_writer: CellWriter<'b>) -> Self {
        Self {
            cell_writer,
            null: NullWrite::Rejected,
        }
    }

    fn write<T: SerializeValue + ?Sized>(
        self,
        value: &T,
        typ: &ColumnType,
    ) -> Result<WrittenCellProof<'b>, WriteError<'b>> {
        value
            .serialize(typ, self.cell_writer)
            .map_err(WriteError::Failed)
    }

    /// Writes a converted native value, else rejects it with a mismatch against `E`.
    fn write_native_or_reject<T: SerializeValue, E: ?Sized>(
        self,
        value: Result<T, ConvertError>,
        typ: &ColumnType,
    ) -> Result<WrittenCellProof<'b>, WriteError<'b>> {
        match value {
            Ok(value) => self.write(&value, typ),
            Err(ConvertError::NotAccepted) => Err(self.reject(Mismatch::native::<E>())),
            Err(ConvertError::Failed(error)) => Err(WriteError::Failed(error)),
        }
    }

    /// Writes `value` if the built-in path accepted the Python type, else rejects it.
    fn write_compound_or_reject<T: PySerializeValue>(
        self,
        value: Result<T, Mismatch>,
        typ: &ColumnType,
        context: &AdapterContext<'_>,
    ) -> Result<WrittenCellProof<'b>, WriteError<'b>> {
        match value {
            Ok(value) => value
                .serialize_py(typ, self.cell_writer, context)
                .map_err(WriteError::Failed),
            Err(mismatch) => Err(self.reject(mismatch)),
        }
    }

    fn set_null(self) -> Result<WrittenCellProof<'b>, WriteError<'b>> {
        match self.null {
            NullWrite::Allowed => Ok(self.cell_writer.set_null()),
            NullWrite::Rejected => Err(DriverSerializationError::null_vector_element().into()),
        }
    }

    fn reject(self, mismatch: Mismatch) -> WriteError<'b> {
        WriteError::Rejected(self, mismatch)
    }
}

impl From<SerializationError> for WriteError<'_> {
    fn from(error: SerializationError) -> Self {
        Self::Failed(error)
    }
}

impl From<DriverSerializationError> for WriteError<'_> {
    fn from(error: DriverSerializationError) -> Self {
        Self::Failed(error.into())
    }
}

/// Why a Python value could not be converted for the built-in path.
enum ConvertError {
    /// The built-in path does not accept the Python type.
    NotAccepted,
    Failed(SerializationError),
}

impl From<DriverSerializationError> for ConvertError {
    fn from(error: DriverSerializationError) -> Self {
        Self::Failed(error.into())
    }
}

impl From<RelativedeltaError> for ConvertError {
    fn from(error: RelativedeltaError) -> Self {
        match error {
            RelativedeltaError::NotRelativedelta => Self::NotAccepted,
            RelativedeltaError::Overflow => DriverSerializationError::value_overflow().into(),
            RelativedeltaError::MixedSign => DriverSerializationError::mixed_sign_duration().into(),
        }
    }
}

/// What the built-in path expected instead of a rejected value.
///
/// The error is built only if nothing else writes the value, because building it allocates.
pub(super) enum Mismatch {
    /// A native CQL type. Holds the name of the Rust type the value was read as.
    Native(&'static str),
    Collection(TypeExpected),
}

impl Mismatch {
    /// A mismatch against the native type `E`.
    fn native<E: ?Sized>() -> Self {
        Self::Native(type_name::<E>())
    }
}

/// What the values of one row share while they are written.
#[derive(Debug)]
pub(super) struct AdapterContext<'a> {
    adapters: &'a Adapters,
    /// Compound CQL types given to adapters, keyed by the address of their `ColumnType`.
    ///
    /// A context lives for one row only, and every `ColumnType` it sees is borrowed from
    /// the metadata of that row, so no address is freed or used again while it lives.
    cql_types: RefCell<HashMap<usize, Py<PyCqlColumnType>>>,
}

impl<'a> AdapterContext<'a> {
    pub(super) fn new(adapters: &'a Adapters) -> Self {
        Self {
            adapters,
            cql_types: RefCell::default(),
        }
    }

    /// Native types are shared objects already, so only compound types are kept.
    fn cql_type(&self, py: Python<'_>, typ: &ColumnType) -> PyResult<Py<PyCqlColumnType>> {
        if matches!(typ, ColumnType::Native(_)) {
            return extract_column_type(py, typ);
        }
        let key = std::ptr::from_ref(typ) as usize;
        if let Some(cql_type) = self.cql_types.borrow().get(&key) {
            return Ok(cql_type.clone_ref(py));
        }
        let cql_type = extract_column_type(py, typ)?;
        self.cql_types
            .borrow_mut()
            .insert(key, cql_type.clone_ref(py));
        Ok(cql_type)
    }
}

/// Writes a Python value to a CQL cell, as [`SerializeValue`] does for a Rust value.
pub(super) trait PySerializeValue {
    fn serialize_py<'b>(
        &self,
        typ: &ColumnType,
        cell_writer: CellWriter<'b>,
        context: &AdapterContext<'_>,
    ) -> Result<WrittenCellProof<'b>, SerializationError>;
}

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

    fn serialize_with_fallback<'b>(
        &self,
        typ: &ColumnType,
        writer: Pending<'b>,
        context: &AdapterContext<'_>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        match self.try_serialize_arbitrary_value(typ, writer, context) {
            Ok(proof) => Ok(proof),
            Err(WriteError::Rejected(writer, mismatch)) => {
                self.serialize_adapted(typ, writer, mismatch, context)
            }
            Err(WriteError::Failed(error)) => Err(error),
        }
    }

    /// Writes a rejected value after its adapter converts it. The result is not adapted again.
    fn serialize_adapted<'b>(
        &self,
        typ: &ColumnType,
        writer: Pending<'b>,
        mismatch: Mismatch,
        context: &AdapterContext<'_>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        let Some(adapted) = self.adapt(typ, context)? else {
            return Err(self.mismatch_error(typ, mismatch));
        };
        let adapted = PyAnyWrapper::new(&adapted);
        adapted
            .try_serialize_arbitrary_value(typ, writer, context)
            .map_err(|error| adapted.final_error(typ, error))
    }

    /// Converts the value with its adapter, else with the `__cql_value__` method of its type.
    fn adapt(
        &self,
        typ: &ColumnType,
        context: &AdapterContext<'_>,
    ) -> Result<Option<Bound<'py, PyAny>>, SerializationError> {
        // Called with the value as the first argument, as a method is.
        let convert = match context.adapters.find(self.0) {
            Some(adapter) => adapter,
            None => match self
                .get_type()
                .getattr_opt(intern!(self.py(), "__cql_value__"))
            {
                Ok(Some(method)) => method,
                // The type is still rejected, so its mismatch error is the useful one.
                Ok(None) => return Ok(None),
                Err(error) => {
                    return Err(DriverSerializationError::python_interop_failed(error).into());
                }
            },
        };
        let cql_type = context
            .cql_type(self.py(), typ)
            .map_err(DriverSerializationError::python_interop_failed)?;
        let adapted = convert
            .call1((self.0, cql_type))
            .map_err(DriverSerializationError::python_interop_failed)?;
        Ok(Some(adapted))
    }

    #[deny(clippy::wildcard_enum_match_arm)]
    /// Writes the value, or gives back the unused writer if its Python type is not accepted.
    fn try_serialize_arbitrary_value<'b>(
        &self,
        typ: &ColumnType,
        writer: Pending<'b>,
        context: &AdapterContext<'_>,
    ) -> Result<WrittenCellProof<'b>, WriteError<'b>> {
        if self.is_none() {
            return writer.set_null();
        }

        match typ {
            ColumnType::Native(native_type) => self.try_serialize_native(typ, native_type, writer),

            ColumnType::Collection {
                typ: collection_typ,
                ..
            } => match collection_typ {
                CollectionType::List(_) => {
                    writer.write_compound_or_reject(PyListWrapper::new(self), typ, context)
                }
                CollectionType::Map(_, _) => {
                    writer.write_compound_or_reject(PyMapWrapper::new(self), typ, context)
                }
                CollectionType::Set(_) => {
                    writer.write_compound_or_reject(PySetWrapper::new(self), typ, context)
                }

                _ => {
                    let name = format!("{:?}", collection_typ);

                    Err(DriverSerializationError::unsupported_type(name).into())
                }
            },

            ColumnType::Vector {
                typ: element_typ,
                dimensions,
            } => writer.write_compound_or_reject(
                PyVectorWrapper::new(self, *dimensions, element_typ),
                typ,
                context,
            ),

            // Supports UDTs passed as Python dicts.
            // For Python dataclass instances, convert to dict first (e.g., using dataclasses.asdict()).
            ColumnType::UserDefinedType { definition, .. } => {
                writer.write_compound_or_reject(PyUdtWrapper::new(self, definition), typ, context)
            }

            ColumnType::Tuple(elements_types) => writer.write_compound_or_reject(
                PyTupleWrapper::new(self, elements_types),
                typ,
                context,
            ),
            _ => Err(DriverSerializationError::unsupported_type(format!("{typ:?}")).into()),
        }
    }

    #[deny(clippy::wildcard_enum_match_arm)]
    fn try_serialize_native<'b>(
        &self,
        typ: &ColumnType,
        native_type: &NativeType,
        writer: Pending<'b>,
    ) -> Result<WrittenCellProof<'b>, WriteError<'b>> {
        match native_type {
            // Integer types.
            NativeType::TinyInt => writer.write_native_or_reject::<i8, i8>(self.extract_int(), typ),
            NativeType::SmallInt => {
                writer.write_native_or_reject::<i16, i16>(self.extract_int(), typ)
            }
            NativeType::Int => writer.write_native_or_reject::<i32, i32>(self.extract_int(), typ),
            NativeType::BigInt => {
                writer.write_native_or_reject::<i64, i64>(self.extract_int(), typ)
            }
            NativeType::Counter => {
                let counter = match self.cast::<PyInt>() {
                    Ok(int) => int
                        .extract::<i64>()
                        .map(Counter)
                        .map_err(|_| DriverSerializationError::value_overflow().into()),
                    Err(_) => Err(ConvertError::NotAccepted),
                };
                writer.write_native_or_reject::<_, Counter>(counter, typ)
            }
            NativeType::Varint => {
                writer.write_native_or_reject::<BigInt, BigInt>(self.extract_native(), typ)
            }

            // Float types.
            NativeType::Float => {
                writer.write_native_or_reject::<f32, f32>(self.extract_native(), typ)
            }
            NativeType::Double => {
                writer.write_native_or_reject::<f64, f64>(self.extract_native(), typ)
            }
            NativeType::Decimal => {
                writer.write_native_or_reject::<BigDecimal, BigDecimal>(self.extract_native(), typ)
            }

            // Boolean type.
            NativeType::Boolean => {
                writer.write_native_or_reject::<bool, bool>(self.extract_native(), typ)
            }

            // Text types.
            // TODO: Python allows strings that are not valid in Rust, conversion to `&str` is fallible.
            // This case is currently ignored and should be handled in the future. See: #41
            NativeType::Ascii | NativeType::Text => {
                writer.write_native_or_reject::<&str, &str>(self.extract_native(), typ)
            }

            // Binary data type.
            NativeType::Blob => {
                let bytes = self
                    .cast::<PyBytes>()
                    .map(|value| value.as_bytes())
                    .map_err(|_| ConvertError::NotAccepted);
                writer.write_native_or_reject::<_, PyBytes>(bytes, typ)
            }

            // Datatime types.
            NativeType::Date => match self.cast::<PyCqlDate>() {
                Ok(date) => writer.write(&CqlDate::from(*date.get()), typ),
                Err(_) => writer
                    .write_native_or_reject::<NaiveDate, NaiveDate>(self.extract_native(), typ),
            },
            NativeType::Duration => {
                let duration = match self.cast::<PyCqlDuration>() {
                    Ok(duration) => Ok(*duration.get()),
                    Err(_) => self
                        .extract::<Relativedelta>()
                        .and_then(PyCqlDuration::try_from)
                        .map_err(ConvertError::from),
                };
                let duration = duration.map(CqlDuration::from);
                writer.write_native_or_reject::<_, CqlDuration>(duration, typ)
            }
            NativeType::Time => {
                let time = match self.cast::<PyCqlTime>() {
                    Ok(time) => Ok(CqlTime::from(*time.get())),
                    Err(_) => self
                        .extract::<NaiveTime>()
                        .ok()
                        .and_then(|time| time.try_into().ok())
                        .ok_or(ConvertError::NotAccepted),
                };
                writer.write_native_or_reject::<_, CqlTime>(time, typ)
            }
            NativeType::Timestamp => {
                let timestamp = match self.cast::<PyCqlTimestamp>() {
                    Ok(timestamp) => Ok(CqlTimestamp::from(*timestamp.get())),
                    Err(_) => self
                        .cast::<PyDateTime>()
                        .map_err(PyErr::from)
                        .and_then(aware_datetime_to_utc)
                        .map(CqlTimestamp::from)
                        .map_err(|_| ConvertError::NotAccepted),
                };
                writer.write_native_or_reject::<_, CqlTimestamp>(timestamp, typ)
            }

            // IP address type.
            NativeType::Inet => {
                let ip = match self.ip_address() {
                    Ok(Some(ip)) => Ok(ip),
                    Ok(None) => Err(ConvertError::NotAccepted),
                    Err(err) => Err(DriverSerializationError::python_interop_failed(err).into()),
                };
                writer.write_native_or_reject::<_, IpAddr>(ip, typ)
            }

            // UUID types.
            NativeType::Timeuuid => {
                let uuid = self
                    .extract::<Uuid>()
                    .map(CqlTimeuuid::from)
                    .map_err(|_| ConvertError::NotAccepted);
                writer.write_native_or_reject::<_, CqlTimeuuid>(uuid, typ)
            }
            NativeType::Uuid => {
                writer.write_native_or_reject::<Uuid, Uuid>(self.extract_native(), typ)
            }

            _ => {
                let name = format!("{:?}", native_type);

                Err(DriverSerializationError::unsupported_type(name).into())
            }
        }
    }

    fn extract_native<T>(&self) -> Result<T, ConvertError>
    where
        T: pyo3::FromPyObject<'a, 'py>,
    {
        self.extract::<T>().map_err(|_| ConvertError::NotAccepted)
    }

    fn extract_int<T>(&self) -> Result<T, ConvertError>
    where
        T: pyo3::FromPyObject<'a, 'py>,
    {
        // The cast to `PyInt` is performed to distinguish between two different error cases:
        // `MismatchedType` and `ValueOverflow`.
        match self.cast::<PyInt>() {
            Ok(int) => int
                .extract::<T>()
                .map_err(|_| DriverSerializationError::value_overflow().into()),
            Err(_) => Err(ConvertError::NotAccepted),
        }
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

    fn mismatch_error(&self, typ: &ColumnType, mismatch: Mismatch) -> SerializationError {
        match mismatch {
            Mismatch::Native(rust_name) => {
                let expected = match self.map_type_to_expected() {
                    Ok(expected) => expected,
                    Err(e) => return e,
                };
                mk_typck_err_named(
                    rust_name,
                    typ,
                    BuiltinTypeCheckErrorKind::MismatchedType { expected },
                )
            }
            Mismatch::Collection(expected) => {
                DriverSerializationError::type_mismatch(expected).into()
            }
        }
    }

    fn final_error(&self, typ: &ColumnType, error: WriteError<'_>) -> SerializationError {
        match error {
            WriteError::Rejected(_, mismatch) => self.mismatch_error(typ, mismatch),
            WriteError::Failed(error) => error,
        }
    }
}

impl<'a, 'py> PySerializeValue for PyAnyWrapper<'a, 'py> {
    fn serialize_py<'b>(
        &self,
        typ: &ColumnType,
        cell_writer: CellWriter<'b>,
        context: &AdapterContext<'_>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        self.serialize_with_fallback(typ, Pending::new(cell_writer), context)
    }
}

/// A vector element. It has no way to hold a null, so `None` is an error.
///
/// The driver's vector helpers take a [`SerializeValue`], which has no context argument,
/// so the element keeps its context.
struct VectorElement<'a, 'py> {
    value: PyAnyWrapper<'a, 'py>,
    context: &'a AdapterContext<'a>,
}

impl SerializeValue for VectorElement<'_, '_> {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        cell_writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        self.value
            .serialize_with_fallback(typ, Pending::vector_element(cell_writer), self.context)
    }
}

fn serialize_sequence<'t, 'b, 'py, T: Any>(
    len: usize,
    iter: impl Iterator<Item = Bound<'py, PyAny>>,
    context: &AdapterContext<'_>,
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
        PyAnyWrapper::new(&el)
            .serialize_py(elt, builder.make_sub_writer(), context)
            .map_err(|err| {
                mk_ser_err::<T>(
                    typ,
                    SetOrListSerializationErrorKind::ElementSerializationFailed(err),
                )
            })?;
    }

    builder
        .finish()
        .map_err(|_| mk_ser_err::<T>(typ, BuiltinSerializationErrorKind::SizeOverflow))
}

fn serialize_vector<'t, 'b, 'py, T: Any>(
    len: usize,
    iter: impl Iterator<Item = Bound<'py, PyAny>>,
    context: &AdapterContext<'_>,
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
                    &VectorElement {
                        value: PyAnyWrapper::new(&element),
                        context,
                    },
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
                    &VectorElement {
                        value: PyAnyWrapper::new(&element),
                        context,
                    },
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
    fn new(value: &PyAnyWrapper<'a, 'py>) -> Result<Self, Mismatch> {
        let list: &Bound<PyList> = value
            .cast::<PyList>()
            .map_err(|_| Mismatch::Collection(TypeExpected::List))?;
        Ok(PyListWrapper { inner: list })
    }
}

impl<'a, 'py> PySerializeValue for PyListWrapper<'a, 'py> {
    fn serialize_py<'b>(
        &self,
        typ: &ColumnType,
        cell_writer: CellWriter<'b>,
        context: &AdapterContext<'_>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        let items = self.iter();

        serialize_sequence::<PyList>(items.len(), items, context, typ, cell_writer)
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
    fn new(value: &PyAnyWrapper<'a, 'py>) -> Result<Self, Mismatch> {
        let set: &Bound<PySet> = value
            .cast::<PySet>()
            .map_err(|_| Mismatch::Collection(TypeExpected::Set))?;
        Ok(PySetWrapper(set))
    }
}

impl<'a, 'py> PySerializeValue for PySetWrapper<'a, 'py> {
    fn serialize_py<'b>(
        &self,
        typ: &ColumnType,
        cell_writer: CellWriter<'b>,
        context: &AdapterContext<'_>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        let items = self.iter();

        serialize_sequence::<PySet>(items.len(), items, context, typ, cell_writer)
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
    fn new(value: &PyAnyWrapper<'a, 'py>) -> Result<Self, Mismatch> {
        let map: &Bound<PyMapping> = value
            .cast::<PyMapping>()
            .map_err(|_| Mismatch::Collection(TypeExpected::Map))?;
        Ok(PyMapWrapper(map))
    }
}

impl<'a, 'py> PySerializeValue for PyMapWrapper<'a, 'py> {
    fn serialize_py<'b>(
        &self,
        typ: &ColumnType,
        cell_writer: CellWriter<'b>,
        context: &AdapterContext<'_>,
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
            PyAnyWrapper::new(&key)
                .serialize_py(ktyp, builder.make_sub_writer(), context)
                .map_err(|err| {
                    mk_ser_err::<PyMapping>(
                        typ,
                        MapSerializationErrorKind::KeySerializationFailed(err),
                    )
                })?;
            PyAnyWrapper::new(&value)
                .serialize_py(vtyp, builder.make_sub_writer(), context)
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
    ) -> Result<Self, Mismatch> {
        let items = if let Ok(list) = value.cast::<PyList>() {
            VectorItems::List(list)
        } else {
            let tuple = value
                .cast::<PyTuple>()
                .map_err(|_| Mismatch::Collection(TypeExpected::Vector))?;
            VectorItems::Tuple(tuple)
        };
        Ok(PyVectorWrapper {
            items,
            dimension,
            element_type,
        })
    }
}

impl<'py, 'a> PySerializeValue for PyVectorWrapper<'py, 'a> {
    fn serialize_py<'b>(
        &self,
        typ: &ColumnType,
        cell_writer: CellWriter<'b>,
        context: &AdapterContext<'_>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        match self.items {
            VectorItems::List(list) => {
                let items = list.iter();
                serialize_vector::<PyList>(
                    items.len(),
                    items,
                    context,
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
                    context,
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
    ) -> Result<Self, Mismatch> {
        let dict: &Bound<PyDict> = value
            .cast::<PyDict>()
            .map_err(|_| Mismatch::Collection(TypeExpected::Udt))?;
        Ok(PyUdtWrapper {
            inner: dict,
            definition,
        })
    }
}

impl<'py> PySerializeValue for PyUdtWrapper<'py, '_> {
    fn serialize_py<'a>(
        &self,
        typ: &ColumnType,
        cell_writer: CellWriter<'a>,
        context: &AdapterContext<'_>,
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

            PyAnyWrapper::new(&item).serialize_py(
                field_type,
                builder.make_sub_writer(),
                context,
            )?;
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
    ) -> Result<Self, Mismatch> {
        let tuple: &Bound<PyTuple> = value
            .cast::<PyTuple>()
            .map_err(|_| Mismatch::Collection(TypeExpected::Tuple))?;
        Ok(PyTupleWrapper {
            inner: tuple,
            elements_types,
        })
    }
}

impl<'py, 'a> PySerializeValue for PyTupleWrapper<'py, 'a> {
    fn serialize_py<'b>(
        &self,
        typ: &ColumnType,
        cell_writer: CellWriter<'b>,
        context: &AdapterContext<'_>,
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
            PyAnyWrapper::new(&val).serialize_py(
                element_type,
                builder.make_sub_writer(),
                context,
            )?;
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
    mk_typck_err_named(type_name::<T>(), got, kind)
}

fn mk_typck_err_named(
    rust_name: &'static str,
    got: &ColumnType,
    kind: impl Into<BuiltinTypeCheckErrorKind>,
) -> SerializationError {
    SerializationError::new(BuiltinTypeCheckError {
        rust_name,
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
