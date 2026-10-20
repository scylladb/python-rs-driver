use pyo3::sync::PyOnceLock;
use pyo3::{Py, PyClassInitializer, PyResult, Python, pyclass};
use scylla::cluster::metadata::{CollectionType, ColumnType, NativeType};

/// Macro to generate native type subclasses
macro_rules! native_type_class {
    ($py_class:ident, $py_name:expr) => {
        #[pyclass(module = "scylla.cql_types", name = $py_name, extends = PyCqlNativeType, frozen)]
        pub(crate) struct $py_class;

        impl $py_class {
            pub(crate) fn new() -> PyClassInitializer<Self> {
                PyCqlNativeType::new().add_subclass(Self {})
            }

            /// The one instance, shared because the type holds no state.
            fn shared(py: Python<'_>) -> PyResult<Py<PyCqlColumnType>> {
                static INSTANCE: PyOnceLock<Py<PyCqlColumnType>> = PyOnceLock::new();
                INSTANCE
                    .get_or_try_init(py, || create_py_and_cast!(py, Self::new()))
                    .map(|instance| instance.clone_ref(py))
            }
        }
    };
}

/// Macro to case PyClassInitializer to PyResult<Py<PyCqlColumnType>>
/// Takes a `py` Python token and a PyClassInitializer expression
/// Usage: create_and_extract!(py, PyCqlAscii::new())
macro_rules! create_py_and_cast {
    ($py:expr, $initializer:expr) => {
        Py::new($py, $initializer).map(|obj| obj.into_bound($py).into_super().into_super().unbind())
    };
}

#[pyclass(module = "scylla.cql_types", name = "CqlNativeType", subclass, extends=PyCqlColumnType, frozen)]
pub(crate) struct PyCqlNativeType;

impl PyCqlNativeType {
    pub(crate) fn new() -> PyClassInitializer<Self> {
        PyClassInitializer::from(PyCqlColumnType::new()).add_subclass(Self {})
    }
}

// Generate all native type classes
native_type_class!(PyCqlAscii, "CqlAscii");
native_type_class!(PyCqlBoolean, "CqlBoolean");
native_type_class!(PyCqlBlob, "CqlBlob");
native_type_class!(PyCqlCounter, "CqlCounter");
native_type_class!(PyCqlDate, "CqlDate");
native_type_class!(PyCqlDecimal, "CqlDecimal");
native_type_class!(PyCqlDouble, "CqlDouble");
native_type_class!(PyCqlDuration, "CqlDuration");
native_type_class!(PyCqlFloat, "CqlFloat");
native_type_class!(PyCqlInt, "CqlInt");
native_type_class!(PyCqlBigInt, "CqlBigInt");
native_type_class!(PyCqlText, "CqlText");
native_type_class!(PyCqlTimestamp, "CqlTimestamp");
native_type_class!(PyCqlInet, "CqlInet");
native_type_class!(PyCqlSmallInt, "CqlSmallInt");
native_type_class!(PyCqlTinyInt, "CqlTinyInt");
native_type_class!(PyCqlTime, "CqlTime");
native_type_class!(PyCqlTimeuuid, "CqlTimeuuid");
native_type_class!(PyCqlUuid, "CqlUuid");
native_type_class!(PyCqlVarint, "CqlVarint");

#[pyclass(module = "scylla.cql_types", name = "CqlCollectionType", subclass, extends=PyCqlColumnType, frozen, get_all)]
pub(crate) struct PyCqlCollectionType {
    pub(crate) frozen: bool,
}

impl PyCqlCollectionType {
    pub(crate) fn new(frozen: bool) -> PyClassInitializer<Self> {
        PyClassInitializer::from(PyCqlColumnType::new()).add_subclass(Self { frozen })
    }
}

#[pyclass(module = "scylla.cql_types", name = "CqlMap", extends=PyCqlCollectionType, frozen, get_all)]
pub(crate) struct PyCqlMap {
    pub(crate) key_type: Py<PyCqlColumnType>,
    pub(crate) value_type: Py<PyCqlColumnType>,
}

impl PyCqlMap {
    pub(crate) fn new(
        frozen: bool,
        key_type: Py<PyCqlColumnType>,
        value_type: Py<PyCqlColumnType>,
    ) -> PyClassInitializer<Self> {
        PyCqlCollectionType::new(frozen).add_subclass(Self {
            key_type,
            value_type,
        })
    }
}

#[pyclass(module = "scylla.cql_types", name = "CqlSet", extends=PyCqlCollectionType, frozen, get_all)]
pub(crate) struct PyCqlSet {
    pub(crate) column_type: Py<PyCqlColumnType>,
}

impl PyCqlSet {
    pub(crate) fn new(frozen: bool, column_type: Py<PyCqlColumnType>) -> PyClassInitializer<Self> {
        PyCqlCollectionType::new(frozen).add_subclass(Self { column_type })
    }
}

#[pyclass(module = "scylla.cql_types", name = "CqlList", extends=PyCqlCollectionType, frozen, get_all)]
pub(crate) struct PyCqlList {
    pub(crate) column_type: Py<PyCqlColumnType>,
}

impl PyCqlList {
    pub(crate) fn new(frozen: bool, column_type: Py<PyCqlColumnType>) -> PyClassInitializer<Self> {
        PyCqlCollectionType::new(frozen).add_subclass(Self { column_type })
    }
}

#[pyclass(module = "scylla.cql_types", name = "CqlTuple", extends=PyCqlColumnType, frozen, get_all)]
pub(crate) struct PyCqlTuple {
    pub(crate) element_types: Vec<Py<PyCqlColumnType>>,
}

impl PyCqlTuple {
    pub(crate) fn new(element_types: Vec<Py<PyCqlColumnType>>) -> PyClassInitializer<Self> {
        PyClassInitializer::from(PyCqlColumnType::new()).add_subclass(Self { element_types })
    }
}

#[pyclass(module = "scylla.cql_types", name = "CqlVector", extends=PyCqlColumnType, frozen, get_all)]
pub(crate) struct PyCqlVector {
    pub(crate) typ: Py<PyCqlColumnType>,
    pub(crate) dimensions: u16,
}

impl PyCqlVector {
    pub(crate) fn new(typ: Py<PyCqlColumnType>, dimensions: u16) -> PyClassInitializer<Self> {
        PyClassInitializer::from(PyCqlColumnType::new()).add_subclass(Self { typ, dimensions })
    }
}

#[pyclass(module = "scylla.cql_types", name = "CqlUserDefinedType", extends=PyCqlColumnType, frozen, get_all)]
pub(crate) struct PyCqlUserDefinedType {
    pub(crate) name: String,
    pub(crate) frozen: bool,
    pub(crate) keyspace: String,
    pub(crate) field_types: Vec<(String, Py<PyCqlColumnType>)>,
}

impl PyCqlUserDefinedType {
    pub(crate) fn new(
        name: String,
        frozen: bool,
        keyspace: String,
        field_types: Vec<(String, Py<PyCqlColumnType>)>,
    ) -> PyClassInitializer<Self> {
        PyClassInitializer::from(PyCqlColumnType::new()).add_subclass(Self {
            name,
            frozen,
            keyspace,
            field_types,
        })
    }
}
#[pyclass(module = "scylla.cql_types", name = "CqlColumnType", subclass, frozen)]
pub(crate) struct PyCqlColumnType {}

impl PyCqlColumnType {
    fn new() -> Self {
        PyCqlColumnType {}
    }
}

pub(crate) fn extract_column_type(
    py: Python<'_>,
    column_type: &ColumnType,
) -> PyResult<Py<PyCqlColumnType>> {
    #[deny(clippy::wildcard_enum_match_arm)]
    match column_type {
        ColumnType::Native(native_type) =>
        {
            #[deny(clippy::wildcard_enum_match_arm)]
            match native_type {
                NativeType::Ascii => PyCqlAscii::shared(py),
                NativeType::BigInt => PyCqlBigInt::shared(py),
                NativeType::Blob => PyCqlBlob::shared(py),
                NativeType::Boolean => PyCqlBoolean::shared(py),
                NativeType::Counter => PyCqlCounter::shared(py),
                NativeType::Date => PyCqlDate::shared(py),
                NativeType::Decimal => PyCqlDecimal::shared(py),
                NativeType::Double => PyCqlDouble::shared(py),
                NativeType::Duration => PyCqlDuration::shared(py),
                NativeType::Float => PyCqlFloat::shared(py),
                NativeType::Inet => PyCqlInet::shared(py),
                NativeType::Int => PyCqlInt::shared(py),
                NativeType::SmallInt => PyCqlSmallInt::shared(py),
                NativeType::Text => PyCqlText::shared(py),
                NativeType::Time => PyCqlTime::shared(py),
                NativeType::Timestamp => PyCqlTimestamp::shared(py),
                NativeType::Timeuuid => PyCqlTimeuuid::shared(py),
                NativeType::TinyInt => PyCqlTinyInt::shared(py),
                NativeType::Uuid => PyCqlUuid::shared(py),
                NativeType::Varint => PyCqlVarint::shared(py),
                _ => unreachable!("clippy testifies that the match is exhaustive"),
            }
        }
        ColumnType::Collection { frozen, typ } => match typ {
            CollectionType::List(element_type) => {
                let column_type = extract_column_type(py, element_type)?;
                create_py_and_cast!(py, PyCqlList::new(*frozen, column_type))
            }
            CollectionType::Set(element_type) => {
                let column_type = extract_column_type(py, element_type)?;
                create_py_and_cast!(py, PyCqlSet::new(*frozen, column_type))
            }
            CollectionType::Map(key_type, value_type) => {
                let key_type = extract_column_type(py, key_type)?;
                let value_type = extract_column_type(py, value_type)?;
                create_py_and_cast!(py, PyCqlMap::new(*frozen, key_type, value_type))
            }
            _ => unreachable!("clippy testifies that the match is exhaustive"),
        },
        ColumnType::Tuple(element_list) => {
            let element_types = element_list
                .iter()
                .map(|col_typ| extract_column_type(py, col_typ))
                .collect::<PyResult<Vec<Py<PyCqlColumnType>>>>()?;

            Py::new(py, PyCqlTuple::new(element_types))
                .map(|obj| obj.into_bound(py).into_super().unbind())
        }
        ColumnType::Vector { typ, dimensions } => {
            let element_type = extract_column_type(py, typ)?;
            Py::new(py, PyCqlVector::new(element_type, *dimensions))
                .map(|obj| obj.into_bound(py).into_super().unbind())
        }
        ColumnType::UserDefinedType { frozen, definition } => {
            let fields = definition
                .field_types
                .iter()
                .map(|(field_name, field_type)| {
                    let field_type = extract_column_type(py, field_type)?;
                    Ok((field_name.to_string(), field_type))
                })
                .collect::<PyResult<Vec<(String, Py<PyCqlColumnType>)>>>()?;

            Py::new(
                py,
                PyCqlUserDefinedType::new(
                    definition.name.to_string(),
                    *frozen,
                    definition.keyspace.to_string(),
                    fields,
                ),
            )
            .map(|obj| obj.into_bound(py).into_super().unbind())
        }
        _ => unreachable!("clippy testifies that the match is exhaustive"),
    }
}
