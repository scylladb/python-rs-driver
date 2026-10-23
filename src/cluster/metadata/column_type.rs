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
        }
    };
}

/// Macro to case PyClassInitializer to PyResult<Py<PyCqlColumnType>>
/// Takes a `py` Python token and a PyClassInitializer expression
/// Usage: create_and_extract!(py, PyCqlAsciiType::new())
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
native_type_class!(PyCqlAsciiType, "CqlAsciiType");
native_type_class!(PyCqlBooleanType, "CqlBooleanType");
native_type_class!(PyCqlBlobType, "CqlBlobType");
native_type_class!(PyCqlCounterType, "CqlCounterType");
native_type_class!(PyCqlDateType, "CqlDateType");
native_type_class!(PyCqlDecimalType, "CqlDecimalType");
native_type_class!(PyCqlDoubleType, "CqlDoubleType");
native_type_class!(PyCqlDurationType, "CqlDurationType");
native_type_class!(PyCqlFloatType, "CqlFloatType");
native_type_class!(PyCqlIntType, "CqlIntType");
native_type_class!(PyCqlBigIntType, "CqlBigIntType");
native_type_class!(PyCqlTextType, "CqlTextType");
native_type_class!(PyCqlTimestampType, "CqlTimestampType");
native_type_class!(PyCqlInetType, "CqlInetType");
native_type_class!(PyCqlSmallIntType, "CqlSmallIntType");
native_type_class!(PyCqlTinyIntType, "CqlTinyIntType");
native_type_class!(PyCqlTimeType, "CqlTimeType");
native_type_class!(PyCqlTimeuuidType, "CqlTimeuuidType");
native_type_class!(PyCqlUuidType, "CqlUuidType");
native_type_class!(PyCqlVarintType, "CqlVarintType");

#[pyclass(module = "scylla.cql_types", name = "CqlCollectionType", subclass, extends=PyCqlColumnType, frozen, get_all)]
pub(crate) struct PyCqlCollectionType {
    pub(crate) frozen: bool,
}

impl PyCqlCollectionType {
    pub(crate) fn new(frozen: bool) -> PyClassInitializer<Self> {
        PyClassInitializer::from(PyCqlColumnType::new()).add_subclass(Self { frozen })
    }
}

#[pyclass(module = "scylla.cql_types", name = "CqlMapType", extends=PyCqlCollectionType, frozen, get_all)]
pub(crate) struct PyCqlMapType {
    pub(crate) key_type: Py<PyCqlColumnType>,
    pub(crate) value_type: Py<PyCqlColumnType>,
}

impl PyCqlMapType {
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

#[pyclass(module = "scylla.cql_types", name = "CqlSetType", extends=PyCqlCollectionType, frozen, get_all)]
pub(crate) struct PyCqlSetType {
    pub(crate) column_type: Py<PyCqlColumnType>,
}

impl PyCqlSetType {
    pub(crate) fn new(frozen: bool, column_type: Py<PyCqlColumnType>) -> PyClassInitializer<Self> {
        PyCqlCollectionType::new(frozen).add_subclass(Self { column_type })
    }
}

#[pyclass(module = "scylla.cql_types", name = "CqlListType", extends=PyCqlCollectionType, frozen, get_all)]
pub(crate) struct PyCqlListType {
    pub(crate) column_type: Py<PyCqlColumnType>,
}

impl PyCqlListType {
    pub(crate) fn new(frozen: bool, column_type: Py<PyCqlColumnType>) -> PyClassInitializer<Self> {
        PyCqlCollectionType::new(frozen).add_subclass(Self { column_type })
    }
}

#[pyclass(module = "scylla.cql_types", name = "CqlTupleType", extends=PyCqlColumnType, frozen, get_all)]
pub(crate) struct PyCqlTupleType {
    pub(crate) element_types: Vec<Py<PyCqlColumnType>>,
}

impl PyCqlTupleType {
    pub(crate) fn new(element_types: Vec<Py<PyCqlColumnType>>) -> PyClassInitializer<Self> {
        PyClassInitializer::from(PyCqlColumnType::new()).add_subclass(Self { element_types })
    }
}

#[pyclass(module = "scylla.cql_types", name = "CqlVectorType", extends=PyCqlColumnType, frozen, get_all)]
pub(crate) struct PyCqlVectorType {
    pub(crate) typ: Py<PyCqlColumnType>,
    pub(crate) dimensions: u16,
}

impl PyCqlVectorType {
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
                NativeType::Ascii => create_py_and_cast!(py, PyCqlAsciiType::new()),
                NativeType::BigInt => create_py_and_cast!(py, PyCqlBigIntType::new()),
                NativeType::Blob => create_py_and_cast!(py, PyCqlBlobType::new()),
                NativeType::Boolean => create_py_and_cast!(py, PyCqlBooleanType::new()),
                NativeType::Counter => create_py_and_cast!(py, PyCqlCounterType::new()),
                NativeType::Date => create_py_and_cast!(py, PyCqlDateType::new()),
                NativeType::Decimal => create_py_and_cast!(py, PyCqlDecimalType::new()),
                NativeType::Double => create_py_and_cast!(py, PyCqlDoubleType::new()),
                NativeType::Duration => create_py_and_cast!(py, PyCqlDurationType::new()),
                NativeType::Float => create_py_and_cast!(py, PyCqlFloatType::new()),
                NativeType::Inet => create_py_and_cast!(py, PyCqlInetType::new()),
                NativeType::Int => create_py_and_cast!(py, PyCqlIntType::new()),
                NativeType::SmallInt => create_py_and_cast!(py, PyCqlSmallIntType::new()),
                NativeType::Text => create_py_and_cast!(py, PyCqlTextType::new()),
                NativeType::Time => create_py_and_cast!(py, PyCqlTimeType::new()),
                NativeType::Timestamp => create_py_and_cast!(py, PyCqlTimestampType::new()),
                NativeType::Timeuuid => create_py_and_cast!(py, PyCqlTimeuuidType::new()),
                NativeType::TinyInt => create_py_and_cast!(py, PyCqlTinyIntType::new()),
                NativeType::Uuid => create_py_and_cast!(py, PyCqlUuidType::new()),
                NativeType::Varint => create_py_and_cast!(py, PyCqlVarintType::new()),
                _ => unreachable!("clippy testifies that the match is exhaustive"),
            }
        }
        ColumnType::Collection { frozen, typ } => match typ {
            CollectionType::List(element_type) => {
                let column_type = extract_column_type(py, element_type)?;
                create_py_and_cast!(py, PyCqlListType::new(*frozen, column_type))
            }
            CollectionType::Set(element_type) => {
                let column_type = extract_column_type(py, element_type)?;
                create_py_and_cast!(py, PyCqlSetType::new(*frozen, column_type))
            }
            CollectionType::Map(key_type, value_type) => {
                let key_type = extract_column_type(py, key_type)?;
                let value_type = extract_column_type(py, value_type)?;
                create_py_and_cast!(py, PyCqlMapType::new(*frozen, key_type, value_type))
            }
            _ => unreachable!("clippy testifies that the match is exhaustive"),
        },
        ColumnType::Tuple(element_list) => {
            let element_types = element_list
                .iter()
                .map(|col_typ| extract_column_type(py, col_typ))
                .collect::<PyResult<Vec<Py<PyCqlColumnType>>>>()?;

            Py::new(py, PyCqlTupleType::new(element_types))
                .map(|obj| obj.into_bound(py).into_super().unbind())
        }
        ColumnType::Vector { typ, dimensions } => {
            let element_type = extract_column_type(py, typ)?;
            Py::new(py, PyCqlVectorType::new(element_type, *dimensions))
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
