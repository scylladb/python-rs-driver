use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock, RwLock};

use pyo3::exceptions::{PyNotImplementedError, PyValueError};
use pyo3::prelude::*;
use pyo3::sync::{PyOnceLock, RwLockExt};
use pyo3::types::{PyDict, PyString, PyTuple};
use pyo3::{Borrowed, PyClass, PyTypeInfo, intern};
use scylla::frame::response::result::ColumnSpec;

use crate::cluster::metadata::query_metadata::{PyColumnSpec, column_spec_tuple};
use crate::deserialize::error::{DriverRowFactoryError, DriverRowIterationError};
use crate::utils::PyValueOrError;

/// Base class of all row factories. A subclass overrides `prepare`, which
/// gets the columns of a page and returns the callable that builds each row.
#[pyclass(module = "scylla.results", name = "RowFactory", subclass, frozen)]
pub(crate) struct PyRowFactoryBase;

#[pymethods]
impl PyRowFactoryBase {
    // Takes any arguments, so a subclass `__init__` can have its own.
    #[new]
    #[pyo3(signature = (*_args, **_kwargs))]
    fn new(_args: &Bound<'_, PyTuple>, _kwargs: Option<&Bound<'_, PyDict>>) -> Self {
        Self
    }

    fn prepare(&self, _columns: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        Err(PyNotImplementedError::new_err(
            "a RowFactory subclass must override 'prepare'",
        ))
    }
}

fn builtin<T: PyClass<BaseType = PyRowFactoryBase>>(factory: T) -> PyClassInitializer<T> {
    PyClassInitializer::from(PyRowFactoryBase).add_subclass(factory)
}

/// Builds every row as a `collections.namedtuple`. This is the default.
#[pyclass(module = "scylla.results", name = "NamedTupleRowFactory", extends = PyRowFactoryBase, frozen)]
pub(crate) struct PyNamedTupleRowFactory;

#[pymethods]
impl PyNamedTupleRowFactory {
    #[new]
    fn new() -> PyClassInitializer<Self> {
        builtin(Self)
    }

    fn prepare(
        &self,
        py: Python<'_>,
        columns: &Bound<'_, PyTuple>,
    ) -> PyResult<PyBuiltinRowBuilder> {
        let names = columns
            .iter_borrowed()
            .map(|column| Ok(column.cast::<PyColumnSpec>()?.get().column_name()))
            .collect::<PyResult<Vec<&str>>>()?;

        Ok(PyBuiltinRowBuilder::new(
            RowBuilder::named_tuple(py, &names)?,
            names.len(),
        ))
    }
}

/// Builds every row as a `dict` mapping column names to values, in column order.
#[pyclass(module = "scylla.results", name = "DictRowFactory", extends = PyRowFactoryBase, frozen)]
pub(crate) struct PyDictRowFactory;

#[pymethods]
impl PyDictRowFactory {
    #[new]
    fn new() -> PyClassInitializer<Self> {
        builtin(Self)
    }

    fn prepare(
        &self,
        py: Python<'_>,
        columns: &Bound<'_, PyTuple>,
    ) -> PyResult<PyBuiltinRowBuilder> {
        let names = py_column_names(py, columns)?;
        let column_count = names.len();

        Ok(PyBuiltinRowBuilder::new(
            RowBuilder::Dict(names),
            column_count,
        ))
    }
}

/// Builds every row as a plain `tuple` of values, in column order.
#[pyclass(module = "scylla.results", name = "TupleRowFactory", extends = PyRowFactoryBase, frozen)]
pub(crate) struct PyTupleRowFactory;

#[pymethods]
impl PyTupleRowFactory {
    #[new]
    fn new() -> PyClassInitializer<Self> {
        builtin(Self)
    }

    fn prepare(&self, columns: &Bound<'_, PyTuple>) -> PyBuiltinRowBuilder {
        PyBuiltinRowBuilder::new(RowBuilder::Tuple, columns.len())
    }
}

/// Builds every row as `cls(**columns)`, passing each column as a keyword
/// argument named after it.
#[pyclass(module = "scylla.results", name = "ClassRowFactory", extends = PyRowFactoryBase, frozen)]
pub(crate) struct PyClassRowFactory {
    #[pyo3(get, name = "cls")]
    class: Py<PyAny>,
}

#[pymethods]
impl PyClassRowFactory {
    #[new]
    fn new(
        py: Python<'_>,
        cls: Py<PyAny>,
    ) -> Result<PyClassInitializer<Self>, DriverRowFactoryError> {
        if !cls.bind(py).is_callable() {
            return Err(DriverRowFactoryError::invalid_class(
                cls.bind(py).as_borrowed(),
            ));
        }

        Ok(builtin(Self { class: cls }))
    }

    fn prepare(
        &self,
        py: Python<'_>,
        columns: &Bound<'_, PyTuple>,
    ) -> PyResult<PyBuiltinRowBuilder> {
        let names = py_column_names(py, columns)?;
        let column_count = names.len();
        let builder = RowBuilder::Class {
            class: self.class.clone_ref(py),
            names,
        };

        Ok(PyBuiltinRowBuilder::new(builder, column_count))
    }
}

/// The builder a built-in factory's `prepare` returns, called with a tuple of
/// column values.
#[pyclass(name = "BuiltinRowBuilder", frozen)]
pub(crate) struct PyBuiltinRowBuilder {
    builder: RowBuilder,
    column_count: usize,
}

impl PyBuiltinRowBuilder {
    fn new(builder: RowBuilder, column_count: usize) -> Self {
        Self {
            builder,
            column_count,
        }
    }
}

#[pymethods]
impl PyBuiltinRowBuilder {
    fn __call__(&self, py: Python<'_>, values: &Bound<'_, PyTuple>) -> PyResult<Py<PyAny>> {
        if values.len() != self.column_count {
            return Err(PyValueError::new_err(format!(
                "expected {} column values, got {}",
                self.column_count,
                values.len()
            )));
        }

        // The values are already Python objects, so only Python errors can occur.
        self.builder
            .build(py, values.iter().map(Ok))
            .map_err(|err| match err {
                DriverRowIterationError::PythonError(err) => err,
                err => err.into(),
            })
    }
}

/// Reuses the name strings each `ColumnSpec` caches.
fn py_column_names(py: Python<'_>, columns: &Bound<'_, PyTuple>) -> PyResult<Vec<Py<PyString>>> {
    columns
        .iter_borrowed()
        .map(|column| Ok(column.cast::<PyColumnSpec>()?.get().name(py)))
        .collect()
}

/// A row factory as handed over from Python, classified but not yet resolved:
/// resolving needs the column metadata, which only arrives with the response.
///
/// Python objects are behind an `Arc`, so cloning it is safe on a thread that
/// is not attached to the interpreter.
#[derive(Clone)]
pub(crate) enum PyRowFactory {
    NamedTuple,
    Dict,
    Tuple,
    Class(Arc<Py<PyAny>>),
    /// A user `RowFactory` subclass, whose `prepare` is called once the
    /// metadata is known.
    Deferred(Arc<Py<PyAny>>),
    /// A callable used directly as the row builder.
    Builder(Arc<Py<PyAny>>),
}

impl<'py> FromPyObject<'_, 'py> for PyRowFactory {
    type Error = DriverRowFactoryError;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if obj.cast::<PyNamedTupleRowFactory>().is_ok() {
            return Ok(Self::NamedTuple);
        }

        if obj.cast::<PyDictRowFactory>().is_ok() {
            return Ok(Self::Dict);
        }

        if obj.cast::<PyTupleRowFactory>().is_ok() {
            return Ok(Self::Tuple);
        }

        if let Ok(factory) = obj.cast::<PyClassRowFactory>() {
            return Ok(Self::Class(Arc::new(
                factory.get().class.clone_ref(obj.py()),
            )));
        }

        if obj.cast::<PyRowFactoryBase>().is_ok() {
            return Ok(Self::Deferred(Arc::new(obj.to_owned().unbind())));
        }

        if obj.is_callable() {
            return Ok(Self::Builder(Arc::new(obj.to_owned().unbind())));
        }

        Err(DriverRowFactoryError::invalid_factory(obj))
    }
}

/// A row factory resolved against the column metadata of one page.
///
/// Pages of one request can differ in columns, for example after a schema
/// change, so each page gets its own builder.
#[derive(Clone)]
pub(crate) enum RowBuilder {
    NamedTuple(Py<PyAny>),
    Dict(Vec<Py<PyString>>),
    Tuple,
    Class {
        class: Py<PyAny>,
        names: Vec<Py<PyString>>,
    },
    Custom(Py<PyAny>),
}

impl RowBuilder {
    pub(crate) fn resolve(
        py: Python<'_>,
        factory: &PyRowFactory,
        specs: &[ColumnSpec<'_>],
    ) -> PyResult<Self> {
        Ok(match factory {
            PyRowFactory::NamedTuple => Self::named_tuple(py, &spec_names(specs))?,
            PyRowFactory::Dict => Self::Dict(column_names(py, specs)),
            PyRowFactory::Tuple => Self::Tuple,
            PyRowFactory::Class(class) => Self::Class {
                class: class.clone_ref(py),
                names: column_names(py, specs),
            },
            PyRowFactory::Deferred(deferred) => {
                let columns = column_spec_tuple(py, specs)?;
                let builder = deferred
                    .bind(py)
                    .call_method1(intern!(py, "prepare"), (columns,))?;

                if !builder.is_callable() {
                    return Err(
                        DriverRowFactoryError::uncallable_builder(builder.as_borrowed()).into(),
                    );
                }

                Self::Custom(builder.unbind())
            }
            PyRowFactory::Builder(build) => Self::Custom(build.clone_ref(py)),
        })
    }

    fn named_tuple(py: Python<'_>, names: &[&str]) -> PyResult<Self> {
        Ok(Self::NamedTuple(namedtuple_class(py, "Row", names)?))
    }

    /// Builds one Python row out of the values of its columns, in column order.
    pub(crate) fn build<'py, V: IntoPyObject<'py>>(
        &self,
        py: Python<'py>,
        values: impl ExactSizeIterator<Item = Result<V, DriverRowIterationError>>,
    ) -> Result<Py<PyAny>, DriverRowIterationError> {
        let row = match self {
            // tuple.__new__(Row, (v, ...))
            Self::NamedTuple(class) => {
                let fields = row_values(py, values)?;
                tuple_new(py)?.call1((class, fields))?
            }
            // {name: v, ...}
            Self::Dict(names) => named_values(py, names, values)?.into_any(),
            // (v, ...)
            Self::Tuple => row_values(py, values)?.into_any(),
            // cls(name=v, ...)
            Self::Class { class, names } => {
                let kwargs = named_values(py, names, values)?;
                class.bind(py).call((), Some(&kwargs))?
            }
            // build((v, ...))
            Self::Custom(build) => {
                let args = row_values(py, values)?;
                build.bind(py).call1((args,))?
            }
        };

        Ok(row.unbind())
    }
}

fn named_values<'py, V: IntoPyObject<'py>>(
    py: Python<'py>,
    names: &[Py<PyString>],
    values: impl Iterator<Item = Result<V, DriverRowIterationError>>,
) -> Result<Bound<'py, PyDict>, DriverRowIterationError> {
    let row = PyDict::new(py);

    for (name, value) in names.iter().zip(values) {
        row.set_item(name, value?)?;
    }

    Ok(row)
}

fn row_values<'py, V: IntoPyObject<'py>>(
    py: Python<'py>,
    values: impl ExactSizeIterator<Item = Result<V, DriverRowIterationError>>,
) -> Result<Bound<'py, PyTuple>, DriverRowIterationError> {
    Ok(PyTuple::new(py, values.map(PyValueOrError::new))?)
}

fn spec_names<'a>(specs: &'a [ColumnSpec<'_>]) -> Vec<&'a str> {
    specs.iter().map(ColumnSpec::name).collect()
}

fn column_names(py: Python<'_>, specs: &[ColumnSpec<'_>]) -> Vec<Py<PyString>> {
    specs
        .iter()
        .map(|spec| PyString::new(py, spec.name()).unbind())
        .collect()
}

/// `tuple.__new__`, which builds a namedtuple instance from an iterable of
/// values.
pub(crate) fn tuple_new(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    static TUPLE_NEW: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

    TUPLE_NEW
        .get_or_try_init(py, || {
            PyTuple::type_object(py)
                .getattr(intern!(py, "__new__"))
                .map(Bound::unbind)
        })
        .map(|new| new.bind(py).clone())
}

/// The namedtuple class for the values of a UDT, named after the UDT as the old driver does.
pub(crate) fn udt_namedtuple_class(
    py: Python<'_>,
    udt_name: &str,
    fields: &[&str],
) -> PyResult<Py<PyAny>> {
    let class_name = clean_name(udt_name);
    if is_identifier(&class_name, keywords(py)?) {
        namedtuple_class(py, &class_name, fields)
    } else {
        namedtuple_class(py, "Udt", fields)
    }
}

/// Namedtuple classes keyed by their class name and the raw names they were built from.
/// Building one is slow, so it is done once per shape.
static NAMEDTUPLE_CLASSES: LazyLock<RwLock<HashMap<String, Py<PyAny>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

const NAMEDTUPLE_CACHE_LIMIT: usize = 512;

/// `class_name` must be a valid class name.
fn namedtuple_class(py: Python<'_>, class_name: &str, columns: &[&str]) -> PyResult<Py<PyAny>> {
    // A class name never holds `\0`, so the key cannot mix it up with a column name.
    let mut key = String::with_capacity(
        class_name.len() + 1 + columns.iter().map(|name| name.len() + 1).sum::<usize>(),
    );
    for name in std::iter::once(class_name).chain(columns.iter().copied()) {
        key.push_str(name);
        key.push('\0');
    }

    if let Ok(classes) = NAMEDTUPLE_CLASSES.read_py_attached(py)
        && let Some(class) = classes.get(&key)
    {
        return Ok(class.clone_ref(py));
    }

    let class = py
        .import(intern!(py, "collections"))?
        .getattr(intern!(py, "namedtuple"))?
        .call1((class_name, field_names(py, class_name, columns)?))?
        .unbind();

    let Ok(mut classes) = NAMEDTUPLE_CLASSES.write_py_attached(py) else {
        return Ok(class);
    };

    if classes.len() >= NAMEDTUPLE_CACHE_LIMIT {
        classes.clear();
    }

    Ok(classes.entry(key).or_insert(class).clone_ref(py))
}

/// Turns column or UDT field names into namedtuple field names, mirroring the
/// old Python driver: strip and replace characters that cannot appear in an
/// identifier, rename what is still unusable to its position, then break ties.
fn field_names(py: Python<'_>, class_name: &str, columns: &[&str]) -> PyResult<Vec<String>> {
    let keywords = keywords(py)?;
    let mut names = Vec::with_capacity(columns.len());
    let mut seen = HashSet::with_capacity(columns.len());
    let mut renamed = false;

    for (index, column) in columns.iter().enumerate() {
        let mut name = clean_name(column);
        if !is_identifier(&name, keywords) {
            name = format!("field_{index}_");
            renamed = true;
        }

        // Cleaning is lossy and namedtuple rejects duplicates, so append `_`
        // until the name is unique, as the old driver does.
        while !seen.insert(name.clone()) {
            name.push('_');
            renamed = true;
        }

        names.push(name);
    }

    // Like the old driver, only renaming warns: plain cleaning such as
    // `[applied]` -> `applied` does not.
    if renamed {
        log::warn!(
            "Names {columns:?} cannot all be used as fields of the namedtuple `{class_name}`, so \
             it uses {names:?}. For a row, choose the names with `SELECT <column> AS <alias>`, \
             or use a different row factory. For a UDT, register a class for it with \
             `Adapters.register_udt()`."
        );
    }

    Ok(names)
}

/// The running interpreter's reserved words, read once from `keyword.kwlist`.
fn keywords(py: Python<'_>) -> PyResult<&'static HashSet<String>> {
    static KEYWORDS: PyOnceLock<HashSet<String>> = PyOnceLock::new();

    KEYWORDS.get_or_try_init(py, || {
        let kwlist = py
            .import(intern!(py, "keyword"))?
            .getattr(intern!(py, "kwlist"))?;

        let mut keywords = HashSet::with_capacity(kwlist.len()?);
        for keyword in kwlist.try_iter()? {
            keywords.insert(keyword?.extract::<String>()?);
        }

        Ok(keywords)
    })
}

fn clean_name(name: &str) -> String {
    name.trim_end_matches(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .trim_start_matches(|c: char| !c.is_ascii_alphanumeric())
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}

/// Whether a name from `clean_name` can be used as a namedtuple field.
fn is_identifier(name: &str, keywords: &HashSet<String>) -> bool {
    let Some(first) = name.chars().next() else {
        return false;
    };

    // `clean_name` already removed a leading `_` and every character that
    // cannot appear in an identifier.
    !first.is_ascii_digit() && !keywords.contains(name)
}
