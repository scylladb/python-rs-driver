use std::collections::{HashMap, HashSet};
use std::sync::{LazyLock, RwLock};

use pyo3::prelude::*;
use pyo3::sync::{PyOnceLock, RwLockExt};
use pyo3::types::{PyDict, PyString, PyTuple};
use pyo3::{Borrowed, PyTypeInfo, intern};
use scylla::frame::response::result::ColumnSpec;

use crate::cluster::metadata::query_metadata::column_spec_tuple;
use crate::deserialize::results::DeserializedColumns;
use crate::errors::{DriverRowFactoryError, DriverRowIterationError};
use crate::utils::PyValueOrError;

/// Returns every row as a `collections.namedtuple`. This is the default.
#[pyclass(name = "NamedTupleRowFactory", frozen)]
pub(crate) struct PyNamedTupleRowFactory {}

#[pymethods]
impl PyNamedTupleRowFactory {
    #[new]
    fn new() -> Self {
        Self {}
    }
}

/// Returns every row as a `dict` mapping column names to values.
#[pyclass(name = "DictRowFactory", frozen)]
pub(crate) struct PyDictRowFactory {}

#[pymethods]
impl PyDictRowFactory {
    #[new]
    fn new() -> Self {
        Self {}
    }
}

/// Returns every row as a plain `tuple` of values.
#[pyclass(name = "TupleRowFactory", frozen)]
pub(crate) struct PyTupleRowFactory {}

#[pymethods]
impl PyTupleRowFactory {
    #[new]
    fn new() -> Self {
        Self {}
    }
}

/// Returns every row as `cls(**columns)`, passing column names as keywords.
#[pyclass(name = "ClassRowFactory", frozen)]
pub(crate) struct PyClassRowFactory {
    #[pyo3(get, name = "cls")]
    class: Py<PyAny>,
}

#[pymethods]
impl PyClassRowFactory {
    #[new]
    fn new(py: Python<'_>, cls: Py<PyAny>) -> Result<Self, DriverRowFactoryError> {
        if !cls.bind(py).is_callable() {
            return Err(DriverRowFactoryError::invalid_class(
                cls.bind(py).as_borrowed(),
            ));
        }

        Ok(Self { class: cls })
    }
}

/// A row factory as handed over from Python, classified but not yet resolved:
/// resolving needs the column metadata, which only arrives with the response.
#[derive(Clone)]
pub(crate) enum PyRowFactory {
    NamedTuple,
    Dict,
    Tuple,
    Class(Py<PyAny>),
    /// An object with a `prepare` method, called once the metadata is known.
    Deferred(Py<PyAny>),
    /// A callable used directly as the row builder.
    Builder(Py<PyAny>),
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
            return Ok(Self::Class(factory.get().class.clone_ref(obj.py())));
        }

        if obj.hasattr(intern!(obj.py(), "prepare")).unwrap_or(false) {
            return Ok(Self::Deferred(obj.to_owned().unbind()));
        }

        if obj.is_callable() {
            return Ok(Self::Builder(obj.to_owned().unbind()));
        }

        Err(DriverRowFactoryError::invalid_factory(obj))
    }
}

/// A row factory resolved against the column metadata of one request.
///
/// Built once per request and reused for every row of every page.
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
            PyRowFactory::NamedTuple => Self::NamedTuple(namedtuple_class(py, specs)?),
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

    /// Builds one Python row out of the deserialized columns.
    pub(crate) fn build(
        &self,
        values: DeserializedColumns<'_, '_>,
    ) -> Result<Py<PyAny>, DriverRowIterationError> {
        let py = values.py();

        let row = match self {
            // tuple.__new__(Row, (v, ...))
            Self::NamedTuple(class) => {
                let fields = row_values(values)?;
                tuple_new(py)?.call1((class, fields))?
            }
            // {name: v, ...}
            Self::Dict(names) => named_values(names, values)?.into_any(),
            // (v, ...)
            Self::Tuple => row_values(values)?.into_any(),
            // cls(name=v, ...)
            Self::Class { class, names } => {
                let kwargs = named_values(names, values)?;
                class.bind(py).call((), Some(&kwargs))?
            }
            // build((v, ...))
            Self::Custom(build) => {
                let args = row_values(values)?;
                build.bind(py).call1((args,))?
            }
        };

        Ok(row.unbind())
    }
}

fn named_values<'py>(
    names: &[Py<PyString>],
    values: DeserializedColumns<'_, 'py>,
) -> Result<Bound<'py, PyDict>, DriverRowIterationError> {
    let row = PyDict::new(values.py());

    for (name, value) in names.iter().zip(values) {
        row.set_item(name, value?)?;
    }

    Ok(row)
}

fn row_values<'py>(
    values: DeserializedColumns<'_, 'py>,
) -> Result<Bound<'py, PyTuple>, DriverRowIterationError> {
    let py = values.py();

    Ok(PyTuple::new(py, values.map(PyValueOrError::new))?)
}

fn column_names(py: Python<'_>, specs: &[ColumnSpec<'_>]) -> Vec<Py<PyString>> {
    specs
        .iter()
        .map(|spec| PyString::new(py, spec.name()).unbind())
        .collect()
}

/// `tuple.__new__`, which builds a namedtuple instance from an iterable of
/// values.
fn tuple_new(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    static TUPLE_NEW: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

    TUPLE_NEW
        .get_or_try_init(py, || {
            PyTuple::type_object(py)
                .getattr(intern!(py, "__new__"))
                .map(Bound::unbind)
        })
        .map(|new| new.bind(py).clone())
}

/// Namedtuple classes keyed by the raw column names they were built from.
/// Building one execs a class template, so it is done once per result shape.
static NAMEDTUPLE_CLASSES: LazyLock<RwLock<HashMap<String, Py<PyAny>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

const NAMEDTUPLE_CACHE_LIMIT: usize = 512;

fn namedtuple_class(py: Python<'_>, specs: &[ColumnSpec<'_>]) -> PyResult<Py<PyAny>> {
    let mut key = String::with_capacity(specs.iter().map(|spec| spec.name().len() + 1).sum());
    for spec in specs {
        key.push_str(spec.name());
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
        .call1((intern!(py, "Row"), field_names(py, specs)?))?
        .unbind();

    let Ok(mut classes) = NAMEDTUPLE_CLASSES.write_py_attached(py) else {
        return Ok(class);
    };

    if classes.len() >= NAMEDTUPLE_CACHE_LIMIT {
        classes.clear();
    }

    Ok(classes.entry(key).or_insert(class).clone_ref(py))
}

/// Turns column names into namedtuple field names, mirroring the old Python
/// driver: strip and replace characters that cannot appear in an identifier,
/// rename what is still unusable to its position, then break ties.
fn field_names(py: Python<'_>, specs: &[ColumnSpec<'_>]) -> PyResult<Vec<String>> {
    let keywords = keywords(py)?;
    let mut names = Vec::with_capacity(specs.len());
    let mut seen = HashSet::with_capacity(specs.len());

    for (index, spec) in specs.iter().enumerate() {
        let mut name = clean_name(spec.name());
        if !is_identifier(&name, keywords) {
            name = format!("field_{index}_");
        }

        // Cleaning is lossy, so distinct columns can land on the same name, and
        // namedtuple rejects duplicate fields. We add `_` at the end same as old
        // python driver did.
        while !seen.insert(name.clone()) {
            name.push('_');
        }

        names.push(name);
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

/// Whether `name` can be used as a namedtuple field
fn is_identifier(name: &str, keywords: &HashSet<String>) -> bool {
    let Some(first) = name.chars().next() else {
        return false;
    };

    if first == '_' {
        return false;
    }

    if first.is_ascii_digit() {
        return false;
    }

    if !name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return false;
    }

    !keywords.contains(name)
}
