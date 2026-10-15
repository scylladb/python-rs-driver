use chrono::{NaiveDate, NaiveTime};
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use scylla_cql::value::{CqlDate, CqlDuration, CqlTime};

static UNSET_INSTANCE: PyOnceLock<Py<UnsetType>> = PyOnceLock::new();

#[pyclass(module = "scylla.statement")]
pub(crate) struct UnsetType;

#[pymethods]
impl UnsetType {
    #[new]
    fn new(py: Python<'_>) -> Py<UnsetType> {
        Self::get_instance(py)
    }

    fn __repr__(&self) -> &'static str {
        "UNSET"
    }

    fn __str__(&self) -> &'static str {
        "UNSET"
    }
}

impl UnsetType {
    pub(crate) fn get_instance(py: Python<'_>) -> Py<UnsetType> {
        UNSET_INSTANCE
            // There is nothing we can do when creating a global instance fails.
            .get_or_init(py, || {
                Py::new(py, UnsetType).expect("Failed to create UnsetType instance")
            })
            .clone_ref(py)
    }
}

/// A setting that may be left unset, represented on the Python side by `Unset`.
pub(crate) enum MaybeUnset<T> {
    Unset,
    Set(T),
}

impl<'a, 'py, T> FromPyObject<'a, 'py> for MaybeUnset<T>
where
    T: FromPyObject<'a, 'py>,
{
    type Error = T::Error;

    fn extract(obj: Borrowed<'a, 'py, PyAny>) -> Result<Self, Self::Error> {
        if obj.is_instance_of::<UnsetType>() {
            return Ok(Self::Unset);
        }
        obj.extract().map(Self::Set)
    }
}

impl<'py, T> IntoPyObject<'py> for MaybeUnset<T>
where
    T: IntoPyObject<'py>,
{
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        match self {
            Self::Unset => Ok(UnsetType::get_instance(py).into_bound(py).into_any()),
            Self::Set(value) => value.into_bound_py_any(py),
        }
    }
}

#[pyclass(module = "scylla.cql_types", frozen)]
pub struct CqlEmpty;

#[pymethods]
impl CqlEmpty {
    fn __repr__(&self) -> &'static str {
        "CqlEmpty"
    }
}

#[pyclass(
    module = "scylla.cql_types",
    name = "Duration",
    frozen,
    eq,
    hash,
    skip_from_py_object
)]
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct DurationValue {
    #[pyo3(get)]
    months: i32,
    #[pyo3(get)]
    days: i32,
    #[pyo3(get)]
    nanoseconds: i64,
}

/// CQL rejects a duration whose fields have different signs.
pub(crate) fn has_uniform_sign(months: i32, days: i32, nanoseconds: i64) -> bool {
    let non_negative = months >= 0 && days >= 0 && nanoseconds >= 0;
    let non_positive = months <= 0 && days <= 0 && nanoseconds <= 0;
    non_negative || non_positive
}

#[pymethods]
impl DurationValue {
    #[new]
    #[pyo3(signature = (months = 0, days = 0, nanoseconds = 0))]
    fn new(months: i32, days: i32, nanoseconds: i64) -> PyResult<Self> {
        if !has_uniform_sign(months, days, nanoseconds) {
            return Err(PyValueError::new_err(
                "All Duration fields must have the same sign.",
            ));
        }

        Ok(Self {
            months,
            days,
            nanoseconds,
        })
    }

    fn __getnewargs__(&self) -> (i32, i32, i64) {
        (self.months, self.days, self.nanoseconds)
    }

    fn __repr__(&self) -> String {
        format!(
            "Duration(months={}, days={}, nanoseconds={})",
            self.months, self.days, self.nanoseconds
        )
    }
}

impl From<CqlDuration> for DurationValue {
    fn from(d: CqlDuration) -> Self {
        Self {
            months: d.months,
            days: d.days,
            nanoseconds: d.nanoseconds,
        }
    }
}

impl From<DurationValue> for CqlDuration {
    fn from(d: DurationValue) -> Self {
        Self {
            months: d.months,
            days: d.days,
            nanoseconds: d.nanoseconds,
        }
    }
}

/// CQL stores a date as an unsigned day count with 1970-01-01 at 2^31.
const CQL_DATE_EPOCH: u32 = 1 << 31;

#[pyclass(
    module = "scylla.cql_types",
    name = "Date",
    frozen,
    eq,
    ord,
    hash,
    skip_from_py_object
)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DateValue {
    #[pyo3(get)]
    days: i32,
}

#[pymethods]
impl DateValue {
    #[new]
    fn new(days: i32) -> Self {
        Self { days }
    }

    #[staticmethod]
    fn from_date(date: NaiveDate) -> Self {
        CqlDate::from(date).into()
    }

    #[expect(
        clippy::wrong_self_convention,
        reason = "pymethods cannot take self by value"
    )]
    fn to_date(&self) -> PyResult<NaiveDate> {
        self.naive_date()
            .ok_or_else(|| PyValueError::new_err("The date is outside the range of datetime.date."))
    }

    fn __getnewargs__(&self) -> (i32,) {
        (self.days,)
    }

    fn __repr__(&self) -> String {
        format!("Date(days={})", self.days)
    }

    fn __str__(&self) -> String {
        match self.naive_date() {
            Some(date) => date.to_string(),
            None => self.days.to_string(),
        }
    }
}

impl DateValue {
    fn naive_date(self) -> Option<NaiveDate> {
        CqlDate::from(self).try_into().ok()
    }
}

impl From<CqlDate> for DateValue {
    fn from(d: CqlDate) -> Self {
        Self {
            days: d.0.wrapping_sub(CQL_DATE_EPOCH) as i32,
        }
    }
}

impl From<DateValue> for CqlDate {
    fn from(d: DateValue) -> Self {
        Self((d.days as u32).wrapping_add(CQL_DATE_EPOCH))
    }
}

const NANOS_PER_SECOND: i64 = 1_000_000_000;
const NANOS_PER_DAY: i64 = 24 * 60 * 60 * NANOS_PER_SECOND;

#[pyclass(
    module = "scylla.cql_types",
    name = "Time",
    frozen,
    eq,
    ord,
    hash,
    skip_from_py_object
)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TimeValue {
    #[pyo3(get)]
    nanoseconds: i64,
}

#[pymethods]
impl TimeValue {
    #[new]
    fn new(nanoseconds: i64) -> PyResult<Self> {
        if !(0..NANOS_PER_DAY).contains(&nanoseconds) {
            return Err(PyValueError::new_err(format!(
                "Time must be from 0 to {} nanoseconds.",
                NANOS_PER_DAY - 1
            )));
        }

        Ok(Self { nanoseconds })
    }

    #[staticmethod]
    fn from_time(time: NaiveTime) -> PyResult<Self> {
        let time: CqlTime = time.try_into().map_err(|_| {
            PyValueError::new_err("The time is a leap second, which CQL time cannot hold.")
        })?;
        Ok(time.into())
    }

    #[expect(
        clippy::wrong_self_convention,
        reason = "pymethods cannot take self by value"
    )]
    fn to_time(&self) -> PyResult<NaiveTime> {
        CqlTime(self.nanoseconds)
            .try_into()
            .map_err(|_| PyValueError::new_err("The time is out of range."))
    }

    #[getter]
    fn hour(&self) -> i64 {
        self.nanoseconds / (60 * 60 * NANOS_PER_SECOND)
    }

    #[getter]
    fn minute(&self) -> i64 {
        self.nanoseconds / (60 * NANOS_PER_SECOND) % 60
    }

    #[getter]
    fn second(&self) -> i64 {
        self.nanoseconds / NANOS_PER_SECOND % 60
    }

    #[getter]
    fn nanosecond(&self) -> i64 {
        self.nanoseconds % NANOS_PER_SECOND
    }

    fn __getnewargs__(&self) -> (i64,) {
        (self.nanoseconds,)
    }

    fn __repr__(&self) -> String {
        format!("Time(nanoseconds={})", self.nanoseconds)
    }

    fn __str__(&self) -> String {
        format!(
            "{:02}:{:02}:{:02}.{:09}",
            self.hour(),
            self.minute(),
            self.second(),
            self.nanosecond()
        )
    }
}

impl From<CqlTime> for TimeValue {
    fn from(t: CqlTime) -> Self {
        Self { nanoseconds: t.0 }
    }
}

impl From<TimeValue> for CqlTime {
    fn from(t: TimeValue) -> Self {
        Self(t.nanoseconds)
    }
}

#[pymodule]
pub(crate) fn types(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<UnsetType>()?;
    module.add_class::<CqlEmpty>()?;
    module.add_class::<DurationValue>()?;
    module.add_class::<DateValue>()?;
    module.add_class::<TimeValue>()?;
    Ok(())
}
