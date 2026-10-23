use chrono::{DateTime, NaiveDate, NaiveTime, TimeDelta, Utc};
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyDateTime, PyTzInfo, PyTzInfoAccess};
use scylla_cql::value::{CqlDate, CqlDuration, CqlTime, CqlTimestamp};

use crate::relativedelta::{Relativedelta, RelativedeltaError};

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
    name = "CqlDuration",
    frozen,
    eq,
    hash,
    skip_from_py_object
)]
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct PyCqlDuration {
    #[pyo3(get)]
    months: i32,
    #[pyo3(get)]
    days: i32,
    #[pyo3(get)]
    nanoseconds: i64,
}

/// CQL rejects a duration whose fields have different signs.
fn has_uniform_sign(months: i32, days: i32, nanoseconds: i64) -> bool {
    let non_negative = months >= 0 && days >= 0 && nanoseconds >= 0;
    let non_positive = months <= 0 && days <= 0 && nanoseconds <= 0;
    non_negative || non_positive
}

#[pymethods]
impl PyCqlDuration {
    #[new]
    #[pyo3(signature = (months = 0, days = 0, nanoseconds = 0))]
    fn new(months: i32, days: i32, nanoseconds: i64) -> PyResult<Self> {
        if !has_uniform_sign(months, days, nanoseconds) {
            return Err(PyValueError::new_err(
                "All CqlDuration fields must have the same sign.",
            ));
        }

        Ok(Self {
            months,
            days,
            nanoseconds,
        })
    }

    #[staticmethod]
    fn from_relativedelta(delta: Relativedelta) -> PyResult<Self> {
        Ok(delta.try_into()?)
    }

    #[expect(
        clippy::wrong_self_convention,
        reason = "pymethods cannot take self by value"
    )]
    fn to_relativedelta(&self) -> Relativedelta {
        Relativedelta {
            months: self.months.into(),
            days: self.days.into(),
            microseconds: self.nanoseconds / 1000,
            ..Default::default()
        }
    }

    fn __getnewargs__(&self) -> (i32, i32, i64) {
        (self.months, self.days, self.nanoseconds)
    }

    fn __repr__(&self) -> String {
        format!(
            "CqlDuration(months={}, days={}, nanoseconds={})",
            self.months, self.days, self.nanoseconds
        )
    }
}

impl TryFrom<Relativedelta> for PyCqlDuration {
    type Error = RelativedeltaError;

    fn try_from(delta: Relativedelta) -> Result<Self, Self::Error> {
        let months = delta
            .years
            .checked_mul(12)
            .and_then(|m| m.checked_add(delta.months))
            .and_then(|m| i32::try_from(m).ok());

        let days = i32::try_from(delta.days).ok();

        let nanoseconds = [
            TimeDelta::try_hours(delta.hours),
            TimeDelta::try_minutes(delta.minutes),
            TimeDelta::try_seconds(delta.seconds),
            Some(TimeDelta::microseconds(delta.microseconds)),
        ]
        .into_iter()
        .try_fold(TimeDelta::zero(), |sum, part| sum.checked_add(&part?))
        .and_then(|sum| sum.num_nanoseconds());

        let (Some(months), Some(days), Some(nanoseconds)) = (months, days, nanoseconds) else {
            return Err(RelativedeltaError::Overflow);
        };
        // Days and nanoseconds stay apart, because a day is not always 24 hours.
        if !has_uniform_sign(months, days, nanoseconds) {
            return Err(RelativedeltaError::MixedSign);
        }
        Ok(Self {
            months,
            days,
            nanoseconds,
        })
    }
}

impl From<CqlDuration> for PyCqlDuration {
    fn from(d: CqlDuration) -> Self {
        Self {
            months: d.months,
            days: d.days,
            nanoseconds: d.nanoseconds,
        }
    }
}

impl From<PyCqlDuration> for CqlDuration {
    fn from(d: PyCqlDuration) -> Self {
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
    name = "CqlDate",
    frozen,
    eq,
    ord,
    hash,
    skip_from_py_object
)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PyCqlDate {
    #[pyo3(get)]
    days: i32,
}

#[pymethods]
impl PyCqlDate {
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
        format!("CqlDate(days={})", self.days)
    }

    fn __str__(&self) -> String {
        match self.naive_date() {
            Some(date) => date.to_string(),
            None => self.days.to_string(),
        }
    }
}

impl PyCqlDate {
    fn naive_date(self) -> Option<NaiveDate> {
        CqlDate::from(self).try_into().ok()
    }
}

impl From<CqlDate> for PyCqlDate {
    fn from(d: CqlDate) -> Self {
        Self {
            days: d.0.wrapping_sub(CQL_DATE_EPOCH) as i32,
        }
    }
}

impl From<PyCqlDate> for CqlDate {
    fn from(d: PyCqlDate) -> Self {
        Self((d.days as u32).wrapping_add(CQL_DATE_EPOCH))
    }
}

const NANOS_PER_SECOND: i64 = 1_000_000_000;
const NANOS_PER_DAY: i64 = 24 * 60 * 60 * NANOS_PER_SECOND;

#[pyclass(
    module = "scylla.cql_types",
    name = "CqlTime",
    frozen,
    eq,
    ord,
    hash,
    skip_from_py_object
)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PyCqlTime {
    #[pyo3(get)]
    nanoseconds: i64,
}

#[pymethods]
impl PyCqlTime {
    #[new]
    fn new(nanoseconds: i64) -> PyResult<Self> {
        if !(0..NANOS_PER_DAY).contains(&nanoseconds) {
            return Err(PyValueError::new_err(format!(
                "CqlTime must be from 0 to {} nanoseconds.",
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
        format!("CqlTime(nanoseconds={})", self.nanoseconds)
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

impl From<CqlTime> for PyCqlTime {
    fn from(t: CqlTime) -> Self {
        Self { nanoseconds: t.0 }
    }
}

impl From<PyCqlTime> for CqlTime {
    fn from(t: PyCqlTime) -> Self {
        Self(t.nanoseconds)
    }
}

/// Converts through Python's `astimezone`, because chrono reads only fixed-offset
/// tzinfo and rejects `ZoneInfo` and pytz zones.
pub(crate) fn aware_datetime_to_utc(datetime: &Bound<'_, PyDateTime>) -> PyResult<DateTime<Utc>> {
    if datetime.get_tzinfo().is_none() {
        return Err(PyTypeError::new_err("The datetime has no time zone."));
    }
    let py = datetime.py();
    datetime
        .call_method1(intern!(py, "astimezone"), (PyTzInfo::utc(py)?,))?
        .extract()
}

#[pyclass(
    module = "scylla.cql_types",
    name = "CqlTimestamp",
    frozen,
    eq,
    ord,
    hash,
    skip_from_py_object
)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PyCqlTimestamp {
    #[pyo3(get)]
    milliseconds: i64,
}

#[pymethods]
impl PyCqlTimestamp {
    #[new]
    fn new(milliseconds: i64) -> Self {
        Self { milliseconds }
    }

    #[staticmethod]
    fn from_datetime(datetime: &Bound<'_, PyDateTime>) -> PyResult<Self> {
        Ok(CqlTimestamp::from(aware_datetime_to_utc(datetime)?).into())
    }

    #[expect(
        clippy::wrong_self_convention,
        reason = "pymethods cannot take self by value"
    )]
    fn to_datetime(&self) -> PyResult<DateTime<Utc>> {
        self.utc_datetime().ok_or_else(|| {
            PyValueError::new_err("The timestamp is outside the range of datetime.datetime.")
        })
    }

    fn __getnewargs__(&self) -> (i64,) {
        (self.milliseconds,)
    }

    fn __repr__(&self) -> String {
        format!("CqlTimestamp(milliseconds={})", self.milliseconds)
    }

    fn __str__(&self) -> String {
        match self.utc_datetime() {
            Some(datetime) => datetime.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            None => self.milliseconds.to_string(),
        }
    }
}

impl PyCqlTimestamp {
    fn utc_datetime(self) -> Option<DateTime<Utc>> {
        CqlTimestamp::from(self).try_into().ok()
    }
}

impl From<CqlTimestamp> for PyCqlTimestamp {
    fn from(t: CqlTimestamp) -> Self {
        Self { milliseconds: t.0 }
    }
}

impl From<PyCqlTimestamp> for CqlTimestamp {
    fn from(t: PyCqlTimestamp) -> Self {
        Self(t.milliseconds)
    }
}

#[pymodule]
pub(crate) fn cql_types(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<CqlEmpty>()?;
    module.add_class::<PyCqlDuration>()?;
    module.add_class::<PyCqlDate>()?;
    module.add_class::<PyCqlTime>()?;
    module.add_class::<PyCqlTimestamp>()?;
    Ok(())
}
