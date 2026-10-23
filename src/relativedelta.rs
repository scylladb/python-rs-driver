use pyo3::exceptions::{PyOverflowError, PyTypeError, PyValueError};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyDict, PyType};

/// Relative fields of a `dateutil.relativedelta.relativedelta`.
#[derive(Default)]
pub(crate) struct Relativedelta {
    pub(crate) years: i64,
    pub(crate) months: i64,
    pub(crate) days: i64,
    pub(crate) hours: i64,
    pub(crate) minutes: i64,
    pub(crate) seconds: i64,
    pub(crate) microseconds: i64,
}

pub(crate) enum RelativedeltaError {
    NotRelativedelta,
    Overflow,
    MixedSign,
}

impl From<RelativedeltaError> for PyErr {
    fn from(err: RelativedeltaError) -> Self {
        match err {
            RelativedeltaError::NotRelativedelta => {
                PyTypeError::new_err("The value is not a dateutil.relativedelta.relativedelta.")
            }
            RelativedeltaError::Overflow => {
                PyOverflowError::new_err("A CqlDuration field does not fit in its integer size.")
            }
            RelativedeltaError::MixedSign => {
                PyValueError::new_err("All CqlDuration fields must have the same sign.")
            }
        }
    }
}

impl<'py> FromPyObject<'_, 'py> for Relativedelta {
    type Error = RelativedeltaError;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        let py = obj.py();
        let attr = |name| {
            obj.getattr(name)
                .and_then(|value| value.extract::<i64>())
                .map_err(|_| RelativedeltaError::NotRelativedelta)
        };

        Ok(Self {
            years: attr(intern!(py, "years"))?,
            months: attr(intern!(py, "months"))?,
            days: attr(intern!(py, "days"))?,
            hours: attr(intern!(py, "hours"))?,
            minutes: attr(intern!(py, "minutes"))?,
            seconds: attr(intern!(py, "seconds"))?,
            microseconds: attr(intern!(py, "microseconds"))?,
        })
    }
}

impl<'py> IntoPyObject<'py> for Relativedelta {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        static RELATIVEDELTA: PyOnceLock<Py<PyType>> = PyOnceLock::new();
        let relativedelta = RELATIVEDELTA.import(py, "dateutil.relativedelta", "relativedelta")?;
        let kwargs = PyDict::new(py);
        kwargs.set_item(intern!(py, "years"), self.years)?;
        kwargs.set_item(intern!(py, "months"), self.months)?;
        kwargs.set_item(intern!(py, "days"), self.days)?;
        kwargs.set_item(intern!(py, "hours"), self.hours)?;
        kwargs.set_item(intern!(py, "minutes"), self.minutes)?;
        kwargs.set_item(intern!(py, "seconds"), self.seconds)?;
        kwargs.set_item(intern!(py, "microseconds"), self.microseconds)?;
        relativedelta.call((), Some(&kwargs))
    }
}
