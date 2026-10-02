//! Python-facing result types: thin facades over [`crate::core::results`].

mod iterators;
mod paging_state;
mod request_result;

pub(crate) use paging_state::PyPagingState;
pub(crate) use request_result::RequestResult;

use crate::deserialize::row_factory::{
    PyClassRowFactory, PyDictRowFactory, PyNamedTupleRowFactory, PyRowFactoryBase,
    PyTupleRowFactory,
};
use iterators::{AsyncRowsIterator, SinglePageIterator};
use pyo3::prelude::{PyModule, PyModuleMethods};
use pyo3::{Bound, PyResult, Python, pymodule};

#[pymodule]
pub(crate) fn results(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyRowFactoryBase>()?;
    module.add_class::<PyNamedTupleRowFactory>()?;
    module.add_class::<PyDictRowFactory>()?;
    module.add_class::<PyTupleRowFactory>()?;
    module.add_class::<PyClassRowFactory>()?;
    module.add_class::<SinglePageIterator>()?;
    module.add_class::<PyPagingState>()?;
    module.add_class::<RequestResult>()?;
    module.add_class::<AsyncRowsIterator>()?;

    Ok(())
}
