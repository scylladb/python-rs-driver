use pyo3::types::PyInt;
use pyo3::{Bound, IntoPyObject, PyErr, Python};
use scylla_cql::value::CqlVarintBorrowed;

pub(crate) struct CqlVarintWrapper<'b> {
    val: CqlVarintBorrowed<'b>,
}

impl<'b> From<CqlVarintBorrowed<'b>> for CqlVarintWrapper<'b> {
    fn from(val: CqlVarintBorrowed<'b>) -> Self {
        Self { val }
    }
}

impl<'py> IntoPyObject<'py> for CqlVarintWrapper<'_> {
    type Target = PyInt;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let bytes = self.val.as_signed_bytes_be_slice();

        // `PyLong_FromNativeBytes` was added in Python 3.13, but is excluded from
        // the Limited API until 3.14. Fall back to `_PyLong_FromByteArray` on older
        // non-limited builds, or use `int.from_bytes()` for 3.13 limited API.
        #[cfg(all(not(Py_LIMITED_API), Py_3_13))]
        {
            use pyo3::ffi;

            unsafe {
                let val = ffi::PyLong_FromNativeBytes(
                    bytes.as_ptr() as *const _,
                    bytes.len(),
                    ffi::Py_ASNATIVEBYTES_BIG_ENDIAN,
                );
                Ok(Bound::from_owned_ptr(py, val).cast_into()?)
            }
        }

        #[cfg(all(not(Py_LIMITED_API), not(Py_3_13)))]
        {
            use pyo3::ffi;

            unsafe {
                let val = ffi::_PyLong_FromByteArray(bytes.as_ptr(), bytes.len(), 0, 1);
                Ok(Bound::from_owned_ptr(py, val).cast_into()?)
            }
        }

        // Py_3_13 + Py_LIMITED_API: _PyLong_FromByteArray is unavailable and
        // PyLong_FromNativeBytes is not in the limited API yet.
        #[cfg(Py_LIMITED_API)]
        {
            use pyo3::intern;
            use pyo3::types::{PyAnyMethods, PyBytes, PyDict, PyDictMethods};

            let bytes_obj = PyBytes::new(py, bytes);
            let kwargs = PyDict::new(py);
            kwargs.set_item(intern!(py, "signed"), true)?;
            unsafe {
                Ok(py
                    .get_type::<PyInt>()
                    .call_method(
                        intern!(py, "from_bytes"),
                        (bytes_obj, intern!(py, "big")),
                        Some(&kwargs),
                    )?
                    .cast_into_unchecked())
            }
        }
    }
}
