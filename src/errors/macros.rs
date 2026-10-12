/// Defines a new exception type with several base classes, e.g. a driver error that is also a `ValueError`.
///
/// Copied from pyo3 0.29.0 `create_exception!` / `create_exception_type_object!`,
/// changed to take a tuple of bases: pyo3 builds the type with `PyErr::new_type`, which accepts
/// a single base, so this calls `PyErr_NewExceptionWithDoc` directly with a tuple instead.
///
/// ```ignore
/// create_exception_multi!(scylla.errors, BadQuery, (ScyllaError, PyValueError));
/// ```
macro_rules! create_exception_multi {
    ($module: expr, $name: ident, ($($base: ty),+ $(,)?)) => {
        #[repr(transparent)]
        pub struct $name(::pyo3::PyAny);

        ::pyo3::impl_exception_boilerplate!($name);

        create_exception_multi!(@type_object $module, $name, ($($base),+), ::core::option::Option::None);
    };
    ($module: expr, $name: ident, ($($base: ty),+ $(,)?), $doc: expr) => {
        #[repr(transparent)]
        #[doc = $doc]
        pub struct $name(::pyo3::PyAny);

        ::pyo3::impl_exception_boilerplate!($name);

        create_exception_multi!(
            @type_object $module,
            $name,
            ($($base),+),
            ::core::option::Option::Some(::pyo3::ffi::c_str!($doc))
        );
    };
    (@type_object $module: expr, $name: ident, ($($base: ty),+), $doc: expr) => {
        ::pyo3::pyobject_native_type_named!($name);

        // SAFETY: macro caller has upheld the safety contracts
        unsafe impl ::pyo3::type_object::PyTypeInfo for $name {
            const NAME: &'static str = stringify!($name);
            const MODULE: ::core::option::Option<&'static str> =
                ::core::option::Option::Some(stringify!($module));
            ::pyo3::create_exception_type_hint!($module, $name);

            #[inline]
            fn type_object_raw(py: ::pyo3::Python<'_>) -> *mut ::pyo3::ffi::PyTypeObject {
                use ::pyo3::sync::PyOnceLock;
                static TYPE_OBJECT: PyOnceLock<::pyo3::Py<::pyo3::types::PyType>> =
                    PyOnceLock::new();

                TYPE_OBJECT
                    .get_or_init(py, || {
                        let doc: ::core::option::Option<&::core::ffi::CStr> = $doc;
                        let bases = ::pyo3::types::PyTuple::new(py, [$(py.get_type::<$base>()),+])
                            .expect("Failed to build exception base classes.");

                        // SAFETY: valid name and doc C strings, and a tuple of exception types;
                        // returns a new reference to the exception type or null on error.
                        unsafe {
                            ::pyo3::Bound::from_owned_ptr_or_err(
                                py,
                                ::pyo3::ffi::PyErr_NewExceptionWithDoc(
                                    ::pyo3::ffi::c_str!(concat!(
                                        stringify!($module),
                                        ".",
                                        stringify!($name)
                                    ))
                                    .as_ptr(),
                                    doc.map_or(::core::ptr::null(), ::core::ffi::CStr::as_ptr),
                                    bases.as_ptr(),
                                    ::core::ptr::null_mut(),
                                ),
                            )
                        }
                        .and_then(|obj| Ok(obj.cast_into::<::pyo3::types::PyType>()?.unbind()))
                        .expect("Failed to initialize new exception type.")
                    })
                    .as_ptr()
                    .cast()
            }
        }

        impl $name {
            #[doc(hidden)]
            pub const _PYO3_DEF: ::pyo3::impl_::pymodule::AddTypeToModule<Self> =
                ::pyo3::impl_::pymodule::AddTypeToModule::new();

            #[allow(dead_code)]
            #[doc(hidden)]
            pub const _PYO3_INTROSPECTION_ID: &'static str =
                concat!(stringify!($module), stringify!($name));
        }
    };
}

/// Creates the exception `$exc` with `$message` and sets each listed binding, converted
/// through `ToPyAttr`, as the attribute of the same name.
///
/// ```ignore
/// py_err!(Unavailable, message; consistency, required, alive)
/// ```
macro_rules! py_err {
    ($exc: ty, $message: expr) => {
        <$exc>::new_err($message)
    };
    ($exc: ty, $message: expr; $($attr: ident),+ $(,)?) => {
        $crate::errors::with_attrs(<$exc>::new_err($message), |exc| {
            use $crate::errors::ToPyAttr as _;
            $( exc.setattr(stringify!($attr), $attr.to_py_attr(exc.py())?)?; )+
            Ok(())
        })
    };
}

pub(crate) use py_err;
