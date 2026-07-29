fn main() {
    // Expose PyO3's Python version and feature flags (e.g., `Py_3_13`, `Py_LIMITED_API`)
    // to `cfg` attributes across the crate.
    pyo3_build_config::use_pyo3_cfgs();
}
