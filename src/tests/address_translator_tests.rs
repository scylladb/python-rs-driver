//! The driver hands a custom address translator a `PyUntranslatedPeer`. The
//! test cluster always reports a datacenter and a rack, so the `None` cases
//! and the conversion back to the Rust driver's type are checked here.

use std::net::{IpAddr, Ipv6Addr, SocketAddr};

use pyo3::prelude::*;
use scylla::policies::address_translator::UntranslatedPeer;
use uuid::Uuid;

use crate::policies::address_translator::PyUntranslatedPeer;

fn attr<'py, T: FromPyObjectOwned<'py>>(obj: &Bound<'py, PyAny>, name: &str) -> T {
    let value = obj.getattr(name).unwrap();
    value.extract().map_err(Into::<PyErr>::into).unwrap()
}

#[test]
fn untranslated_peer_exposes_fields_and_converts_back() {
    Python::initialize();
    let host_id = Uuid::from_u128(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef);
    let address = SocketAddr::new(
        IpAddr::V6(Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 7)),
        19042,
    );

    for (datacenter, rack) in [(Some("dc-east"), Some("rack-3")), (None, None)] {
        let peer = UntranslatedPeer::from_fields(host_id, address, datacenter, rack);
        let py_peer = PyUntranslatedPeer::from(&peer);

        // Back to the Rust driver's type.
        let back = UntranslatedPeer::from(&py_peer);
        assert_eq!(back.host_id(), host_id);
        assert_eq!(back.untranslated_address(), address);
        assert_eq!(back.datacenter(), datacenter);
        assert_eq!(back.rack(), rack);

        Python::attach(|py| {
            let obj = Bound::new(py, py_peer).unwrap().into_any();
            // Read every getter twice: the second read comes from the cache.
            for _ in 0..2 {
                assert_eq!(attr::<Uuid>(&obj, "host_id"), host_id);
                assert_eq!(
                    attr::<(IpAddr, u16)>(&obj, "untranslated_address"),
                    (address.ip(), address.port())
                );
                assert_eq!(
                    attr::<Option<String>>(&obj, "datacenter").as_deref(),
                    datacenter
                );
                assert_eq!(attr::<Option<String>>(&obj, "rack").as_deref(), rack);
            }

            let repr = obj.repr().unwrap();
            let repr = repr.to_str().unwrap();
            let prefix = format!(
                "UntranslatedPeer(host_id='{host_id}', untranslated_address=('fd00::7', 19042), "
            );
            assert!(repr.starts_with(&prefix), "{repr}");
            // A datacenter or rack that is set comes out Rust-style, as
            // `Some("dc-east")`; test_untranslated_peer_repr_is_python_style in
            // python/tests/test_session_builder.py is the xfail for it.
            if datacenter.is_none() {
                assert_eq!(&repr[prefix.len()..], "datacenter=None, rack=None)");
            }
        });
    }
}
