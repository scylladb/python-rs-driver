//! The driver hands a custom address translator a `PyUntranslatedPeer`, its
//! own conversion of the Rust driver's `UntranslatedPeer`. The test cluster
//! always reports a datacenter and a rack, so the conversion of a peer that
//! has neither is checked here. The rest is in
//! `python/tests/test_session_builder.py`.

use std::net::{IpAddr, Ipv6Addr, SocketAddr};

use pyo3::prelude::*;
use scylla::policies::address_translator::UntranslatedPeer;
use uuid::Uuid;

use super::attr;
use crate::policies::address_translator::PyUntranslatedPeer;

#[test]
fn untranslated_peer_without_datacenter_and_rack() {
    Python::initialize();
    let host_id = Uuid::from_u128(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef);
    let address = SocketAddr::new(
        IpAddr::V6(Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 7)),
        19042,
    );
    let peer = UntranslatedPeer::from_fields(host_id, address, None, None);
    let py_peer = PyUntranslatedPeer::from(&peer);

    Python::attach(|py| {
        let obj = Bound::new(py, py_peer).unwrap().into_any();
        // Read every getter twice: the second read comes from the cache.
        for _ in 0..2 {
            assert_eq!(attr::<Uuid>(&obj, "host_id"), host_id);
            assert_eq!(
                attr::<(IpAddr, u16)>(&obj, "untranslated_address"),
                (address.ip(), address.port())
            );
            assert_eq!(attr::<Option<String>>(&obj, "datacenter"), None);
            assert_eq!(attr::<Option<String>>(&obj, "rack"), None);
        }
    });
}
