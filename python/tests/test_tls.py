from __future__ import annotations

from collections.abc import Generator
from datetime import datetime, timedelta, timezone
from ipaddress import ip_address
from pathlib import Path

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID
from scylla.errors import SessionConfigError, TlsError
from scylla.session_builder import SessionBuilder
from scylla.tls import TlsContext, VerifyMode
from tests.helpers.ccm import (  # pyright: ignore[reportMissingTypeStubs]
    create_scylla_cluster,
    get_contact_points,
    start_cluster,
    stop_and_remove_cluster,
)

pytestmark = pytest.mark.requires_ccm

SCYLLA_VERSION = "release:6.2.2"


# ─────────────────────────────────────────────────────────────────────────────
# Certificate generation helpers
# ─────────────────────────────────────────────────────────────────────────────


def _generate_private_key() -> RSAPrivateKey:
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def _name(common_name: str) -> x509.Name:
    return x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])


def _generate_ca(key: RSAPrivateKey) -> x509.Certificate:
    now = datetime.now(timezone.utc)
    name = _name("Test CA")
    return (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(key.public_key()),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )


def _generate_leaf(
    *,
    common_name: str,
    key: RSAPrivateKey,
    ca_cert: x509.Certificate,
    ca_key: RSAPrivateKey,
    usage: x509.ObjectIdentifier,
    san: x509.GeneralName | list[x509.GeneralName] | None = None,
    not_valid_before: datetime | None = None,
    not_valid_after: datetime | None = None,
) -> x509.Certificate:
    now = datetime.now(timezone.utc)
    builder = (
        x509.CertificateBuilder()
        .subject_name(_name(common_name))
        .issuer_name(ca_cert.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(not_valid_before or (now - timedelta(minutes=1)))
        .not_valid_after(not_valid_after or (now + timedelta(days=1)))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=True,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=False,
                crl_sign=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(x509.ExtendedKeyUsage([usage]), critical=False)
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(key.public_key()),
            critical=False,
        )
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()),
            critical=False,
        )
    )
    if san is not None:
        san_list = san if isinstance(san, list) else [san]
        builder = builder.add_extension(
            x509.SubjectAlternativeName(san_list),
            critical=False,
        )
    return builder.sign(ca_key, hashes.SHA256())


def _write_key(path: Path, key: RSAPrivateKey) -> None:
    path.write_bytes(
        key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )


def _write_cert(path: Path, cert: x509.Certificate) -> None:
    path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))


def _read_der_cert(path: Path) -> bytes:
    return x509.load_pem_x509_certificate(path.read_bytes()).public_bytes(serialization.Encoding.DER)


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def certs_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Generate ephemeral CA + server + client certs."""
    d = tmp_path_factory.mktemp("tls-certs")

    # CA
    ca_key = _generate_private_key()
    ca_cert = _generate_ca(ca_key)

    # Server cert (SAN includes all IPs used by CCM clusters)
    server_key = _generate_private_key()
    server_cert = _generate_leaf(
        common_name="127.0.0.1",
        key=server_key,
        ca_cert=ca_cert,
        ca_key=ca_key,
        usage=ExtendedKeyUsageOID.SERVER_AUTH,
        san=[
            x509.IPAddress(ip_address("127.0.0.1")),
            x509.IPAddress(ip_address("127.0.1.1")),
            x509.IPAddress(ip_address("127.0.2.1")),
        ],
    )

    # Client cert (for mutual TLS)
    client_key = _generate_private_key()
    client_cert = _generate_leaf(
        common_name="Test Client",
        key=client_key,
        ca_cert=ca_cert,
        ca_key=ca_key,
        usage=ExtendedKeyUsageOID.CLIENT_AUTH,
    )

    # Expired client cert (for negative test)
    expired_client_key = _generate_private_key()
    expired_client_cert = _generate_leaf(
        common_name="Expired Client",
        key=expired_client_key,
        ca_cert=ca_cert,
        ca_key=ca_key,
        usage=ExtendedKeyUsageOID.CLIENT_AUTH,
        not_valid_before=datetime.now(timezone.utc) - timedelta(days=2),
        not_valid_after=datetime.now(timezone.utc) - timedelta(hours=1),
    )

    # Unrelated CA (for wrong-CA test)
    wrong_ca_key = _generate_private_key()
    wrong_ca_cert = _generate_ca(wrong_ca_key)

    _write_cert(d / "ca.crt", ca_cert)
    _write_key(d / "ca.key", ca_key)
    _write_cert(d / "server.crt", server_cert)
    _write_key(d / "server.key", server_key)
    _write_cert(d / "client.crt", client_cert)
    _write_key(d / "client.key", client_key)
    _write_cert(d / "expired_client.crt", expired_client_cert)
    _write_key(d / "expired_client.key", expired_client_key)
    _write_cert(d / "wrong_ca.crt", wrong_ca_cert)

    return d


@pytest.fixture(scope="module")
def tls_cluster_mutual(
    certs_dir: Path,
) -> Generator[list[tuple[str, int]], None, None]:
    """ScyllaDB cluster with mutual TLS (server verifies client cert)."""
    cluster = create_scylla_cluster(
        name="tls_mutual",
        scylla_version=SCYLLA_VERSION,
        nodes=1,
        ipprefix="127.0.1.",
        config={
            "client_encryption_options": {
                "enabled": True,
                "require_client_auth": True,
                "certificate": str(certs_dir / "server.crt"),
                "keyfile": str(certs_dir / "server.key"),
                "truststore": str(certs_dir / "ca.crt"),
            }
        },
    )
    try:
        start_cluster(cluster)
        yield get_contact_points(cluster)
    finally:
        stop_and_remove_cluster(cluster)


@pytest.fixture(scope="module")
def tls_cluster_server_only(
    certs_dir: Path,
) -> Generator[list[tuple[str, int]], None, None]:
    """ScyllaDB cluster with server-side TLS only (no client cert required)."""
    cluster = create_scylla_cluster(
        name="tls_server_only",
        scylla_version=SCYLLA_VERSION,
        nodes=1,
        ipprefix="127.0.2.",
        config={
            "client_encryption_options": {
                "enabled": True,
                "require_client_auth": False,
                "certificate": str(certs_dir / "server.crt"),
                "keyfile": str(certs_dir / "server.key"),
            }
        },
    )
    try:
        start_cluster(cluster)
        yield get_contact_points(cluster)
    finally:
        stop_and_remove_cluster(cluster)


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
@pytest.mark.requires_ccm
async def test_tls_server_auth_only(
    tls_cluster_server_only: list[tuple[str, int]],
    certs_dir: Path,
) -> None:
    tls = TlsContext()
    tls.load_verify_locations(certs_dir / "ca.crt")
    tls.verify_mode = VerifyMode.CERT_REQUIRED

    session = await SessionBuilder().contact_points(tls_cluster_server_only).tls_context(tls).connect()

    result = await session.execute("SELECT release_version FROM system.local")
    row = await result.first_row()
    assert row is not None, "Expected at least one row from system.local"


@pytest.mark.asyncio
async def test_tls_cadata_pem_connects(
    tls_cluster_server_only: list[tuple[str, int]],
    certs_dir: Path,
) -> None:
    tls = TlsContext()
    tls.load_verify_locations(cadata=(certs_dir / "ca.crt").read_text())

    session = await SessionBuilder().contact_points(tls_cluster_server_only).tls_context(tls).connect()
    result = await session.execute("SELECT release_version FROM system.local")
    assert await result.first_row() is not None


@pytest.mark.asyncio
async def test_tls_cadata_der_connects(
    tls_cluster_server_only: list[tuple[str, int]],
    certs_dir: Path,
) -> None:
    tls = TlsContext()
    tls.load_verify_locations(cadata=_read_der_cert(certs_dir / "ca.crt"))

    session = await SessionBuilder().contact_points(tls_cluster_server_only).tls_context(tls).connect()
    result = await session.execute("SELECT release_version FROM system.local")
    assert await result.first_row() is not None


@pytest.mark.asyncio
@pytest.mark.requires_ccm
async def test_tls_mutual_auth(
    tls_cluster_mutual: list[tuple[str, int]],
    certs_dir: Path,
) -> None:
    tls = TlsContext()
    tls.load_verify_locations(certs_dir / "ca.crt")
    tls.load_cert_chain(certs_dir / "client.crt", certs_dir / "client.key")
    tls.verify_mode = VerifyMode.CERT_REQUIRED

    session = await SessionBuilder().contact_points(tls_cluster_mutual).tls_context(tls).connect()

    result = await session.execute("SELECT release_version FROM system.local")
    row = await result.first_row()
    assert row is not None


@pytest.mark.asyncio
@pytest.mark.requires_ccm
async def test_tls_wrong_ca_rejects_connection(
    tls_cluster_server_only: list[tuple[str, int]],
    certs_dir: Path,
) -> None:
    tls = TlsContext()
    tls.load_verify_locations(certs_dir / "wrong_ca.crt")
    tls.verify_mode = VerifyMode.CERT_REQUIRED

    with pytest.raises(Exception) as exc_info:
        await SessionBuilder().contact_points(tls_cluster_server_only).tls_context(tls).connect()

    error_msg = str(exc_info.value).lower()
    assert any(
        keyword in error_msg
        for keyword in [
            "certificate",
            "verify",
            "ssl",
            "tls",
            "handshake",
            "broken",
            "channel",
            "eof",
            "connection",
        ]
    ), f"Expected a TLS/connection error, got: {exc_info.value}"


@pytest.mark.asyncio
@pytest.mark.requires_ccm
async def test_tls_no_client_cert_rejected_by_mutual_tls(
    tls_cluster_mutual: list[tuple[str, int]],
    certs_dir: Path,
) -> None:
    tls = TlsContext()
    tls.load_verify_locations(certs_dir / "ca.crt")

    tls.verify_mode = VerifyMode.CERT_REQUIRED

    with pytest.raises(Exception) as exc_info:
        await SessionBuilder().contact_points(tls_cluster_mutual).tls_context(tls).connect()

    error_msg = str(exc_info.value).lower()
    assert any(
        keyword in error_msg
        for keyword in [
            "certificate",
            "ssl",
            "tls",
            "handshake",
            "alert",
            "broken",
            "channel",
            "eof",
            "connection",
        ]
    ), f"Expected a connection/TLS error, got: {exc_info.value}"


@pytest.mark.asyncio
@pytest.mark.requires_ccm
async def test_tls_expired_client_cert_rejected(
    tls_cluster_mutual: list[tuple[str, int]],
    certs_dir: Path,
) -> None:
    tls = TlsContext()
    tls.load_verify_locations(certs_dir / "ca.crt")
    tls.load_cert_chain(
        certs_dir / "expired_client.crt",
        certs_dir / "expired_client.key",
    )

    tls.verify_mode = VerifyMode.CERT_REQUIRED

    with pytest.raises(Exception) as exc_info:
        await SessionBuilder().contact_points(tls_cluster_mutual).tls_context(tls).connect()

    error_msg = str(exc_info.value).lower()
    assert any(
        keyword in error_msg
        for keyword in [
            "certificate",
            "expire",
            "ssl",
            "tls",
            "handshake",
            "alert",
            "broken",
            "channel",
            "eof",
            "connection",
        ]
    ), f"Expected a certificate expiry/TLS error, got: {exc_info.value}"


@pytest.mark.asyncio
@pytest.mark.requires_ccm
async def test_tls_no_verify_connects(
    tls_cluster_server_only: list[tuple[str, int]],
) -> None:
    tls = TlsContext()

    tls.verify_mode = VerifyMode.CERT_NONE
    # Deliberately NOT loading any CA certs — should still connect

    session = await SessionBuilder().contact_points(tls_cluster_server_only).tls_context(tls).connect()

    result = await session.execute("SELECT release_version FROM system.local")
    row = await result.first_row()
    assert row is not None


@pytest.mark.asyncio
@pytest.mark.requires_ccm
async def test_tls_query_data_integrity(
    tls_cluster_mutual: list[tuple[str, int]],
    certs_dir: Path,
) -> None:
    tls = TlsContext()
    tls.load_verify_locations(certs_dir / "ca.crt")
    tls.load_cert_chain(certs_dir / "client.crt", certs_dir / "client.key")

    tls.verify_mode = VerifyMode.CERT_REQUIRED

    session = await SessionBuilder().contact_points(tls_cluster_mutual).tls_context(tls).connect()

    await session.execute(
        "CREATE KEYSPACE IF NOT EXISTS tls_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    await session.execute("CREATE TABLE IF NOT EXISTS tls_test.data (id int PRIMARY KEY, value text)")
    await session.execute("INSERT INTO tls_test.data (id, value) VALUES (1, 'hello over TLS')")

    result = await session.execute("SELECT value FROM tls_test.data WHERE id = 1")
    row = await result.first_row()
    assert row is not None

    row_str = str(row)
    assert "hello over TLS" in row_str, f"Data integrity check failed, got: {row_str}"

    await session.execute("DROP KEYSPACE IF EXISTS tls_test")


def test_tls_context_snapshot_preservation_and_clearing(certs_dir: Path) -> None:
    """Test that TLSConfig snapshot is taken at tls_context() call time."""
    cfg = TlsContext()
    cfg.verify_mode = VerifyMode.CERT_NONE
    cfg.load_verify_locations(certs_dir / "ca.crt")

    builder = SessionBuilder().tls_context(cfg)
    snapshot = builder.get_config()

    assert snapshot.tls_context is not None
    assert snapshot.tls_context.verify_mode == VerifyMode.CERT_NONE
    assert snapshot.tls_context.cafile == certs_dir / "ca.crt"
    assert snapshot.tls_context.cadata is None
    assert snapshot.tls_context.certfile is None

    cfg.verify_mode = VerifyMode.CERT_REQUIRED
    assert snapshot.tls_context.verify_mode == VerifyMode.CERT_NONE

    cleared_snapshot = builder.tls_context(None).get_config()
    assert cleared_snapshot.tls_context is None


def test_nonexistent_ca_file_raises_session_config_error() -> None:
    bad_path = "/absolutely/does/not/exist/ca.crt"
    cfg = TlsContext()
    cfg.load_verify_locations(bad_path)

    with pytest.raises(SessionConfigError) as exc_info:
        SessionBuilder().tls_context(cfg)

    cause = exc_info.value.__cause__
    assert cause is not None, "Expected __cause__ to be set on SessionConfigError"
    assert isinstance(cause, TlsError), f"Expected TlsError as __cause__, got {type(cause).__name__}: {cause}"
    assert bad_path in str(cause), f"Expected path '{bad_path}' in error message, got: {cause}"


def test_load_verify_locations_requires_a_source() -> None:
    with pytest.raises(TlsError, match="at least one of cafile, capath, or cadata"):
        TlsContext().load_verify_locations()


def test_mismatched_client_certificate_and_key_raises_session_config_error(certs_dir: Path) -> None:
    cfg = TlsContext()
    cfg.load_cert_chain(certs_dir / "client.crt", certs_dir / "server.key")

    with pytest.raises(SessionConfigError) as exc_info:
        SessionBuilder().tls_context(cfg)

    cause = exc_info.value.__cause__
    assert isinstance(cause, TlsError)
    assert "key values mismatch" in str(cause)
