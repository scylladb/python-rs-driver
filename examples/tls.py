"""
TLS example — connect to ScyllaDB with various TLS configurations.

The example reads certificate paths and the contact point from environment
variables.  When a required file is missing the example prints a skip message
and exits cleanly, so it is safe to run in CI without a TLS-enabled cluster.

Note: A snapshot of TLSConfig is taken when calling SessionBuilder.tls_context(),
not at connection time. Any changes to the config after that call will not affect
the session.

Environment variables
---------------------
SCYLLA_URI        host:port to connect to  (default: 127.0.0.1:9042)
SCYLLA_CA_PATH    path to the CA certificate (PEM)
SCYLLA_CERT_PATH  path to the client certificate (PEM)  — required for mTLS
SCYLLA_KEY_PATH   path to the client private key (PEM)  — required for mTLS
"""

import asyncio
import os
from pathlib import Path

from scylla.session_builder import SessionBuilder
from scylla.tls import TlsContext, VerifyMode


def _contact_points() -> list[tuple[str, int]]:
    uri = os.getenv("SCYLLA_URI", "127.0.0.1:9042")
    host, port_str = uri.split(":")
    return [(host, int(port_str))]


async def connect_server_auth(ca: Path, contact_points: list[tuple[str, int]]) -> None:
    """Verify the server's certificate against a CA — the most common setup."""
    tls = TlsContext()
    tls.load_verify_locations(ca)
    tls.verify_mode = VerifyMode.CERT_REQUIRED

    session = await SessionBuilder().contact_points(contact_points).tls_context(tls).connect()

    result = await session.execute("SELECT release_version FROM system.local")
    row = await result.first_row()
    assert row is not None
    print(f"[server-auth] connected, ScyllaDB version: {row['release_version']}")


async def connect_mutual_tls(ca: Path, cert: Path, key: Path, contact_points: list[tuple[str, int]]) -> None:
    """Present a client certificate so the server can verify us (mTLS)."""
    tls = TlsContext()
    tls.load_verify_locations(ca)
    tls.load_cert_chain(cert, key)
    tls.verify_mode = VerifyMode.CERT_REQUIRED

    session = await SessionBuilder().contact_points(contact_points).tls_context(tls).connect()

    result = await session.execute("SELECT release_version FROM system.local")
    row = await result.first_row()
    assert row is not None
    print(f"[mutual-tls]  connected, ScyllaDB version: {row['release_version']}")


async def connect_no_verify(contact_points: list[tuple[str, int]]) -> None:
    """Skip certificate verification entirely — useful for local testing only."""
    tls = TlsContext()
    tls.verify_mode = VerifyMode.CERT_NONE

    session = await SessionBuilder().contact_points(contact_points).tls_context(tls).connect()

    result = await session.execute("SELECT release_version FROM system.local")
    row = await result.first_row()
    assert row is not None
    print(f"[no-verify]   connected, ScyllaDB version: {row['release_version']}")


async def main() -> None:
    contact_points = _contact_points()

    ca_env = os.getenv("SCYLLA_CA_PATH")
    cert_env = os.getenv("SCYLLA_CERT_PATH")
    key_env = os.getenv("SCYLLA_KEY_PATH")

    ca = Path(ca_env) if ca_env else None
    cert = Path(cert_env) if cert_env else None
    key = Path(key_env) if key_env else None

    if ca is None or not ca.exists():
        print(f"Skipping all TLS examples: SCYLLA_CA_PATH not set or file not found ({ca})")
        return

    await connect_server_auth(ca, contact_points)

    if cert is None or key is None or not cert.exists() or not key.exists():
        print("Skipping mTLS example: SCYLLA_CERT_PATH / SCYLLA_KEY_PATH not set or files not found")
    else:
        await connect_mutual_tls(ca, cert, key, contact_points)

    await connect_no_verify(contact_points)


asyncio.run(main())
