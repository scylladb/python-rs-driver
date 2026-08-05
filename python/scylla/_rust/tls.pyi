from enum import IntEnum
from pathlib import Path

class VerifyMode(IntEnum):
    """Controls how the peer certificate is verified during the TLS handshake."""

    CERT_NONE = ...
    """Do not verify the peer certificate."""

    CERT_REQUIRED = ...
    """Require and verify the peer certificate."""

class TlsContext:
    """
    TLS configuration for a ScyllaDB session.

    Pass an instance to :meth:`~scylla.session_builder.SessionBuilder.tls_context` —
    a snapshot of the configuration is taken at that moment, and the actual OpenSSL
    context is built internally when needed.

    The driver always operates in client mode, verifying server certificates by default.
    """

    def __init__(self) -> None:
        """
        Create a new ``SslConfig`` with default client settings.

        By default, ``verify_mode`` is set to ``CERT_REQUIRED``.
        """

    def load_verify_locations(
        self,
        cafile: str | Path | None = None,
        capath: str | Path | None = None,
        cadata: str | bytes | bytearray | None = None,
    ) -> None:
        """
        Load CA certificates used to verify server certificates.

        At least one argument must be provided. ``cafile`` is a PEM file,
        ``capath`` is an OpenSSL-hashed certificate directory, and ``cadata``
        is either PEM text or a DER-encoded certificate.
        """

    def load_cert_chain(
        self,
        certfile: str | Path,
        keyfile: str | Path | None = None,
    ) -> None:
        """
        Set the client certificate and private key for mutual TLS (mTLS).

        If ``keyfile`` is ``None``, the private key is read from ``certfile``.
        Only unencrypted PEM private keys are currently supported.
        """

    @property
    def verify_mode(self) -> VerifyMode:
        """The current peer-certificate verification mode."""

    @verify_mode.setter
    def verify_mode(self, mode: VerifyMode) -> None: ...

class TlsConfig:
    """Immutable snapshot of a :class:`TlsContext` assigned to a session builder."""

    @property
    def cafile(self) -> Path | None: ...
    @property
    def capath(self) -> Path | None: ...
    @property
    def cadata(self) -> str | bytes | None: ...
    @property
    def certfile(self) -> Path | None: ...
    @property
    def keyfile(self) -> Path | None: ...
    @property
    def verify_mode(self) -> VerifyMode: ...
