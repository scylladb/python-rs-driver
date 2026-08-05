from collections.abc import Sequence
from datetime import timedelta
from ipaddress import IPv4Address, IPv6Address
from typing import Any, TypeAlias

from scylla.policies.address_translator import AddressTranslator
from scylla.policies.authenticator_provider import AuthenticatorProvider
from scylla.policies.host_filter import HostFilter
from scylla.policies.timestamp_generator import TimestampGenerator

from .enums import Compression, PoolSize, SelfIdentity, WriteCoalescingDelay
from .execution_profile import ExecutionProfile
from .session import Session
from .tls import TlsConfig, TlsContext

ContactPoint: TypeAlias = str | tuple[str | IPv4Address | IPv6Address, int]

class SessionBuilderConfig:
    """
    A read-only snapshot of the current SessionBuilder configuration.

    This object is frozen and represents the immutable state of the builder
    at the moment it was generated.
    """

    @property
    def execution_profile(self) -> ExecutionProfile: ...
    @property
    def contact_points(self) -> list[str]: ...
    @property
    def host_filter(self) -> Any | None: ...
    @property
    def authenticator(self) -> Any | None: ...
    @property
    def address_translator(self) -> Any | None: ...
    @property
    def timestamp_generator(self) -> Any | None: ...
    @property
    def tls_context(self) -> TlsConfig | None: ...
    @property
    def shard_aware_local_port_range(self) -> tuple[int, int]: ...
    @property
    def local_ip_address(self) -> IPv4Address | IPv6Address | None: ...
    @property
    def compression(self) -> Compression | None: ...
    @property
    def tcp_nodelay(self) -> bool: ...
    @property
    def tcp_keepalive_interval(self) -> timedelta | None: ...
    @property
    def connect_timeout(self) -> timedelta: ...
    @property
    def connection_pool_size(self) -> PoolSize: ...
    @property
    def disallow_shard_aware_port(self) -> bool: ...
    @property
    def used_keyspace(self) -> str | None: ...
    @property
    def keyspace_case_sensitive(self) -> bool: ...
    @property
    def keyspaces_to_fetch(self) -> list[str]: ...
    @property
    def fetch_schema_metadata(self) -> bool: ...
    @property
    def metadata_request_serverside_timeout(self) -> timedelta | None: ...
    @property
    def schema_agreement_interval(self) -> timedelta: ...
    @property
    def schema_agreement_timeout(self) -> timedelta: ...
    @property
    def schema_agreement_automatic_waiting(self) -> bool: ...
    @property
    def refresh_metadata_on_auto_schema_agreement(self) -> bool: ...
    @property
    def cluster_metadata_refresh_interval(self) -> timedelta: ...
    @property
    def keepalive_interval(self) -> timedelta | None: ...
    @property
    def keepalive_timeout(self) -> timedelta | None: ...
    @property
    def hostname_resolution_timeout(self) -> timedelta | None: ...
    @property
    def enable_write_coalescing(self) -> bool: ...
    @property
    def write_coalescing(self) -> WriteCoalescingDelay | None: ...
    @property
    def identity(self) -> SelfIdentity: ...

class SessionBuilder:
    """
    Builder for configuring and creating a :class:`Session`.

    The builder exposes a chainable API for setting connection options before
    establishing the session with :meth:`connect`.

    Examples
    --------
    >>> session = await (
    ...     SessionBuilder()
    ...     .contact_points(["127.0.0.1:9042", ("127.0.0.2", 9042)])
    ...     .execution_profile(profile)
    ...     .connect()
    ... )
    """

    def __init__(self) -> None:
        """
        Create a new session builder with default configuration.
        """

    def contact_points(self, contact_points: ContactPoint | Sequence[ContactPoint]) -> SessionBuilder:
        """
        Set the contact points used to bootstrap the connection.

        Parameters
        ----------
        contact_points : ContactPoint | Sequence[ContactPoint]
            One contact point or a sequence of contact points.

            Each contact point may be provided as:

            - ``str`` — for example ``"127.0.0.1"`` or ``"127.0.0.1:9042"``
            - ``tuple[str, int]`` — for example ``("127.0.0.1", 9042)``
            - ``tuple[IPv4Address | IPv6Address, int]`` — for example
              ``(IPv4Address("127.0.0.1"), 9042)``

        Returns
        -------
        SessionBuilder
        """

    def execution_profile(self, execution_profile: ExecutionProfile) -> SessionBuilder:
        """
        Set the default execution profile for the session.

        Parameters
        ----------
        execution_profile : ExecutionProfile
            The execution profile to use by default for requests executed through
            the created session.

        Returns
        -------
        SessionBuilder
        """

    async def connect(self) -> Session:
        """
        Establish a session using the current builder configuration.

        Returns
        -------
        Session
            A connected session ready to execute queries.
        """

    def user(self, username: str, password: str) -> SessionBuilder:
        """
        Set plain-text credentials for authentication.

        Parameters
        ----------
        username : str
        password : str

        Returns
        -------
        SessionBuilder
        """

    def authenticator_provider(self, authenticator: AuthenticatorProvider) -> SessionBuilder:
        """
        Set a custom authenticator provider.

        Parameters
        ----------
        authenticator : AuthenticatorProvider
            An instance of a class inheriting from :class:`AuthenticatorProvider`.

        Returns
        -------
        SessionBuilder
        """

    def address_translator(self, translator: AddressTranslator) -> SessionBuilder:
        """
        Registers an address translator for the session.

        The translator is Python object implementing the
        :class:`AddressTranslator` protocol.

        Parameters
        ----------
        translator : AddressTranslator

        Returns
        -------
        SessionBuilder
        """

    def timestamp_generator(self, generator: TimestampGenerator) -> SessionBuilder:
        """
        Registers a custom Python-defined timestamp generator.

        The generator is used to assign client-side timestamps to requests.
        If the custom generator fails or is not implemented, it will fall back
        to the current system timestamp.

        Parameters
        ----------
        generator : TimestampGenerator
            A custom Python object implementing the :class:`TimestampGenerator`
            protocol.

        Returns
        -------
        SessionBuilder
        """

    def host_filter(self, host_filter: HostFilter) -> SessionBuilder:
        """
        Registers a host filter or a list of allowed addresses.

        This decides whether a discovered node should be accepted by the driver.
        You can provide a custom filter object for complex logic or one of the
        built-in host filter implementations provided by the driver.

        Parameters
        ----------
        host_filter : HostFilter
            If a object implements :class:`HostFilter` protocol, the driver calls
            its ``accept`` method for each node.

            Address = str | tuple[str | IPv4Address | IPv6Address, int]

        Returns
        -------
        SessionBuilder
        """

    def local_ip_address(self, ip: IPv4Address | IPv6Address | str | None) -> SessionBuilder:
        """
        Sets the local IP address all TCP sockets are bound to.

        By default, this option is set to ``None``, which allows to
        bind to any available address (equivalent to ``INADDR_ANY`` for IPv4
        or ``in6addr_any`` for IPv6).

        Parameters
        ----------
        ip : IPv4Address | IPv6Address | None
            The local IP address to bind to, or ``None`` for the default behavior.

        Returns
        -------
        SessionBuilder
        """

    def shard_aware_local_port_range(self, port_range: tuple[int, int]) -> SessionBuilder:
        """
        Specifies the local port range used for shard-aware connections.

        A possible use case is when you want to have multiple :class:`Session` objects and do not want
        them to compete for the ports within the same range. It is then advised to assign
        mutually non-overlapping port ranges to each session object.

        The provided range is inclusive on both ends (i.e. ``[start, end]``).

        By default the driver uses port range ``(49152, 65535)``.

        **Validation Rules:**
        A ``SessionConfigError`` is raised if:
        1. The range is empty (``end`` < ``start``).
        2. The range starts below port ``1024`` (reserved system ports).

        Parameters
        ----------
        port_range : tuple[int, int]
            A tuple of (start_port, end_port), e.g., (49152, 65535).

        Returns
        -------
        SessionBuilder
        """

    def compression(self, compression: Compression | None) -> SessionBuilder:
        """
        Sets the preferred compression algorithm for the connection.

        By default, no compression is used.

        If the specified compression algorithm is not supported by the
        database server, the session will automatically fall back to
        no compression.

        Parameters
        ----------
        compression : Compression | None
            The compression algorithm to use (e.g., ``Compression.Lz4``),
            or ``None`` to disable compression.

        Returns
        -------
        SessionBuilder
        """

    def schema_agreement_interval(self, interval: timedelta | float) -> SessionBuilder:
        """
        Sets how often the driver checks for schema agreement.

        The default is 200 milliseconds.

        Parameters
        ----------
        interval : timedelta | float
            The interval duration. If a ``float`` is provided,
            it is interpreted as **seconds**.

        Returns
        -------
        SessionBuilder
        """

    def tcp_nodelay(self, nodelay: bool) -> SessionBuilder:
        """
        Set the nodelay TCP flag. The default is true.

        Parameters
        ----------
        nodelay : bool

        Returns
        -------
        SessionBuilder
        """

    def tcp_keepalive_interval(self, timeout: timedelta | float | None) -> SessionBuilder:
        """
        Sets the TCP-level keepalive interval.

        The default is `None`, which implies that no keepalive messages are sent **on TCP layer** when a connection is idle.

        **Note:**
        CQL-layer keepalives are configured separately, with `keepalive_interval`

        Parameters
        ----------
        timeout : timedelta | float | None
            The interval between keepalive probes. If ``float``, interpreted
            as seconds. Set to ``None`` to disable.

        Returns
        -------
        SessionBuilder
        """

    def use_keyspace(self, keyspace_name: str, case_sensitive: bool) -> SessionBuilder:
        """
        Sets the keyspace to be used for all connections created by this session.

        Each connection created by the driver will automatically execute
        ``USE <keyspace_name>`` before performing any other operations.

        This can be changed later on an active session using ``Session.use_keyspace``.

        Parameters
        ----------
        keyspace_name : str
        case_sensitive : bool

        Returns
        -------
        SessionBuilder
        """

    def connection_timeout(self, timeout: timedelta | float) -> SessionBuilder:
        """
        Sets the timeout for establishing a new connection to a node. The default is 5 seconds.

        Parameters
        ----------
        timeout : timedelta | float
            The connection timeout. If a ``float`` is provided, it is
            interpreted as **seconds**. Must be non-negative and finite.

        Returns
        -------
        SessionBuilder
        """

    def pool_size(self, size: PoolSize) -> SessionBuilder:
        """
        Sets the per-node connection pool size.

        The default is one connection per shard, which is the recommended
        setting for Scylla.

        Parameters
        ----------
        size : PoolSize

        Returns
        -------
        SessionBuilder
        """

    def disallow_shard_aware_port(self, disallow: bool) -> SessionBuilder:
        """
        Controls whether the driver may connect to the shard-aware port.

        By default, shard-aware port connections are allowed. This is a
        Scylla-specific option and usually should not be changed.

        Parameters
        ----------
        disallow : bool

        Returns
        -------
        SessionBuilder
        """

    def keyspaces_to_fetch(self, keyspaces: Sequence[str]) -> SessionBuilder:
        """
        Sets which keyspaces should be fetched.

        By default, all keyspaces are fetched.

        Parameters
        ----------
        keyspaces : Sequence[str]

        Returns
        -------
        SessionBuilder
        """

    def fetch_schema_metadata(self, fetch: bool) -> SessionBuilder:
        """
        Controls whether schema metadata should be fetched.

        The default is true.

        Parameters
        ----------
        fetch : bool

        Returns
        -------
        SessionBuilder
        """

    def metadata_request_serverside_timeout(self, timeout: timedelta | float) -> SessionBuilder:
        """
        Sets the server-side timeout for metadata queries.

        The default is 2 seconds.

        Parameters
        ----------
        timeout : timedelta | float
            The timeout duration. If a ``float`` is provided,
            it is interpreted as **seconds**.

        Returns
        -------
        SessionBuilder
        """

    def keepalive_interval(self, interval: timedelta | float | None) -> SessionBuilder:
        """
        Sets the CQL-level keepalive interval.

        The default is 30 seconds.

        Set to ``None`` to disable CQL-level keepalives.

        Parameters
        ----------
        interval : timedelta | float | None
            The interval duration. If a ``float`` is provided,
            it is interpreted as **seconds**.

        Returns
        -------
        SessionBuilder
        """

    def keepalive_timeout(self, timeout: timedelta | float | None) -> SessionBuilder:
        """
        Sets the keepalive timeout.

        The default is 30 seconds.

        Set to ``None`` to disable the keepalive timeout.

        Parameters
        ----------
        timeout : timedelta | float | None
            The timeout duration. If a ``float`` is provided,
            it is interpreted as **seconds**.

        Returns
        -------
        SessionBuilder
        """

    def schema_agreement_timeout(self, timeout: timedelta | float) -> SessionBuilder:
        """
        Sets the timeout for waiting for schema agreement.

        The default is 60 seconds.

        Parameters
        ----------
        timeout : timedelta | float
            The timeout duration. If a ``float`` is provided,
            it is interpreted as **seconds**.

        Returns
        -------
        SessionBuilder
        """

    def auto_await_schema_agreement(self, enabled: bool) -> SessionBuilder:
        """
        Controls automatic waiting for schema agreement after schema changes.

        The default is true.

        Parameters
        ----------
        enabled : bool

        Returns
        -------
        SessionBuilder
        """

    def hostname_resolution_timeout(self, duration: timedelta | float | None) -> SessionBuilder:
        """
        Sets the DNS hostname resolution timeout.

        The default is 5 seconds. Use ``None`` to disable the timeout.

        Parameters
        ----------
        duration : timedelta | float | None
            The timeout duration. If a ``float`` is provided,
            it is interpreted as **seconds**.

        Returns
        -------
        SessionBuilder
        """

    def refresh_metadata_on_auto_schema_agreement(self, refresh_metadata: bool) -> SessionBuilder:
        """
        Controls whether metadata is refreshed after automatic schema agreement.

        The default is true.

        Parameters
        ----------
        refresh_metadata : bool

        Returns
        -------
        SessionBuilder
        """

    def write_coalescing(self, delay: WriteCoalescingDelay | None) -> SessionBuilder:
        """
        Configures write coalescing.

        When a delay is provided, the driver introduces a wait period before
        flushing data to the socket. This allows it to batch multiple write
        requests into a single system call, improving throughput.

        To disable write coalescing, pass ``None``.

        This optimization may increase latency if the application sends
        requests infrequently. It is recommended to benchmark before
        disabling this feature.

        Default: ``WriteCoalescingDelay.small_nondeterministic()``

        Parameters
        ----------
        delay : WriteCoalescingDelay | None
            The delay configuration to use, or ``None`` to disable write
            coalescing entirely.

        Returns
        -------
        SessionBuilder
        """

    def custom_identity(self, identity: SelfIdentity) -> SessionBuilder:
        """
        Sets self-identifying information sent by the driver in the STARTUP message.

        By default, the driver sends its built-in driver name and version.
        Other identity fields are not sent unless explicitly set.

        Parameters
        ----------
        identity : SelfIdentity
            Self-identifying information to advertise.

        Returns
        -------
        SessionBuilder
        """

    def tls_context(self, tls_context: TlsContext | None) -> SessionBuilder:
        """
        Set the TLS configuration for all connections created by this session.

        Passes the given :class:`~scylla.tls.SslConfig` to the Rust driver,
        which builds an OpenSSL context from it at connection time.  Pass
        ``None`` to disable TLS (the default).

        Raises :class:`~scylla.errors.SessionConfigError` (with a
        :class:`~scylla.errors.TlsError` as ``__cause__``) if the OpenSSL
        context cannot be constructed from the provided configuration (e.g. a
        CA file path does not exist, or a certificate/key pair is invalid).

        Parameters
        ----------
        tls_context : SslConfig | None
            TLS configuration to use, or ``None`` to disable TLS.

        Returns
        -------
        SessionBuilder
        """

    def get_config(self) -> SessionBuilderConfig:
        """
        Returns a read-only snapshot of the current driver configuration state.

        Returns
        -------
        SessionBuilderConfig
        """
