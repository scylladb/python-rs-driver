import ipaddress
import time
from collections.abc import Generator, Sequence
from datetime import timedelta
from typing import Any

import pytest
from _pytest.logging import LogCaptureFixture

# TODO: move the ccm-backed tests and their cluster fixtures out of this file
# into a dedicated module (test_session_builder_ccm.py). They are the only reason
# this file imports helpers.ccm.
from helpers.ccm import (  # pyright: ignore[reportMissingTypeStubs]
    create_scylla_cluster,
    get_contact_points,
    start_cluster,
    stop_and_remove_cluster,
)
from helpers.ddl import ddl
from scylla.enums import Compression, Consistency, PoolSize, SelfIdentity, SerialConsistency, WriteCoalescingDelay
from scylla.errors import AddressTranslationError, HostFilterError, SessionConfigError
from scylla.execution_profile import ExecutionProfile
from scylla.policies.address_translator import (
    AddressTranslator,
    DictAddressTranslator,
    UntranslatedPeer,
)
from scylla.policies.authenticator_provider import (
    Authenticator,
    AuthenticatorProvider,
)
from scylla.policies.host_filter import (
    AcceptAllHostFilter,
    AllowListHostFilter,
    DcHostFilter,
    HostFilter,
    Peer,
)
from scylla.policies.timestamp_generator import (
    MonotonicTimestampGenerator,
    SimpleTimestampGenerator,
    TimestampGenerator,
)
from scylla.session_builder import SessionBuilder


@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize(
    "item",
    [
        "127.0.0.2",
        ("127.0.0.2", 9042),
        (ipaddress.IPv4Address("127.0.0.2"), 9042),
        ["127.0.0.2:9042", ("127.0.0.3", 9042), (ipaddress.IPv6Address("::1"), 9042), ("::2", 9042)],
    ],
)
async def test_contact_points_extraction_formats(item: Any):
    builder = SessionBuilder().contact_points(item)
    await builder.connect()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "item",
    [["127.0.0.1", 9042], (None, 9042), ("127.0.0.1", 9042, "extra"), ("127.0.0.2", 999999), ("127.0.0.2", -1)],
)
async def test_contact_points_invalid_types(item: Any):
    builder = SessionBuilder()
    with pytest.raises(SessionConfigError) as excinfo:
        builder.contact_points(item)  # type: ignore[arg-type]

    cause = excinfo.value.__cause__
    assert cause is not None
    # The cause is the AddressParseError (either InvalidItem or InvalidType)
    # For sequence items, the inner cause contains the type error
    if cause.__cause__ is not None:
        assert (
            "Invalid address type: expected str | tuple(str, int) | tuple(ipaddress, int) or a sequence of these"
            in str(cause.__cause__)
        )
    else:
        assert (
            "Invalid address type: expected str | tuple(str, int) | tuple(ipaddress, int) or a sequence of these"
            in str(cause)
        )


@pytest.fixture(scope="module")
def ccm_contact_points() -> Generator[list[tuple[str, int]], Any, None]:
    cluster = create_scylla_cluster(
        name="auth_cluster",
        scylla_version="release:6.2.2",
        nodes=1,
        config={
            "authenticator": "PasswordAuthenticator",
        },
    )

    start_cluster(cluster)

    try:
        yield get_contact_points(cluster)
    finally:
        stop_and_remove_cluster(cluster)


class MockPlainTextAuthenticator(Authenticator):
    def __init__(self, username: str, password: str):
        super().__init__()
        self.username = username
        self.password = password
        self.challenge_called = False
        self.success_called = False

    def initial_response(self) -> bytes | None:
        return f"\x00{self.username}\x00{self.password}".encode()

    def evaluate_challenge(self, challenge: bytes | None) -> bytes | None:
        self.challenge_called = True
        return b""

    def success(self, token: bytes | None) -> None:
        self.success_called = True


class FailingAuthenticator(Authenticator):
    def initial_response(self) -> bytes | None:
        raise RuntimeError("Python Authentication Exploded!")


class SimpleProvider(AuthenticatorProvider):
    def __init__(self, authenticator: Authenticator):
        super().__init__()
        self.auth = authenticator

    def new_authenticator(self, authenticator_name: str) -> Authenticator:
        return self.auth


@pytest.mark.asyncio
@pytest.mark.requires_ccm
async def test_custom_authenticator_success(ccm_contact_points: list[tuple[str, int]]):
    auth = MockPlainTextAuthenticator("cassandra", "cassandra")

    simple_provider = SimpleProvider(auth)

    builder = SessionBuilder().contact_points(ccm_contact_points).authenticator_provider(simple_provider)

    session = await builder.connect()

    result = await session.execute("SELECT release_version FROM system.local")
    row = await result.first_row()
    assert row is not None
    assert auth.success_called is True


@pytest.mark.asyncio
@pytest.mark.requires_ccm
async def test_custom_authenticator_failing_python_side(ccm_contact_points: list[tuple[str, int]]):
    auth = FailingAuthenticator()

    simple_provider = SimpleProvider(auth)

    builder = SessionBuilder().contact_points(ccm_contact_points).authenticator_provider(simple_provider)

    with pytest.raises(Exception) as excinfo:
        await builder.connect()

    assert "Python Authentication Exploded" in str(excinfo.value)


@pytest.mark.asyncio
@pytest.mark.requires_ccm
async def test_builtin_user_credentials(ccm_contact_points: list[tuple[str, int]]):
    builder = SessionBuilder().contact_points(ccm_contact_points).user("cassandra", "cassandra")

    session = await builder.connect()
    result = await session.execute("SELECT cluster_name FROM system.local")
    assert await result.first_row() is not None


Address = ipaddress.IPv4Address | ipaddress.IPv6Address


class MockAddressTranslator:
    default_ip: Address
    default_port: int
    call_log: list[UntranslatedPeer]

    def __init__(self, default_ip: Address, default_port: int) -> None:
        super().__init__()
        self.default_ip = default_ip
        self.default_port = default_port
        self.call_log = []

    def translate(self, info: UntranslatedPeer) -> tuple[Address, int]:
        self.call_log.append(info)
        return self.default_ip, self.default_port


class FailingTranslator:
    def translate(self, info: UntranslatedPeer) -> tuple[Address, int]:
        raise RuntimeError("Translation Exploded!")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_custom_address_translator_discovery():
    translator = MockAddressTranslator(ipaddress.IPv4Address("127.0.0.2"), 9042)
    assert isinstance(translator, AddressTranslator)

    builder = (
        SessionBuilder()
        .contact_points([("127.0.0.2", 9042)])
        .address_translator(translator)
        .user("cassandra", "cassandra")
    )

    _ = await builder.connect()

    assert len(translator.call_log) > 0, "Translator was never called!"

    translated_ips = [str(p.untranslated_address[0]) for p in translator.call_log]

    print(f"Nodes seen by translator: {translated_ips}")

    assert "127.0.0.3" in translated_ips or "127.0.0.4" in translated_ips


@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.xfail(reason="Currently, Python exceptions in the translator do not propagate to the driver")
async def test_address_translator_failing_python_side():
    translator = FailingTranslator()
    assert isinstance(translator, AddressTranslator)

    builder = (
        SessionBuilder()
        .contact_points([("127.0.0.2", 9042)])
        .user("cassandra", "cassandra")
        .address_translator(translator)
    )

    with pytest.raises(Exception) as excinfo:
        await builder.connect()

    assert "Translation Exploded" in str(excinfo.value)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_address_translator_dict_discovery():
    translator = {
        (ipaddress.IPv4Address("127.0.0.2"), 9042): (ipaddress.IPv4Address("127.0.0.2"), 9042),
        ("127.0.0.3", 9042): ("127.0.0.2", 9042),
        ("127.0.0.4", 9042): ("127.0.0.2", 9042),
    }

    builder = (
        SessionBuilder()
        .contact_points([("127.0.0.2", 9042)])
        .address_translator(DictAddressTranslator(translator))
        .user("cassandra", "cassandra")
    )

    session = await builder.connect()

    assert session is not None


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_address_translator_dict_invalid():
    translator = {
        ("127.0.0.3.3", 9042): ("127.0.0.2.5", 9042),
    }

    with pytest.raises(AddressTranslationError) as excinfo:
        DictAddressTranslator(translator)

    assert "invalid socket address syntax" in str(excinfo.value.__cause__)


class MockTimestampGenerator:
    fixed_ts: int
    called: bool

    def __init__(self, fixed_ts: int) -> None:
        super().__init__()
        self.fixed_ts = fixed_ts
        self.called = False

    def next_timestamp(self) -> int:
        self.called = True
        return self.fixed_ts


class FailingTimestampGenerator:
    def next_timestamp(self) -> int:
        raise RuntimeError("Timestamp Generation Exploded!")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_custom_timestamp_generator_success() -> None:
    my_custom_ts = 1122334455
    ts_gen = MockTimestampGenerator(my_custom_ts)
    assert isinstance(ts_gen, TimestampGenerator)

    builder = (
        SessionBuilder()
        .contact_points([("127.0.0.2", 9042)])
        .user("cassandra", "cassandra")
        .timestamp_generator(ts_gen)
    )

    session = await builder.connect()

    await ddl(
        session,
        "CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = "
        "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}",
    )
    await ddl(session, "CREATE TABLE IF NOT EXISTS ks.verify_ts (id int PRIMARY KEY, val text)")
    await session.execute("INSERT INTO ks.verify_ts (id, val) VALUES (99, 'hello')")

    result = await session.execute("SELECT WRITETIME(val) FROM ks.verify_ts WHERE id = 99")
    row = await result.first_row()

    assert row is not None

    db_timestamp = row["writetime(val)"]
    assert db_timestamp == my_custom_ts
    assert ts_gen.called is True


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_simple_timestamp_generator_success() -> None:
    ts_gen = SimpleTimestampGenerator()

    builder = (
        SessionBuilder()
        .contact_points([("127.0.0.2", 9042)])
        .user("cassandra", "cassandra")
        .timestamp_generator(ts_gen)
    )

    session = await builder.connect()

    await ddl(
        session,
        "CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = "
        "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}",
    )
    await ddl(session, "CREATE TABLE IF NOT EXISTS ks.verify_simple_ts (id int PRIMARY KEY, val text)")

    now_micros = int(time.time() * 1_000_000)

    await session.execute("INSERT INTO ks.verify_simple_ts (id, val) VALUES (1, 'rust-ts')")

    result = await session.execute("SELECT WRITETIME(val) FROM ks.verify_simple_ts WHERE id = 1")
    row = await result.first_row()

    assert row is not None
    db_timestamp = row["writetime(val)"]

    assert db_timestamp >= now_micros
    assert db_timestamp < now_micros + 5_000_000


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_custom_timestamp_generator_fallback_on_failure(
    caplog: LogCaptureFixture,
) -> None:
    ts_gen = FailingTimestampGenerator()
    assert isinstance(ts_gen, TimestampGenerator)

    builder = (
        SessionBuilder()
        .contact_points([("127.0.0.2", 9042)])
        .user("cassandra", "cassandra")
        .timestamp_generator(ts_gen)
    )

    session = await builder.connect()

    await session.execute("SELECT now() FROM system.local")

    assert "Failed to generate custom timestamp from Python" in caplog.text
    assert "Timestamp Generation Exploded!" in caplog.text


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_monotonic_timestamp_generator_works_with_session() -> None:
    ts_gen = MonotonicTimestampGenerator()

    builder = SessionBuilder().contact_points([("127.0.0.2", 9042)]).timestamp_generator(ts_gen)

    session = await builder.connect()

    await ddl(
        session,
        "CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = "
        "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}",
    )
    await ddl(session, "CREATE TABLE IF NOT EXISTS ks.verify_monotonic_ts (id int PRIMARY KEY, val text)")

    await session.execute("INSERT INTO ks.verify_monotonic_ts (id, val) VALUES (1, 'a')")
    await session.execute("UPDATE ks.verify_monotonic_ts SET val = 'b' WHERE id = 1")

    result = await session.execute("SELECT WRITETIME(val) FROM ks.verify_monotonic_ts WHERE id = 1")
    row = await result.first_row()

    assert row is not None
    assert isinstance(row["writetime(val)"], int)
    assert row["writetime(val)"] > 0


class CustomAcceptAllHostFilter:
    def __init__(self) -> None:
        super().__init__()
        self.called = False
        self.last_peer_host_id: object | None = None
        self.last_peer_address: tuple[object, int] | None = None

    def accept(self, peer: Peer) -> bool:
        self.called = True
        self.last_peer_host_id = peer.host_id
        self.last_peer_address = peer.address
        return True


class FailingHostFilter:
    def accept(self, peer: Peer) -> bool:
        raise RuntimeError("Host Filter Exploded!")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_custom_host_filter_success() -> None:
    host_filter = CustomAcceptAllHostFilter()
    assert isinstance(host_filter, HostFilter)

    builder = (
        SessionBuilder().contact_points([("127.0.0.2", 9042)]).user("cassandra", "cassandra").host_filter(host_filter)
    )

    session = await builder.connect()

    result = await session.execute("SELECT now() FROM system.local")
    row = await result.first_row()

    assert row is not None
    assert host_filter.called is True
    assert host_filter.last_peer_host_id is not None
    assert host_filter.last_peer_address is not None


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_custom_host_filter_fallback_on_failure(
    caplog: LogCaptureFixture,
) -> None:
    host_filter = FailingHostFilter()
    assert isinstance(host_filter, HostFilter)

    builder = (
        SessionBuilder().contact_points([("127.0.0.2", 9042)]).user("cassandra", "cassandra").host_filter(host_filter)
    )

    session = await builder.connect()

    result = await session.execute("SELECT now() FROM system.local")
    row = await result.first_row()

    assert row is not None
    assert "Failed to evaluate custom host filter from Python" in caplog.text
    assert "Host Filter Exploded!" in caplog.text


@pytest.mark.parametrize(
    "ip",
    [
        "127.0.0.1",
        "::1",
        ipaddress.IPv4Address("127.0.0.2"),
        ipaddress.IPv6Address("::2"),
        None,
    ],
)
def test_local_ip_address_valid_formats(ip: Any):
    builder = SessionBuilder().local_ip_address(ip)
    assert isinstance(builder, SessionBuilder)


@pytest.mark.parametrize(
    "bad_range",
    [
        ((2000, 1000)),
        ((80, 2000)),
        ((1023, 1024)),
    ],
)
def test_port_range_validation_logic(bad_range: tuple[int, int]):
    builder = SessionBuilder()
    with pytest.raises(SessionConfigError) as excinfo:
        builder.shard_aware_local_port_range(bad_range)

    assert "Invalid port range" in str(excinfo.value)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "valid_range",
    [
        ((1024, 2000)),
        ((1024, 1024)),
    ],
)
async def test_port_range_boundary_valid(valid_range: tuple[int, int]):
    builder = SessionBuilder().shard_aware_local_port_range(valid_range)
    assert isinstance(builder, SessionBuilder)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "valid_duration",
    [
        0.5,
        5,
        timedelta(milliseconds=200),
        timedelta(seconds=2, microseconds=500),
        0.0,
    ],
)
async def test_schema_agreement_interval_happy_path(valid_duration: Any):
    builder = SessionBuilder().schema_agreement_interval(valid_duration)
    assert isinstance(builder, SessionBuilder)


@pytest.mark.parametrize(
    "invalid_input",
    [
        -1.0,
        float("inf"),
    ],
)
def test_schema_agreement_interval_error_consistency(invalid_input: Any):
    builder = SessionBuilder()
    with pytest.raises(ValueError) as excinfo:
        builder.schema_agreement_interval(invalid_input)

    assert "Expected a datetime.timedelta or a non-negative finite float (seconds)" in str(excinfo.value)


def test_tcp_keepalive_warnings(
    caplog: LogCaptureFixture,
):
    _ = SessionBuilder().tcp_keepalive_interval(0.5)
    assert "Setting the TCP keepalive interval to low values" in caplog.text


@pytest.mark.parametrize(
    "pool_size",
    [
        PoolSize.per_host(5),
        PoolSize.per_shard(5),
    ],
)
def test_pool_size_happy_path(pool_size: PoolSize):
    builder = SessionBuilder().pool_size(pool_size)
    assert isinstance(builder, SessionBuilder)


@pytest.mark.parametrize(
    "valid_keyspaces",
    [
        ["ks1", "ks2"],
        ("ks1", "ks2"),
        [],
    ],
)
def test_keyspaces_to_fetch_happy_path(valid_keyspaces: Sequence[str]):
    builder = SessionBuilder().keyspaces_to_fetch(valid_keyspaces)
    assert isinstance(builder, SessionBuilder)


def test_keepalive_warning_on_invalid_values(
    caplog: LogCaptureFixture,
):
    _ = SessionBuilder().keepalive_interval(0.5)
    assert "Setting the keepalive interval to low values" in caplog.text


def test_keepalive_timeout_on_invalid_values(
    caplog: LogCaptureFixture,
):
    _ = SessionBuilder().keepalive_timeout(0.5)
    assert "Setting the keepalive timeout to low values" in caplog.text


@pytest.mark.parametrize(
    "delay",
    [WriteCoalescingDelay.small_nondeterministic(), WriteCoalescingDelay.from_seconds(0.05), None],
)
def test_coalescing_delay(delay: WriteCoalescingDelay | None):
    builder = SessionBuilder().write_coalescing(delay)
    assert isinstance(builder, SessionBuilder)


@pytest.mark.parametrize(
    "zero_input",
    [
        0,
        timedelta(seconds=0),
        timedelta(milliseconds=0),
    ],
)
def test_write_coalescing_delay_zero_error(zero_input: Any):
    with pytest.raises(SessionConfigError) as excinfo:
        WriteCoalescingDelay.from_seconds(zero_input)

    assert "Duration must be greater than zero." in str(excinfo.value)


def test_self_identity_constructor_defaults():
    identity = SelfIdentity()

    assert identity.custom_driver_name == "ScyllaDB Python RS Driver"
    assert identity.custom_driver_version is not None
    assert identity.application_name is None
    assert identity.application_version is None
    assert identity.client_id is None


def test_self_identity_constructor_values():
    identity = SelfIdentity(
        custom_driver_name="custom-driver",
        custom_driver_version="1.2.3",
        application_name="my-app",
        application_version="4.5.6",
        client_id="client-1",
    )

    assert identity.custom_driver_name == "custom-driver"
    assert identity.custom_driver_version == "1.2.3"
    assert identity.application_name == "my-app"
    assert identity.application_version == "4.5.6"
    assert identity.client_id == "client-1"

    builder = SessionBuilder().custom_identity(identity)
    assert isinstance(builder, SessionBuilder)


@pytest.mark.asyncio
async def test_session_builder_config_snapshot_matching():
    target_keyspace = "production_data"
    test_interval = timedelta(seconds=12)
    test_timeout = timedelta(seconds=45)
    test_fetch_keyspaces = ["ks1", "ks2", "system_auth"]

    builder = (
        SessionBuilder()
        .tcp_nodelay(True)
        .disallow_shard_aware_port(True)
        .use_keyspace(target_keyspace, case_sensitive=False)
        .schema_agreement_interval(test_interval)
        .schema_agreement_timeout(test_timeout)
        .keyspaces_to_fetch(test_fetch_keyspaces)
        .fetch_schema_metadata(False)
        .compression(Compression.Lz4)
    )

    config = builder.get_config()

    assert config.tcp_nodelay is True
    assert config.disallow_shard_aware_port is True
    assert config.used_keyspace == target_keyspace
    assert config.keyspace_case_sensitive is False
    assert config.schema_agreement_interval == test_interval
    assert config.schema_agreement_timeout == test_timeout
    assert config.keyspaces_to_fetch == test_fetch_keyspaces
    assert config.fetch_schema_metadata is False
    assert config.compression == Compression.Lz4


@pytest.mark.asyncio
async def test_session_builder_complex_types_and_identity():
    custom_profile = ExecutionProfile(
        timeout=15.0, consistency=Consistency.All, serial_consistency=SerialConsistency.Serial
    )

    custom_identity = SelfIdentity(
        custom_driver_name="Python-Test-Driver",
        custom_driver_version="9.9.9",
        application_name="Scylla-Validation-Suite",
    )

    auth_provider = SimpleProvider(MockPlainTextAuthenticator("admin", "secret"))

    builder = (
        SessionBuilder()
        .execution_profile(custom_profile)
        .custom_identity(custom_identity)
        .authenticator_provider(auth_provider)
    )
    config = builder.get_config()

    assert config.execution_profile is custom_profile

    assert config.identity.custom_driver_name == "Python-Test-Driver"
    assert config.identity.application_name == "Scylla-Validation-Suite"

    assert config.authenticator is auth_provider


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_accept_all_host_filter() -> None:
    host_filter = AcceptAllHostFilter()

    builder = SessionBuilder().contact_points([("127.0.0.2", 9042)]).host_filter(host_filter)

    session = await builder.connect()

    result = await session.execute("SELECT release_version FROM system.local")
    row = await result.first_row()

    assert row is not None
    assert len(row) == 1


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_dc_host_filter_matches() -> None:
    host_filter = DcHostFilter("datacenter1")

    builder = SessionBuilder().contact_points([("127.0.0.2", 9042)]).host_filter(host_filter)

    session = await builder.connect()

    result = await session.execute("SELECT data_center FROM system.local")
    row = await result.first_row()

    assert row is not None
    assert row["data_center"] == "datacenter1"


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_host_filter_list_with_resolvable_dns() -> None:
    accepted_list = ["127.0.0.1:9042", ("127.0.0.2", 9042), "localhost:9042"]

    builder = SessionBuilder().contact_points([("127.0.0.2", 9042)]).host_filter(AllowListHostFilter(accepted_list))

    session = await builder.connect()
    assert session is not None

    await session.execute("SELECT * FROM system.local")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_host_filter_list_with_garbage_string_fails() -> None:
    garbage_list = ["this-is-not-an-address-and-has-no-port", ("127.0.0.1", 9042)]

    with pytest.raises(HostFilterError) as excinfo:
        _ = AllowListHostFilter(garbage_list)

    assert "invalid socket address" in str(excinfo.value).lower()
