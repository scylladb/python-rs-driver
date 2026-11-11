"""Which exception a failure raises, and what it says, is public API.

These tests drive the error paths reachable through the public API: invalid
policy objects given to `SessionBuilder`, and requests the driver rejects
before sending them. The mapping of every Rust driver error variant is pinned
by `src/tests/error_mapping_tests.rs`.
"""

import uuid
from collections.abc import AsyncGenerator, Callable
from typing import Any

import pytest
import pytest_asyncio
from helpers.ddl import ddl
from helpers.session import connect
from scylla.errors import (
    BadKeyspaceName,
    BadQuery,
    CqlResponseKind,
    ExecutionError,
    OperationType,
    PagingStateNotAllowed,
    RequestFailedError,
    ScyllaError,
    SerializationError,
    SessionConfigError,
    StatementConversionError,
    UseKeyspaceError,
    ValueOverflowSerializationError,
    ValueSerializationError,
    WriteType,
)
from scylla.results import PagingState
from scylla.session import Session, SessionBuilder, SessionBuilderConfig


class NotAPolicy:
    """Implements none of the policy protocols."""


class LooksLikeAuthenticatorProvider:
    """Has the method of an `AuthenticatorProvider`, but does not subclass it."""

    def new_authenticator(self, name: str) -> None: ...


# (SessionBuilder method, expected message without the type name, the config property it sets)
INVALID_POLICY_CASES: list[tuple[str, str, Callable[[SessionBuilderConfig], object]]] = [
    (
        "authenticator_provider",
        "Expected an AuthenticatorProvider subclass",
        lambda config: config.authenticator,
    ),
    (
        "address_translator",
        "Expected a class implementing AddressTranslator protocol",
        lambda config: config.address_translator,
    ),
    (
        "timestamp_generator",
        "Expected a class implementing TimestampGenerator protocol",
        lambda config: config.timestamp_generator,
    ),
    (
        "host_filter",
        "Expected a class implementing HostFilter protocol",
        lambda config: config.host_filter,
    ),
]


@pytest.mark.parametrize(
    ("method", "expected_message", "configured"),
    INVALID_POLICY_CASES,
    ids=[case[0] for case in INVALID_POLICY_CASES],
)
@pytest.mark.parametrize("value", [NotAPolicy(), 42], ids=["object", "int"])
def test_invalid_policy_is_rejected_by_the_builder(
    method: str,
    expected_message: str,
    configured: Callable[[SessionBuilderConfig], object],
    value: object,
):
    builder = SessionBuilder()

    with pytest.raises(SessionConfigError) as excinfo:
        getattr(builder, method)(value)

    assert isinstance(excinfo.value, ScyllaError)
    # Not wrapped in pyo3's `TypeError` for a failed argument conversion.
    assert not isinstance(excinfo.value, TypeError)
    assert str(excinfo.value) == f"{expected_message}, got {type(value).__name__}"
    assert excinfo.value.__cause__ is None
    # The rejected object is not stored.
    assert configured(builder.get_config()) is None


def test_authenticator_provider_must_be_a_subclass():
    builder = SessionBuilder()
    provider: Any = LooksLikeAuthenticatorProvider()

    with pytest.raises(SessionConfigError) as excinfo:
        builder.authenticator_provider(provider)

    assert str(excinfo.value) == "Expected an AuthenticatorProvider subclass, got LooksLikeAuthenticatorProvider"
    assert builder.get_config().authenticator is None


@pytest_asyncio.fixture(scope="module")
async def session() -> Session:
    """One session for the module: none of these tests changes its state."""
    return await connect()


@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize(
    "name",
    # CQL allows letters, digits and underscores, up to 48 characters.
    ["", "bad-ks", "k" * 49],
    ids=["empty", "illegal_character", "too_long"],
)
async def test_use_keyspace_with_invalid_name_is_bad_keyspace_name(session: Session, name: str):
    with pytest.raises(BadKeyspaceName) as excinfo:
        await session.use_keyspace(name)

    assert isinstance(excinfo.value, UseKeyspaceError)
    assert isinstance(excinfo.value, ExecutionError)
    assert isinstance(excinfo.value, ValueError)
    # Rejected by the driver, not by the server.
    assert not isinstance(excinfo.value, RequestFailedError)
    # The prefix is this driver's. The reason after it is the Rust driver's wording,
    # so only its presence is checked.
    prefix = "Passed invalid keyspace name to use: "
    message = str(excinfo.value)
    assert message.startswith(prefix)
    assert len(message) > len(prefix)
    # Nothing was sent, so the session is still usable.
    result = await session.execute("SELECT key FROM system.local")
    assert len(await result.all()) == 1


@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize("paging_state", [PagingState(), PagingState.from_bytes(b"\x01\x02")], ids=["start", "bytes"])
async def test_paging_state_on_unpaged_execution_is_not_allowed(session: Session, paging_state: PagingState):
    with pytest.raises(PagingStateNotAllowed) as excinfo:
        await session.execute("SELECT key FROM system.local", paging_state=paging_state, paged=False)

    assert isinstance(excinfo.value, BadQuery)
    assert isinstance(excinfo.value, ValueError)
    assert isinstance(excinfo.value, ScyllaError)
    # Rejected before execution.
    assert not isinstance(excinfo.value, ExecutionError)
    assert str(excinfo.value) == "Paging state must be None for unpaged execution"


@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize("method", ["execute", "prepare"])
async def test_statement_without_utf8_encoding_is_statement_conversion_error(session: Session, method: str):
    # A lone surrogate is a valid `str` that has no UTF-8 encoding.
    statement = "SELECT key FROM system.local WHERE key = '\ud800'"

    with pytest.raises(StatementConversionError) as excinfo:
        await getattr(session, method)(statement)

    assert isinstance(excinfo.value, BadQuery)
    assert isinstance(excinfo.value, ValueError)
    # The statement is fine as a `str`, only its encoding fails.
    assert not isinstance(excinfo.value, TypeError)
    assert str(excinfo.value) == "Failed to convert statement string to Rust string"
    cause = excinfo.value.__cause__
    assert isinstance(cause, UnicodeEncodeError)
    assert cause.object == statement
    assert cause.reason == "surrogates not allowed"


@pytest_asyncio.fixture
async def int_table(session: Session) -> AsyncGenerator[str, None]:
    keyspace = f"error_mapping_ks_{uuid.uuid4().hex[:12]}"
    try:
        await ddl(
            session,
            f"CREATE KEYSPACE {keyspace} WITH replication = "
            "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}",
        )
        await ddl(session, f"CREATE TABLE {keyspace}.t (id int PRIMARY KEY, v int)")
        yield f"{keyspace}.t"
    finally:
        # Also when the creation failed on our side but reached the server.
        await ddl(session, f"DROP KEYSPACE IF EXISTS {keyspace}")


@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize(
    ("values", "bind_marker"),
    [((1, 2**31), 1), ({"id": 1, "v": 2**31}, "v")],
    ids=["positional", "named"],
)
async def test_value_the_column_cannot_hold_is_value_overflow(
    session: Session, int_table: str, values: Any, bind_marker: int | str
):
    prepared = await session.prepare(f"INSERT INTO {int_table} (id, v) VALUES (?, ?)")

    with pytest.raises(ValueOverflowSerializationError) as excinfo:
        await session.execute(prepared, values)

    error = excinfo.value
    assert isinstance(error, ValueSerializationError)
    assert isinstance(error, SerializationError)
    assert isinstance(error, BadQuery)
    assert isinstance(error, OverflowError)
    assert isinstance(error, ValueError)
    # Serialized on the client before the request is built.
    assert not isinstance(error, ExecutionError)
    # The bind marker is its position for positional values and its name for named ones.
    assert type(error.bind_marker) is type(bind_marker)
    assert error.bind_marker == bind_marker
    message = str(error)
    assert message.startswith("Failed to serialize values: ")
    assert message.endswith(f"Value overflow during serialization (bind_marker={bind_marker})")
    # Nothing was written.
    result = await session.execute(f"SELECT v FROM {int_table} WHERE id = 1")
    assert await result.all() == []


def test_error_attribute_types_are_built_from_python():
    # Equality and the Rust conversions are pinned by src/tests/error_mapping_tests.rs.
    # This checks what a user does: build the values through the public import path.
    custom_write = WriteType.Other("CUSTOM")
    assert isinstance(custom_write, WriteType)
    assert custom_write.value == "CUSTOM"
    assert custom_write == WriteType.Other("CUSTOM")

    custom_operation = OperationType.Other(7)
    assert isinstance(custom_operation, OperationType)
    assert custom_operation.code == 7
    assert custom_operation == OperationType.Other(7)

    assert int(CqlResponseKind.Ready) == 1
    assert repr(CqlResponseKind.Ready) == "CqlResponseKind.Ready"


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="Bug: errors.pyi declares CqlResponseKind as an IntEnum, but its members are not `int`s.",
)
def test_cql_response_kind_is_an_int_as_its_stub_declares():
    assert int in type(CqlResponseKind.Ready).__mro__


@pytest.mark.xfail(
    strict=True,
    raises=TypeError,
    reason="Bug: errors.pyi declares CqlResponseKind as an IntEnum, whose members are hashable, "
    "but CqlResponseKind defines equality without a hash.",
)
def test_cql_response_kind_is_hashable_as_its_stub_declares():
    assert hash(CqlResponseKind.Ready) == hash(CqlResponseKind.Ready)


@pytest.mark.xfail(
    strict=True,
    raises=TypeError,
    reason="Bug: errors.pyi declares OperationType.Other(value: int), but the constructor's parameter is `code` "
    "(src/errors/types.rs), so the keyword the stub promises is rejected.",
)
def test_operation_type_other_accepts_the_keyword_of_its_stub():
    assert OperationType.Other(value=7).code == 7
