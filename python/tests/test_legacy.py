"""Tests of the legacy (`cassandra-driver` compatible) API: LegacySession, ResponseFuture, ResultSet."""

import threading
import uuid
import weakref
from collections.abc import Callable, Generator
from typing import Any

import pytest
from helpers.ddl import SchemaQueriesLBP, SchemaQueriesRetryPolicy
from scylla.batch import Batch
from scylla.cluster.metadata import ColumnSpec, CqlColumnType
from scylla.enums import Consistency
from scylla.errors import ExecuteError, QueryExhausted, ScyllaError
from scylla.execution_profile import ExecutionProfile
from scylla.legacy import LegacySession, ResponseFuture, ResultSet
from scylla.results import ColumnIterator, PagingState, RowFactory
from scylla.routing import Target
from scylla.session_builder import SessionBuilder
from scylla.statement import PreparedStatement, Statement
from scylla.types import Unset

CONTACT_POINTS = [("127.0.0.2", 9042)]
KEYSPACE = "legacy_testks"
KEYSPACE_WITHOUT_TABLETS = "legacy_testks_without_tablets"
PAGED_TABLE = "paged_rows"
PAGED_ROWS = 25
PAGE_SIZE = 10

CALLBACK_TIMEOUT = 10.0

TableFactory = Callable[[str, str], str]


Call = tuple[tuple[Any, ...], dict[str, Any]]


class Recorder:
    """Records every call as `(args, kwargs)`; `wait(n)` blocks until `n` calls arrived."""

    def __init__(self) -> None:
        self._arrived = threading.Condition()
        self.calls: list[Call] = []

    def __call__(self, *args: Any, **kwargs: Any) -> None:
        with self._arrived:
            self.calls.append((args, kwargs))
            self._arrived.notify_all()

    def wait(self, count: int = 1, timeout: float = CALLBACK_TIMEOUT) -> list[Call]:
        with self._arrived:
            arrived = self._arrived.wait_for(lambda: len(self.calls) >= count, timeout)
            assert arrived, f"expected {count} calls, got {len(self.calls)}"
            return list(self.calls)

    def first_args(self) -> tuple[Any, ...]:
        return self.wait()[0][0]


def ddl(session: LegacySession, query: str) -> None:
    """Execute a DDL statement, guarded against group 0 conflicts like `helpers.ddl.ddl`."""
    statement = (
        Statement(query).with_load_balancing_policy(SchemaQueriesLBP()).with_retry_policy(SchemaQueriesRetryPolicy())
    )
    session.execute(statement)


def connect(keyspace: str, *, tablets: bool) -> LegacySession:
    session = SessionBuilder().contact_points(CONTACT_POINTS).connect_legacy()
    tablets_clause = "" if tablets else " AND tablets = {'enabled': false}"
    ddl(
        session,
        f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}{tablets_clause};
        """,
    )
    session.set_keyspace(keyspace)
    return session


@pytest.fixture(scope="module")
def session() -> Generator[LegacySession, None, None]:
    session = connect(KEYSPACE, tablets=True)
    yield session
    ddl(session, f"DROP KEYSPACE {KEYSPACE}")


@pytest.fixture(scope="module")
def session_without_tablets() -> Generator[LegacySession, None, None]:
    session = connect(KEYSPACE_WITHOUT_TABLETS, tablets=False)
    yield session
    ddl(session, f"DROP KEYSPACE {KEYSPACE_WITHOUT_TABLETS}")


@pytest.fixture
def table_factory(session: LegacySession) -> Generator[TableFactory, None, None]:
    created: list[str] = []

    def create(schema: str, name: str) -> str:
        ddl(session, f"CREATE TABLE IF NOT EXISTS {name} ({schema});")
        created.append(name)
        return name

    yield create

    for table in created:
        ddl(session, f"DROP TABLE IF EXISTS {table};")


@pytest.fixture(scope="module")
def paged_table(session: LegacySession) -> str:
    """A read-only table of `PAGED_ROWS` rows `(id, x)`, dropped with the keyspace."""
    ddl(session, f"CREATE TABLE IF NOT EXISTS {PAGED_TABLE} (id int PRIMARY KEY, x int);")
    insert = session.prepare(f"INSERT INTO {PAGED_TABLE} (id, x) VALUES (?, ?)")
    for i in range(PAGED_ROWS):
        session.execute(insert, (i, i * 10))
    return PAGED_TABLE


def paged_select(table: str) -> Statement:
    return Statement(f"SELECT id, x FROM {table}").with_page_size(PAGE_SIZE)


def ids(rows: Any) -> list[int]:
    return sorted(row["id"] for row in rows)


# ── LegacySession ─────────────────────────────────────────────────────────────


@pytest.mark.requires_db
def test_connect_legacy_returns_legacy_session(session: LegacySession) -> None:
    assert isinstance(session, LegacySession)
    assert session.keyspace == KEYSPACE
    assert not session.is_shutdown


@pytest.mark.requires_db
def test_execute_returns_result_set_of_dict_rows(session: LegacySession) -> None:
    rows = session.execute("SELECT release_version FROM system.local")
    assert isinstance(rows, ResultSet)
    row = rows.one()
    assert isinstance(row, dict)
    assert "release_version" in row


@pytest.mark.requires_db
def test_execute_binds_parameters(session: LegacySession, table_factory: TableFactory) -> None:
    table = table_factory("id int PRIMARY KEY, name text", "bind_params")
    session.execute(f"INSERT INTO {table} (id, name) VALUES (?, ?)", (1, "Alice"))
    session.execute(f"INSERT INTO {table} (id, name) VALUES (?, ?)", [2, "Bob"])

    rows = session.execute(f"SELECT name FROM {table} WHERE id = ?", (2,))
    assert rows.one() == {"name": "Bob"}


@pytest.mark.requires_db
def test_prepare_returns_prepared_statement(session: LegacySession, table_factory: TableFactory) -> None:
    table = table_factory("id int PRIMARY KEY, name text", "prepared_rows")
    prepared = session.prepare(f"INSERT INTO {table} (id, name) VALUES (?, ?)")
    assert isinstance(prepared, PreparedStatement)

    session.execute(prepared, (7, "Grace"))
    assert session.execute(f"SELECT id, name FROM {table}").all() == [{"id": 7, "name": "Grace"}]


@pytest.mark.requires_db
def test_prepare_accepts_statement(session: LegacySession) -> None:
    prepared = session.prepare(Statement("SELECT release_version FROM system.local"))
    assert isinstance(prepared, PreparedStatement)


@pytest.mark.requires_db
def test_non_row_result_is_empty(session: LegacySession, table_factory: TableFactory) -> None:
    table = table_factory("id int PRIMARY KEY", "void_result")
    rows = session.execute(f"INSERT INTO {table} (id) VALUES (1)")

    assert rows.one() is None
    assert rows.all() == []
    assert rows.current_rows == []
    assert not rows
    assert not rows.has_more_pages
    assert rows.column_names is None
    assert rows.paging_state is None


@pytest.mark.requires_db
def test_execute_raises_on_failure(session: LegacySession) -> None:
    with pytest.raises(ExecuteError):
        session.execute("SELECT * FROM no_such_table")


@pytest.mark.requires_db
def test_execute_batch(session: LegacySession, table_factory: TableFactory) -> None:
    table = table_factory("id int PRIMARY KEY, name text", "batched_rows")
    batch = Batch()
    batch.add_all(
        [
            (f"INSERT INTO {table} (id, name) VALUES (?, ?)", (1, "Alice")),
            (f"INSERT INTO {table} (id, name) VALUES (?, ?)", (2, "Bob")),
        ]
    )

    rows = session.execute(batch)
    assert rows.all() == []
    assert ids(session.execute(f"SELECT id FROM {table}")) == [1, 2]


@pytest.mark.requires_db
def test_batch_rejects_parameters_and_paging_state(session: LegacySession) -> None:
    with pytest.raises(TypeError):
        session.execute(Batch(), (1,))
    with pytest.raises(TypeError):
        session.execute(Batch(), paging_state=PagingState())


@pytest.mark.requires_db
@pytest.mark.parametrize(
    "kwargs",
    [{"custom_payload": {"k": b"v"}}, {"execute_as": "someone"}],
    ids=["custom_payload", "execute_as"],
)
def test_unsupported_execute_arguments_raise(session: LegacySession, kwargs: dict[str, Any]) -> None:
    with pytest.raises(NotImplementedError):
        session.execute("SELECT release_version FROM system.local", **kwargs)
    with pytest.raises(NotImplementedError):
        session.execute_async("SELECT release_version FROM system.local", **kwargs)


@pytest.mark.requires_db
def test_unsupported_prepare_arguments_raise(session: LegacySession) -> None:
    with pytest.raises(NotImplementedError):
        session.prepare("SELECT release_version FROM system.local", keyspace="system")  # pyright: ignore[reportArgumentType]
    with pytest.raises(NotImplementedError):
        session.prepare("SELECT release_version FROM system.local", custom_payload={})  # pyright: ignore[reportArgumentType]


@pytest.mark.requires_db
def test_timeout_argument_is_reported_by_future(session: LegacySession) -> None:
    query = "SELECT release_version FROM system.local"

    assert session.default_timeout == 30.0
    assert session.execute_async(query).timeout == session.default_timeout
    assert session.execute_async(query, timeout=Unset).timeout == session.default_timeout
    assert session.execute_async(query, timeout=None).timeout is None
    assert session.execute_async(query, timeout=2.5).timeout == 2.5

    # The driver's precedence: the statement's own, else its profile's.
    profile = ExecutionProfile(timeout=5.0)
    assert session.execute_async(query, execution_profile=profile).timeout == 5.0
    assert session.execute_async(Statement(query).with_request_timeout(1.5), execution_profile=profile).timeout == 1.5
    assert session.execute_async(Statement(query).with_request_timeout(None)).timeout is None


@pytest.mark.requires_db
def test_default_timeout_attribute(session: LegacySession) -> None:
    """A session default sits between the statement's own timeout and the profile's."""
    query = "SELECT release_version FROM system.local"
    profile_timeout = session.default_timeout

    session.default_timeout = 7.0
    try:
        assert session.default_timeout == 7.0
        assert session.execute_async(query).timeout == 7.0
        assert session.execute_async(query, timeout=2.5).timeout == 2.5
        assert session.execute_async(Statement(query).with_request_timeout(1.5)).timeout == 1.5

        session.default_timeout = None
        assert session.default_timeout is None
        assert session.execute_async(query).timeout is None
    finally:
        session.default_timeout = Unset

    assert session.default_timeout == profile_timeout


@pytest.mark.requires_db
def test_execution_profile_argument_applies(session: LegacySession, paged_table: str) -> None:
    # The keyspace has replication factor 1, so this consistency cannot be met.
    profile = ExecutionProfile(consistency=Consistency.Three)
    query = f"SELECT id FROM {paged_table} WHERE id = 1"

    assert session.execute(query).one() == {"id": 1}
    with pytest.raises(ExecuteError):
        session.execute(query, execution_profile=profile)


@pytest.mark.requires_db
def test_trace_argument_yields_tracing_id(session: LegacySession, paged_table: str) -> None:
    query = "SELECT release_version FROM system.local"

    traced = session.execute_async(query, trace=True)
    traced.result()
    trace_ids = traced.get_query_trace_ids()
    assert len(trace_ids) == 1
    assert isinstance(trace_ids[0], uuid.UUID)

    untraced = session.execute_async(query)
    untraced.result()
    assert untraced.get_query_trace_ids() == []

    # One id per page, accumulated as the pages arrive.
    paged = session.execute_async(paged_select(paged_table), trace=True)
    paged.result().all()
    assert len(paged.get_query_trace_ids()) == PAGED_ROWS // PAGE_SIZE + 1


@pytest.mark.requires_db
def test_host_argument_pins_the_request(session: LegacySession) -> None:
    nodes = list(session.cluster_state.nodes_info.values())
    assert len(nodes) > 1, "pinning needs more than one node to be observable"

    for node in nodes:
        row = session.execute("SELECT host_id FROM system.local", host=node).one()
        assert row is not None and row["host_id"] == node.host_id

        row = session.execute("SELECT host_id FROM system.local", host=Target(node.host_id, 0)).one()
        assert row is not None and row["host_id"] == node.host_id


@pytest.mark.requires_db
def test_paging_state_argument_resumes(session: LegacySession, paged_table: str) -> None:
    statement = paged_select(paged_table)

    first = session.execute(statement)
    assert first.has_more_pages
    assert isinstance(first.paging_state, PagingState)

    second = session.execute(statement, paging_state=first.paging_state)
    raw = first.paging_state.as_bytes()
    assert raw is not None
    second_from_bytes = session.execute(statement, paging_state=raw)

    assert ids(second.current_rows) == ids(second_from_bytes.current_rows)
    assert not set(ids(first.current_rows)) & set(ids(second.current_rows))


@pytest.mark.requires_db
def test_default_fetch_size_applies_to_every_request(session: LegacySession, paged_table: str) -> None:
    query = f"SELECT id FROM {paged_table}"
    driver_default = session.default_fetch_size
    assert driver_default is not None and driver_default > PAGED_ROWS

    session.default_fetch_size = PAGE_SIZE
    try:
        assert session.default_fetch_size == PAGE_SIZE
        rows = session.execute(query)
        assert len(rows.current_rows) == PAGE_SIZE
        assert rows.has_more_pages
        assert len(rows.all()) == PAGED_ROWS

        # A statement's own page size wins over the session default.
        statement = Statement(query).with_page_size(PAGED_ROWS + 1)
        assert len(session.execute(statement).current_rows) == PAGED_ROWS
        assert len(session.execute(session.prepare(statement)).current_rows) == PAGED_ROWS

        # `None` disables paging, as in the legacy driver, unless the statement pages itself.
        session.default_fetch_size = None
        assert session.default_fetch_size is None
        rows = session.execute(query)
        assert len(rows.current_rows) == PAGED_ROWS
        assert not rows.has_more_pages
        assert rows.paging_state is None
        assert session.execute(paged_select(paged_table)).has_more_pages
    finally:
        session.default_fetch_size = driver_default

    rows = session.execute(query)
    assert len(rows.current_rows) == PAGED_ROWS
    assert not rows.has_more_pages


@pytest.mark.requires_db
def test_default_fetch_size_must_be_positive(session: LegacySession) -> None:
    before = session.default_fetch_size
    for invalid in (0, -1):
        with pytest.raises(ValueError):
            session.default_fetch_size = invalid
    assert session.default_fetch_size == before


@pytest.mark.requires_db
def test_row_factory_attribute_applies_to_every_request(session: LegacySession, paged_table: str) -> None:
    class TupleFactory(RowFactory):
        def build(self, column_iterator: ColumnIterator) -> Any:
            return tuple(column.value for column in column_iterator)

    query = f"SELECT id, x FROM {paged_table} WHERE id = 3"
    assert session.row_factory is None
    assert session.execute(query).one() == {"id": 3, "x": 30}

    factory = TupleFactory()
    session.row_factory = factory
    try:
        assert session.row_factory is factory
        assert session.execute(query).one() == (3, 30)

        future = session.execute_async(query)
        future.result()
        assert future.row_factory is factory
    finally:
        session.row_factory = None

    assert session.execute(query).one() == {"id": 3, "x": 30}


@pytest.mark.requires_db
def test_shutdown_refuses_later_requests() -> None:
    session = SessionBuilder().contact_points(CONTACT_POINTS).connect_legacy()
    assert not session.is_shutdown

    session.shutdown()
    assert session.is_shutdown

    with pytest.raises(ScyllaError):
        session.execute("SELECT release_version FROM system.local")
    with pytest.raises(ScyllaError):
        session.execute_async("SELECT release_version FROM system.local")
    with pytest.raises(ScyllaError):
        session.prepare("SELECT release_version FROM system.local")
    with pytest.raises(ScyllaError):
        session.set_keyspace("system")


# ── ResponseFuture ────────────────────────────────────────────────────────────


@pytest.mark.requires_db
def test_execute_async_returns_response_future(session: LegacySession) -> None:
    future = session.execute_async("SELECT release_version FROM system.local")
    assert isinstance(future, ResponseFuture)
    assert "ResponseFuture" in repr(future)

    rows = future.result()
    assert isinstance(rows, ResultSet)
    assert rows.one() is not None


@pytest.mark.requires_db
def test_result_returns_a_fresh_result_set_each_time(session: LegacySession, paged_table: str) -> None:
    future = session.execute_async(f"SELECT id FROM {paged_table} WHERE id = 1")
    first = future.result()
    second = future.result()

    assert first is not second
    assert first.all() == second.all() == [{"id": 1}]


@pytest.mark.requires_db
def test_result_raises_the_request_error(session: LegacySession) -> None:
    future = session.execute_async("SELECT * FROM no_such_table")
    with pytest.raises(ExecuteError):
        future.result()
    # Settled: raises again rather than hanging.
    with pytest.raises(ExecuteError):
        future.result()


@pytest.mark.requires_db
def test_result_from_another_thread(session: LegacySession) -> None:
    future = session.execute_async("SELECT release_version FROM system.local")
    outcome: list[ResultSet] = []

    worker = threading.Thread(target=lambda: outcome.append(future.result()))
    worker.start()
    worker.join(timeout=CALLBACK_TIMEOUT)

    assert not worker.is_alive()
    assert outcome and outcome[0].one() is not None


@pytest.mark.requires_db
def test_query_attribute_is_the_statement_passed(session: LegacySession) -> None:
    statement = Statement("SELECT release_version FROM system.local")
    future = session.execute_async(statement)
    assert future.query is statement

    future = session.execute_async("SELECT release_version FROM system.local")
    assert future.query == "SELECT release_version FROM system.local"


@pytest.mark.requires_db
def test_add_callback_receives_rows_and_extra_arguments(session: LegacySession, paged_table: str) -> None:
    recorder = Recorder()
    future = session.execute_async(f"SELECT id FROM {paged_table} WHERE id = 5")
    assert future.add_callback(recorder, "extra", flag=True) is future

    ((rows, extra), kwargs) = recorder.wait()[0]
    assert rows == [{"id": 5}]
    assert extra == "extra"
    assert kwargs == {"flag": True}


@pytest.mark.requires_db
def test_add_callback_receives_none_for_non_row_result(session: LegacySession, table_factory: TableFactory) -> None:
    table = table_factory("id int PRIMARY KEY", "callback_void")
    recorder = Recorder()
    session.execute_async(f"INSERT INTO {table} (id) VALUES (1)").add_callback(recorder)

    assert recorder.first_args() == (None,)


@pytest.mark.requires_db
def test_add_callback_on_settled_future_runs_immediately(session: LegacySession) -> None:
    future = session.execute_async("SELECT release_version FROM system.local")
    future.result()

    recorder = Recorder()
    future.add_callback(recorder)
    # Ran on the calling thread, before add_callback returned.
    assert len(recorder.calls) == 1


@pytest.mark.requires_db
def test_add_errback_receives_the_exception(session: LegacySession) -> None:
    recorder = Recorder()
    future = session.execute_async("SELECT * FROM no_such_table")
    assert future.add_errback(recorder, "extra") is future

    (exc, extra) = recorder.first_args()
    assert isinstance(exc, ExecuteError)
    assert extra == "extra"


@pytest.mark.requires_db
def test_add_errback_on_settled_future_runs_immediately(session: LegacySession) -> None:
    future = session.execute_async("SELECT * FROM no_such_table")
    with pytest.raises(ExecuteError):
        future.result()

    recorder = Recorder()
    future.add_errback(recorder)
    assert len(recorder.calls) == 1
    assert isinstance(recorder.calls[0][0][0], ExecuteError)


@pytest.mark.requires_db
def test_add_callbacks_routes_each_outcome(session: LegacySession) -> None:
    on_rows, on_error = Recorder(), Recorder()
    session.execute_async("SELECT release_version FROM system.local").add_callbacks(
        on_rows, on_error, callback_args=("ok",), errback_args=["failed"], callback_kwargs={"k": 1}
    )
    ((rows, label), kwargs) = on_rows.wait()[0]
    assert len(rows) == 1
    assert label == "ok"
    assert kwargs == {"k": 1}
    assert on_error.calls == []

    on_rows, on_error = Recorder(), Recorder()
    session.execute_async("SELECT * FROM no_such_table").add_callbacks(
        on_rows, on_error, callback_args=("ok",), errback_args=("failed",)
    )
    (exc, label) = on_error.first_args()
    assert isinstance(exc, ExecuteError)
    assert label == "failed"
    assert on_rows.calls == []


@pytest.mark.requires_db
@pytest.mark.parametrize(
    "make_args",
    [lambda: ("extra",), lambda: ["extra"], lambda: {"extra"}, lambda: (arg for arg in ["extra"])],
    ids=["tuple", "list", "set", "generator"],
)
def test_add_callbacks_accepts_any_iterable_args(session: LegacySession, make_args: Callable[[], Any]) -> None:
    """The args are unpacked like `*args`, so any iterable works — a set and a generator are not sequences."""
    on_rows, on_error = Recorder(), Recorder()
    session.execute_async("SELECT release_version FROM system.local").add_callbacks(
        on_rows, on_error, callback_args=make_args()
    )
    assert on_rows.first_args()[1] == "extra"

    on_rows, on_error = Recorder(), Recorder()
    session.execute_async("SELECT * FROM no_such_table").add_callbacks(on_rows, on_error, errback_args=make_args())
    assert on_error.first_args()[1] == "extra"


@pytest.mark.requires_db
@pytest.mark.parametrize(
    "kwargs",
    [{}, {"callback_args": None, "errback_args": None}],
    ids=["omitted", "none"],
)
def test_add_callbacks_args_default_to_nothing(session: LegacySession, kwargs: dict[str, Any]) -> None:
    on_rows, on_error = Recorder(), Recorder()
    session.execute_async("SELECT release_version FROM system.local").add_callbacks(on_rows, on_error, **kwargs)
    assert len(on_rows.first_args()) == 1


@pytest.mark.requires_db
def test_callbacks_fire_again_for_each_page(session: LegacySession, paged_table: str) -> None:
    pages = PAGED_ROWS // PAGE_SIZE + 1
    recorder = Recorder()

    future = session.execute_async(paged_select(paged_table))
    future.add_callback(recorder)

    for page in range(1, pages):
        recorder.wait(page)
        assert future.has_more_pages
        future.start_fetching_next_page()
    calls = recorder.wait(pages)

    seen = [row["id"] for (rows,), _ in calls for row in rows]
    assert sorted(seen) == list(range(PAGED_ROWS))
    assert not future.has_more_pages


@pytest.mark.requires_db
def test_callbacks_keep_their_page_when_result_drives_paging(session: LegacySession, paged_table: str) -> None:
    """Paging from the main thread must not skip or reorder the callbacks it races past."""
    pages = PAGED_ROWS // PAGE_SIZE + 1
    recorder = Recorder()
    paged_out = threading.Event()

    def held_back(rows: list[Any]) -> None:
        assert paged_out.wait(CALLBACK_TIMEOUT)
        recorder(rows)

    future = session.execute_async(paged_select(paged_table))
    future.add_callback(held_back)
    driven = [future.result().current_rows]
    for _ in range(1, pages):
        future.start_fetching_next_page()
        driven.append(future.result().current_rows)
    paged_out.set()

    calls = recorder.wait(pages)
    assert [rows for (rows,), _ in calls] == driven


@pytest.mark.requires_db
def test_start_fetching_next_page_raises_when_exhausted(session: LegacySession, paged_table: str) -> None:
    future = session.execute_async(f"SELECT id FROM {paged_table} WHERE id = 1")
    future.result()
    assert not future.has_more_pages
    with pytest.raises(QueryExhausted):
        future.start_fetching_next_page()
    # Control flow, as in the legacy driver: an `except ScyllaError` must not swallow it.
    assert not issubclass(QueryExhausted, ScyllaError)


@pytest.mark.requires_db
def test_callbacks_are_released_after_the_last_page(session: LegacySession, paged_table: str) -> None:
    """`future.add_callback(fn, future)` is a cycle no GC breaks; the future must let go itself."""

    class Sentinel:
        pass

    def on_page(rows: list[Any], future: ResponseFuture, sentinel: Sentinel, recorder: Recorder) -> None:
        recorder(len(rows))

    def watch(sentinel: Sentinel) -> threading.Event:
        """Set once `sentinel`, passed as a callback argument, has been freed."""
        released = threading.Event()
        weakref.finalize(sentinel, released.set)
        return released

    # Registered before the (only) page arrived: dropped once its callbacks ran.
    recorder = Recorder()
    sentinel = Sentinel()
    released = watch(sentinel)
    future = session.execute_async(f"SELECT id FROM {paged_table} WHERE id = 1")
    future.add_callback(on_page, future, sentinel, recorder)
    recorder.wait()
    del sentinel
    assert released.wait(CALLBACK_TIMEOUT)

    # Registered after the last page: fired right away and not kept.
    sentinel = Sentinel()
    released = watch(sentinel)
    future.add_callback(on_page, future, sentinel, recorder)
    assert len(recorder.calls) == 2
    del sentinel
    assert released.is_set()

    # With pages still to come, the callback is kept for them.
    recorder = Recorder()
    sentinel = Sentinel()
    released = watch(sentinel)
    future = session.execute_async(paged_select(paged_table))
    future.add_callback(on_page, future, sentinel, recorder)
    recorder.wait()
    del sentinel
    assert not released.is_set()
    while future.has_more_pages:
        future.start_fetching_next_page()
        future.result()
    recorder.wait(PAGED_ROWS // PAGE_SIZE + 1)
    assert released.wait(CALLBACK_TIMEOUT)


@pytest.mark.requires_db
def test_clear_callbacks_stops_further_pages(session: LegacySession, paged_table: str) -> None:
    recorder = Recorder()
    future = session.execute_async(paged_select(paged_table))
    future.add_callback(recorder)
    recorder.wait()

    future.clear_callbacks()
    future.start_fetching_next_page()
    future.result()

    assert len(recorder.calls) == 1


@pytest.mark.requires_db
def test_future_metadata(session: LegacySession, paged_table: str) -> None:
    future = session.execute_async(f"SELECT id, x FROM {paged_table} WHERE id = 1")
    future.result()

    assert future.column_names == ["id", "x"]
    columns = future.columns
    assert columns is not None
    assert [spec.name for spec in columns] == ["id", "x"]
    assert all(isinstance(spec, ColumnSpec) for spec in columns)

    assert future.warnings == []
    assert future.custom_payload is None
    assert future.is_schema_agreed is True
    assert not future.has_more_pages
    assert future.paging_state is None

    with pytest.raises(NotImplementedError):
        future.get_query_trace()
    with pytest.raises(NotImplementedError):
        future.get_all_query_traces()


# ── ResultSet ─────────────────────────────────────────────────────────────────


@pytest.mark.requires_db
def test_iteration_fetches_every_page(session: LegacySession, paged_table: str) -> None:
    rows = session.execute(paged_select(paged_table))
    assert len(rows.current_rows) == PAGE_SIZE

    seen = [row["id"] for row in rows]
    assert sorted(seen) == list(range(PAGED_ROWS))
    assert not rows.has_more_pages
    # Exhausted: the page buffer is released.
    assert rows.current_rows == []


@pytest.mark.requires_db
def test_all_and_one(session: LegacySession, paged_table: str) -> None:
    rows = session.execute(paged_select(paged_table))
    first = rows.one()
    assert first is not None and first in rows.current_rows

    assert ids(rows.all()) == list(range(PAGED_ROWS))


@pytest.mark.requires_db
def test_manual_paging(session: LegacySession, paged_table: str) -> None:
    rows = session.execute(paged_select(paged_table))
    seen: list[int] = []
    pages = 0
    while True:
        seen.extend(ids(rows.current_rows))
        pages += 1
        if not rows.has_more_pages:
            break
        rows.fetch_next_page()

    assert pages == PAGED_ROWS // PAGE_SIZE + 1
    assert sorted(seen) == list(range(PAGED_ROWS))

    rows.fetch_next_page()
    assert rows.current_rows == []


@pytest.mark.requires_db
def test_fetch_next_page_while_iterating_rewinds(session: LegacySession, paged_table: str) -> None:
    rows = session.execute(paged_select(paged_table))
    iterator = iter(rows)
    for _ in range(PAGE_SIZE // 2):
        next(iterator)

    rows.fetch_next_page()

    # Iteration continues from the first row of the page just fetched, rather
    # than from where it left off in the page before it.
    assert ids([next(iterator)]) == ids(rows.current_rows[:1])


@pytest.mark.requires_db
def test_equality_materializes_the_whole_result(session: LegacySession, paged_table: str) -> None:
    statement = paged_select(paged_table)
    expected = session.execute(statement).all()

    rows = session.execute(statement)
    assert rows == expected
    # Still usable after materializing.
    assert rows.all() == expected
    assert list(rows) == expected


@pytest.mark.requires_db
def test_indexing(session: LegacySession, paged_table: str) -> None:
    statement = paged_select(paged_table)
    expected = session.execute(statement).all()

    rows = session.execute(statement)
    with pytest.warns(DeprecationWarning):
        assert rows[0] == expected[0]
    assert rows[PAGE_SIZE + 1] == expected[PAGE_SIZE + 1]
    assert rows[1:3] == expected[1:3]
    assert rows[-1] == expected[-1]


@pytest.mark.requires_db
def test_list_operators_refused_after_iteration_started(session: LegacySession, paged_table: str) -> None:
    rows = session.execute(paged_select(paged_table))
    next(iter(rows))

    with pytest.raises(RuntimeError, match="iterated"):
        _ = rows == []
    with pytest.raises(RuntimeError, match="iterated"):
        _ = rows[3]


@pytest.mark.requires_db
def test_bool(session: LegacySession, paged_table: str) -> None:
    assert session.execute(f"SELECT id FROM {paged_table} WHERE id = 1")
    assert not session.execute(f"SELECT id FROM {paged_table} WHERE id = -1")


def test_not_constructible() -> None:
    """A ResultSet only ever comes from ResponseFuture.result()."""
    with pytest.raises(TypeError):
        ResultSet()


@pytest.mark.requires_db
def test_column_metadata(session: LegacySession, paged_table: str) -> None:
    rows = session.execute(f"SELECT id, x FROM {paged_table} WHERE id = 1")
    assert rows.column_names == ["id", "x"]
    types = rows.column_types
    assert types is not None
    assert len(types) == 2
    assert all(isinstance(t, CqlColumnType) for t in types)


@pytest.mark.requires_db
def test_paging_state_property(session: LegacySession, paged_table: str) -> None:
    rows = session.execute(paged_select(paged_table))
    assert isinstance(rows.paging_state, PagingState)

    rows.all()
    assert rows.paging_state is None


@pytest.mark.requires_db
def test_query_traces_not_supported(session: LegacySession) -> None:
    rows = session.execute("SELECT release_version FROM system.local")
    with pytest.raises(NotImplementedError):
        rows.get_query_trace()
    with pytest.raises(NotImplementedError):
        rows.get_all_query_traces()


@pytest.mark.requires_db
def test_was_applied_for_lwt_statement(session_without_tablets: LegacySession) -> None:
    table = "lwt_rows"
    ddl(session_without_tablets, f"CREATE TABLE IF NOT EXISTS {table} (id int PRIMARY KEY, name text);")
    try:
        insert = f"INSERT INTO {table} (id, name) VALUES (?, ?) IF NOT EXISTS"
        assert session_without_tablets.execute(insert, (1, "Alice")).was_applied is True
        assert session_without_tablets.execute(insert, (1, "Bob")).was_applied is False
    finally:
        ddl(session_without_tablets, f"DROP TABLE IF EXISTS {table};")


@pytest.mark.requires_db
def test_was_applied_for_lwt_batch(session_without_tablets: LegacySession) -> None:
    table = "lwt_batch_rows"
    ddl(session_without_tablets, f"CREATE TABLE IF NOT EXISTS {table} (id int, sub int, PRIMARY KEY (id, sub));")
    try:
        insert = f"INSERT INTO {table} (id, sub) VALUES (?, ?) IF NOT EXISTS"

        batch = Batch()
        batch.add_all([(insert, (1, 1)), (insert, (1, 2))])
        assert session_without_tablets.execute(batch).was_applied is True

        batch = Batch()
        batch.add_all([(insert, (1, 1)), (insert, (1, 3))])
        assert session_without_tablets.execute(batch).was_applied is False

        plain = Batch()
        plain.add(f"INSERT INTO {table} (id, sub) VALUES (?, ?)", (2, 1))
        with pytest.raises(RuntimeError, match="No LWT"):
            _ = session_without_tablets.execute(plain).was_applied
    finally:
        ddl(session_without_tablets, f"DROP TABLE IF EXISTS {table};")


@pytest.mark.requires_db
def test_was_applied_rejects_custom_row_factory(session_without_tablets: LegacySession) -> None:
    class TupleFactory(RowFactory):
        def build(self, column_iterator: ColumnIterator) -> Any:
            return tuple(column.value for column in column_iterator)

    table = "lwt_factory_rows"
    ddl(session_without_tablets, f"CREATE TABLE IF NOT EXISTS {table} (id int PRIMARY KEY, name text);")
    session_without_tablets.row_factory = TupleFactory()
    try:
        insert = f"INSERT INTO {table} (id, name) VALUES (?, ?) IF NOT EXISTS"
        result = session_without_tablets.execute(insert, (1, "Alice"))
        with pytest.raises(RuntimeError, match="Cannot determine LWT result with row factory"):
            _ = result.was_applied
    finally:
        session_without_tablets.row_factory = None
        ddl(session_without_tablets, f"DROP TABLE IF EXISTS {table};")


@pytest.mark.requires_db
def test_was_applied_requires_exactly_one_row(session: LegacySession, paged_table: str) -> None:
    rows = session.execute(f"SELECT id FROM {paged_table}")
    with pytest.raises(RuntimeError, match="exactly one row"):
        _ = rows.was_applied
