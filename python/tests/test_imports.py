"""The public module layout: every name is where the docs say it is."""

import importlib

import pytest

PUBLIC_MODULES = [
    "scylla",
    "scylla.auth",
    "scylla.cluster",
    "scylla.errors",
    "scylla.future",
    "scylla.policies.address_translator",
    "scylla.policies.host_filter",
    "scylla.policies.load_balancing",
    "scylla.policies.retry",
    "scylla.policies.speculative_execution",
    "scylla.policies.timestamp_generator",
    "scylla.results",
    "scylla.routing",
    "scylla.session",
    "scylla.statement",
    "scylla.tls",
    "scylla.cql_types",
]


@pytest.mark.parametrize("name", PUBLIC_MODULES)
def test_public_module_exports_everything_in_all(name: str) -> None:
    module = importlib.import_module(name)
    for attr in module.__all__:
        assert hasattr(module, attr), f"{name}.__all__ lists {attr} but it is missing"


def test_root_has_the_everyday_names() -> None:
    from scylla import (
        UNSET,
        Batch,
        BatchType,
        Consistency,
        ExecutionProfile,
        PreparedStatement,
        RequestResult,
        ScyllaError,
        SerialConsistency,
        Session,
        SessionBuilder,
        Statement,
    )

    assert Session is importlib.import_module("scylla.session").Session
    assert SessionBuilder is importlib.import_module("scylla.session").SessionBuilder
    assert ExecutionProfile is importlib.import_module("scylla.session").ExecutionProfile
    assert Statement is importlib.import_module("scylla.statement").Statement
    assert PreparedStatement is importlib.import_module("scylla.statement").PreparedStatement
    assert Batch is importlib.import_module("scylla.statement").Batch
    assert BatchType is importlib.import_module("scylla.statement").BatchType
    assert Consistency is importlib.import_module("scylla.statement").Consistency
    assert SerialConsistency is importlib.import_module("scylla.statement").SerialConsistency
    assert UNSET is importlib.import_module("scylla.statement").UNSET
    assert RequestResult is importlib.import_module("scylla.results").RequestResult
    assert ScyllaError is importlib.import_module("scylla.errors").ScyllaError


def test_classes_report_their_public_module() -> None:
    from scylla.cluster import Keyspace
    from scylla.cql_types import CqlInt
    from scylla.errors import OperationTimedOut
    from scylla.session import PoolSize, SessionBuilder
    from scylla.statement import Consistency

    assert SessionBuilder.__module__ == "scylla.session"
    assert PoolSize.__module__ == "scylla.session"
    assert Consistency.__module__ == "scylla.statement"
    assert CqlInt.__module__ == "scylla.cql_types"
    assert Keyspace.__module__ == "scylla.cluster"
    assert OperationTimedOut.__module__ == "scylla.errors"
