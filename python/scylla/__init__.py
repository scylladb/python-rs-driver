"""
Python driver for ScyllaDB.

The names most programs need are importable straight from ``scylla``. The
rest is grouped into a handful of modules, roughly in the order you meet
them:

- ``scylla.session`` - connecting: ``SessionBuilder``, ``Session``,
  ``ExecutionProfile`` and the connection-level options.
- ``scylla.statement`` - what you send: ``Statement``, ``PreparedStatement``,
  ``Batch``, consistency levels and ``UNSET``.
- ``scylla.results`` - what comes back: ``RequestResult``, paging and row
  factories.
- ``scylla.cql_types`` - the CQL type system: type descriptors, the ``CqlValue``
  aliases and ``CqlEmpty``.
- ``scylla.cluster`` - what the driver knows about the cluster: nodes,
  keyspaces, tables.
- ``scylla.policies`` - load balancing, retries, speculative execution, host
  filters, address translation, timestamps.
- ``scylla.auth`` and ``scylla.tls`` - authentication and encryption.
- ``scylla.errors`` - every exception the driver raises.
- ``scylla.routing`` and ``scylla.future`` - tokens, replica lookup and the
  awaitable returned by the driver.
- ``scylla.legacy`` - the ``cassandra-driver`` compatible API: ``LegacySession``,
  ``ResponseFuture`` and ``ResultSet``.
"""

from .errors import ScyllaError
from .results import RequestResult
from .session import ExecutionProfile, Session, SessionBuilder
from .statement import UNSET, Batch, BatchType, Consistency, PreparedStatement, SerialConsistency, Statement

__all__ = [
    "UNSET",
    "Batch",
    "BatchType",
    "Consistency",
    "ExecutionProfile",
    "PreparedStatement",
    "RequestResult",
    "ScyllaError",
    "SerialConsistency",
    "Session",
    "SessionBuilder",
    "Statement",
]
