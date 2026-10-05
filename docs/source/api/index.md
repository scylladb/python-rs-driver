# API Reference

## Package layout

The names most programs use come straight from `scylla`:

```python
from scylla import Consistency, ExecutionProfile, Session, SessionBuilder, Statement
```

The root also exports `Batch`, `BatchType`, `PreparedStatement`, `RequestResult`, `ScyllaError`, `SerialConsistency` and `UNSET`. Everything else lives in one of the modules below, listed in the order you meet them when writing an application.

| Module | What is in it |
|---|---|
| [`scylla.session`](reference/scylla/session/index) | Connecting. `SessionBuilder` and its options (`PoolSize`, `Compression`, `WriteCoalescingDelay`, `SelfIdentity`), the `Session` it produces, and `ExecutionProfile`. |
| [`scylla.statement`](reference/scylla/statement/index) | What you send. `Statement`, `PreparedStatement`, `Batch` and `BatchType`, the `Consistency` and `SerialConsistency` levels, and `UNSET`, which options such as `serial_consistency` report when they are left to the execution profile. |
| [`scylla.results`](reference/scylla/results/index) | What comes back. `RequestResult`, `PagingState`, the row iterators, `Column`, `ColumnSpec` and `RowFactory`. |
| [`scylla.cql_types`](reference/scylla/cql_types/index) | The CQL type system. The `Cql*` type descriptors reported by the schema and by prepared statements, the `CqlValue` aliases describing which Python objects the driver reads and writes, and the read-only `CqlEmpty`, which writing does not accept. |
| [`scylla.cluster`](reference/scylla/cluster/index) | What the driver knows about the cluster. `ClusterState`, `Node`, and the schema metadata: `Keyspace`, `Table`, `MaterializedView`, `Column`, `Strategy`. |
| [`scylla.policies`](reference/scylla/policies/index) | Pluggable driver behaviour: which nodes a request goes to and in what order (load balancing, host filters), what happens when a request fails or is slow (retry, speculative execution), how node addresses are translated, and where client-side timestamps come from. Each has its own submodule, such as `scylla.policies.retry`, to import from; `scylla.policies` itself exports nothing. |
| [`scylla.auth`](reference/scylla/auth/index) | `Authenticator` and `AuthenticatorProvider` for custom authentication. Plain username and password needs nothing from here, see `SessionBuilder.user()`. |
| [`scylla.tls`](reference/scylla/tls/index) | Encrypting connections to the cluster. `TlsContext` holds the certificates and the `VerifyMode`, and is passed to `SessionBuilder.tls_context()`; `TlsConfig` is the snapshot the builder keeps. |
| [`scylla.errors`](reference/scylla/errors/index) | Every exception the driver raises. |
| [`scylla.routing`](reference/scylla/routing/index) | `Token`, `ReplicaLocator`, `Shard` and `Target`, for code that cares which node a request goes to. |
| [`scylla.future`](reference/scylla/future/index) | `DriverFuture`, the awaitable returned by the driver. It also offers callbacks and a blocking `result()` for code that is not async. |
| [`scylla.legacy`](reference/scylla/legacy/index) | The `cassandra-driver` compatible API, for code written against it. `LegacySession`, created with `SessionBuilder.connect_legacy()`, blocks in `execute()` and `prepare()`; its `execute_async()` returns a `ResponseFuture`, which delivers a `ResultSet` through `result()` or callbacks. `QueryExhausted` is the one from `scylla.errors`. |

```{toctree}
:hidden:

reference/scylla/session/index
reference/scylla/statement/index
reference/scylla/results/index
reference/scylla/cql_types/index
reference/scylla/cluster/index
reference/scylla/policies/index
reference/scylla/auth/index
reference/scylla/tls/index
reference/scylla/errors/index
reference/scylla/routing/index
reference/scylla/future/index
reference/scylla/legacy/index
```
