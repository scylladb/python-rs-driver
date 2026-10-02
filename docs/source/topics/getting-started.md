# Getting Started

First, make sure the driver has been properly installed. See [Installation](installation.md).

> **Note:** This driver is **async-first** and requires Python's `asyncio`. All network operations - connecting, executing statements, fetching results - are coroutines, which must be awaited inside an `async` function.

## Connecting to a Cluster

Before executing any statements you need to establish a connection to a ScyllaDB cluster. This is done using `SessionBuilder`, which provides a chainable API for configuring the connection before calling `connect()`.

### Basic connection

The simplest way to create a `Session` is to provide a single contact point. The driver will automatically discover the rest of the cluster:

```python
import asyncio
from scylla.session_builder import SessionBuilder


async def main():
    builder = SessionBuilder().contact_points("127.0.0.2")
    session = await builder.connect()


asyncio.run(main())
```

### Multiple contact points

You can pass a list of initial contact points. The driver needs only one node to discover the rest of the cluster automatically:

```python
builder = SessionBuilder().contact_points(["127.0.0.2", "127.0.0.3"])
session = await builder.connect()
```

Each contact point can be provided as:
- a `str` - e.g. `"127.0.0.2"` or `"127.0.0.2:9042"`
- a `tuple[str, int]` - e.g. `("127.0.0.2", 9042)`
- a `tuple[IPv4Address | IPv6Address, int]`

### Specifying a non-standard port

```python
builder = SessionBuilder().contact_points([("127.0.0.2", 9142), ("127.0.0.3", 9142)])
session = await builder.connect()
```

### Authentication

Use `.user()` for plain-text authentication:

```python
builder = SessionBuilder().contact_points("127.0.0.2").user("username", "password")
session = await builder.connect()
```

## Working with Keyspaces

To set a default keyspace for the session a `use_keyspace` method is available:

```python
await session.execute(
    "CREATE KEYSPACE IF NOT EXISTS my_ks "
    "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
)
await session.use_keyspace("my_ks")
```

All subsequent requests will use `my_ks` as the default keyspace for unqualified table names.

## Executing Statements

Use `session.execute()` to run a CQL statement. It is a coroutine - always `await` it:

```python
await session.execute("CREATE TABLE IF NOT EXISTS users (id int PRIMARY KEY, name text, age int)")
```

### Passing parameters

Use `?` as the placeholder for bound parameters. Supply parameter values as a list or tuple:

```python
await session.execute("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", (1, "Alice", 30))
```

### Reading rows

Rows are returned in pages as namedtuples by default. Iterate over all rows in all pages with transparent async paging using `async for`:

```python
result = await session.execute("SELECT id, name, age FROM users")
async for row in result:
    print(row.id, row.name, row.age)
```

### Concurrent execution

Because the driver is async, you can execute many statements concurrently using `asyncio.gather`. This is the recommended approach for high-throughput workloads:

```python
coroutines = [
    session.execute("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", [i, f"user_{i}", i % 100])
    for i in range(1000)
]
await asyncio.gather(*coroutines)
```

> Awaiting each `execute()` call sequentially in a loop is suboptimal - always prefer `asyncio.gather` or any other way of non-blocking execution when inserting or querying large amounts of data.

## Prepared Statements

Prepared statements are parsed by ScyllaDB once and cached. Every subsequent execution only sends the bound parameter values (and prepared statement id, which is a small hash instead of the full statement string to yet be parsed), reducing network traffic and CPU usage.

Prepare a statement with `session.prepare()`:

```python
insert_statement = await session.prepare("INSERT INTO users (id, name, age) VALUES (?, ?, ?)")
```

Execute it as many times as needed:

```python
await session.execute(insert_statement, [2, "Bob", 25])
await session.execute(insert_statement, [3, "Carol", 35])
```

Combine preparation with concurrent execution:

```python
insert_statement = await session.prepare("INSERT INTO users (id, name, age) VALUES (?, ?, ?)")

coroutines = [session.execute(insert_statement, [i, f"user_{i}", i % 100]) for i in range(1000)]
await asyncio.gather(*coroutines)
```

## Statement Configuration

Both `Statement` (for unprepared statements) and `PreparedStatement` use an **immutable builder pattern** - every `with_*` method returns a new object with the updated configuration, leaving the original unchanged.

### Consistency level

```python
from scylla.enums import Consistency
from scylla.statement import Statement

statement = Statement("INSERT INTO users (id, name) VALUES (?, ?)").with_consistency(Consistency.Quorum)
await session.execute(statement, [4, "Dave"])
```

To set consistency on a prepared statement:

```python
prepared = await session.prepare("INSERT INTO users (id, name) VALUES (?, ?)")
prepared = prepared.with_consistency(Consistency.Quorum)
```

### Request timeout

Set a per-statement timeout in seconds. If the timeout elapses the execution fails immediately. This is true for all retry attempts for this request:

```python
prepared = prepared.with_request_timeout(5.0)  # 5-second timeout
```

### Page size

Control how many rows are fetched per page (default: 5000):

```python
statement = Statement("SELECT * FROM users").with_page_size(50)
```

### Configuration preservation

All configuration options are preserved when preparing a `Statement`:
 
```python
statement = Statement("SELECT * FROM users").with_page_size(10).with_request_timeout(2.0)
prepared = await session.prepare(statement)
assert prepared.page_size == 10
assert prepared.request_timeout == 2.0
```

## Execution Profiles

An `ExecutionProfile` bundles default timeout and consistency settings. Attach it to a `SessionBuilder` to make it the session-wide default, or attach it to individual statements to override per-request:

```python
from scylla.execution_profile import ExecutionProfile
from scylla.enums import Consistency, SerialConsistency

profile = ExecutionProfile(
    timeout=10.0,
    consistency=Consistency.LocalQuorum,
    serial_consistency=SerialConsistency.LocalSerial,
)
```

**Session-wide default:**

```python
builder = SessionBuilder().contact_points("127.0.0.2").execution_profile(profile)
session = await builder.connect()
```

**Per-statement override:**

```python
insert_statement = await session.prepare("INSERT INTO users (id, name) VALUES (?, ?)")
insert_statement = insert_statement.with_execution_profile(profile)
await session.execute(insert_statement, [5, "Eve"])
```

Statement-level settings (e.g. `with_consistency`) take precedence over the execution profile, which in turn takes precedence over the session default.

## Working with Results

`session.execute()` returns a `RequestResult`.

### Async iteration

Iterate over all rows, transparently following pages:

```python
result = await session.execute("SELECT id, name, age FROM users")
async for row in result:
    print(row.id, row.name, row.age)
```

### Convenience helpers

**`first_row()`** - fetch only the first row without materialising the full result:

```python
result = await session.execute("SELECT id, name FROM users WHERE id = ?", [1])
row = await result.first_row()
if row is not None:
    print(row.name)
```

**`all()`** - materialise all pages into a Python list (use with care for large result sets):

```python
result = await session.execute("SELECT id, name FROM users")
rows = await result.all()
print(f"Total rows: {len(rows)}")
```

### Manual paging

Use `iter_current_page()` and `fetch_next_page()` to consume one page at a time:

```python
from scylla.statement import Statement

statement = Statement("SELECT id, name FROM users").with_page_size(100)
page_result = await session.execute(statement)

while page_result:
    for row in page_result.iter_current_page():
        print(row)
    page_result = await page_result.fetch_next_page()
```

### Resuming from a paging state

`PagingState` lets you persist and resume pagination across separate requests:

```python
from scylla.statement import Statement

statement = Statement("SELECT id, name FROM users").with_page_size(10)

# --- First request ---
result = await session.execute(statement)
page = list(result.iter_current_page())
state = result.paging_state()

# --- Later request ---
if state is not None:
    result = await session.execute(statement, paging_state=state)
    next_page = list(result.iter_current_page())
```

## Batch Statements

Batch multiple writes into a single CQL `BATCH`. Mixing prepared and unprepared statements is supported. Prepared statements are recommended for performance.

```python
from scylla.batch import Batch, BatchType
from scylla.enums import Consistency

insert = await session.prepare("INSERT INTO users (id, name, age) VALUES (?, ?, ?)")

batch = Batch()
batch.add(insert, [10, "Frank", 40])
batch.add(insert, [11, "Grace", 28])
batch.add(insert, [12, "Heidi", 33])

# Optionally configure consistency on the batch itself.
batch = batch.with_consistency(Consistency.LocalQuorum)

# Or add an ExecutionProfile.
batch = batch.with_execution_profile(ExecutionProfile(timeout=120.0))

await session.batch(batch)
```

## Configuring Logging

The driver uses Python's standard `logging` module. Driver log messages are emitted under the `scylla` logger hierarchy.

```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
```

Standard log levels apply: `DEBUG` (10), `INFO` (20), `WARNING` (30), `ERROR` (40), `CRITICAL` (50).

For very verbose tracing - more detailed than `DEBUG` - set the level to `5`:

```python
logging.basicConfig(level=5, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
```

## Putting It All Together

A complete, minimal example demonstrating the concepts above:

```python
import asyncio
import logging
from scylla.session_builder import SessionBuilder
from scylla.enums import Consistency
from scylla.execution_profile import ExecutionProfile

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


async def main():
    # Build a session with a custom execution profile.
    profile = ExecutionProfile(timeout=10.0, consistency=Consistency.LocalQuorum)
    builder = SessionBuilder().contact_points("127.0.0.2").execution_profile(profile)
    session = await builder.connect()

    # Set up schema.
    await session.execute(
        "CREATE KEYSPACE IF NOT EXISTS demo "
        "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
    )
    await session.execute("USE demo")
    await session.execute("CREATE TABLE IF NOT EXISTS users (id int PRIMARY KEY, name text, age int)")

    # Prepare and execute concurrently.
    insert = await session.prepare("INSERT INTO users (id, name, age) VALUES (?, ?, ?)")
    await asyncio.gather(*[session.execute(insert, [i, f"user_{i}", 20 + i % 50]) for i in range(20)])

    # Read back all rows.
    result = await session.execute("SELECT id, name, age FROM users")
    async for row in result:
        print(f"id={row.id}  name={row.name}  age={row.age}")


asyncio.run(main())
```
