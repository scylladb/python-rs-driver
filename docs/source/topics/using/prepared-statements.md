# Prepared Statements

Prepared statements are parsed by ScyllaDB once and cached. Every subsequent execution only sends the bound parameter values (and prepared statement id, which is a small hash instead of the full statement string to yet be parsed), reducing network traffic and CPU usage. Once created, prepared statements should be reused with different bind variables. Prepared statements use `?` marker to denote bind variables in the literal statement.

You can prepare a `Statement` or a literal `str`. One reason for using `Statement` instead of literal `str` is to create a new instance with desired configuration options or [`ExecutionProfile`](execution-profiles.md) using `with_*` methods before `await`ing using `session.prepare()`:

```python
from scylla.enums import Consistency
from scylla.execution_profile import ExecutionProfile
from scylla.statement import Statement
from scylla.types import Unset

# NOTE: This snippet is intended to run inside an `async def` where `session` is a connected Session.

insert_prepared_statement = await session.prepare(
    "INSERT INTO users (id, name, age) VALUES (?, ?, ?)"
)

# Or you can use a `Statement` with configuration options or attach an `ExecutionProfile`

configured_statement = Statement("INSERT INTO users (id, name, age) VALUES (?, ?, ?)").with_request_timeout(10.0)
```


Worth noting that you can set both `with_*` options and an `ExecutionProfile`, but driver still adheres to hierarchy described in [Execution Profiles](execution-profiles.md)


```python
profile = ExecutionProfile(timeout=10.0, consistency=Consistency.LocalQuorum)
configured_statement = Statement("INSERT INTO users (id, name, age) VALUES (?, ?, ?)").with_execution_profile(profile)

configured_prepared = await session.prepare(configured_statement)

# All `with_*` methods use immutable objects, so they do not modify the original statement.

statement = Statement("INSERT INTO users (id, name, age) VALUES (?, ?, ?)")
new_statement = statement.with_execution_profile(profile)
assert statement.execution_profile is None
assert new_statement.execution_profile is not None

# The same `with_*` methods can be used with already prepared statements. You don't need to call `prepare` again.

statement = Statement("INSERT INTO users (id, name, age) VALUES (?, ?, ?)")
prepared = await session.prepare(statement)
new_statement = prepared.with_request_timeout(10.0).with_consistency(Consistency.LocalQuorum)
assert statement.request_timeout is Unset
assert new_statement.request_timeout == 10.0
assert statement.consistency is None
assert new_statement.consistency is Consistency.LocalQuorum

# You can execute the prepared statement with bind variables.
# The variables can be passed as a `Sequence` of variables or a `Mapping` of column name to variable.
# Passing a `tuple` should be preferred as `tuple` serialization has the least overhead.
 
statement = Statement("INSERT INTO users (id, name, age) VALUES (?, ?, ?)")
prepared = await session.prepare(statement)

result_for_prepared_executed_with_tuple = await session.execute(prepared, (1, "Alice", 30))
result_for_prepared_executed_with_list = await session.execute(prepared, [1, "Alice", 30])
result_for_prepared_executed_with_dict = await session.execute(prepared, {"id": 1, "name": "Alice", "age": 30})
```
