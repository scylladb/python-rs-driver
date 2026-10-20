# Statement values

Statement text is constant, while values may change between executions. Values can be passed separately from the statement text using bound parameters.

Values can be provided as:

- a list,
- a tuple,
- a mapping,
- or omitted when the statement does not need any values.

Bound parameters should be used instead of building queries by concatenating strings. This keeps the query easier to read and avoids problems such as invalid escaping or injection vulnerabilities as well as improves routing and performance of requests.

```python
await session.execute(
    "INSERT INTO users (id, name, age) VALUES (?, ?, ?)",
    (user_id, "Alice", 30),
)
```

## Passing values

Use a tuple or list when the statement contains positional bind markers, written as `?`.

Each `?` in the statement is filled with the matching value from the provided sequence, in order. The sequence may be a tuple:

```python
await session.execute(
    "INSERT INTO users (id, name, age) VALUES (?, ?, ?)",
    (user_id, "Alice", 30),
)
```

or a list:

```python
await session.execute(
    "INSERT INTO users (id, name, age) VALUES (?, ?, ?)",
    [user_id, "Alice", 30],
)
```

Values may also be passed as a mapping, such as a dictionary. When a mapping is used, dictionary keys are matched by name. The order of keys does not matter:

```python
await session.execute(
    "INSERT INTO users (id, name, age) VALUES (:user_id, :user_name, :user_age)",
    {
        "user_id": user_id,
        "user_name": "Alice",
        "user_age": 30,
    },
)

await session.execute(
    "INSERT INTO users (id, name, age) VALUES (?, ?, ?)",
    {
        "age": 30,
        "name": "Alice",
        "id": user_id,
    },
)
```

For a single positional value, remember to pass a one-element tuple or list:

```python
await session.execute(
    "SELECT * FROM users WHERE id = ?",
    (user_id,),
)
```

If the statement does not contain any bind markers, values can be omitted:

```python
await session.execute("SELECT * FROM users")
```

## Prepared and unprepared statements

Prepared statements and statement objects accept values in the same way as plain query strings.

```python
prepared = await session.prepare("INSERT INTO users (id, name, age) VALUES (?, ?, ?)")

await session.execute(
    prepared,
    (user_id, "Alice", 30),
)
```

## Type conversion

Python values are converted to the CQL types expected by the database schema.

For the full mapping of supported CQL and Python types, see [Data Types](../data-types.md).

## Values of your own types

The driver cannot write an object of a type that it does not know, such as a `Money` class or an `Enum`. An adapter converts such an object into a value that the driver can write. Register the adapters on `scylla.serialization.Adapters` and give them to the session builder:

```python
from scylla.cql_types import CqlTextType
from scylla.serialization import Adapters


def money(value: Money, cql_type):
    if isinstance(cql_type, CqlTextType):
        return f"{value.amount} {value.currency}"
    return value.amount


adapters = Adapters()
adapters.register_writer(Money, money)
adapters.register_writer(Address, lambda address, cql_type: dataclasses.asdict(address))

session = await SessionBuilder().contact_points(["127.0.0.1"]).adapters(adapters).connect()

await session.execute(
    "INSERT INTO orders (id, price, address) VALUES (?, ?, ?)",
    (order_id, Money(Decimal("9.99"), "EUR"), Address("Main St", 12345)),
)
```

The driver calls an adapter with the value and the CQL type of the column, so one adapter can write a type in different ways for different columns. The rules are:

- The driver calls an adapter only for a value that it cannot write. An adapter cannot change how the driver writes a type that it accepts, such as `int` or `str`.
- If a type has no adapter, the driver uses the adapter of its nearest base class.
- The driver does not adapt the result of an adapter again. The result must be a value that the driver can write to the column. If the result is a collection, the driver adapts its elements.
- The adapters also apply to `session.cluster_state.compute_token()`. The cluster state that a load balancing policy gets has no adapters, because a policy can serve more than one session.
- Adapters also apply to the elements of collections, tuples, vectors and user defined types.
- The session copies the adapters when it connects. Changes after the connection have no effect on that session.
