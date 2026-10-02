# Batch statement

A batch statement allows to execute many data-modifying statements at once.\
These statements can be [unprepared or prepared](statement-values.md#prepared-and-unprepared-statements).\
Only `INSERT`, `UPDATE` and `DELETE` statements are allowed.

```python
from scylla.statement import Batch, Statement

# Create a batch statement.
batch = Batch()

# Add an unprepared statement to the batch using its text.
batch.add("INSERT INTO tab (a, b) VALUES (1, 2)")

# Add an unprepared statement created manually to the batch.
unprepared = Statement("INSERT INTO tab (a, b) VALUES (3, ?)", (4,))
batch.add(unprepared)

# Add a prepared statement to the batch.
prepared = await session.prepare("INSERT INTO tab (a, b) VALUES(5, 6)", None)
batch.add(prepared)

# Add multiple statements to the batch at once.
unprepared_str = "INSERT INTO tab (a, b) VALUES (7, 8)"
unprepared = Statement("INSERT INTO tab (a, b) VALUES (9, 10)")
prepared = await session.prepare("INSERT INTO tab (a, b) VALUES(11, 12)")
batch.add_all([(unprepared_str, None), (unprepared, None), (prepared, None)])

# Run the batch.
await session.batch(batch)
```


## Batch values
When adding statements to a batch, you can separate the statement's text from its values
using bind markers.\
Values can be provided as a **list**, a **tuple**, a **mapping**.
If the statement requires no values, they can be completely **omitted**, explicitly passed
as `None` or empty list, tuple or mapping.


Example:
```python
from scylla.statement import Batch

# Create a batch statement.
batch = Batch()

# A statement with two bound values.
batch.add("INSERT INTO tab (a, b) VALUES (?, ?)", (1, 2))

# A statement with one bound value.
batch.add("INSERT INTO tab (a, b) VALUES (3, ?)", (4,))

# A statement with no bound values.
batch.add("INSERT INTO tab (a, b) VALUES (5, 6)")

# A statement with no bound values and explicit None.
batch.add("INSERT INTO tab (a, b) VALUES (7, 8)", None)

# A statement with no bound values and empty tuple.
batch.add("INSERT INTO tab (a, b) VALUES (9, 10)", ())

# Run the batch.
# Note that the driver will prepare the first two statements, due to them
# not being prepared and having a non-empty list of values.
await session.batch(batch)
```


When adding multiple statements at once using `Batch.add_all`, you **must** explicitly
provide a value argument for each statement, even if there are no values. This can be done
using `None`, empty list, tuple or mapping.

Example:
```python
from scylla.statement import Batch

# Create a batch statement.
batch = Batch()

# Add multiple statements with values.
batch.add_all(
    [
        ("INSERT INTO tab (a, b) VALUES (?, ?)", (7, 8)),
        ("INSERT INTO tab (a, b) VALUES (9, ?)", (10,)),
        ("INSERT INTO tab (a, b) VALUES (11, 12)", None),
        ("INSERT INTO tab (a, b) VALUES (13, 14)", ()),
    ]
)

# Run the batch.
# Note that the driver will prepare the first two statements, due to them
# not being prepared and having a non-empty list of values.
await session.batch(batch)
```


> ***Warning***\
> Using unprepared statements with bind markers in batches is strongly discouraged.
> For each unprepared statement with a non-empty list of values in the batch,
> the driver will send a prepare request, and it will be done **sequentially**.
> Results of preparation are not cached between `Session.batch` calls.
> Consider preparing the statements before putting them into the batch.


## Batch options
You can configure a batch statement by assigning to its properties, such as
`consistency`, `serial_consistency`, `request_timeout`, `execution_profile`,
`load_balancing_policy`, `retry_policy` or `is_idempotent`.\
**Note:** Assigning a property **modifies the batch in place**. Assigning `UNSET`
(`from scylla.statement import UNSET`) makes the option fall back to the execution profile.

Example:
```python
from scylla.statement import Batch, Consistency

# Create a batch statement.
batch = Batch()
batch.add("INSERT INTO tab (a) VALUES (16)")

# Set consistency of the batch to One.
batch.consistency = Consistency.One

# Run the batch.
await session.batch(batch)
```


## Performance
Batches use token/shard-aware load balancing, but routing is calculated based **only**
on the **first statement** in the batch. Therefore, to get full shard awareness, only group
queries targeting the same partition/shard into the same batch.
