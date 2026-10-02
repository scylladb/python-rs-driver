# Serial Consistency

Additional consistency level parameter for Lightweight
transaction (LWT) statements.

## Default Serial Consistency
The default serial consistency is `SerialConsistency.LocalSerial`.


## Serial Consistency Levels

There are only two serial consistency levels available:

<table class="table table-striped table-hover table-condensed">
  <thead>
  <tr>
    <th>Level</th>
    <th>Behaviour</th>
  </tr>
  </thead>

  <tbody>
  <tr>
    <td><code>SerialConsistency.LocalSerial</code></td>
    <td>Ensures consistency only within the same datacenter.</td>
  </tr>
  <tr>
    <td><code>SerialConsistency.Serial</code></td>
    <td>Ensures cross-datacenter consistency.</td>
  </tr>
  </tbody>
</table>

**Note**: For cross-datacenter consistency, please remember to always
override the default with `SerialConsistency.Serial`.

## Setting Serial Consistency Level

Serial consistency level can be set on `ExecutionProfile`, `Statement`,
`PreparedStatement` or `Batch`.

For `ExecutionProfile` it can be set to either one of the serial consistencies
or to `None`. Setting serial consistency to `None` means that no serial consistency
will be used.

```python
from scylla.session import ExecutionProfile
from scylla.statement import SerialConsistency

# Setting serial consistency to LocalSerial.
profile = ExecutionProfile(serial_consistency=SerialConsistency.LocalSerial)

# Setting serial consistency to None.
profile = ExecutionProfile(serial_consistency=None)
```

For statements, serial consistency can also be set to one of the
two serial consistencies or `None`, and additionally it can be `UNSET`.

### `None` vs `UNSET`
While `None` overrides serial consistency from the `ExecutionProfile`,
`UNSET` means that serial consistency will be derived from the execution
profile (of the statement or, if absent, the `Session`).
For statements, the default state is `UNSET`.

```python
from scylla.statement import UNSET, Batch, PreparedStatement, SerialConsistency, Statement

query_str = "INSERT INTO tab (a, b) VALUES (1, 2) IF NOT EXISTS"

# Creating a Statement. Now serial consistency is UNSET.
statement = Statement(query_str)

# Setting serial consistency to LocalSerial for Statement.
statement.serial_consistency = SerialConsistency.LocalSerial

# Setting serial consistency to None for Prepared.
prepared = await session.prepare(query_str)
prepared.serial_consistency = None

# Setting serial consistency to Serial for Batch.
batch = Batch()
batch.serial_consistency = SerialConsistency.Serial

# Unsetting serial consistency for Batch.
batch.serial_consistency = UNSET
# Now serial consistency for batch is UNSET.
```

## Serial Consistency Hierarchy

Like for other options, the hierarchy of serial consistency is the following:

1. Serial consistency set directly on a statement
2. Statement’s profile
3. `Session`’s profile
4. `Session`’s default profile
