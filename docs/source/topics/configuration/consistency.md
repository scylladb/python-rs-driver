# Consistency

A setting that defines a successful write or read by the number
of cluster replicas that acknowledge the write or respond to the read
request, respectively.


## Default Consistency
The default consistency is `Consistency.LocalQuorum`.


## Consistency Levels

The consistency level determines the number of replicas on which the
read/write must respond/succeed before returning an acknowledgment
to the client application. Descriptions for each read/write
consistency level can be found
[here](https://docs.scylladb.com/manual/stable/cql/consistency.html).

<table class="table table-striped table-hover table-condensed">
  <thead>
  <tr>
    <th>Level</th>
    <th>Driver</th>
  </tr>
  </thead>

  <tbody>
  <tr>
    <td>Any (Write only)</td>
    <td><code>Consistency.Any</code></td>
  </tr>
  <tr>
    <td><b>One</b></td>
    <td><code><b>Consistency.One</b></code></td>
  </tr>
  <tr>
    <td>Two</td>
    <td><code>Consistency.Two</code></td>
  </tr>
  <tr>
    <td>Three</td>
    <td><code>Consistency.Three</code></td>
  </tr>
  <tr>
    <td>Quorum</td>
    <td><code>Consistency.Quorum</code></td>
  </tr>
  <tr>
    <td>Local Quorum</td>
    <td><code>Consistency.LocalQuorum</code></td>
  </tr>
  <tr>
    <td>All</td>
    <td><code>Consistency.All</code></td>
  </tr>
  <tr>
    <td>Each Quorum (Write only)</td>
    <td><code>Consistency.EachQuorum</code></td>
  </tr>
  <tr>
    <td>Local One</td>
    <td><code>Consistency.LocalOne</code></td>
  </tr>
  <tr>
    <td>Serial (Read only)</td>
    <td><code>Consistency.Serial</code></td>
  </tr>
  <tr>
    <td>Local Serial (Read only)</td>
    <td><code>Consistency.LocalSerial</code></td>
  </tr>
  </tbody>
</table>

**Note**: For queries against a single partition, it is possible
to ensure a read is (locally) serial by setting plain consistency to
`Consistency.Serial`/`Consistency.LocalSerial`. With other consistency
levels, a read may not see a value that is currently being updated by
a conditional write.


## Setting Consistency Level

Consistency Level can be set on `ExecutionProfile`, `Statement`,
`PreparedStatement` or `Batch`.

Setting Consistency Level for `ExecutionProfile`:

```python
from scylla.session import ExecutionProfile
from scylla.statement import Consistency

profile = ExecutionProfile(consistency=Consistency.One)
```

Setting Consistency Level for `Statement`, `PreparedStatement`
and `Batch`:

```python
from scylla.statement import UNSET, Batch, Consistency, PreparedStatement, Statement

query_str = "INSERT INTO tab (a, b) VALUES (1, 2)"

# Setting consistency to Two for Statement.
statement = Statement(query_str)
statement.consistency = Consistency.Two

# Setting consistency to Three for Prepared.
prepared = await session.prepare(query_str)
prepared.consistency = Consistency.Three

# Setting consistency to All for Batch.
batch = Batch()
batch.consistency = Consistency.All

# Unsetting consistency, so it is taken from the execution profile again.
batch.consistency = UNSET
```

The property is modified in place. When consistency is not set on the
statement, reading it returns `UNSET`.

## Consistency Hierarchy

Like for other options, the hierarchy of consistency is the following:

1. Consistency set directly on a statement
2. Statement’s profile
3. `Session`’s profile
4. `Session`’s default profile
