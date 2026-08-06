# Execution Profiles

Execution profiles provide a mechanism for centralizing all configuration options related to statement execution within a single object.

## Configuration Hierarchy

You can configure statement execution at three different levels, establishing a clear hierarchy where more specific configuration overrides more general defaults:

1. **Statement-level options** (highest priority) – Set directly on individual statements
2. **Statement-level profiles** – Assigned to `PreparedStatement` or `Statement`
3. **Session-level profiles** (lowest priority) – Defined during session configuration

When several levels define the same option, more specific configuration overrides more general defaults.

For example, if a session has a default timeout of 10 seconds, but a statement has an execution profile with a 3-second timeout, and the statement also has a direct timeout setting of 5 seconds, the statement will use a 5-second timeout.

## ExecutionProfile

The `ExecutionProfile` class encapsulates multiple configuration parameters for statement execution.

### Creating an Execution Profile

The simplest way to create an execution profile is to instantiate `ExecutionProfile()` with desired options as arguments:

```python
from scylla.enums import Consistency, SerialConsistency
from scylla.execution_profile import ExecutionProfile

# NOTE: All `await` calls should be made inside an `async def` function

profile = ExecutionProfile(timeout=10.5, consistency=Consistency.All, serial_consistency=SerialConsistency.Serial)
```

### Configurable Parameters

An execution profile supports the following configuration parameters:

#### Request Timeout

Specifies the maximum time (in seconds) the driver will wait for a response from the cluster. If the timeout elapses the execution fails immediately. This is true for all retry attempts for this request:

```python
from scylla.execution_profile import ExecutionProfile

profile = ExecutionProfile(timeout=10.5)

# Access the timeout value
timeout_value = profile.request_timeout
```

#### Consistency Level

Determines the number of nodes that must acknowledge a read or write operation for it to be considered successful.

```python
from scylla.enums import Consistency
from scylla.execution_profile import ExecutionProfile

profile = ExecutionProfile(consistency=Consistency.One)

# Access the consistency level
consistency = profile.consistency
```

#### Serial Consistency Level

Similar to the consistency level, but used for conditional operations. This setting is independent of the regular consistency level.

```python
from scylla.enums import SerialConsistency
from scylla.execution_profile import ExecutionProfile

profile = ExecutionProfile(serial_consistency=SerialConsistency.Serial)

# Access the serial consistency level
serial_consistency = profile.serial_consistency
```

## Using Profiles at Session Level

You can set a default execution profile when creating a session using `SessionBuilder`:

```python
from scylla.enums import Consistency
from scylla.execution_profile import ExecutionProfile
from scylla.session_builder import SessionBuilder

profile = ExecutionProfile(timeout=10.5, consistency=Consistency.All)

builder = SessionBuilder().contact_points([("127.0.0.1", 9042)]).execution_profile(profile)
session = await builder.connect()

# The profile settings apply to all statements executed with this session
result = await session.execute("SELECT * FROM users")
```

## Using Profiles at Statement Level

You can assign an execution profile to individual `Statement` or `PreparedStatement` objects:

```python
from scylla.execution_profile import ExecutionProfile
from scylla.session_builder import SessionBuilder
from scylla.statement import Statement

builder = SessionBuilder().contact_points([("127.0.0.1", 9042)])
session = await builder.connect()

stmt = Statement("SELECT * FROM users")
profile = ExecutionProfile(timeout=2.5)

stmt = stmt.with_execution_profile(profile)

# Access the assigned profile
assigned_profile = stmt.execution_profile

prepared = await session.prepare("SELECT * FROM users")
profile = ExecutionProfile(timeout=1.5)

prepared = prepared.with_execution_profile(profile)

# Access the assigned profile
assigned_profile = prepared.execution_profile
```

To remove a previously assigned profile:

```python
stmt = stmt.without_execution_profile()
```
