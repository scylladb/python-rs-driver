# Data Types

The driver maps CQL data types to matching Python objects for sending values to the database and receiving values from query results.

See [Statement values](using/statement-values.md) for more information about passing values to statements. 
See [Query result](getting-started.md#working-with-results) for more information about reading values from query results.

The table below shows the default Python objects returned by the driver when reading CQL values from query results, and the Python objects that can currently be provided when sending values to the database.

| CQL type | Values returned by the driver | Values accepted by the driver |
|---|---|---|
| `boolean` | `bool` | `bool` |
| `tinyint` | `int` | `int` |
| `smallint` | `int` | `int` |
| `int` | `int` | `int` |
| `bigint` | `int` | `int` |
| `counter` | `int` | `int` |
| `varint` | `int` | `int` |
| `float` | `float` | `float` |
| `double` | `float` | `float` |
| `ascii` | `str` | `str` |
| `text` | `str` | `str` |
| `varchar` | `str` | `str` |
| `blob` | `bytes` | `bytes` |
| `inet` | `ipaddress.IPv4Address` or `ipaddress.IPv6Address` | `ipaddress.IPv4Address` or `ipaddress.IPv6Address` |
| `uuid` | `uuid.UUID` | `uuid.UUID` |
| `timeuuid` | `uuid.UUID` | `uuid.UUID` |
| `date` | `scylla.cql_types.CqlDate` | `scylla.cql_types.CqlDate` or `datetime.date` |
| `time` | `scylla.cql_types.CqlTime` | `scylla.cql_types.CqlTime` or `datetime.time` |
| `timestamp` | `scylla.cql_types.CqlTimestamp` | `scylla.cql_types.CqlTimestamp` or `datetime.datetime` with a time zone |
| `duration` | `scylla.cql_types.CqlDuration` | `scylla.cql_types.CqlDuration` or `dateutil.relativedelta.relativedelta` (requires `python-dateutil`) |
| `decimal` | `decimal.Decimal` | `decimal.Decimal` |
| `list<T>` | `list` | `list` |
| `set<T>` | `set` | `set` |
| `map<K, V>` | `dict` | `dict` |
| `tuple<...>` | `tuple` | `tuple` |
| `udt` | `dict[str, object]` | `dict[str, object]` |
| `vector<T>` | `list` | `list` |
| `null` | `None` | `None` |

Values supplied by the user are validated against the CQL types expected by the database schema. If a value does not match the expected type, the driver returns an error.
The set of accepted input types may be extended in the future. For now, the accepted input types use the same Python object kinds as the default values returned by the driver.
