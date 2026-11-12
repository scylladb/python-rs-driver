# Driver configuration report

The driver reports its configuration to the server. Thus, you can see the
configuration of a client from the server side. You do not have to find it in
the logs of the client.

## What the driver sends

The control connection sends a `DRIVER_CONFIG` option in the CQL STARTUP message.
The value is a JSON description of the configuration that the session uses.
The description includes:

- the timeouts,
- the connection pool settings,
- the load balancing, retry and speculative execution policies.

The description is the same for all connections of a session. Thus, only the
control connection sends it.

All ScyllaDB drivers use the same schema for the description. Thus, the same
query shows the configuration of a client, for each ScyllaDB driver.

Each connection also sends a `SESSION_ID` option. Its value is a UUID that the
driver makes once for each session. All connections of one session send the same
`SESSION_ID`.

## Read the report

ScyllaDB shows the options in `system.clients.client_options`:

```sql
SELECT address, port, client_options FROM system.clients;
```

Each node shows only the connections to that node. The report is on the node of
the control connection. Thus, to find the report, read `system.clients` on each
node.

To find the rows of your application, set an application name with
`SessionBuilder.custom_identity()`. ScyllaDB shows it as `APPLICATION_NAME` in
`client_options`.

Older ScyllaDB versions do not have the `client_options` column.

## Turn the report off

The report is on by default. If the cluster must not get the configuration of the
client, turn the report off:

```python
session = await SessionBuilder().contact_points([("127.0.0.1", 9042)]).driver_config_reporting(False).connect()
```

This has no effect on `SESSION_ID`. Each connection still sends it. `SESSION_ID`
gives no information about the client.
