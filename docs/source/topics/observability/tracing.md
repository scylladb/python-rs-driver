# Query tracing

When a request is traced, the server records how it ran the request. It writes one
row to `system_traces.sessions` and one row for each step to `system_traces.events`.
The response contains a tracing id, which is the key of these rows.

Each page of a paged query is a separate request, so each page has its own tracing id.

## Enable tracing

Set `tracing` on a statement or a batch to trace each of its executions:

```python
statement = Statement("SELECT * FROM ks.t WHERE pk = ?")
statement.tracing = True
result = await session.execute(statement, (1,))
```

Or use the `trace` argument of `Session.execute()` and `Session.batch()` for one
execution. It overrides the `tracing` attribute of the statement:

```python
result = await session.execute("SELECT * FROM ks.t", trace=True)
result = await session.execute(statement, (1,), trace=False)  # not traced
```

## Get the tracing ids

- `Page.trace_id` is the id of the request that fetched that page.
- `RequestResult.trace_ids` is the list of the ids of all requests made for the
  result, in the order in which the responses arrived. It starts with the id of
  the first page. Each page that `all()`, `first_row()`, `async for`, `pages()` or
  `Page.fetch_next_page()` fetches adds its id. If a later read fetches the same
  page again, that request adds a new id.

```python
result = await session.execute(statement, trace=True)
rows = await result.all()
print(result.first_page.trace_id)
print(result.trace_ids)  # one id for each page
```

## Fetch the trace

The server writes a trace some time after it sends the response. To fetch a trace,
the driver reads the two tables and tries again until the trace is available.

```python
trace = await result.fetch_first_page_query_trace()  # the first page, or None
traces = await result.fetch_all_query_traces()  # one for each id in trace_ids
trace = await page.fetch_query_trace()  # one page, or None
trace = await session.fetch_query_trace(trace_id)  # any tracing id

print(trace.coordinator, trace.duration)
for event in trace.events:
    print(event.source_elapsed, event.source, event.thread_name, event.description)
```

If the trace is not available after all attempts, the driver raises
`scylla.errors.TraceUnavailable`.

```{warning}
The driver cannot know if a fetched trace is complete. It returns the trace when
the `sessions` row has a duration and at least one event exists. At that time, the
server can still write more events. If a trace looks incomplete, fetch it again.
```

A `QueryTrace` contains the details of the whole request, for example the
coordinator, the start time and the duration. Its `events` are `TraceEvent`
objects, one for each step of the request. Addresses are `ipaddress.IPv4Address`
or `ipaddress.IPv6Address` objects, and times are in UTC.

## Configure how the driver fetches traces

```python
session = await (
    SessionBuilder()
    .contact_points([("127.0.0.1", 9042)])
    .tracing_info_fetch_attempts(20)  # default: 10
    .tracing_info_fetch_interval(0.05)  # default: 3 ms
    .tracing_info_fetch_consistency(Consistency.Quorum)  # default: One
    .connect()
)
```

Cassandra can need a longer interval than ScyllaDB.

## Trace a prepare

`Session.prepare()` traces the preparation if it gets a `Statement` with `tracing`
set to `True`. `PreparedStatement.prepare_trace_ids` contains one id for each node
that prepared the statement. The prepared statement keeps `tracing` set to `True`,
so its executions are traced too.
