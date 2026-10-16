import datetime as dt
import uuid
from ipaddress import IPv4Address, IPv6Address

class QueryTrace:
    """
    The trace of one request: how the server ran it, step by step.

    The server writes a trace only for a request that has tracing turned on.
    The fields contain the details of the whole request, and `events` contains
    the steps.

    The trace can be incomplete. The server can write more events after the
    driver fetches the trace. To get the new events, fetch the trace again.
    """

    @property
    def trace_id(self) -> uuid.UUID:
        """The tracing id of the request."""
    @property
    def request_type(self) -> str | None:
        """The type of the request, for example ``"Execute CQL3 query"``."""
    @property
    def command(self) -> str | None:
        """The type of the command, for example ``"QUERY"``."""
    @property
    def duration(self) -> dt.timedelta | None:
        """
        The time that the coordinator used to run the request.

        `None` if the server did not write the duration yet.
        """
    @property
    def coordinator(self) -> IPv4Address | IPv6Address | None:
        """The address of the node that coordinated the request."""
    @property
    def client(self) -> IPv4Address | IPv6Address | None:
        """The address of the client that sent the request."""
    @property
    def parameters(self) -> dict[str, str] | None:
        """The parameters of the request, for example the query text and the consistency level."""
    @property
    def started_at(self) -> dt.datetime | None:
        """The time, in UTC, when the coordinator started the request."""
    @property
    def events(self) -> tuple[TraceEvent, ...]:
        """The events of the request, in the order of their time."""

class TraceEvent:
    """
    One step of a query trace: what occurred, on which node, and when.
    """

    @property
    def event_id(self) -> uuid.UUID:
        """The time-based UUID of the event."""
    @property
    def description(self) -> str | None:
        """What occurred, for example ``"Parsing a statement"``."""
    @property
    def datetime(self) -> dt.datetime | None:
        """The time of the event, in UTC. It is the time that `event_id` contains."""
    @property
    def source(self) -> IPv4Address | IPv6Address | None:
        """The address of the node where the event occurred."""
    @property
    def source_elapsed(self) -> dt.timedelta | None:
        """The time from when `source` received the request to the event."""
    @property
    def thread_name(self) -> str | None:
        """The thread or shard where the event occurred, for example ``"shard 0"``."""
