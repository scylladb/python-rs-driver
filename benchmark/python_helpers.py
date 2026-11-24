import asyncio
import ipaddress
import os
import uuid
from datetime import date, time
from typing import Any

from cassandra.cluster import Cluster, ResponseFuture, Session  # pyright: ignore[reportMissingTypeStubs]
from cassandra.util import Duration  # pyright: ignore[reportMissingTypeStubs]
from dateutil.relativedelta import relativedelta

# Force non-paged queries by setting a large page size
PAGE_SIZE = 1_000_000


def relativedelta_to_duration(rd: relativedelta) -> Duration:
    """
    Convert a dateutil.relativedelta to cassandra.util.Duration.

    cassandra.util.Duration(months, days, nanoseconds) expects:
    - months: total months (years * 12 + months)
    - days: total days
    - nanoseconds: total nanoseconds (microseconds * 1000)
    """
    months = (rd.years or 0) * 12 + (rd.months or 0)
    days = rd.days or 0
    nanoseconds = (rd.microseconds or 0) * 1000

    return Duration(months, days, nanoseconds)


def convert_complex_data_for_cassandra(
    data: tuple[
        uuid.UUID,
        int,
        uuid.UUID,
        ipaddress.IPv4Address,
        date,
        time,
        tuple[str, int],
        dict[str, Any],
        set[int],
        relativedelta,
    ],
) -> tuple[
    uuid.UUID,
    int,
    uuid.UUID,
    ipaddress.IPv4Address,
    date,
    time,
    tuple[str, int],
    tuple[str, int],
    set[int],
    Duration,
]:
    """
    Convert complex data from common.get_complex_data() format to cassandra-driver compatible format.

    Specifically converts:
    - relativedelta -> cassandra.util.Duration
    - UDT dict -> tuple (cassandra-driver expects positional tuple values for UDTs)

    Args:
        data: tuple from get_complex_data() containing:
              (id, val, tuuid, ip, date, time, tuple, udt, set, duration)

    Returns:
        tuple with duration and UDT converted to cassandra-driver format
    """
    # Unpack the tuple
    id, val, tuuid, ip, date_val, time_val, tuple_val, udt, set_val, duration = data

    # Convert duration from relativedelta to cassandra Duration
    cassandra_duration = relativedelta_to_duration(duration)

    # Convert UDT from dict to tuple (field1, field2) - cassandra-driver expects ordered tuple
    # UDT schema is: CREATE TYPE benchmarks.udt1 (field1 text, field2 int)
    udt_tuple: tuple[str, int] = (udt["field1"], udt["field2"])  # type: ignore[misc]

    # Return with converted duration and UDT
    return (id, val, tuuid, ip, date_val, time_val, tuple_val, udt_tuple, set_val, cassandra_duration)


async def connect() -> Session:
    uri = os.getenv("SCYLLA_URI", "127.0.0.2:9042")
    if ":" in uri:
        host, port_str = uri.rsplit(":", 1)
        port = int(port_str)
    else:
        host = uri
        port = 9042
    cluster = Cluster([host], port=port)
    session = cluster.connect()  # pyright: ignore[reportUnknownMemberType]
    return session


def to_asyncio(response_future: ResponseFuture) -> asyncio.Future[Any]:
    """
    Create an awaitable asyncio future from a cassandra response future.

    This merges cassandra and asyncio API by adding
    thread-safe asyncio compatible callbacks to the cassandra response future.
    """
    loop = asyncio.get_running_loop()
    fut = loop.create_future()

    def on_success(result: Any):
        loop.call_soon_threadsafe(fut.set_result, result)

    def on_error(exc: Any):
        loop.call_soon_threadsafe(fut.set_exception, exc)

    response_future.add_callbacks(on_success, on_error)  # pyright: ignore[reportUnknownMemberType]

    return fut
