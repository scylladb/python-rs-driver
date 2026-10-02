import ipaddress
import uuid
from datetime import date, datetime, time, timezone
from typing import Any

from dateutil.relativedelta import relativedelta

SIMPLE_INSERT_QUERY = "INSERT INTO benchmarks.basic (id, val) VALUES (?, ?)"
COMPLEX_INSERT_QUERY = "INSERT INTO benchmarks.complex (id, val, tuuid, ip, date, time, tuple, udt, set1, duration) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
COMPLEX_SELECT_QUERY = "SELECT * FROM benchmarks.complex USING TIMEOUT 120s;"
COMPLEX_SELECT_COUNT = "SELECT COUNT(*) FROM benchmarks.complex USING TIMEOUT 120s;"
SIMPLE_SELECT_COUNT = "SELECT COUNT(*) FROM benchmarks.basic USING TIMEOUT 120s;"
SIMPLE_SELECT_QUERY = "SELECT * FROM benchmarks.basic"


def get_simple_data() -> tuple[uuid.UUID, int]:
    id = uuid.uuid4()
    return id, 100


# Static data for complex inserts - created once to avoid recreation overhead
_STATIC_TUUID = uuid.UUID("8e14e760-7fa8-11eb-bc66-000000000001")
_STATIC_IP = ipaddress.IPv4Address("192.168.0.1")
_STATIC_NOW = datetime(1, 1, 1, 23, 59, 59, 999000, tzinfo=timezone.utc)
_STATIC_DATE = _STATIC_NOW.date()
_STATIC_TIME = _STATIC_NOW.time()
_STATIC_TUPLE = (
    "Litwo! Ojczyzno moja! ty jesteś jak zdrowie: Ile cię trzeba cenić, ten tylko się dowie, "
    "Kto cię stracił. Dziś piękność twą w całej ozdobie Widzę i opisuję, bo tęsknię po tobie.",
    1,
)
_STATIC_UDT = {
    "field1": "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Duis congue egestas sapien id maximus eget.",
    "field2": 4321,
}
_STATIC_SET = {1, 2, 3, 4, 5, 6, 7, 8, 9, 11}
_STATIC_DURATION = relativedelta(months=2, days=5, microseconds=36)


def get_complex_data() -> tuple[
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
]:
    # Only regenerate UUID - everything else is static
    id = uuid.uuid4()

    return (
        id,
        100,
        _STATIC_TUUID,
        _STATIC_IP,
        _STATIC_DATE,
        _STATIC_TIME,
        _STATIC_TUPLE,
        _STATIC_UDT,
        _STATIC_SET,
        _STATIC_DURATION,
    )
