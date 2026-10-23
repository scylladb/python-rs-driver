import datetime

from dateutil.relativedelta import relativedelta

class CqlEmpty:
    """Represents an empty CQL value."""

class CqlDuration:
    """
    Value of a CQL ``duration`` column.

    A duration has three independent parts. The driver does not convert one
    part into another, because months and days do not have a fixed length.

    Parameters
    ----------
    months : int
        Number of months. Must fit in a signed 32-bit integer.
    days : int
        Number of days. Must fit in a signed 32-bit integer.
    nanoseconds : int
        Number of nanoseconds. Must fit in a signed 64-bit integer.

    Raises
    ------
    ValueError
        If the parts do not all have the same sign.
    OverflowError
        If a part does not fit in its integer size.
    """

    def __init__(self, months: int = 0, days: int = 0, nanoseconds: int = 0) -> None: ...
    @staticmethod
    def from_relativedelta(delta: relativedelta) -> CqlDuration:
        """
        Make a ``CqlDuration`` from a ``dateutil.relativedelta.relativedelta``.

        Years become months. Hours, minutes, seconds and microseconds become
        nanoseconds.

        Raises
        ------
        TypeError
            If ``delta`` is not a ``relativedelta``.
        ValueError
            If the parts do not all have the same sign.
        OverflowError
            If a part does not fit in its integer size.
        """
    def to_relativedelta(self) -> relativedelta:
        """
        Convert the value to a ``dateutil.relativedelta.relativedelta``.

        The result has microsecond precision. The last three digits of
        ``nanoseconds`` are lost. ``relativedelta`` moves each full 24 hours
        into ``days``.

        Raises
        ------
        ImportError
            If ``python-dateutil`` is not installed.
        """
    @property
    def months(self) -> int:
        """Number of months."""
    @property
    def days(self) -> int:
        """Number of days."""
    @property
    def nanoseconds(self) -> int:
        """Number of nanoseconds."""
    def __eq__(self, other: object) -> bool: ...
    def __hash__(self) -> int: ...

class CqlDate:
    """
    Value of a CQL ``date`` column.

    A CQL date can be far outside the range of ``datetime.date``. ``CqlDate``
    holds every CQL date without loss.

    Parameters
    ----------
    days : int
        Number of days after 1970-01-01. A negative value is a day before
        1970-01-01. Must fit in a signed 32-bit integer.

    Raises
    ------
    OverflowError
        If ``days`` does not fit in a signed 32-bit integer.
    """

    def __init__(self, days: int) -> None: ...
    @staticmethod
    def from_date(date: datetime.date) -> CqlDate:
        """Make a ``CqlDate`` from a ``datetime.date``."""
    def to_date(self) -> datetime.date:
        """
        Convert the value to a ``datetime.date``.

        Raises
        ------
        ValueError
            If the date is outside the range of ``datetime.date``.
        """
    @property
    def days(self) -> int:
        """Number of days after 1970-01-01."""
    def __eq__(self, other: object) -> bool: ...
    def __lt__(self, other: CqlDate) -> bool: ...
    def __le__(self, other: CqlDate) -> bool: ...
    def __gt__(self, other: CqlDate) -> bool: ...
    def __ge__(self, other: CqlDate) -> bool: ...
    def __hash__(self) -> int: ...

class CqlTime:
    """
    Value of a CQL ``time`` column.

    A CQL time has nanosecond precision, and ``datetime.time`` has only
    microsecond precision. ``CqlTime`` holds every CQL time without loss.

    Parameters
    ----------
    nanoseconds : int
        Number of nanoseconds after midnight. Must be from 0 to
        86399999999999.

    Raises
    ------
    ValueError
        If ``nanoseconds`` is outside the range.
    """

    def __init__(self, nanoseconds: int) -> None: ...
    @staticmethod
    def from_time(time: datetime.time) -> CqlTime:
        """
        Make a ``CqlTime`` from a ``datetime.time``.

        Raises
        ------
        ValueError
            If the time is a leap second.
        """
    def to_time(self) -> datetime.time:
        """
        Convert the value to a ``datetime.time``.

        The result has microsecond precision. The last three digits of
        ``nanosecond`` are lost.
        """
    @property
    def nanoseconds(self) -> int:
        """Number of nanoseconds after midnight."""
    @property
    def hour(self) -> int:
        """Hour, from 0 to 23."""
    @property
    def minute(self) -> int:
        """Minute, from 0 to 59."""
    @property
    def second(self) -> int:
        """Second, from 0 to 59."""
    @property
    def nanosecond(self) -> int:
        """Nanosecond in the current second, from 0 to 999999999."""
    def __eq__(self, other: object) -> bool: ...
    def __lt__(self, other: CqlTime) -> bool: ...
    def __le__(self, other: CqlTime) -> bool: ...
    def __gt__(self, other: CqlTime) -> bool: ...
    def __ge__(self, other: CqlTime) -> bool: ...
    def __hash__(self) -> int: ...

class CqlTimestamp:
    """
    Value for a CQL ``timestamp`` column.

    A CQL timestamp can be far outside the range of ``datetime.datetime``.
    ``CqlTimestamp`` holds every CQL timestamp without loss. Reading returns it,
    and writing accepts it.

    Parameters
    ----------
    milliseconds : int
        Number of milliseconds after 1970-01-01 00:00:00 UTC. A negative
        value is a time before that. Must fit in a signed 64-bit integer.

    Raises
    ------
    OverflowError
        If ``milliseconds`` does not fit in a signed 64-bit integer.
    """

    def __init__(self, milliseconds: int) -> None: ...
    @staticmethod
    def from_datetime(datetime: datetime.datetime) -> CqlTimestamp:
        """
        Make a ``CqlTimestamp`` from a timezone-aware ``datetime.datetime``.

        The datetime can be in any time zone, including a ``zoneinfo.ZoneInfo``
        zone. The result has millisecond precision. The microseconds after the
        last full millisecond are lost.

        Raises
        ------
        TypeError
            If the datetime has no time zone.
        """
    def to_datetime(self) -> datetime.datetime:
        """
        Convert the value to a ``datetime.datetime`` in UTC.

        Raises
        ------
        ValueError
            If the timestamp is outside the range of ``datetime.datetime``.
        """
    @property
    def milliseconds(self) -> int:
        """Number of milliseconds after 1970-01-01 00:00:00 UTC."""
    def __eq__(self, other: object) -> bool: ...
    def __lt__(self, other: CqlTimestamp) -> bool: ...
    def __le__(self, other: CqlTimestamp) -> bool: ...
    def __gt__(self, other: CqlTimestamp) -> bool: ...
    def __ge__(self, other: CqlTimestamp) -> bool: ...
    def __hash__(self) -> int: ...
