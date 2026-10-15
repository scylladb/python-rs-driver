import datetime

class UnsetType:
    """
    Type of the `UNSET` singleton.
    """

class CqlEmpty:
    """Represents an empty CQL value."""

class Duration:
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

class Date:
    """
    Value of a CQL ``date`` column.

    A CQL date can be far outside the range of ``datetime.date``. ``Date``
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
    def from_date(date: datetime.date) -> Date:
        """Make a ``Date`` from a ``datetime.date``."""
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
    def __lt__(self, other: Date) -> bool: ...
    def __le__(self, other: Date) -> bool: ...
    def __gt__(self, other: Date) -> bool: ...
    def __ge__(self, other: Date) -> bool: ...
    def __hash__(self) -> int: ...
