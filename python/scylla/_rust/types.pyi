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
