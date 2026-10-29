from collections.abc import Callable
from typing import TypeVar

from scylla.cql_types import CqlColumnType

_T = TypeVar("_T")

class Adapters:
    """
    Adapters that convert values of your own types into values the driver can write.

    When the driver cannot write a value, it looks for an adapter for the type
    of the value. If there is no adapter for that type, it looks for an adapter
    for a base class, in method resolution order. The driver calls the adapter
    with the value and the CQL type of the column, and writes the result. The
    driver does not adapt the result itself again, but it adapts the elements
    of a returned collection.

    If no adapter matches, the driver calls the ``__cql_value__(cql_type)``
    method of the value, if the value has one.

    The driver calls an adapter only for a value that it cannot write. An
    adapter cannot change how the driver writes a type that it accepts.

    Adapters also apply to the elements of lists, sets, maps, tuples, vectors
    and user defined types.

    Give the adapters to :meth:`~scylla.session.SessionBuilder.adapters`. The
    session copies them when it connects. Changes after the connection have no
    effect on that session. The session's adapters also apply to
    ``session.cluster_state.compute_token()``. The cluster state that a load
    balancing policy gets has no adapters, because a policy can serve more
    than one session.

    Examples
    --------
    >>> adapters = Adapters()
    >>> adapters.register_writer(Money, lambda money, cql_type: money.amount)
    >>> session = await SessionBuilder().adapters(adapters).connect()
    """

    def __init__(self) -> None: ...
    def register_writer(self, cls: type[_T], adapter: Callable[[_T, CqlColumnType], object]) -> None:
        """
        Register the adapter for a type.

        If the type already has an adapter, the new adapter replaces it.

        Parameters
        ----------
        cls : type
            The type of the values to convert. The adapter also applies to
            subclasses that do not have their own adapter.
        adapter : Callable[[Any, CqlColumnType], object]
            Called with the value and the CQL type of the column. It must
            return a value that the driver can write to that column. The driver
            checks the result when it writes it.

        Raises
        ------
        TypeError
            If ``adapter`` is not callable.
        """
