from collections.abc import Callable
from typing import TypeVar

from scylla.cql_types import CqlColumnType, CqlType

_T = TypeVar("_T")

class Adapters:
    """
    Adapters that convert values of your own types into values the driver can write,
    and values that the driver reads into your own types.

    Writers are described below. For readers, see :meth:`register_reader` and
    :meth:`register_udt`.

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
    def register_writer(self, cls: type[_T], adapter: Callable[[_T, CqlType], object]) -> None:
        """
        Register the adapter for a type.

        If the type already has an adapter, the new adapter replaces it.

        Parameters
        ----------
        cls : type
            The type of the values to convert. The adapter also applies to
            subclasses that do not have their own adapter.
        adapter : Callable[[Any, CqlType], object]
            Called with the value and the CQL type of the column. It must
            return a value that the driver can write to that column. The driver
            checks the result when it writes it.

        Raises
        ------
        TypeError
            If ``adapter`` is not callable.
        """
    def register_reader(
        self,
        cql_type: CqlColumnType | type[CqlColumnType],
        reader: Callable[..., object],
    ) -> None:
        """
        Register the reader for a CQL type.

        The driver calls the reader with each value of this type that it reads,
        and returns the result in place of the value. This includes the
        elements of lists, sets, maps, tuples, vectors and user defined types.
        The driver converts the elements of a value before the value itself.
        The driver does not call the reader for a null or empty value. The
        exception is a null list, set or map: the driver reads it as an empty
        one, so the reader gets an empty ``list``, ``set`` or ``dict``.

        A class with type arguments, such as ``CqlListType[CqlTimestampType]``,
        selects only that type. A collection, tuple or vector class with no
        type arguments, such as ``CqlVectorType``, selects all types of that
        kind. Such a class can also be a type argument:
        ``CqlListType[CqlListType]`` selects a list of lists. Matching ignores
        ``frozen`` and the dimensions of a vector, so
        ``CqlVectorType[CqlFloatType]`` selects float vectors of all sizes.

        If more than one reader matches a value, the driver uses the most
        specific one. The driver compares the keys from the outside to the
        inside. At the first place where they are different, a type with type
        arguments is more specific than a class with no type arguments. The
        elements of a map or tuple compare from left to right.

        A CQL type object, such as one from the schema metadata, selects the
        same type as the class with the same type arguments.

        ``CqlUserDefinedType`` selects every user defined type. For such a
        value, the driver calls ``reader(**fields)`` with one keyword argument
        for each field, in place of the value it reads without the reader. A
        null field gives ``None``. A class from :meth:`register_udt` takes
        precedence over this reader. A user defined type from the schema
        metadata is not a valid key: use :meth:`register_udt` for one user
        defined type.

        If the type already has a reader, the new reader replaces it.

        Parameters
        ----------
        cql_type : CqlColumnType | type[CqlColumnType]
            The CQL type of the values to convert.
        reader : Callable[..., object]
            Called with the value that the driver reads without the reader. For
            a user defined type, called with the fields as keyword arguments.

        Raises
        ------
        TypeError
            If ``reader`` is not callable, or if ``cql_type`` is one user
            defined type. Use :meth:`register_udt` for one user defined type.

        Examples
        --------
        >>> adapters.register_reader(CqlTimestampType, lambda ts: ts.to_datetime())
        >>> adapters.register_reader(CqlVectorType[CqlFloatType], np.asarray)
        >>> adapters.register_reader(CqlUserDefinedType, dict)
        """
    def register_udt(self, keyspace: str, name: str, cls: type) -> None:
        """
        Register the class for a user defined type.

        The driver writes an object of the class, or of a subclass, to a column
        of this type. It reads one attribute for each field of the type. A
        missing attribute is an error. Other attributes are ignored.

        The driver reads a value of this type as an object of the class. It
        calls ``cls(**fields)`` with one keyword argument for each field of the
        type. A null field gives ``None``.

        The attribute names and the keyword arguments must be the same as the
        field names in the schema.

        The class takes precedence over a reader for ``CqlUserDefinedType``.

        If the type already has a class, the new class replaces it.

        Parameters
        ----------
        keyspace : str
            The keyspace of the type, as in the schema.
        name : str
            The name of the type, as in the schema.
        cls : type
            The class of the values.

        Raises
        ------
        TypeError
            If ``cls`` is not a class.
        """
