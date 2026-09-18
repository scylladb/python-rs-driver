from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Protocol, TypeAlias, runtime_checkable

from ._rust.cluster.metadata import ColumnSpec  # pyright: ignore[reportMissingModuleSource]
from ._rust.results import (  # pyright: ignore[reportMissingModuleSource]
    AsyncRowsIterator,
    ClassRowFactory,
    DictRowFactory,
    NamedTupleRowFactory,
    PagingState,
    RequestResult,
    SinglePageIterator,
    TupleRowFactory,
)

if TYPE_CHECKING:
    from ._rust.results import CqlValue  # pyright: ignore[reportMissingModuleSource]

RowBuilder: TypeAlias = Callable[[tuple["CqlValue", ...]], Any]
"""Builds a single row from its column values, in column order."""


@runtime_checkable
class RowFactory(Protocol):
    """
    Row factory resolved once per request, before any row is built.

    `prepare` receives the column metadata of the result and returns the
    callable used to build every row of every page, so work that depends only
    on the columns - a namedtuple class, a name lookup table, validation of a
    target type - is done once rather than per row.

    A bare callable is also accepted wherever a factory is: it is used as the
    builder directly, skipping the `prepare` step.
    """

    def prepare(self, columns: tuple[ColumnSpec, ...]) -> RowBuilder: ...


BuiltinRowFactory: TypeAlias = NamedTupleRowFactory | DictRowFactory | TupleRowFactory | ClassRowFactory
"""A factory the driver recognizes by type and builds rows for itself.

Unlike a `RowFactory`, these are opaque: they carry no `prepare` step to call
or delegate to.
"""

RowFactoryLike: TypeAlias = BuiltinRowFactory | RowFactory | RowBuilder
"""Anything accepted as `factory=`: a built-in, a `RowFactory`, or a bare row builder."""


__all__ = [
    "AsyncRowsIterator",
    "BuiltinRowFactory",
    "ClassRowFactory",
    "DictRowFactory",
    "NamedTupleRowFactory",
    "PagingState",
    "RequestResult",
    "RowBuilder",
    "RowFactory",
    "RowFactoryLike",
    "SinglePageIterator",
    "TupleRowFactory",
]
