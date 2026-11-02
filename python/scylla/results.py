from collections.abc import Callable
from typing import Any, TypeAlias

from ._rust.cluster.metadata import ColumnSpec  # pyright: ignore[reportMissingModuleSource]
from ._rust.results import (  # pyright: ignore[reportMissingModuleSource]
    AsyncPagesIterator,
    AsyncRowsIterator,
    ClassRowFactory,
    DictRowFactory,
    NamedTupleRowFactory,
    Page,
    PagingState,
    RequestResult,
    RowFactory,
    SinglePageIterator,
    TupleRowFactory,
)

RowBuilder: TypeAlias = Callable[[tuple[Any, ...]], Any]
"""
Builds a single row from its column values, in column order.

Without adapters, each value is a ``CqlValue``. A registered reader or user
defined type class can give a value of any type.
"""


RowFactoryLike: TypeAlias = RowFactory | RowBuilder
"""Anything accepted as `factory=`: a `RowFactory` instance, or a bare row builder."""


__all__ = [
    "AsyncPagesIterator",
    "AsyncRowsIterator",
    "ClassRowFactory",
    "ColumnSpec",
    "DictRowFactory",
    "NamedTupleRowFactory",
    "Page",
    "PagingState",
    "RequestResult",
    "RowBuilder",
    "RowFactory",
    "RowFactoryLike",
    "SinglePageIterator",
    "TupleRowFactory",
]
