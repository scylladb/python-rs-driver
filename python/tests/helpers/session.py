"""Session construction shared by the test suite.

Every test that talks to a cluster goes through here, so the connection
details, the row factory and the adapters the assertions expect live in one
place.
"""

from scylla.adapters import Adapters
from scylla.cql_types import CqlUserDefinedType
from scylla.results import DictRowFactory
from scylla.session import ExecutionProfile, Session, SessionBuilder

CONTACT_POINTS = [("127.0.0.2", 9042)]


def suite_adapters() -> Adapters:
    """Adapters that read every user defined type as a `dict`.

    The suite asserts on dict-shaped UDT values, as it does on dict rows.
    Tests that need their own adapters start from these.
    """
    adapters = Adapters()
    adapters.register_reader(CqlUserDefinedType, dict)
    return adapters


def session_builder() -> SessionBuilder:
    """A builder pointed at the test cluster, yielding `dict` rows and UDTs.

    The suite asserts on dict-shaped rows, so the default execution profile
    carries `DictRowFactory` rather than the driver's namedtuple default. For
    the same reason, user defined types are read as `dict`. Tests needing
    further configuration chain onto the returned builder.
    """
    return (
        SessionBuilder()
        .contact_points(CONTACT_POINTS)
        .execution_profile(ExecutionProfile(row_factory=DictRowFactory()))
        .adapters(suite_adapters())
    )


async def connect() -> Session:
    """Connects to the test cluster with the suite's default configuration."""
    return await session_builder().connect()
