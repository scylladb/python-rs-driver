"""Session construction shared by the test suite.

Every test that talks to a cluster goes through here, so the connection
details and the row factory the assertions expect live in one place.
"""

from scylla.execution_profile import ExecutionProfile
from scylla.results import DictRowFactory
from scylla.session import Session
from scylla.session_builder import SessionBuilder

CONTACT_POINTS = [("127.0.0.2", 9042)]


def session_builder() -> SessionBuilder:
    """A builder pointed at the test cluster, yielding `dict` rows.

    The suite asserts on dict-shaped rows, so the default execution profile
    carries `DictRowFactory` rather than the driver's namedtuple default.
    Tests needing further configuration chain onto the returned builder.
    """
    return (
        SessionBuilder()
        .contact_points(CONTACT_POINTS)
        .execution_profile(ExecutionProfile(row_factory=DictRowFactory()))
    )


async def connect() -> Session:
    """Connects to the test cluster with the suite's default configuration."""
    return await session_builder().connect()
