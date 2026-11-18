"""Reads `system.clients`, the table in which ScyllaDB shows its client connections."""

import pytest
from scylla.policies.load_balancing import SingleTargetPolicy
from scylla.results import DictRowFactory
from scylla.session import ExecutionProfile, Session
from scylla.statement import Statement


async def own_client_options(session: Session) -> list[dict[str, str]]:
    """Returns the `client_options` of the connections of `session`.

    Each node shows only the connections to that node in `system.clients`.
    Thus, this reads `system.clients` on each node. It finds the rows of
    `session` by the `SESSION_ID` option.

    Skips the test if `system.clients` has no `client_options` column.
    """
    result = await session.execute(
        "SELECT column_name FROM system_schema.columns "
        "WHERE keyspace_name = 'system' AND table_name = 'clients' AND column_name = 'client_options'"
    )
    if await result.first_row() is None:
        pytest.skip("system.clients has no client_options column")

    session_id = str(session.session_id)
    own_options: list[dict[str, str]] = []
    for node in session.cluster_state.nodes_info.values():
        statement = Statement("SELECT client_options FROM system.clients")
        statement.execution_profile = ExecutionProfile(
            row_factory=DictRowFactory(),
            load_balancing_policy=SingleTargetPolicy(node),
        )
        result = await session.execute(statement)
        for row in await result.all():
            options: dict[str, str] | None = row["client_options"]
            if options is not None and options.get("SESSION_ID") == session_id:
                own_options.append(options)
    return own_options
