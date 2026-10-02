from collections.abc import Mapping
from ipaddress import IPv4Address, IPv6Address
from uuid import UUID

from scylla.cluster import Keyspace
from scylla.routing import ReplicaLocator, Shard, Token

class Node:
    """
    Represents a node in the cluster.
    """

    @property
    def host_id(self) -> UUID:
        """
        Access the host ID of the node.
        """
    @property
    def address(self) -> tuple[IPv4Address | IPv6Address, int]:
        """
        Returns a tuple of (`IPv4Address` | `IPv6Address`, `int`) representing address and port of a Node.
        """
    @property
    def datacenter(self) -> str | None:
        """
        Access the datacenter of the node.
        """
    @property
    def rack(self) -> str | None:
        """
        Access the rack of the node.
        """
    @property
    def nr_shards(self) -> int | None:
        """
        Access the number of shards of the node.
        """
    @property
    def connected(self) -> bool:
        """
        Is `True` if the driver has a connection open to this node,
        `False` otherwise.
        """
    @property
    def enabled(self) -> bool:
        """
        Indicates whether the node is enabled.

        A node is considered enabled if it successfully passes the `Session`'s `HostFilter`.
        If this value is `False`, the driver has decided not to open a connection
        to the node.

        Only enabled nodes will have connections open.
        """

class ClusterState:
    """
    Represents state of the cluster allowing access to known nodes,
    keyspaces, replica locator and token calculation.
    """

    def get_keyspace(self, keyspace: str) -> Keyspace | None:
        """
        Get the keyspace by name.
        """
    @property
    def keyspaces(self) -> Mapping[str, Keyspace]:
        """
        Access the keyspaces of the cluster as a dictionary of
        keyspace name to Keyspace object.
        """
    @property
    def nodes_info(self) -> Mapping[UUID, Node]:
        """
        Access the nodes of the cluster as a dict of host ID to Node objects.
        """
    def compute_token(self, keyspace: str, table: str, partition_key: object) -> Token:
        """
        Computes the token for a given keyspace, table and partition key.

        Raises:
            SerializationError: If a partition key value can't be serialized.
            ClusterStateTokenError: If the token calculation fails otherwise.

        `partition_key` must be a `Sequence` of partition key values or a
        `Mapping` of column names to partition key values.
        """
    def get_token_endpoints(self, keyspace: str, table: str, token: Token) -> list[tuple[Node, Shard]]:
        """
        Returns a list of `[Node, Shard]` tuples that are considered
        to be replicas for a given token.
        """
    def get_endpoints(self, keyspace: str, table: str, partition_key: object) -> list[tuple[Node, Shard]]:
        """
        Returns a list of `[Node, Shard]` tuples that are replicas owning the partition key.

        Raises:
            SerializationError: If a partition key value can't be serialized.
            ClusterStateTokenError: If the token calculation fails otherwise.

        `partition_key` must be a `Sequence` of partition key values or a
        `Mapping` of column names to partition key values.
        """
    @property
    def replica_locator(self) -> ReplicaLocator:
        """
        Access replica location info.
        """
