import shutil
import socket
import time
from pathlib import Path
from typing import Any, Protocol, cast

from ccmlib.scylla_cluster import ScyllaCluster  # pyright: ignore[reportMissingTypeStubs]

CCM_DIR = Path(__file__).resolve().parent.parent / "ccm"


class _CCMNode(Protocol):
    network_interfaces: dict[str, tuple[str, int] | None]


class _CCMCluster(Protocol):
    def set_configuration_options(
        self, values: dict[str, Any] | None = None, batch_commitlog: Any | None = None
    ) -> Any: ...
    def populate(
        self,
        nodes: list[int] | int,
        debug: bool = False,
        tokens: Any | None = None,
        use_vnodes: bool = False,
        ipprefix: Any | None = None,
        ipformat: Any | None = None,
    ) -> Any: ...
    def start(
        self,
        no_wait: bool = False,
        verbose: bool = False,
        wait_for_binary_proto: bool | None = None,
        wait_other_notice: bool | None = None,
        jvm_args: Any | None = None,
        profile_options: Any | None = None,
        quiet_start: bool = False,
    ) -> Any: ...
    def stop(self, *args: Any, **kwargs: Any) -> Any: ...
    def remove(self, *args: Any, **kwargs: Any) -> Any: ...
    def nodelist(self) -> list[_CCMNode]: ...


def wait_for_socket(host: str, port: int, timeout: float = 60.0) -> None:
    deadline = time.time() + timeout

    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1.0):
                return
        except OSError:
            time.sleep(0.2)

    raise TimeoutError(f"Timed out waiting for socket {host}:{port}")


def create_scylla_cluster(
    name: str,
    *,
    scylla_version: str,
    nodes: int = 1,
    config: dict[str, Any] | None = None,
    ipprefix: str = "127.0.0.",
) -> _CCMCluster:
    CCM_DIR.mkdir(parents=True, exist_ok=True)

    cluster_path = CCM_DIR / name
    if cluster_path.exists():
        shutil.rmtree(cluster_path)

    cluster = cast(
        _CCMCluster,
        ScyllaCluster(str(CCM_DIR), name, cassandra_version=scylla_version),
    )

    merged_config: dict[str, Any] = {
        "start_native_transport": True,
    }
    if config:
        merged_config.update(config)

    cluster.set_configuration_options(merged_config)
    cluster.populate([nodes], ipprefix=ipprefix)

    return cluster


def _get_binary_interface(node: _CCMNode) -> tuple[str, int]:
    interface = node.network_interfaces.get("binary")
    if interface is None:
        raise RuntimeError("Node has no binary interface configured")
    return interface


def start_cluster(cluster: _CCMCluster, timeout: float = 120.0) -> None:
    cluster.start(wait_for_binary_proto=True, wait_other_notice=True)

    for node in cluster.nodelist():
        host, port = _get_binary_interface(node)
        wait_for_socket(host, port, timeout)


def get_contact_points(cluster: _CCMCluster) -> list[tuple[str, int]]:
    points: list[tuple[str, int]] = []

    for node in cluster.nodelist():
        host, port = _get_binary_interface(node)
        points.append((host, port))

    return points


def stop_and_remove_cluster(cluster: _CCMCluster) -> None:
    try:
        cluster.stop()
    finally:
        cluster.remove()
