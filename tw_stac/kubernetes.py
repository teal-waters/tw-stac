"""Functions to deal with kubernetes."""

from functools import cache
from typing import Tuple

from kubernetes import client
from kubernetes import config
from kubernetes.stream import stream
from kubernetes.stream.ws_client import WSResponse

from tw_stac.config import APP
from tw_stac.config import NAMESPACE


@cache
def _api() -> client.CoreV1Api:
    config.load_kube_config()
    return client.CoreV1Api()


def get_stac_pod_name(namespace: str = NAMESPACE, app: str = APP) -> Tuple[str, str]:
    """Get the name and namespace of the pod on which we should run stac-related commands.

    Args:
        namespace: The namespace where the pod lives.
        app: The app name.

    Returns:
        The name and namespace of the pod.
    """
    podlist = _api().list_namespaced_pod(namespace, label_selector=f"app={app}").items
    if len(podlist) == 0:
        raise Exception("Pod not found")
    else:
        return podlist[0].metadata.name, namespace  # pyright: ignore


def run_command_on_stac_pod(command: str, **kwargs: str) -> str:
    """Run a command on the stac pod.

    Args:
        command: The command to run.
        kwargs: Additional arguments to :py:func:`get_stac_pod_name`.
    """
    pod_name, namespace = get_stac_pod_name(**kwargs)
    return run_command_on_pod(command=command, pod_name=pod_name, namespace=namespace)


def run_command_on_pod(command: str, pod_name: str, namespace: str = NAMESPACE) -> None:
    """Run a command on a pod.

    Args:
        command: The command to run.
        pod_name:  The name of the pod to run it on.
        namespace: The namespace containing the pod.
    """
    resp: WSResponse = stream(
        _api().connect_get_namespaced_pod_exec,
        pod_name,
        namespace,
        command=["bash", "-c", command],
        stderr=True,
        stdin=False,
        stdout=True,
        tty=False,
        _preload_content=False,
    )

    # Read output as it arrives
    while resp.is_open():
        resp.update(timeout=1)
        if resp.peek_stdout():
            print(resp.read_stdout(), end="")
        if resp.peek_stderr():
            print(resp.read_stderr(), end="")
