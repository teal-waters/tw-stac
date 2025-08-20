"""Tools to deploy stac collections."""

from tw_stac.kubernetes import run_command_on_stac_pod


def install_psycopg_on_stac_pod(**kwargs: str) -> None:
    """Installs psycopg on the stac pod.

    Args:
      kwargs: additional arguments to `run_command_on_stac_pod`.
    """
    command = """apt update -y \
        && apt install python3 python3-pip -y \
        && pip install pypgstac[psycopg]"""
    run_command_on_stac_pod(command, **kwargs)
