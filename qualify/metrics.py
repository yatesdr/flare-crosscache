"""Prometheus metrics for Flare replicas.

Flare's qualify Run helper requests `/metrics` with `Accept: application/json`, but the
server negotiates that to the JSON snapshot and the helper then parses the body as
Prometheus text. Read the endpoint explicitly in its text exposition format instead.
"""

from __future__ import annotations

import http.client


def patch(run) -> None:
    """Replace `run.metrics(name)` with a text-format read of the same endpoint."""
    if not run.args.require_metrics:
        return

    def metrics(_name: str, *, wait_idle: bool = True) -> dict[str, int]:
        url = run.url
        connection = http.client.HTTPConnection(url.hostname, url.port,
                                                timeout=run.args.timeout)
        try:
            connection.request("GET", url.path.rstrip("/") + "/metrics",
                               headers={"Accept": "text/plain"})
            response = connection.getresponse()
            body = response.read(1 << 20)
            status = response.status
        finally:
            connection.close()
        if status != 200:
            raise ValueError("required native /metrics endpoint failed")
        values: dict[str, int] = {}
        for line in body.decode().splitlines():
            if line and not line.startswith("#"):
                key, value = line.rsplit(" ", 1)
                values[key] = int(value)
        return values

    run.metrics = metrics
