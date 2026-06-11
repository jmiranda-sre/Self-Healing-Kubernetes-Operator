"""Prometheus metrics server — exposes operator telemetry on /metrics and /health."""

from __future__ import annotations

import asyncio
import time
from typing import Any

import structlog
from aiohttp import web

logger = structlog.get_logger()

# ── Metric registries (lightweight — no prometheus_client dependency) ──

_metrics: dict[str, dict[str, Any]] = {}
_start_time: float = time.time()


def _init_metrics() -> None:
    """Initialize metric registries with default values."""
    global _metrics, _start_time
    _start_time = time.time()
    _metrics = {
        "checks_total": {"type": "counter", "value": 0, "labels": {"operator": "self-healing"}},
        "checks_failed_total": {"type": "counter", "value": 0, "labels": {"operator": "self-healing"}},
        "remediations_total": {"type": "counter", "value": 0, "labels": {"operator": "self-healing"}},
        "remediations_failed_total": {"type": "counter", "value": 0, "labels": {"operator": "self-healing"}},
        "istio_ejections_total": {"type": "counter", "value": 0, "labels": {"operator": "self-healing"}},
        "istio_ejections_failed_total": {"type": "counter", "value": 0, "labels": {"operator": "self-healing"}},
        "tickets_created_total": {"type": "counter", "value": 0, "labels": {"operator": "self-healing"}},
        "tickets_failed_total": {"type": "counter", "value": 0, "labels": {"operator": "self-healing"}},
        "active_monitored_deployments": {"type": "gauge", "value": 0, "labels": {"operator": "self-healing"}},
        "active_violations": {"type": "gauge", "value": 0, "labels": {"operator": "self-healing"}},
        "leader_status": {"type": "gauge", "value": 0, "labels": {"operator": "self-healing"}},
        "leader_transitions_total": {"type": "counter", "value": 0, "labels": {"operator": "self-healing"}},
        "check_duration_seconds": {"type": "histogram", "value": 0.0, "count": 0, "sum": 0.0, "labels": {"operator": "self-healing"}},
    }


_init_metrics()


# ── Public metric helpers ──


def inc(name: str, value: int | float = 1) -> None:
    """Increment a counter or gauge metric."""
    if name in _metrics:
        _metrics[name]["value"] += value


def dec(name: str, value: int | float = 1) -> None:
    """Decrement a gauge metric."""
    if name in _metrics and _metrics[name]["type"] == "gauge":
        _metrics[name]["value"] = max(0, _metrics[name]["value"] - value)


def set_gauge(name: str, value: int | float) -> None:
    """Set a gauge metric to an absolute value."""
    if name in _metrics and _metrics[name]["type"] == "gauge":
        _metrics[name]["value"] = value


def observe_histogram(name: str, value: float) -> None:
    """Record an observation for a histogram metric."""
    if name in _metrics and _metrics[name]["type"] == "histogram":
        _metrics[name]["count"] += 1
        _metrics[name]["sum"] += value


def get_metrics() -> dict[str, dict[str, Any]]:
    """Return a copy of all metrics (for testing)."""
    return dict(_metrics)


def reset_metrics() -> None:
    """Reset all metrics to defaults (for testing)."""
    _init_metrics()


# ── Prometheus exposition format ──


def _format_prometheus() -> str:
    """Render metrics in Prometheus text exposition format."""
    lines: list[str] = []

    for name, metric in _metrics.items():
        mtype = metric["type"]
        labels_str = ",".join(f'{k}="{v}"' for k, v in metric.get("labels", {}).items())
        label_suffix = f"{{{labels_str}}}" if labels_str else ""

        # TYPE line
        lines.append(f"# TYPE self_healing_{name} {mtype}")

        if mtype == "histogram":
            count = metric.get("count", 0)
            total_sum = metric.get("sum", 0.0)
            # Simplified: no buckets, just _count and _sum
            lines.append(f"self_healing_{name}_count{label_suffix} {count}")
            lines.append(f"self_healing_{name}_sum{label_suffix} {total_sum}")
        else:
            lines.append(f"self_healing_{name}{label_suffix} {metric['value']}")

    # Uptime gauge
    uptime = time.time() - _start_time
    lines.append("# TYPE self_healing_uptime_seconds gauge")
    lines.append(f'self_healing_uptime_seconds{{operator="self-healing"}} {uptime:.2f}')

    return "\n".join(lines) + "\n"


# ── HTTP handlers ──


async def metrics_handler(request: web.Request) -> web.Response:  # noqa: ARG001
    """Handle GET /metrics — Prometheus scrape endpoint."""
    return web.Response(
        text=_format_prometheus(),
        content_type="text/plain; version=0.0.4; charset=utf-8",
    )


async def health_handler(request: web.Request) -> web.Response:  # noqa: ARG001
    """Handle GET /health — liveness/readiness probe."""
    uptime = time.time() - _start_time
    body = {
        "status": "healthy",
        "uptime_seconds": round(uptime, 2),
        "checks_total": int(_metrics.get("checks_total", {}).get("value", 0)),
        "remediations_total": int(_metrics.get("remediations_total", {}).get("value", 0)),
    }
    return web.json_response(body)


async def ready_handler(request: web.Request) -> web.Response: # noqa: ARG001
    """Handle GET /ready — readiness probe (reflects leader election status)."""
    if _leader_elector_ref is not None:
        status = _leader_elector_ref.get_status()
        if not status.get("enabled", True):
            # Leader election disabled — always ready
            return web.json_response({"status": "ready", "leader_election": "disabled"})
        if status.get("is_leader", False):
            return web.json_response({
                "status": "ready",
                "leader_election": "leader",
                "identity": status.get("identity", ""),
            })
        return web.json_response({
            "status": "standby",
            "leader_election": "standby",
            "identity": status.get("identity", ""),
            "lease_holder": "another_replica",
        }, status=200)  # 200 — standby is healthy, just not active
    return web.json_response({"status": "ready"})


# ── Leader election integration ──
# The operator sets this reference after creating the LeaderElector
_leader_elector_ref: Any = None


def set_leader_elector(elector: Any) -> None:
    """Set the leader elector reference for /ready probe integration."""
    global _leader_elector_ref
    _leader_elector_ref = elector


# ── Server lifecycle ──


_app: web.Application | None = None
_runner: web.AppRunner | None = None


async def start_metrics_server(host: str = "0.0.0.0", port: int = 9090) -> None:
    """Start the metrics HTTP server as a background asyncio task."""
    global _app, _runner

    _app = web.Application()
    _app.router.add_get("/metrics", metrics_handler)
    _app.router.add_get("/health", health_handler)
    _app.router.add_get("/ready", ready_handler)

    _runner = web.AppRunner(_app)
    await _runner.setup()
    site = web.TCPSite(_runner, host, port)
    await site.start()

    logger.info("metrics_server.started", host=host, port=port)


async def stop_metrics_server() -> None:
    """Gracefully stop the metrics HTTP server."""
    global _runner, _app
    if _runner:
        await _runner.cleanup()
        _runner = None
        _app = None
        logger.info("metrics_server.stopped")
