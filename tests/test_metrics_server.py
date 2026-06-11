"""Tests for the operator Prometheus metrics server."""

from __future__ import annotations

import pytest

from src.metrics_server import (
    _format_prometheus,
    _leader_elector_ref,
    dec,
    get_metrics,
    inc,
    observe_histogram,
    reset_metrics,
    set_gauge,
    set_leader_elector,
    start_metrics_server,
    stop_metrics_server,
)


@pytest.fixture(autouse=True)
def _reset():
    """Reset metrics before each test."""
    reset_metrics()
    yield
    reset_metrics()


# ── Counter / Gauge helpers ──


def test_inc_counter() -> None:
    inc("checks_total")
    assert get_metrics()["checks_total"]["value"] == 1


def test_inc_counter_by_value() -> None:
    inc("checks_total", 5)
    assert get_metrics()["checks_total"]["value"] == 5


def test_inc_gauge() -> None:
    inc("active_monitored_deployments")
    assert get_metrics()["active_monitored_deployments"]["value"] == 1


def test_dec_gauge() -> None:
    inc("active_monitored_deployments", 3)
    dec("active_monitored_deployments")
    assert get_metrics()["active_monitored_deployments"]["value"] == 2


def test_dec_gauge_floor_zero() -> None:
    """Gauge cannot go below 0."""
    dec("active_monitored_deployments", 5)
    assert get_metrics()["active_monitored_deployments"]["value"] == 0


def test_set_gauge() -> None:
    set_gauge("active_violations", 7)
    assert get_metrics()["active_violations"]["value"] == 7


def test_set_gauge_overwrite() -> None:
    set_gauge("active_violations", 3)
    set_gauge("active_violations", 0)
    assert get_metrics()["active_violations"]["value"] == 0


def test_inc_unknown_metric_noop() -> None:
    """Incrementing an unknown metric is a no-op."""
    inc("nonexistent_metric")


def test_dec_counter_noop() -> None:
    """Decrementing a counter is a no-op (only gauges support dec)."""
    inc("checks_total", 5)
    dec("checks_total")  # counter — should be no-op
    assert get_metrics()["checks_total"]["value"] == 5


# ── Histogram ──


def test_observe_histogram() -> None:
    observe_histogram("check_duration_seconds", 0.5)
    observe_histogram("check_duration_seconds", 1.5)
    m = get_metrics()["check_duration_seconds"]
    assert m["count"] == 2
    assert m["sum"] == 2.0


# ── Prometheus exposition format ──


def test_format_prometheus_counters() -> None:
    inc("checks_total", 10)
    inc("checks_failed_total", 2)
    output = _format_prometheus()
    assert "self_healing_checks_total" in output
    assert "self_healing_checks_failed_total" in output
    assert "# TYPE self_healing_checks_total counter" in output


def test_format_prometheus_gauges() -> None:
    set_gauge("active_violations", 3)
    output = _format_prometheus()
    assert "self_healing_active_violations" in output
    assert "# TYPE self_healing_active_violations gauge" in output


def test_format_prometheus_histogram() -> None:
    observe_histogram("check_duration_seconds", 0.42)
    output = _format_prometheus()
    assert "self_healing_check_duration_seconds_count" in output
    assert "self_healing_check_duration_seconds_sum" in output
    assert "# TYPE self_healing_check_duration_seconds histogram" in output


def test_format_prometheus_uptime() -> None:
    output = _format_prometheus()
    assert "self_healing_uptime_seconds" in output
    assert "# TYPE self_healing_uptime_seconds gauge" in output


# ── Leader election metrics (Sprint 4) ──


def test_leader_status_gauge() -> None:
    set_gauge("leader_status", 1)
    assert get_metrics()["leader_status"]["value"] == 1
    output = _format_prometheus()
    assert "self_healing_leader_status" in output
    assert "# TYPE self_healing_leader_status gauge" in output


def test_leader_transitions_counter() -> None:
    inc("leader_transitions_total", 3)
    assert get_metrics()["leader_transitions_total"]["value"] == 3
    output = _format_prometheus()
    assert "self_healing_leader_transitions_total" in output
    assert "# TYPE self_healing_leader_transitions_total counter" in output


# ── Ready handler with leader elector (Sprint 4) ──


@pytest.mark.asyncio
async def test_ready_handler_with_leader() -> None:
    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer

    from src.metrics_server import ready_handler

    # Mock elector that reports as leader
    mock_elector = type("MockElector", (), {
        "get_status": lambda self: {
            "enabled": True,
            "is_leader": True,
            "identity": "pod-1",
        },
    })()
    set_leader_elector(mock_elector)

    app = web.Application()
    app.router.add_get("/ready", ready_handler)

    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/ready")
        assert resp.status == 200
        body = await resp.json()
        assert body["status"] == "ready"
        assert body["leader_election"] == "leader"
        assert body["identity"] == "pod-1"

    # Clean up
    set_leader_elector(None)


@pytest.mark.asyncio
async def test_ready_handler_with_standby() -> None:
    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer

    from src.metrics_server import ready_handler

    mock_elector = type("MockElector", (), {
        "get_status": lambda self: {
            "enabled": True,
            "is_leader": False,
            "identity": "pod-2",
        },
    })()
    set_leader_elector(mock_elector)

    app = web.Application()
    app.router.add_get("/ready", ready_handler)

    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/ready")
        assert resp.status == 200
        body = await resp.json()
        assert body["status"] == "standby"
        assert body["leader_election"] == "standby"

    set_leader_elector(None)


@pytest.mark.asyncio
async def test_ready_handler_leader_election_disabled() -> None:
    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer

    from src.metrics_server import ready_handler

    mock_elector = type("MockElector", (), {
        "get_status": lambda self: {
            "enabled": False,
        },
    })()
    set_leader_elector(mock_elector)

    app = web.Application()
    app.router.add_get("/ready", ready_handler)

    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/ready")
        assert resp.status == 200
        body = await resp.json()
        assert body["status"] == "ready"
        assert body["leader_election"] == "disabled"

    set_leader_elector(None)


# ── HTTP handlers (integration with aiohttp test client) ──


@pytest.mark.asyncio
async def test_metrics_handler() -> None:
    from aiohttp import web
    from aiohttp.test_utils import AioHTTPTestCase, TestClient, TestServer

    from src.metrics_server import metrics_handler

    app = web.Application()
    app.router.add_get("/metrics", metrics_handler)

    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/metrics")
        assert resp.status == 200
        text = await resp.text()
        assert "self_healing_checks_total" in text
        assert "text/plain" in resp.content_type


@pytest.mark.asyncio
async def test_health_handler() -> None:
    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer

    from src.metrics_server import health_handler

    app = web.Application()
    app.router.add_get("/health", health_handler)

    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/health")
        assert resp.status == 200
        body = await resp.json()
        assert body["status"] == "healthy"
        assert "uptime_seconds" in body
        assert "checks_total" in body


@pytest.mark.asyncio
async def test_ready_handler() -> None:
    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer

    from src.metrics_server import ready_handler

    app = web.Application()
    app.router.add_get("/ready", ready_handler)

    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/ready")
        assert resp.status == 200
        body = await resp.json()
        assert body["status"] == "ready"


# ── Server lifecycle ──


@pytest.mark.asyncio
async def test_start_stop_metrics_server() -> None:
    """Start and stop the metrics server on a random available port."""
    import asyncio
    import httpx

    await start_metrics_server(host="127.0.0.1", port=0)
    # Find the actual port (aiohttp picks one when port=0, but we specified 0)
    # For simplicity, use a fixed high port
    await stop_metrics_server()

    # Start on a fixed high port
    await start_metrics_server(host="127.0.0.1", port=19190)
    async with httpx.AsyncClient() as client:
        resp = await client.get("http://127.0.0.1:19190/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "healthy"

        resp = await client.get("http://127.0.0.1:19190/metrics")
        assert resp.status_code == 200
        assert "self_healing_" in resp.text

        resp = await client.get("http://127.0.0.1:19190/ready")
        assert resp.status_code == 200

    await stop_metrics_server()
