"""Tests for the config module (including Sprint 2 additions)."""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest

from src.config import (
    IstioConfig,
    LeaderElectionConfig,
    MetricsConfig,
    MetricsServerConfig,
    OperatorConfig,
    RemediationConfig,
    TicketConfig,
    load_config,
)


def test_default_config() -> None:
    config = OperatorConfig()
    assert config.metrics.prometheus_url == "http://prometheus:9090"
    assert config.remediation.enable_pod_isolation is True
    assert config.remediation.max_replicas == 10
    assert config.istio.enabled is False
    assert config.istio.consecutive_5xx_errors == 3
    assert config.istio.interval_seconds == "30s"
    assert config.istio.base_ejection_time_seconds == "120s"
    assert config.istio.enable_virtual_service_eject is True
    assert config.ticket.retry_max_attempts == 3
    assert config.dry_run is False
    assert config.metrics_server.port == 9090
    assert config.metrics_server.host == "0.0.0.0"
    assert config.watch_namespace == ""


def test_env_override_metrics_server() -> None:
    with patch.dict(os.environ, {"METRICS_SERVER_PORT": "8080", "METRICS_SERVER_HOST": "127.0.0.1"}):
        cfg = MetricsServerConfig()
        assert cfg.port == 8080
        assert cfg.host == "127.0.0.1"


def test_env_override_watch_namespace() -> None:
    with patch.dict(os.environ, {"WATCH_NAMESPACE": "production"}):
        cfg = OperatorConfig()
        assert cfg.watch_namespace == "production"


def test_env_override_prometheus_url() -> None:
    with patch.dict(os.environ, {"PROMETHEUS_URL": "http://custom-prom:9091"}):
        cfg = MetricsConfig()
        assert cfg.prometheus_url == "http://custom-prom:9091"


def test_env_override_dry_run() -> None:
    with patch.dict(os.environ, {"DRY_RUN": "true"}):
        cfg = OperatorConfig()
        assert cfg.dry_run is True


def test_env_override_disabled_remediation() -> None:
    with patch.dict(os.environ, {"ENABLE_POD_ISOLATION": "false", "ENABLE_H_SCALING": "false"}):
        cfg = RemediationConfig()
        assert cfg.enable_pod_isolation is False
        assert cfg.enable_h_scaling is False


def test_env_override_istio() -> None:
    with patch.dict(os.environ, {
        "ISTIO_ENABLED": "true",
        "ISTIO_CONSECUTIVE_5XX_ERRORS": "5",
        "ISTIO_INTERVAL_SECONDS": "15s",
        "ISTIO_BASE_EJECTION_TIME": "300s",
    }):
        cfg = IstioConfig()
        assert cfg.enabled is True
        assert cfg.consecutive_5xx_errors == 5
        assert cfg.interval_seconds == "15s"
        assert cfg.base_ejection_time_seconds == "300s"


def test_env_override_ticket_retry() -> None:
    with patch.dict(os.environ, {"TICKET_RETRY_MAX_ATTEMPTS": "5"}):
        cfg = TicketConfig()
        assert cfg.retry_max_attempts == 5


def test_env_override_ticket_auth() -> None:
    with patch.dict(os.environ, {
        "TICKET_AUTH_EMAIL": "bot@example.com",
        "TICKET_AUTH_TOKEN": "secret-token",
    }):
        cfg = TicketConfig()
        assert cfg.auth_email == "bot@example.com"
        assert cfg.auth_token == "secret-token"


def test_load_config_logs() -> None:
    config = load_config()
    assert isinstance(config, OperatorConfig)
    assert isinstance(config.istio, IstioConfig)
    assert isinstance(config.ticket, TicketConfig)
    assert isinstance(config.metrics_server, MetricsServerConfig)
