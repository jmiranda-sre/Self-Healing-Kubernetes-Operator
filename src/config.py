"""Centralized configuration with env-var overrides and validation."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

import structlog

logger = structlog.get_logger()

PROJECT_ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class MetricsConfig:
    prometheus_url: str = os.getenv("PROMETHEUS_URL", "http://prometheus:9090")
    query_timeout: float = float(os.getenv("PROMETHEUS_QUERY_TIMEOUT", "10"))
    check_interval: int = int(os.getenv("CHECK_INTERVAL_SECONDS", "30"))


@dataclass(frozen=True)
class RemediationConfig:
    enable_pod_isolation: bool = os.getenv("ENABLE_POD_ISOLATION", "true").lower() == "true"
    enable_log_dump: bool = os.getenv("ENABLE_LOG_DUMP", "true").lower() == "true"
    enable_ticket_creation: bool = os.getenv("ENABLE_TICKET_CREATION", "true").lower() == "true"
    enable_h_scaling: bool = os.getenv("ENABLE_H_SCALING", "true").lower() == "true"
    scale_up_replicas: int = int(os.getenv("SCALE_UP_REPLICAS", "1"))
    max_replicas: int = int(os.getenv("MAX_REPLICAS", "10"))
    cooldown_seconds: int = int(os.getenv("COOLDOWN_SECONDS", "120"))
    log_dump_path: str = os.getenv("LOG_DUMP_PATH", "/var/log/self-healing")


@dataclass(frozen=True)
class IstioConfig:
    enabled: bool = os.getenv("ISTIO_ENABLED", "false").lower() == "true"
    consecutive_5xx_errors: int = int(os.getenv("ISTIO_CONSECUTIVE_5XX_ERRORS", "3"))
    interval_seconds: str = os.getenv("ISTIO_INTERVAL_SECONDS", "30s")
    base_ejection_time_seconds: str = os.getenv("ISTIO_BASE_EJECTION_TIME", "120s")
    enable_virtual_service_eject: bool = os.getenv("ISTIO_ENABLE_VS_EJECT", "true").lower() == "true"


@dataclass(frozen=True)
class TicketConfig:
    webhook_url: str = os.getenv("TICKET_WEBHOOK_URL", "")
    webhook_secret: str = os.getenv("TICKET_WEBHOOK_SECRET", "")
    auth_email: str = os.getenv("TICKET_AUTH_EMAIL", "")
    auth_token: str = os.getenv("TICKET_AUTH_TOKEN", "")
    timeout: float = float(os.getenv("TICKET_WEBHOOK_TIMEOUT", "10"))
    retry_max_attempts: int = int(os.getenv("TICKET_RETRY_MAX_ATTEMPTS", "3"))


@dataclass(frozen=True)
class MetricsServerConfig:
    host: str = os.getenv("METRICS_SERVER_HOST", "0.0.0.0")
    port: int = int(os.getenv("METRICS_SERVER_PORT", "9090"))


@dataclass(frozen=True)
class LeaderElectionConfig:
    enabled: bool = os.getenv("LEADER_ELECT", "false").lower() == "true"
    lease_name: str = os.getenv("LEADER_ELECTION_LEASE_NAME", "self-healing-operator")
    namespace: str = os.getenv("LEADER_ELECTION_NAMESPACE", os.getenv("OPERATOR_NAMESPACE", ""))
    lease_duration_seconds: int = int(os.getenv("LEADER_ELECTION_DURATION_SECONDS", "15"))
    renewal_interval_seconds: float = float(os.getenv("LEADER_ELECTION_RENEW_INTERVAL_SECONDS", "5.0"))


@dataclass(frozen=True)
class OperatorConfig:
    metrics: MetricsConfig = field(default_factory=MetricsConfig)
    remediation: RemediationConfig = field(default_factory=RemediationConfig)
    istio: IstioConfig = field(default_factory=IstioConfig)
    ticket: TicketConfig = field(default_factory=TicketConfig)
    metrics_server: MetricsServerConfig = field(default_factory=MetricsServerConfig)
    leader_election: LeaderElectionConfig = field(default_factory=LeaderElectionConfig)
    log_level: str = os.getenv("LOG_LEVEL", "info")
    dry_run: bool = os.getenv("DRY_RUN", "false").lower() == "true"
    watch_namespace: str = os.getenv("WATCH_NAMESPACE", "")


def load_config() -> OperatorConfig:
    cfg = OperatorConfig()
    logger.info(
        "config.loaded",
        prometheus_url=cfg.metrics.prometheus_url,
        dry_run=cfg.dry_run,
        pod_isolation=cfg.remediation.enable_pod_isolation,
        log_dump=cfg.remediation.enable_log_dump,
        ticket=cfg.remediation.enable_ticket_creation,
        h_scaling=cfg.remediation.enable_h_scaling,
        istio_enabled=cfg.istio.enabled,
        istio_consecutive_5xx=cfg.istio.consecutive_5xx_errors,
        ticket_retry=cfg.ticket.retry_max_attempts,
        metrics_server_port=cfg.metrics_server.port,
        watch_namespace=cfg.watch_namespace or "(all)",
        leader_election=cfg.leader_election.enabled,
        lease_name=cfg.leader_election.lease_name,
    )
    return cfg
