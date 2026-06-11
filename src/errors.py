"""Hierarchical error classes for the Self-Healing Operator."""

from __future__ import annotations


class OperatorError(Exception):
    """Base error — all operator errors inherit from this."""

    status_code: int = 500
    retryable: bool = False
    log_level: str = "error"

    def __init__(self, message: str, context: dict | None = None) -> None:
        self.message = message
        self.context = context or {}
        super().__init__(message)


class MetricsUnavailableError(OperatorError):
    """Prometheus query failed or returned unexpected data."""

    status_code = 502
    retryable = True
    log_level = "warn"


class MetricsQueryError(OperatorError):
    """PromQL syntax or execution error."""

    status_code = 400
    retryable = False
    log_level = "warn"


class PodIsolationError(OperatorError):
    """Failed to isolate (cordon/label) a Pod."""

    status_code = 500
    retryable = True
    log_level = "error"


class LogDumpError(OperatorError):
    """Failed to collect or persist Pod logs."""

    status_code = 500
    retryable = True
    log_level = "error"


class ScalingError(OperatorError):
    """Failed to scale the Deployment."""

    status_code = 500
    retryable = True
    log_level = "error"


class TicketCreationError(OperatorError):
    """Failed to create a ticket via webhook."""

    status_code = 502
    retryable = True
    log_level = "error"


class CooldownActiveError(OperatorError):
    """Remediation skipped because cooldown window is active."""

    status_code = 429
    retryable = False
    log_level = "info"


class MaxReplicasError(OperatorError):
    """Cannot scale beyond configured max_replicas."""

    status_code = 409
    retryable = False
    log_level = "warn"


class IstioEjectionError(OperatorError):
    """Failed to configure Istio outlierDetection or VirtualService ejection."""

    status_code = 500
    retryable = True
    log_level = "error"


class IstioRecoveryError(OperatorError):
    """Failed to remove Istio ejection config during recovery."""

    status_code = 500
    retryable = True
    log_level = "warn"


class LeaderElectionError(OperatorError):
    """Failed to acquire, renew, or release the leader Lease."""

    status_code = 500
    retryable = True
    log_level = "error"
