"""Tests for the remediation module."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

from src.config import IstioConfig, RemediationConfig
from src.errors import CooldownActiveError, MaxReplicasError, PodIsolationError, ScalingError
from src.metrics_adapter import EvaluationResult, RuleSeverity
from src.remediation import (
    _check_cooldown,
    isolate_pod,
    dump_pod_logs,
    scale_deployment,
    execute_remediation,
)


@pytest.fixture
def config() -> RemediationConfig:
    return RemediationConfig(
        enable_pod_isolation=True,
        enable_log_dump=True,
        enable_h_scaling=True,
        scale_up_replicas=1,
        max_replicas=10,
        cooldown_seconds=120,
        log_dump_path="/tmp/test-self-healing",
        enable_ticket_creation=True,
    )


@pytest.fixture
def istio_config() -> IstioConfig:
    return IstioConfig(
        enabled=True,
        consecutive_5xx_errors=3,
        interval_seconds="30s",
        base_ejection_time_seconds="120s",
        enable_virtual_service_eject=True,
    )


@pytest.fixture
def evaluation() -> EvaluationResult:
    return EvaluationResult(
        rule_name="high-5xx-rate",
        violated=True,
        value=10.5,
        threshold=5.0,
        severity=RuleSeverity.CRITICAL,
        pod_name="payment-api-abc123",
        query='sum(rate(http_requests_total{code=~"5.."}[2m]))',
    )


# ── Cooldown tests ──


def test_cooldown_passes_when_no_last_remediation() -> None:
    _check_cooldown(None, 120)  # should not raise


def test_cooldown_passes_when_window_expired() -> None:
    import datetime as dt
    last = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=200)).isoformat()
    _check_cooldown(last, 120)  # should not raise


def test_cooldown_blocks_when_active() -> None:
    last = datetime.now(timezone.utc).isoformat()
    with pytest.raises(CooldownActiveError):
        _check_cooldown(last, 120)


# ── Isolation tests ──


@pytest.mark.asyncio
async def test_isolate_pod_dry_run() -> None:
    result = await isolate_pod("pod-1", "default", dry_run=True)
    assert result["status"] == "skipped"
    assert result["dry_run"] is True


# ── Log dump tests ──


@pytest.mark.asyncio
async def test_dump_pod_logs_dry_run() -> None:
    result = await dump_pod_logs("pod-1", "default", "/tmp/test-logs", dry_run=True)
    assert result["status"] == "skipped"
    assert result["dry_run"] is True


# ── Scale tests ──


@pytest.mark.asyncio
async def test_scale_deployment_dry_run() -> None:
    result = await scale_deployment("my-app", "default", 1, 10, dry_run=True)
    assert result["status"] == "skipped"
    assert result["dry_run"] is True


# ── Pipeline tests (without Istio) ──


@pytest.mark.asyncio
async def test_execute_remediation_dry_run(config: RemediationConfig, evaluation: EvaluationResult) -> None:
    """Full pipeline in dry-run mode should return skipped results."""
    results = await execute_remediation(
        evaluation=evaluation,
        deployment_name="payment-api",
        namespace="default",
        config=config,
        istio_config=None,
        dry_run=True,
    )
    assert len(results) > 0
    for r in results:
        assert r.get("dry_run") is True


@pytest.mark.asyncio
async def test_execute_remediation_cooldown_active(config: RemediationConfig, evaluation: EvaluationResult) -> None:
    """Remediation blocked when cooldown is active."""
    last = datetime.now(timezone.utc).isoformat()

    with pytest.raises(CooldownActiveError):
        await execute_remediation(
            evaluation=evaluation,
            deployment_name="payment-api",
            namespace="default",
            config=config,
            istio_config=None,
            last_remediation_time=last,
            dry_run=True,
        )


@pytest.mark.asyncio
async def test_execute_remediation_no_pod_name(config: RemediationConfig) -> None:
    """Evaluation without pod_name should return empty results."""
    eval_no_pod = EvaluationResult(
        rule_name="test-rule",
        violated=True,
        value=10.0,
        threshold=5.0,
        severity=RuleSeverity.CRITICAL,
        pod_name=None,
        query="up",
    )
    results = await execute_remediation(
        evaluation=eval_no_pod,
        deployment_name="payment-api",
        namespace="default",
        config=config,
        istio_config=None,
        dry_run=True,
    )
    assert results == []


@pytest.mark.asyncio
async def test_execute_remediation_isolation_disabled(config: RemediationConfig, evaluation: EvaluationResult) -> None:
    """Pod isolation disabled should skip isolation step."""
    config = RemediationConfig(
        enable_pod_isolation=False,
        enable_log_dump=True,
        enable_h_scaling=True,
        scale_up_replicas=1,
        max_replicas=10,
        cooldown_seconds=120,
        log_dump_path="/tmp/test",
        enable_ticket_creation=True,
    )
    results = await execute_remediation(
        evaluation=evaluation,
        deployment_name="payment-api",
        namespace="default",
        config=config,
        istio_config=None,
        dry_run=True,
    )
    actions = [r["action"] for r in results]
    assert "pod_isolation" not in actions


# ── Pipeline tests with Istio ──


@pytest.mark.asyncio
async def test_execute_remediation_with_istio(
    config: RemediationConfig,
    istio_config: IstioConfig,
    evaluation: EvaluationResult,
) -> None:
    """Pipeline with Istio enabled includes Istio ejection steps."""
    results = await execute_remediation(
        evaluation=evaluation,
        deployment_name="payment-api",
        namespace="default",
        config=config,
        istio_config=istio_config,
        dry_run=True,
    )
    actions = [r["action"] for r in results]
    assert "pod_isolation" in actions
    assert "istio_outlier_detection" in actions
    assert "istio_virtual_service_eject" in actions
    assert "log_dump" in actions
    assert "horizontal_scale" in actions


@pytest.mark.asyncio
async def test_execute_remediation_istio_disabled_config(
    config: RemediationConfig,
    evaluation: EvaluationResult,
) -> None:
    """IstioConfig with enabled=False should not execute Istio steps."""
    istio_off = IstioConfig(enabled=False)
    results = await execute_remediation(
        evaluation=evaluation,
        deployment_name="payment-api",
        namespace="default",
        config=config,
        istio_config=istio_off,
        dry_run=True,
    )
    actions = [r["action"] for r in results]
    assert "istio_outlier_detection" not in actions
    assert "istio_virtual_service_eject" not in actions


@pytest.mark.asyncio
async def test_execute_remediation_all_disabled(evaluation: EvaluationResult) -> None:
    """All remediation steps disabled returns empty list (after cooldown passes)."""
    config = RemediationConfig(
        enable_pod_isolation=False,
        enable_log_dump=False,
        enable_h_scaling=False,
        scale_up_replicas=1,
        max_replicas=10,
        cooldown_seconds=120,
        log_dump_path="/tmp/test",
        enable_ticket_creation=False,
    )
    results = await execute_remediation(
        evaluation=evaluation,
        deployment_name="payment-api",
        namespace="default",
        config=config,
        istio_config=None,
        dry_run=True,
    )
    assert results == []
