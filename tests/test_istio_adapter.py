"""Integration tests for Istio adapter — outlierDetection and VirtualService ejection."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.config import IstioConfig
from src.errors import IstioEjectionError
from src.istio_adapter import (
    ISTIO_EJECTED_LABEL,
    ISTIO_EJECTED_VALUE,
    eject_pod_outlier_detection,
    eject_pod_virtual_service,
    remove_outlier_detection,
    remove_pod_ejection_label,
)


# ── Fixtures ──


@pytest.fixture
def istio_config() -> IstioConfig:
    return IstioConfig(
        enabled=True,
        consecutive_5xx_errors=3,
        interval_seconds="30s",
        base_ejection_time_seconds="120s",
        enable_virtual_service_eject=True,
    )


# ── outlierDetection tests ──


@pytest.mark.asyncio
async def test_outlier_detection_dry_run() -> None:
    """Dry-run mode skips DestinationRule creation."""
    result = await eject_pod_outlier_detection(
        deployment_name="payment-api",
        namespace="default",
        pod_name="pod-1",
        dry_run=True,
    )
    assert result["status"] == "skipped"
    assert result["dry_run"] is True
    assert result["action"] == "istio_outlier_detection"


@patch("src.istio_adapter._k8s_custom_api")
@pytest.mark.asyncio
async def test_outlier_detection_create_new_dr(mock_custom_api) -> None:
    """Creates a new DestinationRule when none exists."""
    mock_api = MagicMock()
    mock_custom_api.return_value = mock_api

    # No existing DestinationRule (404)
    mock_api.get_namespaced_custom_object.side_effect = Exception("not found")
    # Override with proper ApiException 404
    import kubernetes.client
    mock_api.get_namespaced_custom_object.side_effect = kubernetes.client.ApiException(
        status=404, reason="Not Found"
    )
    mock_api.create_namespaced_custom_object.return_value = None

    result = await eject_pod_outlier_detection(
        deployment_name="payment-api",
        namespace="default",
        pod_name="pod-1",
        consecutive_5xx_errors=3,
        interval_seconds="30s",
        base_ejection_time_seconds="120s",
    )

    assert result["status"] == "created"
    assert result["destination_rule"] == "payment-api-self-healing"
    assert result["action"] == "istio_outlier_detection"

    # Verify the DR was created with correct outlierDetection config
    create_call = mock_api.create_namespaced_custom_object.call_args
    dr_body = create_call.kwargs.get("body", create_call[1].get("body", {}))
    assert dr_body["kind"] == "DestinationRule"
    assert dr_body["spec"]["host"] == "payment-api"
    outlier = dr_body["spec"]["trafficPolicy"]["outlierDetection"]
    assert outlier["consecutive5xxErrors"] == 3
    assert outlier["interval"] == "30s"
    assert outlier["baseEjectionTime"] == "120s"
    assert outlier["maxEjectionPercent"] == 100


@patch("src.istio_adapter._k8s_custom_api")
@pytest.mark.asyncio
async def test_outlier_detection_patch_existing_dr(mock_custom_api) -> None:
    """Patches existing DestinationRule with outlierDetection config."""
    mock_api = MagicMock()
    mock_custom_api.return_value = mock_api

    existing_dr = {
        "apiVersion": "networking.istio.io/v1beta1",
        "kind": "DestinationRule",
        "metadata": {"name": "payment-api-self-healing", "namespace": "default"},
        "spec": {
            "host": "payment-api",
            "trafficPolicy": {
                "connectionPool": {"http": {"h2UpgradePolicy": "DEFAULT"}},
            },
        },
    }
    mock_api.get_namespaced_custom_object.return_value = existing_dr
    mock_api.patch_namespaced_custom_object.return_value = None

    result = await eject_pod_outlier_detection(
        deployment_name="payment-api",
        namespace="default",
        pod_name="pod-1",
        consecutive_5xx_errors=5,
        interval_seconds="15s",
        base_ejection_time_seconds="300s",
    )

    assert result["status"] == "patched"
    assert result["destination_rule"] == "payment-api-self-healing"

    # Verify the patch preserved existing trafficPolicy and added outlierDetection
    patch_call = mock_api.patch_namespaced_custom_object.call_args
    patch_body = patch_call.kwargs.get("body", patch_call[1].get("body", {}))
    assert "connectionPool" in patch_body["spec"]["trafficPolicy"]
    assert patch_body["spec"]["trafficPolicy"]["outlierDetection"]["consecutive5xxErrors"] == 5
    assert patch_body["spec"]["trafficPolicy"]["outlierDetection"]["baseEjectionTime"] == "300s"


@patch("src.istio_adapter._k8s_custom_api")
@pytest.mark.asyncio
async def test_outlier_detection_create_fails(mock_custom_api) -> None:
    """Failed DestinationRule creation raises IstioEjectionError."""
    import kubernetes.client
    mock_api = MagicMock()
    mock_custom_api.return_value = mock_api

    mock_api.get_namespaced_custom_object.side_effect = kubernetes.client.ApiException(
        status=404, reason="Not Found"
    )
    mock_api.create_namespaced_custom_object.side_effect = kubernetes.client.ApiException(
        status=403, reason="Forbidden"
    )

    with pytest.raises(IstioEjectionError) as exc_info:
        await eject_pod_outlier_detection(
            deployment_name="payment-api",
            namespace="default",
            pod_name="pod-1",
        )
    assert exc_info.value.retryable is True


# ── VirtualService ejection tests ──


@pytest.mark.asyncio
async def test_virtual_service_eject_dry_run() -> None:
    """Dry-run mode skips Pod labeling."""
    result = await eject_pod_virtual_service(
        deployment_name="payment-api",
        namespace="default",
        pod_name="pod-1",
        dry_run=True,
    )
    assert result["status"] == "skipped"
    assert result["dry_run"] is True


@patch("src.istio_adapter._k8s_core_api")
@pytest.mark.asyncio
async def test_virtual_service_eject_labels_pod(mock_core_api) -> None:
    """VirtualService ejection adds ejection label to Pod."""
    mock_api = MagicMock()
    mock_core_api.return_value = mock_api
    mock_api.patch_namespaced_pod.return_value = None

    result = await eject_pod_virtual_service(
        deployment_name="payment-api",
        namespace="default",
        pod_name="pod-1",
    )

    assert result["status"] == "ejected"
    assert result["label"] == ISTIO_EJECTED_LABEL

    # Verify the Pod was labeled
    patch_call = mock_api.patch_namespaced_pod.call_args
    patch_body = patch_call.kwargs.get("body", patch_call[1].get("body", {}))
    assert patch_body["metadata"]["labels"][ISTIO_EJECTED_LABEL] == ISTIO_EJECTED_VALUE


@patch("src.istio_adapter._k8s_core_api")
@pytest.mark.asyncio
async def test_virtual_service_eject_fails(mock_core_api) -> None:
    """Failed Pod labeling raises IstioEjectionError."""
    import kubernetes.client
    mock_api = MagicMock()
    mock_core_api.return_value = mock_api
    mock_api.patch_namespaced_pod.side_effect = kubernetes.client.ApiException(
        status=404, reason="Not Found"
    )

    with pytest.raises(IstioEjectionError):
        await eject_pod_virtual_service(
            deployment_name="payment-api",
            namespace="default",
            pod_name="nonexistent-pod",
        )


# ── Remove outlierDetection tests ──


@pytest.mark.asyncio
async def test_remove_outlier_detection_dry_run() -> None:
    """Dry-run mode skips DestinationRule deletion."""
    result = await remove_outlier_detection(
        deployment_name="payment-api",
        namespace="default",
        dry_run=True,
    )
    assert result["status"] == "skipped"
    assert result["dry_run"] is True


@patch("src.istio_adapter._k8s_custom_api")
@pytest.mark.asyncio
async def test_remove_outlier_detection_success(mock_custom_api) -> None:
    """Successfully deletes the self-healing DestinationRule."""
    mock_api = MagicMock()
    mock_custom_api.return_value = mock_api
    mock_api.delete_namespaced_custom_object.return_value = None

    result = await remove_outlier_detection(
        deployment_name="payment-api",
        namespace="default",
    )

    assert result["status"] == "removed"
    assert result["destination_rule"] == "payment-api-self-healing"


@patch("src.istio_adapter._k8s_custom_api")
@pytest.mark.asyncio
async def test_remove_outlier_detection_not_found(mock_custom_api) -> None:
    """Returns not_found status when DR doesn't exist (404)."""
    import kubernetes.client
    mock_api = MagicMock()
    mock_custom_api.return_value = mock_api
    mock_api.delete_namespaced_custom_object.side_effect = kubernetes.client.ApiException(
        status=404, reason="Not Found"
    )

    result = await remove_outlier_detection(
        deployment_name="payment-api",
        namespace="default",
    )

    assert result["status"] == "not_found"


# ── Remove pod ejection label tests ──


@pytest.mark.asyncio
async def test_remove_pod_ejection_label_dry_run() -> None:
    """Dry-run mode skips label removal."""
    result = await remove_pod_ejection_label(
        pod_name="pod-1",
        namespace="default",
        dry_run=True,
    )
    assert result["status"] == "skipped"


@patch("src.istio_adapter._k8s_core_api")
@pytest.mark.asyncio
async def test_remove_pod_ejection_label_success(mock_core_api) -> None:
    """Successfully removes the ejection label from Pod."""
    mock_api = MagicMock()
    mock_core_api.return_value = mock_api
    mock_api.patch_namespaced_pod.return_value = None

    result = await remove_pod_ejection_label(
        pod_name="pod-1",
        namespace="default",
    )

    assert result["status"] == "label_removed"

    # Verify the label was removed (set to null)
    patch_call = mock_api.patch_namespaced_pod.call_args
    patch_body = patch_call.kwargs.get("body", patch_call[1].get("body", {}))
    assert patch_body["metadata"]["labels"][ISTIO_EJECTED_LABEL] is None


# ── Remediation istio_eject_pod integration ──


@pytest.mark.asyncio
async def test_istio_eject_pod_full_pipeline(istio_config: IstioConfig) -> None:
    """Full Istio ejection pipeline in dry-run mode."""
    from src.remediation import istio_eject_pod

    results = await istio_eject_pod(
        deployment_name="payment-api",
        namespace="default",
        pod_name="pod-1",
        istio_config=istio_config,
        dry_run=True,
    )

    assert len(results) == 2  # outlierDetection + VS eject
    assert results[0]["action"] == "istio_outlier_detection"
    assert results[1]["action"] == "istio_virtual_service_eject"
    for r in results:
        assert r["dry_run"] is True


@pytest.mark.asyncio
async def test_istio_eject_pod_vs_disabled() -> None:
    """When VS eject is disabled, only outlierDetection runs."""
    from src.remediation import istio_eject_pod

    config = IstioConfig(
        enabled=True,
        consecutive_5xx_errors=3,
        interval_seconds="30s",
        base_ejection_time_seconds="120s",
        enable_virtual_service_eject=False,
    )
    results = await istio_eject_pod(
        deployment_name="payment-api",
        namespace="default",
        pod_name="pod-1",
        istio_config=config,
        dry_run=True,
    )

    assert len(results) == 1
    assert results[0]["action"] == "istio_outlier_detection"


# ── Full remediation pipeline with Istio ──


@pytest.mark.asyncio
async def test_execute_remediation_with_istio_dry_run(istio_config: IstioConfig) -> None:
    """Full remediation pipeline includes Istio steps in dry-run."""
    from src.config import RemediationConfig
    from src.metrics_adapter import EvaluationResult, RuleSeverity
    from src.remediation import execute_remediation

    config = RemediationConfig(
        enable_pod_isolation=True,
        enable_log_dump=True,
        enable_h_scaling=True,
        scale_up_replicas=1,
        max_replicas=10,
        cooldown_seconds=120,
        log_dump_path="/tmp/test",
        enable_ticket_creation=True,
    )
    evaluation = EvaluationResult(
        rule_name="high-5xx-rate",
        violated=True,
        value=10.5,
        threshold=5.0,
        severity=RuleSeverity.CRITICAL,
        pod_name="pod-1",
        query="up",
    )

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
async def test_execute_remediation_without_istio() -> None:
    """When Istio config is None, no Istio steps are executed."""
    from src.config import RemediationConfig
    from src.metrics_adapter import EvaluationResult, RuleSeverity
    from src.remediation import execute_remediation

    config = RemediationConfig(
        enable_pod_isolation=True,
        enable_log_dump=True,
        enable_h_scaling=True,
        scale_up_replicas=1,
        max_replicas=10,
        cooldown_seconds=120,
        log_dump_path="/tmp/test",
        enable_ticket_creation=True,
    )
    evaluation = EvaluationResult(
        rule_name="high-5xx-rate",
        violated=True,
        value=10.5,
        threshold=5.0,
        severity=RuleSeverity.CRITICAL,
        pod_name="pod-1",
        query="up",
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
    assert "istio_outlier_detection" not in actions
    assert "istio_virtual_service_eject" not in actions
    assert "pod_isolation" in actions
