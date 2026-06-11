"""Istio Service Mesh adapter — outlierDetection and VirtualService ejection."""

from __future__ import annotations

import time
from typing import Any

import kubernetes.client
import kubernetes.config
import structlog

from src.errors import IstioEjectionError

logger = structlog.get_logger()

# ── Labels / Annotations ──
ISTIO_EJECTED_LABEL = "self-healing.sre.k8s.io/istio-ejected"
ISTIO_EJECTED_VALUE = "true"
OPERATOR_ANNOTATION = "self-healing.sre.k8s.io/last-remediation"

# ── Istio CRD API groups ──
ISTIO_NETWORKING_GROUP = "networking.istio.io"
ISTIO_NETWORKING_VERSION = "v1beta1"


def _k8s_custom_api() -> kubernetes.client.CustomObjectsApi:
    kubernetes.config.load_incluster_config()
    return kubernetes.client.CustomObjectsApi()


def _k8s_core_api() -> kubernetes.client.CoreV1Api:
    kubernetes.config.load_incluster_config()
    return kubernetes.client.CoreV1Api()


def _now_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


async def eject_pod_outlier_detection(
    deployment_name: str,
    namespace: str,
    pod_name: str,
    consecutive_5xx_errors: int = 3,
    interval_seconds: str = "30s",
    base_ejection_time_seconds: str = "120s",
    dry_run: bool = False,
) -> dict[str, Any]:
    """Apply or update Istio DestinationRule outlierDetection to eject the unhealthy Pod.

    Strategy: Add a DestinationRule (or merge into existing one) with outlierDetection
    configured to eject endpoints returning consecutive 5xx errors. This is the
    Istio-native way to stop routing traffic to unhealthy Pods at the mesh level.

    If a DestinationRule already exists for the host, we merge the outlierDetection
    config. If not, we create one with safe defaults.
    """
    log = logger.bind(deployment=deployment_name, namespace=namespace, pod=pod_name, dry_run=dry_run)
    log.info("istio.outlier_detection_apply", consecutive_5xx=consecutive_5xx_errors)

    if dry_run:
        return {
            "action": "istio_outlier_detection",
            "deployment": deployment_name,
            "pod": pod_name,
            "dry_run": True,
            "status": "skipped",
        }

    custom_api = _k8s_custom_api()
    host = deployment_name

    dr_name = f"{deployment_name}-self-healing"

    # Check if DestinationRule already exists
    existing_dr: dict[str, Any] | None = None
    try:
        existing_dr = custom_api.get_namespaced_custom_object(
            group=ISTIO_NETWORKING_GROUP,
            version=ISTIO_NETWORKING_VERSION,
            namespace=namespace,
            plural="destinationrules",
            name=dr_name,
        )
    except kubernetes.client.ApiException as exc:
        if exc.status != 404:
            raise IstioEjectionError(
                f"Failed to read DestinationRule {dr_name}",
                context={"deployment": deployment_name, "namespace": namespace, "status": exc.status},
            ) from exc

    outlier_config: dict[str, Any] = {
        "consecutive5xxErrors": consecutive_5xx_errors,
        "interval": interval_seconds,
        "baseEjectionTime": base_ejection_time_seconds,
        "maxEjectionPercent": 100,
    }

    if existing_dr:
        # Merge outlierDetection into existing DestinationRule
        existing_spec = existing_dr.get("spec", {})
        existing_spec["outlierDetection"] = outlier_config
        # Mark the specific pod as ejected via label (for observability)
        patch_body: dict[str, Any] = {
            "spec": existing_spec,
        }
        try:
            custom_api.patch_namespaced_custom_object(
                group=ISTIO_NETWORKING_GROUP,
                version=ISTIO_NETWORKING_VERSION,
                namespace=namespace,
                plural="destinationrules",
                name=dr_name,
                body=patch_body,
            )
        except kubernetes.client.ApiException as exc:
            raise IstioEjectionError(
                f"Failed to patch DestinationRule {dr_name}",
                context={"deployment": deployment_name, "namespace": namespace, "status": exc.status},
            ) from exc

        log.info("istio.destinationrule_patched", dr=dr_name)
        return {
            "action": "istio_outlier_detection",
            "deployment": deployment_name,
            "pod": pod_name,
            "status": "patched",
            "destination_rule": dr_name,
        }

    # Create new DestinationRule
    dr_body: dict[str, Any] = {
        "apiVersion": f"{ISTIO_NETWORKING_GROUP}/{ISTIO_NETWORKING_VERSION}",
        "kind": "DestinationRule",
        "metadata": {
            "name": dr_name,
            "namespace": namespace,
            "labels": {
                "app.kubernetes.io/managed-by": "self-healing-operator",
            },
            "annotations": {
                OPERATOR_ANNOTATION: _now_iso(),
            },
        },
        "spec": {
            "host": host,
            "trafficPolicy": {
                "outlierDetection": outlier_config,
            },
        },
    }

    try:
        custom_api.create_namespaced_custom_object(
            group=ISTIO_NETWORKING_GROUP,
            version=ISTIO_NETWORKING_VERSION,
            namespace=namespace,
            plural="destinationrules",
            body=dr_body,
        )
    except kubernetes.client.ApiException as exc:
        raise IstioEjectionError(
            f"Failed to create DestinationRule {dr_name}",
            context={"deployment": deployment_name, "namespace": namespace, "status": exc.status, "body": exc.body},
        ) from exc

    log.info("istio.destinationrule_created", dr=dr_name)
    return {
        "action": "istio_outlier_detection",
        "deployment": deployment_name,
        "pod": pod_name,
        "status": "created",
        "destination_rule": dr_name,
    }


async def eject_pod_virtual_service(
    deployment_name: str,
    namespace: str,
    pod_name: str,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Eject a specific Pod by adding a route-level fault injection or header match.

    Strategy: If a VirtualService exists for the deployment, we add a route rule
    that matches traffic destined for the specific Pod IP and redirects it to a
    healthy endpoint. This is a more targeted ejection than outlierDetection.

    For simplicity and safety, this method focuses on labeling the Pod so that
    existing VirtualService routing rules (based on subset labels) automatically
    exclude it. Combined with outlierDetection, this provides defense-in-depth.
    """
    log = logger.bind(deployment=deployment_name, namespace=namespace, pod=pod_name, dry_run=dry_run)
    log.info("istio.virtual_service_eject", pod=pod_name)

    if dry_run:
        return {
            "action": "istio_virtual_service_eject",
            "deployment": deployment_name,
            "pod": pod_name,
            "dry_run": True,
            "status": "skipped",
        }

    core_api = _k8s_core_api()

    # Label the Pod as ejected — VirtualService subsets can filter on this
    try:
        core_api.patch_namespaced_pod(
            name=pod_name,
            namespace=namespace,
            body={
                "metadata": {
                    "labels": {ISTIO_EJECTED_LABEL: ISTIO_EJECTED_VALUE},
                    "annotations": {
                        OPERATOR_ANNOTATION: _now_iso(),
                    },
                }
            },
        )
    except kubernetes.client.ApiException as exc:
        raise IstioEjectionError(
            f"Failed to label Pod {pod_name} for Istio ejection",
            context={"pod": pod_name, "namespace": namespace, "status": exc.status},
        ) from exc

    log.info("istio.pod_ejected_label", pod=pod_name, label=ISTIO_EJECTED_LABEL)
    return {
        "action": "istio_virtual_service_eject",
        "deployment": deployment_name,
        "pod": pod_name,
        "status": "ejected",
        "label": ISTIO_EJECTED_LABEL,
    }


async def remove_outlier_detection(
    deployment_name: str,
    namespace: str,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Remove the self-healing DestinationRule (rollback the outlierDetection).

    Called when the AppHealth resource is deleted or the application recovers
    to restore normal Istio routing behavior.
    """
    log = logger.bind(deployment=deployment_name, namespace=namespace, dry_run=dry_run)
    log.info("istio.outlier_detection_remove")

    if dry_run:
        return {
            "action": "istio_outlier_detection_remove",
            "deployment": deployment_name,
            "dry_run": True,
            "status": "skipped",
        }

    custom_api = _k8s_custom_api()
    dr_name = f"{deployment_name}-self-healing"

    try:
        custom_api.delete_namespaced_custom_object(
            group=ISTIO_NETWORKING_GROUP,
            version=ISTIO_NETWORKING_VERSION,
            namespace=namespace,
            plural="destinationrules",
            name=dr_name,
            body=kubernetes.client.V1DeleteOptions(),
        )
    except kubernetes.client.ApiException as exc:
        if exc.status == 404:
            log.info("istio.destinationrule_not_found", dr=dr_name)
            return {"action": "istio_outlier_detection_remove", "status": "not_found", "dr": dr_name}
        raise IstioEjectionError(
            f"Failed to delete DestinationRule {dr_name}",
            context={"deployment": deployment_name, "namespace": namespace, "status": exc.status},
        ) from exc

    log.info("istio.destinationrule_removed", dr=dr_name)
    return {
        "action": "istio_outlier_detection_remove",
        "deployment": deployment_name,
        "status": "removed",
        "destination_rule": dr_name,
    }


async def remove_pod_ejection_label(
    pod_name: str,
    namespace: str,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Remove the Istio ejection label from a Pod (rollback)."""
    if dry_run:
        return {
            "action": "istio_ejection_label_remove",
            "pod": pod_name,
            "dry_run": True,
            "status": "skipped",
        }

    core_api = _k8s_core_api()

    # Remove the label by setting it to null
    try:
        core_api.patch_namespaced_pod(
            name=pod_name,
            namespace=namespace,
            body={
                "metadata": {
                    "labels": {ISTIO_EJECTED_LABEL: None},
                }
            },
        )
    except kubernetes.client.ApiException as exc:
        raise IstioEjectionError(
            f"Failed to remove ejection label from Pod {pod_name}",
            context={"pod": pod_name, "namespace": namespace, "status": exc.status},
        ) from exc

    logger.info("istio.pod_ejection_label_removed", pod=pod_name)
    return {
        "action": "istio_ejection_label_remove",
        "pod": pod_name,
        "status": "label_removed",
    }
