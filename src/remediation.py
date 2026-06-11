"""Remediation engine — sequential, safe, auditable self-healing actions."""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import kubernetes.client
import kubernetes.config
import structlog

from src.config import IstioConfig, RemediationConfig
from src.errors import (
    CooldownActiveError,
    IstioEjectionError,
    LogDumpError,
    MaxReplicasError,
    PodIsolationError,
    ScalingError,
)
from src.istio_adapter import (
    eject_pod_outlier_detection,
    eject_pod_virtual_service,
)
from src.metrics_adapter import EvaluationResult

logger = structlog.get_logger()

NO_TRAFFIC_LABEL = "self-healing.sre.k8s.io/no-traffic"
NO_TRAFFIC_VALUE = "true"
OPERATOR_ANNOTATION = "self-healing.sre.k8s.io/last-remediation"
MANAGED_ANNOTATION = "self-healing.sre.k8s.io/managed"


def _k8s_apps_api() -> kubernetes.client.AppsV1Api:
    kubernetes.config.load_incluster_config()
    return kubernetes.client.AppsV1Api()


def _k8s_core_api() -> kubernetes.client.CoreV1Api:
    kubernetes.config.load_incluster_config()
    return kubernetes.client.CoreV1Api()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _check_cooldown(
    last_remediation: str | None,
    cooldown_seconds: int,
) -> None:
    if not last_remediation:
        return
    try:
        last_time = datetime.fromisoformat(last_remediation)
        elapsed = (datetime.now(timezone.utc) - last_time).total_seconds()
        if elapsed < cooldown_seconds:
            raise CooldownActiveError(
                f"Cooldown active: {cooldown_seconds - elapsed:.0f}s remaining",
                context={"elapsed": elapsed, "cooldown": cooldown_seconds},
            )
    except (ValueError, TypeError):
        logger.warn("remediation.invalid_timestamp", value=last_remediation)


async def isolate_pod(
    pod_name: str,
    namespace: str,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Add no-traffic label to Pod, signaling service meshes / ingress to stop routing."""
    logger.info("remediation.isolate_pod", pod=pod_name, namespace=namespace, dry_run=dry_run)
    if dry_run:
        return {"action": "pod_isolation", "pod": pod_name, "dry_run": True, "status": "skipped"}

    core_api = _k8s_core_api()
    try:
        core_api.patch_namespaced_pod(
            name=pod_name,
            namespace=namespace,
            body={
                "metadata": {
                    "labels": {NO_TRAFFIC_LABEL: NO_TRAFFIC_VALUE},
                    "annotations": {
                        OPERATOR_ANNOTATION: _now_iso(),
                        MANAGED_ANNOTATION: "true",
                    },
                }
            },
        )
    except kubernetes.client.ApiException as exc:
        raise PodIsolationError(
            f"Failed to isolate Pod {pod_name}",
            context={"pod": pod_name, "namespace": namespace, "status": exc.status, "body": exc.body},
        ) from exc

    logger.info("remediation.pod_isolated", pod=pod_name, namespace=namespace)
    return {"action": "pod_isolation", "pod": pod_name, "status": "isolated", "label": NO_TRAFFIC_LABEL}


async def dump_pod_logs(
    pod_name: str,
    namespace: str,
    log_dump_path: str,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Collect Pod logs and persist to disk for post-incident analysis."""
    logger.info("remediation.dump_logs", pod=pod_name, namespace=namespace, dry_run=dry_run)
    if dry_run:
        return {"action": "log_dump", "pod": pod_name, "dry_run": True, "status": "skipped"}

    core_api = _k8s_core_api()
    try:
        log_response = core_api.read_namespaced_pod_log(
            name=pod_name,
            namespace=namespace,
            tail_lines=500,
            timestamps=True,
        )
    except kubernetes.client.ApiException as exc:
        raise LogDumpError(
            f"Failed to read logs from Pod {pod_name}",
            context={"pod": pod_name, "namespace": namespace, "status": exc.status},
        ) from exc

    dump_dir = Path(log_dump_path) / namespace / pod_name
    dump_dir.mkdir(parents=True, exist_ok=True)

    timestamp = time.strftime("%Y%m%d-%H%M%S")
    log_file = dump_dir / f"{pod_name}-{timestamp}.log"
    meta_file = dump_dir / f"{pod_name}-{timestamp}.meta.json"

    log_file.write_text(log_response)
    meta_file.write_text(json.dumps({
        "pod": pod_name,
        "namespace": namespace,
        "collected_at": _now_iso(),
        "tail_lines": 500,
    }))

    logger.info("remediation.logs_dumped", pod=pod_name, path=str(log_file))
    return {"action": "log_dump", "pod": pod_name, "path": str(log_file), "status": "dumped"}


async def scale_deployment(
    deployment_name: str,
    namespace: str,
    scale_up_replicas: int,
    max_replicas: int,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Scale the Deployment to compensate for the isolated Pod."""
    logger.info(
        "remediation.scale_deployment",
        deployment=deployment_name,
        namespace=namespace,
        scale_up=scale_up_replicas,
        max_replicas=max_replicas,
        dry_run=dry_run,
    )
    if dry_run:
        return {"action": "horizontal_scale", "deployment": deployment_name, "dry_run": True, "status": "skipped"}

    apps_api = _k8s_apps_api()
    try:
        deployment = apps_api.read_namespaced_deployment(
            name=deployment_name,
            namespace=namespace,
        )
    except kubernetes.client.ApiException as exc:
        raise ScalingError(
            f"Failed to read Deployment {deployment_name}",
            context={"deployment": deployment_name, "namespace": namespace, "status": exc.status},
        ) from exc

    current_replicas = deployment.spec.replicas or 0
    desired_replicas = current_replicas + scale_up_replicas

    if desired_replicas > max_replicas:
        raise MaxReplicasError(
            f"Cannot scale to {desired_replicas}: max_replicas={max_replicas}",
            context={
                "deployment": deployment_name,
                "current": current_replicas,
                "desired": desired_replicas,
                "max": max_replicas,
            },
        )

    try:
        apps_api.patch_namespaced_deployment(
            name=deployment_name,
            namespace=namespace,
            body={
                "spec": {
                    "replicas": desired_replicas,
                },
                "metadata": {
                    "annotations": {
                        OPERATOR_ANNOTATION: _now_iso(),
                    },
                },
            },
        )
    except kubernetes.client.ApiException as exc:
        raise ScalingError(
            f"Failed to scale Deployment {deployment_name}",
            context={"deployment": deployment_name, "namespace": namespace, "status": exc.status},
        ) from exc

    logger.info(
        "remediation.deployment_scaled",
        deployment=deployment_name,
        namespace=namespace,
        previous=current_replicas,
        current=desired_replicas,
    )
    return {
        "action": "horizontal_scale",
        "deployment": deployment_name,
        "previous_replicas": current_replicas,
        "current_replicas": desired_replicas,
        "status": "scaled",
    }


async def istio_eject_pod(
    deployment_name: str,
    namespace: str,
    pod_name: str,
    istio_config: IstioConfig,
    dry_run: bool = False,
) -> list[dict[str, Any]]:
    """Execute Istio ejection: outlierDetection + VirtualService label (defense-in-depth)."""
    results: list[dict[str, Any]] = []

    # Step A: DestinationRule outlierDetection
    try:
        result = await eject_pod_outlier_detection(
            deployment_name=deployment_name,
            namespace=namespace,
            pod_name=pod_name,
            consecutive_5xx_errors=istio_config.consecutive_5xx_errors,
            interval_seconds=istio_config.interval_seconds,
            base_ejection_time_seconds=istio_config.base_ejection_time_seconds,
            dry_run=dry_run,
        )
        results.append(result)
    except IstioEjectionError as exc:
        logger.error(
            "remediation.istio_outlier_detection_failed",
            deployment=deployment_name,
            pod=pod_name,
            error=str(exc),
            **exc.context,
        )
        results.append({"action": "istio_outlier_detection", "status": "failed", "error": str(exc)})

    # Step B: VirtualService pod-level ejection label
    if istio_config.enable_virtual_service_eject:
        try:
            result = await eject_pod_virtual_service(
                deployment_name=deployment_name,
                namespace=namespace,
                pod_name=pod_name,
                dry_run=dry_run,
            )
            results.append(result)
        except IstioEjectionError as exc:
            logger.error(
                "remediation.istio_vs_ejection_failed",
                deployment=deployment_name,
                pod=pod_name,
                error=str(exc),
                **exc.context,
            )
            results.append({"action": "istio_virtual_service_eject", "status": "failed", "error": str(exc)})

    return results


async def execute_remediation(
    evaluation: EvaluationResult,
    deployment_name: str,
    namespace: str,
    config: RemediationConfig,
    istio_config: IstioConfig | None = None,
    last_remediation_time: str | None = None,
    dry_run: bool = False,
) -> list[dict[str, Any]]:
    """Execute the full sequential remediation pipeline for a single violation.

    Pipeline: cooldown check → pod isolation → Istio ejection → log dump → scale
    Note: ticket creation is orchestrated by the operator handler.
    """
    _check_cooldown(last_remediation_time, config.cooldown_seconds)

    results: list[dict[str, Any]] = []
    pod_name = evaluation.pod_name
    if not pod_name:
        logger.warn("remediation.no_pod_in_evaluation", rule=evaluation.rule_name)
        return results

    # Step 1: Isolate the unhealthy Pod (K8s label-based)
    if config.enable_pod_isolation:
        result = await isolate_pod(pod_name, namespace, dry_run=dry_run)
        results.append(result)
    else:
        logger.info("remediation.isolation_disabled", pod=pod_name)

    # Step 1b: Istio Service Mesh ejection (outlierDetection + VS label)
    if istio_config and istio_config.enabled:
        istio_results = await istio_eject_pod(
            deployment_name=deployment_name,
            namespace=namespace,
            pod_name=pod_name,
            istio_config=istio_config,
            dry_run=dry_run,
        )
        results.extend(istio_results)

    # Step 2: Dump logs before Pod termination
    if config.enable_log_dump:
        result = await dump_pod_logs(pod_name, namespace, config.log_dump_path, dry_run=dry_run)
        results.append(result)
    else:
        logger.info("remediation.log_dump_disabled", pod=pod_name)

    # Step 3: Ticket creation is called from the operator handler
    # (to maintain orchestration flexibility with provider selection)

    # Step 4: Scale the Deployment
    if config.enable_h_scaling:
        result = await scale_deployment(
            deployment_name,
            namespace,
            config.scale_up_replicas,
            config.max_replicas,
            dry_run=dry_run,
        )
        results.append(result)
    else:
        logger.info("remediation.h_scaling_disabled", deployment=deployment_name)

    logger.info(
        "remediation.pipeline_complete",
        pod=pod_name,
        deployment=deployment_name,
        actions=len(results),
        istio_enabled=bool(istio_config and istio_config.enabled),
    )
    return results
