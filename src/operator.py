"""Self-Healing Kubernetes Operator — main entry point powered by Kopf."""

from __future__ import annotations

import os
import time
from typing import Any

import kopf
import structlog

from src.config import IstioConfig, LeaderElectionConfig, OperatorConfig, RemediationConfig, TicketConfig, load_config
from src.errors import (
    CooldownActiveError,
    IstioEjectionError,
    MaxReplicasError,
    MetricsUnavailableError,
    OperatorError,
)
from src.istio_adapter import remove_outlier_detection, remove_pod_ejection_label
from src.leader_election import LeaderElector
from src.metrics_adapter import CheckResult, MetricRule, MetricsAdapter, RuleSeverity
from src.metrics_server import metrics_server as operator_metrics
from src.metrics_server import set_leader_elector
from src.remediation import execute_remediation
from src.ticket_integration import TicketProvider, create_ticket

logger = structlog.get_logger()

CONFIG = load_config()

# ── Leader election ──
_leader_elector = LeaderElector(
    lease_name=CONFIG.leader_election.lease_name,
    namespace=CONFIG.leader_election.namespace,
    lease_duration_seconds=CONFIG.leader_election.lease_duration_seconds,
    renewal_interval_seconds=CONFIG.leader_election.renewal_interval_seconds,
    enabled=CONFIG.leader_election.enabled,
)

# ── Structured logging setup ──
structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.BoundLogger,
    cache_logger_on_first_use=True,
)


# ── CRD Group/Version ──
GROUP = "sre.k8s.io"
VERSION = "v1alpha1"
KIND = "AppHealth"
PLURAL = "apphealths"

# ── Namespace scope ──
# WATCH_NAMESPACE="" (default) → cluster-wide (all namespaces)
# WATCH_NAMESPACE="self-healing-system" → single namespace only
WATCH_NAMESPACE = CONFIG.watch_namespace or None  # None = all namespaces in Kopf


def _parse_rules(spec_metrics: dict) -> list[MetricRule]:
    """Parse CRD spec.metrics.rules into MetricRule objects."""
    rules = []
    for r in spec_metrics.get("rules", []):
        rules.append(
            MetricRule(
                name=r["name"],
                query=r["query"],
                threshold=float(r["threshold"]),
                operator=r["operator"],
                for_seconds=int(r.get("forSeconds", 0)),
                severity=RuleSeverity(r.get("severity", "critical")),
            )
        )
    return rules


def _parse_istio(spec: dict) -> dict[str, Any]:
    """Extract Istio config from CRD spec, falling back to operator defaults."""
    i = spec.get("serviceMesh", {}).get("istio", {})
    return {
        "enabled": i.get("enabled", CONFIG.istio.enabled),
        "consecutive_5xx_errors": i.get("consecutive5xxErrors", CONFIG.istio.consecutive_5xx_errors),
        "interval_seconds": i.get("intervalSeconds", CONFIG.istio.interval_seconds),
        "base_ejection_time_seconds": i.get("baseEjectionTimeSeconds", CONFIG.istio.base_ejection_time_seconds),
        "enable_virtual_service_eject": i.get("enableVirtualServiceEject", CONFIG.istio.enable_virtual_service_eject),
    }


def _parse_remediation(spec: dict) -> dict[str, Any]:
    """Extract remediation config from CRD spec, falling back to operator defaults."""
    r = spec.get("remediation", {})
    return {
        "enable_pod_isolation": r.get("enablePodIsolation", CONFIG.remediation.enable_pod_isolation),
        "enable_log_dump": r.get("enableLogDump", CONFIG.remediation.enable_log_dump),
        "enable_ticket_creation": r.get("enableTicketCreation", CONFIG.remediation.enable_ticket_creation),
        "enable_h_scaling": r.get("enableHScaling", CONFIG.remediation.enable_h_scaling),
        "scale_up_replicas": r.get("scaleUpReplicas", CONFIG.remediation.scale_up_replicas),
        "max_replicas": r.get("maxReplicas", CONFIG.remediation.max_replicas),
        "cooldown_seconds": r.get("cooldownSeconds", CONFIG.remediation.cooldown_seconds),
        "log_dump_path": r.get("logDumpPath", CONFIG.remediation.log_dump_path),
        "ticket_webhook_url": r.get("ticketWebhookUrl", CONFIG.ticket.webhook_url),
        "ticket_webhook_secret": r.get("ticketWebhookSecret", CONFIG.ticket.webhook_secret),
        "ticket_auth_email": r.get("ticketAuthEmail", CONFIG.ticket.auth_email),
        "ticket_auth_token": r.get("ticketAuthToken", CONFIG.ticket.auth_token),
        "ticket_provider": r.get("ticketProvider", "generic"),
        "ticket_project_key": r.get("ticketProjectKey", "SRE"),
    }


def _build_conditions(
    phase: str,
    check_result: CheckResult | None = None,
    error: str | None = None,
) -> list[dict[str, Any]]:
    """Build K8s-style conditions list for the status sub-resource."""
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    conditions = []

    # MetricsAvailable condition
    metrics_status = "True" if (check_result and not check_result.error) else "False"
    conditions.append({
        "type": "MetricsAvailable",
        "status": metrics_status,
        "reason": "PrometheusReachable" if metrics_status == "True" else "PrometheusUnreachable",
        "message": check_result.error or "Metrics queries successful",
        "lastTransitionTime": now,
    })

    # SLOViolated condition
    violated = check_result and not check_result.healthy
    conditions.append({
        "type": "SLOViolated",
        "status": "True" if violated else "False",
        "reason": "ThresholdExceeded" if violated else "WithinThreshold",
        "message": f"Rules violated: {sum(1 for e in (check_result.evaluations or []) if e.violated)}" if violated else "All rules within threshold",
        "lastTransitionTime": now,
    })

    # Remediating condition
    conditions.append({
        "type": "Remediating",
        "status": "True" if phase == "remediating" else "False",
        "reason": "RemediationActive" if phase == "remediating" else "RemediationIdle",
        "message": "Remediation pipeline executing" if phase == "remediating" else "No active remediation",
        "lastTransitionTime": now,
    })

    # IstioEjection condition
    istio_cfg = _parse_istio({"serviceMesh": {}})  # use defaults
    if istio_cfg["enabled"]:
        conditions.append({
            "type": "IstioEjection",
            "status": "True" if phase == "remediating" else "False",
            "reason": "OutlierDetectionActive" if phase == "remediating" else "OutlierDetectionIdle",
            "message": "Istio outlierDetection applied" if phase == "remediating" else "No Istio ejection active",
            "lastTransitionTime": now,
        })

    return conditions


# ── Kopf Handlers ──


@kopf.on.create(GROUP, VERSION, PLURAL, namespace=WATCH_NAMESPACE)
async def on_create(
    spec: dict,
    meta: dict,
    namespace: str,
    name: str,
    uid: str,
    patch: kopf.Patch,
    **kwargs: Any,
) -> None:
    """Initialize AppHealth status when the resource is first created."""
    logger.info("operator.resource_created", name=name, namespace=namespace, uid=uid)

    operator_metrics.inc("active_monitored_deployments")

    # Only the leader should patch status; standby still tracks gauges
    if not _leader_elector.is_leader():
        logger.info("operator.resource_created_standby", name=name, namespace=namespace)
        return

    patch.status["phase"] = "healthy"
    patch.status["observedGeneration"] = meta.get("generation", 1)
    patch.status["incidentCount"] = 0
    patch.status["currentReplicas"] = 0
    patch.status["unhealthyPods"] = ""
    patch.status["conditions"] = _build_conditions("healthy")
    patch.status["lastCheckTime"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


@kopf.on.resume(GROUP, VERSION, PLURAL, namespace=WATCH_NAMESPACE)
async def on_resume(
    spec: dict,
    meta: dict,
    namespace: str,
    name: str,
    patch: kopf.Patch,
    **kwargs: Any,
) -> None:
    """Reconcile on operator restart — re-attach to existing resources."""
    logger.info("operator.resource_resumed", name=name, namespace=namespace)
    if not _leader_elector.is_leader():
        logger.info("operator.resource_resumed_standby", name=name, namespace=namespace)
        return
    patch.status["observedGeneration"] = meta.get("generation", 1)


@kopf.on.timer(
    GROUP,
    VERSION,
    PLURAL,
    interval=lambda spec: spec.get("metrics", {}).get("checkIntervalSeconds", 30),
    idle=5,
    namespace=WATCH_NAMESPACE,
)
async def on_timer(
    spec: dict,
    meta: dict,
    status: dict,
    namespace: str,
    name: str,
    patch: kopf.Patch,
    **kwargs: Any,
) -> None:
    """Periodic metrics check — the core reconciliation loop."""
    correlation_id = f"{name}-{int(time.time())}"
    log = logger.bind(correlation_id=correlation_id, resource=name, namespace=namespace)

    # Only the leader executes reconciliation; standby skips entirely
    if not _leader_elector.is_leader():
        log.debug("operator.check_skipped_standby")
        return

    target_deployment = spec.get("targetDeployment", "")
    target_ns = spec.get("targetNamespace", namespace)
    prometheus_url = spec.get("metrics", {}).get("prometheusUrl", CONFIG.metrics.prometheus_url)

    check_start = time.time()
    operator_metrics.inc("checks_total")

    log.info("operator.check_start", deployment=target_deployment, prometheus=prometheus_url)

    # Parse rules and config
    rules = _parse_rules(spec.get("metrics", {}))
    remediation_cfg = _parse_remediation(spec)
    istio_cfg = _parse_istio(spec)

    if not rules:
        log.warn("operator.no_rules_defined")
        patch.status["phase"] = "error"
        patch.status["conditions"] = _build_conditions("error", error="No metric rules defined")
        return

    # Check metrics
    metrics_config = CONFIG.metrics
    metrics_config = type(metrics_config)(  # override URL from CRD if specified
        prometheus_url=prometheus_url,
        query_timeout=metrics_config.query_timeout,
        check_interval=metrics_config.check_interval,
    )
    adapter = MetricsAdapter(metrics_config)

    try:
        check_result = await adapter.check(rules)
    except MetricsUnavailableError as exc:
        operator_metrics.inc("checks_failed_total")
        log.error("operator.metrics_unavailable", error=str(exc), **exc.context)
        patch.status["phase"] = "error"
        patch.status["conditions"] = _build_conditions("error", error=str(exc))
        patch.status["lastCheckTime"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        return
    finally:
        await adapter.close()

    # Determine phase
    violated_evals = [e for e in check_result.evaluations if e.violated]
    unhealthy_pods = list({e.pod_name for e in violated_evals if e.pod_name})

    if check_result.healthy:
        phase = "healthy"
    elif any(e.severity == RuleSeverity.CRITICAL for e in violated_evals):
        phase = "remediating"
    else:
        phase = "degrading"

    log.info(
        "operator.check_complete",
        phase=phase,
        violated=len(violated_evals),
        unhealthy_pods=unhealthy_pods,
        istio_enabled=istio_cfg["enabled"],
    )

    # Record check duration
    check_duration = time.time() - check_start
    operator_metrics.observe_histogram("check_duration_seconds", check_duration)

    # Update active gauges
    operator_metrics.set_gauge("active_violations", len(violated_evals))

    # Execute remediation for critical violations
    remediation_results: list[dict[str, Any]] = []
    ticket_results: list[dict[str, Any]] = []
    incident_count = status.get("incidentCount", 0)

    if phase == "remediating" and unhealthy_pods:
        rem_cfg = RemediationConfig(
            enable_pod_isolation=remediation_cfg["enable_pod_isolation"],
            enable_log_dump=remediation_cfg["enable_log_dump"],
            enable_h_scaling=remediation_cfg["enable_h_scaling"],
            scale_up_replicas=remediation_cfg["scale_up_replicas"],
            max_replicas=remediation_cfg["max_replicas"],
            cooldown_seconds=remediation_cfg["cooldown_seconds"],
            log_dump_path=remediation_cfg["log_dump_path"],
            enable_ticket_creation=remediation_cfg["enable_ticket_creation"],
        )

        istio_config_obj = IstioConfig(
            enabled=istio_cfg["enabled"],
            consecutive_5xx_errors=istio_cfg["consecutive_5xx_errors"],
            interval_seconds=istio_cfg["interval_seconds"],
            base_ejection_time_seconds=istio_cfg["base_ejection_time_seconds"],
            enable_virtual_service_eject=istio_cfg["enable_virtual_service_eject"],
        )

        tick_cfg = TicketConfig(
            webhook_url=remediation_cfg["ticket_webhook_url"],
            webhook_secret=remediation_cfg["ticket_webhook_secret"],
            auth_email=remediation_cfg["ticket_auth_email"],
            auth_token=remediation_cfg["ticket_auth_token"],
            timeout=CONFIG.ticket.timeout,
            retry_max_attempts=CONFIG.ticket.retry_max_attempts,
        )

        ticket_provider = TicketProvider(remediation_cfg.get("ticket_provider", "generic"))
        ticket_project_key = remediation_cfg.get("ticket_project_key", "SRE")

        for evaluation in violated_evals:
            if not evaluation.violated or not evaluation.pod_name:
                continue

        try:
            results = await execute_remediation(
                evaluation=evaluation,
                deployment_name=target_deployment,
                namespace=target_ns,
                config=rem_cfg,
                istio_config=istio_config_obj,
                last_remediation_time=status.get("lastRemediationTime"),
                dry_run=CONFIG.dry_run,
            )
            remediation_results.extend(results)
            operator_metrics.inc("remediations_total")

            # Step 3: Ticket creation (orchestrated here for provider selection)
            if remediation_cfg["enable_ticket_creation"]:
                try:
                    ticket_result = await create_ticket(
                        evaluation=evaluation,
                        deployment_name=target_deployment,
                        namespace=target_ns,
                        config=tick_cfg,
                        dry_run=CONFIG.dry_run,
                        provider=ticket_provider,
                        project_key=ticket_project_key,
                    )
                    ticket_results.append(ticket_result)
                    operator_metrics.inc("tickets_created_total")
                except OperatorError as exc:
                    log.error(
                        "operator.ticket_failed",
                        error=exc.message,
                        rule=evaluation.rule_name,
                        pod=evaluation.pod_name,
                        **exc.context,
                    )
                    ticket_results.append({
                        "action": "ticket_creation",
                        "status": "failed",
                        "error": exc.message,
                    })
                    operator_metrics.inc("tickets_failed_total")

            incident_count += 1

        except CooldownActiveError as exc:
            log.info("operator.cooldown_active", **exc.context)
        except MaxReplicasError as exc:
            log.warn("operator.max_replicas_reached", **exc.context)
            operator_metrics.inc("remediations_failed_total")
        except IstioEjectionError as exc:
            log.error("operator.istio_ejection_failed", error=str(exc), **exc.context)
            operator_metrics.inc("istio_ejections_failed_total")
        except OperatorError as exc:
            log.error(
                "operator.remediation_failed",
                error=exc.message,
                rule=evaluation.rule_name,
                pod=evaluation.pod_name,
                **exc.context,
            )
            operator_metrics.inc("remediations_failed_total")

        # Track Istio ejection metrics when enabled
        if istio_cfg["enabled"] and remediation_results:
            istio_actions = [r for r in remediation_results if "istio" in r.get("action", "")]
            for _ in istio_actions:
                operator_metrics.inc("istio_ejections_total")

    # Build remediation history entry
    remediation_history: list[dict[str, Any]] = status.get("remediationHistory", [])
    if remediation_results or ticket_results:
        remediation_history.append({
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "podName": ",".join(unhealthy_pods),
            "ruleName": ",".join(e.rule_name for e in violated_evals),
            "action": "full_remediation",
            "results": remediation_results + ticket_results,
        })
        # Keep only last 50 entries
        remediation_history = remediation_history[-50:]

    # Patch status
    now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    patch.status["phase"] = phase
    patch.status["observedGeneration"] = meta.get("generation", 1)
    patch.status["lastCheckTime"] = now_iso
    patch.status["unhealthyPods"] = ",".join(unhealthy_pods) if unhealthy_pods else ""
    patch.status["incidentCount"] = incident_count
    patch.status["conditions"] = _build_conditions(phase, check_result)

    if remediation_results or ticket_results:
        patch.status["lastRemediationTime"] = now_iso

    patch.status["remediationHistory"] = remediation_history

    # Log evaluation details for observability
    for evaluation in check_result.evaluations:
        log.info(
            "operator.evaluation",
            rule=evaluation.rule_name,
            violated=evaluation.violated,
            value=evaluation.value,
            threshold=evaluation.threshold,
            pod=evaluation.pod_name,
            severity=evaluation.severity.value,
        )


@kopf.on.delete(GROUP, VERSION, PLURAL, namespace=WATCH_NAMESPACE)
async def on_delete(
    spec: dict,
    meta: dict,
    namespace: str,
    name: str,
    status: dict,
    **kwargs: Any,
) -> None:
    """Cleanup when AppHealth resource is deleted — remove Istio config if present."""
    logger.info("operator.resource_deleted", name=name, namespace=namespace)

    operator_metrics.dec("active_monitored_deployments")

    # Only the leader should perform cleanup actions
    if not _leader_elector.is_leader():
        logger.info("operator.resource_deleted_standby", name=name, namespace=namespace)
        return

    target_deployment = spec.get("targetDeployment", "")
    target_ns = spec.get("targetNamespace", namespace)
    istio_cfg = _parse_istio(spec)

    if istio_cfg["enabled"] and target_deployment:
        try:
            await remove_outlier_detection(
                deployment_name=target_deployment,
                namespace=target_ns,
                dry_run=CONFIG.dry_run,
            )
        except IstioEjectionError as exc:
            logger.warn(
                "operator.istio_cleanup_failed",
                deployment=target_deployment,
                error=str(exc),
                **exc.context,
            )

        # Remove ejection labels from any ejected pods
        unhealthy = status.get("unhealthyPods", "")
        if unhealthy:
            for pod_name in unhealthy.split(","):
                pod_name = pod_name.strip()
                if pod_name:
                    try:
                        await remove_pod_ejection_label(pod_name, target_ns, dry_run=CONFIG.dry_run)
                    except IstioEjectionError as exc:
                        logger.warn(
                            "operator.istio_pod_label_cleanup_failed",
                            pod=pod_name,
                            error=str(exc),
                        )


# ── Health probe for the operator itself ──
@kopf.on.startup()
async def startup(**kwargs: Any) -> None:
    metrics_port = int(os.getenv("METRICS_SERVER_PORT", "9090"))
    metrics_host = os.getenv("METRICS_SERVER_HOST", "0.0.0.0")
    await operator_metrics.start_metrics_server(host=metrics_host, port=metrics_port)

    # Start leader election
    await _leader_elector.start()
    set_leader_elector(_leader_elector)
    leader_status = "leader" if _leader_elector.is_leader() else "standby"

    # Set initial leader gauge
    operator_metrics.set_gauge("leader_status", 1 if _leader_elector.is_leader() else 0)

    # Register callbacks to update gauge on leadership transitions
    async def _on_gained() -> None:
        operator_metrics.set_gauge("leader_status", 1)
        operator_metrics.inc("leader_transitions_total")

    async def _on_lost() -> None:
        operator_metrics.set_gauge("leader_status", 0)

    _leader_elector.on_leadership_gained(_on_gained)
    _leader_elector.on_leadership_lost(_on_lost)

    logger.info(
        "operator.startup",
        version="1.0.0",
        dry_run=CONFIG.dry_run,
        istio=CONFIG.istio.enabled,
        metrics_port=metrics_port,
        watch_namespace=WATCH_NAMESPACE or "(cluster-wide)",
        leader_election=CONFIG.leader_election.enabled,
        leader_status=leader_status,
    )


@kopf.on.cleanup()
async def cleanup(**kwargs: Any) -> None:
    await _leader_elector.stop()
    await operator_metrics.stop_metrics_server()
    logger.info("operator.shutdown")
