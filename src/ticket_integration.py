"""Ticket integration — Jira, ServiceNow, and generic webhooks with retry."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import random
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any

import httpx
import structlog

from src.config import TicketConfig
from src.errors import TicketCreationError
from src.metrics_adapter import EvaluationResult

logger = structlog.get_logger()

INCIDENT_LABEL = "self-healing-incident"

# ── Retry defaults ──
RETRY_MAX_ATTEMPTS = 3
RETRY_BASE_DELAY = 1.0      # seconds
RETRY_MAX_DELAY = 30.0      # seconds
RETRY_JITTER = 0.1          # ±10% randomization


class TicketProvider(str, Enum):
    GENERIC = "generic"
    JIRA = "jira"
    SERVICENOW = "servicenow"


@dataclass
class TicketPayload:
    incident_id: str
    rule_name: str
    pod_name: str
    namespace: str
    deployment: str
    value: float
    threshold: float
    severity: str
    timestamp: str
    description: str


def generate_incident_id(
    rule_name: str,
    pod_name: str,
    timestamp: str,
) -> str:
    """Deterministic incident ID from rule + pod + time — prevents duplicates."""
    raw = f"{rule_name}:{pod_name}:{timestamp}"
    return f"SH-{hashlib.sha256(raw.encode()).hexdigest()[:12].upper()}"


def build_ticket_payload(
    evaluation: EvaluationResult,
    deployment_name: str,
    namespace: str,
) -> TicketPayload:
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    incident_id = generate_incident_id(evaluation.rule_name, evaluation.pod_name or "", timestamp)
    description = (
        f"Self-Healing Operator detected violation of rule '{evaluation.rule_name}'. "
        f"Pod '{evaluation.pod_name}' in namespace '{namespace}' reported value "
        f"{evaluation.value} (threshold: {evaluation.threshold}). "
        f"Severity: {evaluation.severity.value}. Remediation actions in progress."
    )
    return TicketPayload(
        incident_id=incident_id,
        rule_name=evaluation.rule_name,
        pod_name=evaluation.pod_name or "unknown",
        namespace=namespace,
        deployment=deployment_name,
        value=evaluation.value or 0.0,
        threshold=evaluation.threshold,
        severity=evaluation.severity.value,
        timestamp=timestamp,
        description=description,
    )


# ── Provider-specific payload formatters ──


def _format_generic_payload(payload: TicketPayload) -> dict[str, Any]:
    """Generic webhook payload — provider-agnostic."""
    return {
        "incident_id": payload.incident_id,
        "rule_name": payload.rule_name,
        "pod_name": payload.pod_name,
        "namespace": payload.namespace,
        "deployment": payload.deployment,
        "value": payload.value,
        "threshold": payload.threshold,
        "severity": payload.severity,
        "timestamp": payload.timestamp,
        "description": payload.description,
        "labels": [INCIDENT_LABEL],
    }


def _format_jira_payload(payload: TicketPayload, project_key: str = "SRE") -> dict[str, Any]:
    """Jira-issue payload compatible with /rest/api/2/issue or /rest/api/3/issue."""
    severity_map = {"critical": "Highest", "warning": "Medium"}
    priority_name = severity_map.get(payload.severity, "Medium")
    summary = f"[Self-Healing] {payload.rule_name} — Pod {payload.pod_name} ({payload.incident_id})"

    return {
        "fields": {
            "project": {"key": project_key},
            "summary": summary,
            "description": {
                "type": "doc",
                "version": 1,
                "content": [
                    {
                        "type": "paragraph",
                        "content": [{"type": "text", "text": payload.description}],
                    },
                    {
                        "type": "panel",
                        "attrs": {"panelType": "info"},
                        "content": [
                            {
                                "type": "paragraph",
                                "content": [
                                    {
                                        "type": "text",
                                        "text": (
                                            f"Deployment: {payload.deployment}\n"
                                            f"Namespace: {payload.namespace}\n"
                                            f"Pod: {payload.pod_name}\n"
                                            f"Metric Value: {payload.value}\n"
                                            f"Threshold: {payload.threshold}\n"
                                            f"Severity: {payload.severity}\n"
                                            f"Detected At: {payload.timestamp}"
                                        ),
                                        "marks": [{"type": "code"}],
                                    }
                                ],
                            }
                        ],
                    },
                ],
            },
            "issuetype": {"name": "Incident"},
            "priority": {"name": priority_name},
            "labels": [INCIDENT_LABEL, payload.namespace, payload.deployment],
        },
    }


def _format_servicenow_payload(payload: TicketPayload) -> dict[str, Any]:
    """ServiceNow incident payload compatible with /api/now/table/incident."""
    severity_map = {"critical": "1", "warning": "3"}
    sn_severity = severity_map.get(payload.severity, "3")

    return {
        "short_description": (
            f"[Self-Healing] {payload.rule_name} — Pod {payload.pod_name} "
            f"({payload.incident_id})"
        ),
        "description": payload.description,
        "severity": sn_severity,
        "incident_state": "2",  # Active / In Progress
        "u_incident_id": payload.incident_id,
        "u_rule_name": payload.rule_name,
        "u_pod_name": payload.pod_name,
        "u_namespace": payload.namespace,
        "u_deployment": payload.deployment,
        "u_metric_value": str(payload.value),
        "u_threshold": str(payload.threshold),
        "u_detected_at": payload.timestamp,
        "u_source": "self-healing-operator",
        "comments": (
            f"Auto-created by Self-Healing Kubernetes Operator.\n"
            f"Deployment: {payload.deployment} | Namespace: {payload.namespace}\n"
            f"Pod: {payload.pod_name} | Rule: {payload.rule_name}\n"
            f"Value: {payload.value} | Threshold: {payload.threshold}"
        ),
    }


PAYLOAD_FORMATTERS: dict[TicketProvider, Any] = {
    TicketProvider.GENERIC: _format_generic_payload,
    TicketProvider.JIRA: _format_jira_payload,
    TicketProvider.SERVICENOW: _format_servicenow_payload,
}


def format_payload(
    payload: TicketPayload,
    provider: TicketProvider,
    project_key: str = "SRE",
) -> dict[str, Any]:
    """Format ticket payload for the target provider."""
    formatter = PAYLOAD_FORMATTERS.get(provider, _format_generic_payload)
    if provider == TicketProvider.JIRA:
        return formatter(payload, project_key=project_key)
    return formatter(payload)


# ── Retry with exponential backoff ──


def _compute_backoff(attempt: int) -> float:
    """Exponential backoff with jitter: min(2^n * base ± jitter, max_delay)."""
    delay = min(RETRY_BASE_DELAY * (2 ** attempt), RETRY_MAX_DELAY)
    jitter = delay * RETRY_JITTER * (2 * random.random() - 1)
    return max(0.0, delay + jitter)


def _is_retryable_status(status_code: int) -> bool:
    """HTTP status codes that warrant a retry."""
    return status_code in {408, 429, 500, 502, 503, 504}


async def _retry_request(
    method: str,
    url: str,
    *,
    body: str,
    headers: dict[str, str],
    timeout: float,
    max_attempts: int = RETRY_MAX_ATTEMPTS,
) -> httpx.Response:
    """Execute an HTTP request with exponential backoff retry."""
    last_exc: Exception | None = None

    for attempt in range(max_attempts):
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                resp = await client.request(method, url, content=body, headers=headers)

            if resp.status_code < 400:
                return resp

            if not _is_retryable_status(resp.status_code):
                resp.raise_for_status()

            # Retryable server error — log and retry
            logger.warn(
                "ticket.retryable_http_error",
                attempt=attempt + 1,
                max_attempts=max_attempts,
                status=resp.status_code,
                url=url,
            )

        except httpx.TimeoutException as exc:
            logger.warn(
                "ticket.timeout_retry",
                attempt=attempt + 1,
                max_attempts=max_attempts,
                url=url,
            )
            last_exc = exc

        except httpx.RequestError as exc:
            logger.warn(
                "ticket.request_error_retry",
                attempt=attempt + 1,
                max_attempts=max_attempts,
                url=url,
                error=str(exc),
            )
            last_exc = exc

        if attempt < max_attempts - 1:
            backoff = _compute_backoff(attempt)
            logger.info("ticket.backoff", attempt=attempt + 1, backoff_seconds=round(backoff, 2))
            await asyncio.sleep(backoff)

    # All attempts exhausted
    if last_exc is not None:
        raise TicketCreationError(
            f"Ticket webhook failed after {max_attempts} attempts",
            context={"url": url, "last_error": str(last_exc), "attempts": max_attempts},
        ) from last_exc

    # If we got here via non-retryable status, raise with the response
    raise TicketCreationError(
        f"Ticket webhook failed after {max_attempts} attempts",
        context={"url": url, "attempts": max_attempts},
    )


# ── Webhook dispatch with HMAC ──


def _sign_payload(body: str, secret: str) -> str:
    """HMAC-SHA256 signature for webhook payload verification."""
    return hmac.new(secret.encode(), body.encode(), hashlib.sha256).hexdigest()


async def create_ticket_webhook(
    payload: TicketPayload,
    config: TicketConfig,
    provider: TicketProvider = TicketProvider.GENERIC,
    project_key: str = "SRE",
) -> dict[str, Any]:
    """POST incident payload to external ticketing webhook with retry + HMAC."""
    if not config.webhook_url:
        logger.info("ticket.no_webhook_configured", incident_id=payload.incident_id)
        return {"action": "ticket_creation", "status": "no_webhook", "incident_id": payload.incident_id}

    formatted = format_payload(payload, provider, project_key=project_key)
    body = json.dumps(formatted)

    headers: dict[str, str] = {"Content-Type": "application/json"}

    if config.webhook_secret:
        headers["X-Signature"] = _sign_payload(body, config.webhook_secret)

    # Provider-specific auth headers
    if provider == TicketProvider.JIRA and config.auth_email and config.auth_token:
        import base64
        creds = base64.b64encode(f"{config.auth_email}:{config.auth_token}".encode()).decode()
        headers["Authorization"] = f"Basic {creds}"
    elif provider == TicketProvider.SERVICENOW and config.auth_token:
        headers["Authorization"] = f"Bearer {config.auth_token}"

    try:
        resp = await _retry_request(
            "POST",
            config.webhook_url,
            body=body,
            headers=headers,
            timeout=config.timeout,
            max_attempts=config.retry_max_attempts,
        )
    except TicketCreationError:
        raise
    except Exception as exc:
        raise TicketCreationError(
            "Ticket webhook unexpected error",
            context={"incident_id": payload.incident_id, "url": config.webhook_url, "error": str(exc)},
        ) from exc

    # Extract provider-specific ticket ID from response
    ticket_id = _extract_ticket_id(resp, provider)

    logger.info(
        "ticket.created_via_webhook",
        incident_id=payload.incident_id,
        provider=provider.value,
        ticket_id=ticket_id,
        url=config.webhook_url,
    )
    return {
        "action": "ticket_creation",
        "status": "created",
        "incident_id": payload.incident_id,
        "provider": provider.value,
        "ticket_id": ticket_id,
        "webhook_response_code": resp.status_code,
    }


def _extract_ticket_id(resp: httpx.Response, provider: TicketProvider) -> str:
    """Extract the remote ticket ID from the provider response."""
    try:
        data = resp.json()
    except Exception:
        return ""

    if provider == TicketProvider.JIRA:
        return data.get("key", data.get("id", ""))
    elif provider == TicketProvider.SERVICENOW:
        return data.get("result", {}).get("sys_id", data.get("result", {}).get("number", ""))
    return data.get("id", data.get("ticket_id", ""))


# ── Simulated ticket (fallback) ──


async def create_ticket_simulated(
    payload: TicketPayload,
) -> dict[str, Any]:
    """Simulate ticket creation — logs the full payload as if a ticket was opened."""
    logger.info(
        "ticket.simulated",
        incident_id=payload.incident_id,
        rule=payload.rule_name,
        pod=payload.pod_name,
        namespace=payload.namespace,
        deployment=payload.deployment,
        value=payload.value,
        threshold=payload.threshold,
        severity=payload.severity,
        description=payload.description,
    )
    return {
        "action": "ticket_creation",
        "status": "simulated",
        "incident_id": payload.incident_id,
    }


# ── Public entry point ──


async def create_ticket(
    evaluation: EvaluationResult,
    deployment_name: str,
    namespace: str,
    config: TicketConfig,
    dry_run: bool = False,
    provider: TicketProvider = TicketProvider.GENERIC,
    project_key: str = "SRE",
) -> dict[str, Any]:
    """Create a ticket — real webhook with retry or simulated fallback."""
    payload = build_ticket_payload(evaluation, deployment_name, namespace)

    if dry_run:
        logger.info("ticket.dry_run", incident_id=payload.incident_id, provider=provider.value)
        return {
            "action": "ticket_creation",
            "status": "dry_run",
            "incident_id": payload.incident_id,
            "provider": provider.value,
        }

    if config.webhook_url:
        return await create_ticket_webhook(payload, config, provider=provider, project_key=project_key)

    return await create_ticket_simulated(payload)
