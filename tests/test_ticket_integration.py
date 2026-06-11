"""Integration tests for ticket webhook with retry and provider-specific payloads."""

from __future__ import annotations

import asyncio
import json

import httpx
import pytest
import respx

from src.config import TicketConfig
from src.errors import TicketCreationError
from src.metrics_adapter import EvaluationResult, RuleSeverity
from src.ticket_integration import (
    TicketProvider,
    _compute_backoff,
    _format_generic_payload,
    _format_jira_payload,
    _format_servicenow_payload,
    _is_retryable_status,
    _retry_request,
    _sign_payload,
    build_ticket_payload,
    create_ticket,
    create_ticket_simulated,
    create_ticket_webhook,
    format_payload,
    generate_incident_id,
)


# ── Fixtures ──


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


@pytest.fixture
def payload(evaluation: EvaluationResult):
    return build_ticket_payload(evaluation, "payment-api", "default")


@pytest.fixture
def ticket_config() -> TicketConfig:
    return TicketConfig(
        webhook_url="http://ticket-system:8080/webhook",
        webhook_secret="test-secret-key",
        timeout=5.0,
        retry_max_attempts=3,
    )


# ── Provider payload format tests ──


def test_generic_payload_format(payload) -> None:
    """Generic payload contains all standard fields."""
    formatted = _format_generic_payload(payload)
    assert formatted["incident_id"] == payload.incident_id
    assert formatted["rule_name"] == "high-5xx-rate"
    assert formatted["pod_name"] == "payment-api-abc123"
    assert formatted["namespace"] == "default"
    assert formatted["deployment"] == "payment-api"
    assert formatted["value"] == 10.5
    assert formatted["threshold"] == 5.0
    assert formatted["severity"] == "critical"
    assert "self-healing-incident" in formatted["labels"]


def test_jira_payload_format(payload) -> None:
    """Jira payload uses Atlassian document format with project key."""
    formatted = _format_jira_payload(payload, project_key="SRE")
    assert formatted["fields"]["project"]["key"] == "SRE"
    assert formatted["fields"]["issuetype"]["name"] == "Incident"
    assert formatted["fields"]["priority"]["name"] == "Highest"  # critical = Highest
    assert "self-healing-incident" in formatted["fields"]["labels"]
    assert "[Self-Healing]" in formatted["fields"]["summary"]
    # Description uses Atlassian doc format
    desc = formatted["fields"]["description"]
    assert desc["type"] == "doc"
    assert desc["version"] == 1


def test_jira_payload_warning_severity(payload) -> None:
    """Jira warning severity maps to Medium priority."""
    payload.severity = "warning"
    formatted = _format_jira_payload(payload, project_key="INC")
    assert formatted["fields"]["priority"]["name"] == "Medium"
    assert formatted["fields"]["project"]["key"] == "INC"


def test_servicenow_payload_format(payload) -> None:
    """ServiceNow payload uses table/incident format with custom fields."""
    formatted = _format_servicenow_payload(payload)
    assert formatted["severity"] == "1"  # critical = 1
    assert formatted["incident_state"] == "2"  # Active
    assert formatted["u_incident_id"] == payload.incident_id
    assert formatted["u_rule_name"] == "high-5xx-rate"
    assert formatted["u_pod_name"] == "payment-api-abc123"
    assert formatted["u_source"] == "self-healing-operator"
    assert "[Self-Healing]" in formatted["short_description"]


def test_servicenow_payload_warning_severity(payload) -> None:
    """ServiceNow warning severity maps to 3."""
    payload.severity = "warning"
    formatted = _format_servicenow_payload(payload)
    assert formatted["severity"] == "3"


def test_format_payload_dispatches_correctly(payload) -> None:
    """format_payload routes to the correct formatter."""
    generic = format_payload(payload, TicketProvider.GENERIC)
    assert "incident_id" in generic

    jira = format_payload(payload, TicketProvider.JIRA, project_key="OPS")
    assert "fields" in jira

    sn = format_payload(payload, TicketProvider.SERVICENOW)
    assert "short_description" in sn


# ── HMAC signature tests ──


def test_sign_payload_deterministic() -> None:
    """Same body + secret produces same signature."""
    sig1 = _sign_payload('{"test": true}', "secret")
    sig2 = _sign_payload('{"test": true}', "secret")
    assert sig1 == sig2


def test_sign_payload_different_secrets() -> None:
    """Different secrets produce different signatures."""
    sig1 = _sign_payload('{"test": true}', "secret1")
    sig2 = _sign_payload('{"test": true}', "secret2")
    assert sig1 != sig2


# ── Retry backoff tests ──


def test_compute_backoff_increases() -> None:
    """Backoff increases exponentially across attempts."""
    delays = [_compute_backoff(i) for i in range(5)]
    # Each delay should generally be larger (except for jitter)
    assert delays[1] > delays[0] * 0.5  # allow jitter
    assert delays[2] > delays[1] * 0.5


def test_compute_backoff_capped() -> None:
    """Backoff never exceeds max delay."""
    for i in range(20):
        assert _compute_backoff(i) <= 35.0  # RETRY_MAX_DELAY + jitter margin


def test_is_retryable_status() -> None:
    """Retryable status codes are correctly identified."""
    assert _is_retryable_status(408) is True
    assert _is_retryable_status(429) is True
    assert _is_retryable_status(500) is True
    assert _is_retryable_status(502) is True
    assert _is_retryable_status(503) is True
    assert _is_retryable_status(504) is True
    assert _is_retryable_status(400) is False
    assert _is_retryable_status(401) is False
    assert _is_retryable_status(404) is False
    assert _is_retryable_status(201) is False


# ── Webhook with retry integration tests ──


@respx.mock
@pytest.mark.asyncio
async def test_webhook_success_first_attempt(payload, ticket_config) -> None:
    """Webhook succeeds on first attempt."""
    respx.post("http://ticket-system:8080/webhook").mock(
        return_value=httpx.Response(201, json={"id": "TICKET-001"}),
    )
    result = await create_ticket_webhook(payload, ticket_config, provider=TicketProvider.GENERIC)
    assert result["status"] == "created"
    assert result["webhook_response_code"] == 201
    assert result["ticket_id"] == "TICKET-001"


@respx.mock
@pytest.mark.asyncio
async def test_webhook_retry_on_503_then_success(payload, ticket_config) -> None:
    """Webhook retries on 503 and succeeds on second attempt."""
    route = respx.post("http://ticket-system:8080/webhook")
    route.mock(
        side_effect=[
            httpx.Response(503, text="Service Unavailable"),
            httpx.Response(201, json={"id": "TICKET-002"}),
        ]
    )
    result = await create_ticket_webhook(payload, ticket_config, provider=TicketProvider.GENERIC)
    assert result["status"] == "created"
    assert result["webhook_response_code"] == 201
    assert route.call_count == 2


@respx.mock
@pytest.mark.asyncio
async def test_webhook_retry_exhausted(payload, ticket_config) -> None:
    """Webhook raises TicketCreationError after all retries exhausted."""
    respx.post("http://ticket-system:8080/webhook").mock(
        return_value=httpx.Response(503, text="Service Unavailable"),
    )
    with pytest.raises(TicketCreationError) as exc_info:
        await create_ticket_webhook(payload, ticket_config, provider=TicketProvider.GENERIC)
    assert exc_info.value.retryable is True
    assert "3 attempts" in exc_info.value.message


@respx.mock
@pytest.mark.asyncio
async def test_webhook_timeout_retry_then_success(payload, ticket_config) -> None:
    """Webhook retries on timeout and succeeds on second attempt."""
    route = respx.post("http://ticket-system:8080/webhook")
    route.mock(
        side_effect=[
            httpx.TimeoutException("timeout"),
            httpx.Response(201, json={"id": "TICKET-003"}),
        ]
    )
    result = await create_ticket_webhook(payload, ticket_config, provider=TicketProvider.GENERIC)
    assert result["status"] == "created"
    assert route.call_count == 2


@respx.mock
@pytest.mark.asyncio
async def test_webhook_non_retryable_status_fails_immediately(payload, ticket_config) -> None:
    """Non-retryable HTTP errors (400, 401, 403) fail without retry."""
    route = respx.post("http://ticket-system:8080/webhook").mock(
        return_value=httpx.Response(401, text="Unauthorized"),
    )
    with pytest.raises(TicketCreationError):
        await create_ticket_webhook(payload, ticket_config, provider=TicketProvider.GENERIC)
    # Should only be called once (no retry for 401)
    assert route.call_count == 1


@respx.mock
@pytest.mark.asyncio
async def test_webhook_includes_hmac_signature(payload, ticket_config) -> None:
    """Webhook request includes X-Signature header when secret is configured."""
    captured_headers: dict[str, str] = {}

    def capture_request(request: httpx.Request) -> httpx.Response:
        nonlocal captured_headers
        captured_headers = dict(request.headers)
        return httpx.Response(201, json={"id": "TICKET-004"})

    respx.post("http://ticket-system:8080/webhook").mock(side_effect=capture_request)
    await create_ticket_webhook(payload, ticket_config, provider=TicketProvider.GENERIC)
    assert "x-signature" in captured_headers
    assert captured_headers["x-signature"] != ""


@respx.mock
@pytest.mark.asyncio
async def test_jira_webhook_with_basic_auth(payload) -> None:
    """Jira webhook includes Basic auth header when credentials are provided."""
    config = TicketConfig(
        webhook_url="http://jira:8080/rest/api/2/issue",
        webhook_secret="",
        auth_email="operator@example.com",
        auth_token="jira-api-token",
        timeout=5.0,
        retry_max_attempts=1,
    )
    captured_headers: dict[str, str] = {}

    def capture_request(request: httpx.Request) -> httpx.Response:
        nonlocal captured_headers
        captured_headers = dict(request.headers)
        return httpx.Response(201, json={"key": "SRE-42", "id": "12345"})

    respx.post("http://jira:8080/rest/api/2/issue").mock(side_effect=capture_request)
    result = await create_ticket_webhook(payload, config, provider=TicketProvider.JIRA, project_key="SRE")

    assert result["status"] == "created"
    assert result["ticket_id"] == "SRE-42"  # Jira returns 'key'
    assert "authorization" in captured_headers
    assert captured_headers["authorization"].startswith("Basic ")


@respx.mock
@pytest.mark.asyncio
async def test_servicenow_webhook_with_bearer_auth(payload) -> None:
    """ServiceNow webhook includes Bearer auth header."""
    config = TicketConfig(
        webhook_url="http://snow:8080/api/now/table/incident",
        webhook_secret="",
        auth_email="",
        auth_token="sn-oauth-token",
        timeout=5.0,
        retry_max_attempts=1,
    )
    captured_headers: dict[str, str] = {}

    def capture_request(request: httpx.Request) -> httpx.Response:
        nonlocal captured_headers
        captured_headers = dict(request.headers)
        body = json.loads(request.content.decode())
        return httpx.Response(201, json={
            "result": {"sys_id": "SYS-123", "number": "INC0010001"}
        })

    respx.post("http://snow:8080/api/now/table/incident").mock(side_effect=capture_request)
    result = await create_ticket_webhook(
        payload, config, provider=TicketProvider.SERVICENOW
    )

    assert result["status"] == "created"
    assert result["ticket_id"] == "SYS-123"
    assert captured_headers["authorization"] == "Bearer sn-oauth-token"


# ── Simulated ticket tests ──


@pytest.mark.asyncio
async def test_create_ticket_simulated(payload) -> None:
    """Simulated ticket creation logs and returns correct status."""
    result = await create_ticket_simulated(payload)
    assert result["status"] == "simulated"
    assert result["incident_id"].startswith("SH-")


# ── create_ticket entry point tests ──


@pytest.mark.asyncio
async def test_create_ticket_no_webhook(evaluation) -> None:
    """No webhook URL configured falls back to simulated."""
    config = TicketConfig(webhook_url="", webhook_secret="", timeout=5.0)
    result = await create_ticket(evaluation, "payment-api", "default", config)
    assert result["status"] == "simulated"


@pytest.mark.asyncio
async def test_create_ticket_dry_run(evaluation, ticket_config) -> None:
    """Dry-run mode skips all ticket creation."""
    result = await create_ticket(
        evaluation, "payment-api", "default", ticket_config,
        dry_run=True, provider=TicketProvider.JIRA, project_key="SRE",
    )
    assert result["status"] == "dry_run"
    assert result["provider"] == "jira"


def test_incident_id_deterministic() -> None:
    """Same inputs produce same incident ID."""
    id1 = generate_incident_id("rule", "pod", "2024-01-01T00:00:00Z")
    id2 = generate_incident_id("rule", "pod", "2024-01-01T00:00:00Z")
    assert id1 == id2
    assert id1.startswith("SH-")
