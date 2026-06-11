"""Tests for the Prometheus metrics adapter."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
import respx

from src.config import MetricsConfig
from src.errors import MetricsQueryError, MetricsUnavailableError
from src.metrics_adapter import CheckResult, MetricRule, MetricsAdapter, RuleSeverity


@pytest.fixture
def config() -> MetricsConfig:
    return MetricsConfig(prometheus_url="http://prometheus:9090", query_timeout=5.0, check_interval=30)


@pytest.fixture
def adapter(config: MetricsConfig) -> MetricsAdapter:
    return MetricsAdapter(config)


@pytest.fixture
def rule_5xx() -> MetricRule:
    return MetricRule(
        name="high-5xx-rate",
        query='sum(rate(http_requests_total{code=~"5.."}[2m]))',
        threshold=5.0,
        operator="gt",
        for_seconds=0,
        severity=RuleSeverity.CRITICAL,
    )


@pytest.fixture
def rule_latency() -> MetricRule:
    return MetricRule(
        name="high-p99-latency",
        query="histogram_quantile(0.99, sum(rate(http_bucket[2m])) by (le, pod))",
        threshold=0.5,
        operator="gt",
        for_seconds=0,
        severity=RuleSeverity.CRITICAL,
    )


@respx.mock
@pytest.mark.asyncio
async def test_query_success(adapter: MetricsAdapter) -> None:
    """Successful Prometheus query returns result vectors."""
    promql_response = {
        "status": "success",
        "data": {
            "resultType": "vector",
            "result": [
                {
                    "metric": {"pod": "payment-api-abc123", "code": "500"},
                    "value": [1700000000, "10.5"],
                },
            ],
        },
    }
    respx.get("http://prometheus:9090/api/v1/query").mock(
        return_value=httpx.Response(200, json=promql_response),
    )

    results = await adapter.query('sum(rate(http_requests_total{code=~"5.."}[2m]))')
    assert len(results) == 1
    assert results[0]["metric"]["pod"] == "payment-api-abc123"
    await adapter.close()


@respx.mock
@pytest.mark.asyncio
async def test_query_timeout(adapter: MetricsAdapter) -> None:
    """Timeout raises MetricsUnavailableError (retryable)."""
    respx.get("http://prometheus:9090/api/v1/query").mock(
        side_effect=httpx.TimeoutException("timeout"),
    )

    with pytest.raises(MetricsUnavailableError) as exc_info:
        await adapter.query("up")

    assert exc_info.value.retryable is True
    await adapter.close()


@respx.mock
@pytest.mark.asyncio
async def test_query_promql_error(adapter: MetricsAdapter) -> None:
    """PromQL error raises MetricsQueryError (not retryable)."""
    promql_response = {
        "status": "error",
        "errorType": "bad_data",
        "error": "invalid parameter 'query'",
    }
    respx.get("http://prometheus:9090/api/v1/query").mock(
        return_value=httpx.Response(200, json=promql_response),
    )

    with pytest.raises(MetricsQueryError):
        await adapter.query("invalid{query")
    await adapter.close()


def test_evaluate_rule_violated(adapter: MetricsAdapter, rule_5xx: MetricRule) -> None:
    """Rule triggers when value exceeds threshold."""
    query_results = [
        {"metric": {"pod": "api-pod-1"}, "value": [0, "10.5"]},
    ]
    evals = adapter._evaluate_rule(rule_5xx, query_results)
    assert len(evals) == 1
    assert evals[0].violated is True
    assert evals[0].value == 10.5
    assert evals[0].pod_name == "api-pod-1"


def test_evaluate_rule_healthy(adapter: MetricsAdapter, rule_5xx: MetricRule) -> None:
    """Rule is healthy when value is below threshold."""
    query_results = [
        {"metric": {"pod": "api-pod-1"}, "value": [0, "2.0"]},
    ]
    evals = adapter._evaluate_rule(rule_5xx, query_results)
    assert len(evals) == 1
    assert evals[0].violated is False


def test_evaluate_rule_empty_results(adapter: MetricsAdapter, rule_5xx: MetricRule) -> None:
    """Empty query results = healthy (no data = no violation)."""
    evals = adapter._evaluate_rule(rule_5xx, [])
    assert len(evals) == 1
    assert evals[0].violated is False
    assert evals[0].value is None


def test_evaluate_multiple_pods(adapter: MetricsAdapter, rule_5xx: MetricRule) -> None:
    """Multiple vector results produce per-pod evaluations."""
    query_results = [
        {"metric": {"pod": "pod-1"}, "value": [0, "10.0"]},
        {"metric": {"pod": "pod-2"}, "value": [0, "1.0"]},
    ]
    evals = adapter._evaluate_rule(rule_5xx, query_results)
    assert len(evals) == 2
    assert evals[0].violated is True
    assert evals[1].violated is False


def test_evaluate_lt_operator(adapter: MetricsAdapter) -> None:
    """Less-than operator works correctly."""
    rule = MetricRule(
        name="low-availability",
        query="up",
        threshold=0.5,
        operator="lt",
        severity=RuleSeverity.CRITICAL,
    )
    query_results = [
        {"metric": {"pod": "pod-1"}, "value": [0, "0.0"]},
    ]
    evals = adapter._evaluate_rule(rule, query_results)
    assert evals[0].violated is True


def test_evaluate_sustain_window(adapter: MetricsAdapter) -> None:
    """Sustain window (for_seconds) requires violation to persist."""
    import time

    rule = MetricRule(
        name="sustained-5xx",
        query="up",
        threshold=5.0,
        operator="gt",
        for_seconds=60,
        severity=RuleSeverity.CRITICAL,
    )
    query_results = [
        {"metric": {"pod": "pod-1"}, "value": [0, "10.0"]},
    ]

    # First check — violation detected but not sustained yet
    evals = adapter._evaluate_rule(rule, query_results)
    assert evals[0].violated is False  # not yet 60 seconds

    # Simulate waiting by backdating the violation start
    key = f"sustained-5xx:pod-1"
    adapter._violation_start[key] = time.time() - 61  # 61 seconds ago

    evals = adapter._evaluate_rule(rule, query_results)
    assert evals[0].violated is True  # now sustained


@respx.mock
@pytest.mark.asyncio
async def test_check_healthy(adapter: MetricsAdapter, rule_5xx: MetricRule) -> None:
    """Full check with healthy result."""
    promql_response = {
        "status": "success",
        "data": {
            "resultType": "vector",
            "result": [
                {"metric": {"pod": "pod-1"}, "value": [0, "2.0"]},
            ],
        },
    }
    respx.get("http://prometheus:9090/api/v1/query").mock(
        return_value=httpx.Response(200, json=promql_response),
    )

    result = await adapter.check([rule_5xx])
    assert result.healthy is True
    assert len(result.evaluations) == 1
    await adapter.close()


@respx.mock
@pytest.mark.asyncio
async def test_check_violated(adapter: MetricsAdapter, rule_5xx: MetricRule) -> None:
    """Full check with violated result."""
    promql_response = {
        "status": "success",
        "data": {
            "resultType": "vector",
            "result": [
                {"metric": {"pod": "pod-1"}, "value": [0, "10.0"]},
            ],
        },
    }
    respx.get("http://prometheus:9090/api/v1/query").mock(
        return_value=httpx.Response(200, json=promql_response),
    )

    result = await adapter.check([rule_5xx])
    assert result.healthy is False
    assert result.evaluations[0].violated is True
    assert result.evaluations[0].pod_name == "pod-1"
    await adapter.close()
