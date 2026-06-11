"""Prometheus metrics adapter — queries and evaluates business-metric rules."""

from __future__ import annotations

import time
from dataclasses import dataclass
from enum import Enum
from typing import Any

import httpx
import structlog

from src.config import MetricsConfig
from src.errors import MetricsQueryError, MetricsUnavailableError

logger = structlog.get_logger()

COMPARATORS: dict[str, Any] = {
    "gt": lambda a, b: a > b,
    "gte": lambda a, b: a >= b,
    "lt": lambda a, b: a < b,
    "lte": lambda a, b: a <= b,
    "eq": lambda a, b: a == b,
    "neq": lambda a, b: a != b,
}


class RuleSeverity(str, Enum):
    CRITICAL = "critical"
    WARNING = "warning"


@dataclass
class MetricRule:
    name: str
    query: str
    threshold: float
    operator: str
    for_seconds: int = 0
    severity: RuleSeverity = RuleSeverity.CRITICAL


@dataclass
class EvaluationResult:
    rule_name: str
    violated: bool
    value: float | None
    threshold: float
    severity: RuleSeverity
    pod_name: str | None = None
    query: str = ""


@dataclass
class CheckResult:
    healthy: bool
    evaluations: list[EvaluationResult]
    timestamp: str
    error: str | None = None


class MetricsAdapter:
    """Queries Prometheus and evaluates AppHealth metric rules."""

    def __init__(self, config: MetricsConfig | None = None) -> None:
        self.config = config or MetricsConfig()
        self._client: httpx.AsyncClient | None = None
        # Track first-violation timestamps for for_seconds sustain window
        self._violation_start: dict[str, float] = {}

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self.config.prometheus_url,
                timeout=self.config.query_timeout,
            )
        return self._client

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def query(self, promql: str) -> list[dict[str, Any]]:
        """Execute an instant PromQL query. Returns list of result vectors."""
        client = await self._get_client()
        try:
            resp = await client.get(
                "/api/v1/query",
                params={"query": promql},
            )
            resp.raise_for_status()
            body = resp.json()
        except httpx.TimeoutException as exc:
            raise MetricsUnavailableError(
                f"Prometheus timeout after {self.config.query_timeout}s",
                context={"query": promql, "error": str(exc)},
            ) from exc
        except httpx.HTTPStatusError as exc:
            raise MetricsUnavailableError(
                f"Prometheus HTTP {exc.response.status_code}",
                context={"query": promql, "status": exc.response.status_code},
            ) from exc
        except httpx.RequestError as exc:
            raise MetricsUnavailableError(
                f"Prometheus request error",
                context={"query": promql, "error": str(exc)},
            ) from exc

        if body.get("status") != "success":
            raise MetricsQueryError(
                f"PromQL error: {body.get('error', 'unknown')}",
                context={"query": promql, "response": body},
            )

        results = body.get("data", {}).get("result", [])
        return results

    def _evaluate_rule(
        self,
        rule: MetricRule,
        query_results: list[dict[str, Any]],
    ) -> list[EvaluationResult]:
        """Evaluate a single rule against Prometheus query results."""
        results: list[EvaluationResult] = []
        compare_fn = COMPARATORS.get(rule.operator)

        if compare_fn is None:
            logger.error("metrics.unknown_operator", operator=rule.operator, rule=rule.name)
            return results

        if not query_results:
            results.append(
                EvaluationResult(
                    rule_name=rule.name,
                    violated=False,
                    value=None,
                    threshold=rule.threshold,
                    severity=rule.severity,
                    query=rule.query,
                )
            )
            return results

        for vector in query_results:
            try:
                value = float(vector.get("value", [None, 0])[1])
            except (TypeError, ValueError, IndexError):
                logger.warn("metrics.parse_error", rule=rule.name, vector=vector)
                continue

            labels = vector.get("metric", {})
            pod_name = labels.get("pod", labels.get("exported_pod", None))

            is_violated = compare_fn(value, rule.threshold)

            # Sustain window: only trigger after for_seconds of continuous violation
            violation_key = f"{rule.name}:{pod_name}"
            now = time.time()

            if is_violated:
                if violation_key not in self._violation_start:
                    self._violation_start[violation_key] = now
                elapsed = now - self._violation_start[violation_key]
                sustained = elapsed >= rule.for_seconds
            else:
                self._violation_start.pop(violation_key, None)
                sustained = False

            results.append(
                EvaluationResult(
                    rule_name=rule.name,
                    violated=sustained,
                    value=value,
                    threshold=rule.threshold,
                    severity=rule.severity,
                    pod_name=pod_name,
                    query=rule.query,
                )
            )

        return results

    async def check(
        self,
        rules: list[MetricRule],
    ) -> CheckResult:
        """Run all rules against Prometheus and return aggregated check result."""
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        all_evals: list[EvaluationResult] = []
        error: str | None = None

        for rule in rules:
            try:
                query_results = await self.query(rule.query)
                evals = self._evaluate_rule(rule, query_results)
                all_evals.extend(evals)
            except (MetricsUnavailableError, MetricsQueryError) as exc:
                logger.error(
                    "metrics.rule_query_failed",
                    rule=rule.name,
                    error=str(exc),
                    **exc.context,
                )
                error = str(exc)
                all_evals.append(
                    EvaluationResult(
                        rule_name=rule.name,
                        violated=False,
                        value=None,
                        threshold=rule.threshold,
                        severity=rule.severity,
                        query=rule.query,
                    )
                )

        healthy = not any(e.violated for e in all_evals)
        return CheckResult(
            healthy=healthy,
            evaluations=all_evals,
            timestamp=now,
            error=error,
        )
