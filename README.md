# Self-Healing Kubernetes Operator

> SRE automation that detects **business-metric violations** and executes autonomous remediation — before a human gets paged. Supports **Istio service mesh** ejection, **Jira/ServiceNow** ticket integration, **Prometheus self-metrics**, and **Helm deployment**.

---

## Overview

This operator extends Kubernetes with a **Custom Resource Definition (CRD)** called `AppHealth` that lets you declare **what healthy looks like** for your application using business metrics (HTTP 5xx rate, P99 latency, gRPC error rate, etc.). When a violation is sustained, the operator executes a **sequential remediation pipeline**:

```
Metrics Check → Pod Isolation → Istio Ejection → Log Dump → Ticket Creation → Horizontal Scale
```

Every action is auditable, configurable, and safe — with cooldown windows, max-replica caps, retry with exponential backoff, and dry-run mode.

## Architecture

```
┌───────────────────────────────────────────────────────────────┐
│                    AppHealth CRD                              │
│  spec.targetDeployment: "payment-api"                         │
│  spec.metrics.rules: [5xx-rate, p99-latency, ...]             │
│  spec.serviceMesh.istio: {enabled, consecutive5xxErrors, …}   │
│  spec.remediation: {isolate, istio, dump, ticket, scale}      │
└─────────────┬─────────────────────────────────────────────────┘
              │ kopf timer handler (every N seconds)
              ▼
┌───────────────────────────────────────────────────────────────┐
│               Metrics Adapter (Prometheus)                    │
│  1. Execute each PromQL rule                                  │
│  2. Evaluate threshold + sustain window (forSeconds)          │
│  3. Return per-pod violation results                          │
└─────────────┬─────────────────────────────────────────────────┘
              │ if violation detected
              ▼
┌───────────────────────────────────────────────────────────────┐
│              Remediation Pipeline (sequential)                │
│                                                               │
│  Step 1: Pod Isolation (K8s label)                            │
│    → Label pod with self-healing.sre.k8s.io/no-traffic=true   │
│                                                               │
│  Step 1b: Istio Service Mesh Ejection (Sprint 2)              │
│    → DestinationRule outlierDetection (consecutive5xxErrors)  │
│    → Pod label self-healing.sre.k8s.io/istio-ejected=true     │
│    → VirtualService subsets can filter on this label          │
│                                                               │
│  Step 2: Log Dump                                             │
│    → Collect last 500 lines with timestamps                   │
│    → Persist to PVC: /var/log/self-healing/<ns>/<pod>/        │
│                                                               │
│  Step 3: Ticket Creation (Sprint 2 — real payloads)           │
│    → Provider-specific payload (Jira / ServiceNow / generic)  │
│    → HMAC-SHA256 webhook signature verification               │
│    → Retry with exponential backoff (3 attempts by default)   │
│    → Basic auth (Jira) / Bearer auth (ServiceNow)             │
│    → Deterministic incident IDs prevent duplicates            │
│                                                               │
│  Step 4: Horizontal Scale                                     │
│    → Increase Deployment replicas by scaleUpReplicas          │
│    → Hard cap at maxReplicas (prevents runaway scaling)       │
└───────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────┐
│ Operator Metrics Server (Sprint 3)                           │
│                                                              │
│ GET /metrics → Prometheus scrape endpoint                    │
│ GET /health → Liveness probe (JSON)                          │
│ GET /ready → Readiness probe (JSON)                          │
│                                                              │
│ Exposes: checks_total, remediations_total,                   │
│ istio_ejections_total, tickets_created_total,                │
│ active_monitored_deployments, active_violations,             │
│ check_duration_seconds, uptime_seconds                       │
│ leader_status, leader_transitions_total                      │
└──────────────────────────────────────────────────────────────┘

┌───────────────────────────────────────────────────────────────┐
│ Leader Election (Sprint 4)                                    │
│                                                               │
│ coordination.k8s.io/Lease → single active leader              │
│ Leader → runs reconciliation handlers (timer, create, delete) │
│ Standby → exposes /metrics + /health, /ready = "standby"      │
│ Heartbeat loop → renews Lease every renewal_interval_seconds  │
│ Graceful shutdown → releases Lease for fast failover          │
└───────────────────────────────────────────────────────────────┘
```

## What's New

### Sprint 4

| Feature | Description |
|---------|-------------|
| **Leader Election** | Lease-based leader election (`coordination.k8s.io/Lease`) — safe multi-replica deployments without split-brain |
| **Leadership Metrics** | `leader_status` gauge (1=leader, 0=standby) + `leader_transitions_total` counter |
| **Smart Readiness Probe** | `/ready` reflects leader election state — leader → `ready`, standby → `standby` (both return 200) |
| **Conditional Handlers** | Timer, create, resume, delete handlers skip mutations when replica is standby |
| **ServiceMonitor Template** | Helm chart includes optional `ServiceMonitor` for Prometheus Operator |
| **RBAC: Leases** | Added `coordination.k8s.io/leases` permission for leader election |

### Sprint 3

| Feature | Description |
|---------|-------------|
| **Prometheus Metrics Server** | Operator exposes own telemetry on `:9090/metrics` — counters, gauges, histograms for all SRE operations |
| **Health Probes** | `/health` (liveness) and `/ready` (readiness) endpoints — replaces process-pgrep healthcheck |
| **Multi-Namespace** | `WATCH_NAMESPACE` env var — empty = cluster-wide, set value = single-namespace scope |
| **Helm Chart** | Full chart with templated CRD, RBAC, Deployment, Service, PVC — `make helm-install` |

### Sprint 2

| Feature | Description |
|---------|-------------|
| **Istio Integration** | DestinationRule `outlierDetection` + VirtualService Pod ejection label — network-level traffic isolation |
| **Jira Payload** | Atlassian document format with project key, incident type, priority mapping (critical → Highest) |
| **ServiceNow Payload** | `table/incident` format with severity mapping, custom fields, auto-comments |
| **Retry with Backoff** | Exponential backoff (2^n × base ± jitter) on 408/429/5xx — max 3 attempts by default |
| **HMAC Webhook Signing** | `X-Signature` header with SHA256 for webhook payload verification |
| **Provider Auth** | Basic auth for Jira, Bearer auth for ServiceNow — via K8s Secrets |
| **Cleanup on Delete** | `on_delete` handler removes DestinationRules and ejection labels |

## Why Python + Kopf?

| Factor | Python/Kopf | Go/controller-runtime |
|--------|-------------|----------------------|
| Development speed | Fast — less boilerplate | More scaffolding |
| Readability for portfolio | High — Python is universal | Medium — Go K8s API is verbose |
| Ecosystem | kubernetes-client, httpx, structlog | k8s.io/client-go |
| Runtime | Slightly higher memory | Lower memory footprint |
| Best for | SRE automation, rapid prototyping | High-throughput controllers |

**Decision:** Python + Kopf — optimal for an SRE portfolio project where clarity and rapid iteration matter more than raw throughput. The operator's bottleneck is Prometheus query latency, not runtime speed.

## Quick Start

### Option A: Make + kubectl (Manual)

```bash
# 1. Install CRD
make crd

# 2. Build + Deploy
make build
kind load docker-image self-healing-operator:latest  # if using kind
make deploy

# 3. Apply an AppHealth resource
kubectl apply -f examples/apphealth-payment-api.yaml

# 4. Verify
make status
```

### Option B: Helm Chart (Recommended)

```bash
# 1. Build + push image (or load into kind)
make build
kind load docker-image self-healing-operator:latest

# 2. Install via Helm
make helm-install

# Or with custom values:
helm install self-healing-operator ./helm/self-healing-operator \
  --namespace self-healing-system --create-namespace \
  --set metrics.prometheusUrl=http://my-prometheus:9090 \
  --set istio.enabled=true \
  --set ticket.provider=jira

# 3. Verify
kubectl get apphealths -A
kubectl get pods -n self-healing-system
```

### Prerequisites

- Kubernetes cluster (kind, minikube, GKE, EKS, etc.)
- `kubectl` configured
- Prometheus deployed in the cluster
- Istio service mesh (optional — for network-level ejection)
- Docker (for building the operator image)
- Helm 3+ (for Helm-based deployment)

## Configuration

### CRD Spec Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `spec.targetDeployment` | string | **required** | Deployment name to monitor |
| `spec.targetNamespace` | string | same namespace | Namespace of the target Deployment |
| `spec.metrics.prometheusUrl` | string | env `PROMETHEUS_URL` | Prometheus server URL |
| `spec.metrics.checkIntervalSeconds` | int | 30 | Check interval (10–3600s) |
| `spec.metrics.rules[]` | array | **required** | List of metric rules |
| `spec.metrics.rules[].name` | string | **required** | Rule name |
| `spec.metrics.rules[].query` | string | **required** | PromQL query |
| `spec.metrics.rules[].threshold` | float | **required** | Numeric threshold |
| `spec.metrics.rules[].operator` | enum | **required** | `gt`, `gte`, `lt`, `lte`, `eq`, `neq` |
| `spec.metrics.rules[].forSeconds` | int | 0 | Sustain window before triggering |
| `spec.metrics.rules[].severity` | enum | `critical` | `critical` or `warning` |
| `spec.serviceMesh.istio.enabled` | bool | `false` | Enable Istio integration |
| `spec.serviceMesh.istio.consecutive5xxErrors` | int | 3 | 5xx errors before ejection |
| `spec.serviceMesh.istio.intervalSeconds` | string | `"30s"` | OutlierDetection check interval |
| `spec.serviceMesh.istio.baseEjectionTimeSeconds` | string | `"120s"` | Ejection duration |
| `spec.serviceMesh.istio.enableVirtualServiceEject` | bool | `true` | Add ejection label for VS subsets |
| `spec.remediation.enablePodIsolation` | bool | true | Isolate unhealthy Pods |
| `spec.remediation.enableLogDump` | bool | true | Collect Pod logs |
| `spec.remediation.enableTicketCreation` | bool | true | Create incident ticket |
| `spec.remediation.enableHScaling` | bool | true | Scale Deployment up |
| `spec.remediation.scaleUpReplicas` | int | 1 | Additional replicas per remediation |
| `spec.remediation.maxReplicas` | int | 10 | Hard cap on total replicas |
| `spec.remediation.cooldownSeconds` | int | 120 | Minimum gap between remediations |
| `spec.remediation.logDumpPath` | string | `/var/log/self-healing` | PVC path for logs |
| `spec.remediation.ticketProvider` | enum | `generic` | `generic`, `jira`, `servicenow` |
| `spec.remediation.ticketProjectKey` | string | `"SRE"` | Jira project key |
| `spec.remediation.ticketWebhookUrl` | string | env | Webhook URL |
| `spec.remediation.ticketWebhookSecret` | string | env | HMAC signing secret |
| `spec.remediation.ticketAuthEmail` | string | env | Auth email (Jira Basic auth) |
| `spec.remediation.ticketAuthToken` | string | env | Auth token (Jira/ServiceNow) |

### Environment Variables

See `.env.example` for the full list. All CRD fields can be overridden via environment variables.

#### Sprint 3 Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `METRICS_SERVER_HOST` | `0.0.0.0` | Metrics server bind address |
| `METRICS_SERVER_PORT` | `9090` | Metrics server port |
| `WATCH_NAMESPACE` | `""` (cluster-wide) | Namespace scope — empty = all namespaces |

#### Sprint 4 Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `LEADER_ELECT` | `false` | Enable leader election for multi-replica |
| `LEADER_ELECTION_LEASE_NAME` | `self-healing-operator` | Lease object name |
| `LEADER_ELECTION_NAMESPACE` | operator namespace | Lease namespace |
| `LEADER_ELECTION_DURATION_SECONDS` | `15` | Lease TTL (seconds before failover) |
| `LEADER_ELECTION_RENEW_INTERVAL_SECONDS` | `5.0` | Leader heartbeat interval |

### Dry-Run Mode

Set `DRY_RUN=true` to execute the full pipeline without making any Kubernetes mutations. All actions log as `skipped`.

## Operator Metrics (Sprint 3)

The operator exposes its own Prometheus metrics on `:9090/metrics`:

| Metric | Type | Description |
|--------|------|-------------|
| `self_healing_checks_total` | counter | Total metric checks executed |
| `self_healing_checks_failed_total` | counter | Checks that failed (Prometheus unavailable) |
| `self_healing_remediations_total` | counter | Successful remediations |
| `self_healing_remediations_failed_total` | counter | Failed remediations |
| `self_healing_istio_ejections_total` | counter | Istio ejections applied |
| `self_healing_istio_ejections_failed_total` | counter | Failed Istio ejections |
| `self_healing_tickets_created_total` | counter | Tickets created via webhook |
| `self_healing_tickets_failed_total` | counter | Failed ticket creations |
| `self_healing_active_monitored_deployments` | gauge | Currently monitored AppHealth resources |
| `self_healing_active_violations` | gauge | Current SLO violations |
| `self_healing_check_duration_seconds` | histogram | Check cycle duration |
| `self_healing_uptime_seconds` | gauge | Operator uptime |
| `self_healing_leader_status` | gauge | 1 if this replica is leader, 0 if standby (Sprint 4) |
| `self_healing_leader_transitions_total` | counter | Number of leadership transitions (Sprint 4) |

### Health Endpoints

| Endpoint | Purpose |
|----------|---------|
| `GET /health` | Liveness probe — returns JSON with uptime, check/remediation counts |
| `GET /ready` | Readiness probe — reflects leader election state: `ready` (leader), `standby` (non-leader), or `ready` (election disabled) |
| `GET /metrics` | Prometheus scrape — text exposition format |

### Prometheus ServiceMonitor

The Helm chart creates a `Service` with Prometheus annotations. For `ServiceMonitor`-based discovery (Prometheus Operator), enable it in values:

```bash
helm install self-healing-operator ./helm/self-healing-operator \
--set metricsServer.serviceMonitor.enabled=true
```

Or apply manually:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: self-healing-operator
spec:
  selector:
    matchLabels:
      app: self-healing-operator
      component: metrics
  endpoints:
  - port: metrics
    path: /metrics
    interval: 30s
```

## Multi-Namespace Support (Sprint 3)

By default, the operator watches **all namespaces** (cluster-wide). This is controlled by the `WATCH_NAMESPACE` environment variable:

| Value | Scope | Use Case |
|-------|-------|----------|
| `""` (empty) | Cluster-wide | Single operator manages all AppHealth resources across all namespaces |
| `"production"` | Single namespace | Operator only watches resources in the `production` namespace |
| `"self-healing-system"` | Operator namespace | Restrict to the operator's own namespace |

### When to use single-namespace mode

- **Multi-tenant clusters** — different teams get their own operator instance
- **Compliance** — restrict operator RBAC to a single namespace
- **Testing** — run a canary operator in a test namespace without affecting production

## Leader Election (Sprint 4)

When running **more than 1 replica**, leader election prevents split-brain — only the replica holding the `Lease` object runs reconciliation handlers.

### How It Works

```
┌──────────────┐    ┌─────────────┐    ┌─────────────┐
│  Replica 1   │    │  Replica 2  │    │  Replica 3  │
│  LEADER ✓    │    │  STANDBY    │    │  STANDBY    │
│  Runs timer  │    │  /metrics   │    │  /metrics   │
│  /ready=ok   │    │  /ready=    │    │  /ready=    │
│              │    │  standby    │    │  standby    │
└──────┬───────┘    └─────────────┘    └─────────────┘
       │ heartbeat every 5s
       ▼
┌──────────────────────────────┐
│ coordination.k8s.io/Lease    │
│ name: self-healing-operator  │
│ holder: replica-1-pod-abc    │
│ duration: 15s                │
└──────────────────────────────┘
```

1. **Acquisition**: On startup, each replica tries to create or acquire the Lease
2. **Heartbeat**: The leader renews the Lease every `renewal_interval_seconds` (default 5s)
3. **Failover**: If the leader crashes, the Lease expires after `lease_duration_seconds` (default 15s), and a standby takes over
4. **Standby behavior**: Exposes `/metrics` and `/health` normally; `/ready` returns `{status: "standby"}`; skips all reconciliation handlers
5. **Graceful shutdown**: On SIGTERM, the leader clears `holderIdentity` for immediate failover

### Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `LEADER_ELECT` | `false` | Enable leader election (required for >1 replica) |
| `LEADER_ELECTION_LEASE_NAME` | `self-healing-operator` | Lease object name |
| `LEADER_ELECTION_NAMESPACE` | operator namespace | Lease namespace |
| `LEADER_ELECTION_DURATION_SECONDS` | `15` | Lease TTL (how long before failover) |
| `LEADER_ELECTION_RENEW_INTERVAL_SECONDS` | `5.0` | How often the leader renews the Lease |

### When to enable

- **HA deployments** — `replicaCount: 3` with `leaderElection.enabled: true`
- **Rolling updates** — new pod acquires lease before old pod terminates
- **Zero-downtime** — standby replicas are already warm and ready

## Istio Integration — How It Works

### DestinationRule outlierDetection

When Istio is enabled, the operator creates (or patches) a DestinationRule for the target deployment:

```yaml
apiVersion: networking.istio.io/v1beta1
kind: DestinationRule
metadata:
  name: payment-api-self-healing
  labels:
    app.kubernetes.io/managed-by: self-healing-operator
spec:
  host: payment-api
  trafficPolicy:
    outlierDetection:
      consecutive5xxErrors: 3
      interval: 30s
      baseEjectionTime: 120s
      maxEjectionPercent: 100
```

Istio's sidecar proxy will automatically eject endpoints that return consecutive 5xx errors, stopping traffic at the **network level** — no label propagation delay, no DNS churn.

### VirtualService Pod Ejection Label

As a defense-in-depth layer, the operator also labels the Pod:

```yaml
metadata:
  labels:
    self-healing.sre.k8s.io/istio-ejected: "true"
```

VirtualService configurations can use this label in subset matching to exclude ejected Pods from routing rules.

### Cleanup

When the `AppHealth` resource is deleted, the operator:
1. Removes the self-healing DestinationRule
2. Strips ejection labels from any affected Pods

This ensures no orphaned Istio config remains in the cluster.

## Ticket Integration — Provider Payloads

### Jira

POST to `/rest/api/2/issue` with Basic auth. Creates an Incident issue type:

```json
{
  "fields": {
    "project": {"key": "SRE"},
    "summary": "[Self-Healing] high-5xx-rate — Pod payment-api-abc123 (SH-A1B2C3D4E5F6)",
    "description": {
      "type": "doc",
      "version": 1,
      "content": [
        {"type": "paragraph", "content": [{"type": "text", "text": "Self-Healing Operator detected..."}]},
        {"type": "panel", "attrs": {"panelType": "info"}, "content": [...]}
      ]
    },
    "issuetype": {"name": "Incident"},
    "priority": {"name": "Highest"},
    "labels": ["self-healing-incident", "default", "payment-api"]
  }
}
```

### ServiceNow

POST to `/api/now/table/incident` with Bearer auth. Creates an active incident with custom fields:

```json
{
  "short_description": "[Self-Healing] high-5xx-rate — Pod payment-api-abc123 (SH-A1B2C3D4E5F6)",
  "description": "Self-Healing Operator detected...",
  "severity": "1",
  "incident_state": "2",
  "u_incident_id": "SH-A1B2C3D4E5F6",
  "u_rule_name": "high-5xx-rate",
  "u_source": "self-healing-operator",
  "comments": "Auto-created by Self-Healing Kubernetes Operator..."
}
```

### Retry Behavior

All webhook calls use exponential backoff with jitter:

| Attempt | Base Delay | With Jitter |
|---------|-----------|-------------|
| 1 | 1s | 0.9s–1.1s |
| 2 | 2s | 1.8s–2.2s |
| 3 | 4s | 3.6s–4.4s |

Retries are triggered only for retryable HTTP codes (408, 429, 500, 502, 503, 504) and network timeouts. Client errors (400, 401, 403, 404) fail immediately without retry.

### HMAC Signature

Every webhook request includes an `X-Signature` header with an HMAC-SHA256 of the request body, using the configured secret. The receiving system should verify this signature before processing.

## Helm Chart

### Installation

```bash
# Default (cluster-wide, no Istio)
helm install self-healing-operator ./helm/self-healing-operator \
  --namespace self-healing-system --create-namespace

# With Istio + Jira
helm install self-healing-operator ./helm/self-healing-operator \
  --namespace self-healing-system --create-namespace \
  --set istio.enabled=true \
  --set ticket.provider=jira \
  --set ticket.projectKey=OPS

# Single-namespace mode
helm install self-healing-operator ./helm/self-healing-operator \
  --namespace self-healing-system --create-namespace \
  --set watchNamespace=production

# Dry-run mode (safe testing)
helm install self-healing-operator ./helm/self-healing-operator \
--namespace self-healing-system --create-namespace \
--set dryRun=true

# Multi-replica with leader election (Sprint 4)
helm install self-healing-operator ./helm/self-healing-operator \
--namespace self-healing-system --create-namespace \
--set replicaCount=3 \
--set leaderElection.enabled=true
```

### Key Helm Values

| Value | Default | Description |
|-------|---------|-------------|
| `image.repository` | `self-healing-operator` | Container image |
| `image.tag` | `latest` | Image tag |
| `watchNamespace` | `""` | Cluster-wide or scoped namespace |
| `dryRun` | `false` | Dry-run mode |
| `metricsServer.enabled` | `true` | Enable operator metrics server |
| `metricsServer.port` | `9090` | Metrics server port |
| `metricsServer.serviceMonitor.enabled` | `false` | Create Prometheus Operator ServiceMonitor |
| `leaderElection.enabled` | `false` | Enable Lease-based leader election (required for >1 replica) |
| `leaderElection.leaseDurationSeconds` | `15` | Lease TTL in seconds |
| `leaderElection.renewalIntervalSeconds` | `5.0` | Heartbeat interval in seconds |
| `istio.enabled` | `false` | Enable Istio integration |
| `ticket.provider` | `generic` | Ticket provider |
| `persistence.enabled` | `true` | Enable PVC for log dumps |
| `persistence.size` | `1Gi` | PVC size |

See `helm/self-healing-operator/values.yaml` for the complete list.

## SRE Principles Applied

| Principle | Implementation |
|-----------|---------------|
| **Automation over toil** | Full remediation pipeline — zero human intervention |
| **Business metrics first** | 5xx rate, latency, error rate — not just CPU/memory |
| **Sustain windows** | `forSeconds` prevents flapping on transient spikes |
| **Cooldown periods** | Prevents remediation storms |
| **Max replica caps** | Prevents runaway scaling |
| **Defense in depth** | K8s label + Istio outlierDetection + VS label (3 layers of isolation) |
| **Retry with backoff** | Exponential backoff prevents webhook avalanche |
| **Audit trail** | `remediationHistory` in status, structured JSON logs with correlation ID |
| **Dry-run mode** | Validate rules without side effects |
| **Progressive remediation** | Sequential: isolate → Istio eject → dump → ticket → scale |
| **Cleanup on delete** | No orphaned Istio config or labels |
| **Error budgets** | `sloTargetPercent` field for SLO tracking |
| **Observability** | Operator exposes own Prometheus metrics + health probes (Sprint 3) |
| **Multi-tenancy** | Namespace-scoped or cluster-wide via `WATCH_NAMESPACE` (Sprint 3) |
| **High Availability** | Leader election prevents split-brain in multi-replica deployments (Sprint 4) |
| **Fast Failover** | Graceful lease release on shutdown — standby takes over immediately (Sprint 4) |

## Development

### Local Setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

### Run Tests

```bash
make test
# or: pytest tests/ -v --tb=short
```

### Lint & Type Check

```bash
make lint
```

### Run Locally (Dry-Run)

```bash
make dry-run
# or: DRY_RUN=true python -m kopf run --standalone src/operator.py
```

### Test Prometheus Connectivity

```bash
make check-prometheus
# or: python -m src.cli check --prometheus http://localhost:9090 -q "up"
```

### Helm Template Debug

```bash
make helm-template
# or: helm template self-healing-operator ./helm/self-healing-operator --debug
```

## Project Structure

```
.
├── deploy/
│   ├── crd.yaml              # Custom Resource Definition (v1alpha1)
│   ├── rbac.yaml             # ServiceAccount, ClusterRole, Binding
│   └── deployment.yaml       # Operator Deployment + PVC
├── helm/
│   └── self-healing-operator/
│       ├── Chart.yaml        # Helm chart metadata
│       ├── values.yaml       # Default values (all config)
│       └── templates/
│           ├── _helpers.tpl  # Helm helper functions
│           ├── NOTES.txt     # Post-install instructions
│           ├── namespace.yaml
│           ├── crd.yaml
│           ├── serviceaccount.yaml
│           ├── role.yaml     # ClusterRole + ClusterRoleBinding
│   ├── deployment.yaml
│   ├── service.yaml # Metrics server Service
│   ├── servicemonitor.yaml # Prometheus Operator ServiceMonitor (NEW)
│   └── pvc.yaml
├── examples/
│   ├── apphealth-payment-api.yaml   # HTTP API + Istio + Jira
│   ├── apphealth-order-service.yaml # gRPC + Istio + ServiceNow
│   └── apphealth-frontend.yaml      # Simple (no mesh, generic webhook)
├── src/
│   ├── __init__.py
│   ├── cli.py                # Typer CLI (run, check, version)
│   ├── config.py             # Env-var config (incl. MetricsServerConfig, IstioConfig, TicketConfig)
│   ├── errors.py             # Hierarchical error classes
│   ├── metrics_adapter.py    # Prometheus query + rule evaluation
│   ├── metrics_server.py     # Operator Prometheus metrics + health probes (NEW)
│   ├── operator.py           # Kopf handlers (with Istio, metrics, multi-namespace)
│   ├── remediation.py        # Pod isolation, Istio ejection, log dump, HScale
│   ├── istio_adapter.py # Istio DestinationRule + VirtualService ejection
│   ├── leader_election.py # Lease-based leader election (NEW)
│   └── ticket_integration.py # Jira/ServiceNow/generic + retry + HMAC
├── tests/
│   ├── test_config.py               # Config with all env overrides
│   ├── test_metrics_adapter.py      # Prometheus adapter tests
│   ├── test_metrics_server.py       # Metrics server + health probes (NEW)
│   ├── test_multi_namespace.py      # WATCH_NAMESPACE scope tests (NEW)
│   ├── test_remediation.py          # Pipeline tests (with and without Istio)
│   ├── test_ticket_integration.py   # Webhook + retry + provider payloads
│   ├── test_istio_adapter.py # Istio outlierDetection + VS eject
│   └── test_leader_election.py # Leader election + Lease + heartbeat (NEW)
├── .github/workflows/
│   └── ci.yml                # Lint → Test → Build → Push
├── Dockerfile                # Multi-stage, non-root, health endpoint
├── Makefile                  # Common operations + Helm commands
├── pyproject.toml            # Dependencies + tool config
├── .env.example              # Environment variable template
└── .dockerignore
```

## Security Considerations

- **Non-root container** — runs as UID 1001
- **Read-only root filesystem** — only `/tmp` and log volume are writable
- **Dropped all capabilities** — `capabilities.drop: [ALL]`
- **Seccomp profile** — `RuntimeDefault`
- **Restricted pod security** — namespace enforces restricted profile
- **HMAC webhook signing** — `X-Signature` with SHA256 for all ticket webhooks
- **No secrets in code** — all credentials via env vars or K8s Secrets
- **RBAC least privilege** — only the resources and verbs the operator needs (incl. Istio CRDs, Leases)
- **Leader election** — Lease-based election prevents split-brain in HA deployments
- **Retry isolation** — webhook failures don't block the remediation pipeline
- **Namespace isolation** — `WATCH_NAMESPACE` limits operator scope for multi-tenant clusters

## License

MIT
