"""Leader election via coordination.k8s.io/Lease — prevents split-brain in multi-replica deployments.

When leader election is enabled (LEADER_ELECT=true), only the replica holding
the Lease runs reconciliation handlers. Standby replicas expose /metrics and
/health but report /ready as {status: "standby"}.

Implementation follows the Kubernetes controller-runtime pattern:
  1. On startup, attempt to acquire or renew the Lease
  2. If acquired, become leader and run a heartbeat loop
  3. If renewal fails, transition to standby
  4. On shutdown, release the Lease gracefully
"""

from __future__ import annotations

import asyncio
import os
import time
import uuid
from typing import Any, Callable, Coroutine

import kubernetes.client
import kubernetes.config
import structlog

from src.errors import LeaderElectionError

logger = structlog.get_logger()

LEASE_GROUP = "coordination.k8s.io"
LEASE_VERSION = "v1"
LEASE_PLURAL = "leases"

# ── Identity ──

# Stable identity per pod: use hostname (Pod name in K8s) or fallback to UUID
_IDENTITY = os.getenv("HOSTNAME", "") or os.getenv("POD_NAME", "") or str(uuid.uuid4())[:12]


def get_identity() -> str:
    """Return the current replica's unique identity string."""
    return _IDENTITY


# ── K8s API helper ──


def _k8s_coordination_api() -> kubernetes.client.CoordinationV1Api:
    kubernetes.config.load_incluster_config()
    return kubernetes.client.CoordinationV1Api()


def _k8s_core_api() -> kubernetes.client.CoreV1Api:
    kubernetes.config.load_incluster_config()
    return kubernetes.client.CoreV1Api()


def _get_operator_namespace() -> str:
    """Determine the namespace the operator is running in."""
    ns_file = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
    if os.path.exists(ns_file):
        return open(ns_file).read().strip()  # noqa: SIM115
    return os.getenv("OPERATOR_NAMESPACE", "self-healing-system")


# ── Lease operations ──


def _build_lease(
    lease_name: str,
    namespace: str,
    holder_identity: str,
    lease_duration_seconds: int,
    acquire_time: str | None = None,
    renew_time: str | None = None,
    lease_transitions: int = 0,
) -> dict[str, Any]:
    """Build a Lease object body for create or update."""
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    return {
        "apiVersion": f"{LEASE_GROUP}/{LEASE_VERSION}",
        "kind": "Lease",
        "metadata": {
            "name": lease_name,
            "namespace": namespace,
            "labels": {
                "app.kubernetes.io/managed-by": "self-healing-operator",
            },
        },
        "spec": {
            "holderIdentity": holder_identity,
            "leaseDurationSeconds": lease_duration_seconds,
            "acquireTime": acquire_time or now,
            "renewTime": renew_time or now,
            "leaseTransitions": lease_transitions,
        },
    }


async def try_acquire_lease(
    lease_name: str,
    namespace: str,
    holder_identity: str,
    lease_duration_seconds: int,
) -> bool:
    """Attempt to acquire the leader Lease. Returns True if this replica is now the leader.

    Logic:
    - If Lease doesn't exist → create it (we become leader)
    - If Lease exists and held by us → renew it
    - If Lease exists and held by other → check if it expired
      - Expired → take over (increment transitions)
      - Not expired → we are standby
    """
    api = _k8s_coordination_api()
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    try:
        existing = api.read_namespaced_lease(
            name=lease_name,
            namespace=namespace,
        )
    except kubernetes.client.ApiException as exc:
        if exc.status == 404:
            # Lease doesn't exist — create it and become leader
            body = _build_lease(
                lease_name=lease_name,
                namespace=namespace,
                holder_identity=holder_identity,
                lease_duration_seconds=lease_duration_seconds,
                acquire_time=now,
                renew_time=now,
                lease_transitions=0,
            )
            try:
                api.create_namespaced_lease(
                    namespace=namespace,
                    body=body,
                )
            except kubernetes.client.ApiException as create_exc:
                raise LeaderElectionError(
                    f"Failed to create Lease {lease_name}",
                    context={"namespace": namespace, "status": create_exc.status},
                ) from create_exc

            logger.info(
                "leader_election.acquired",
                lease=lease_name,
                namespace=namespace,
                identity=holder_identity,
            )
            return True

        raise LeaderElectionError(
            f"Failed to read Lease {lease_name}",
            context={"namespace": namespace, "status": exc.status},
        ) from exc

    # Lease exists — check holder
    spec = existing.spec
    current_holder = spec.holder_identity or ""
    last_renew = spec.renew_time or ""
    duration = spec.lease_duration_seconds or lease_duration_seconds
    transitions = spec.lease_transitions or 0

    if current_holder == holder_identity:
        # We hold the lease — renew
        renew_body = {
            "spec": {
                "renewTime": now,
                "leaseDurationSeconds": duration,
            },
        }
        try:
            api.patch_namespaced_lease(
                name=lease_name,
                namespace=namespace,
                body=renew_body,
            )
        except kubernetes.client.ApiException as exc:
            raise LeaderElectionError(
                f"Failed to renew Lease {lease_name}",
                context={"namespace": namespace, "status": exc.status},
            ) from exc

        logger.debug("leader_election.renewed", lease=lease_name, identity=holder_identity)
        return True

    # Held by another — check if expired
    if _is_lease_expired(last_renew, duration):
        # Take over the lease
        takeover_body = _build_lease(
            lease_name=lease_name,
            namespace=namespace,
            holder_identity=holder_identity,
            lease_duration_seconds=lease_duration_seconds,
            acquire_time=now,
            renew_time=now,
            lease_transitions=transitions + 1,
        )
        try:
            api.replace_namespaced_lease(
                name=lease_name,
                namespace=namespace,
                body=takeover_body,
            )
        except kubernetes.client.ApiException as exc:
            raise LeaderElectionError(
                f"Failed to takeover Lease {lease_name}",
                context={"namespace": namespace, "status": exc.status},
            ) from exc

        logger.info(
            "leader_election.takeover",
            lease=lease_name,
            namespace=namespace,
            previous_holder=current_holder,
            identity=holder_identity,
            transitions=transitions + 1,
        )
        return True

    # Lease is valid and held by another replica — we are standby
    logger.debug(
        "leader_election.standby",
        lease=lease_name,
        holder=current_holder,
        identity=holder_identity,
    )
    return False


async def release_lease(
    lease_name: str,
    namespace: str,
    holder_identity: str,
) -> None:
    """Release the leader Lease on graceful shutdown.

    Clears the holderIdentity so another replica can acquire immediately.
    """
    api = _k8s_coordination_api()

    try:
        existing = api.read_namespaced_lease(
            name=lease_name,
            namespace=namespace,
        )
    except kubernetes.client.ApiException as exc:
        if exc.status == 404:
            logger.debug("leader_election.release_not_found", lease=lease_name)
            return
        raise LeaderElectionError(
            f"Failed to read Lease for release {lease_name}",
            context={"namespace": namespace, "status": exc.status},
        ) from exc

    if existing.spec.holder_identity != holder_identity:
        logger.debug(
            "leader_election.release_not_holder",
            lease=lease_name,
            holder=existing.spec.holder_identity,
            identity=holder_identity,
        )
        return

    release_body = {
        "spec": {
            "holderIdentity": "",
            "renewTime": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        },
    }
    try:
        api.patch_namespaced_lease(
            name=lease_name,
            namespace=namespace,
            body=release_body,
        )
    except kubernetes.client.ApiException as exc:
        raise LeaderElectionError(
            f"Failed to release Lease {lease_name}",
            context={"namespace": namespace, "status": exc.status},
        ) from exc

    logger.info(
        "leader_election.released",
        lease=lease_name,
        namespace=namespace,
        identity=holder_identity,
    )


def _is_lease_expired(renew_time: str, duration_seconds: int) -> bool:
    """Check if a Lease has expired based on its renewTime and leaseDurationSeconds."""
    if not renew_time:
        return True

    try:
        # Parse ISO format timestamp
        renew = time.strptime(renew_time.replace("Z", ""), "%Y-%m-%dT%H:%M:%S")
        renew_epoch = time.mktime(time.gmtime(time.mktime(renew)))
        now_epoch = time.time()
        return (now_epoch - renew_epoch) > duration_seconds
    except (ValueError, TypeError, OverflowError):
        logger.warn("leader_election.invalid_renew_time", renew_time=renew_time)
        return True


# ── LeaderElector — orchestrates the election lifecycle ──


class LeaderElector:
    """Manages leader election state and the heartbeat loop.

    Usage:
        elector = LeaderElector(config=LeaderElectionConfig())
        await elector.start()          # attempts acquisition, starts heartbeat
        if elector.is_leader():        # check status
            ...                        # run reconciliation handlers
        await elector.stop()           # graceful shutdown, releases lease
    """

    def __init__(
        self,
        lease_name: str = "self-healing-operator",
        namespace: str = "",
        identity: str = "",
        lease_duration_seconds: int = 15,
        renewal_interval_seconds: float = 5.0,
        enabled: bool = True,
    ) -> None:
        self.lease_name = lease_name
        self.namespace = namespace or _get_operator_namespace()
        self.identity = identity or _IDENTITY
        self.lease_duration_seconds = lease_duration_seconds
        self.renewal_interval_seconds = renewal_interval_seconds
        self.enabled = enabled

        self._is_leader: bool = False
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._stop_event: asyncio.Event = asyncio.Event()
        self._acquired_at: float | None = None

        # Callbacks
        self._on_leadership_gained: list[Callable[[], Coroutine[Any, Any, None]]] = []
        self._on_leadership_lost: list[Callable[[], Coroutine[Any, Any, None]]] = []

    # ── Public API ──

    def is_leader(self) -> bool:
        """Return True if this replica currently holds the leader Lease."""
        return self._is_leader

    @property
    def acquired_at(self) -> float | None:
        """Epoch time when leadership was acquired, or None."""
        return self._acquired_at

    def on_leadership_gained(self, callback: Callable[[], Coroutine[Any, Any, None]]) -> None:
        """Register a coroutine callback to be called when this replica becomes leader."""
        self._on_leadership_gained.append(callback)

    def on_leadership_lost(self, callback: Callable[[], Coroutine[Any, Any, None]]) -> None:
        """Register a coroutine callback to be called when this replica loses leadership."""
        self._on_leadership_lost.append(callback)

    async def start(self) -> None:
        """Attempt to acquire the Lease and start the heartbeat loop."""
        if not self.enabled:
            # Leader election disabled — always leader (single-replica mode)
            self._is_leader = True
            self._acquired_at = time.time()
            logger.info("leader_election.disabled_single_replica")
            return

        logger.info(
            "leader_election.starting",
            lease=self.lease_name,
            namespace=self.namespace,
            identity=self.identity,
            duration=self.lease_duration_seconds,
            renewal_interval=self.renewal_interval_seconds,
        )

        # Initial acquisition attempt
        await self._try_acquire()

        # Start heartbeat loop
        self._stop_event.clear()
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def stop(self) -> None:
        """Stop the heartbeat loop and release the Lease."""
        if not self.enabled:
            return

        logger.info("leader_election.stopping", identity=self.identity)
        self._stop_event.set()

        if self._heartbeat_task and not self._heartbeat_task.done():
            try:
                await asyncio.wait_for(self._heartbeat_task, timeout=10.0)
            except asyncio.TimeoutError:
                self._heartbeat_task.cancel()

        # Release the Lease if we hold it
        if self._is_leader:
            try:
                await release_lease(
                    lease_name=self.lease_name,
                    namespace=self.namespace,
                    holder_identity=self.identity,
                )
            except LeaderElectionError as exc:
                logger.error(
                    "leader_election.release_failed",
                    error=str(exc),
                    **exc.context,
                )

            was_leader = self._is_leader
            self._is_leader = False
            self._acquired_at = None

            if was_leader:
                for callback in self._on_leadership_lost:
                    try:
                        await callback()
                    except Exception:  # noqa: BLE001
                        logger.warn("leader_election.on_lost_callback_error")

    # ── Internal ──

    async def _try_acquire(self) -> None:
        """Attempt to acquire or renew the Lease."""
        try:
            acquired = await try_acquire_lease(
                lease_name=self.lease_name,
                namespace=self.namespace,
                holder_identity=self.identity,
                lease_duration_seconds=self.lease_duration_seconds,
            )

            if acquired and not self._is_leader:
                # Transition: standby → leader
                self._is_leader = True
                self._acquired_at = time.time()
                logger.info("leader_election.became_leader", identity=self.identity)
                for callback in self._on_leadership_gained:
                    try:
                        await callback()
                    except Exception:  # noqa: BLE001
                        logger.warn("leader_election.on_gained_callback_error")

            elif not acquired and self._is_leader:
                # Transition: leader → standby (we lost the Lease)
                self._is_leader = False
                self._acquired_at = None
                logger.warn("leader_election.lost_leadership", identity=self.identity)
                for callback in self._on_leadership_lost:
                    try:
                        await callback()
                    except Exception:  # noqa: BLE001
                        logger.warn("leader_election.on_lost_callback_error")

        except LeaderElectionError as exc:
            logger.error(
                "leader_election.acquire_failed",
                error=str(exc),
                **exc.context,
            )
            if self._is_leader:
                # We thought we were leader but can't renew — step down
                self._is_leader = False
                self._acquired_at = None
                logger.warn("leader_election.stepping_down", identity=self.identity)
                for callback in self._on_leadership_lost:
                    try:
                        await callback()
                    except Exception:  # noqa: BLE001
                        logger.warn("leader_election.on_lost_callback_error")

    async def _heartbeat_loop(self) -> None:
        """Periodically renew the Lease or attempt acquisition."""
        while not self._stop_event.is_set():
            try:
                await self._try_acquire()
            except Exception as exc:  # noqa: BLE001
                logger.error("leader_election.heartbeat_error", error=str(exc))

            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=self.renewal_interval_seconds,
                )
                # If wait_for returns without timeout, stop_event was set
                break
            except asyncio.TimeoutError:
                # Normal — renewal interval elapsed, loop again
                pass

    def get_status(self) -> dict[str, Any]:
        """Return a dict describing the current leader election state (for /ready)."""
        return {
            "is_leader": self._is_leader,
            "identity": self.identity,
            "lease_name": self.lease_name,
            "namespace": self.namespace,
            "enabled": self.enabled,
            "acquired_at": self._acquired_at,
            "lease_duration_seconds": self.lease_duration_seconds,
            "renewal_interval_seconds": self.renewal_interval_seconds,
        }
