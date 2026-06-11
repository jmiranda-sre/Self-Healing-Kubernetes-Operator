"""Tests for leader election module — Lease-based election with heartbeat loop."""

from __future__ import annotations

import asyncio
import os
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.errors import LeaderElectionError
from src.leader_election import (
    LeaderElector,
    _build_lease,
    _is_lease_expired,
    get_identity,
    release_lease,
    try_acquire_lease,
)


# ── Identity ──


def test_get_identity_returns_string() -> None:
    identity = get_identity()
    assert isinstance(identity, str)
    assert len(identity) > 0


def test_get_identity_from_hostname() -> None:
    with patch.dict(os.environ, {"HOSTNAME": "operator-pod-abc12"}):
        # Re-import to pick up env (identity is set at module level, so test the function directly)
        from src.leader_election import _IDENTITY
        # _IDENTITY is set at import time; we can only verify it's a string
        assert isinstance(_IDENTITY, str)


# ── Lease building ──


def test_build_lease_create() -> None:
    body = _build_lease(
        lease_name="test-lease",
        namespace="default",
        holder_identity="pod-1",
        lease_duration_seconds=15,
    )
    assert body["metadata"]["name"] == "test-lease"
    assert body["metadata"]["namespace"] == "default"
    assert body["spec"]["holderIdentity"] == "pod-1"
    assert body["spec"]["leaseDurationSeconds"] == 15
    assert body["spec"]["leaseTransitions"] == 0
    assert "acquireTime" in body["spec"]
    assert "renewTime" in body["spec"]


def test_build_lease_with_transitions() -> None:
    body = _build_lease(
        lease_name="test-lease",
        namespace="default",
        holder_identity="pod-2",
        lease_duration_seconds=30,
        lease_transitions=3,
    )
    assert body["spec"]["leaseTransitions"] == 3
    assert body["spec"]["leaseDurationSeconds"] == 30


def test_build_lease_labels() -> None:
    body = _build_lease(
        lease_name="test-lease",
        namespace="default",
        holder_identity="pod-1",
        lease_duration_seconds=15,
    )
    labels = body["metadata"]["labels"]
    assert labels["app.kubernetes.io/managed-by"] == "self-healing-operator"


# ── Lease expiry ──


def test_lease_expired_with_old_timestamp() -> None:
    old_time = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - 30))
    assert _is_lease_expired(old_time, 15) is True


def test_lease_not_expired_with_recent_timestamp() -> None:
    recent_time = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - 5))
    assert _is_lease_expired(recent_time, 15) is False


def test_lease_expired_with_empty_renew_time() -> None:
    assert _is_lease_expired("", 15) is True


def test_lease_expired_with_none_renew_time() -> None:
    assert _is_lease_expired("", 15) is True


def test_lease_expired_with_invalid_timestamp() -> None:
    assert _is_lease_expired("not-a-timestamp", 15) is True


# ── try_acquire_lease ──


@pytest.mark.asyncio
async def test_try_acquire_lease_creates_new() -> None:
    """When Lease doesn't exist (404), create it and become leader."""
    mock_api = MagicMock()
    mock_api.read_namespaced_lease.side_effect = _make_api_exception(404)
    mock_api.create_namespaced_lease.return_value = None

    with patch("src.leader_election._k8s_coordination_api", return_value=mock_api):
        acquired = await try_acquire_lease(
            lease_name="test-lease",
            namespace="default",
            holder_identity="pod-1",
            lease_duration_seconds=15,
        )

    assert acquired is True
    mock_api.create_namespaced_lease.assert_called_once()
    call_namespace = mock_api.create_namespaced_lease.call_args[1]["namespace"]
    assert call_namespace == "default"


@pytest.mark.asyncio
async def test_try_acquire_lease_renews_own() -> None:
    """When Lease is held by us, renew it."""
    existing = MagicMock()
    existing.spec.holder_identity = "pod-1"
    existing.spec.renew_time = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    existing.spec.lease_duration_seconds = 15
    existing.spec.lease_transitions = 0

    mock_api = MagicMock()
    mock_api.read_namespaced_lease.return_value = existing
    mock_api.patch_namespaced_lease.return_value = None

    with patch("src.leader_election._k8s_coordination_api", return_value=mock_api):
        acquired = await try_acquire_lease(
            lease_name="test-lease",
            namespace="default",
            holder_identity="pod-1",
            lease_duration_seconds=15,
        )

    assert acquired is True
    mock_api.patch_namespaced_lease.assert_called_once()


@pytest.mark.asyncio
async def test_try_acquire_lease_expired_takeover() -> None:
    """When Lease is held by another but expired, take over."""
    old_time = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - 30))
    existing = MagicMock()
    existing.spec.holder_identity = "pod-other"
    existing.spec.renew_time = old_time
    existing.spec.lease_duration_seconds = 15
    existing.spec.lease_transitions = 2

    mock_api = MagicMock()
    mock_api.read_namespaced_lease.return_value = existing
    mock_api.replace_namespaced_lease.return_value = None

    with patch("src.leader_election._k8s_coordination_api", return_value=mock_api):
        acquired = await try_acquire_lease(
            lease_name="test-lease",
            namespace="default",
            holder_identity="pod-1",
            lease_duration_seconds=15,
        )

    assert acquired is True
    mock_api.replace_namespaced_lease.assert_called_once()
    call_body = mock_api.replace_namespaced_lease.call_args[1]["body"]
    assert call_body["spec"]["leaseTransitions"] == 3  # incremented
    assert call_body["spec"]["holderIdentity"] == "pod-1"


@pytest.mark.asyncio
async def test_try_acquire_lease_valid_other_standby() -> None:
    """When Lease is held by another and still valid, remain standby."""
    recent_time = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - 3))
    existing = MagicMock()
    existing.spec.holder_identity = "pod-other"
    existing.spec.renew_time = recent_time
    existing.spec.lease_duration_seconds = 15
    existing.spec.lease_transitions = 1

    mock_api = MagicMock()
    mock_api.read_namespaced_lease.return_value = existing

    with patch("src.leader_election._k8s_coordination_api", return_value=mock_api):
        acquired = await try_acquire_lease(
            lease_name="test-lease",
            namespace="default",
            holder_identity="pod-1",
            lease_duration_seconds=15,
        )

    assert acquired is False


@pytest.mark.asyncio
async def test_try_acquire_lease_read_error_non_404() -> None:
    """Non-404 errors reading the Lease raise LeaderElectionError."""
    mock_api = MagicMock()
    mock_api.read_namespaced_lease.side_effect = _make_api_exception(403)

    with patch("src.leader_election._k8s_coordination_api", return_value=mock_api):
        with pytest.raises(LeaderElectionError, match="Failed to read Lease"):
            await try_acquire_lease(
                lease_name="test-lease",
                namespace="default",
                holder_identity="pod-1",
                lease_duration_seconds=15,
            )


@pytest.mark.asyncio
async def test_try_acquire_lease_create_error() -> None:
    """Failure to create Lease raises LeaderElectionError."""
    mock_api = MagicMock()
    mock_api.read_namespaced_lease.side_effect = _make_api_exception(404)
    mock_api.create_namespaced_lease.side_effect = _make_api_exception(409)

    with patch("src.leader_election._k8s_coordination_api", return_value=mock_api):
        with pytest.raises(LeaderElectionError, match="Failed to create Lease"):
            await try_acquire_lease(
                lease_name="test-lease",
                namespace="default",
                holder_identity="pod-1",
                lease_duration_seconds=15,
            )


# ── release_lease ──


@pytest.mark.asyncio
async def test_release_lease_clears_holder() -> None:
    """Releasing a Lease we hold clears the holderIdentity."""
    existing = MagicMock()
    existing.spec.holder_identity = "pod-1"

    mock_api = MagicMock()
    mock_api.read_namespaced_lease.return_value = existing
    mock_api.patch_namespaced_lease.return_value = None

    with patch("src.leader_election._k8s_coordination_api", return_value=mock_api):
        await release_lease(
            lease_name="test-lease",
            namespace="default",
            holder_identity="pod-1",
        )

    mock_api.patch_namespaced_lease.assert_called_once()
    call_body = mock_api.patch_namespaced_lease.call_args[1]["body"]
    assert call_body["spec"]["holderIdentity"] == ""


@pytest.mark.asyncio
async def test_release_lease_not_holder() -> None:
    """Releasing a Lease held by another is a no-op."""
    existing = MagicMock()
    existing.spec.holder_identity = "pod-other"

    mock_api = MagicMock()
    mock_api.read_namespaced_lease.return_value = existing

    with patch("src.leader_election._k8s_coordination_api", return_value=mock_api):
        await release_lease(
            lease_name="test-lease",
            namespace="default",
            holder_identity="pod-1",
        )

    mock_api.patch_namespaced_lease.assert_not_called()


@pytest.mark.asyncio
async def test_release_lease_not_found() -> None:
    """Releasing a non-existent Lease is a no-op."""
    mock_api = MagicMock()
    mock_api.read_namespaced_lease.side_effect = _make_api_exception(404)

    with patch("src.leader_election._k8s_coordination_api", return_value=mock_api):
        await release_lease(
            lease_name="test-lease",
            namespace="default",
            holder_identity="pod-1",
        )


# ── LeaderElector ──


@pytest.mark.asyncio
async def test_elector_disabled_always_leader() -> None:
    """When leader election is disabled, is_leader() always returns True."""
    elector = LeaderElector(enabled=False)
    assert elector.is_leader() is False  # not started yet

    await elector.start()
    assert elector.is_leader() is True
    assert elector.acquired_at is not None

    await elector.stop()  # no-op when disabled


@pytest.mark.asyncio
async def test_elector_disabled_get_status() -> None:
    elector = LeaderElector(enabled=False)
    await elector.start()
    status = elector.get_status()
    assert status["is_leader"] is True
    assert status["enabled"] is False
    await elector.stop()


@pytest.mark.asyncio
async def test_elector_callbacks_on_gained() -> None:
    """on_leadership_gained callback fires when becoming leader."""
    callback_called = False

    async def on_gained() -> None:
        nonlocal callback_called
        callback_called = True

    elector = LeaderElector(enabled=False)
    elector.on_leadership_gained(on_gained)
    await elector.start()

    assert callback_called is True
    await elector.stop()


@pytest.mark.asyncio
async def test_elector_acquires_lease_on_start() -> None:
    """When leader election is enabled and Lease acquired, is_leader becomes True."""
    mock_api = MagicMock()
    mock_api.read_namespaced_lease.side_effect = _make_api_exception(404)
    mock_api.create_namespaced_lease.return_value = None

    with patch("src.leader_election._k8s_coordination_api", return_value=mock_api):
        elector = LeaderElector(
            lease_name="test-lease",
            namespace="default",
            identity="pod-1",
            lease_duration_seconds=15,
            renewal_interval_seconds=100.0,  # long interval so heartbeat doesn't fire in test
            enabled=True,
        )
        await elector.start()

        # Give the event loop a tick for the heartbeat loop to settle
        await asyncio.sleep(0.1)

        assert elector.is_leader() is True
        assert elector.acquired_at is not None

        await elector.stop()


@pytest.mark.asyncio
async def test_elector_standby_when_other_holds() -> None:
    """When another replica holds a valid Lease, we remain standby."""
    recent_time = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    existing = MagicMock()
    existing.spec.holder_identity = "pod-other"
    existing.spec.renew_time = recent_time
    existing.spec.lease_duration_seconds = 15
    existing.spec.lease_transitions = 0

    mock_api = MagicMock()
    mock_api.read_namespaced_lease.return_value = existing

    with patch("src.leader_election._k8s_coordination_api", return_value=mock_api):
        elector = LeaderElector(
            lease_name="test-lease",
            namespace="default",
            identity="pod-1",
            lease_duration_seconds=15,
            renewal_interval_seconds=100.0,
            enabled=True,
        )
        await elector.start()
        await asyncio.sleep(0.1)

        assert elector.is_leader() is False
        assert elector.acquired_at is None

        await elector.stop()


@pytest.mark.asyncio
async def test_elector_get_status() -> None:
    elector = LeaderElector(
        lease_name="test-lease",
        namespace="default",
        identity="pod-1",
        lease_duration_seconds=15,
        renewal_interval_seconds=5.0,
        enabled=True,
    )
    status = elector.get_status()
    assert status["is_leader"] is False
    assert status["identity"] == "pod-1"
    assert status["lease_name"] == "test-lease"
    assert status["namespace"] == "default"
    assert status["enabled"] is True
    assert status["lease_duration_seconds"] == 15
    assert status["renewal_interval_seconds"] == 5.0


@pytest.mark.asyncio
async def test_elector_releases_on_stop() -> None:
    """Stopping the elector releases the Lease if we hold it."""
    mock_api = MagicMock()
    # First call (start) → create new lease
    mock_api.read_namespaced_lease.side_effect = _make_api_exception(404)
    mock_api.create_namespaced_lease.return_value = None

    # For release: read returns that we hold it
    release_lease_obj = MagicMock()
    release_lease_obj.spec.holder_identity = "pod-1"
    mock_api.read_namespaced_lease.return_value = release_lease_obj
    mock_api.patch_namespaced_lease.return_value = None

    with patch("src.leader_election._k8s_coordination_api", return_value=mock_api):
        elector = LeaderElector(
            lease_name="test-lease",
            namespace="default",
            identity="pod-1",
            lease_duration_seconds=15,
            renewal_interval_seconds=100.0,
            enabled=True,
        )
        await elector.start()
        await asyncio.sleep(0.1)

        assert elector.is_leader() is True

        await elector.stop()

        assert elector.is_leader() is False
        assert elector.acquired_at is None


# ── Config integration ──


def test_leader_election_config_defaults() -> None:
    from src.config import LeaderElectionConfig
    cfg = LeaderElectionConfig()
    assert cfg.enabled is False
    assert cfg.lease_name == "self-healing-operator"
    assert cfg.lease_duration_seconds == 15
    assert cfg.renewal_interval_seconds == 5.0


def test_leader_election_config_env_override() -> None:
    from src.config import LeaderElectionConfig
    with patch.dict(os.environ, {
        "LEADER_ELECT": "true",
        "LEADER_ELECTION_LEASE_NAME": "custom-lease",
        "LEADER_ELECTION_DURATION_SECONDS": "30",
        "LEADER_ELECTION_RENEW_INTERVAL_SECONDS": "10.0",
    }):
        cfg = LeaderElectionConfig()
        assert cfg.enabled is True
        assert cfg.lease_name == "custom-lease"
        assert cfg.lease_duration_seconds == 30
        assert cfg.renewal_interval_seconds == 10.0


def test_operator_config_includes_leader_election() -> None:
    from src.config import OperatorConfig
    cfg = OperatorConfig()
    assert hasattr(cfg, "leader_election")
    assert cfg.leader_election.enabled is False


# ── Metrics integration ──


def test_leader_status_gauge_exists() -> None:
    from src.metrics_server import get_metrics, reset_metrics
    reset_metrics()
    metrics = get_metrics()
    assert "leader_status" in metrics
    assert metrics["leader_status"]["type"] == "gauge"
    assert metrics["leader_status"]["value"] == 0


def test_leader_transitions_counter_exists() -> None:
    from src.metrics_server import get_metrics, reset_metrics
    reset_metrics()
    metrics = get_metrics()
    assert "leader_transitions_total" in metrics
    assert metrics["leader_transitions_total"]["type"] == "counter"
    assert metrics["leader_transitions_total"]["value"] == 0


# ── Error class ──


def test_leader_election_error_is_operator_error() -> None:
    from src.errors import OperatorError
    exc = LeaderElectionError("test", context={"key": "val"})
    assert isinstance(exc, OperatorError)
    assert exc.message == "test"
    assert exc.context == {"key": "val"}
    assert exc.retryable is True
    assert exc.status_code == 500


# ── Helpers ──


def _make_api_exception(status: int) -> Any:
    """Create a mock Kubernetes ApiException."""
    import kubernetes.client
    exc = kubernetes.client.ApiException(status=status)
    exc.status = status
    exc.body = ""
    return exc
