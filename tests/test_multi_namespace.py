"""Tests for multi-namespace support in the operator module."""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest


def test_watch_namespace_default_is_none() -> None:
    """When WATCH_NAMESPACE is empty, operator watches cluster-wide (None in Kopf)."""
    with patch.dict(os.environ, {"WATCH_NAMESPACE": ""}, clear=False):
        # Re-import to pick up the env var at module level
        # Since operator.py sets WATCH_NAMESPACE at import time from CONFIG,
        # we test the config layer directly
        from src.config import OperatorConfig
        cfg = OperatorConfig()
        assert cfg.watch_namespace == ""
        # Kopf treats None as cluster-wide, empty string should also mean all namespaces
        watch_ns = cfg.watch_namespace or None
        assert watch_ns is None


def test_watch_namespace_scoped() -> None:
    """When WATCH_NAMESPACE is set, operator is scoped to that namespace."""
    with patch.dict(os.environ, {"WATCH_NAMESPACE": "production"}):
        from src.config import OperatorConfig
        cfg = OperatorConfig()
        assert cfg.watch_namespace == "production"
        watch_ns = cfg.watch_namespace or None
        assert watch_ns == "production"


def test_watch_namespace_self_healing_system() -> None:
    """WATCH_NAMESPACE can be set to the operator's own namespace."""
    with patch.dict(os.environ, {"WATCH_NAMESPACE": "self-healing-system"}):
        from src.config import OperatorConfig
        cfg = OperatorConfig()
        assert cfg.watch_namespace == "self-healing-system"


def test_operator_config_with_all_sprint3_fields() -> None:
    """OperatorConfig includes all Sprint 3 additions."""
    with patch.dict(os.environ, {
        "METRICS_SERVER_PORT": "9091",
        "METRICS_SERVER_HOST": "0.0.0.0",
        "WATCH_NAMESPACE": "staging",
    }):
        from src.config import OperatorConfig
        cfg = OperatorConfig()
        assert cfg.metrics_server.port == 9091
        assert cfg.metrics_server.host == "0.0.0.0"
        assert cfg.watch_namespace == "staging"
