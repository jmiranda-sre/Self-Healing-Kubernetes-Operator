#!/usr/bin/env python3
"""CLI entry point for the Self-Healing Kubernetes Operator."""

import typer
import structlog

from src.config import load_config

app = typer.Typer(help="Self-Healing Kubernetes Operator CLI")
logger = structlog.get_logger()


@app.command()
def run(
    dry_run: bool = typer.Option(False, "--dry-run", help="Run in dry-run mode (no K8s mutations)"),
    log_level: str = typer.Option("info", "--log-level", help="Log level: debug|info|warn|error"),
) -> None:
    """Start the Kopf operator."""
    import os
    if dry_run:
        os.environ["DRY_RUN"] = "true"
    os.environ["LOG_LEVEL"] = log_level

    config = load_config()
    logger.info("cli.run_start", dry_run=config.dry_run, log_level=config.log_level)

    # Kopf reads handlers from imported modules — we import operator to register them
    import src.operator  # noqa: F401

    # Kopf CLI entry point
    import kopf
    kopf.run()


@app.command()
def check(
    prometheus_url: str = typer.Option("http://localhost:9090", "--prometheus", help="Prometheus URL"),
    query: str = typer.Option("up", "--query", "-q", help="PromQL query to test"),
) -> None:
    """Test Prometheus connectivity with a sample query."""
    import asyncio
    from src.metrics_adapter import MetricsAdapter, MetricsConfig

    async def _check() -> None:
        cfg = MetricsConfig(prometheus_url=prometheus_url)
        adapter = MetricsAdapter(cfg)
        try:
            results = await adapter.query(query)
            typer.echo(f"✓ Prometheus OK — {len(results)} result(s)")
            for r in results[:5]:
                typer.echo(f"  {r.get('metric', {})} = {r.get('value', [None, None])[1]}")
        except Exception as exc:
            typer.echo(f"✗ Prometheus error: {exc}", err=True)
            raise typer.Exit(code=1)
        finally:
            await adapter.close()

    asyncio.run(_check())


@app.command()
def version() -> None:
    """Print operator version."""
    from src import __version__
    typer.echo(f"self-healing-operator v{__version__}")


if __name__ == "__main__":
    app()
