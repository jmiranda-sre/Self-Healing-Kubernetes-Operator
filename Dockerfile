# ── Stage 1: Build / Dependencies ──
FROM python:3.12-slim AS builder

WORKDIR /app

# Cache: install deps first
COPY pyproject.toml README.md ./
COPY src/ src/

RUN pip install --no-cache-dir --user .

# ── Stage 2: Production ──
FROM python:3.12-slim AS production

# Security: non-root user
RUN addgroup --gid 1001 operatorgroup && \
    adduser --uid 1001 --ingroup operatorgroup --disabled-password --gecos "" operatoruser

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /home/operatoruser/.local /home/operatoruser/.local
COPY --from=builder /app/src/ ./src/
COPY --from=builder /app/pyproject.toml ./

# Ensure non-root user owns the app directory
RUN chown -R operatoruser:operatorgroup /app

# Log dump directory (mounted as volume in production)
RUN mkdir -p /var/log/self-healing && \
    chown -R operatoruser:operatorgroup /var/log/self-healing

# Install curl for health check
RUN apt-get update && apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

USER operatoruser

ENV PATH=/home/operatoruser/.local/bin:$PATH
ENV PYTHONUNBUFFERED=1
ENV LOG_LEVEL=info

# Health check — Kopf doesn't expose HTTP by default,
# so we check process existence
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD curl -sf http://localhost:9090/health || exit 1

ENTRYPOINT ["python", "-m", "kopf", "run", "--standalone", "src/operator.py"]
