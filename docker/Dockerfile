# ═══════════════════════════════════════════════════════════════════
# 阶段 1：builder —— 只负责装依赖
# ═══════════════════════════════════════════════════════════════════
FROM ghcr.io/astral-sh/uv:python3.14-bookworm-slim AS builder

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# ═══════════════════════════════════════════════════════════════════
# 阶段 2：runtime —— 最小运行环境
# ═══════════════════════════════════════════════════════════════════
FROM python:3.14-slim AS runtime

ENV TZ=Asia/Shanghai
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PATH="/app/.venv/bin:$PATH"

WORKDIR /app

COPY --from=builder /app/.venv /app/.venv
COPY common/ ./common/
COPY jobs/ ./jobs/

RUN useradd --create-home --shell /bin/bash --uid 1000 app && \
    chown -R app:app /app
USER app

CMD ["python", "-m", "jobs.scheduler"]
