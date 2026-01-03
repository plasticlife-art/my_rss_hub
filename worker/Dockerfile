# ---- build stage ----
FROM mcr.microsoft.com/playwright/python:v1.57.0-jammy AS builder

WORKDIR /app

# uv
RUN pip install --no-cache-dir uv

# ВАЖНО: говорим uv, где будет project environment (venv)
ENV UV_PROJECT_ENVIRONMENT=/venv

# Копируем проект
COPY app/pyproject.toml /app/pyproject.toml
COPY app/src /app/src

# На всякий случай создаём окружение (uv сам умеет, но так стабильнее)
RUN uv venv /venv

# Синхронизируем зависимости в /venv
RUN uv sync --project /app


# ---- runtime stage ----
FROM mcr.microsoft.com/playwright/python:v1.57.0-jammy AS runtime

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Etc/UTC


RUN apt-get update \
 && apt-get install -y --no-install-recommends tzdata \
 && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime \
 && echo $TZ > /etc/timezone \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Копируем готовое окружение и код
COPY --from=builder /venv /venv
COPY --from=builder /app /app

# runner
COPY worker/run.sh /run.sh
RUN chmod +x /run.sh

# out будет volume
ENV OUT_DIR=/out
ENV PYTHONPATH=/app/src
ENTRYPOINT ["/run.sh"]
