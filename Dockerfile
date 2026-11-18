FROM python:3.11-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless ca-certificates kcat \
    && rm -rf /var/lib/apt/lists/*
# Java is on PATH on both amd64 and arm64.
ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app/src
WORKDIR /app
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt
RUN useradd --create-home --uid 10001 app && mkdir /app/checkpoints && chown -R app:app /app
COPY --chown=app:app src /app/src
COPY --chown=app:app sql /app/sql
COPY --chown=app:app scripts /app/scripts
USER app
CMD ["python", "-m", "restaurant_streaming.app"]
