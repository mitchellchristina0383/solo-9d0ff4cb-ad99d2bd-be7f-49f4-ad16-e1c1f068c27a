FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    RECON_DATA_DIR=/data \
    PORT=8000

WORKDIR /srv

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY wsgi.py ./

# Unprivileged runtime user; /data is a volume owned by it.
RUN useradd --system --uid 10001 --create-home --home-dir /srv reconciler \
    && mkdir -p /data \
    && chown -R reconciler:reconciler /data /srv
USER reconciler

EXPOSE 8000
VOLUME ["/data"]

HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/healthz', timeout=2).status == 200 else 1)"

# A single worker keeps the in-process job registry coherent; threads let the
# page poll while an operator thread is busy.  Job state is intentionally not
# shared across processes for this trial page.
CMD ["gunicorn", "--workers", "1", "--threads", "8", \
     "--bind", "0.0.0.0:8000", "--access-logfile", "-", "wsgi:app"]
