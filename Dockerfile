# Image for Goao_iwo8. The same image runs any role via the MODE env var
# (owner / customer / worker). The owner panel provisions workers by building
# this image on a fresh server and running it as MODE=worker.
FROM python:3.11-slim

RUN (apt-get update \
    && apt-get install -y --no-install-recommends gcc libffi-dev curl \
    && rm -rf /var/lib/apt/lists/*) || true

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir --retries 10 --timeout 120 \
       -i https://pypi.org/simple -r requirements.txt

COPY . /app

RUN mkdir -p /app/data
ENV MODE=worker
ENV PYTHONUNBUFFERED=1

EXPOSE 8765

CMD ["python", "main.py"]
