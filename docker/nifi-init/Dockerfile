# One-off bootstrap container for configuring NiFi via REST API.
# This avoids needing Python/requests installed on the host.
FROM python:3.12-slim
RUN pip install --no-cache-dir 'requests>=2.32,<3'
WORKDIR /bootstrap
CMD ["python", "-u", "scripts/init-nifi.py"]
