FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
WORKDIR /app
COPY acquisition/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY acquisition/simulator.py /app/acquisition/simulator.py
CMD ["python", "-u", "acquisition/simulator.py", "--run", "--send"]
