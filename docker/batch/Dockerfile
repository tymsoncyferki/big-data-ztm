# Spark-only container; uses Spark distribution for HDFS/Parquet access.
# Both manual and scheduled jobs use the same Spark distribution.
FROM apache/spark:3.5.7-scala2.12-java17-python3-ubuntu
USER root
RUN mkdir -p /opt/project /var/lock/ztm && chmod 0777 /var/lock/ztm
ENV PYSPARK_PYTHON=python3 PYTHONUNBUFFERED=1
WORKDIR /opt/project
USER spark
CMD ["python3", "-u", "/opt/project/scripts/container_scheduler.py", "--once"]
