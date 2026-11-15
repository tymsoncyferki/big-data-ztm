# A single Docker-only batch image: Spark processing + Hive JDBC partition repair.
# Both one-shot runs and the optional in-container scheduler use this image.
FROM apache/hive:4.0.1 AS hive-client

FROM apache/spark:3.5.7-scala2.12-java17-python3-ubuntu
USER root
# Beeline needs both its Hive distribution and the matching Hadoop client runtime.
# It runs in a separate JVM process, avoiding Spark/Hive classpath collisions.
COPY --from=hive-client /opt/hive /opt/hive
COPY --from=hive-client /opt/hadoop /opt/hadoop
RUN mkdir -p /opt/project /var/lock/ztm /tmp/hive-home/.beeline && \
    touch /var/lock/ztm/.initialized && \
    chmod -R a+rX /opt/hive /opt/hadoop && chmod 0777 /var/lock/ztm && chmod 1777 /tmp/hive-home && chmod 0777 /tmp/hive-home/.beeline
ENV HIVE_HOME=/opt/hive \
    HADOOP_HOME=/opt/hadoop \
    HIVE_CONF_DIR=/opt/hive/conf \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    HOME=/tmp/hive-home \
    PYSPARK_PYTHON=python3 \
    PYTHONUNBUFFERED=1
WORKDIR /opt/project
# Preserve the HDFS identity of the previously validated Spark image.
USER spark
# No cron, Docker CLI, or Docker socket is needed inside this container.
CMD ["python3", "-u", "/opt/project/scripts/container_scheduler.py", "--once"]
