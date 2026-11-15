# Hive metastore needs a PostgreSQL JDBC driver; the base image omits it.
FROM alpine:3.21 AS fetch
RUN apk add --no-cache curl && \
    curl -fsSL -o /postgresql.jar \
      https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.7.5/postgresql-42.7.5.jar

FROM apache/hive:4.0.1
USER root
COPY --from=fetch /postgresql.jar /opt/hive/lib/postgres.jar
RUN mkdir -p /home/hive/.beeline \
    && chown -R hive:hive /home/hive

USER hive
