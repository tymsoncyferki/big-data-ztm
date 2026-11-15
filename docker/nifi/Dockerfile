# NiFi with Hadoop PutHDFS processors. Build once; no manual NAR installation.
FROM alpine:3.22 AS downloader
RUN apk add --no-cache ca-certificates curl
ARG NIFI_VERSION=2.12.0
RUN mkdir -p /nars && \
    for artifact in nifi-hadoop-nar nifi-hadoop-libraries-nar; do \
      curl --fail --location --retry 5 --retry-all-errors \
        "https://repo.maven.apache.org/maven2/org/apache/nifi/${artifact}/${NIFI_VERSION}/${artifact}-${NIFI_VERSION}.nar" \
        --output "/nars/${artifact}-${NIFI_VERSION}.nar"; \
    done

FROM apache/nifi:2.12.0
# Install NARs in lib, NOT nar_extensions: persisted nar_extensions mounts will not hide them.
COPY --from=downloader --chown=nifi:nifi /nars/ /opt/nifi/nifi-current/lib/
# On a fresh named conf volume, Docker initializes it from the image's conf directory.
COPY --chown=nifi:nifi docker/nifi/hadoop-core-site.xml /opt/nifi/nifi-current/conf/hadoop-core-site.xml
