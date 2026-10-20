# syntax=docker/dockerfile:1

# Build the self-contained application JAR with the project's pinned Maven Wrapper.
FROM eclipse-temurin:21-jdk-jammy AS build

WORKDIR /workspace
# The script-only Maven Wrapper falls back from its checksummed ZIP URL to a tarball when `unzip`
# is absent. Installing `unzip` keeps the downloaded archive aligned with the pinned ZIP checksum.
RUN apt-get update \
    && apt-get install --yes --no-install-recommends unzip \
    && rm -rf /var/lib/apt/lists/*
COPY .mvn/ .mvn/
COPY mvnw pom.xml ./
RUN chmod +x mvnw
COPY src/ src/
RUN ./mvnw -B -ntp -Dmaven.test.skip=true package \
    && test -s target/quantafs.jar

# Run on glibc-based Ubuntu so RocksDB's bundled JNI library has a compatible C runtime.
FROM eclipse-temurin:21-jre-jammy AS runtime

ARG APP_UID=10001
ARG APP_GID=10001
RUN groupadd --gid "${APP_GID}" quantafs \
    && useradd --uid "${APP_UID}" --gid "${APP_GID}" \
        --home-dir /nonexistent --shell /usr/sbin/nologin quantafs \
    && install -d -o quantafs -g quantafs \
        /opt/quantafs /var/lib/quantafs /var/lib/quantafs-client

COPY --from=build --chown=quantafs:quantafs \
    /workspace/target/quantafs.jar /opt/quantafs/quantafs.jar

USER quantafs:quantafs
WORKDIR /var/lib/quantafs
VOLUME ["/var/lib/quantafs"]
STOPSIGNAL SIGTERM

ENTRYPOINT ["java", "-jar", "/opt/quantafs/quantafs.jar"]
CMD ["server", "--data-dir", "/var/lib/quantafs"]
