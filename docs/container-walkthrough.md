# Stage 1: from Java source to one durable container

This stage packages the existing **single-node** QuantaFs server in one Docker image. It does not
containerize the Raft cluster and does not add Docker Compose or Kubernetes.

The server still binds to `127.0.0.1` by default. Inside a container, that address means the
container's own loopback interface. The examples therefore run the client with `docker exec`, in
the same container network namespace. They do not publish a port or enable remote access. QuantaFs
has no TLS or authentication yet, so keeping this boundary is intentional.

## The path from source to process

```text
Java and .proto source
        |
        | Maven Wrapper runs the protobuf compiler and Java compiler
        v
target/quantafs.jar
        |
        | Docker copies the JAR into a Java 21 runtime image
        v
quantafs:stage1 image
        |
        | docker run creates a container and mounts durable storage
        v
Java process (PID 1) + /var/lib/quantafs RocksDB volume
```

- **Source** is the human-readable code under `src/main/java` and `src/main/proto`.
- A **JAR** is a Java archive. Maven Shade combines QuantaFs and its runtime dependencies into the
  runnable `target/quantafs.jar`; `pom.xml` names `Main` as its entry point.
- An **image** is an immutable filesystem and startup recipe. `Dockerfile` defines it.
- A **container** is a running instance of an image. Replacing a container does not modify the
  image, and data written only into the container filesystem should be treated as disposable.
- A **volume** is storage managed separately from a container. Mounting the same named volume into
  a replacement container gives RocksDB the same files again.

## What the Dockerfile does

The file is a **multi-stage build**, meaning it uses one image to compile and another to run:

1. `FROM eclipse-temurin:21-jdk-jammy AS build` selects a Java 21 **JDK**. A JDK contains the
   compiler and build tools. Ubuntu Jammy uses glibc, the native C runtime expected by the bundled
   RocksDB JNI library; an Alpine/musl runtime is deliberately not used.
2. `WORKDIR /workspace` chooses the builder's working directory.
3. The builder installs `unzip`, which lets the script-only Maven Wrapper download the ZIP whose
   exact SHA-256 is pinned in `.mvn/wrapper/maven-wrapper.properties`. The package cache is then
   removed from the layer.
4. The `COPY` instructions bring in the Maven Wrapper, `pom.xml`, and source. The wrapper downloads
   the checksum-protected Maven version and Maven resolves the pinned project dependencies.
5. `RUN ./mvnw ... package` compiles a self-contained JAR. Tests are skipped *inside image
   construction* to avoid rerunning the full process suite in a build layer; run `./mvnw verify`
   on the host first.
6. The second `FROM` starts a clean Java 21 **JRE** image. A JRE runs Java but omits compiler tools,
   so the final image contains less build machinery.
7. `groupadd`, `useradd`, and `USER quantafs:quantafs` make the process run as UID/GID 10001 instead
   of root. `/var/lib/quantafs` and the later Compose client's `/var/lib/quantafs-client` mount
   points are created as writable by that user.
8. `COPY --from=build` transfers only the runnable JAR from the builder.
9. `VOLUME ["/var/lib/quantafs"]` documents the persistent-storage boundary. The commands below
   explicitly attach a named volume there.
10. `STOPSIGNAL SIGTERM` tells Docker which signal requests shutdown. `Main` catches SIGTERM with a
   JVM shutdown hook, drains gRPC work, and closes RocksDB.
11. The JSON-array `ENTRYPOINT` and `CMD` are **exec style**. No intermediary shell absorbs signals:
    Java becomes PID 1 and receives Docker's SIGTERM directly.

The base tags support common Linux AMD64 and ARM64 hosts. Native RocksDB compatibility is still
validated only when the image actually starts and opens its database; a successful compile alone
does not prove native loading.

`.dockerignore` keeps host build output, databases, journals, credentials, tests, and documentation
out of the build context. The context is the set of files Docker sends to its builder.

## Build and run

Prerequisites are Docker Engine or Docker Desktop with the `docker` command available. From the
repository root:

```bash
# First prove the ordinary project still passes outside Docker.
./mvnw verify

# Build and label the image.
docker build --tag quantafs:stage1 .

# Create durable storage independently of any container.
docker volume create quantafs-stage1-data

# Start the server in the background. No host port is published.
docker run --detach \
  --name quantafs-stage1 \
  --mount type=volume,source=quantafs-stage1-data,target=/var/lib/quantafs \
  quantafs:stage1

docker logs --follow quantafs-stage1
```

The log should include text similar to:

```text
QuantaFs listening on 127.0.0.1:50051 (data dir /var/lib/quantafs)
```

After that line appears, press **Control-C** to stop following the logs. This does not stop the
container; it only exits the `docker logs` command.

Confirm the process is non-root:

```bash
docker exec quantafs-stage1 id
```

Expected identifiers include `uid=10001(quantafs)` and `gid=10001(quantafs)`.

Because the server is loopback-only, run the demonstration client inside the same container:

```bash
docker exec quantafs-stage1 \
  java -jar /opt/quantafs/quantafs.jar client put greeting "hello from Docker"

docker exec quantafs-stage1 \
  java -jar /opt/quantafs/quantafs.jar client get greeting
```

Expected client output:

```text
OK
hello from Docker
```

## Graceful stop and durable replacement

Docker first sends SIGTERM. The 15-second allowance exceeds QuantaFs's default ten-second gRPC
drain period:

```bash
docker stop --timeout 15 quantafs-stage1
docker logs quantafs-stage1
```

Near the end, the logs should include `Shutdown requested`, `gRPC server stopped`, and
`Storage closed`. Remove only the stopped container, not its volume:

```bash
docker rm quantafs-stage1
```

Now create a replacement from the unchanged image and attach the same volume:

```bash
docker run --detach \
  --name quantafs-stage1 \
  --mount type=volume,source=quantafs-stage1-data,target=/var/lib/quantafs \
  quantafs:stage1

docker logs --follow quantafs-stage1
# After "QuantaFs listening" appears, press Control-C, then run:
docker exec quantafs-stage1 \
  java -jar /opt/quantafs/quantafs.jar client get greeting
```

The result should still be `hello from Docker`. The old container was deleted, but the named
volume—and therefore the RocksDB files—was not.

This demonstrates process/container replacement on one machine. It does not demonstrate survival
of disk loss, host loss, or power loss, and the volume is not a backup.

## Safe cleanup

These commands remove the disposable container and image while preserving database data:

```bash
docker stop --timeout 15 quantafs-stage1
docker rm quantafs-stage1
docker image rm quantafs:stage1
docker volume inspect quantafs-stage1-data
```

The last command proves the data volume still exists. Only when the data is intentionally no longer
needed should it be destroyed:

```bash
# DESTRUCTIVE: permanently removes this demonstration's RocksDB files.
docker volume rm quantafs-stage1-data
```

## What comes later

- Stage 2 now adds three-node Compose networking, separate node identities, and separate volumes
  without changing this image's safe single-node default. See [the Compose walkthrough](compose-walkthrough.md).
- Stage 3 now provides local Kubernetes resources: StatefulSet, headless Service, persistent volume
  claims, configuration, and probes. The deployment and recovery demo have been verified locally; see
  [the Kubernetes walkthrough](kubernetes-walkthrough.md).
- Stage 4 now demonstrates controlled Pod replacement, majority-loss refusal, and recovery.

Stage 2 is activated only by Compose commands and explicit network-mode arguments; this Stage 1
default command remains the loopback single-node server.
