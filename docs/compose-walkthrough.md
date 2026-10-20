# Stage 2: three Raft nodes with Docker Compose

This stage runs three real QuantaFs server processes in three containers on one Mac. Docker Compose
describes and manages the containers, their private network, and their persistent volumes. It does
not add Kubernetes, TLS, authentication, remote hosts, or production deployment.

## Why the network code had to change

`127.0.0.1` is a machine's **loopback address**: traffic sent there returns to the same network
namespace. Each container has its own network namespace, so `127.0.0.1` inside `n1` means only
`n1`; it cannot reach `n2` or `n3`.

Compose connects the services to one bridge network and provides DNS:

```text
n1 -> current IP of the n1 container
n2 -> current IP of the n2 container
n3 -> current IP of the n3 container
```

The IP may change when a container is replaced, but the service DNS name stays stable. QuantaFs
therefore persists the fixed membership as:

```text
n1=n1:7000,n2=n2:7000,n3=n3:7000
```

A **bind address** says which local interfaces accept connections. An **advertised endpoint** says
where another node should connect:

- The server binds `0.0.0.0:7000` in explicit Compose mode so it accepts traffic from the private
  bridge interface as well as its own loopback interface.
- Each node advertises its stable service address, such as `n2:7000`.

The old mode still defaults to, binds, advertises, and accepts only `127.0.0.1`. Compose behavior
requires `--network-mode compose`. In that mode, each lowercase service DNS name must exactly match
its node ID. This prevents accidentally treating arbitrary remote addresses as this local demo mode.

DNS resolution is deferred to gRPC. Containers may start in any order: early connection attempts
can fail, then ordinary Raft heartbeats retry after DNS and the peer process become ready.

## Why every node has its own volume

Raft nodes are not interchangeable copies of one directory. Each node durably owns:

- its node identity and complete fixed membership;
- its current term and vote;
- its own replicated log and snapshot boundary;
- its locally applied RocksDB state machine;
- an exclusive directory lock while running.

The server volumes are therefore distinct:

```text
quantafs-compose-n1-data -> n1 /var/lib/quantafs
quantafs-compose-n2-data -> n2 /var/lib/quantafs
quantafs-compose-n3-data -> n3 /var/lib/quantafs
```

Sharing one volume would mix different Raft histories and violate the directory lock. Replacing
`n2` must reattach only `quantafs-compose-n2-data`.

`quantafs-compose-client-session` is a fourth, separate volume containing only the demonstration
client's retry journal. It is never mounted into a server. The image pre-creates both mount points
as writable by non-root UID/GID 10001.

## Reading `compose.yaml`

- `name: quantafs-stage2` gives this local Compose project a stable name.
- `x-quantafs-node` is a YAML anchor containing settings shared by all three nodes.
- Every node uses the same `quantafs:stage2` image but gets a different `--id` and volume.
- Every node receives the exact same ordered `--peers` membership.
- All nodes can use container port 7000 because they have separate network namespaces.
- There is deliberately no `ports:` section, so no cluster port is published to macOS.
- `raft-internal` has `internal: true`. Docker permits traffic among attached containers but does
  not give that network an external route.
- `stop_grace_period: 40s` gives the JVM shutdown hook time to stop networking, drain its event
  queue, close RocksDB, and release the directory lock.
- The health check calls each node's own `127.0.0.1:7000` status RPC. It checks that the local Java
  process and gRPC event path respond and the node has not halted. A halted node returns a nonzero
  exit code and is unhealthy. A follower and candidate are healthy; leadership and majority
  availability are intentionally not health conditions. There is no automatic restart policy, so
  normal elections cannot cause restart loops.
- The `client` service is in the `tools` profile, so ordinary `docker compose up` starts only the
  servers. `docker compose run` creates a temporary non-root client container on the private network.

## Build and start

From the repository root:

```bash
docker compose config --quiet
docker compose build
docker compose up --detach --wait --wait-timeout 60
docker compose ps
```

`config --quiet` validates the YAML after anchors and defaults are resolved. `--wait` is bounded:
it returns when all three local health checks pass or fails after 60 seconds.

Expected `docker compose ps` shape:

```text
NAME                   SERVICE   STATUS                  PORTS
quantafs-stage2-n1-1   n1        Up ... (healthy)
quantafs-stage2-n2-1   n2        Up ... (healthy)
quantafs-stage2-n3-1   n3        Up ... (healthy)
```

The `PORTS` column should be empty.

## Find the elected leader

Each command runs inside that node's container and contacts its own loopback endpoint:

```bash
for node in n1 n2 n3; do
  docker compose exec -T "$node" \
    java -jar /opt/quantafs/quantafs.jar \
    cluster-status --target 127.0.0.1:7000
done
```

One line eventually contains `role=LEADER`; the other two contain `role=FOLLOWER`. The winning node
and term vary. `commit` and `applied` should be at least 1 after the leader's current-term no-op.
Status is diagnostic local state: an isolated old leader may temporarily still print `LEADER`.

## Write and read through automatic leader discovery

```bash
docker compose --profile tools run --rm client \
  cluster-client --network-mode compose \
  --seeds n1:7000,n2:7000,n3:7000 \
  --session /var/lib/quantafs-client/demo.session \
  put stage2-key "three-node value"

docker compose --profile tools run --rm client \
  cluster-client --network-mode compose \
  --seeds n1:7000,n2:7000,n3:7000 \
  --session /var/lib/quantafs-client/demo.session \
  get stage2-key
```

Expected application output:

```text
OK
three-node value
```

Compose also prints container lifecycle lines. The same client-session volume is mounted into both
temporary client containers.

## What happens between `put` and `OK`

1. The client reads or creates its durable ID and next sequence in `demo.session`.
2. Before sending, it durably journals the exact pending mutation.
3. It resolves the three service names and probes node status until it finds a leader.
4. The leader validates the command, appends it to its own Raft RocksDB log, and syncs that write.
5. It sends `AppendEntries` over plaintext gRPC to the followers' advertised DNS endpoints.
6. A follower checks the previous log position and durably appends before acknowledging.
7. After acknowledgements from a majority—including the leader—the entry can become committed.
8. Every node applies committed entries in index order. Application data and `lastApplied` move
   atomically in that node's separate RocksDB state-machine database.
9. The leader replies only after local application. The client then marks the journal confirmed.

A `get` first commits and applies a fresh read barrier. This prevents a minority or isolated old
leader from returning stale local data. The barrier is conservative and adds a no-op to the log.

## Failure and recovery demonstration

Run the status loop and note the service whose line says `role=LEADER`. The examples below assume
it is `n2`; substitute the actual service name.

```bash
docker compose stop --timeout 40 n2
```

Use `--no-deps` during the failure test so Compose does not restart the stopped dependency:

```bash
docker compose --profile tools run --rm --no-deps client \
  cluster-client --network-mode compose \
  --seeds n1:7000,n2:7000,n3:7000 \
  --session /var/lib/quantafs-client/demo.session \
  get stage2-key

docker compose --profile tools run --rm --no-deps client \
  cluster-client --network-mode compose \
  --seeds n1:7000,n2:7000,n3:7000 \
  --session /var/lib/quantafs-client/demo.session \
  put post-failover "leader changed safely"
```

The read should print `three-node value` and the write should print `OK`. A warning that the stopped
service name cannot be resolved is expected; the client rotates to another seed. The two survivors
form a majority and elect a replacement. With two nodes stopped, a one-node minority cannot complete
the write or read barrier.

Restart the old leader and inspect its progress:

```bash
docker compose start n2

docker compose exec -T n2 \
  java -jar /opt/quantafs/quantafs.jar \
  cluster-status --target 127.0.0.1:7000
```

After heartbeat catch-up, it should be a follower at the current term with commit/applied indexes
matching the active cluster.

## Replace one container without replacing its node

This removes only the `n1` container and reattaches the existing `n1` volume:

```bash
docker compose stop --timeout 40 n1
docker compose rm --force n1
docker compose up --detach --wait --wait-timeout 45 n1

docker compose exec -T n1 \
  java -jar /opt/quantafs/quantafs.jar \
  cluster-status --target 127.0.0.1:7000
```

The persisted identity must still be `n1` with the same membership. Its term/log/application state
reopens from `quantafs-compose-n1-data`, then Raft catches up anything it missed. Never attach a
different node's volume or change a persisted node's identity/membership in place.

## Safe stop and cleanup

Stop and remove only Stage 2 containers and its private network:

```bash
docker compose down
```

Ordinary `down` preserves all named volumes. Confirm:

```bash
docker volume ls --format '{{.Name}}' | grep '^quantafs-compose-'
docker volume inspect quantafs-stage1-data
```

Do **not** use `docker compose down -v`, `docker volume prune`, or `docker system prune`; those can
destroy persistent data. Start the same nodes again with:

```bash
docker compose up --detach --wait --wait-timeout 60
```

## Security and failure boundary

The internal bridge and lack of published ports reduce accidental exposure. They are not
cryptographic security:

- peer and client traffic is plaintext;
- a process attached to the network is not authenticated;
- there is no authorization;
- the membership key detects configuration mismatch but is not a secret;
- all containers, volumes, and Docker itself still depend on one physical Mac.

This demo tolerates one node container/process stopping while the other two communicate. It does
not tolerate loss of the Docker host, its disk, or two voters. It does not prove power-loss
durability. Before any remote or production network is enabled, QuantaFs needs authenticated peer
identity, TLS, client access control, secret handling, and an explicit threat model.

Kubernetes remains the next user-controlled stage and is not implemented here.
