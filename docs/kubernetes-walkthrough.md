# Stage 3: local Kubernetes manifests

This stage describes a fixed three-voter QuantaFs cluster for **local Docker Desktop Kubernetes**.
It does not create a cloud cluster, publish an image, expose a public service, or claim production
security. The initial files were prepared offline. Docker Desktop Kubernetes has now been enabled locally,
and the manifests passed server validation and started three ready voters with four bound PVCs.
The local image was used without a registry upload. Runtime evidence is recorded below.

## Manual prerequisite

Docker Desktop Kubernetes must be enabled manually by Richard:

1. Open Docker Desktop settings.
2. Open the Kubernetes section.
3. Enable Kubernetes and apply/restart.
4. Wait for Docker Desktop to report that Kubernetes is running.

QuantaFs does not change that setting automatically. Afterward, from the repository root:

```bash
kubectl config current-context
kubectl config get-contexts
kubectl cluster-info
sh deploy/kubernetes/verify-local-context.sh
```

The expected context and only node are both `docker-desktop`. The verification script refuses any
other context, any non-local API address, multiple nodes, or an unreachable cluster. Do not continue
if it refuses. This is a deliberate guard against applying the demo to a remote/shared cluster.

## Terms mapped to QuantaFs

- A **container image** is the immutable Java 21 runtime and QuantaFs JAR built by `Dockerfile`.
- A **Pod** is a running image instance. Each server Pod runs one Java process and one Raft node.
- A **StatefulSet** manages Pods with stable ordinal names. QuantaFs uses `quantafs-0`,
  `quantafs-1`, and `quantafs-2`; the replica count must remain exactly three while running.
- A **headless Service** has no load-balancing virtual IP. `quantafs-peer` creates stable peer DNS,
  such as `quantafs-1.quantafs-peer`, directly naming a StatefulSet Pod.
- A **PersistentVolumeClaim (PVC)** requests durable storage. The StatefulSet creates one distinct
  claim per ordinal, such as `data-quantafs-1`. Replacing the Pod reattaches that same claim.
- A **Namespace** groups names. `quantafs-local` prevents naming collisions, but it is not a network
  or security boundary.
- A **startup probe** allows bounded startup time before liveness begins.
- A **readiness probe** says whether the local process may receive traffic.
- A **liveness probe** asks whether Kubernetes should restart the local process.
- A resource **request** reserves scheduler capacity; a **limit** bounds consumption.

## Why Kubernetes has its own explicit network mode

Compose advertises node `n1` as `n1:7000` and requires hostname `n1` to equal node ID `n1`.
StatefulSet DNS instead has this shape:

```text
quantafs-0.quantafs-peer
quantafs-1.quantafs-peer
quantafs-2.quantafs-peer
```

QuantaFs therefore has a separate `--network-mode kubernetes`. It accepts only a lowercase node ID
advertised at exactly `<node-id>.quantafs-peer`. The fixed persisted membership is:

```text
quantafs-0=quantafs-0.quantafs-peer:7000,
quantafs-1=quantafs-1.quantafs-peer:7000,
quantafs-2=quantafs-2.quantafs-peer:7000
```

Line breaks are omitted in the actual argument. Loopback remains the default, Compose validation is
unchanged, and existing loopback/Compose identity files keep their old membership representation.

The headless Service sets `publishNotReadyAddresses: true`. Without it, DNS might omit a new Pod
until readiness succeeds. Raft peers could then be unable to contact one another during startup.
Publishing addresses early is safe here because gRPC retries until the process is listening.
Readiness still controls ordinary endpoint readiness; it does not gate peer DNS.

## File roles

`deploy/kubernetes/namespace.yaml`
: Creates only the dedicated `quantafs-local` namespace.

`deploy/kubernetes/service.yaml`
: Creates the internal headless `quantafs-peer` Service. It has `clusterIP: None` and no NodePort,
  LoadBalancer, external IP, or Ingress.

`deploy/kubernetes/statefulset.yaml`
: Runs exactly three fixed voters and generates three separate 2 GiB PVCs. `Parallel` startup
  avoids ordered startup dependencies. `OnDelete` updates require an explicit Pod replacement;
  Kubernetes will not silently roll the entire Raft cluster after an image change.

`deploy/kubernetes/client-pvc.yaml`
: Stores only the retry client's durable session journal. It is never mounted by a server.

`deploy/kubernetes/client-deployment.yaml`
: Keeps one non-root tool Pod available for `kubectl exec`. `Recreate` prevents two client Pods from
  trying to own the same single-writer journal.

`deploy/kubernetes/kustomization.yaml`
: Renders the resource group and centralizes the local image tag. Change `newTag` only when building
  a matching local tag.

`deploy/kubernetes/verify-local-context.sh`
: Performs read-only checks and accepts only the reachable single-node `docker-desktop` cluster.

## Identity, disk, and security settings

The StatefulSet automatically makes the Pod hostname equal its ordinal name. The downward API
places that name in `POD_NAME`; the startup shell uses `exec`, so Java replaces the shell and
receives SIGTERM directly as PID 1.

Each Pod mounts only its own PVC at `/var/lib/quantafs`. QuantaFs persists its node ID and complete
fixed membership there before opening its two RocksDB databases. Reattaching `data-quantafs-1` to
`quantafs-1` is valid; attaching it to another identity fails closed.

Servers and the client run as UID/GID 10001. `fsGroup: 10001` lets a compatible local volume driver
make mounted storage writable. Capabilities are dropped, privilege escalation is disabled, the root
filesystem is read-only, and a small writable `emptyDir` is mounted at `/tmp` for JVM/RocksDB native
library extraction. Service-account tokens are not mounted.

The server requests 250 millicores and 256 MiB and is limited to one CPU and 1 GiB. The server JVM uses a 64 MiB initial heap and a 512 MiB maximum heap, leaving room for
RocksDB/native memory and probe processes. Each health-check JVM has its own 16 MiB initial and
64 MiB maximum heap; the client uses the same smaller limits. Server heap flags are passed only
to the server so probes do not inherit an oversized initial heap. These are local-demo bounds, not benchmark-derived production sizing.

## Probe design

All three probes run:

```bash
java -jar /opt/quantafs/quantafs.jar \
  cluster-status --target 127.0.0.1:7000
```

`cluster-status` returns:

- `0` for a responding, non-halted follower, candidate, or leader;
- `3` for a halted node or failed RPC.

It does not test leadership or majority availability. A minority node can remain alive and ready
even though client read barriers and writes cannot complete. This avoids restart loops during normal
elections or temporary majority loss.

- Startup: every 10 seconds, at most 18 failures—approximately 180 seconds.
- Readiness: every 10 seconds; two failures remove readiness.
- Liveness: every 20 seconds; three failures trigger a restart.
- Probe commands get up to ten seconds because launching a JVM can be slow on a busy laptop.
- Startup must pass before readiness/liveness control the container.

A durable corruption or repeatable startup failure can still cause Kubernetes restart attempts.
Inspect events and previous logs instead of assuming restart repairs disk state.

## Build, render, and validate before applying

Build only into Docker Desktop's local image store:

```bash
docker build --tag quantafs:stage3 .
docker image inspect quantafs:stage3 --format '{{.Id}} {{.Architecture}}'
```

The manifests use `imagePullPolicy: Never`, so Kubernetes must use that local image and cannot pull
from a registry. No image upload is needed or authorized.

Render locally:

```bash
kubectl kustomize deploy/kubernetes
```

After the local-context verification script succeeds, ask the reachable API server to validate the
schemas without persisting anything:

```bash
kubectl apply -f deploy/kubernetes/namespace.yaml
kubectl apply --dry-run=server -k deploy/kubernetes
```

Only then apply:

```bash
kubectl apply -k deploy/kubernetes
kubectl -n quantafs-local wait --for=jsonpath='{.status.readyReplicas}'=3 statefulset/quantafs --timeout=240s
kubectl -n quantafs-local rollout status deployment/quantafs-client --timeout=120s
kubectl -n quantafs-local get pods,pvc
```

Expected shape:

```text
pod/quantafs-0                 1/1 Running
pod/quantafs-1                 1/1 Running
pod/quantafs-2                 1/1 Running
pod/quantafs-client-...        1/1 Running

persistentvolumeclaim/data-quantafs-0          Bound
persistentvolumeclaim/data-quantafs-1          Bound
persistentvolumeclaim/data-quantafs-2          Bound
persistentvolumeclaim/quantafs-client-session  Bound
```

Pod suffixes, ages, terms, indexes, image IDs, and volume names assigned by the local provisioner
can vary.

## Find the leader

```bash
for pod in quantafs-0 quantafs-1 quantafs-2; do
  kubectl -n quantafs-local exec "$pod" -- \
    java -jar /opt/quantafs/quantafs.jar \
    cluster-status --target 127.0.0.1:7000
done
```

Eventually one line says `role=LEADER`; the other two normally say `role=FOLLOWER`. A local status
line is diagnostic, not proof that the node can currently reach a majority.

## Durable client write and read

The seed DNS names are internal to the namespace:

```bash
kubectl -n quantafs-local exec deployment/quantafs-client -- \
  java -jar /opt/quantafs/quantafs.jar \
  cluster-client --network-mode kubernetes \
  --seeds quantafs-0.quantafs-peer:7000,quantafs-1.quantafs-peer:7000,quantafs-2.quantafs-peer:7000 \
  --session /var/lib/quantafs-client/demo.session \
  put kubernetes-key "local persistent value"

kubectl -n quantafs-local exec deployment/quantafs-client -- \
  java -jar /opt/quantafs/quantafs.jar \
  cluster-client --network-mode kubernetes \
  --seeds quantafs-0.quantafs-peer:7000,quantafs-1.quantafs-peer:7000,quantafs-2.quantafs-peer:7000 \
  --session /var/lib/quantafs-client/demo.session \
  get kubernetes-key
```

Expected application output:

```text
OK
local persistent value
```

## Request path

1. The client PVC supplies the durable client ID, sequence, and any unresolved mutation.
2. The client resolves headless-Service DNS and probes seeds until it finds a leader.
3. The leader durably syncs the new Raft log entry to its own PVC.
4. Plaintext gRPC `AppendEntries` goes directly to follower Pod DNS.
5. Each follower durably appends to its own PVC before acknowledging.
6. A majority acknowledgement permits commitment under Raft's current-term rule.
7. Committed commands apply in order to each node's separate state-machine RocksDB.
8. The leader replies only after local application; the client then confirms its journal.

Reads use a fresh majority-committed barrier, so an isolated former leader cannot successfully serve
stale local state.

## Leader Pod replacement and recovery

Run the status loop and record the current leader Pod—for example, `quantafs-1`. Record its Pod UID
and PVC before deletion:

```bash
kubectl -n quantafs-local get pod quantafs-1 \
  -o jsonpath='{.metadata.uid}{"\n"}'
kubectl -n quantafs-local get pvc data-quantafs-1
```

Delete only the Pod, never its PVC:

```bash
kubectl -n quantafs-local delete pod quantafs-1 --wait=true --timeout=60s
```

The StatefulSet automatically creates a replacement with the same Pod name and PVC. While the old
leader is absent, run the client read and another journaled write; the two survivors should elect a
replacement and the client should rotate automatically:

```bash
kubectl -n quantafs-local exec deployment/quantafs-client -- \
  java -jar /opt/quantafs/quantafs.jar \
  cluster-client --network-mode kubernetes \
  --seeds quantafs-0.quantafs-peer:7000,quantafs-1.quantafs-peer:7000,quantafs-2.quantafs-peer:7000 \
  --session /var/lib/quantafs-client/demo.session \
  put after-pod-replacement "still committed"

kubectl -n quantafs-local wait --for=condition=Ready \
  pod/quantafs-1 --timeout=120s
```

Check again:

```bash
kubectl -n quantafs-local get pod quantafs-1 \
  -o jsonpath='{.metadata.uid}{"\n"}'
kubectl -n quantafs-local get pvc data-quantafs-1
kubectl -n quantafs-local exec quantafs-1 -- \
  java -jar /opt/quantafs/quantafs.jar \
  cluster-status --target 127.0.0.1:7000
```

The Pod UID should change, the PVC name should not, and the restarted node should catch up to the
cluster's current term/commit/applied indexes. Re-read both values through the retry client.

## Majority-loss boundary

The fixed membership remains three voters. Temporarily scaling the StatefulSet to one stops two
processes; it does not reconfigure Raft membership. A one-node minority must refuse reads requiring
a committed barrier. The repeatable demo restores all three replicas in a `finally` block:

```bash
python3 scripts/kubernetes-demo.py
```

Run only when all three voters and the client are already running. It verifies the local context,
writes a dedicated demonstration value, replaces the current leader Pod, checks that the new Pod
has a different UID but the exact same PVC UID and bound volume, verifies data, stops two voters,
expects a consensus-related client error, and restores three voters. It never removes PVCs.
It leaves the recovered cluster running. It uses its own sequential journal;
do not run other clients using `recovery-demo.session` concurrently.


## Safe stop and restart while preserving PVCs

Scaling all server Pods to zero is an operational stop, not a membership change; the persisted fixed
membership remains three:

```bash
kubectl -n quantafs-local scale deployment/quantafs-client --replicas=0
kubectl -n quantafs-local scale statefulset/quantafs --replicas=0
kubectl -n quantafs-local get pvc
```

All four PVCs should remain `Bound`. Restart the exact fixed group:

```bash
kubectl -n quantafs-local scale statefulset/quantafs --replicas=3
kubectl -n quantafs-local wait --for=jsonpath='{.status.readyReplicas}'=3 statefulset/quantafs --timeout=240s
kubectl -n quantafs-local scale deployment/quantafs-client --replicas=1
kubectl -n quantafs-local rollout status deployment/quantafs-client --timeout=120s
```

Do not delete the PVCs, namespace, Docker Desktop cluster, or local volumes. Do not run pruning
commands. Deleting the `quantafs-local` namespace would also delete its PVC objects and risks data
loss.

## Troubleshooting

**Context verification refuses**
: Kubernetes is disabled, unreachable, or the context is not the supported local `docker-desktop`
  cluster. Stop; do not bypass the guard for a remote context.

**`ErrImageNeverPull` or `ImagePullBackOff`**
: `quantafs:stage3` is not visible in Docker Desktop's Kubernetes image store. Rebuild the exact
  local tag after Kubernetes is enabled. Keep `imagePullPolicy: Never`; do not publish merely to fix
  this local demo.

**PVC remains `Pending`**
: Check `kubectl -n quantafs-local describe pvc <name>` and `kubectl get storageclass`. Docker
  Desktop needs a default local StorageClass. The manifests intentionally do not hardcode a
  vendor-specific class.

**Permission error under `/var/lib/quantafs` or client session path**
: Inspect Pod events and volume-driver `fsGroup` support. The image and Pod both use UID/GID 10001.
  Do not solve it by making the container privileged or root.

**Peer DNS fails**
: Confirm the Service has `clusterIP: None`, `publishNotReadyAddresses: true`, and selector-matching
  Pods. From the client Pod, run:

```bash
kubectl -n quantafs-local exec deployment/quantafs-client -- \
  getent hosts quantafs-0.quantafs-peer
```

**Probe failures**
: Run the same `cluster-status` command manually, then inspect:

```bash
kubectl -n quantafs-local describe pod <pod>
kubectl -n quantafs-local logs <pod>
kubectl -n quantafs-local logs <pod> --previous
```

A candidate is healthy. A halted core is not. Majority loss alone should not fail liveness.

## Security and failure limits

There is intentionally no NetworkPolicy. Whether policy is enforced depends on the installed CNI,
and a policy file alone would not prove isolation. The namespace also does not isolate traffic.
The headless Service is not published outside Kubernetes, but other Pods able to reach the Pod
network could send unauthenticated plaintext requests.

Before any remote use, QuantaFs needs TLS, authenticated peer identity, client authorization,
certificate/secret lifecycle, and a threat model. All three Pods and their local persistent volumes
still run on one Mac. Pod replacement evidence is not host-failure tolerance, disk-loss tolerance,
backup, or power-loss proof.

No remote deployment is part of this stage.

## Verified locally on October 20, 2026

- Docker Desktop kubeadm Kubernetes v1.36.1, one `docker-desktop` ARM64 node.
- Server-side dry-run validation passed after creating the dedicated namespace.
- Three server Pods and the client became ready; all four persistent claims bound.
- The local `quantafs:stage3` image loaded without registry publishing.
- Journaled put/get succeeded through StatefulSet peer DNS.
- The recovery demo replaced leader `quantafs-0`: its Pod UID changed while its PVC UID
  and bound volume remained identical. The acknowledged values were still readable.
- Temporarily running one voter refused the read with a consensus-related deadline, while
  the remaining node stayed non-halted. Restoring three voters restored reads.
- `./mvnw verify`: 520 unit/in-process/simulation plus six process integration tests,
  526 total, zero failures/errors/skips.
- Live testing caught and fixed overly short JVM probe timeouts and the unsupported
  `rollout status` command for an `OnDelete` StatefulSet. Use the Pod readiness waits above.

The cluster was left running with three ready voters and one client. No PVCs or existing Docker
volumes were removed. These observations are local process/Pod recovery evidence, not a
performance benchmark or proof of host/disk/power-loss durability.
