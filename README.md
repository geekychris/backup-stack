# backup-stack

Scheduled backup and cross-location sync for cloud-stack's infrastructure
(CS-39). One Go binary that is both the server and its own CLI, a
self-contained server-rendered admin UI, and a SQLite metadata store — no
npm toolchain, no extra services to run.

## Target types

- **ssh-path**: a directory tree on a remote host, reached over SSH (every
  host in `cloud-stack/infra/inventory.yaml` already has an `ssh:` field —
  no agent to install). Config:
  ```json
  {"addr": "spark.local:22", "user": "chris", "path": "/etc",
   "privateKeyPath": "/home/me/.ssh/id_ed25519", "knownHostsPath": "/home/me/.ssh/known_hosts"}
  ```
- **k8s-namespace**: the namespaced objects (ConfigMaps, Secrets,
  Deployments) in one or more namespaces of a microk8s cluster, reached
  directly via its API server (`kubernetes.api` in inventory.yaml) with
  client-go — no agent needed. Config:
  ```json
  {"kubeconfigPath": "", "namespaces": ["default"], "excludeKinds": []}
  ```
  Secrets are included by default (a backup that can't restore them isn't
  useful for disaster recovery); set `excludeKinds: ["Secret"]` to opt out.
  This captures object manifests, not PersistentVolume byte data — there is
  no CSI snapshot controller in this lab's microk8s.

cloud-stack's infra registry (`GET /v1/infra`) is used only to *suggest*
candidates (`GET /v1/discover`, or `backup-stack discover`); it never
auto-enrolls a target.

## Full and incremental backups

A full run captures every item a Source reports. An incremental run diffs
the Source's current state against the reconstructed state as of the
target's most recent run (full or incremental) and captures only what's
added or changed, recording the deleted paths alongside — a
tar-incremental-style chain, not content-addressed dedup. An incremental
against a target with no prior successful full run fails clearly
(`backup: target has no prior successful full run`) rather than silently
running a full.

Change detection is a Fingerprint, not necessarily a cryptographic hash: for
ssh-path it's `<size>:<mtime>` from the remote `find`, cheap enough to run on
every file on every snapshot; for k8s-namespace it's a SHA-256 of the
object's meaningful fields (spec, labels, annotations, data — not
`resourceVersion`/`status`/other server bookkeeping).

## Restore

`backup-stack restore <target-id> <run-id> <dest-dir>` reconstructs the
target's full state as of `run-id` by replaying its chain of runs (starting
from the nearest preceding full run) and fetches each surviving item from
whichever run actually captured it, writing everything under `dest-dir`. A
k8s-namespace restore produces one JSON manifest file per object
(`kubectl apply -f` each to restore it); an ssh-path restore produces the
directory tree as it looked at that run.

## Sync

A sync job one-way mirrors a bucket's objects onto a second S3-compatible
(MinIO) endpoint, for cross-location redundancy — copying whatever the
source has that the destination is missing or disagrees on in size. It
**never deletes** anything from the destination, even an object the source
no longer has: there is no code path in `internal/sync` that can call
`Delete`. This is one-way, additive-only sync, not bidirectional
replication — there is no conflict resolution.

## Scheduling

Each target and sync job has an optional cron `schedule` (standard 5-field
cron, or `@daily`/`@hourly`/`@every 1h`); blank means manual-only. The
scheduler polls for due targets, skips one with a run already in flight
(from a crash, or from the scheduler's own concurrent dispatch), and bounds
how many runs go at once (`BACKUP_STACK_CONCURRENCY`, default 4).

## Configuration

| Env var | Purpose |
| --- | --- |
| `BACKUP_STACK_DB` | SQLite file path (default `backup-stack.db`) |
| `BACKUP_STACK_TOKEN` | the single bearer token the JSON API requires (required for `serve`) |
| `BACKUP_STACK_ADDR` | listen address (default `:8481`) |
| `BACKUP_STACK_SINKS_JSON` | `{"<name>": {"endpoint":...,"bucket":...,"accessKey":...,"secretKey":...,"prefix":...,"useSSL":...,"createBucket":...}}` — named S3 sinks a target's `sinkRef` selects (default `"primary"`) |
| `BACKUP_STACK_S3_*` | shorthand for a single sink named `primary` when `BACKUP_STACK_SINKS_JSON` isn't set (`_ENDPOINT`, `_BUCKET`, `_ACCESS_KEY`, `_SECRET_KEY`, `_PREFIX`, `_USE_SSL`, `_CREATE_BUCKET`) |
| `CLOUDSTACK_URL`, `CLOUDSTACK_TOKEN` | cloud-stack instance to discover candidates from (optional) |

## Admin UI and cloud-stack integration

`serve` exposes the JSON API under `/v1` and a plain HTML+JS admin UI at
`/` (setup: create targets and sync jobs, run cloud-stack discovery;
monitoring: target list with last-run status). It is meant to sit behind
cloud-stack's existing sibling-app proxy (`internal/appproxy`, the
`APP_PROXIES` env var already used for saaspack-admin, faas and
blackboard) so it shows up signed-in under cloud-stack's Consoles menu —
that's a deployment config change on the running cloud-stack instance, not
a code change. `infra/inventory.yaml` in cloud-stack has an additive
`backup-stack` row so it shows up on the Infra tab too.

## CLI

```
backup-stack serve
backup-stack targets list|create|delete ...
backup-stack run [--full] <target-id>
backup-stack sync <sync-job-id>
backup-stack restore <target-id> <run-id> <dest-dir>
backup-stack discover
```

## Scope cuts

- No PV/PVC byte-level volume backup — this lab's microk8s has no CSI
  snapshot controller, so manifest-level capture is what's actually
  restorable today.
- No content-addressed chunk-level dedup or garbage collection — the
  incremental chain plus manual/future keep-last-N-fulls pruning
  (`Sink.Prune`) is the whole retention story.
- Sync has no bidirectional mode, conflict resolution, or delete
  propagation.
- No remote agent (`cmd/backup-agent`) in this increment — every host in
  the infra inventory already exposes `ssh:` or `kubernetes.api`, which the
  two target types use directly.
- One static admin bearer token, no per-user auth/RBAC.

## Development

```
make check   # gofmt -l, go vet, go build, go test
```

`internal/sink/s3`'s integration test needs a real MinIO endpoint and is
skipped unless `BACKUP_TEST_S3_ENDPOINT` is set.
