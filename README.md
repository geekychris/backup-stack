# backup-stack

Scheduled backups and one-way syncs for cloud-stack's lab (CS-39 and CS-40).
It is one Go binary that is both the server and its own CLI, with a
self-contained admin UI (history and graphs included) and a SQLite metadata
store. There is no npm toolchain, no cgo and no extra service to run.

It does two things:

- **Backup targets** take full and incremental, restorable backups of a k8s
  namespace's objects or a directory on a lab host, into an S3 sink.
- **Sync jobs** copy files one way between a directory in the service's pod, a
  directory on a lab host (over SSH) and an S3 bucket prefix, using rsync when
  it can.

Both run on cron schedules. Both use the same ordered include/exclude filters.
Every run records bytes, files, errors, duration and transfer rate.

## Backup targets

- **ssh-path** is a directory tree on a remote host, reached over SSH. Every
  host in `cloud-stack/infra/inventory.yaml` has an `ssh:` field, so there is
  no agent to install. Config:
  ```json
  {"addr": "spark.local:22", "user": "chris", "path": "/etc",
   "privateKeyPath": "/home/me/.ssh/id_ed25519", "knownHostsPath": "/home/me/.ssh/known_hosts",
   "filters": [{"kind": "exclude", "pattern": "**/*.swp"}]}
  ```
  If `privateKeyPath` or `knownHostsPath` is blank, the service uses
  `BACKUP_STACK_SSH_KEY` or `BACKUP_STACK_SSH_KNOWN_HOSTS` instead. In
  Kubernetes those point at the mounted `backup-stack-ssh` Secret. Host keys
  are always checked unless the config sets `insecureSkipHostKeyCheck`
  explicitly. The remote host needs a POSIX shell and GNU find.
- **k8s-namespace** ("Kubernetes objects" in the UI) is the manifests that
  recreate a namespace, read with client-go: ConfigMaps, Secrets, Services,
  ServiceAccounts, PersistentVolumeClaims, Deployments, StatefulSets,
  DaemonSets, CronJobs, Ingresses, NetworkPolicies, Roles and RoleBindings.
  Each is saved cleaned for `kubectl apply`, with no uid, resourceVersion or
  status, no Service cluster IPs and no claim's bound volume name. Left out
  are objects a controller owns and recreates (a CronJob's Jobs, …),
  `kube-root-ca.crt`, the `default` ServiceAccount and service-account token
  Secrets. Config:
  ```json
  {"kubeconfigPath": "", "namespaces": ["wiki"], "excludeKinds": []}
  ```
  Use one namespace per target: the UI creates them that way. A blank
  `kubeconfigPath` means the cluster the service runs in, through its
  ServiceAccount, named by `BACKUP_STACK_CLUSTER_NAME`. Any other cluster is
  a kubeconfig file in `BACKUP_STACK_KUBE_DIR` (the `backup-stack-kubeconfigs`
  Secret, one key per cluster, mounted at `/etc/backup-stack/kube/<cluster>`).
  The API refuses a path that is not one of these clusters (`GET
  /v1/clusters`). Secrets are included by default; set
  `excludeKinds: ["Secret"]` to leave them out.

  This copies **manifests, not data**. A PVC is saved as a claim, not as the
  files on it, and a database's contents are not included. The UI's
  namespace view lists each namespace's volumes and the databases its pods
  run (found by image), so you can see what is still unprotected. Volume-file
  and database-dump backups are the next step.

### Every target says what it copies

`GET /v1/targets` adds to each target `cluster` (the cluster name, or the
host for ssh-path), `namespaces`, `what` ("Kubernetes objects", "Files over
SSH"), `copies` (a sentence) and `lastRun`. The UI groups backups by cluster
or host, then by namespace or path.

### Finding what to back up

- `GET /v1/clusters` lists the clusters and the object kinds a k8s target
  copies, each with a line about what it is.
- `GET /v1/clusters/{name}/namespaces[?namespace=]` lists every namespace
  with object counts by kind, its PVCs (size, class, which workloads mount
  them, and whether a database runs on them), the databases its pods run
  (postgres, mysql, mariadb, mongodb, redis, by image or well-known port), and
  the targets that already cover it. The UI's **What isn't backed up?** view
  is this.
- `GET /v1/sinks` lists the destinations (name, endpoint, bucket, prefix; no
  keys).
- `GET /v1/schedule?expr=` validates a cron schedule and returns its next
  three runs. Create and update refuse an invalid schedule.

`PUT /v1/targets/{id}` and `PUT /v1/sync-jobs/{id}` change only the fields
they are sent: `{"enabled": false}` disables a target without touching its
schedule, and `"schedule": ""` makes it manual. A name that is already taken
is a 409 that says so.

### The UI

Every backup and sync job can be edited, disabled, enabled and deleted. Delete
warns that the run history goes with it, and with it the ability to restore
from here, and points to Disable instead. The editors have a field and an
explanation for every setting, so there is no JSON to write:

- **cluster and namespace pickers**, showing what is in the namespace and
  whether it is already backed up;
- **kinds to leave out**, each with a line about what it is;
- **SSH fields**: host, port, user, directory, and the keys under
  *advanced*;
- **filter rules**, checked as you type, with the syntax and examples;
- a **schedule picker**: manual, hourly, daily at, weekly on, or custom cron,
  with the next three runs shown. New namespace backups get the next free
  quarter hour from 03:00;
- **the destination**, showing its endpoint and bucket;
- for sync jobs, **each side** as a directory on a host, an S3 bucket (a
  configured destination, or another endpoint) or a directory in the pod.

cloud-stack's infra registry (`GET /v1/infra`) only *suggests* candidates
(`GET /v1/discover`, or `backup-stack discover`). It never enrolls a target by
itself.

### Full and incremental backups

A full run captures every item the source reports.

An incremental run compares the source's current state with the state
rebuilt from the target's most recent run. It captures only what was added or
changed, and records the paths that were deleted. This is a tar-style
incremental chain, not content-addressed dedup. If the target has no
successful full run yet, an incremental fails with
`backup: target has no prior successful full run`; it never quietly runs a
full instead.

Change detection uses a fingerprint:

- for ssh-path, `<size>:<mtime>` from the remote `find`;
- for k8s-namespace, a SHA-256 of the object's meaningful fields.

A backup stops at the first item it can't read or write. A backup that
skipped a file would restore an incomplete state, so the run is marked failed
and its stats show how far it got.

### Restore

`backup-stack restore <target-id> <run-id> <dest-dir>` rebuilds the target's
state as of `run-id`. It replays the chain of runs back to the nearest full
run and fetches each item from the run that captured it.

- A k8s-namespace restore writes one JSON manifest per object
  (`kubectl apply -f` each one).
- An ssh-path restore writes the directory tree as it was at that run.

## Sync jobs

A sync job copies a source endpoint onto a destination endpoint. Each side is
one of these:

| type | config | where |
| --- | --- | --- |
| `dir` | `{"type":"dir","path":"/data/photos"}` | a directory in the service's pod (a mounted volume) |
| `ssh` | `{"type":"ssh","addr":"wopr.local:22","user":"chris","path":"/srv/photos"}` (+ `privateKeyPath`, `knownHostsPath`, which default as for ssh-path) | a directory on a lab host |
| `s3` | `{"type":"s3","sink":"primary","prefix":"mirror/photos"}`, or `endpoint`/`bucket`/`accessKey`/`secretKey`/`prefix` | a bucket prefix |

The job's `kind` is derived from the two sides, for example `dir->dir`,
`ssh->s3`, `s3->dir`, `ssh->ssh` or `s3->s3`. An `s3` endpoint that names a
configured `sink` uses that sink's endpoint and credentials, so no keys are
stored in the database. A config with no `type` is S3; that was the only kind
before CS-40.

**How files move**

- **rsync** is used when both sides are directories, at most one of them is
  remote, and rsync is installed both in the service and on the remote host.
  It runs over `ssh -e` with the same key and known_hosts as the native path,
  in batch mode with strict host key checking. Only changed files cross the
  wire.
- **The native copier** handles everything else, and every job when rsync is
  missing (`BACKUP_STACK_RSYNC=off` forces it). It streams over the SSH
  session (`find -printf` to list, `cat` to read, `cat >` into a temporary
  file followed by `mv` to write), or over S3.
- **ssh->ssh** (host to host) always streams through the service, because
  rsync can't copy between two remote hosts.

A file is skipped when the destination already has the same size and
modification time, compared to the second. S3 objects have no modification
time to compare, so they are compared by size only. Modification times are
carried over to directory destinations.

**Never deletes by default.** A job's `delete` option is off unless you set
it. With it on, destination files the source no longer has are removed, but
only files the filters include: an exclude rule means "not this job's
business", never "delete it". A per-file error is counted and the run carries
on. A run with any error is recorded as failed, with sample messages.

## Filters

A filter list is an ordered list of rules:
`[{"kind":"exclude","pattern":"**/node_modules/**"}, {"kind":"include","pattern":"*.jpg"}, {"kind":"exclude","pattern":"**"}]`.
The UI accepts the same rules one per line, as `exclude:pattern` or
`include:pattern` (rsync-style `- pattern` and `+ pattern` also work).

- Patterns are matched against the path relative to the root.
- `*` and `?` stay within one path segment. `**` crosses segments.
- The first matching rule wins. A path no rule matches is included.

Filters apply to sync jobs and to ssh-path targets. backup-stack always
evaluates them itself. For rsync it passes the selected files as an explicit
list (`--files-from`), so rsync and the native copier pick exactly the same
files.

## History, stats and graphs

Every backup run and sync run records:

- bytes and files transferred;
- files skipped (and, for syncs, deleted);
- errors: a count plus up to 10 sample messages;
- duration and rate (bytes per second).

| Endpoint | What it returns |
| --- | --- |
| `GET /v1/targets/{id}/runs`, `GET /v1/sync-jobs/{id}/runs` | one target's or job's runs |
| `GET /v1/history?source=backup\|sync&owner=<id>&since=7d&limit=200` | backup and sync runs together, newest first |
| `GET /v1/stats?source=&owner=&since=24h\|7d\|30d&bucket=1h` | runs grouped into evenly spaced UTC time buckets (runs, failed, bytes, files, errors, busy time, average rate), with totals overall and per target or job; buckets are hourly up to 2 days and daily beyond |
| `POST /v1/sync-jobs/{id}:run` | runs a sync job now; 409 if it is already running |

The admin UI's **History and stats** section has a scope picker (everything,
all backups, all syncs, or one target or job) and a period picker. It shows
four inline-SVG bar charts over time (bytes, files, errors, transfer rate),
with a tooltip on each bar. Below the charts are a per-target and per-job
totals table and the run table. The UI needs no chart library or CDN.

## Scheduling

Each target and each sync job has an optional cron `schedule`: standard
5-field cron, or `@daily`/`@hourly`/`@every 1h`. Blank means manual only.

The scheduler skips anything that already has a run in flight. Runs left
"running" by a crash are marked failed at startup. Backups and syncs share
one concurrency limit (`BACKUP_STACK_CONCURRENCY`, default 4).

## Configuration

| Env var | Purpose |
| --- | --- |
| `BACKUP_STACK_DB` | SQLite file (default `backup-stack.db`) |
| `BACKUP_STACK_TOKEN` | the bearer token the `/v1` API requires (required for `serve`) |
| `BACKUP_STACK_ADDR` | listen address (default `:8481`) |
| `BACKUP_STACK_SINKS_JSON` | `{"<name>": {"endpoint","bucket","accessKey","secretKey","prefix","useSSL","createBucket"}}`: named S3 sinks that a target's `sinkRef` (default `primary`) or an s3 endpoint's `sink` selects |
| `BACKUP_STACK_S3_*` | shorthand for one sink named `primary` when `BACKUP_STACK_SINKS_JSON` isn't set (`_ENDPOINT`, `_BUCKET`, `_ACCESS_KEY`, `_SECRET_KEY`, `_PREFIX`, `_USE_SSL`, `_CREATE_BUCKET`) |
| `BACKUP_STACK_SSH_KEY`, `BACKUP_STACK_SSH_KNOWN_HOSTS` | default SSH key and known_hosts for ssh targets and endpoints |
| `BACKUP_STACK_RSYNC` | rsync binary path, or `off` (default: `rsync` on `PATH`) |
| `BACKUP_STACK_CLUSTER_NAME` | what to call the cluster the service runs in (default `local`; `spark` in the manifest) |
| `BACKUP_STACK_KUBE_DIR` | directory of kubeconfig files, one per other cluster, named after it (default `/etc/backup-stack/kube`) |
| `CLOUDSTACK_URL`, `CLOUDSTACK_TOKEN` | the cloud-stack instance to discover candidates from (optional) |

`GET /healthz` needs no token. It answers 200 while the store answers and 503
otherwise.

## Deployment (spark)

`scripts/deploy-spark.sh` does the following:

1. Stages the source on `spark.local` over ssh.
2. Builds the image there (`Dockerfile`: a static Go binary on
   debian-slim with rsync, openssh-client and ca-certificates, running as
   UID 65532).
3. Imports the image into microk8s' containerd, the same way cloud-stack and
   Depot do. With `REGISTRY=localhost:32000` it pushes there instead.
4. Applies `deploy/k8s/backup-stack.yaml` and rolls the Deployment.

`deploy/k8s/backup-stack.yaml` contains:

- the `backup` namespace;
- a 2 Gi `microk8s-hostpath` PVC for the SQLite database;
- the ConfigMap `backup-stack-config`, whose default sink `primary` is
  `http://wopr.local:9010`, bucket `lab-backups`;
- a ServiceAccount with a ClusterRole that can get and list namespaced
  objects cluster-wide, Secrets included, for backing up spark itself;
- the Deployment: 1 replica, `Recreate`, read-only root filesystem,
  readiness and liveness probes on `/healthz`;
- the Service: NodePort **30922** to 8481.

The script never creates Secrets. It checks for them and prints how to create
any that are missing:

| Secret (namespace `backup`) | Keys | |
| --- | --- | --- |
| `backup-stack-env` | `BACKUP_STACK_TOKEN`, `BACKUP_STACK_S3_ACCESS_KEY`, `BACKUP_STACK_S3_SECRET_KEY` (optionally `BACKUP_STACK_SINKS_JSON`, `CLOUDSTACK_TOKEN`, or any `BACKUP_STACK_S3_*` override) | required |
| `backup-stack-ssh` | `id_ed25519`, `known_hosts`, mounted at `/etc/backup-stack/ssh` | optional; needed for ssh targets and endpoints |
| `backup-stack-kubeconfigs` | one kubeconfig per key, mounted at `/etc/backup-stack/kube` | optional; only for other clusters |

## cloud-stack integration

**Infra tab.** cloud-stack's `infra/inventory.yaml` has a `backup-stack` row
(spark, port 30922, health check `/healthz`).

**Admin UI behind the sign-in.** The admin UI opens through cloud-stack's
sibling-app proxy (`internal/appproxy`, the `APP_PROXIES` setting), the same
way as Depot, Tickets and Blackboard.

- Each proxied app gets its own port. The proxy accepts cloud-stack's session
  cookie and replaces the request's `Authorization` header with the app's real
  token, read from a mounted Secret file.
- It puts the placeholder `via-cloudstack` into the page's localStorage under
  the app's `storageKey`. For backup-stack that key is `backupStackToken`.
- The UI sees `window.__cloudstackProxied`, says "Signed in through
  cloud-stack" and never asks for a token.

Opened directly (NodePort 30922), the UI prompts once for
`BACKUP_STACK_TOKEN` and keeps it in localStorage. Every UI asset and API call
uses a relative URL (`v1/...`), so the UI also works under a path prefix. A
proxy that strips a prefix and sends `X-Forwarded-Prefix` gets a matching
`<base href>`.

Setting this up is a cloud-stack deployment change. In cloud-stack's
`deploy/spark/cloudstack.yaml`:

1. Add this entry to `APP_PROXIES`:
   ```json
   {"name": "backup", "title": "Backups", "listen": ":8501", "upstream": "http://backup-stack.backup.svc.cluster.local:8481",
    "publicPort": 30911, "path": "/", "tokenFile": "/var/run/cloudstack/app-tokens/BACKUP_STACK_TOKEN", "storageKey": "backupStackToken"}
   ```
2. Add the port `{name: backup, port: 8501, nodePort: 30911}` to the
   `cloudstack-apps` Service.
3. Add this source to the api's `app-tokens` projected volume:
   `- secret: {name: backup-stack-app-token, optional: true, items: [{key: BACKUP_STACK_TOKEN, path: BACKUP_STACK_TOKEN}]}`
4. Copy the token into that Secret once:
   ```sh
   kubectl -n backup get secret backup-stack-env -o jsonpath='{.data.BACKUP_STACK_TOKEN}' | base64 -d |
     kubectl -n cloudstack create secret generic backup-stack-app-token --from-file=BACKUP_STACK_TOKEN=/dev/stdin
   ```

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

- No PV/PVC volume-file backup and no database dumps yet. They are the next step: run inside the app's own pods (`tar` of a mounted volume; `pg_dump`/`mysqldump`/`mongodump` in the database container), which needs `pods/exec`.
- No content-addressed chunk-level dedup or garbage collection.
- No bidirectional sync or conflict resolution. Deletes propagate only with
  `delete` explicitly on.
- ssh->ssh syncs stream through the service rather than using rsync.
- No remote agent: every lab host already exposes `ssh:` or a Kubernetes API.
- One static admin bearer token, with no per-user auth or RBAC.

## Development

```
make check   # gofmt -l, go vet, go build, go test (CGO_ENABLED=0)
```

- `internal/sink/s3`'s integration test needs a real MinIO endpoint. It is
  skipped unless `BACKUP_TEST_S3_ENDPOINT` is set.
- `internal/sync`'s SSH end-to-end test runs the real remote commands through
  an in-process SSH server. It needs GNU find and touch, so it is skipped on
  macOS. Run it on Linux, for example in a `golang` container.
- The rsync tests are skipped when rsync isn't installed.
