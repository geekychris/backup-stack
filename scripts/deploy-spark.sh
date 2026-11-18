#!/usr/bin/env bash
# Builds backup-stack's image on spark, makes it available to microk8s
# (imported into its containerd, as cloud-stack and Depot do, or pushed to
# REGISTRY when that is set), applies deploy/k8s/backup-stack.yaml and rolls
# the Deployment. Re-run to redeploy.
#
#   scripts/deploy-spark.sh
#   BACKUP_HOST=spark.tail8791ce.ts.net SSH_OPTS="-o HostKeyAlias=spark.local" scripts/deploy-spark.sh
#   REGISTRY=localhost:32000 scripts/deploy-spark.sh   # push to microk8s' registry addon instead
#
# BACKUP_HOST    ssh host (default spark.local)
# SSH_OPTS       extra ssh/rsync options
# KUBECTL        kubectl on the host (default "microk8s kubectl")
# STORAGE_CLASS  PVC storage class (default microk8s-hostpath)
# REGISTRY       push the image there instead of importing it into containerd
# CTR            containerd client on the host
#                (default "ctr -a /var/snap/microk8s/common/run/containerd.sock")
#
# It never creates Secrets: it checks for the ones the Deployment reads and
# prints how to create any that are missing (see deploy/k8s/backup-stack.yaml).
set -euo pipefail
cd "$(dirname "$0")/.."

host="${BACKUP_HOST:-spark.local}"
ssh_opts=(${SSH_OPTS:-})
kubectl="${KUBECTL:-microk8s kubectl}"
storage_class="${STORAGE_CLASS:-microk8s-hostpath}"
registry="${REGISTRY:-}"
ctr="${CTR:-ctr -a /var/snap/microk8s/common/run/containerd.sock}"
remote_dir="backup-stack-build"
version="$(git describe --always --dirty 2>/dev/null || echo dev)"
image="backup-stack:${version}"
[ -n "$registry" ] && image="${registry%/}/${image}"
on() { ssh "${ssh_opts[@]}" "$host" "$@"; }
say() { printf '\n[deploy-backup-stack] %s\n' "$*"; }

say "staging sources on $host:~/$remote_dir"
on "mkdir -p ~/$remote_dir"
rsync -az --delete -e "ssh ${ssh_opts[*]}" \
  --exclude .git --exclude '/bin/' --exclude '*.db' --exclude '*.db-*' \
  ./ "$host:$remote_dir/"

say "building $image on $host"
on "cd ~/$remote_dir && docker build -q -t $(printf %q "$image") ."

if [ -n "$registry" ]; then
  say "pushing to $registry"
  on "docker push -q $(printf %q "$image") >/dev/null"
else
  say "importing into microk8s containerd"
  on "docker save $(printf %q "$image") | $ctr -n k8s.io images import -"
fi

say "applying manifests"
sed -e "s|@IMAGE@|$image|" -e "s|@STORAGE_CLASS@|$storage_class|" deploy/k8s/backup-stack.yaml | on "$kubectl apply -f -"

say "secrets"
missing_env=0
if ! on "$kubectl -n backup get secret backup-stack-env >/dev/null 2>&1"; then
  missing_env=1
  cat <<EOF
  MISSING (required): Secret backup/backup-stack-env. Create it on $host, e.g.:
    $kubectl -n backup create secret generic backup-stack-env \\
      --from-literal=BACKUP_STACK_TOKEN="\$(openssl rand -hex 24)" \\
      --from-literal=BACKUP_STACK_S3_ACCESS_KEY=<wopr MinIO access key> \\
      --from-literal=BACKUP_STACK_S3_SECRET_KEY=<wopr MinIO secret key>
EOF
else
  echo "  backup-stack-env: present"
fi
if ! on "$kubectl -n backup get secret backup-stack-ssh >/dev/null 2>&1"; then
  cat <<EOF
  missing (optional, needed for ssh-path targets and ssh sync endpoints): Secret backup/backup-stack-ssh:
    $kubectl -n backup create secret generic backup-stack-ssh \\
      --from-file=id_ed25519=<private key authorised on the lab hosts> \\
      --from-file=known_hosts=<known_hosts with every host you will reach>
EOF
else
  echo "  backup-stack-ssh: present"
fi
if ! on "$kubectl -n backup get secret backup-stack-kubeconfigs >/dev/null 2>&1"; then
  echo "  missing (optional, only for k8s-namespace targets on other clusters): Secret backup/backup-stack-kubeconfigs (one kubeconfig file per key)"
else
  echo "  backup-stack-kubeconfigs: present"
fi

if [ "$missing_env" = 1 ]; then
  say "not rolling out: the pod can't start without backup-stack-env (create it, then re-run)"
  exit 1
fi

on "$kubectl -n backup set image deployment/backup-stack backup-stack=$(printf %q "$image") >/dev/null"
on "$kubectl -n backup rollout status deployment/backup-stack --timeout=180s"

say "done: UI and API http://${host}:30922  (health: /healthz)"
