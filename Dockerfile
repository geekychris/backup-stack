# backup-stack: one static binary (API, admin UI, scheduler), plus rsync and
# the OpenSSH client for directory syncs to and from lab hosts.
#   docker build -t backup-stack:dev .
FROM golang:1.26-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/backup-stack ./cmd/backup-stack

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends rsync openssh-client ca-certificates tzdata \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd --gid 65532 backupstack \
 && useradd --uid 65532 --gid 65532 --home-dir /data --no-create-home --shell /usr/sbin/nologin backupstack \
 && mkdir -p /data && chown 65532:65532 /data
COPY --from=build /out/backup-stack /usr/local/bin/backup-stack
USER 65532:65532
# HOME is the data volume so ssh has somewhere to put its state; known_hosts
# itself comes from the mounted Secret (BACKUP_STACK_SSH_KNOWN_HOSTS).
ENV HOME=/data BACKUP_STACK_DB=/data/backup-stack.db BACKUP_STACK_ADDR=:8481
EXPOSE 8481
VOLUME /data
ENTRYPOINT ["/usr/local/bin/backup-stack"]
CMD ["serve"]
