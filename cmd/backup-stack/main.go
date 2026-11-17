// Command backup-stack is a scheduled backup and cross-location sync
// service for cloud-stack's infrastructure. It is both the server (serve)
// and its own CLI client (targets, run, sync, restore, discover).
//
// Usage:
//
//	backup-stack serve
//	backup-stack targets list|create|delete ...
//	backup-stack run <target-id> [--full]
//	backup-stack sync <sync-job-id>
//	backup-stack restore <target-id> <run-id> <dest-dir>
//	backup-stack discover
//
// See README.md for configuration (environment variables) and the full
// command reference.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path"
	"syscall"
	"time"

	"github.com/geekychris/backup-stack/internal/backup"
	"github.com/geekychris/backup-stack/internal/cloudstackinfra"
	"github.com/geekychris/backup-stack/internal/httpapi"
	"github.com/geekychris/backup-stack/internal/scheduler"
	"github.com/geekychris/backup-stack/internal/sink"
	"github.com/geekychris/backup-stack/internal/source"
	"github.com/geekychris/backup-stack/internal/store"
	"github.com/geekychris/backup-stack/internal/webui"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "backup-stack:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: backup-stack <serve|targets|run|sync|restore|discover> ...")
	}
	ctx := context.Background()
	switch args[0] {
	case "serve":
		return cmdServe(ctx, args[1:])
	case "targets":
		return cmdTargets(ctx, args[1:])
	case "run":
		return cmdRun(ctx, args[1:])
	case "sync":
		return cmdSync(ctx, args[1:])
	case "restore":
		return cmdRestore(ctx, args[1:])
	case "discover":
		return cmdDiscover(ctx, args[1:])
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

// ---- shared configuration ----

func dbPath() string {
	if p := os.Getenv("BACKUP_STACK_DB"); p != "" {
		return p
	}
	return "backup-stack.db"
}

func openStore(ctx context.Context) (*store.Store, error) {
	return store.Open(ctx, dbPath())
}

// sinks is the configured set of named Sinks (S3-compatible buckets), read
// from BACKUP_STACK_SINKS_JSON: {"<name>": {"endpoint":...,"bucket":...}}.
// A target's SinkRef (or "primary" if empty) selects one.
func sinksConfig() (map[string]s3Config, error) {
	raw := os.Getenv("BACKUP_STACK_SINKS_JSON")
	if raw == "" {
		return map[string]s3Config{"primary": s3ConfigFromEnv("BACKUP_STACK_S3")}, nil
	}
	var m map[string]s3Config
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return nil, fmt.Errorf("BACKUP_STACK_SINKS_JSON: %w", err)
	}
	return m, nil
}

type s3Config struct {
	Endpoint     string `json:"endpoint"`
	Region       string `json:"region"`
	Bucket       string `json:"bucket"`
	AccessKey    string `json:"accessKey"`
	SecretKey    string `json:"secretKey"`
	Prefix       string `json:"prefix"`
	UseSSL       bool   `json:"useSSL"`
	CreateBucket bool   `json:"createBucket"`
}

func s3ConfigFromEnv(prefix string) s3Config {
	return s3Config{
		Endpoint:     os.Getenv(prefix + "_ENDPOINT"),
		Region:       os.Getenv(prefix + "_REGION"),
		Bucket:       os.Getenv(prefix + "_BUCKET"),
		AccessKey:    os.Getenv(prefix + "_ACCESS_KEY"),
		SecretKey:    os.Getenv(prefix + "_SECRET_KEY"),
		Prefix:       os.Getenv(prefix + "_PREFIX"),
		UseSSL:       os.Getenv(prefix+"_USE_SSL") == "true",
		CreateBucket: os.Getenv(prefix+"_CREATE_BUCKET") != "false",
	}
}

func cloudStackClient() *cloudstackinfra.Client {
	return cloudstackinfra.New(os.Getenv("CLOUDSTACK_URL"), os.Getenv("CLOUDSTACK_TOKEN"))
}

// ---- target Config parsing (shared by serve's BuildSource and the CLI) ----

type sshTargetConfig struct {
	Addr                     string `json:"addr"`
	User                     string `json:"user"`
	Path                     string `json:"path"`
	PrivateKeyPath           string `json:"privateKeyPath"`
	KnownHostsPath           string `json:"knownHostsPath"`
	InsecureSkipHostKeyCheck bool   `json:"insecureSkipHostKeyCheck"`
}

type k8sTargetConfig struct {
	KubeconfigPath string   `json:"kubeconfigPath"`
	Namespaces     []string `json:"namespaces"`
	ExcludeKinds   []string `json:"excludeKinds"`
}

func buildSource(ctx context.Context, t *store.Target) (source.Source, error) {
	switch t.Type {
	case store.TypeSSHPath:
		var cfg sshTargetConfig
		if err := json.Unmarshal(t.Config, &cfg); err != nil {
			return nil, fmt.Errorf("target %s: ssh-path config: %w", t.ID, err)
		}
		return newSSHSource(cfg)
	case store.TypeK8sNamespace:
		var cfg k8sTargetConfig
		if err := json.Unmarshal(t.Config, &cfg); err != nil {
			return nil, fmt.Errorf("target %s: k8s-namespace config: %w", t.ID, err)
		}
		return newK8sSource(cfg)
	default:
		return nil, fmt.Errorf("target %s: unknown type %q", t.ID, t.Type)
	}
}

func resolveSink(t *store.Target, sinks map[string]s3Config) (sink.Sink, error) {
	ref := t.SinkRef
	if ref == "" {
		ref = "primary"
	}
	cfg, ok := sinks[ref]
	if !ok {
		return nil, fmt.Errorf("target %s: no sink configured named %q", t.ID, ref)
	}
	return newS3Sink(context.Background(), cfg)
}

// ---- serve ----

func cmdServe(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	addr := fs.String("addr", envOr("BACKUP_STACK_ADDR", ":8481"), "listen address")
	if err := fs.Parse(args); err != nil {
		return err
	}
	token := os.Getenv("BACKUP_STACK_TOKEN")
	if token == "" {
		return errors.New("BACKUP_STACK_TOKEN must be set")
	}

	st, err := openStore(ctx)
	if err != nil {
		return err
	}
	defer st.Close()
	if n, err := st.ReconcileStaleRuns(ctx); err != nil {
		return fmt.Errorf("reconcile stale runs: %w", err)
	} else if n > 0 {
		slog.Warn("backup-stack: marked stale in-flight runs as failed on startup", "count", n)
	}

	sinks, err := sinksConfig()
	if err != nil {
		return err
	}
	cs := cloudStackClient()
	runner := &backup.Runner{Store: st}

	apiServer := httpapi.New(httpapi.Deps{
		Store:       st,
		Runner:      runner,
		BuildSource: buildSource,
		ResolveSink: func(t *store.Target) (sink.Sink, error) { return resolveSink(t, sinks) },
		Infra:       cs,
		Token:       token,
	})
	webHandler := &webui.Handler{Store: st, CloudStackConfigured: os.Getenv("CLOUDSTACK_URL") != ""}

	mux := http.NewServeMux()
	mux.Handle("/v1/", apiServer)
	mux.Handle("/healthz", apiServer)
	mux.Handle("/", webHandler)

	runCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	sched := &scheduler.Scheduler{
		Store: st,
		Run: func(ctx context.Context, t *store.Target) error {
			src, err := buildSource(ctx, t)
			if err != nil {
				return err
			}
			defer src.Close()
			sk, err := resolveSink(t, sinks)
			if err != nil {
				return err
			}
			_, fullErr := st.LatestRunOfKind(ctx, t.ID, store.KindFull)
			if errors.Is(fullErr, store.ErrNotFound) {
				_, err = runner.RunFull(ctx, t.ID, src, sk)
				return err
			}
			_, err = runner.RunIncremental(ctx, t.ID, src, sk)
			return err
		},
		Logger: slog.Default(),
	}
	// schedStopped closes once RunLoop has returned (ctx canceled) and every
	// run it dispatched has actually finished, so the caller below can wait
	// for that before defer st.Close() fires — otherwise a scheduler-
	// dispatched backup could still be writing to the Store after it closes.
	schedStopped := make(chan struct{})
	go func() {
		defer close(schedStopped)
		sched.RunLoop(runCtx)
		sched.Wait()
	}()

	srv := &http.Server{Addr: *addr, Handler: mux}
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		<-runCtx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	slog.Info("backup-stack: listening", "addr", *addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	<-shutdownDone

	const schedGrace = 30 * time.Second
	select {
	case <-schedStopped:
	case <-time.After(schedGrace):
		slog.Warn("backup-stack: scheduler and in-flight runs did not stop within grace period; exiting anyway", "grace", schedGrace)
	}
	return nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// ---- targets ----

func cmdTargets(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: backup-stack targets <list|create|delete> ...")
	}
	st, err := openStore(ctx)
	if err != nil {
		return err
	}
	defer st.Close()

	switch args[0] {
	case "list":
		list, err := st.ListTargets(ctx)
		if err != nil {
			return err
		}
		return printJSON(list)
	case "create":
		fs := flag.NewFlagSet("targets create", flag.ExitOnError)
		id := fs.String("id", "", "target id (default: generated)")
		name := fs.String("name", "", "target name")
		typ := fs.String("type", "", "ssh-path | k8s-namespace")
		config := fs.String("config", "{}", "JSON config")
		sinkRef := fs.String("sink", "primary", "sink name")
		schedule := fs.String("schedule", "", "cron schedule (blank = manual only)")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if *name == "" || *typ == "" {
			return errors.New("--name and --type are required")
		}
		if *id == "" {
			*id = "tgt_" + *name
		}
		t := &store.Target{ID: *id, Name: *name, Type: *typ, Config: []byte(*config), SinkRef: *sinkRef, Schedule: *schedule, Enabled: true}
		if err := st.CreateTarget(ctx, t); err != nil {
			return err
		}
		return printJSON(t)
	case "delete":
		fs := flag.NewFlagSet("targets delete", flag.ExitOnError)
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if fs.NArg() != 1 {
			return errors.New("usage: backup-stack targets delete <id>")
		}
		return st.DeleteTarget(ctx, fs.Arg(0))
	default:
		return fmt.Errorf("unknown targets subcommand %q", args[0])
	}
}

// ---- run ----

func cmdRun(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	full := fs.Bool("full", false, "force a full backup even if the target has run before")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: backup-stack run [--full] <target-id>")
	}
	st, err := openStore(ctx)
	if err != nil {
		return err
	}
	defer st.Close()
	t, err := st.GetTarget(ctx, fs.Arg(0))
	if err != nil {
		return err
	}
	src, err := buildSource(ctx, t)
	if err != nil {
		return err
	}
	defer src.Close()
	sinks, err := sinksConfig()
	if err != nil {
		return err
	}
	sk, err := resolveSink(t, sinks)
	if err != nil {
		return err
	}
	runner := &backup.Runner{Store: st}
	_, fullErr := st.LatestRunOfKind(ctx, t.ID, store.KindFull)
	var runResult *store.Run
	if *full || errors.Is(fullErr, store.ErrNotFound) {
		runResult, err = runner.RunFull(ctx, t.ID, src, sk)
	} else {
		runResult, err = runner.RunIncremental(ctx, t.ID, src, sk)
	}
	if err != nil {
		return err
	}
	return printJSON(runResult)
}

// ---- sync ----

func cmdSync(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("sync", flag.ExitOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: backup-stack sync <sync-job-id>")
	}
	st, err := openStore(ctx)
	if err != nil {
		return err
	}
	defer st.Close()
	j, err := st.GetSyncJob(ctx, fs.Arg(0))
	if err != nil {
		return err
	}
	result, err := runSyncJob(ctx, j)
	if err != nil {
		_ = st.FinishSyncJob(ctx, j.ID, store.StateFailed, err.Error())
		return err
	}
	if err := st.FinishSyncJob(ctx, j.ID, store.StateSucceeded, ""); err != nil {
		return err
	}
	return printJSON(result)
}

// ---- restore ----

func cmdRestore(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("restore", flag.ExitOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 3 {
		return errors.New("usage: backup-stack restore <target-id> <run-id> <dest-dir>")
	}
	targetID, runID, destDir := fs.Arg(0), fs.Arg(1), fs.Arg(2)

	st, err := openStore(ctx)
	if err != nil {
		return err
	}
	defer st.Close()
	t, err := st.GetTarget(ctx, targetID)
	if err != nil {
		return err
	}
	sinks, err := sinksConfig()
	if err != nil {
		return err
	}
	sk, err := resolveSink(t, sinks)
	if err != nil {
		return err
	}
	runner := &backup.Runner{Store: st}
	state, owner, err := runner.ChainCatalog(ctx, targetID, runID, sk)
	if err != nil {
		return err
	}
	for _, item := range state.Items {
		ownerRunID := owner[item.Path]
		r, err := sk.OpenItem(ctx, ownerRunID, item.Path)
		if err != nil {
			return fmt.Errorf("restore %s (from run %s): %w", item.Path, ownerRunID, err)
		}
		dest := path.Join(destDir, item.Path)
		if err := writeFileAll(dest, r); err != nil {
			r.Close()
			return err
		}
		r.Close()
	}
	fmt.Printf("restored %d items to %s\n", len(state.Items), destDir)
	return nil
}

// ---- discover ----

func cmdDiscover(ctx context.Context, _ []string) error {
	cs := cloudStackClient()
	candidates, err := cs.ListCandidates(ctx)
	if err != nil {
		return err
	}
	return printJSON(candidates)
}

// ---- helpers ----

func printJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
