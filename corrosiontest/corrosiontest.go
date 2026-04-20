// Package corrosiontest provides a test harness for running a real Corrosion
// agent in Docker via testcontainers-go.
//
// Typical usage:
//
//	agent, err := corrosiontest.StartAgent(ctx)
//	if err != nil { t.Fatal(err) }
//	t.Cleanup(func() { agent.Stop(context.Background()) })
//
//	// Apply schema before tests
//	err = agent.ApplySchema(ctx, "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY);")
package corrosiontest

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	// defaultImage is the pre-built corrosion test image. Change when bumping
	// the corrosion pin (update Dockerfile ARG CORROSION_REF too).
	defaultImage = "ghcr.io/iris-xyz/corrosion-test:1b0fcf1"

	apiPort    = "8080/tcp"
	gossipPort = "8787/tcp"
)

// Agent is a running Corrosion agent in Docker.
type Agent struct {
	container testcontainers.Container
	baseURL   string
	adminSock string // host path, empty if not bound

	schemaSeq atomic.Uint64
}

// BaseURL returns the HTTP base URL, e.g. "http://localhost:32768".
func (a *Agent) BaseURL() string { return a.baseURL }

// AdminSocket returns the host path to the admin Unix socket,
// or empty string if WithAdminSocketBind was not used.
func (a *Agent) AdminSocket() string { return a.adminSock }

// Stop terminates the container.
func (a *Agent) Stop(ctx context.Context) error {
	return a.container.Terminate(ctx)
}

// ApplySchema registers DDL with the Corrosion agent so that subsequent
// transactions, queries, AND subscriptions can see the tables.
//
// The DDL is written to a file under the agent's configured schema_paths
// directory (/etc/corrosion/schemas) and then "corrosion reload" is executed
// inside the container so the agent re-reads the schema. Using POST
// /v1/transactions for DDL is insufficient because the subscription engine
// reads from a separate schema catalog populated only via schema_paths.
//
// Tables intended for subscriptions must declare INTEGER NOT NULL PRIMARY KEY
// (or equivalent non-nullable PK) — Corrosion's CRR layer rejects nullable PKs.
func (a *Agent) ApplySchema(ctx context.Context, ddl string) error {
	seq := a.schemaSeq.Add(1)
	path := fmt.Sprintf("/etc/corrosion/schemas/ddl_%d.sql", seq)

	if err := a.container.CopyToContainer(ctx, []byte(ddl), path, 0o644); err != nil {
		return fmt.Errorf("copy schema to container: %w", err)
	}

	code, reader, err := a.container.Exec(ctx, []string{
		"corrosion", "reload", "-c", "/etc/corrosion/config.toml",
	})
	if err != nil {
		return fmt.Errorf("exec corrosion reload: %w", err)
	}
	out, _ := io.ReadAll(reader)
	if code != 0 {
		return fmt.Errorf("corrosion reload exit %d: %s", code, string(out))
	}
	return nil
}

// config holds resolved options for StartAgent.
type config struct {
	image          string // empty = use fallback Dockerfile build
	configTOML     string // empty = use baked image config
	adminSocketDir string // empty = don't bind-mount the socket
}

// Option modifies how the agent container is started.
type Option func(*config)

// WithImage overrides the default pre-built image reference.
// Pass an empty string to force building from the bundled Dockerfile.
func WithImage(ref string) Option {
	return func(c *config) { c.image = ref }
}

// WithConfig overrides the baked-in /etc/corrosion/config.toml with the
// given TOML content. Useful for edge-case tests (e.g. enabling auth).
func WithConfig(toml string) Option {
	return func(c *config) { c.configTOML = toml }
}

// WithAdminSocketBind bind-mounts the container admin socket directory to
// hostDir so tests can reach the admin Unix socket from the host.
func WithAdminSocketBind(hostDir string) Option {
	return func(c *config) { c.adminSocketDir = hostDir }
}

// StartAgent starts a fresh single-node Corrosion agent in Docker and
// waits for the API port to be reachable. The caller must call Stop.
func StartAgent(ctx context.Context, opts ...Option) (*Agent, error) {
	cfg := &config{image: defaultImage}
	for _, o := range opts {
		o(cfg)
	}

	req := testcontainers.ContainerRequest{
		ExposedPorts: []string{apiPort, gossipPort},
		WaitingFor: wait.ForListeningPort(apiPort).
			WithStartupTimeout(120 * time.Second),
		Cmd: []string{"corrosion", "agent", "-c", "/etc/corrosion/config.toml"},
	}

	if cfg.configTOML != "" {
		req.Files = append(req.Files, testcontainers.ContainerFile{
			Reader:            bytes.NewReader([]byte(cfg.configTOML)),
			ContainerFilePath: "/etc/corrosion/config.toml",
			FileMode:          0o644,
		})
	}

	if cfg.adminSocketDir != "" {
		bind := fmt.Sprintf("%s:/var/run/corrosion", cfg.adminSocketDir)
		req.HostConfigModifier = func(hc *container.HostConfig) {
			hc.Binds = append(hc.Binds, bind)
		}
	}

	var (
		container testcontainers.Container
		startErr  error
	)

	if cfg.image != "" {
		req.Image = cfg.image
		container, startErr = testcontainers.GenericContainer(ctx,
			testcontainers.GenericContainerRequest{
				ContainerRequest: req,
				Started:          true,
			})
	} else {
		// Fallback: build from the Dockerfile next to this file.
		_, thisFile, _, _ := runtime.Caller(0)
		dockerfileDir := filepath.Dir(thisFile)

		req.FromDockerfile = testcontainers.FromDockerfile{
			Context:    dockerfileDir,
			Dockerfile: "Dockerfile",
		}
		container, startErr = testcontainers.GenericContainer(ctx,
			testcontainers.GenericContainerRequest{
				ContainerRequest: req,
				Started:          true,
			})
	}

	if startErr != nil {
		return nil, fmt.Errorf("start corrosion container: %w", startErr)
	}

	host, err := container.Host(ctx)
	if err != nil {
		_ = container.Terminate(ctx)
		return nil, fmt.Errorf("container host: %w", err)
	}
	port, err := container.MappedPort(ctx, apiPort)
	if err != nil {
		_ = container.Terminate(ctx)
		return nil, fmt.Errorf("container port: %w", err)
	}

	baseURL := fmt.Sprintf("http://%s:%s", host, port.Port())

	adminSock := ""
	if cfg.adminSocketDir != "" {
		adminSock = filepath.Join(cfg.adminSocketDir, "admin.sock")
		// Corrosion creates admin.sock shortly after startup (after the API port opens),
		// and as root inside the container with mode 0755 — non-root hosts (CI runners)
		// need write permission to connect. Wait for the socket to appear, then chmod it.
		if err := prepareAdminSocket(ctx, container); err != nil {
			_ = container.Terminate(ctx)
			return nil, fmt.Errorf("prepare admin socket: %w", err)
		}
	}

	return &Agent{
		container: container,
		baseURL:   baseURL,
		adminSock: adminSock,
	}, nil
}

// prepareAdminSocket waits for /var/run/corrosion/admin.sock to exist inside the
// container and relaxes its permissions so a non-root host user can dial it.
func prepareAdminSocket(ctx context.Context, c testcontainers.Container) error {
	deadline := time.Now().Add(10 * time.Second)
	for {
		code, _, err := c.Exec(ctx, []string{"test", "-S", "/var/run/corrosion/admin.sock"})
		if err == nil && code == 0 {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("admin socket did not appear within 10s")
		}
		time.Sleep(100 * time.Millisecond)
	}
	code, reader, err := c.Exec(ctx, []string{"chmod", "0666", "/var/run/corrosion/admin.sock"})
	if err != nil {
		return fmt.Errorf("chmod admin socket: %w", err)
	}
	if code != 0 {
		out, _ := io.ReadAll(reader)
		return fmt.Errorf("chmod admin socket exit %d: %s", code, string(out))
	}
	return nil
}
