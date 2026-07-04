//
// Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package e2e_go

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	rootPassword = "playwithdata"
	tlsStorePass = "changeit"
	httpPort     = "2480"
	boltPort     = "7687"

	// baseJavaOpts is shared by the plain container here and the TLS containers
	// in tls_test.go (which append their own arcadedb.bolt.ssl/arcadedb.ssl.*
	// sysprops). It matches the e2e-python/e2e-csharp fixtures: root password,
	// the imported OpenBeer dataset seeded as the "beer" database, and the Bolt
	// plugin enabled.
	baseJavaOpts = "-Darcadedb.server.rootPassword=" + rootPassword + " " +
		"-Darcadedb.server.defaultDatabases=beer[root]{import:https://github.com/ArcadeData/arcadedb-datasets/raw/main/orientdb/OpenBeer.gz} " +
		"-Darcadedb.server.plugins=BoltProtocolPlugin"
)

// httpClient carries a timeout so a hung server fails the seeding/readiness
// calls promptly instead of stalling the whole suite until the go test deadline.
var httpClient = &http.Client{Timeout: 30 * time.Second}

// plainContainer is shared by all non-TLS scenarios. Several tests write into
// its "beer"/"boltscratch" databases (TX-002/004, RESULT-004, MDB-002, the
// race probes); isolation rests on unique per-test marker values and on the
// tests running sequentially (no t.Parallel), not on separate databases. A
// future count-based assertion must target a fresh database instead.
var (
	ctx            = context.Background()
	plainContainer *arcadeContainer

	// tlsCleanups holds teardown callbacks for lazily-started TLS resources
	// (derived image, containers, temp cert dir) registered by tls_test.go and
	// run at the end of the package by tlsTeardown.
	tlsCleanupMu sync.Mutex
	tlsCleanups  []func()
)

func addTLSCleanup(f func()) {
	tlsCleanupMu.Lock()
	tlsCleanups = append(tlsCleanups, f)
	tlsCleanupMu.Unlock()
}

func tlsTeardown() {
	tlsCleanupMu.Lock()
	defer tlsCleanupMu.Unlock()
	for i := len(tlsCleanups) - 1; i >= 0; i-- {
		tlsCleanups[i]()
	}
}

// arcadeContainer wraps a running testcontainers container with its cached host
// and mapped ports.
type arcadeContainer struct {
	container testcontainers.Container
	host      string
	httpPort  string
	boltPort  string
}

// imageName honors ARCADEDB_DOCKER_IMAGE (the CI job passes the exact
// build-and-package output tag) and defaults to arcadedata/arcadedb:latest,
// which the CI docker-load step populates in the local daemon.
func imageName() string {
	if v := strings.TrimSpace(os.Getenv("ARCADEDB_DOCKER_IMAGE")); v != "" {
		return v
	}
	return "arcadedata/arcadedb:latest"
}

// startArcade boots an ArcadeDB container with the given JAVA_OPTS and image,
// waiting until the HTTP readiness endpoint returns 204.
func startArcade(javaOpts, image string, startupTimeout time.Duration) (*arcadeContainer, error) {
	req := testcontainers.ContainerRequest{
		Image:        image,
		ExposedPorts: []string{httpPort + "/tcp", boltPort + "/tcp"},
		Env:          map[string]string{"JAVA_OPTS": javaOpts},
		WaitingFor: wait.ForHTTP("/api/v1/ready").
			WithPort(httpPort + "/tcp").
			WithStatusCodeMatcher(func(status int) bool { return status == http.StatusNoContent }).
			WithStartupTimeout(startupTimeout),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, err
	}
	host, err := c.Host(ctx)
	if err != nil {
		_ = c.Terminate(ctx)
		return nil, err
	}
	hp, err := c.MappedPort(ctx, httpPort)
	if err != nil {
		_ = c.Terminate(ctx)
		return nil, err
	}
	bp, err := c.MappedPort(ctx, boltPort)
	if err != nil {
		_ = c.Terminate(ctx)
		return nil, err
	}
	return &arcadeContainer{container: c, host: host, httpPort: hp.Port(), boltPort: bp.Port()}, nil
}

func (c *arcadeContainer) httpBase() string {
	return fmt.Sprintf("http://%s:%s", c.host, c.httpPort)
}

func boltURI(c *arcadeContainer, scheme string) string {
	return fmt.Sprintf("%s://%s:%s", scheme, c.host, c.boltPort)
}

// newDriver opens a driver with basic root auth and registers its close with
// the test's cleanup.
func newDriver(t *testing.T, uri string) neo4j.DriverWithContext {
	t.Helper()
	d, err := neo4j.NewDriverWithContext(uri, neo4j.BasicAuth("root", rootPassword, ""))
	if err != nil {
		t.Fatalf("failed to create driver for %s: %v", uri, err)
	}
	t.Cleanup(func() { _ = d.Close(ctx) })
	return d
}

// repoRoot walks up from the test working directory until it finds a
// bolt/conformance directory (the Go analog of the C# suite's FindRepoRoot).
func repoRoot() string {
	dir, _ := os.Getwd()
	for {
		if _, err := os.Stat(filepath.Join(dir, "bolt", "conformance")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			panic("could not locate repo root containing bolt/conformance")
		}
		dir = parent
	}
}

// httpCommand POSTs a JSON body to an ArcadeDB HTTP endpoint with root auth.
func httpCommand(c *arcadeContainer, path string, body []byte) error {
	req, err := http.NewRequest(http.MethodPost, c.httpBase()+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.SetBasicAuth("root", rootPassword)
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("POST %s -> %d: %s", path, resp.StatusCode, string(msg))
	}
	return nil
}

func createDatabase(c *arcadeContainer, name string) error {
	body, _ := json.Marshal(map[string]string{"command": "create database " + name})
	return httpCommand(c, "/api/v1/server", body)
}

// seedTypeMatrix loads the shared type-matrix Cypher fixture over HTTP (never
// over Bolt - seeding must not exercise the serialization path under test).
func seedTypeMatrix(c *arcadeContainer) error {
	cypher, err := os.ReadFile(filepath.Join(repoRoot(), "bolt", "conformance", "fixtures", "type-matrix.cypher"))
	if err != nil {
		return err
	}
	body, _ := json.Marshal(map[string]string{"language": "cypher", "command": string(cypher)})
	return httpCommand(c, "/api/v1/command/beer", body)
}

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

// run isolates setup/teardown so deferred cleanups execute before os.Exit.
func run(m *testing.M) int {
	c, err := startArcade(baseJavaOpts, imageName(), 120*time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start ArcadeDB container: %v\n", err)
		return 1
	}
	defer func() { _ = c.container.Terminate(ctx) }()
	plainContainer = c

	if err := createDatabase(c, "boltscratch"); err != nil {
		fmt.Fprintf(os.Stderr, "failed to create boltscratch db: %v\n", err)
		return 1
	}
	if err := seedTypeMatrix(c); err != nil {
		fmt.Fprintf(os.Stderr, "failed to seed type-matrix: %v\n", err)
		return 1
	}

	// tlsTeardown (tls_test.go) tears down any lazily-started TLS containers;
	// it is a no-op when no TLS scenario ran.
	defer tlsTeardown()
	return m.Run()
}
