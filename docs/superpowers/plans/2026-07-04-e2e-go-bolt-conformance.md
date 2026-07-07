# e2e-go Bolt Conformance Suite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a new standalone `e2e-go/` module that certifies the official `neo4j-go-driver/v5` against all 39 scenarios in `bolt/conformance/spec.yaml`, plus a PR-gating `go-e2e-tests` CI job - mirroring the merged e2e-python (#4885) and e2e-csharp (#4886) suites.

**Architecture:** Standalone Go module (outside the Maven reactor, like e2e-python/e2e-js/e2e-csharp). A `TestMain`-managed shared testcontainers-go container serves ~37 non-TLS scenarios; two lazy TLS containers serve CONN-002/005. Data is seeded over HTTP (never Bolt). Known gaps use a strict-xfail helper ported from C#'s `KnownGapAssertions`; infra-unavailable scenarios use `t.Skip`.

**Tech Stack:** Go 1.23, `github.com/neo4j/neo4j-go-driver/v5`, `github.com/testcontainers/testcontainers-go`, `github.com/stretchr/testify`.

## Global Constraints

- Module lives at repo-root `e2e-go/`; it is NOT added to the root `pom.xml` `<modules>` (matches the other e2e modules).
- One test function per spec scenario, named `Test_<AREA>_<NNN>_<slug>` (e.g. `Test_TYPE_011_DurationRoundtrip`) - the cross-language traceability convention in `bolt/conformance/README.md`.
- Every scenario's `current_status` outcome must match `spec.yaml` and the Python/C# suites: `passing`/`unverified` -> a normal green test; `expected-fail` -> strict-xfail via `assertStillFails`; HA/raw-socket -> `t.Skip`.
- Container image resolution honors `ARCADEDB_DOCKER_IMAGE` env, default `arcadedata/arcadedb:latest`.
- Root password: `playwithdata`. TLS store password: `changeit`.
- Data seeding (create db, load type-matrix) is over HTTP port 2480 only - never over Bolt.
- Apache 2.0 license header on every `.go` file (copy the exact header block from any e2e-csharp `.cs` file, in `//` form).
- No `System.out`-style debug leftovers; no Claude attribution in commits or files.
- All commits are made from the worktree `.worktrees/feat/4887-e2e-go` on branch `feat/4887-e2e-go`.

## File Structure

```
e2e-go/
  go.mod                       # module arcadedb.com/e2e-go; pinned deps
  go.sum
  README.md                    # driver bands, run instructions, traceability
  license_header.txt           # (reference only, optional) not required
  fixtures_test.go             # TestMain, shared plain container, HTTP seeding, repo-root + image resolution, bolt URI helper
  tls_test.go                  # keytool cert-gen, derived-image build, TLS-required/optional containers (lazy sync.Once)
  knowngap_test.go             # assertStillFails() strict-xfail helper
  bolt_conformance_test.go     # 39 Test_<AREA>_<NNN>_<slug> functions + two-writer race helper
```

`.github/workflows/mvn-test.yml` - add one `go-e2e-tests` job (Task 10).

---

### Task 1: Module scaffold, container fixture, and first green test (CONN-001, CONN-003, CONN-004)

**Files:**
- Create: `e2e-go/go.mod`
- Create: `e2e-go/fixtures_test.go`
- Create: `e2e-go/bolt_conformance_test.go`
- Create: `e2e-go/knowngap_test.go`

**Interfaces:**
- Produces (used by all later tasks):
  - `plainContainer` (package var, `*arcadeContainer`) - the shared running container, set in `TestMain`.
  - `boltURI(c *arcadeContainer, scheme string) string` - e.g. `boltURI(plainContainer, "bolt")`.
  - `httpBase(c *arcadeContainer) string` - `http://host:mappedPort2480`.
  - `newDriver(t, uri) neo4j.DriverWithContext` - opens a driver with basic root auth, registers `t.Cleanup` close.
  - `ctx` (package var, `context.Background()`).
  - `rootPassword` const = `"playwithdata"`.
  - `repoRoot() string` - walks up to the dir containing `bolt/conformance`.
  - `assertStillFails(t *testing.T, reason string, body func() error)` - strict-xfail helper (in knowngap_test.go).

- [ ] **Step 1: Create `e2e-go/go.mod`**

```
module arcadedb.com/e2e-go

go 1.23

require (
	github.com/neo4j/neo4j-go-driver/v5 v5.28.1
	github.com/stretchr/testify v1.9.0
	github.com/testcontainers/testcontainers-go v0.34.0
)
```

(Exact patch versions are resolved by `go mod tidy` in Step 4; these are floors.)

- [ ] **Step 2: Create `e2e-go/knowngap_test.go`**

```go
// <Apache 2.0 header in // form>
package e2e_go

import "testing"

// assertStillFails ports C#'s KnownGapAssertions.AssertStillFailsAsync: Go's
// testing package has no xfail(strict=True). body asserts the Neo4j-correct
// behavior and returns a non-nil error while the known gap still reproduces
// (a driver error, or a value that does not match the correct expectation).
// If body returns nil the gap has been fixed - fail loudly so whoever fixed
// it converts this to a normal test and updates current_status in
// bolt/conformance/spec.yaml in the same PR.
func assertStillFails(t *testing.T, reason string, body func() error) {
	t.Helper()
	if err := body(); err != nil {
		t.Logf("known gap still reproduces (expected): %v", err)
		return
	}
	t.Fatalf("XPASS: known gap no longer reproduces - convert to a normal "+
		"test and update current_status in bolt/conformance/spec.yaml. Gap: %s", reason)
}
```

- [ ] **Step 3: Create `e2e-go/fixtures_test.go`**

```go
// <Apache 2.0 header in // form>
package e2e_go

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	rootPassword    = "playwithdata"
	tlsStorePass    = "changeit"
	httpPort        = "2480"
	boltPort        = "7687"
	baseJavaOptsFmt = "-Darcadedb.server.rootPassword=" + rootPassword + " " +
		"-Darcadedb.server.defaultDatabases=beer[root]{import:https://github.com/ArcadeData/arcadedb-datasets/raw/main/orientdb/OpenBeer.gz} " +
		"-Darcadedb.server.plugins=BoltProtocolPlugin"
)

var (
	ctx            = context.Background()
	plainContainer *arcadeContainer
)

// arcadeContainer wraps a running testcontainers container with cached host/ports.
type arcadeContainer struct {
	container testcontainers.Container
	host      string
	httpPort  string
	boltPort  string
}

func imageName() string {
	if v := strings.TrimSpace(os.Getenv("ARCADEDB_DOCKER_IMAGE")); v != "" {
		return v
	}
	return "arcadedata/arcadedb:latest"
}

func startArcade(javaOpts, image string) (*arcadeContainer, error) {
	req := testcontainers.ContainerRequest{
		Image:        image,
		ExposedPorts: []string{httpPort + "/tcp", boltPort + "/tcp"},
		Env:          map[string]string{"JAVA_OPTS": javaOpts},
		WaitingFor: wait.ForHTTP("/api/v1/ready").
			WithPort(httpPort+"/tcp").
			WithStatusCodeMatcher(func(status int) bool { return status == http.StatusNoContent }).
			WithStartupTimeout(120 * time.Second),
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
		return nil, err
	}
	hp, err := c.MappedPort(ctx, httpPort)
	if err != nil {
		return nil, err
	}
	bp, err := c.MappedPort(ctx, boltPort)
	if err != nil {
		return nil, err
	}
	return &arcadeContainer{container: c, host: host, httpPort: hp.Port(), boltPort: bp.Port()}, nil
}

func (c *arcadeContainer) httpBase() string { return fmt.Sprintf("http://%s:%s", c.host, c.httpPort) }

func boltURI(c *arcadeContainer, scheme string) string {
	return fmt.Sprintf("%s://%s:%s", scheme, c.host, c.boltPort)
}

// httpBase is also exposed as a free function for symmetry with call sites.
func httpBase(c *arcadeContainer) string { return c.httpBase() }

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
// bolt/conformance directory (the Go analog of C#'s FindRepoRoot).
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

func httpCommand(c *arcadeContainer, path, contentType, body string) error {
	req, err := http.NewRequest(http.MethodPost, c.httpBase()+path, strings.NewReader(body))
	if err != nil {
		return err
	}
	req.SetBasicAuth("root", rootPassword)
	req.Header.Set("Content-Type", contentType)
	resp, err := http.DefaultClient.Do(req)
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
	return httpCommand(c, "/api/v1/server", "application/json",
		fmt.Sprintf(`{"command":"create database %s"}`, name))
}

func seedTypeMatrix(c *arcadeContainer) error {
	cypher, err := os.ReadFile(filepath.Join(repoRoot(), "bolt", "conformance", "fixtures", "type-matrix.cypher"))
	if err != nil {
		return err
	}
	payload, err := jsonCypher(string(cypher))
	if err != nil {
		return err
	}
	return httpCommand(c, "/api/v1/command/beer", "application/json", payload)
}

// jsonCypher builds a well-formed JSON body for the HTTP command endpoint,
// letting encoding/json escape the multi-line Cypher fixture safely.
func jsonCypher(command string) (string, error) {
	b, err := jsonMarshal(map[string]string{"language": "cypher", "command": command})
	return string(b), err
}

func TestMain(m *testing.M) {
	code := run(m)
	os.Exit(code)
}

// run isolates setup/teardown so deferred cleanups execute before os.Exit.
func run(m *testing.M) int {
	c, err := startArcade(baseJavaOptsFmt, imageName())
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

	defer tlsTeardown() // no-op if no TLS container was started; defined in tls_test.go
	return m.Run()
}
```

Add a tiny `encoding/json` wrapper to avoid importing json in two files:

```go
// at top of fixtures_test.go imports add "encoding/json"
func jsonMarshal(v any) ([]byte, error) { return json.Marshal(v) }
```

(Fold the `encoding/json` import into the import block; remove the standalone comment.)

- [ ] **Step 4: Create `e2e-go/bolt_conformance_test.go` with the connection scenarios**

```go
// <Apache 2.0 header in // form>
package e2e_go

import (
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/require"
)

// runSingle runs a Cypher statement against the given database and returns the
// single result record, failing the test on any driver error.
func runSingle(t *testing.T, d neo4j.DriverWithContext, database, cypher string, params map[string]any) *neo4j.Record {
	t.Helper()
	sess := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: database})
	t.Cleanup(func() { _ = sess.Close(ctx) })
	res, err := sess.Run(ctx, cypher, params)
	require.NoError(t, err)
	rec, err := res.Single(ctx)
	require.NoError(t, err)
	return rec
}

// --- connection ---------------------------------------------------------

func Test_CONN_001_ConnectBolt(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	require.NoError(t, d.VerifyConnectivity(ctx))
}

func Test_CONN_003_Neo4jRoutingSingleNode(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "neo4j"))
	require.NoError(t, d.VerifyConnectivity(ctx))
	rec := runSingle(t, d, "beer", "MATCH (b:Beer) RETURN b.name AS name LIMIT 1", nil)
	name, _ := rec.Get("name")
	require.NotNil(t, name)
}

func Test_CONN_004_Neo4jRoutingHATopology(t *testing.T) {
	t.Skip("Requires a 3-node HA cluster; the single-node harness cannot " +
		"meaningfully exercise this scenario without new multi-node " +
		"orchestration infrastructure - see #4890")
}
```

- [ ] **Step 5: Tidy and run the harness**

Run:
```bash
cd e2e-go && go mod tidy && go test -run 'Test_CONN_001|Test_CONN_003|Test_CONN_004' -v ./...
```
Expected: `Test_CONN_001` PASS, `Test_CONN_003` PASS, `Test_CONN_004` SKIP. (Requires the branch image locally, or it pulls `arcadedata/arcadedb:latest`.)

> If the local image is stale, build it first: from repo root `./mvnw install -Pdocker -DskipTests -pl bolt,package -am -q` produces `arcadedata/arcadedb:latest`.

- [ ] **Step 6: Commit**

```bash
git add e2e-go/go.mod e2e-go/go.sum e2e-go/fixtures_test.go e2e-go/knowngap_test.go e2e-go/bolt_conformance_test.go
git commit -m "feat(#4887): scaffold e2e-go module with container fixture and connection scenarios"
```

---

### Task 2: TLS scenarios (CONN-002, CONN-005)

**Files:**
- Create: `e2e-go/tls_test.go`
- Modify: `e2e-go/bolt_conformance_test.go` (add the two TLS tests)

**Interfaces:**
- Produces: `tlsRequiredContainer(t) *arcadeContainer`, `tlsOptionalContainer(t) *arcadeContainer` (lazy, `sync.Once`), `tlsTeardown()` (called from `run` in fixtures_test.go).

- [ ] **Step 1: Create `e2e-go/tls_test.go`**

```go
// <Apache 2.0 header in // form>
package e2e_go

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"net/http"
)

var (
	tlsOnce      sync.Once
	tlsReqOnce   sync.Once
	tlsOptOnce   sync.Once
	tlsImageTag  string
	tlsBuildErr  error
	tlsReq       *arcadeContainer
	tlsOpt       *arcadeContainer
	tlsCleanups  []func()
	tlsCleanupMu sync.Mutex
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

// buildTLSImage generates a throwaway self-signed keystore/truststore with
// keytool and bakes them into a derived image via docker build COPY (the same
// approach the Python/C# suites use - bind mounts are unreliable on CI
// runners). Returns the derived image tag.
func buildTLSImage() (string, error) {
	tlsOnce.Do(func() {
		keytool, err := exec.LookPath("keytool")
		if err != nil {
			tlsBuildErr = fmt.Errorf("keytool not found on PATH - a JDK is required for the TLS scenarios (CONN-002/CONN-005): %w", err)
			return
		}
		dir, err := os.MkdirTemp("", "bolt-tls-certs")
		if err != nil {
			tlsBuildErr = err
			return
		}
		ks := filepath.Join(dir, "keystore.p12")
		ts := filepath.Join(dir, "truststore.jks")
		cer := filepath.Join(dir, "bolt.cer")
		runKeytool := func(args ...string) error {
			out, err := exec.Command(keytool, args...).CombinedOutput()
			if err != nil {
				return fmt.Errorf("keytool %v: %v\n%s", args, err, out)
			}
			return nil
		}
		if tlsBuildErr = runKeytool("-genkeypair", "-alias", "bolt", "-keyalg", "RSA",
			"-keysize", "2048", "-validity", "3650", "-keystore", ks, "-storetype", "PKCS12",
			"-storepass", tlsStorePass, "-keypass", tlsStorePass,
			"-dname", "CN=localhost, OU=ArcadeDB, O=ArcadeDB, L=Test, ST=Test, C=US"); tlsBuildErr != nil {
			return
		}
		if tlsBuildErr = runKeytool("-exportcert", "-alias", "bolt", "-keystore", ks,
			"-storetype", "PKCS12", "-storepass", tlsStorePass, "-file", cer); tlsBuildErr != nil {
			return
		}
		if tlsBuildErr = runKeytool("-importcert", "-alias", "bolt", "-keystore", ts,
			"-storetype", "JKS", "-storepass", tlsStorePass, "-file", cer, "-noprompt"); tlsBuildErr != nil {
			return
		}
		dockerfile := fmt.Sprintf("FROM %s\nCOPY --chown=arcadedb:arcadedb keystore.p12 truststore.jks /home/arcadedb/tls_certs/\n", imageName())
		if tlsBuildErr = os.WriteFile(filepath.Join(dir, "Dockerfile"), []byte(dockerfile), 0o644); tlsBuildErr != nil {
			return
		}
		tag := "arcadedb-bolt-tls-go:latest"
		out, err := exec.Command("docker", "build", "-t", tag, dir).CombinedOutput()
		if err != nil {
			tlsBuildErr = fmt.Errorf("docker build: %v\n%s", err, out)
			return
		}
		tlsImageTag = tag
		addTLSCleanup(func() {
			_ = exec.Command("docker", "image", "rm", "-f", tag).Run()
			_ = os.RemoveAll(dir)
		})
	})
	return tlsImageTag, tlsBuildErr
}

func startTLSContainer(sslMode string) (*arcadeContainer, error) {
	image, err := buildTLSImage()
	if err != nil {
		return nil, err
	}
	opts := baseJavaOptsFmt + " " +
		"-Darcadedb.bolt.ssl=" + sslMode + " " +
		"-Darcadedb.ssl.keyStore=/home/arcadedb/tls_certs/keystore.p12 " +
		"-Darcadedb.ssl.keyStorePassword=" + tlsStorePass + " " +
		"-Darcadedb.ssl.trustStore=/home/arcadedb/tls_certs/truststore.jks " +
		"-Darcadedb.ssl.trustStorePassword=" + tlsStorePass
	req := testcontainers.ContainerRequest{
		Image:        image,
		ExposedPorts: []string{httpPort + "/tcp", boltPort + "/tcp"},
		Env:          map[string]string{"JAVA_OPTS": opts},
		WaitingFor: wait.ForHTTP("/api/v1/ready").
			WithPort(httpPort+"/tcp").
			WithStatusCodeMatcher(func(status int) bool { return status == http.StatusNoContent }).
			WithStartupTimeout(150 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		return nil, err
	}
	host, _ := c.Host(ctx)
	hp, _ := c.MappedPort(ctx, httpPort)
	bp, _ := c.MappedPort(ctx, boltPort)
	addTLSCleanup(func() { _ = c.Terminate(ctx) })
	return &arcadeContainer{container: c, host: host, httpPort: hp.Port(), boltPort: bp.Port()}, nil
}

func tlsRequiredContainer(t *testing.T) *arcadeContainer {
	t.Helper()
	tlsReqOnce.Do(func() {
		c, err := startTLSContainer("REQUIRED")
		if err != nil {
			tlsReq, tlsBuildErr = nil, err
			return
		}
		tlsReq = c
	})
	if tlsReq == nil {
		t.Skipf("TLS-required container unavailable: %v", tlsBuildErr)
	}
	return tlsReq
}

func tlsOptionalContainer(t *testing.T) *arcadeContainer {
	t.Helper()
	tlsOptOnce.Do(func() {
		c, err := startTLSContainer("OPTIONAL")
		if err != nil {
			tlsOpt, tlsBuildErr = nil, err
			return
		}
		tlsOpt = c
	})
	if tlsOpt == nil {
		t.Skipf("TLS-optional container unavailable: %v", tlsBuildErr)
	}
	return tlsOpt
}
```

- [ ] **Step 2: Add the TLS tests to `bolt_conformance_test.go`** (in the connection section)

```go
func Test_CONN_002_TLSRequired(t *testing.T) {
	c := tlsRequiredContainer(t)
	d := newDriver(t, boltURI(c, "bolt+ssc"))
	require.NoError(t, d.VerifyConnectivity(ctx))
	rec := runSingle(t, d, "beer", "MATCH (b:Beer) RETURN b.name AS name LIMIT 1", nil)
	name, _ := rec.Get("name")
	require.NotNil(t, name)
}

func Test_CONN_005_TLSOptionalPlaintextConnects(t *testing.T) {
	c := tlsOptionalContainer(t)
	d := newDriver(t, boltURI(c, "bolt"))
	require.NoError(t, d.VerifyConnectivity(ctx))
}
```

- [ ] **Step 3: Run**

Run: `cd e2e-go && go test -run 'Test_CONN_002|Test_CONN_005' -v ./...`
Expected: both PASS (or SKIP with a clear message if `keytool`/`docker` unavailable locally). In CI both PASS.

- [ ] **Step 4: Commit**

```bash
git add e2e-go/tls_test.go e2e-go/bolt_conformance_test.go
git commit -m "feat(#4887): add TLS Bolt conformance scenarios (CONN-002, CONN-005)"
```

---

### Task 3: Auth scenarios (AUTH-001, AUTH-002, AUTH-003)

**Files:**
- Modify: `e2e-go/bolt_conformance_test.go`

**Interfaces:**
- Produces: `neo4jCode(err error) string` helper - extracts the `Neo.*` code from a driver error, or `""`.

- [ ] **Step 1: Add the `neo4jCode` helper and auth tests**

```go
// neo4jCode returns the structured Neo4j error code from a driver error, or ""
// if the error is not a neo4j.Neo4jError.
func neo4jCode(err error) string {
	var ne *neo4j.Neo4jError
	if errors.As(err, &ne) {
		return ne.Code
	}
	return ""
}

// --- auth ----------------------------------------------------------------

func Test_AUTH_001_BasicAuthValid(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	require.NoError(t, d.VerifyConnectivity(ctx))
	rec := runSingle(t, d, "beer", "RETURN 1 AS value", nil)
	v, _ := rec.Get("value")
	require.Equal(t, int64(1), v)
}

func Test_AUTH_002_BasicAuthInvalid(t *testing.T) {
	d, err := neo4j.NewDriverWithContext(boltURI(plainContainer, "bolt"), neo4j.BasicAuth("root", "wrong-password", ""))
	require.NoError(t, err)
	t.Cleanup(func() { _ = d.Close(ctx) })
	err = d.VerifyConnectivity(ctx)
	require.Error(t, err)
	require.Equal(t, "Neo.ClientError.Security.Unauthorized", neo4jCode(err))
}

func Test_AUTH_003_AuthNoneRejected(t *testing.T) {
	d, err := neo4j.NewDriverWithContext(boltURI(plainContainer, "bolt"), neo4j.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { _ = d.Close(ctx) })
	// Intentional rejection (not a bug): connecting with no auth must fail.
	require.Error(t, d.VerifyConnectivity(ctx))
}
```

Add `"errors"` to the import block.

- [ ] **Step 2: Run**

Run: `cd e2e-go && go test -run Test_AUTH -v ./...`
Expected: all three PASS.

- [ ] **Step 3: Commit**

```bash
git add e2e-go/bolt_conformance_test.go
git commit -m "feat(#4887): add auth Bolt conformance scenarios (AUTH-001..003)"
```

---

### Task 4: Transaction scenarios (TX-001..005) + two-writer race helper

**Files:**
- Modify: `e2e-go/bolt_conformance_test.go`

**Interfaces:**
- Produces: `raceTwoWriters(t, d, database, marker) []error` - shared by TX-005 and ERR-004.

- [ ] **Step 1: Add the transaction tests and race helper**

```go
// --- transactions ----------------------------------------------------------

func Test_TX_001_AutocommitQuery(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	sess := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "beer"})
	t.Cleanup(func() { _ = sess.Close(ctx) })
	res, err := sess.Run(ctx, "MATCH (b:Beer) RETURN b.name AS name LIMIT 5", nil)
	require.NoError(t, err)
	recs, err := res.Collect(ctx)
	require.NoError(t, err)
	require.Len(t, recs, 5)
}

func Test_TX_002_ExplicitCommitPersists(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	sess := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "beer"})
	t.Cleanup(func() { _ = sess.Close(ctx) })
	tx, err := sess.BeginTransaction(ctx)
	require.NoError(t, err)
	_, err = tx.Run(ctx, "CREATE (:TxCommitProbe {marker: 'tx-002'})", nil)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	rec := runSingle(t, d, "beer", "MATCH (n:TxCommitProbe {marker: 'tx-002'}) RETURN count(n) AS c", nil)
	c, _ := rec.Get("c")
	require.Equal(t, int64(1), c)
}

func Test_TX_003_ExplicitRollbackDiscards(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	sess := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "beer"})
	t.Cleanup(func() { _ = sess.Close(ctx) })
	tx, err := sess.BeginTransaction(ctx)
	require.NoError(t, err)
	_, err = tx.Run(ctx, "CREATE (:TxRollbackProbe {marker: 'tx-003'})", nil)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback(ctx))

	rec := runSingle(t, d, "beer", "MATCH (n:TxRollbackProbe {marker: 'tx-003'}) RETURN count(n) AS c", nil)
	c, _ := rec.Get("c")
	require.Equal(t, int64(0), c)
}

func Test_TX_004_ManagedWriteCommits(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	sess := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "beer"})
	t.Cleanup(func() { _ = sess.Close(ctx) })
	_, err := sess.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		_, err := tx.Run(ctx, "CREATE (:Beer {name: $n})", map[string]any{"n": "TX-004-Beer"})
		return nil, err
	})
	require.NoError(t, err)
	rec := runSingle(t, d, "beer", "MATCH (b:Beer {name: $n}) RETURN count(b) AS c", map[string]any{"n": "TX-004-Beer"})
	c, _ := rec.Get("c")
	require.Equal(t, int64(1), c)
}

// raceTwoWriters has two sessions race to increment the same node inside an
// explicit transaction, one held open past the other's commit. Returns every
// error the racing sessions surfaced. Shared by TX-005 and ERR-004.
func raceTwoWriters(t *testing.T, d neo4j.DriverWithContext, database, marker string) []error {
	t.Helper()
	setup := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: database})
	_, err := setup.Run(ctx, "MERGE (n:RaceProbe {marker: $marker}) SET n.value = 0", map[string]any{"marker": marker})
	require.NoError(t, err)
	_ = setup.Close(ctx)

	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []error
	barrier := make(chan struct{})
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sess := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: database})
			defer sess.Close(ctx)
			<-barrier
			tx, err := sess.BeginTransaction(ctx)
			if err != nil {
				mu.Lock(); errs = append(errs, err); mu.Unlock(); return
			}
			if res, err := tx.Run(ctx, "MATCH (n:RaceProbe {marker: $marker}) SET n.value = n.value + 1 RETURN n", map[string]any{"marker": marker}); err != nil {
				mu.Lock(); errs = append(errs, err); mu.Unlock(); return
			} else {
				_, _ = res.Consume(ctx)
			}
			time.Sleep(500 * time.Millisecond)
			if err := tx.Commit(ctx); err != nil {
				mu.Lock(); errs = append(errs, err); mu.Unlock()
			}
		}()
	}
	close(barrier)
	wg.Wait()
	for i, e := range errs {
		t.Logf("raceTwoWriters[%s] errs[%d]: code=%q msg=%v", marker, i, neo4jCode(e), e)
	}
	return errs
}

func Test_TX_005_ManagedWriteRetriesOnTransientError(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	errs := raceTwoWriters(t, d, "beer", "tx-005")
	require.NotEmpty(t, errs, "expected at least one racing session to fail on the write conflict")
	require.True(t, anyTransient(errs), "expected at least one Neo.TransientError.*, got %v", codes(errs))
	require.False(t, anyNonRetryable(errs), "expected no non-retryable ClientError/DatabaseError, got %v", codes(errs))
}
```

Add these small classifiers near `neo4jCode`:

```go
func anyTransient(errs []error) bool {
	for _, e := range errs {
		if strings.HasPrefix(neo4jCode(e), "Neo.TransientError") {
			return true
		}
	}
	return false
}

func anyNonRetryable(errs []error) bool {
	for _, e := range errs {
		code := neo4jCode(e)
		if strings.HasPrefix(code, "Neo.ClientError") || strings.HasPrefix(code, "Neo.DatabaseError") {
			return true
		}
	}
	return false
}

func codes(errs []error) []string {
	out := make([]string, 0, len(errs))
	for _, e := range errs {
		out = append(out, neo4jCode(e))
	}
	return out
}
```

Add `"sync"`, `"strings"`, and `"time"` to the imports if not already present.

- [ ] **Step 2: Run**

Run: `cd e2e-go && go test -run Test_TX -v ./...`
Expected: TX-001..005 PASS. (TX-005 depends on the server classifying the lock conflict as `Neo.TransientError.*`, which the Python suite confirms it does.)

- [ ] **Step 3: Commit**

```bash
git add e2e-go/bolt_conformance_test.go
git commit -m "feat(#4887): add transaction Bolt conformance scenarios (TX-001..005)"
```

---

### Task 5: Causal consistency + multi-database (CAUSAL-001, MDB-001, MDB-002)

**Files:**
- Modify: `e2e-go/bolt_conformance_test.go`

- [ ] **Step 1: Add the tests**

```go
// --- causal-consistency ---------------------------------------------------

func Test_CAUSAL_001_BookmarkReadAfterWrite(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	sessA := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "beer"})
	res, err := sessA.Run(ctx, "CREATE (:CausalProbe {marker: 'causal-001'})", nil)
	require.NoError(t, err)
	_, err = res.Consume(ctx)
	require.NoError(t, err)
	bm := sessA.LastBookmarks()
	require.NoError(t, sessA.Close(ctx))

	sessB := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "beer", Bookmarks: bm})
	t.Cleanup(func() { _ = sessB.Close(ctx) })
	res, err = sessB.Run(ctx, "MATCH (n:CausalProbe {marker: 'causal-001'}) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	rec, err := res.Single(ctx)
	require.NoError(t, err)
	c, _ := rec.Get("c")
	require.Equal(t, int64(1), c)
}

// --- multi-database --------------------------------------------------------

func Test_MDB_001_SessionSelectsNamedDatabase(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	rec := runSingle(t, d, "beer", "MATCH (b:Beer) RETURN b.name AS name LIMIT 1", nil)
	name, _ := rec.Get("name")
	require.NotNil(t, name)
}

func Test_MDB_002_SessionsAcrossDatabasesAreIsolated(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	scratch := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "boltscratch"})
	tx, err := scratch.BeginTransaction(ctx)
	require.NoError(t, err)
	_, err = tx.Run(ctx, "CREATE (:ScratchProbe {marker: 'mdb-002'})", nil)
	require.NoError(t, err)

	// beer must not see the uncommitted boltscratch write.
	rec := runSingle(t, d, "beer", "MATCH (n:ScratchProbe {marker: 'mdb-002'}) RETURN count(n) AS c", nil)
	c, _ := rec.Get("c")
	require.Equal(t, int64(0), c)

	require.NoError(t, tx.Commit(ctx))
	require.NoError(t, scratch.Close(ctx))

	verify := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "boltscratch"})
	t.Cleanup(func() { _ = verify.Close(ctx) })
	res, err := verify.Run(ctx, "MATCH (n:ScratchProbe {marker: 'mdb-002'}) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	rec, err = res.Single(ctx)
	require.NoError(t, err)
	c, _ = rec.Get("c")
	require.Equal(t, int64(1), c)
}
```

- [ ] **Step 2: Run**

Run: `cd e2e-go && go test -run 'Test_CAUSAL|Test_MDB' -v ./...`
Expected: all three PASS.

- [ ] **Step 3: Commit**

```bash
git add e2e-go/bolt_conformance_test.go
git commit -m "feat(#4887): add causal-consistency + multi-database scenarios (CAUSAL-001, MDB-001/002)"
```

---

### Task 6: Result-handling (RESULT-001..004)

**Files:**
- Modify: `e2e-go/bolt_conformance_test.go`

- [ ] **Step 1: Add the tests** (RESULT-004 is the first strict-xfail)

```go
// --- result-handling ---------------------------------------------------

func Test_RESULT_001_StreamingPullIncremental(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	sess := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "beer"})
	t.Cleanup(func() { _ = sess.Close(ctx) })
	res, err := sess.Run(ctx, "MATCH (b:Beer) RETURN b.name AS name LIMIT 10", nil)
	require.NoError(t, err)
	seen := 0
	for res.Next(ctx) {
		name, _ := res.Record().Get("name")
		require.NotNil(t, name)
		seen++
	}
	require.NoError(t, res.Err())
	require.Equal(t, 10, seen)
}

func Test_RESULT_002_PartialPullThenContinue(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	sess := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "beer", FetchSize: 2})
	t.Cleanup(func() { _ = sess.Close(ctx) })
	res, err := sess.Run(ctx, "MATCH (b:Beer) RETURN b.name AS name LIMIT 5", nil)
	require.NoError(t, err)
	require.True(t, res.Next(ctx))
	require.True(t, res.Next(ctx)) // consumed first 2
	rest, err := res.Collect(ctx)
	require.NoError(t, err)
	require.Len(t, rest, 3)
}

func Test_RESULT_003_DiscardAbandonsRemaining(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	sess := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "beer"})
	t.Cleanup(func() { _ = sess.Close(ctx) })
	res, err := sess.Run(ctx, "MATCH (b:Beer) RETURN b.name AS name LIMIT 5", nil)
	require.NoError(t, err)
	require.True(t, res.Next(ctx))
	summary, err := res.Consume(ctx)
	require.NoError(t, err)
	require.NotNil(t, summary)
}

func Test_RESULT_004_SummaryCountersReflectWrites(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	assertStillFails(t,
		"BoltNetworkExecutor.handlePull/handleDiscard never populate a 'stats' "+
			"key in SUCCESS metadata for writes, so SummaryCounters is always "+
			"empty; see RESULT-004 in spec.yaml (#4890)",
		func() error {
			sess := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "beer"})
			defer sess.Close(ctx)
			res, err := sess.Run(ctx,
				"CREATE (:Beer {name: $n})-[:BREWED_BY]->(:Brewery {name: $b})",
				map[string]any{"n": "RESULT-004-Beer", "b": "RESULT-004-Brewery"})
			if err != nil {
				return err
			}
			summary, err := res.Consume(ctx)
			if err != nil {
				return err
			}
			cnt := summary.Counters()
			if cnt.NodesCreated() != 2 {
				return fmt.Errorf("nodes_created=%d, want 2", cnt.NodesCreated())
			}
			if cnt.RelationshipsCreated() != 1 {
				return fmt.Errorf("relationships_created=%d, want 1", cnt.RelationshipsCreated())
			}
			if cnt.PropertiesSet() < 2 {
				return fmt.Errorf("properties_set=%d, want >=2", cnt.PropertiesSet())
			}
			return nil
		})
}
```

Add `"fmt"` to the imports if not present.

- [ ] **Step 2: Run**

Run: `cd e2e-go && go test -run Test_RESULT -v ./...`
Expected: RESULT-001..003 PASS; RESULT-004 PASS (the gap reproduces: counters are empty, so `body` returns an error and `assertStillFails` treats it as expected). The `t.Logf` shows the reproduced gap.

- [ ] **Step 3: Commit**

```bash
git add e2e-go/bolt_conformance_test.go
git commit -m "feat(#4887): add result-handling scenarios (RESULT-001..004)"
```

---

### Task 7: Type round-trip (TYPE-001..012)

**Files:**
- Modify: `e2e-go/bolt_conformance_test.go`

**Notes for the implementer:** the neo4j-go-driver maps Cypher types to `github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype`. Confirm the exact concrete type each scenario returns by running the test and reading the failure/log (TDD) - the assertions below encode the intended target type; adjust the concrete `dbtype.X` if the driver differs, but do NOT weaken an `expected-fail` scenario into a skip. Import `"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"` and `"time"`.

- [ ] **Step 1: Add the type tests**

```go
// --- type-roundtrip ---------------------------------------------------

func Test_TYPE_001_NodeRoundtrip(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	rec := runSingle(t, d, "beer", "MATCH (b:Beer) RETURN b LIMIT 1", nil)
	v, _ := rec.Get("b")
	node, ok := v.(dbtype.Node)
	require.True(t, ok, "expected dbtype.Node, got %T", v)
	require.Contains(t, node.Labels, "Beer")
	require.NotNil(t, node.Props["name"])
}

func Test_TYPE_002_RelationshipRoundtrip(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	rec := runSingle(t, d, "beer", "MATCH ()-[r]->() RETURN r LIMIT 1", nil)
	v, _ := rec.Get("r")
	rel, ok := v.(dbtype.Relationship)
	require.True(t, ok, "expected dbtype.Relationship, got %T", v)
	require.NotEmpty(t, rel.Type)
}

func Test_TYPE_003_PathRoundtrip(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	assertStillFails(t,
		"structure/BoltPath.java has zero call sites in BoltStructureMapper; "+
			"query results never produce native Path structures; see #4890",
		func() error {
			sess := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "beer"})
			defer sess.Close(ctx)
			res, err := sess.Run(ctx, "MATCH p=(b:Beer)-[*1..2]-() RETURN p LIMIT 1", nil)
			if err != nil {
				return err
			}
			rec, err := res.Single(ctx)
			if err != nil {
				return err
			}
			v, _ := rec.Get("p")
			path, ok := v.(dbtype.Path)
			if !ok {
				return fmt.Errorf("expected dbtype.Path, got %T", v)
			}
			if len(path.Nodes) < 2 {
				return fmt.Errorf("path has %d nodes, want >=2", len(path.Nodes))
			}
			return nil
		})
}

func Test_TYPE_004_ByteArrayParamRoundtrip(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	payload := []byte{1, 2, 3, 4}
	rec := runSingle(t, d, "beer", "RETURN $b AS echo", map[string]any{"b": payload})
	v, _ := rec.Get("echo")
	got, ok := v.([]byte)
	require.True(t, ok, "expected []byte, got %T", v)
	require.Equal(t, payload, got)
}

func Test_TYPE_005_NestedListMapRoundtrip(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	rec := runSingle(t, d, "beer", "MATCH (t:TypeMatrix) RETURN t.nestedListProp AS l, t.nestedMapProp AS m", nil)
	l, _ := rec.Get("l")
	m, _ := rec.Get("m")
	require.Equal(t, []any{int64(1), int64(2), []any{int64(3), int64(4)}}, l)
	require.Equal(t, map[string]any{"a": int64(1), "b": map[string]any{"c": int64(2)}}, m)
}

func Test_TYPE_006_NullRoundtrip(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	rec := runSingle(t, d, "beer", "MATCH (t:TypeMatrix) RETURN t.nullProp AS n", nil)
	n, _ := rec.Get("n")
	require.Nil(t, n)
	echo := runSingle(t, d, "beer", "RETURN $p AS echo", map[string]any{"p": nil})
	e, _ := echo.Get("echo")
	require.Nil(t, e)
}

func Test_TYPE_007_LocalDateRoundtrip(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	rec := runSingle(t, d, "beer", "MATCH (t:TypeMatrix) RETURN t.localDateProp AS d", nil)
	v, _ := rec.Get("d")
	_, ok := v.(dbtype.Date)
	require.True(t, ok, "expected dbtype.Date, got %T", v)
	echo := runSingle(t, d, "beer", "RETURN $d AS echo", map[string]any{"d": v})
	e, _ := echo.Get("echo")
	require.Equal(t, v, e)
}

func Test_TYPE_008_LocalTimeRoundtrip(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	rec := runSingle(t, d, "beer", "MATCH (t:TypeMatrix) RETURN t.localTimeProp AS t2", nil)
	v, _ := rec.Get("t2")
	_, ok := v.(dbtype.LocalTime)
	require.True(t, ok, "expected dbtype.LocalTime, got %T", v)
	echo := runSingle(t, d, "beer", "RETURN $t AS echo", map[string]any{"t": v})
	e, _ := echo.Get("echo")
	require.Equal(t, v, e)
}

func Test_TYPE_009_LocalDateTimeRoundtrip(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	rec := runSingle(t, d, "beer", "MATCH (t:TypeMatrix) RETURN t.localDateTimeProp AS dt", nil)
	v, _ := rec.Get("dt")
	_, ok := v.(dbtype.LocalDateTime)
	require.True(t, ok, "expected dbtype.LocalDateTime, got %T", v)
	echo := runSingle(t, d, "beer", "RETURN $dt AS echo", map[string]any{"dt": v})
	e, _ := echo.Get("echo")
	require.Equal(t, v, e)
}

func Test_TYPE_010_OffsetDateTimeRoundtrip(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	rec := runSingle(t, d, "beer", "MATCH (t:TypeMatrix) RETURN t.offsetDateTimeProp AS dt", nil)
	v, _ := rec.Get("dt")
	tm, ok := v.(time.Time)
	require.True(t, ok, "expected time.Time, got %T", v)
	_, offset := tm.Zone()
	require.Equal(t, 2*60*60, offset, "expected +02:00 offset")
	echo := runSingle(t, d, "beer", "RETURN $dt AS echo", map[string]any{"dt": tm})
	e, _ := echo.Get("echo")
	require.WithinDuration(t, tm, e.(time.Time), 0)
}

func Test_TYPE_011_DurationRoundtrip(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	assertStillFails(t,
		"BoltStructureMapper/PackStreamWriter have no Duration handling; falls "+
			"through to value.toString(); see #4890",
		func() error {
			rec := runSingleErr(d, "beer", "MATCH (t:TypeMatrix) RETURN t.durationProp AS d", nil)
			v, _ := rec.Get("d")
			if _, ok := v.(dbtype.Duration); !ok {
				return fmt.Errorf("expected dbtype.Duration, got %T", v)
			}
			return nil
		})
}

func Test_TYPE_012_PointRoundtrip(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	assertStillFails(t,
		"No Point/spatial handling in BoltStructureMapper or PackStreamWriter "+
			"(engine point() works; the gap is Bolt wire serialization); see #4890",
		func() error {
			rec := runSingleErr(d, "beer", "MATCH (t:TypeMatrix) RETURN t.pointProp AS p", nil)
			v, _ := rec.Get("p")
			if _, ok := v.(dbtype.Point2D); !ok {
				return fmt.Errorf("expected dbtype.Point2D, got %T", v)
			}
			return nil
		})
}
```

Add a non-fatal single-record helper for the gap bodies (they must return errors, not call `t.Fatal`):

```go
// runSingleErr is the error-returning sibling of runSingle for use inside
// assertStillFails bodies, which must surface failures as errors.
func runSingleErr(d neo4j.DriverWithContext, database, cypher string, params map[string]any) *neo4j.Record {
	sess := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: database})
	defer sess.Close(ctx)
	res, err := sess.Run(ctx, cypher, params)
	if err != nil {
		return &neo4j.Record{}
	}
	rec, err := res.Single(ctx)
	if err != nil {
		return &neo4j.Record{}
	}
	return rec
}
```

> Implementer note: if `runSingleErr` masking an error makes a gap body return `nil` incorrectly, prefer inlining the session/run/single calls in that body and returning the real error (as RESULT-004 and TYPE-003 do). The helper is a convenience only for the type-tag gap checks where the query itself succeeds and only the returned type is wrong.

- [ ] **Step 2: Run and adjust concrete types via TDD**

Run: `cd e2e-go && go test -run Test_TYPE -v ./...`
Expected: TYPE-001,002,004,005,006,007,008,009,010 PASS; TYPE-003,011,012 PASS as reproduced gaps (bodies error). If a passing scenario fails only because the concrete `dbtype.X`/int width differs from the assertion, correct the assertion to the real driver type and re-run. Never convert a passing scenario to skip.

- [ ] **Step 3: Commit**

```bash
git add e2e-go/bolt_conformance_test.go
git commit -m "feat(#4887): add type round-trip scenarios (TYPE-001..012)"
```

---

### Task 8: Error scenarios (ERR-001..004)

**Files:**
- Modify: `e2e-go/bolt_conformance_test.go`

- [ ] **Step 1: Add the tests**

```go
// --- errors ---------------------------------------------------------------

func Test_ERR_001_SyntaxError(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	sess := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "beer"})
	t.Cleanup(func() { _ = sess.Close(ctx) })
	res, err := sess.Run(ctx, "MATCH (n RETURN n", nil)
	if err == nil {
		_, err = res.Consume(ctx)
	}
	require.Error(t, err)
	require.Equal(t, "Neo.ClientError.Statement.SyntaxError", neo4jCode(err))
}

func Test_ERR_002_SemanticError(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	assertStillFails(t,
		"CypherSemanticValidator throws undefined-variable via "+
			"CommandParsingException, the same class as ANTLR syntax errors, so "+
			"the Bolt RUN handler cannot distinguish them; "+
			"Neo.ClientError.Statement.SemanticError is dead code in Bolt; see #4890",
		func() error {
			sess := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "beer"})
			defer sess.Close(ctx)
			res, err := sess.Run(ctx, "MATCH (n:Beer) RETURN undeclaredVariable", nil)
			if err == nil {
				_, err = res.Consume(ctx)
			}
			if err == nil {
				return fmt.Errorf("expected an error, got none")
			}
			if code := neo4jCode(err); code != "Neo.ClientError.Statement.SemanticError" {
				return fmt.Errorf("got code %q, want Neo.ClientError.Statement.SemanticError", code)
			}
			return nil
		})
}

func Test_ERR_003_UnauthenticatedRequestRejected(t *testing.T) {
	t.Skip("Requires sending RUN before completing HELLO/LOGON; the official " +
		"neo4j-go-driver always completes the handshake internally, so this " +
		"cannot be triggered without a bespoke raw-socket client, which is out " +
		"of scope per the epic's 'official drivers only' principle. spec.yaml " +
		"ERR-003 current_status is not-applicable.")
}

func Test_ERR_004_TransientConditionErrorCode(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	errs := raceTwoWriters(t, d, "beer", "err-004")
	require.NotEmpty(t, errs, "expected at least one racing session to fail on the write conflict")
	require.True(t, anyTransient(errs), "expected at least one Neo.TransientError.*, got %v", codes(errs))
	require.False(t, anyNonRetryable(errs), "expected no non-retryable ClientError/DatabaseError, got %v", codes(errs))
}
```

- [ ] **Step 2: Run**

Run: `cd e2e-go && go test -run Test_ERR -v ./...`
Expected: ERR-001 PASS; ERR-002 PASS as reproduced gap; ERR-003 SKIP; ERR-004 PASS.

- [ ] **Step 3: Commit**

```bash
git add e2e-go/bolt_conformance_test.go
git commit -m "feat(#4887): add error scenarios (ERR-001..004)"
```

---

### Task 9: Protocol scenarios (PROTO-001..003)

**Files:**
- Modify: `e2e-go/bolt_conformance_test.go`

- [ ] **Step 1: Add the tests**

```go
// --- protocol ---------------------------------------------------------

func Test_PROTO_001_VersionNegotiationSucceeds(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	rec := runSingle(t, d, "beer", "RETURN 1 AS value", nil)
	v, _ := rec.Get("value")
	require.Equal(t, int64(1), v)
}

func Test_PROTO_002_Bolt5xNegotiationIsDocumented(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	assertStillFails(t,
		"BoltNetworkExecutor.SUPPORTED_VERSIONS never advertises Bolt 5.x; "+
			"drivers only work by silently downgrading to 4.4; see #4890",
		func() error {
			sess := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "beer"})
			defer sess.Close(ctx)
			res, err := sess.Run(ctx, "RETURN 1 AS value", nil)
			if err != nil {
				return err
			}
			summary, err := res.Consume(ctx)
			if err != nil {
				return err
			}
			ver := summary.Server().ProtocolVersion()
			if ver.Major >= 5 {
				return nil // gap fixed: 5.x negotiated
			}
			return fmt.Errorf("driver negotiated Bolt %d.%d, not 5.x", ver.Major, ver.Minor)
		})
}

func Test_PROTO_003_ResetMidStream(t *testing.T) {
	d := newDriver(t, boltURI(plainContainer, "bolt"))
	sess := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "beer", FetchSize: 2})
	t.Cleanup(func() { _ = sess.Close(ctx) })
	res, err := sess.Run(ctx, "MATCH (b:Beer) RETURN b.name AS name LIMIT 10", nil)
	require.NoError(t, err)
	require.True(t, res.Next(ctx))
	_, err = res.Consume(ctx) // abandon remaining stream mid-flight
	require.NoError(t, err)

	res2, err := sess.Run(ctx, "RETURN 1 AS value", nil)
	require.NoError(t, err)
	rec, err := res2.Single(ctx)
	require.NoError(t, err)
	v, _ := rec.Get("value")
	require.Equal(t, int64(1), v)
}
```

> Implementer note: confirm the `ResultSummary.Server().ProtocolVersion()` accessor shape against the pinned driver (v5's `neo4j.ProtocolVersion` has `Major`/`Minor` fields). If the accessor differs, adjust; the intent is "read the negotiated Bolt version".

- [ ] **Step 2: Run the full suite**

Run: `cd e2e-go && go test -v ./...`
Expected: 39 tests total - passing scenarios green, the 6 strict-xfail scenarios green-as-reproduced-gaps (RESULT-004, TYPE-003/011/012, ERR-002, PROTO-002), CONN-004 + ERR-003 skipped.

- [ ] **Step 3: Commit**

```bash
git add e2e-go/bolt_conformance_test.go
git commit -m "feat(#4887): add protocol scenarios (PROTO-001..003)"
```

---

### Task 10: README + CI job

**Files:**
- Create: `e2e-go/README.md`
- Modify: `.github/workflows/mvn-test.yml`

- [ ] **Step 1: Create `e2e-go/README.md`**

Content must cover: purpose (Bolt conformance for `neo4j-go-driver`, part of #4882), the driver-version bands (`lts`/`current`/`latest` mapped to `neo4j-go-driver/v5`, with the single pinned version used for PR-gating and a note that the multi-version nightly matrix is #4891), how to run locally (`go test -v ./...`, plus the note that a local `arcadedata/arcadedb:latest` image or `ARCADEDB_DOCKER_IMAGE` is required, and that TLS scenarios need `keytool` + `docker`), and the traceability convention (`Test_<AREA>_<NNN>_<slug>` maps 1:1 to `bolt/conformance/spec.yaml` scenario ids). Mirror the tone of `e2e-python/README.md` (read it first).

- [ ] **Step 2: Add the `go-e2e-tests` job to `.github/workflows/mvn-test.yml`**

Insert after the `csharp-e2e-tests` job (copy its structure; pin the `setup-go` action SHA the same way the repo pins other actions - check an existing `setup-go` usage in the repo, or use the current `actions/setup-go@v5` pinned by SHA):

```yaml
  go-e2e-tests:
    runs-on: ubuntu-latest
    needs: build-and-package
    steps:
      - uses: actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0 # v7.0.0

      - name: Set up Go
        uses: actions/setup-go@d35c59abb061a4a6fb18e82ac0862c26744d6ab5 # v5.5.0
        with:
          go-version: "1.23"
          cache-dependency-path: "e2e-go/go.sum"

      - name: Restore Docker image
        uses: actions/cache/restore@55cc8345863c7cc4c66a329aec7e433d2d1c52a9 # v6.1.0
        with:
          path: /tmp/arcadedb-image.tar
          key: docker-image-${{ github.run_id }}-${{ github.run_attempt }}

      - name: Load Docker image
        run: docker load < /tmp/arcadedb-image.tar

      - name: E2E Go Tests
        working-directory: e2e-go
        run: go test -v ./...
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
          ARCADEDB_DOCKER_IMAGE: ${{ needs.build-and-package.outputs.image-tag }}
```

> Verify the `actions/setup-go` SHA against a current release before committing (the SHA above is illustrative). If any other workflow in the repo already uses `setup-go`, copy that exact pinned SHA for consistency.

- [ ] **Step 3: Validate the workflow YAML**

Run: `cd <repo-root> && python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/mvn-test.yml')); print('yaml ok')"`
Expected: `yaml ok`.

- [ ] **Step 4: Commit**

```bash
git add e2e-go/README.md .github/workflows/mvn-test.yml
git commit -m "feat(#4887): add e2e-go README and go-e2e-tests CI job"
```

---

### Task 11: Full-suite verification against the branch image

**Files:** none (verification only)

- [ ] **Step 1: Build the branch Docker image**

Run (from repo root): `./mvnw install -Pdocker -DskipTests -pl bolt,package -am -q`
Expected: builds `arcadedata/arcadedb:latest` from this branch.

- [ ] **Step 2: Run the entire suite**

Run: `cd e2e-go && go test -v ./... 2>&1 | tee /tmp/e2e-go-run.log`
Expected: exit code 0. Confirm in the log:
- 31 scenarios PASS as normal green tests.
- 6 scenarios PASS with a `known gap still reproduces (expected)` log line (RESULT-004, TYPE-003, TYPE-011, TYPE-012, ERR-002, PROTO-002).
- 2 scenarios SKIP (CONN-004, ERR-003).
- No `XPASS:` failures (would mean a gap was silently fixed - if so, update `spec.yaml` and convert that test).

- [ ] **Step 3: gofmt / vet**

Run: `cd e2e-go && gofmt -l . && go vet ./...`
Expected: `gofmt -l` prints nothing (all files formatted); `go vet` clean.

- [ ] **Step 4: Final commit if gofmt/vet required changes**

```bash
git add -A && git commit -m "chore(#4887): gofmt and go vet cleanups"
```

(Skip if nothing changed.)

---

## Self-Review notes

- **Spec coverage:** All 39 `spec.yaml` scenarios have a task (Tasks 1-9 cover the 9 areas; the mapping table in the design doc is 1:1). Module scaffold, TLS, README, and CI job are Tasks 1/2/10. `ARCADEDB_DOCKER_IMAGE` honored (Task 1). Strict-xfail helper (Task 1). Driver-band documentation (Task 10 README). Multi-version matrix / badge / protocol fixes correctly deferred to #4891/#4892/#4890.
- **Type consistency:** `assertStillFails`, `boltURI`, `runSingle`, `runSingleErr`, `neo4jCode`, `raceTwoWriters`, `anyTransient`/`anyNonRetryable`/`codes`, `arcadeContainer`, `plainContainer`, `tlsRequiredContainer`/`tlsOptionalContainer`/`tlsTeardown` are defined once (Tasks 1/2/3/4) and referenced consistently thereafter.
- **Known driver-API risk:** the exact `dbtype` concrete types (Task 7) and `ResultSummary.Server().ProtocolVersion()` shape (Task 9) are confirmed empirically during TDD; the plan flags this and forbids weakening a passing/expected-fail scenario to a skip to make it compile.
```
