# e2e-csharp Bolt Conformance Suite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `Neo4j.Driver`-based Bolt conformance test suite to `e2e-csharp`, implementing every scenario in `bolt/conformance/spec.yaml` (issue #4886, epic #4882 Group B), matching the coverage and conventions of the already-merged `e2e-python` suite (#4885, PR #4920).

**Architecture:** One default xUnit collection fixture (`ArcadeDbBoltFixture`) spins a single ArcadeDB container with `BoltProtocolPlugin` enabled and the `beer` + `type_matrix` fixtures seeded via HTTP; it backs `BoltE2ETests.cs`, which covers 37 of the 39 spec.yaml scenarios. Two more fixtures spin dedicated containers built from a throwaway Docker image (self-signed keystore/truststore baked in via `COPY`) for the two TLS-mode scenarios, covered by `BoltTlsE2ETests.cs`. Non-automatable scenarios use `[Fact(Skip = ...)]`; real, currently-failing product gaps use a new `KnownGapAssertions.AssertStillFailsAsync` helper (xUnit's equivalent of pytest's `xfail(strict=True)`) so the build breaks the moment a gap is actually fixed.

**Tech Stack:** .NET 10, xUnit 2.9.3, Testcontainers 4.11.0 (incl. `ImageFromDockerfileBuilder`), Neo4j.Driver 6.2.1 (NuGet).

## Global Constraints

- Design doc: `docs/superpowers/specs/2026-07-04-e2e-csharp-bolt-conformance-design.md` - read it for full rationale.
- Target `bolt/conformance/spec.yaml` **as it exists on `main` right now** (post #4885), not the version originally authored in #4883 - several `expected-fail` scenarios there have already flipped to `passing` upstream, and two new gaps (`ERR-002`, `RESULT-004`) were added. Do not "fix" a test to match the stale pre-#4885 spec.yaml.
- Every test's `[Fact(DisplayName = ...)]` must embed its spec.yaml scenario id (e.g. `"TYPE-011: ..."`), per `bolt/conformance/README.md`'s traceability convention.
- No new CI workflow changes - `csharp-e2e-tests` in `.github/workflows/mvn-test.yml` already runs `dotnet test` against this project.
- Root password for every ArcadeDB container in this suite: `playwithdata`. Root user: `root`.
- Do not commit on behalf of the user beyond what this plan's steps direct - each task ends with its own commit, per repo convention of frequent, focused commits.
- `CLAUDE.md`: use `final` where reasonable, avoid unnecessary abstractions, no stray `System.out`/`Console.WriteLine` debug output left behind, don't add code comments explaining *what* code does (only non-obvious *why*).

---

## File Structure

- **Modify:** `e2e-csharp/ArcadeDB.E2ETests/ArcadeDB.E2ETests.csproj` - add `Neo4j.Driver` package reference.
- **Create:** `e2e-csharp/ArcadeDB.E2ETests/BoltFixtures.cs` - `ArcadeDbBoltFixture` (default, no-TLS) + `ArcadeDbBoltTlsRequiredFixture` + `ArcadeDbBoltTlsOptionalFixture` + their collection definitions + a shared `BoltTlsCertificates` helper for the throwaway TLS image.
- **Create:** `e2e-csharp/ArcadeDB.E2ETests/KnownGapAssertions.cs` - the `AssertStillFailsAsync` helper.
- **Create:** `e2e-csharp/ArcadeDB.E2ETests/BoltE2ETests.cs` - all 27 non-TLS, non-HA scenarios (connection, auth, transactions, causal-consistency, multi-database, result-handling, type-roundtrip, errors, protocol).
- **Create:** `e2e-csharp/ArcadeDB.E2ETests/BoltTlsE2ETests.cs` - `CONN-002` and `CONN-005`.

No existing file other than the `.csproj` is modified; `PostgresE2ETests.cs` and `ArcadeDbFixture.cs` are untouched.

---

## Task 1: Add the `Neo4j.Driver` package reference

**Files:**
- Modify: `e2e-csharp/ArcadeDB.E2ETests/ArcadeDB.E2ETests.csproj`

**Interfaces:**
- Produces: `Neo4j.Driver` NuGet package (v6.2.1) available to every subsequent task.

- [ ] **Step 1: Add the package reference**

In `e2e-csharp/ArcadeDB.E2ETests/ArcadeDB.E2ETests.csproj`, inside the existing `<ItemGroup>` that lists `Testcontainers`/`Npgsql`, add:

```xml
    <PackageReference Include="Neo4j.Driver" Version="6.2.1" />
```

- [ ] **Step 2: Restore and build**

Run: `cd e2e-csharp/ArcadeDB.E2ETests && dotnet build`
Expected: `Build succeeded. 0 Warning(s) 0 Error(s)`

- [ ] **Step 3: Commit**

```bash
git add e2e-csharp/ArcadeDB.E2ETests/ArcadeDB.E2ETests.csproj
git commit -m "$(cat <<'EOF'
build(#4886): add Neo4j.Driver dependency for the e2e-csharp Bolt suite

Pinned to 6.2.1 (latest stable), matching the Java suite's 6.2.0 and
Python's neo4j>=6.2.0 baseline per bolt/conformance/spec.yaml's
driver_version_bands.
EOF
)"
```

---

## Task 2: `ArcadeDbBoltFixture` (default, no-TLS container)

**Files:**
- Create: `e2e-csharp/ArcadeDB.E2ETests/BoltFixtures.cs`

**Interfaces:**
- Consumes: nothing from earlier tasks besides the `Neo4j.Driver`/`Testcontainers` packages.
- Produces: `ArcadeDbBoltFixture` with public members `IDriver Driver`, `string BoltUri`, `string NeoRoutingUri`, `const string RootUser = "root"`, `const string RootPassword = "playwithdata"`, and collection name `"ArcadeDB-Bolt"` (via `[CollectionDefinition("ArcadeDB-Bolt")] class ArcadeDbBoltCollection : ICollectionFixture<ArcadeDbBoltFixture>`). Seeds `beer` (via container boot import), `boltscratch` (via HTTP), and `bolt/conformance/fixtures/type-matrix.cypher` into `beer` (via HTTP `/api/v1/command/beer`, language `cypher`) before the fixture is handed to any test.

- [ ] **Step 1: Write the fixture**

Create `e2e-csharp/ArcadeDB.E2ETests/BoltFixtures.cs`:

```csharp
/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System.Diagnostics;
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Images;
using Neo4j.Driver;
using Xunit;

namespace ArcadeDB.E2ETests;

[CollectionDefinition("ArcadeDB-Bolt")]
public class ArcadeDbBoltCollection : ICollectionFixture<ArcadeDbBoltFixture> { }

public class ArcadeDbBoltFixture : IAsyncLifetime
{
    public const string RootUser = "root";
    public const string RootPassword = "playwithdata";

    private const string BeerDatabase = "beer";
    private const string ScratchDatabase = "boltscratch";

    private IContainer _container = null!;
    private string _host = null!;
    private int _httpPort;

    public IDriver Driver { get; private set; } = null!;
    public string BoltUri { get; private set; } = null!;
    public string NeoRoutingUri { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        var imageEnv = Environment.GetEnvironmentVariable("ARCADEDB_DOCKER_IMAGE");
        var image = string.IsNullOrWhiteSpace(imageEnv) ? "arcadedata/arcadedb:latest" : imageEnv;

        _container = new ContainerBuilder(image)
            .WithPortBinding(2480, true)
            .WithPortBinding(7687, true)
            .WithEnvironment("JAVA_OPTS",
                $"-Darcadedb.server.rootPassword={RootPassword} " +
                $"-Darcadedb.server.defaultDatabases={BeerDatabase}[root]{{import:https://github.com/ArcadeData/arcadedb-datasets/raw/main/orientdb/OpenBeer.gz}} " +
                "-Darcadedb.server.plugins=BoltProtocolPlugin")
            .WithWaitStrategy(Wait.ForUnixContainer()
                .UntilHttpRequestIsSucceeded(r => r
                    .ForPort(2480)
                    .ForPath("/api/v1/ready")
                    .ForStatusCode(HttpStatusCode.NoContent)))
            .Build();

        await _container.StartAsync();

        _host = _container.Hostname;
        _httpPort = _container.GetMappedPublicPort(2480);
        var boltPort = _container.GetMappedPublicPort(7687);

        using (var http = CreateHttpClient())
        {
            await CreateDatabaseAsync(http, ScratchDatabase);
            await SeedTypeMatrixAsync(http);
        }

        BoltUri = $"bolt://{_host}:{boltPort}";
        NeoRoutingUri = $"neo4j://{_host}:{boltPort}";
        Driver = GraphDatabase.Driver(BoltUri, AuthTokens.Basic(RootUser, RootPassword));
    }

    public async Task DisposeAsync()
    {
        if (Driver is not null)
            await Driver.DisposeAsync();
        if (_container is not null)
            await _container.DisposeAsync();
    }

    private HttpClient CreateHttpClient()
    {
        var http = new HttpClient { BaseAddress = new Uri($"http://{_host}:{_httpPort}") };
        http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue(
            "Basic", Convert.ToBase64String(Encoding.UTF8.GetBytes($"{RootUser}:{RootPassword}")));
        return http;
    }

    private static async Task CreateDatabaseAsync(HttpClient http, string dbName)
    {
        var payload = JsonSerializer.Serialize(new { command = $"create database {dbName}" });
        using var response = await http.PostAsync("/api/v1/server", new StringContent(payload, Encoding.UTF8, "application/json"));
        response.EnsureSuccessStatusCode();
    }

    private static async Task SeedTypeMatrixAsync(HttpClient http)
    {
        var fixturePath = Path.Combine(FindRepoRoot(), "bolt", "conformance", "fixtures", "type-matrix.cypher");
        var command = await File.ReadAllTextAsync(fixturePath);

        var payload = JsonSerializer.Serialize(new { language = "cypher", command });
        using var response = await http.PostAsync("/api/v1/command/beer", new StringContent(payload, Encoding.UTF8, "application/json"));
        response.EnsureSuccessStatusCode();
    }

    internal static string FindRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "pom.xml")))
            dir = dir.Parent;
        if (dir is null)
            throw new InvalidOperationException("Could not locate repository root (no pom.xml found) from " + AppContext.BaseDirectory);
        return dir.FullName;
    }
}
```

- [ ] **Step 2: Add a smoke test to prove the fixture works end-to-end**

Create `e2e-csharp/ArcadeDB.E2ETests/BoltE2ETests.cs`:

```csharp
/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Bolt protocol conformance suite for issue #4886.
 * Implements every non-TLS, non-HA scenario in bolt/conformance/spec.yaml
 * (issue #4883) against the official Neo4j.Driver NuGet package. Every
 * [Fact(DisplayName = ...)] embeds its spec.yaml scenario id per
 * bolt/conformance/README.md's traceability convention.
 */

using Neo4j.Driver;
using Xunit;

namespace ArcadeDB.E2ETests;

[Collection("ArcadeDB-Bolt")]
public class BoltE2ETests
{
    private readonly ArcadeDbBoltFixture _fixture;

    public BoltE2ETests(ArcadeDbBoltFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact(DisplayName = "CONN-001: Connect via bolt:// scheme")]
    public async Task Conn001_ConnectBolt()
    {
        await _fixture.Driver.VerifyConnectivityAsync();
    }
}
```

- [ ] **Step 3: Run it**

Run: `cd e2e-csharp/ArcadeDB.E2ETests && dotnet test --filter "FullyQualifiedName~Conn001_ConnectBolt"`
Expected: `Passed! - Failed: 0, Passed: 1, Skipped: 0`

If it fails to pull the ArcadeDB image or the container doesn't reach `ready`, check `docker images` for a local `arcadedata/arcadedb:latest` (or set `ARCADEDB_DOCKER_IMAGE` to a tag you have built locally via `mvn -Pdocker` at the repo root).

- [ ] **Step 4: Commit**

```bash
git add e2e-csharp/ArcadeDB.E2ETests/BoltFixtures.cs e2e-csharp/ArcadeDB.E2ETests/BoltE2ETests.cs
git commit -m "$(cat <<'EOF'
feat(#4886): add default Bolt fixture and CONN-001 smoke test

ArcadeDbBoltFixture boots one ArcadeDB container with BoltProtocolPlugin,
imports beer at container start, and seeds boltscratch + the
type-matrix.cypher fixture via HTTP - never over Bolt itself, per
bolt/conformance/spec.yaml's fixture note on avoiding the very
serialization path several type-roundtrip scenarios test.
EOF
)"
```

---

## Task 3: Connection scenarios (`CONN-003`, `CONN-004`)

**Files:**
- Modify: `e2e-csharp/ArcadeDB.E2ETests/BoltE2ETests.cs`

**Interfaces:**
- Consumes: `ArcadeDbBoltFixture.Driver`, `.NeoRoutingUri`, `ArcadeDbBoltFixture.RootUser`/`.RootPassword` (Task 2).

- [ ] **Step 1: Add the two tests**

Append to the `BoltE2ETests` class body in `BoltE2ETests.cs` (after `Conn001_ConnectBolt`):

```csharp
    [Fact(DisplayName = "CONN-003: Connect via neo4j:// routing discovery, single-node deployment")]
    public async Task Conn003_NeoRoutingSingleNode()
    {
        await using var driver = GraphDatabase.Driver(
            _fixture.NeoRoutingUri,
            AuthTokens.Basic(ArcadeDbBoltFixture.RootUser, ArcadeDbBoltFixture.RootPassword));
        await driver.VerifyConnectivityAsync();

        await using var session = driver.AsyncSession(o => o.WithDatabase("beer"));
        var result = await session.RunAsync("MATCH (b:Beer) RETURN b.name AS name LIMIT 1");
        var record = await result.SingleAsync();
        Assert.NotNull(record["name"].As<string>());
    }

    [Fact(Skip = "Requires a 3-node HA cluster; e2e-csharp's single-node harness cannot exercise this without new multi-node orchestration infrastructure - see #4890")]
    public void Conn004_NeoRoutingHaTopology()
    {
    }
```

- [ ] **Step 2: Run**

Run: `dotnet test --filter "FullyQualifiedName~Conn003_NeoRoutingSingleNode|FullyQualifiedName~Conn004_NeoRoutingHaTopology"`
Expected: `Passed! - Failed: 0, Passed: 1, Skipped: 1`

- [ ] **Step 3: Commit**

```bash
git add e2e-csharp/ArcadeDB.E2ETests/BoltE2ETests.cs
git commit -m "test(#4886): add CONN-003/CONN-004 connection scenarios"
```

---

## Task 4: Auth scenarios (`AUTH-001..003`)

**Files:**
- Modify: `e2e-csharp/ArcadeDB.E2ETests/BoltE2ETests.cs`

**Interfaces:**
- Consumes: `ArcadeDbBoltFixture.Driver`, `.BoltUri`, `.RootUser`/`.RootPassword`.
- Produces: nothing new consumed by later tasks.

- [ ] **Step 1: Add the tests**

Append to `BoltE2ETests`:

```csharp
    [Fact(DisplayName = "AUTH-001: Basic auth succeeds with valid credentials")]
    public async Task Auth001_BasicAuthValid()
    {
        await using var session = _fixture.Driver.AsyncSession(o => o.WithDatabase("beer"));
        var result = await session.RunAsync("RETURN 1 AS value");
        var record = await result.SingleAsync();
        Assert.Equal(1L, record["value"].As<long>());
    }

    [Fact(DisplayName = "AUTH-002: Basic auth fails with invalid credentials")]
    public async Task Auth002_BasicAuthInvalid()
    {
        await using var driver = GraphDatabase.Driver(
            _fixture.BoltUri,
            AuthTokens.Basic(ArcadeDbBoltFixture.RootUser, "wrong-password"));

        var ex = await Assert.ThrowsAsync<SecurityException>(() => driver.VerifyConnectivityAsync());
        Assert.Equal("Neo.ClientError.Security.Unauthorized", ex.Code);
    }

    [Fact(DisplayName = "AUTH-003: Auth scheme 'none' is rejected (intentional, not a bug)")]
    public async Task Auth003_AuthNoneRejected()
    {
        await using var driver = GraphDatabase.Driver(_fixture.BoltUri, AuthTokens.None);

        await Assert.ThrowsAnyAsync<Neo4jException>(() => driver.VerifyConnectivityAsync());
    }
```

- [ ] **Step 2: Run**

Run: `dotnet test --filter "FullyQualifiedName~Auth00"`
Expected: `Passed! - Failed: 0, Passed: 3, Skipped: 0`

- [ ] **Step 3: Commit**

```bash
git add e2e-csharp/ArcadeDB.E2ETests/BoltE2ETests.cs
git commit -m "test(#4886): add AUTH-001..003 auth scenarios"
```

---

## Task 5: Transaction scenarios + shared race helper (`TX-001..005`)

**Files:**
- Modify: `e2e-csharp/ArcadeDB.E2ETests/BoltE2ETests.cs`

**Interfaces:**
- Produces: `private static Task<List<Exception>> RaceTwoWritersAsync(IDriver driver, string database, string marker)`, reused by `ERR-004` in Task 9.

- [ ] **Step 1: Add the `using` directives needed for the race helper**

At the top of `BoltE2ETests.cs`, change:

```csharp
using Neo4j.Driver;
using Xunit;
```

to:

```csharp
using System.Collections.Concurrent;
using System.Threading;
using Neo4j.Driver;
using Xunit;
```

- [ ] **Step 2: Add the transaction tests and race helper**

Append to `BoltE2ETests`:

```csharp
    [Fact(DisplayName = "TX-001: Autocommit query executes and returns results")]
    public async Task Tx001_AutocommitQuery()
    {
        await using var session = _fixture.Driver.AsyncSession(o => o.WithDatabase("beer"));
        var result = await session.RunAsync("MATCH (b:Beer) RETURN b.name AS name LIMIT 5");
        var records = await result.ToListAsync();
        Assert.Equal(5, records.Count);
    }

    [Fact(DisplayName = "TX-002: Explicit BEGIN/RUN/COMMIT persists changes")]
    public async Task Tx002_ExplicitCommitPersists()
    {
        await using var session = _fixture.Driver.AsyncSession(o => o.WithDatabase("beer"));
        var tx = await session.BeginTransactionAsync();
        await tx.RunAsync("CREATE (:TxCommitProbe {marker: 'tx-002'})");
        await tx.CommitAsync();

        var result = await session.RunAsync("MATCH (n:TxCommitProbe {marker: 'tx-002'}) RETURN count(n) AS c");
        var record = await result.SingleAsync();
        Assert.Equal(1L, record["c"].As<long>());
    }

    [Fact(DisplayName = "TX-003: Explicit BEGIN/RUN/ROLLBACK discards changes")]
    public async Task Tx003_ExplicitRollbackDiscards()
    {
        await using var session = _fixture.Driver.AsyncSession(o => o.WithDatabase("beer"));
        var tx = await session.BeginTransactionAsync();
        await tx.RunAsync("CREATE (:TxRollbackProbe {marker: 'tx-003'})");
        await tx.RollbackAsync();

        var result = await session.RunAsync("MATCH (n:TxRollbackProbe {marker: 'tx-003'}) RETURN count(n) AS c");
        var record = await result.SingleAsync();
        Assert.Equal(0L, record["c"].As<long>());
    }

    [Fact(DisplayName = "TX-004: Managed transaction function executeWrite commits on success")]
    public async Task Tx004_ManagedWriteCommits()
    {
        await using var session = _fixture.Driver.AsyncSession(o => o.WithDatabase("beer"));
        await session.ExecuteWriteAsync(async tx =>
        {
            await tx.RunAsync("CREATE (:Beer {name: $n})", new { n = "TX-004-Beer" });
            return true;
        });

        var result = await session.RunAsync("MATCH (b:Beer {name: $n}) RETURN count(b) AS c", new { n = "TX-004-Beer" });
        var record = await result.SingleAsync();
        Assert.Equal(1L, record["c"].As<long>());
    }

    [Fact(DisplayName = "TX-005: Managed transaction function retries on Neo.TransientError.*")]
    public async Task Tx005_ManagedWriteRetriesOnTransientError()
    {
        var errors = await RaceTwoWritersAsync(_fixture.Driver, "beer", "tx-005");

        Assert.NotEmpty(errors);
        Assert.Contains(errors, e => e is TransientException);
        Assert.DoesNotContain(errors, e => e is ClientException || e is DatabaseException);
    }

    // Shared by TX-005 and ERR-004: two sessions race to update the same node
    // inside an explicit transaction, one held open past the other's commit
    // attempt. Returns every exception the racing writes raised.
    private static async Task<List<Exception>> RaceTwoWritersAsync(IDriver driver, string database, string marker)
    {
        await using (var setupSession = driver.AsyncSession(o => o.WithDatabase(database)))
        {
            var setup = await setupSession.RunAsync("MERGE (n:RaceProbe {marker: $marker}) SET n.value = 0", new { marker });
            await setup.ConsumeAsync();
        }

        var barrier = new Barrier(2);
        var errors = new ConcurrentBag<Exception>();

        async Task RacingWriteAsync()
        {
            await using var session = driver.AsyncSession(o => o.WithDatabase(database));
            try
            {
                barrier.SignalAndWait(TimeSpan.FromSeconds(5));
                var tx = await session.BeginTransactionAsync();
                var result = await tx.RunAsync(
                    "MATCH (n:RaceProbe {marker: $marker}) SET n.value = n.value + 1 RETURN n", new { marker });
                await result.ConsumeAsync();
                await Task.Delay(500);
                await tx.CommitAsync();
            }
            catch (Exception ex)
            {
                errors.Add(ex);
            }
        }

        await Task.WhenAll(RacingWriteAsync(), RacingWriteAsync());
        return errors.ToList();
    }
```

- [ ] **Step 3: Run**

Run: `dotnet test --filter "FullyQualifiedName~Tx00"`
Expected: `Passed! - Failed: 0, Passed: 5, Skipped: 0`

`TX-005` is timing-sensitive (matching the Python suite's same caveat); if it's flaky in your environment, re-run once before investigating further.

- [ ] **Step 4: Commit**

```bash
git add e2e-csharp/ArcadeDB.E2ETests/BoltE2ETests.cs
git commit -m "test(#4886): add TX-001..005 transaction scenarios and race helper"
```

---

## Task 6: Causal-consistency + multi-database scenarios (`CAUSAL-001`, `MDB-001..002`)

**Files:**
- Modify: `e2e-csharp/ArcadeDB.E2ETests/BoltE2ETests.cs`

**Interfaces:**
- Consumes: `ArcadeDbBoltFixture.Driver` (already exposes `boltscratch`, created in Task 2).

- [ ] **Step 1: Add the tests**

Append to `BoltE2ETests`:

```csharp
    [Fact(DisplayName = "CAUSAL-001: Bookmark enforces read-after-write across sessions")]
    public async Task Causal001_BookmarkReadAfterWrite()
    {
        Bookmarks bookmarks;
        await using (var sessionA = _fixture.Driver.AsyncSession(o => o.WithDatabase("beer")))
        {
            var result = await sessionA.RunAsync("CREATE (:CausalProbe {marker: 'causal-001'})");
            await result.ConsumeAsync();
            bookmarks = sessionA.LastBookmarks;
        }

        await using var sessionB = _fixture.Driver.AsyncSession(o => o.WithDatabase("beer").WithBookmarks(bookmarks));
        var readResult = await sessionB.RunAsync("MATCH (n:CausalProbe {marker: 'causal-001'}) RETURN count(n) AS c");
        var record = await readResult.SingleAsync();
        Assert.Equal(1L, record["c"].As<long>());
    }

    [Fact(DisplayName = "MDB-001: Session selects a specific named database")]
    public async Task Mdb001_SessionSelectsNamedDatabase()
    {
        await using var session = _fixture.Driver.AsyncSession(o => o.WithDatabase("beer"));
        var result = await session.RunAsync("MATCH (b:Beer) RETURN b.name AS name LIMIT 1");
        var record = await result.SingleAsync();
        Assert.NotNull(record["name"].As<string>());
    }

    [Fact(DisplayName = "MDB-002: Sessions against different databases on the same driver are isolated")]
    public async Task Mdb002_SessionsAcrossDatabasesAreIsolated()
    {
        await using var scratchSession = _fixture.Driver.AsyncSession(o => o.WithDatabase("boltscratch"));
        var tx = await scratchSession.BeginTransactionAsync();
        await tx.RunAsync("CREATE (:ScratchProbe {marker: 'mdb-002'})");

        await using (var beerSession = _fixture.Driver.AsyncSession(o => o.WithDatabase("beer")))
        {
            var checkResult = await beerSession.RunAsync("MATCH (n:ScratchProbe {marker: 'mdb-002'}) RETURN count(n) AS c");
            var checkRecord = await checkResult.SingleAsync();
            Assert.Equal(0L, checkRecord["c"].As<long>());
        }

        await tx.CommitAsync();

        var verifyResult = await scratchSession.RunAsync("MATCH (n:ScratchProbe {marker: 'mdb-002'}) RETURN count(n) AS c");
        var verifyRecord = await verifyResult.SingleAsync();
        Assert.Equal(1L, verifyRecord["c"].As<long>());
    }
```

- [ ] **Step 2: Run**

Run: `dotnet test --filter "FullyQualifiedName~Causal001|FullyQualifiedName~Mdb00"`
Expected: `Passed! - Failed: 0, Passed: 3, Skipped: 0`

- [ ] **Step 3: Commit**

```bash
git add e2e-csharp/ArcadeDB.E2ETests/BoltE2ETests.cs
git commit -m "test(#4886): add CAUSAL-001, MDB-001/002 scenarios"
```

---

## Task 7: `KnownGapAssertions` helper + result-handling scenarios (`RESULT-001..004`)

**Files:**
- Create: `e2e-csharp/ArcadeDB.E2ETests/KnownGapAssertions.cs`
- Modify: `e2e-csharp/ArcadeDB.E2ETests/BoltE2ETests.cs`

**Interfaces:**
- Produces: `public static class KnownGapAssertions { public static Task AssertStillFailsAsync(Func<Task> action, string reason); }`, reused by Tasks 8, 9, 10 for `TYPE-003/011/012`, `ERR-002`, `PROTO-002`.

- [ ] **Step 1: Write the helper**

Create `e2e-csharp/ArcadeDB.E2ETests/KnownGapAssertions.cs`:

```csharp
/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using Xunit.Sdk;

namespace ArcadeDB.E2ETests;

// xUnit has no built-in equivalent of pytest's xfail(strict=True). This
// helper reproduces that property: the action is the target-correct
// assertion; a failure today means the gap still reproduces (test passes),
// success means the gap was fixed and this now fails the build - forcing
// whoever fixed it to convert the test to a plain [Fact] and correct
// bolt/conformance/spec.yaml's current_status in the same PR.
public static class KnownGapAssertions
{
    public static async Task AssertStillFailsAsync(Func<Task> action, string reason)
    {
        try
        {
            await action();
        }
        catch
        {
            return;
        }

        throw new XunitException(
            $"Expected known gap to still reproduce, but the assertion passed - " +
            $"convert this to a normal [Fact] and update current_status in " +
            $"bolt/conformance/spec.yaml. Gap: {reason}");
    }
}
```

- [ ] **Step 2: Add the result-handling tests**

Append to `BoltE2ETests`:

```csharp
    [Fact(DisplayName = "RESULT-001: Streaming PULL returns records incrementally")]
    public async Task Result001_StreamingPullIncremental()
    {
        await using var session = _fixture.Driver.AsyncSession(o => o.WithDatabase("beer"));
        var result = await session.RunAsync("MATCH (b:Beer) RETURN b.name AS name LIMIT 10");

        var seen = 0;
        while (await result.FetchAsync())
        {
            Assert.NotNull(result.Current["name"].As<string>());
            seen++;
        }
        Assert.Equal(10, seen);
    }

    [Fact(DisplayName = "RESULT-002: PULL n streams exactly n rows, further PULL continues from where it left off")]
    public async Task Result002_PartialPullThenContinue()
    {
        await using var session = _fixture.Driver.AsyncSession(o => o.WithDatabase("beer").WithFetchSize(2));
        var result = await session.RunAsync("MATCH (b:Beer) RETURN b.name AS name LIMIT 5");

        var firstTwo = new List<IRecord>();
        for (var i = 0; i < 2; i++)
        {
            Assert.True(await result.FetchAsync());
            firstTwo.Add(result.Current);
        }
        Assert.Equal(2, firstTwo.Count);

        var remaining = await result.ToListAsync();
        Assert.Equal(3, remaining.Count);
    }

    [Fact(DisplayName = "RESULT-003: DISCARD abandons remaining rows without materializing them")]
    public async Task Result003_DiscardAbandonsRemaining()
    {
        await using var session = _fixture.Driver.AsyncSession(o => o.WithDatabase("beer"));
        var result = await session.RunAsync("MATCH (b:Beer) RETURN b.name AS name LIMIT 5");
        await result.FetchAsync();
        var summary = await result.ConsumeAsync();
        Assert.NotNull(summary);
    }

    [Fact(DisplayName = "RESULT-004: ResultSummary counters accurately reflect write operations")]
    public async Task Result004_SummaryCountersReflectWrites()
    {
        await KnownGapAssertions.AssertStillFailsAsync(async () =>
        {
            await using var session = _fixture.Driver.AsyncSession(o => o.WithDatabase("beer"));
            var result = await session.RunAsync(
                "CREATE (:Beer {name: $n})-[:BREWED_BY]->(:Brewery {name: $b})",
                new { n = "RESULT-004-Beer", b = "RESULT-004-Brewery" });
            var summary = await result.ConsumeAsync();

            Assert.Equal(2, summary.Counters.NodesCreated);
            Assert.Equal(1, summary.Counters.RelationshipsCreated);
            Assert.True(summary.Counters.PropertiesSet >= 2);
        }, "RESULT-004: BoltNetworkExecutor never populates SUCCESS 'stats' for write queries - see #4890");
    }
```

- [ ] **Step 3: Run**

Run: `dotnet test --filter "FullyQualifiedName~Result00"`
Expected: `Passed! - Failed: 0, Passed: 4, Skipped: 0` (the `RESULT-004` test itself passes - it confirms the gap still reproduces).

- [ ] **Step 4: Commit**

```bash
git add e2e-csharp/ArcadeDB.E2ETests/KnownGapAssertions.cs e2e-csharp/ArcadeDB.E2ETests/BoltE2ETests.cs
git commit -m "test(#4886): add KnownGapAssertions helper and RESULT-001..004 scenarios"
```

---

## Task 8: Type-roundtrip scenarios (`TYPE-001..012`)

**Files:**
- Modify: `e2e-csharp/ArcadeDB.E2ETests/BoltE2ETests.cs`

**Interfaces:**
- Consumes: `KnownGapAssertions.AssertStillFailsAsync` (Task 7).

- [ ] **Step 1: Add the tests**

Append to `BoltE2ETests`:

```csharp
    [Fact(DisplayName = "TYPE-001: Node round-trips as a native Bolt structure")]
    public async Task Type001_NodeRoundtrip()
    {
        await using var session = _fixture.Driver.AsyncSession(o => o.WithDatabase("beer"));
        var result = await session.RunAsync("MATCH (b:Beer) RETURN b LIMIT 1");
        var record = await result.SingleAsync();
        var node = record["b"].As<INode>();
        Assert.Contains("Beer", node.Labels);
        Assert.NotNull(node.Get<string>("name"));
    }

    [Fact(DisplayName = "TYPE-002: Relationship round-trips as a native Bolt structure")]
    public async Task Type002_RelationshipRoundtrip()
    {
        await using var session = _fixture.Driver.AsyncSession(o => o.WithDatabase("beer"));
        var result = await session.RunAsync("MATCH ()-[r]->() RETURN r LIMIT 1");
        var record = await result.SingleAsync();
        var relationship = record["r"].As<IRelationship>();
        Assert.NotNull(relationship.Type);
    }

    [Fact(DisplayName = "TYPE-003: Path round-trips as a native Bolt structure")]
    public async Task Type003_PathRoundtrip()
    {
        await KnownGapAssertions.AssertStillFailsAsync(async () =>
        {
            await using var session = _fixture.Driver.AsyncSession(o => o.WithDatabase("beer"));
            var result = await session.RunAsync("MATCH p=(b:Beer)-[*1..2]-() RETURN p LIMIT 1");
            var record = await result.SingleAsync();
            var path = record["p"].As<IPath>();
            Assert.True(path.Nodes.Count() >= 2);
        }, "TYPE-003: structure/BoltPath.java has zero call sites - query results never construct native Path structures - see #4890");
    }

    [Fact(DisplayName = "TYPE-004: ByteArray round-trips as a bound parameter")]
    public async Task Type004_ByteArrayParamRoundtrip()
    {
        await using var session = _fixture.Driver.AsyncSession(o => o.WithDatabase("beer"));
        var payload = new byte[] { 1, 2, 3, 4 };
        var result = await session.RunAsync("RETURN $b AS echo", new { b = payload });
        var record = await result.SingleAsync();
        Assert.Equal(payload, record["echo"].As<byte[]>());
    }

    [Fact(DisplayName = "TYPE-005: Nested lists and maps round-trip structurally")]
    public async Task Type005_NestedListMapRoundtrip()
    {
        await using var session = _fixture.Driver.AsyncSession(o => o.WithDatabase("beer"));
        var result = await session.RunAsync("MATCH (t:TypeMatrix) RETURN t.nestedListProp AS l, t.nestedMapProp AS m");
        var record = await result.SingleAsync();

        var list = record["l"].As<List<object>>();
        Assert.Equal(1L, list[0].As<long>());
        Assert.Equal(2L, list[1].As<long>());
        var nestedList = list[2].As<List<object>>();
        Assert.Equal(3L, nestedList[0].As<long>());
        Assert.Equal(4L, nestedList[1].As<long>());

        var map = record["m"].As<Dictionary<string, object>>();
        Assert.Equal(1L, map["a"].As<long>());
        var nestedMap = map["b"].As<Dictionary<string, object>>();
        Assert.Equal(2L, nestedMap["c"].As<long>());
    }

    [Fact(DisplayName = "TYPE-006: Null values round-trip")]
    public async Task Type006_NullRoundtrip()
    {
        await using var session = _fixture.Driver.AsyncSession(o => o.WithDatabase("beer"));
        var result = await session.RunAsync("MATCH (t:TypeMatrix) RETURN t.nullProp AS n");
        var record = await result.SingleAsync();
        Assert.Null(record["n"]);

        var echoResult = await session.RunAsync("RETURN $p AS echo", new { p = (object?)null });
        var echoRecord = await echoResult.SingleAsync();
        Assert.Null(echoRecord["echo"]);
    }

    [Fact(DisplayName = "TYPE-007: LocalDate round-trips as a native Bolt Date structure")]
    public async Task Type007_LocalDateRoundtrip()
    {
        await using var session = _fixture.Driver.AsyncSession(o => o.WithDatabase("beer"));
        var result = await session.RunAsync("MATCH (t:TypeMatrix) RETURN t.localDateProp AS d");
        var record = await result.SingleAsync();
        var date = record["d"].As<LocalDate>();

        var echoResult = await session.RunAsync("RETURN $d AS echo", new { d = date });
        var echoRecord = await echoResult.SingleAsync();
        Assert.Equal(date, echoRecord["echo"].As<LocalDate>());
    }

    [Fact(DisplayName = "TYPE-008: LocalTime round-trips as a native Bolt LocalTime structure")]
    public async Task Type008_LocalTimeRoundtrip()
    {
        await using var session = _fixture.Driver.AsyncSession(o => o.WithDatabase("beer"));
        var result = await session.RunAsync("MATCH (t:TypeMatrix) RETURN t.localTimeProp AS t2");
        var record = await result.SingleAsync();
        var time = record["t2"].As<LocalTime>();

        var echoResult = await session.RunAsync("RETURN $t AS echo", new { t = time });
        var echoRecord = await echoResult.SingleAsync();
        Assert.Equal(time, echoRecord["echo"].As<LocalTime>());
    }

    [Fact(DisplayName = "TYPE-009: LocalDateTime round-trips as a native Bolt LocalDateTime structure")]
    public async Task Type009_LocalDateTimeRoundtrip()
    {
        await using var session = _fixture.Driver.AsyncSession(o => o.WithDatabase("beer"));
        var result = await session.RunAsync("MATCH (t:TypeMatrix) RETURN t.localDateTimeProp AS dt");
        var record = await result.SingleAsync();
        var dateTime = record["dt"].As<LocalDateTime>();

        var echoResult = await session.RunAsync("RETURN $dt AS echo", new { dt = dateTime });
        var echoRecord = await echoResult.SingleAsync();
        Assert.Equal(dateTime, echoRecord["echo"].As<LocalDateTime>());
    }

    [Fact(DisplayName = "TYPE-010: Offset/zoned DateTime round-trips as a native Bolt DateTime structure")]
    public async Task Type010_OffsetDateTimeRoundtrip()
    {
        await using var session = _fixture.Driver.AsyncSession(o => o.WithDatabase("beer"));
        var result = await session.RunAsync("MATCH (t:TypeMatrix) RETURN t.offsetDateTimeProp AS dt");
        var record = await result.SingleAsync();
        var dateTime = record["dt"].As<ZonedDateTime>();
        Assert.Equal(2 * 3600, dateTime.OffsetSeconds);

        var echoResult = await session.RunAsync("RETURN $dt AS echo", new { dt = dateTime });
        var echoRecord = await echoResult.SingleAsync();
        Assert.Equal(dateTime, echoRecord["echo"].As<ZonedDateTime>());
    }

    [Fact(DisplayName = "TYPE-011: Duration round-trips as a native Bolt Duration structure")]
    public async Task Type011_DurationRoundtrip()
    {
        await KnownGapAssertions.AssertStillFailsAsync(async () =>
        {
            await using var session = _fixture.Driver.AsyncSession(o => o.WithDatabase("beer"));
            var result = await session.RunAsync("MATCH (t:TypeMatrix) RETURN t.durationProp AS d");
            var record = await result.SingleAsync();
            var duration = record["d"].As<Duration>();

            var echoResult = await session.RunAsync("RETURN $d AS echo", new { d = duration });
            var echoRecord = await echoResult.SingleAsync();
            Assert.Equal(duration, echoRecord["echo"].As<Duration>());
        }, "TYPE-011: BoltStructureMapper/PackStreamWriter have no Duration handling at all - see #4890");
    }

    [Fact(DisplayName = "TYPE-012: Point round-trips as a native Bolt Point structure")]
    public async Task Type012_PointRoundtrip()
    {
        await KnownGapAssertions.AssertStillFailsAsync(async () =>
        {
            await using var session = _fixture.Driver.AsyncSession(o => o.WithDatabase("beer"));
            var result = await session.RunAsync("MATCH (t:TypeMatrix) RETURN t.pointProp AS p");
            var record = await result.SingleAsync();
            var point = record["p"].As<Point>();
            Assert.Equal(12.34, point.X, 2);
            Assert.Equal(56.78, point.Y, 2);

            var echoResult = await session.RunAsync("RETURN $p AS echo", new { p = point });
            var echoRecord = await echoResult.SingleAsync();
            var echoPoint = echoRecord["echo"].As<Point>();
            Assert.Equal(12.34, echoPoint.X, 2);
            Assert.Equal(56.78, echoPoint.Y, 2);
        }, "TYPE-012: no Point/spatial handling exists in BoltStructureMapper or PackStreamWriter - see #4890");
    }
```

Note: `TYPE-003` uses `path.Nodes.Count()` (LINQ `Count()` on `IEnumerable<INode>`) - add `using System.Linq;` to the top of `BoltE2ETests.cs` if not already implicitly available (`ImplicitUsings` is enabled in the `.csproj`, which already brings in `System.Linq` - verify with a build; if it fails to resolve, add the explicit `using`).

- [ ] **Step 2: Run**

Run: `dotnet test --filter "FullyQualifiedName~Type0"`
Expected: `Passed! - Failed: 0, Passed: 12, Skipped: 0` (`TYPE-003/011/012` pass by confirming the gap still reproduces).

- [ ] **Step 3: Commit**

```bash
git add e2e-csharp/ArcadeDB.E2ETests/BoltE2ETests.cs
git commit -m "test(#4886): add TYPE-001..012 type-roundtrip scenarios"
```

---

## Task 9: Error scenarios (`ERR-001..004`)

**Files:**
- Modify: `e2e-csharp/ArcadeDB.E2ETests/BoltE2ETests.cs`

**Interfaces:**
- Consumes: `KnownGapAssertions.AssertStillFailsAsync` (Task 7), `RaceTwoWritersAsync` (Task 5).

- [ ] **Step 1: Add the tests**

Append to `BoltE2ETests`:

```csharp
    [Fact(DisplayName = "ERR-001: Syntax error returns Neo.ClientError.Statement.SyntaxError")]
    public async Task Err001_SyntaxError()
    {
        await using var session = _fixture.Driver.AsyncSession(o => o.WithDatabase("beer"));
        var ex = await Assert.ThrowsAsync<ClientException>(async () =>
        {
            var result = await session.RunAsync("MATCH (n RETURN n");
            await result.ConsumeAsync();
        });
        Assert.Equal("Neo.ClientError.Statement.SyntaxError", ex.Code);
    }

    [Fact(DisplayName = "ERR-002: Semantic error returns Neo.ClientError.Statement.SemanticError")]
    public async Task Err002_SemanticError()
    {
        await KnownGapAssertions.AssertStillFailsAsync(async () =>
        {
            await using var session = _fixture.Driver.AsyncSession(o => o.WithDatabase("beer"));
            var ex = await Assert.ThrowsAsync<ClientException>(async () =>
            {
                var result = await session.RunAsync("MATCH (n:Beer) RETURN undeclaredVariable");
                await result.ConsumeAsync();
            });
            Assert.Equal("Neo.ClientError.Statement.SemanticError", ex.Code);
        }, "ERR-002: BoltNetworkExecutor's RUN handler maps error codes by exception type and cannot distinguish semantic from syntax errors - SemanticError is dead code - see #4890");
    }

    [Fact(Skip = "ERR-003 requires sending RUN before completing HELLO/LOGON; no official driver's public API exposes that - current_status is not-applicable in spec.yaml, see #4890")]
    public void Err003_UnauthenticatedRequestRejected()
    {
    }

    [Fact(DisplayName = "ERR-004: Transient conditions surface Neo.TransientError.* codes")]
    public async Task Err004_TransientConditionErrorCode()
    {
        var errors = await RaceTwoWritersAsync(_fixture.Driver, "beer", "err-004");

        Assert.NotEmpty(errors);
        Assert.Contains(errors, e => e is TransientException);
        Assert.DoesNotContain(errors, e => e is ClientException || e is DatabaseException);
    }
```

- [ ] **Step 2: Run**

Run: `dotnet test --filter "FullyQualifiedName~Err00"`
Expected: `Passed! - Failed: 0, Passed: 3, Skipped: 1`

- [ ] **Step 3: Commit**

```bash
git add e2e-csharp/ArcadeDB.E2ETests/BoltE2ETests.cs
git commit -m "test(#4886): add ERR-001..004 error scenarios"
```

---

## Task 10: Protocol scenarios (`PROTO-001..003`)

**Files:**
- Modify: `e2e-csharp/ArcadeDB.E2ETests/BoltE2ETests.cs`

**Interfaces:**
- Consumes: `KnownGapAssertions.AssertStillFailsAsync` (Task 7).

- [ ] **Step 1: Add the tests**

Append to `BoltE2ETests`:

```csharp
    [Fact(DisplayName = "PROTO-001: Version negotiation succeeds for Bolt 4.4, 4.0, and 3.0")]
    public async Task Proto001_VersionNegotiationSucceeds()
    {
        await using var session = _fixture.Driver.AsyncSession(o => o.WithDatabase("beer"));
        var result = await session.RunAsync("RETURN 1 AS value");
        var record = await result.SingleAsync();
        Assert.Equal(1L, record["value"].As<long>());
    }

    [Fact(DisplayName = "PROTO-002: Version negotiation with a Bolt 5.x-only driver")]
    public async Task Proto002_Bolt5xNegotiationIsDocumented()
    {
        await KnownGapAssertions.AssertStillFailsAsync(async () =>
        {
            await using var session = _fixture.Driver.AsyncSession(o => o.WithDatabase("beer"));
            var result = await session.RunAsync("RETURN 1 AS value");
            var summary = await result.ConsumeAsync();

            var protocolVersionString = summary.Server.ProtocolVersion.ToString();
            var majorVersion = int.Parse(protocolVersionString!.Split('.')[0]);
            Assert.True(majorVersion >= 5,
                $"driver negotiated Bolt {protocolVersionString} instead of a documented Bolt 5.x version");
        }, "PROTO-002: BoltNetworkExecutor.SUPPORTED_VERSIONS never advertises Bolt 5.x - see #4890");
    }

    [Fact(DisplayName = "PROTO-003: RESET returns the connection to a clean state mid-stream")]
    public async Task Proto003_ResetMidStream()
    {
        await using var session = _fixture.Driver.AsyncSession(o => o.WithDatabase("beer").WithFetchSize(2));
        var result = await session.RunAsync("MATCH (b:Beer) RETURN b.name AS name LIMIT 10");
        await result.FetchAsync();
        await result.ConsumeAsync();

        var followUp = await session.RunAsync("RETURN 1 AS value");
        var followUpRecord = await followUp.SingleAsync();
        Assert.Equal(1L, followUpRecord["value"].As<long>());
    }
```

- [ ] **Step 2: Run the whole `BoltE2ETests` class**

Run: `dotnet test --filter "FullyQualifiedName~BoltE2ETests"`
Expected: `Passed! - Failed: 0, Passed: 35, Skipped: 2` (37 test methods total, matching the 37 non-TLS/non-HA scenarios this class covers: 3 CONN + 3 AUTH + 5 TX + 1 CAUSAL + 2 MDB + 4 RESULT + 12 TYPE + 4 ERR + 3 PROTO. 2 are `[Fact(Skip=...)]` (`CONN-004`, `ERR-003`); the rest - including the 6 `AssertStillFailsAsync`-wrapped known gaps - report as passed.)

- [ ] **Step 3: Commit**

```bash
git add e2e-csharp/ArcadeDB.E2ETests/BoltE2ETests.cs
git commit -m "test(#4886): add PROTO-001..003 protocol scenarios"
```

---

## Task 11: TLS fixtures (`ArcadeDbBoltTlsRequiredFixture`, `ArcadeDbBoltTlsOptionalFixture`)

**Files:**
- Modify: `e2e-csharp/ArcadeDB.E2ETests/BoltFixtures.cs`

**Interfaces:**
- Produces: `ArcadeDbBoltTlsRequiredFixture` / `ArcadeDbBoltTlsOptionalFixture`, each with public `string BoltSscUri` / `string BoltUri` respectively, and collection names `"ArcadeDB-Bolt-Tls-Required"` / `"ArcadeDB-Bolt-Tls-Optional"`.

- [ ] **Step 1: Add a shared TLS-certificate helper and the two fixtures**

Append to `BoltFixtures.cs` (after the closing brace of `ArcadeDbBoltFixture`):

```csharp
// Shared by both TLS fixtures below: generates a throwaway self-signed
// keystore/truststore pair via the JDK `keytool` binary (present by default
// on GitHub's ubuntu-latest runners - no extra CI setup needed, same
// assumption the already-merged e2e-python suite makes), then bakes it into
// a derived image via `docker build COPY` rather than a runtime bind-mount:
// the Python suite's fixture comment on this is explicit that bind-mounts
// were observed to start with an empty mounted directory on some CI
// runners, while a Dockerfile COPY always resolves its own build context
// correctly.
internal static class BoltTlsImage
{
    public const string StorePassword = "changeit";

    public static async Task<string> BuildAsync()
    {
        var certDir = Directory.CreateTempSubdirectory("bolt-tls-certs-").FullName;
        var keystorePath = Path.Combine(certDir, "keystore.p12");
        var truststorePath = Path.Combine(certDir, "truststore.jks");
        var certPath = Path.Combine(certDir, "bolt.cer");

        RunKeytool(
            "-genkeypair", "-alias", "bolt", "-keyalg", "RSA", "-keysize", "2048",
            "-validity", "3650", "-keystore", keystorePath, "-storetype", "PKCS12",
            "-storepass", StorePassword, "-keypass", StorePassword,
            "-dname", "CN=localhost, OU=ArcadeDB, O=ArcadeDB, L=Test, ST=Test, C=US");
        RunKeytool(
            "-exportcert", "-alias", "bolt", "-keystore", keystorePath, "-storetype", "PKCS12",
            "-storepass", StorePassword, "-file", certPath);
        RunKeytool(
            "-importcert", "-alias", "bolt", "-keystore", truststorePath, "-storetype", "JKS",
            "-storepass", StorePassword, "-file", certPath, "-noprompt");

        await File.WriteAllTextAsync(Path.Combine(certDir, "Dockerfile"),
            "FROM arcadedata/arcadedb:latest\n" +
            "COPY --chown=arcadedb:arcadedb keystore.p12 truststore.jks /home/arcadedb/tls_certs/\n");

        // NOTE for implementer: verify ImageFromDockerfileBuilder's exact
        // WithDockerfileDirectory overload against the installed
        // Testcontainers package (IntelliSense/decompiled source) - some
        // versions take (commonDirectory, subDirectory) instead of a single
        // path. Adjust this call if it fails to compile as written.
        var image = new ImageFromDockerfileBuilder()
            .WithDockerfileDirectory(certDir, string.Empty)
            .WithDockerfile("Dockerfile")
            .WithName("arcadedb-bolt-tls-test:latest")
            .WithCleanUp(true)
            .Build();
        await image.CreateAsync();

        Directory.Delete(certDir, recursive: true);
        return image.FullName;
    }

    private static void RunKeytool(params string[] arguments)
    {
        var startInfo = new ProcessStartInfo(FindKeytool())
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        foreach (var arg in arguments)
            startInfo.ArgumentList.Add(arg);

        using var process = Process.Start(startInfo)
            ?? throw new InvalidOperationException("Failed to start keytool");
        var stderr = process.StandardError.ReadToEnd();
        process.WaitForExit();
        if (process.ExitCode != 0)
            throw new InvalidOperationException($"keytool failed: {stderr}");
    }

    private static string FindKeytool()
    {
        var javaHome = Environment.GetEnvironmentVariable("JAVA_HOME");
        if (!string.IsNullOrWhiteSpace(javaHome))
        {
            var candidate = Path.Combine(javaHome, "bin", "keytool");
            if (File.Exists(candidate) || File.Exists(candidate + ".exe"))
                return candidate;
        }
        return "keytool";
    }
}

[CollectionDefinition("ArcadeDB-Bolt-Tls-Required")]
public class ArcadeDbBoltTlsRequiredCollection : ICollectionFixture<ArcadeDbBoltTlsRequiredFixture> { }

public class ArcadeDbBoltTlsRequiredFixture : IAsyncLifetime
{
    private IContainer _container = null!;
    public string BoltSscUri { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        var imageTag = await BoltTlsImage.BuildAsync();

        _container = new ContainerBuilder(imageTag)
            .WithPortBinding(2480, true)
            .WithPortBinding(7687, true)
            .WithEnvironment("JAVA_OPTS",
                $"-Darcadedb.server.rootPassword={ArcadeDbBoltFixture.RootPassword} " +
                "-Darcadedb.server.plugins=BoltProtocolPlugin " +
                "-Darcadedb.bolt.ssl=REQUIRED " +
                "-Darcadedb.ssl.keyStore=/home/arcadedb/tls_certs/keystore.p12 " +
                $"-Darcadedb.ssl.keyStorePassword={BoltTlsImage.StorePassword} " +
                "-Darcadedb.ssl.trustStore=/home/arcadedb/tls_certs/truststore.jks " +
                $"-Darcadedb.ssl.trustStorePassword={BoltTlsImage.StorePassword}")
            .WithWaitStrategy(Wait.ForUnixContainer()
                .UntilHttpRequestIsSucceeded(r => r
                    .ForPort(2480)
                    .ForPath("/api/v1/ready")
                    .ForStatusCode(HttpStatusCode.NoContent)))
            .Build();

        await _container.StartAsync();

        var boltPort = _container.GetMappedPublicPort(7687);
        BoltSscUri = $"bolt+ssc://{_container.Hostname}:{boltPort}";
    }

    public async Task DisposeAsync()
    {
        if (_container is not null)
            await _container.DisposeAsync();
    }
}

[CollectionDefinition("ArcadeDB-Bolt-Tls-Optional")]
public class ArcadeDbBoltTlsOptionalCollection : ICollectionFixture<ArcadeDbBoltTlsOptionalFixture> { }

public class ArcadeDbBoltTlsOptionalFixture : IAsyncLifetime
{
    private IContainer _container = null!;
    public string BoltUri { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        var imageTag = await BoltTlsImage.BuildAsync();

        _container = new ContainerBuilder(imageTag)
            .WithPortBinding(2480, true)
            .WithPortBinding(7687, true)
            .WithEnvironment("JAVA_OPTS",
                $"-Darcadedb.server.rootPassword={ArcadeDbBoltFixture.RootPassword} " +
                "-Darcadedb.server.plugins=BoltProtocolPlugin " +
                "-Darcadedb.bolt.ssl=OPTIONAL " +
                "-Darcadedb.ssl.keyStore=/home/arcadedb/tls_certs/keystore.p12 " +
                $"-Darcadedb.ssl.keyStorePassword={BoltTlsImage.StorePassword} " +
                "-Darcadedb.ssl.trustStore=/home/arcadedb/tls_certs/truststore.jks " +
                $"-Darcadedb.ssl.trustStorePassword={BoltTlsImage.StorePassword}")
            .WithWaitStrategy(Wait.ForUnixContainer()
                .UntilHttpRequestIsSucceeded(r => r
                    .ForPort(2480)
                    .ForPath("/api/v1/ready")
                    .ForStatusCode(HttpStatusCode.NoContent)))
            .Build();

        await _container.StartAsync();

        var boltPort = _container.GetMappedPublicPort(7687);
        BoltUri = $"bolt://{_container.Hostname}:{boltPort}";
    }

    public async Task DisposeAsync()
    {
        if (_container is not null)
            await _container.DisposeAsync();
    }
}
```

- [ ] **Step 2: Build**

Run: `cd e2e-csharp/ArcadeDB.E2ETests && dotnet build`
Expected: `Build succeeded. 0 Warning(s) 0 Error(s)`. If `ImageFromDockerfileBuilder` calls fail to compile, fix the builder call chain against the installed `Testcontainers` package's actual API (see the NOTE comment above) - the certificate-generation and Dockerfile logic do not need to change.

- [ ] **Step 3: Commit**

```bash
git add e2e-csharp/ArcadeDB.E2ETests/BoltFixtures.cs
git commit -m "$(cat <<'EOF'
feat(#4886): add TLS-required/optional Bolt fixtures

Each builds a throwaway arcadedata/arcadedb image with a self-signed
keystore/truststore baked in via Dockerfile COPY (not a runtime
bind-mount - unreliable on some CI runners per e2e-python's precedent),
then starts its own container with -Darcadedb.bolt.ssl=REQUIRED/OPTIONAL.
EOF
)"
```

---

## Task 12: TLS scenarios (`CONN-002`, `CONN-005`)

**Files:**
- Create: `e2e-csharp/ArcadeDB.E2ETests/BoltTlsE2ETests.cs`

**Interfaces:**
- Consumes: `ArcadeDbBoltTlsRequiredFixture.BoltSscUri`, `ArcadeDbBoltTlsOptionalFixture.BoltUri`, `ArcadeDbBoltFixture.RootUser`/`.RootPassword` (Task 2/11).

- [ ] **Step 1: Write the test file**

Create `e2e-csharp/ArcadeDB.E2ETests/BoltTlsE2ETests.cs`:

```csharp
/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using Neo4j.Driver;
using Xunit;

namespace ArcadeDB.E2ETests;

[Collection("ArcadeDB-Bolt-Tls-Required")]
public class BoltTlsRequiredE2ETests
{
    private readonly ArcadeDbBoltTlsRequiredFixture _fixture;

    public BoltTlsRequiredE2ETests(ArcadeDbBoltTlsRequiredFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact(DisplayName = "CONN-002: Connect via bolt+s:// with TLS required")]
    public async Task Conn002_TlsRequired()
    {
        await using var driver = GraphDatabase.Driver(
            _fixture.BoltSscUri,
            AuthTokens.Basic(ArcadeDbBoltFixture.RootUser, ArcadeDbBoltFixture.RootPassword));
        await driver.VerifyConnectivityAsync();

        await using var session = driver.AsyncSession(o => o.WithDatabase("beer"));
        var result = await session.RunAsync("MATCH (b:Beer) RETURN b.name AS name LIMIT 1");
        var record = await result.SingleAsync();
        Assert.NotNull(record["name"].As<string>());
    }
}

[Collection("ArcadeDB-Bolt-Tls-Optional")]
public class BoltTlsOptionalE2ETests
{
    private readonly ArcadeDbBoltTlsOptionalFixture _fixture;

    public BoltTlsOptionalE2ETests(ArcadeDbBoltTlsOptionalFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact(DisplayName = "CONN-005: TLS OPTIONAL mode falls back to plaintext bolt://")]
    public async Task Conn005_TlsOptionalPlaintextConnects()
    {
        await using var driver = GraphDatabase.Driver(
            _fixture.BoltUri,
            AuthTokens.Basic(ArcadeDbBoltFixture.RootUser, ArcadeDbBoltFixture.RootPassword));
        await driver.VerifyConnectivityAsync();
    }
}
```

- [ ] **Step 2: Run**

Run: `dotnet test --filter "FullyQualifiedName~BoltTls"`
Expected: `Passed! - Failed: 0, Passed: 2, Skipped: 0`

TLS container startup is slower than the default fixture (keystore/truststore loading); if it times out, this mirrors the Python suite's own note that TLS containers need extra startup margin - the `120`-ish second default `dotnet test` timeout should be sufficient, but if not, this is a good place to raise it (not expected to be needed based on the Python suite's experience).

- [ ] **Step 3: Commit**

```bash
git add e2e-csharp/ArcadeDB.E2ETests/BoltTlsE2ETests.cs
git commit -m "test(#4886): add CONN-002/CONN-005 TLS scenarios"
```

---

## Task 13: Full-suite verification

**Files:** none (verification only).

- [ ] **Step 1: Run the entire E2E project, including the pre-existing Postgres suite**

Run: `cd e2e-csharp/ArcadeDB.E2ETests && dotnet test --logger "console;verbosity=normal"`
Expected: all `PostgresE2ETests` (7 tests) still pass unmodified, all `BoltE2ETests` (37 tests: 35 passed + 2 skipped), `BoltTlsRequiredE2ETests`/`BoltTlsOptionalE2ETests` (2 tests) pass. Total: `Passed! - Failed: 0, Passed: 44, Skipped: 2`.

- [ ] **Step 2: Confirm no stray debug output**

Run: `grep -rn "Console.WriteLine" e2e-csharp/ArcadeDB.E2ETests/Bolt*.cs`
Expected: no output (empty). If anything is found, remove it before proceeding.

- [ ] **Step 3: Confirm every spec.yaml scenario id is referenced exactly once**

Run:
```bash
grep -oE '"(CONN|AUTH|TX|CAUSAL|MDB|RESULT|TYPE|ERR|PROTO)-[0-9]+' \
  e2e-csharp/ArcadeDB.E2ETests/BoltE2ETests.cs \
  e2e-csharp/ArcadeDB.E2ETests/BoltTlsE2ETests.cs \
  | tr -d '"' | sort -u | wc -l
```
Expected: `39` (every scenario in `bolt/conformance/spec.yaml` is represented exactly once: 5 CONN + 3 AUTH + 5 TX + 1 CAUSAL + 2 MDB + 4 RESULT + 12 TYPE + 4 ERR + 3 PROTO).

- [ ] **Step 4: No commit needed** - this task is verification-only. If Step 1 or Step 3 surfaces a gap, fix it in the relevant task's file and amend that task's commit (or add a small follow-up commit) before moving on.

---

## Self-Review Notes

- **Spec coverage:** all 39 scenarios in `bolt/conformance/spec.yaml` (as corrected by #4885 on `main`) map to a test: 31 plain `[Fact]`, 2 `[Fact(Skip=...)]` (`CONN-004`, `ERR-003`), 6 `AssertStillFailsAsync`-wrapped (`TYPE-003`, `TYPE-011`, `TYPE-012`, `RESULT-004`, `ERR-002`, `PROTO-002`). 31 + 2 + 6 = 39.
- **Type consistency:** `RaceTwoWritersAsync` is defined once in Task 5 and consumed identically by `TX-005` (Task 5) and `ERR-004` (Task 9); `KnownGapAssertions.AssertStillFailsAsync` is defined once in Task 7 and consumed by name in Tasks 8, 9, 10 with the same signature (`Func<Task> action, string reason`).
- **Residual API uncertainty flagged inline:** `ImageFromDockerfileBuilder.WithDockerfileDirectory`'s exact overload (Task 11) could not be fully confirmed from public docs; the plan flags it with a NOTE comment rather than silently guessing. Everything else (`GraphDatabase.Driver`, `AsyncSession`, `AuthTokens`, `Neo4jException` hierarchy, `INode`/`IRelationship`/`IPath`, `LocalDate`/`LocalTime`/`LocalDateTime`/`ZonedDateTime`/`Duration`/`Point`, `WithFetchSize`, `LastBookmarks`/`WithBookmarks`) was confirmed against the official Neo4j .NET driver API docs during design.
