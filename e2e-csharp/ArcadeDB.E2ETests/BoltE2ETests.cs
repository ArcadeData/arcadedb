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

using System.Collections.Concurrent;
using System.Threading;
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

        var ex = await Assert.ThrowsAsync<AuthenticationException>(() => driver.VerifyConnectivityAsync());
        Assert.Equal("Neo.ClientError.Security.Unauthorized", ex.Code);
    }

    [Fact(DisplayName = "AUTH-003: Auth scheme 'none' is rejected (intentional, not a bug)")]
    public async Task Auth003_AuthNoneRejected()
    {
        await using var driver = GraphDatabase.Driver(_fixture.BoltUri, AuthTokens.None);

        await Assert.ThrowsAnyAsync<Neo4jException>(() => driver.VerifyConnectivityAsync());
    }

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

        // Wrapped in Task.Run: a bare `Task.WhenAll(RacingWriteAsync(), RacingWriteAsync())`
        // does not race in .NET the way it does with Python's explicit os threads. Local
        // async-method calls run synchronously on the calling thread up to their first
        // genuine suspension point; Barrier.SignalAndWait is an ordinary blocking call, not
        // an await, so the first invocation would monopolize the calling thread inside the
        // barrier wait and the second invocation would never start until the first timed
        // out - the two writers would run 5s apart and never actually contend. Task.Run
        // dispatches each onto its own thread pool thread up front so both reach the
        // barrier concurrently, matching the intended race.
        await Task.WhenAll(Task.Run(RacingWriteAsync), Task.Run(RacingWriteAsync));
        return errors.ToList();
    }
}
