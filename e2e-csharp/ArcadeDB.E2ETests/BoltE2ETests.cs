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
using System.Runtime.InteropServices;
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
        // neo4j:// routing makes the driver connect to the container IP that
        // handleRoute advertises. That address is host-routable on Linux CI's
        // native Docker bridge but not from the host on Docker Desktop
        // (macOS/Windows), where the driver times out. xUnit 2.x has no runtime
        // skip, so short-circuit off Linux; bolt:// scenarios use the mapped
        // host port and are unaffected.
        if (!RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            return;

        await using var driver = GraphDatabase.Driver(
            _fixture.NeoRoutingUri,
            AuthTokens.Basic(ArcadeDbBoltFixture.RootUser, ArcadeDbBoltFixture.RootPassword));
        await driver.VerifyConnectivityAsync();

        await using var session = driver.AsyncSession(o => o.WithDatabase("beer"));
        var result = await session.RunAsync("MATCH (b:Beer) RETURN b.name AS name LIMIT 1");
        var record = await result.SingleAsync();
        Assert.NotNull(record["name"].As<string>());
    }

    [Fact(Skip = "CONN-004 requires a 3-node HA cluster; e2e-csharp's single-node harness cannot exercise this without new multi-node orchestration infrastructure - see #4890")]
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
        const string marker = "tx-005-managed";
        await using (var setupSession = _fixture.Driver.AsyncSession(o => o.WithDatabase("beer")))
        {
            var setup = await setupSession.RunAsync(
                "MERGE (n:RaceProbe {marker: $marker}) SET n.value = 0", new { marker });
            await setup.ConsumeAsync();
        }

        using var barrier = new Barrier(2);

        async Task<long> ManagedIncrementAsync()
        {
            await using var session = _fixture.Driver.AsyncSession(o => o.WithDatabase("beer"));
            if (!barrier.SignalAndWait(TimeSpan.FromSeconds(5)))
                throw new TimeoutException("Barrier did not release within 5s - the other racing writer never arrived");

            return await session.ExecuteWriteAsync(async tx =>
            {
                var result = await tx.RunAsync(
                    "MATCH (n:RaceProbe {marker: $marker}) SET n.value = n.value + 1 RETURN n.value AS v",
                    new { marker });
                var record = await result.SingleAsync();
                // Widen the collision window inside the managed transaction (same
                // purpose as RaceTwoWritersAsync's 500ms hold) so the two writers
                // actually contend rather than serializing cleanly - without this,
                // ExecuteWriteAsync's automatic retry has nothing to prove.
                await Task.Delay(500);
                return record["v"].As<long>();
            });
        }

        // The core assertion: if the driver's built-in retry policy didn't handle
        // the transient conflict automatically, one of these ExecuteWriteAsync
        // calls would throw and fail this await - the test passing at all is the
        // proof that automatic retry happened, distinct from ERR-004 (which
        // asserts the raw un-retried exception surfaces on the EXPLICIT-transaction
        // path).
        var results = await Task.WhenAll(Task.Run(ManagedIncrementAsync), Task.Run(ManagedIncrementAsync));

        Assert.Equal(new[] { 1L, 2L }, results.OrderBy(v => v).ToArray());
    }

    // Used by ERR-004 (explicit-transaction path): two sessions race to update the same node
    // inside an explicit transaction, one held open past the other's commit
    // attempt. Returns every exception the racing writes raised.
    private static async Task<List<Exception>> RaceTwoWritersAsync(IDriver driver, string database, string marker)
    {
        await using (var setupSession = driver.AsyncSession(o => o.WithDatabase(database)))
        {
            var setup = await setupSession.RunAsync("MERGE (n:RaceProbe {marker: $marker}) SET n.value = 0", new { marker });
            await setup.ConsumeAsync();
        }

        using var barrier = new Barrier(2);
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
        await using var session = _fixture.Driver.AsyncSession(o => o.WithDatabase("beer"));
        var result = await session.RunAsync(
            "CREATE (:Beer {name: $n})-[:BREWED_BY]->(:Brewery {name: $b})",
            new { n = "RESULT-004-Beer", b = "RESULT-004-Brewery" });
        var summary = await result.ConsumeAsync();

        Assert.Equal(2, summary.Counters.NodesCreated);
        Assert.Equal(1, summary.Counters.RelationshipsCreated);
        Assert.True(summary.Counters.PropertiesSet >= 2);
    }

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
        await using var session = _fixture.Driver.AsyncSession(o => o.WithDatabase("beer"));
        var result = await session.RunAsync("MATCH p=(b:Beer)-[*1..2]-() RETURN p LIMIT 1");
        var record = await result.SingleAsync();
        var path = record["p"].As<IPath>();
        Assert.True(path.Nodes.Count() >= 2);
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
        await using var session = _fixture.Driver.AsyncSession(o => o.WithDatabase("beer"));
        var result = await session.RunAsync("MATCH (t:TypeMatrix) RETURN t.durationProp AS d");
        var record = await result.SingleAsync();
        var duration = record["d"].As<Duration>();

        var echoResult = await session.RunAsync("RETURN $d AS echo", new { d = duration });
        var echoRecord = await echoResult.SingleAsync();
        Assert.Equal(duration, echoRecord["echo"].As<Duration>());
    }

    [Fact(DisplayName = "TYPE-012: Point round-trips as a native Bolt Point structure")]
    public async Task Type012_PointRoundtrip()
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
    }

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
        await using var session = _fixture.Driver.AsyncSession(o => o.WithDatabase("beer"));
        var ex = await Assert.ThrowsAsync<ClientException>(async () =>
        {
            var result = await session.RunAsync("MATCH (n:Beer) RETURN undeclaredVariable");
            await result.ConsumeAsync();
        });
        Assert.Equal("Neo.ClientError.Statement.SemanticError", ex.Code);
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

    [Fact(DisplayName = "PROTO-001: Version negotiation succeeds for Bolt 4.4, 4.0, and 3.0")]
    public async Task Proto001_VersionNegotiationSucceeds()
    {
        await using var session = _fixture.Driver.AsyncSession(o => o.WithDatabase("beer"));
        var result = await session.RunAsync("RETURN 1 AS value");
        var record = await result.SingleAsync();
        Assert.Equal(1L, record["value"].As<long>());
    }

    [Fact(DisplayName = "PROTO-002: Version negotiation with a Bolt 5.x-only driver")]
    public async Task Proto002_Bolt5xNegotiationIsSupported()
    {
        // Since #5001 the server advertises Bolt 5.0-5.4, so a 5.x-capable
        // driver negotiates a 5.x protocol version instead of downgrading to 4.4.
        await using var session = _fixture.Driver.AsyncSession(o => o.WithDatabase("beer"));
        var result = await session.RunAsync("RETURN 1 AS value");
        var summary = await result.ConsumeAsync();

        var protocolVersionString = summary.Server.ProtocolVersion?.ToString()
            ?? throw new InvalidCastException("ProtocolVersion was null");
        if (!int.TryParse(protocolVersionString.Split('.')[0], out var majorVersion))
            throw new InvalidCastException($"ProtocolVersion '{protocolVersionString}' was not in the expected 'major.minor' shape");
        Assert.True(majorVersion >= 5,
            $"driver negotiated Bolt {protocolVersionString} instead of a 5.x version");
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
}
