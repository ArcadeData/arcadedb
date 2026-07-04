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
}
