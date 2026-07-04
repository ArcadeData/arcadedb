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

    [Fact(DisplayName = "CONN-002: Connect via bolt+ssc:// with TLS required")]
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
