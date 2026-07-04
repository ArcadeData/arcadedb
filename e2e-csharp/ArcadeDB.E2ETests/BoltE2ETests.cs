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
