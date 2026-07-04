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

using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
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
