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
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Npgsql;
using Xunit;

namespace ArcadeDB.E2ETests;

[CollectionDefinition("ArcadeDB")]
public class ArcadeDbCollection : ICollectionFixture<ArcadeDbFixture> { }

public class ArcadeDbFixture : IAsyncLifetime
{
    private const string RootUser = "root";
    private const string RootPassword = "playwithdata";

    private IContainer _container = null!;
    public NpgsqlDataSource DataSource { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        var image = Environment.GetEnvironmentVariable("ARCADEDB_DOCKER_IMAGE")
                    ?? "arcadedata/arcadedb:latest";

        _container = new ContainerBuilder(image)
            .WithPortBinding(2480, true)
            .WithPortBinding(5432, true)
            .WithEnvironment("JAVA_OPTS",
                $"-Darcadedb.server.rootPassword={RootPassword} " +
                "-Darcadedb.server.plugins=PostgresProtocolPlugin")
            .WithWaitStrategy(Wait.ForUnixContainer()
                .UntilHttpRequestIsSucceeded(r => r
                    .ForPort(2480)
                    .ForPath("/api/v1/ready")
                    .ForStatusCode(HttpStatusCode.NoContent)))
            .Build();

        await _container.StartAsync();

        var httpPort = _container.GetMappedPublicPort(2480);
        using var http = new HttpClient();
        http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue(
            "Basic", Convert.ToBase64String(Encoding.UTF8.GetBytes($"{RootUser}:{RootPassword}")));
        using var response = await http.PostAsync(
            $"http://{_container.Hostname}:{httpPort}/api/v1/server",
            new StringContent(
                "{\"command\":\"create database NpgsqlE2ETest\"}",
                Encoding.UTF8,
                "application/json"));
        response.EnsureSuccessStatusCode();

        var pgPort = _container.GetMappedPublicPort(5432);
        DataSource = NpgsqlDataSource.Create(
            $"Host={_container.Hostname};Port={pgPort};Database=NpgsqlE2ETest;" +
            $"Username={RootUser};Password={RootPassword};SSL Mode=Disable");

        await using var conn = await DataSource.OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE DOCUMENT TYPE IF NOT EXISTS NpgsqlTest";
        await cmd.ExecuteNonQueryAsync();
    }

    public async Task DisposeAsync()
    {
        if (DataSource is not null)
            await DataSource.DisposeAsync();
        await _container.DisposeAsync();
    }
}
