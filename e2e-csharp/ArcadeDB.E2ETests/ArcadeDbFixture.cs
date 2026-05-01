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
        var imageEnv = Environment.GetEnvironmentVariable("ARCADEDB_DOCKER_IMAGE");
        var image = string.IsNullOrWhiteSpace(imageEnv) ? "arcadedata/arcadedb:latest" : imageEnv;

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
        var authHeader = new AuthenticationHeaderValue(
            "Basic", Convert.ToBase64String(Encoding.UTF8.GetBytes($"{RootUser}:{RootPassword}")));

        using var http = new HttpClient();
        http.DefaultRequestHeaders.Authorization = authHeader;
        using var response = await http.PostAsync(
            $"http://{_container.Hostname}:{httpPort}/api/v1/server",
            new StringContent(
                "{\"command\":\"create database NpgsqlE2ETest\"}",
                Encoding.UTF8,
                "application/json"));
        response.EnsureSuccessStatusCode();

        using var createTypeResponse = await http.PostAsync(
            $"http://{_container.Hostname}:{httpPort}/api/v1/command/NpgsqlE2ETest",
            new StringContent(
                "{\"language\":\"sql\",\"command\":\"CREATE DOCUMENT TYPE NpgsqlTest\"}",
                Encoding.UTF8,
                "application/json"));
        createTypeResponse.EnsureSuccessStatusCode();

        var pgPort = _container.GetMappedPublicPort(5432);
        DataSource = NpgsqlDataSource.Create(
            $"Host={_container.Hostname};Port={pgPort};Database=NpgsqlE2ETest;" +
            $"Username={RootUser};Password={RootPassword};SSL Mode=Disable;" +
            "Server Compatibility Mode=NoTypeLoading;No Reset On Close=true");
    }

    public async Task DisposeAsync()
    {
        if (DataSource is not null)
            await DataSource.DisposeAsync();
        if (_container is not null)
            await _container.DisposeAsync();
    }
}
