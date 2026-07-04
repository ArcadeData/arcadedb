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

    public static async Task<string> BuildAsync(string tagSuffix)
    {
        var certDir = Directory.CreateTempSubdirectory("bolt-tls-certs-").FullName;

        // The whole body is wrapped so certDir is cleaned up on ANY failure
        // path, not just a docker build failure - keytool itself can throw
        // (missing/misconfigured JDK, bad cert params) before a build is ever
        // attempted.
        try
        {
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

            // Honor ARCADEDB_DOCKER_IMAGE like ArcadeDbBoltFixture does - otherwise
            // a developer running against a custom local build would get the
            // default/Postgres suites on their image but the TLS suites silently
            // on arcadedata/arcadedb:latest, a split-suite trap.
            var imageEnv = Environment.GetEnvironmentVariable("ARCADEDB_DOCKER_IMAGE");
            var baseImage = string.IsNullOrWhiteSpace(imageEnv) ? "arcadedata/arcadedb:latest" : imageEnv;
            await File.WriteAllTextAsync(Path.Combine(certDir, "Dockerfile"),
                $"FROM {baseImage}\n" +
                "COPY --chown=arcadedb:arcadedb keystore.p12 truststore.jks /home/arcadedb/tls_certs/\n");

            // The installed Testcontainers 4.11.0 API only accepts a raw string
            // directory (which doubles as the Docker build context when no
            // separate WithContextDirectory is set) or a (CommonDirectoryPath,
            // relativeSubDirectory) pair meant for paths relative to the repo's
            // solution/git root. certDir is already an absolute throwaway temp
            // directory, so the single-string overload is the correct one here.
            //
            // tagSuffix keeps this tag distinct per caller: xUnit v2 parallelizes
            // distinct collections by default (no DisableTestParallelization here),
            // and the TLS-required/TLS-optional fixtures live in separate
            // collections, so both can call BuildAsync concurrently. A shared
            // hardcoded tag would race on which build "wins" the name and would
            // register two Ryuk cleanups against the same image.
            var image = new ImageFromDockerfileBuilder()
                .WithDockerfileDirectory(certDir)
                .WithDockerfile("Dockerfile")
                .WithName($"arcadedb-bolt-tls-test-{tagSuffix}:latest")
                .WithCleanUp(true)
                .Build();
            await image.CreateAsync();
            return image.FullName;
        }
        finally
        {
            Directory.Delete(certDir, recursive: true);
        }
    }

    private static void RunKeytool(params string[] arguments)
    {
        var startInfo = new ProcessStartInfo(FindKeytool())
        {
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
        var imageTag = await BoltTlsImage.BuildAsync("required");

        _container = new ContainerBuilder(imageTag)
            .WithPortBinding(2480, true)
            .WithPortBinding(7687, true)
            .WithEnvironment("JAVA_OPTS",
                $"-Darcadedb.server.rootPassword={ArcadeDbBoltFixture.RootPassword} " +
                "-Darcadedb.server.defaultDatabases=beer[root]{import:https://github.com/ArcadeData/arcadedb-datasets/raw/main/orientdb/OpenBeer.gz} " +
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
        var imageTag = await BoltTlsImage.BuildAsync("optional");

        _container = new ContainerBuilder(imageTag)
            .WithPortBinding(2480, true)
            .WithPortBinding(7687, true)
            .WithEnvironment("JAVA_OPTS",
                $"-Darcadedb.server.rootPassword={ArcadeDbBoltFixture.RootPassword} " +
                "-Darcadedb.server.defaultDatabases=beer[root]{import:https://github.com/ArcadeData/arcadedb-datasets/raw/main/orientdb/OpenBeer.gz} " +
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
