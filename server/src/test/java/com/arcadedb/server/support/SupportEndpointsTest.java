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
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server.support;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The support endpoints against a real server and a local mock of the customer portal: authorisation, registration, the
 * preview, what is sent to the portal, the error mapping and, above all, that the Client key is in no response.
 */
class SupportEndpointsTest extends BaseGraphServerTest {
  private static MockPortal portal;

  private final List<String> everyBody = new ArrayList<>();

  private record Resp(int status, String body, Map<String, String> headers, byte[] bytes) {
    JSONObject json() {
      return new JSONObject(body);
    }
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    try {
      if (portal == null)
        portal = new MockPortal();
    } catch (final IOException e) {
      throw new IllegalStateException(e);
    }
    config.setValue(GlobalConfiguration.SUPPORT_URL, portal.url());
  }

  @AfterEach
  void cleanUp() throws Exception {
    // support.json lives in the configuration directory of the test server
    if (getServer(0) != null) {
      getServer(0).getConfiguration().setValue(GlobalConfiguration.SUPPORT_CLIENT_ID, "");
      getServer(0).getConfiguration().setValue(GlobalConfiguration.SUPPORT_CLIENT_KEY, "");
      call("DELETE", "/api/v1/server/support/register", null, "root", DEFAULT_PASSWORD_FOR_TESTS);
    }
    portal.close();
    portal = null;
  }

  private SupportService service() {
    return getServer(0).getSupportService();
  }

  private Resp call(final String method, final String path, final String body) throws Exception {
    return call(method, path, body, "root", DEFAULT_PASSWORD_FOR_TESTS);
  }

  private Resp call(final String method, final String path, final String body, final String user, final String password)
      throws Exception {
    final int port = getServer(0).getHttpServer().getPort();
    final HttpURLConnection connection = (HttpURLConnection) new URI("http://localhost:" + port + path).toURL().openConnection();
    connection.setRequestMethod(method);
    if (user != null)
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8)));
    try {
      if (body != null) {
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setDoOutput(true);
        connection.getOutputStream().write(body.getBytes(StandardCharsets.UTF_8));
      }
      final int status = connection.getResponseCode();
      final InputStream stream = status < 400 ? connection.getInputStream() : connection.getErrorStream();
      final byte[] bytes = stream == null ? new byte[0] : stream.readAllBytes();
      final Map<String, String> headers = new HashMap<>();
      connection.getHeaderFields().forEach((k, v) -> {
        if (k != null)
          headers.put(k.toLowerCase(), v.get(0));
      });
      final String text = "application/zip".equals(headers.get("content-type")) ? "<zip>" : new String(bytes, StandardCharsets.UTF_8);
      everyBody.add(text);
      return new Resp(status, text, headers, bytes);
    } finally {
      connection.disconnect();
    }
  }

  private Resp register() throws Exception {
    return call("POST", "/api/v1/server/support/register",
        new JSONObject().put("clientId", MockPortal.CLIENT_ID).put("key", MockPortal.KEY).toString());
  }

  private void assertKeyNeverServed() {
    for (final String body : everyBody)
      assertThat(body).doesNotContain(MockPortal.KEY).doesNotContain("wsk_MOCKKEY");
  }

  @Test
  void verifyOnlyChecksWithThePortalWithoutStoring() throws Exception {
    final Resp ok = call("POST", "/api/v1/server/support/register",
        new JSONObject().put("clientId", MockPortal.CLIENT_ID).put("key", MockPortal.KEY).put("verifyOnly", true).toString());
    assertThat(ok.status()).isEqualTo(200);
    assertThat(ok.json().getBoolean("verified")).isTrue();
    assertThat(ok.json().getString("workspaceName")).isEqualTo("Acme Corp");
    assertThat(ok.json().getJSONObject("plan").getBoolean("entitled")).isTrue();
    assertThat(ok.json().getString("keyHint")).startsWith("…");
    assertThat(service().getConfiguration().get()).isNull();
    assertThat(Path.of(getServer(0).getConfigPath(), "support.json")).doesNotExist();

    final Resp bad = call("POST", "/api/v1/server/support/register",
        new JSONObject().put("clientId", MockPortal.CLIENT_ID).put("key", "wsk_thisIsNotTheKey0000000000000000000000000000").put("verifyOnly", true)
            .toString());
    assertThat(bad.status()).isEqualTo(400);
    assertThat(bad.json().getString("error")).isEqualTo("invalid_key");
    assertKeyNeverServed();
  }

  @Test
  void anExpiredPlanIsShownByTheStatus() throws Exception {
    portal.handler = r -> new MockPortal.Response(200, """
        {"workspace":{"id":"ws-mock-1","name":"Acme Corp"},"key":{"id":"k1","label":"Studio: prod","scopes":["support:read"]},\
        "plan":{"entitled":false,"label":"Gold","units":1,"endsOn":1600000000000},"sla":null,"buyUrl":"https://arcadedb.com/pricing.html"}""");
    final JSONObject status = register().json();
    assertThat(status.getJSONObject("plan").getBoolean("entitled")).isFalse();
    assertThat(status.isNull("sla")).isTrue();
    assertThat(status.getJSONArray("scopes").toList()).containsExactly("support:read");
  }

  @Test
  void onlyTheRootUserIsAuthorised() throws Exception {
    final String[][] routes = { { "GET", "/api/v1/server/support" }, { "POST", "/api/v1/server/support/register" },
        { "DELETE", "/api/v1/server/support/register" }, { "POST", "/api/v1/server/support/preview" },
        { "POST", "/api/v1/server/support/issues" }, { "GET", "/api/v1/server/support/issues" },
        { "GET", "/api/v1/server/support/issues/1" }, { "PUT", "/api/v1/server/support/issues/1" },
        { "POST", "/api/v1/server/support/issues/1/comments" }, { "POST", "/api/v1/server/support/issues/1/attachments" },
        { "POST", "/api/v1/server/support/bundle" } };

    // a user that is not root
    assertThat(call("POST", "/api/v1/server/users", new JSONObject().put("name", "bob").put("password", "bobs-password-1234").toString())
        .status()).isLessThan(300);
    try {
      for (final String[] route : routes) {
        final String body = route[0].equals("GET") || route[0].equals("DELETE") ? null : "{}";
        assertThat(call(route[0], route[1], body, null, null).status()).as("anonymous " + route[0] + " " + route[1]).isEqualTo(401);
        assertThat(call(route[0], route[1], body, "bob", "bobs-password-1234").status()).as("bob " + route[0] + " " + route[1])
            .isEqualTo(403);
      }
    } finally {
      call("DELETE", "/api/v1/server/users?name=bob", null);
    }
    // nothing reached the portal
    assertThat(portal.requests).isEmpty();
  }

  @Test
  void statusWhenNotRegistered() throws Exception {
    final Resp resp = call("GET", "/api/v1/server/support", null);
    assertThat(resp.status()).isEqualTo(200);
    final JSONObject status = resp.json();
    assertThat(status.getBoolean("registered")).isFalse();
    assertThat(status.getString("portalUrl")).isEqualTo(portal.url());
    assertThat(status.getBoolean("canWriteConfig")).isTrue();
    assertThat(status.getString("instanceId")).isEqualTo(getServer(0).getInstanceId()).startsWith("adb-");
    assertThat(status.getString("buyUrl")).isEqualTo("https://arcadedb.com/pricing.html");
    assertThat(status.getJSONObject("logTimeZone").getString("id")).isEqualTo(ZoneId.systemDefault().getId());
    assertThat(status.has("keyHint")).isFalse();
    assertThat(portal.requests).isEmpty();
  }

  @Test
  void registrationWithTheWrongKeyIsRefusedAndNothingIsStored() throws Exception {
    final Resp resp = call("POST", "/api/v1/server/support/register",
        new JSONObject().put("clientId", MockPortal.CLIENT_ID).put("key", "wsk_thisIsNotTheKey0000000000000000000000000000").toString());
    // never 401: Studio would log the user out
    assertThat(resp.status()).isEqualTo(400);
    assertThat(resp.json().getString("error")).isEqualTo("invalid_key");
    assertThat(resp.json().getString("message")).contains("rejected the Client key");
    assertThat(service().getConfiguration().get()).isNull();
    assertThat(Path.of(getServer(0).getConfigPath(), "support.json")).doesNotExist();

    final Resp mismatch = call("POST", "/api/v1/server/support/register",
        new JSONObject().put("clientId", "another-workspace").put("key", MockPortal.KEY).toString());
    assertThat(mismatch.status()).isEqualTo(400);
    assertThat(mismatch.json().getString("error")).isEqualTo("client_mismatch");

    assertThat(call("POST", "/api/v1/server/support/register", new JSONObject().put("clientId", "").put("key", "").toString()).status())
        .isEqualTo(400);
    assertThat(call("POST", "/api/v1/server/support/register", null).status()).isEqualTo(400);
  }

  @Test
  void registerStatusAndUnregister() throws Exception {
    final Resp registered = register();
    assertThat(registered.status()).isEqualTo(200);
    final JSONObject status = registered.json();
    assertThat(status.getBoolean("registered")).isTrue();
    assertThat(status.getString("clientId")).isEqualTo(MockPortal.CLIENT_ID);
    assertThat(status.getString("keyHint")).isEqualTo("…" + MockPortal.KEY.substring(MockPortal.KEY.length() - 4));
    assertThat(status.getString("workspaceName")).isEqualTo("Acme Corp");
    assertThat(status.getJSONObject("plan").getBoolean("entitled")).isTrue();
    assertThat(status.getJSONObject("plan").getString("label")).isEqualTo("Gold");
    assertThat(status.getJSONObject("sla").getString("S1")).isEqualTo("1 hour");
    assertThat(status.getString("keyLabel")).isEqualTo("Studio: prod");
    assertThat(status.getBoolean("fromSettings")).isFalse();

    // the contract headers reached the portal, with the instance id of this server
    final MockPortal.Recorded whoami = portal.requests.get(0);
    assertThat(whoami.path()).isEqualTo("/api/v1/support/whoami");
    assertThat(whoami.header("X-Instance-Id")).isEqualTo(getServer(0).getInstanceId());
    assertThat(whoami.header("Authorization")).isEqualTo("Bearer " + MockPortal.KEY);

    final Path file = Path.of(getServer(0).getConfigPath(), "support.json");
    assertThat(file).exists();
    assertThat(new JSONObject(Files.readString(file)).getString("key")).isEqualTo(MockPortal.KEY);

    final JSONObject again = call("GET", "/api/v1/server/support", null).json();
    assertThat(again.getBoolean("registered")).isTrue();
    assertThat(again.has("key")).isFalse();

    assertThat(call("DELETE", "/api/v1/server/support/register", null).status()).isEqualTo(204);
    assertThat(file).doesNotExist();
    assertThat(call("GET", "/api/v1/server/support", null).json().getBoolean("registered")).isFalse();

    assertKeyNeverServed();
  }

  @Test
  void aRegistrationFromTheSettingsCannotBeRemovedFromStudio() throws Exception {
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SUPPORT_CLIENT_ID, MockPortal.CLIENT_ID);
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SUPPORT_CLIENT_KEY, MockPortal.KEY);

    final JSONObject status = call("GET", "/api/v1/server/support", null).json();
    assertThat(status.getBoolean("registered")).isTrue();
    assertThat(status.getBoolean("fromSettings")).isTrue();
    assertThat(status.getString("workspaceName")).isEqualTo("Acme Corp");

    final Resp delete = call("DELETE", "/api/v1/server/support/register", null);
    assertThat(delete.status()).isEqualTo(409);
    assertThat(delete.json().getString("error")).isEqualTo("registered_by_settings");
    assertThat(register().status()).isEqualTo(409);

    // and the key setting is masked in the settings listing of the server
    final JSONArray settings = call("GET", "/api/v1/server?mode=default", null).json().getJSONArray("settings");
    boolean found = false;
    for (int i = 0; i < settings.length(); i++)
      if (settings.getJSONObject(i).getString("key").equals("arcadedb.support.clientKey")) {
        found = true;
        assertThat(settings.getJSONObject(i).get("value")).isEqualTo("*****");
      }
    assertThat(found).isTrue();

    // set through the settings, the key is a masked setting of the diagnostics too
    final JSONObject preview = call("POST", "/api/v1/server/support/preview", new JSONObject().put("includeDiagnostics", true).toString()).json();
    final Map<String, String> zip = unzip(call("POST", "/api/v1/server/support/bundle",
        new JSONObject().put("previewId", preview.getString("previewId")).toString()).bytes());
    assertThat(zip.get("diagnostics.json")).doesNotContain(MockPortal.KEY).doesNotContain("wsk_MOCKKEY");
    assertThat(new JSONObject(zip.get("diagnostics.json")).getJSONObject("configuration").getJSONArray("masked").toList())
        .contains("arcadedb.support.clientKey");
    assertKeyNeverServed();
  }

  @Test
  void previewOfTheDiagnosticsFollowsTheV1Schema() throws Exception {
    final Resp preview = call("POST", "/api/v1/server/support/preview",
        new JSONObject().put("includeDiagnostics", true).put("includeThreads", true).toString());
    assertThat(preview.status()).isEqualTo(200);
    final JSONObject json = preview.json();
    assertThat(json.getString("previewId")).hasSize(32);
    assertThat(json.getString("expiresAt")).isNotEmpty();
    assertThat(json.getJSONObject("logTimeZone").getString("id")).isEqualTo(ZoneId.systemDefault().getId());

    final JSONArray files = json.getJSONArray("files");
    assertThat(files.length()).isEqualTo(2);
    final Map<String, JSONObject> byName = new HashMap<>();
    for (int i = 0; i < files.length(); i++)
      byName.put(files.getJSONObject(i).getString("name"), files.getJSONObject(i));
    assertThat(byName).containsKeys("diagnostics.json", "threads.txt");
    assertThat(byName.get("diagnostics.json").getLong("sizeBytes")).isGreaterThan(100);
    assertThat(byName.get("diagnostics.json").getLong("lines")).isGreaterThan(10);
    assertThat(byName.get("diagnostics.json").getInt("redactions")).isGreaterThanOrEqualTo(0);
    assertThat(json.getString("githubSummary")).contains("Environment").contains("ArcadeDB:");

    final Resp download = call("POST", "/api/v1/server/support/bundle", new JSONObject().put("previewId", json.getString("previewId")).toString());
    assertThat(download.status()).isEqualTo(200);
    assertThat(download.headers().get("content-type")).isEqualTo("application/zip");
    assertThat(download.headers().get("content-disposition")).startsWith("attachment; filename=\"arcadedb-support-");
    assertThat(Integer.parseInt(download.headers().get("content-length"))).isEqualTo(download.bytes().length);

    final Map<String, String> zip = unzip(download.bytes());
    assertThat(zip).containsKeys("diagnostics.json", "threads.txt").doesNotContainKey("summary.json");

    final JSONObject diagnostics = new JSONObject(zip.get("diagnostics.json"));
    assertThat(diagnostics.getInt("schema")).isEqualTo(1);
    assertThat(diagnostics.getString("generatedAt")).endsWith("Z");
    assertThat(diagnostics.getString("instanceId")).isEqualTo(getServer(0).getInstanceId());
    final JSONObject server = diagnostics.getJSONObject("server");
    assertThat(server.getString("version")).isNotEmpty();
    assertThat(server.getString("name")).isEqualTo(getServer(0).getServerName());
    assertThat(server.getLong("uptimeSeconds")).isGreaterThanOrEqualTo(0);
    assertThat(server.getString("startedAt")).isNotEmpty();
    final JSONObject os = diagnostics.getJSONObject("os");
    assertThat(os.getString("name")).isNotEmpty();
    assertThat(os.getInt("cpuCores")).isGreaterThan(0);
    final JSONObject jvm = diagnostics.getJSONObject("jvm");
    assertThat(jvm.getLong("maxHeapBytes")).isGreaterThan(0);
    assertThat(jvm.getJSONArray("inputArguments")).isNotNull();
    assertThat(jvm.getJSONArray("gc").length()).isGreaterThan(0);
    assertThat(diagnostics.getJSONObject("runtime").getString("container")).isIn("none", "docker", "kubernetes", "unknown");
    final JSONObject configuration = diagnostics.getJSONObject("configuration");
    assertThat(configuration.getJSONArray("nonDefault")).isNotNull();
    // the root password of the test server is set, so it is listed as masked and its value is nowhere
    assertThat(configuration.getJSONArray("masked").toList()).contains("arcadedb.server.rootPassword");
    for (final Object entry : configuration.getJSONArray("nonDefault").toList())
      assertThat(String.valueOf(entry)).doesNotContain("rootPassword");
    final List<String> plugins = diagnostics.getJSONArray("plugins").toListOfStrings();
    assertThat(List.of("gremlin", "postgresql", "mongodb", "redis", "grpc", "bolt", "graphql", "mcp")).containsAll(plugins);
    final JSONArray databases = diagnostics.getJSONArray("databases");
    boolean hasTestDatabase = false;
    for (int i = 0; i < databases.length(); i++) {
      final JSONObject db = databases.getJSONObject(i);
      assertThat(db.keySet()).isSubsetOf("name", "sizeBytes", "mode");
      hasTestDatabase |= db.getString("name").equals(getDatabaseName());
    }
    assertThat(hasTestDatabase).isTrue();
    assertThat(diagnostics.getJSONObject("ha").has("enabled")).isTrue();
    assertThat(diagnostics.getJSONObject("metrics").getInt("threads")).isGreaterThan(0);

    // the thread dump is a dump
    assertThat(zip.get("threads.txt")).contains("Thread dump of").contains("RUNNABLE");

    // no secret anywhere in the bundle
    for (final String content : zip.values())
      assertThat(content).doesNotContain(DEFAULT_PASSWORD_FOR_TESTS);
    // and no user names or hashes: only the keys of the whitelist are at the top level
    assertThat(diagnostics.keySet()).isSubsetOf("schema", "generatedAt", "instanceId", "server", "os", "jvm", "runtime",
        "configuration", "plugins", "databases", "ha", "metrics");
  }

  @Test
  void previewWithLogsRedactsCountsAndSummarises() throws Exception {
    final Path logDirectory = Files.createTempDirectory("support-logs");
    try {
      final DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
      final LocalDateTime now = LocalDateTime.now();
      final Path log = logDirectory.resolve("arcadedb.log");
      Files.writeString(log, "\n" + format.format(now.minusMinutes(3)) + " INFO  [Server] Started with -Darcadedb.server.rootPassword=SuperSecret1\n"
          + format.format(now.minusMinutes(2)) + " SEVER [Http] Request failed url=https://admin:pw123@host/x\n"
          + "java.lang.IllegalStateException: boom token=abc\n\tat com.arcadedb.X.y(X.java:1)\n"
          + format.format(now.minusDays(2)) + " INFO  [Old] way before the window\n");
      service().setLogFiles(() -> List.of(log));

      final JSONObject json = call("POST", "/api/v1/server/support/preview", new JSONObject().put("includeLogs", true)
          .put("window", new JSONObject().put("preset", "10m")).put("includeDiagnostics", false).toString()).json();

      final JSONArray files = json.getJSONArray("files");
      final Map<String, JSONObject> byName = new HashMap<>();
      for (int i = 0; i < files.length(); i++)
        byName.put(files.getJSONObject(i).getString("name"), files.getJSONObject(i));
      assertThat(byName).containsKeys("logs.zip", "summary.json").doesNotContainKey("diagnostics.json");
      assertThat(byName.get("logs.zip").getLong("lines")).isEqualTo(4);
      assertThat(byName.get("logs.zip").getInt("redactions")).isEqualTo(3);
      assertThat(byName.get("logs.zip").getJSONArray("entries").length()).isEqualTo(1);
      assertThat(json.getJSONObject("window").getString("from")).isNotEmpty();
      // The public GitHub text names the exception class and its count, never its message (it may carry record data or SQL)
      assertThat(json.getString("githubSummary")).contains("IllegalStateException").doesNotContain("boom");

      final Resp download = call("POST", "/api/v1/server/support/bundle", new JSONObject().put("previewId", json.getString("previewId")).toString());
      final Map<String, String> zip = unzip(download.bytes());
      assertThat(zip).containsKeys("logs/arcadedb.log", "summary.json");
      assertThat(zip.get("logs/arcadedb.log")).doesNotContain("SuperSecret1").doesNotContain("pw123").doesNotContain("token=abc")
          .contains("rootPassword=***").contains("admin:***@host").doesNotContain("way before");
      // ... while the summary sent to the portal keeps the (redacted) message
      assertThat(zip.get("summary.json")).contains("boom");
    } finally {
      Files.deleteIfExists(logDirectory.resolve("arcadedb.log"));
      Files.deleteIfExists(logDirectory);
    }
  }

  @Test
  void aWindowWithoutLinesIsReportedNotAnError() throws Exception {
    final Path logDirectory = Files.createTempDirectory("support-logs");
    try {
      service().setLogFiles(() -> List.of());
      final Resp resp = call("POST", "/api/v1/server/support/preview", new JSONObject().put("includeLogs", true)
          .put("window", new JSONObject().put("preset", "1h")).put("includeDiagnostics", true).toString());
      assertThat(resp.status()).isEqualTo(200);
      final JSONObject json = resp.json();
      assertThat(json.getJSONArray("warnings").toList()).anyMatch(w -> String.valueOf(w).contains("No log lines"));
      assertThat(json.getJSONArray("files").length()).isEqualTo(1);
    } finally {
      Files.deleteIfExists(logDirectory);
    }
  }

  @Test
  void aWindowOverTheCapIsRefusedWithoutTruncating() throws Exception {
    final Path logDirectory = Files.createTempDirectory("support-logs");
    final Path log = logDirectory.resolve("arcadedb.log");
    try {
      final DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
      final StringBuilder content = new StringBuilder("\n");
      final java.util.Random random = new java.util.Random(1);
      for (int i = 0; i < 3000; i++) {
        final byte[] noise = new byte[150];
        random.nextBytes(noise);
        content.append(format.format(LocalDateTime.now().minusMinutes(1))).append(" INFO  [A] ").append(Base64.getEncoder().encodeToString(noise))
            .append('\n');
      }
      Files.writeString(log, content);
      service().setLogFiles(() -> List.of(log));
      service().setMaxZipBytes(20_000);

      final Resp resp = call("POST", "/api/v1/server/support/preview",
          new JSONObject().put("includeLogs", true).put("window", new JSONObject().put("preset", "10m")).toString());
      assertThat(resp.status()).isEqualTo(413);
      assertThat(resp.json().getString("error")).isEqualTo("bundle_too_large");
      assertThat(resp.json().getString("message")).contains("Narrow the time window");
      assertThat(service().getBundles().size()).isZero();
    } finally {
      service().setMaxZipBytes(SupportLogCollector.DEFAULT_MAX_ZIP_BYTES);
      Files.deleteIfExists(log);
      Files.deleteIfExists(logDirectory);
    }
  }

  @Test
  void invalidPreviewRequests() throws Exception {
    assertThat(call("POST", "/api/v1/server/support/preview", new JSONObject().put("includeDiagnostics", false).toString()).status())
        .isEqualTo(400);
    assertThat(call("POST", "/api/v1/server/support/preview", new JSONObject().put("includeLogs", true).toString()).status()).isEqualTo(400);
    assertThat(call("POST", "/api/v1/server/support/preview", new JSONObject().put("includeLogs", true)
        .put("window", new JSONObject().put("preset", "3d")).toString()).status()).isEqualTo(400);
    assertThat(call("POST", "/api/v1/server/support/preview", new JSONObject().put("includeLogs", true)
        .put("window", new JSONObject().put("from", "2026-01-02T00:00:00Z").put("to", "2026-01-01T00:00:00Z")).toString()).status())
        .isEqualTo(400);
    assertThat(call("POST", "/api/v1/server/support/bundle", new JSONObject().put("previewId", "nope").toString()).status()).isEqualTo(404);
  }

  @Test
  void createIssueSendsExactlyThePreviewedFiles() throws Exception {
    register();
    portal.requests.clear();

    final JSONObject preview = call("POST", "/api/v1/server/support/preview",
        new JSONObject().put("includeDiagnostics", true).put("includeThreads", true).toString()).json();
    final Resp created = call("POST", "/api/v1/server/support/issues", new JSONObject().put("previewId", preview.getString("previewId"))
        .put("title", "Slow queries").put("body", "since yesterday").put("severity", "S2").put("kind", "performance").toString());
    assertThat(created.status()).isEqualTo(201);
    assertThat(created.json().getInt("number")).isEqualTo(42);
    assertThat(created.json().getString("url")).endsWith("/#/issues/42");

    final MockPortal.Recorded r = portal.last();
    assertThat(r.method()).isEqualTo("POST");
    assertThat(r.path()).isEqualTo("/api/v1/support/issues");
    assertThat(r.header("X-Instance-Id")).isEqualTo(getServer(0).getInstanceId());
    final String body = r.bodyText();
    assertThat(body).contains("\"title\":\"Slow queries\"").contains("\"severity\":\"S2\"").contains("\"kind\":\"performance\"")
        .contains("\"source\":\"studio\"").contains("\"studioVersion\"");
    assertThat(body).contains("filename=\"diagnostics.json\"").contains("filename=\"threads.txt\"").doesNotContain("filename=\"logs.zip\"");
    assertThat(body).contains("\"schema\": 1");

    // the preview was consumed by the send
    assertThat(call("POST", "/api/v1/server/support/bundle", new JSONObject().put("previewId", preview.getString("previewId")).toString())
        .status()).isEqualTo(404);
    assertKeyNeverServed();
  }

  @Test
  void createIssueValidatesAndNeedsARegistration() throws Exception {
    final String issue = new JSONObject().put("title", "t").put("body", "b").put("severity", "S3").toString();
    final Resp notRegistered = call("POST", "/api/v1/server/support/issues", issue);
    assertThat(notRegistered.status()).isEqualTo(409);
    assertThat(notRegistered.json().getString("error")).isEqualTo("not_registered");

    register();
    assertThat(call("POST", "/api/v1/server/support/issues", new JSONObject().put("title", "").put("severity", "S3").toString()).status())
        .isEqualTo(400);
    assertThat(call("POST", "/api/v1/server/support/issues", new JSONObject().put("title", "x".repeat(201)).put("severity", "S3").toString())
        .status()).isEqualTo(400);
    assertThat(call("POST", "/api/v1/server/support/issues", new JSONObject().put("title", "t").put("severity", "S9").toString()).status())
        .isEqualTo(400);
    assertThat(call("POST", "/api/v1/server/support/issues", new JSONObject().put("title", "t").put("severity", "S1").put("kind", "rant")
        .toString()).status()).isEqualTo(400);
    assertThat(call("POST", "/api/v1/server/support/issues", new JSONObject().put("title", "t").put("severity", "S1").put("previewId", "gone")
        .toString()).status()).isEqualTo(404);
    // none of them reached the portal
    assertThat(portal.requests.stream().filter(r -> r.method().equals("POST"))).isEmpty();
  }

  @Test
  void issuesAreProxiedWithoutTheKey() throws Exception {
    register();

    final Resp list = call("GET", "/api/v1/server/support/issues?status=all", null);
    assertThat(list.status()).isEqualTo(200);
    assertThat(new JSONArray(list.body()).getJSONObject(0).getInt("number")).isEqualTo(42);
    assertThat(portal.last().query()).isEqualTo("status=all");

    assertThat(call("GET", "/api/v1/server/support/issues/42", null).json().getInt("number")).isEqualTo(42);
    assertThat(call("GET", "/api/v1/server/support/issues/abc", null).status()).isEqualTo(400);

    final Resp comment = call("POST", "/api/v1/server/support/issues/42/comments", new JSONObject().put("body", "thanks").toString());
    assertThat(comment.status()).isEqualTo(201);
    assertThat(new JSONObject(portal.last().bodyText()).getString("body")).isEqualTo("thanks");
    assertThat(call("POST", "/api/v1/server/support/issues/42/comments", new JSONObject().put("body", " ").toString()).status()).isEqualTo(400);

    assertThat(call("PUT", "/api/v1/server/support/issues/42", new JSONObject().put("open", false).toString()).status()).isEqualTo(204);
    assertThat(new JSONObject(portal.last().bodyText()).getBoolean("open")).isFalse();
    assertThat(call("PUT", "/api/v1/server/support/issues/42", "{}").status()).isEqualTo(400);

    final JSONObject preview = call("POST", "/api/v1/server/support/preview", new JSONObject().put("includeDiagnostics", true).toString()).json();
    final Resp attach = call("POST", "/api/v1/server/support/issues/42/attachments",
        new JSONObject().put("previewId", preview.getString("previewId")).toString());
    assertThat(attach.status()).isEqualTo(200);
    assertThat(portal.last().path()).isEqualTo("/api/v1/support/issues/42/attachments");

    assertKeyNeverServed();
  }

  @Test
  void portalFailuresAreMappedAndNeverBecome401Or403() throws Exception {
    register();

    portal.handler = r -> new MockPortal.Response(402, MockPortal.error("support_not_active", "plan lapsed"));
    Resp resp = call("GET", "/api/v1/server/support/issues", null);
    assertThat(resp.status()).isEqualTo(402);
    assertThat(resp.json().getString("error")).isEqualTo("support_not_active");
    assertThat(resp.json().getString("message")).contains("not active");

    portal.handler = r -> new MockPortal.Response(401, MockPortal.error("invalid_key", "revoked " + MockPortal.KEY));
    resp = call("GET", "/api/v1/server/support/issues", null);
    assertThat(resp.status()).isEqualTo(502);
    assertThat(resp.json().getString("error")).isEqualTo("invalid_key");

    portal.handler = r -> new MockPortal.Response(403, MockPortal.error("scope_denied", "x"));
    resp = call("POST", "/api/v1/server/support/issues/42/comments", new JSONObject().put("body", "x").toString());
    assertThat(resp.status()).isEqualTo(502);
    assertThat(resp.json().getString("error")).isEqualTo("scope_denied");

    portal.handler = r -> new MockPortal.Response(429, MockPortal.error("rate_limited", "x"), Map.of("Retry-After", "90"));
    resp = call("GET", "/api/v1/server/support/issues/42", null);
    assertThat(resp.status()).isEqualTo(429);
    assertThat(resp.headers().get("retry-after")).isEqualTo("90");
    assertThat(resp.json().getInt("retryAfterSeconds")).isEqualTo(90);

    portal.handler = r -> new MockPortal.Response(404, MockPortal.error("not_found", "x"));
    assertThat(call("GET", "/api/v1/server/support/issues/999", null).status()).isEqualTo(404);

    portal.handler = r -> new MockPortal.Response(413, MockPortal.error("too_large", "x"));
    final JSONObject preview = call("POST", "/api/v1/server/support/preview", new JSONObject().put("includeDiagnostics", true).toString()).json();
    resp = call("POST", "/api/v1/server/support/issues", new JSONObject().put("previewId", preview.getString("previewId")).put("title", "t")
        .put("severity", "S3").toString());
    assertThat(resp.status()).isEqualTo(413);
    assertThat(resp.json().getString("error")).isEqualTo("too_large");
    // the preview is kept, so the user can send it again once the portal accepts it
    assertThat(call("POST", "/api/v1/server/support/bundle", new JSONObject().put("previewId", preview.getString("previewId")).toString())
        .status()).isEqualTo(200);

    assertKeyNeverServed();
  }

  @Test
  void anUnreachablePortalShowsOnTheStatusAndAs503() throws Exception {
    register();
    portal.close();
    try {
      final Resp status = call("GET", "/api/v1/server/support?refresh=true", null);
      assertThat(status.status()).isEqualTo(200);
      assertThat(status.json().getBoolean("registered")).isTrue();
      assertThat(status.json().getJSONObject("portalError").getString("error")).isEqualTo("portal_unreachable");
      assertThat(status.json().getString("keyHint")).startsWith("…");

      final Resp resp = call("GET", "/api/v1/server/support/issues", null);
      assertThat(resp.status()).isEqualTo(503);
      assertThat(resp.json().getString("error")).isEqualTo("portal_unreachable");
      assertKeyNeverServed();
    } finally {
      portal = new MockPortal();
    }
  }

  @Test
  void twoConcurrentDownloadsOfOnePreviewBothSucceed() throws Exception {
    register();
    final JSONObject preview = call("POST", "/api/v1/server/support/preview", new JSONObject().put("includeDiagnostics", true).toString()).json();
    final String body = new JSONObject().put("previewId", preview.getString("previewId")).toString();

    // Both build the zip of the same preview at once: the second used to fail moving over the first one's file
    final java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newFixedThreadPool(2);
    final java.util.concurrent.CountDownLatch go = new java.util.concurrent.CountDownLatch(1);
    try {
      final List<java.util.concurrent.Future<Resp>> results = new ArrayList<>();
      for (int i = 0; i < 2; i++)
        results.add(pool.submit(() -> {
          go.await();
          return call("POST", "/api/v1/server/support/bundle", body);
        }));
      go.countDown();
      for (final java.util.concurrent.Future<Resp> result : results) {
        final Resp response = result.get(30, java.util.concurrent.TimeUnit.SECONDS);
        assertThat(response.status()).isEqualTo(200);
        assertThat(unzip(response.bytes())).containsKey("diagnostics.json");
      }
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  void aChangedKeyOrPortalNeverReadsTheCachedWhoamiOfAnother() throws Exception {
    register();
    final int before = (int) portal.requests.stream().filter(r -> r.path().endsWith("/whoami")).count();
    assertThat(call("GET", "/api/v1/server/support", null).status()).isEqualTo(200);
    // Served from the cache: no new call
    assertThat(portal.requests.stream().filter(r -> r.path().endsWith("/whoami")).count()).isEqualTo(before);

    // A different key with the same last four characters must not reuse the answer of the first
    final String sameTail = "wsk_DIFFERENT0123456789abcdefghijklmnopq" + MockPortal.KEY.substring(MockPortal.KEY.length() - 4);
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SUPPORT_CLIENT_ID, MockPortal.CLIENT_ID);
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SUPPORT_CLIENT_KEY, sameTail);
    final Resp status = call("GET", "/api/v1/server/support", null);
    assertThat(status.status()).isEqualTo(200);
    assertThat(portal.requests.stream().filter(r -> r.path().endsWith("/whoami")).count()).isGreaterThan(before);
    // the mock portal refuses this key: the status says so instead of showing the workspace of the first
    assertThat(status.json().has("portalError")).isTrue();
  }

  @Test
  void aBadPortalUrlInTheConfigurationIsReportedByTheStatusNotAServerError() throws Exception {
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SUPPORT_CLIENT_ID, MockPortal.CLIENT_ID);
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SUPPORT_CLIENT_KEY, MockPortal.KEY);
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SUPPORT_URL, "http://portal.example.com");
    try {
      final Resp status = call("GET", "/api/v1/server/support?refresh=true", null);
      assertThat(status.status()).isEqualTo(200);
      assertThat(status.json().getBoolean("registered")).isTrue();
      assertThat(status.json().getJSONObject("portalError").getString("error")).isEqualTo("portal_url_invalid");
      assertKeyNeverServed();
    } finally {
      getServer(0).getConfiguration().setValue(GlobalConfiguration.SUPPORT_URL, portal.url());
    }
  }

  @Test
  void onlyOnePreviewIsBuiltAtATime() throws Exception {
    final java.util.concurrent.CountDownLatch scanning = new java.util.concurrent.CountDownLatch(1);
    final java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
    service().setLogFiles(() -> {
      scanning.countDown();
      try {
        release.await(30, java.util.concurrent.TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return List.of();
    });
    final String logs = new JSONObject().put("includeLogs", true).put("includeDiagnostics", false)
        .put("window", new JSONObject().put("preset", "10m")).toString();
    final java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newSingleThreadExecutor();
    try {
      final java.util.concurrent.Future<Resp> first = pool.submit(() -> call("POST", "/api/v1/server/support/preview", logs));
      assertThat(scanning.await(30, java.util.concurrent.TimeUnit.SECONDS)).isTrue();

      // A second scan while the first is running is refused, not queued
      final Resp second = call("POST", "/api/v1/server/support/preview", logs);
      assertThat(second.status()).isEqualTo(409);
      assertThat(second.json().getString("error")).isEqualTo("preview_busy");

      release.countDown();
      assertThat(first.get(30, java.util.concurrent.TimeUnit.SECONDS).status()).isEqualTo(200);
      // and the slot is free again afterwards
      assertThat(call("POST", "/api/v1/server/support/preview", new JSONObject().put("includeDiagnostics", true).toString()).status())
          .isEqualTo(200);
    } finally {
      release.countDown();
      pool.shutdownNow();
    }
  }

  private static Map<String, String> unzip(final byte[] bytes) throws IOException {
    final Map<String, String> result = new HashMap<>();
    try (final ZipInputStream zip = new ZipInputStream(new ByteArrayInputStream(bytes))) {
      ZipEntry entry;
      while ((entry = zip.getNextEntry()) != null)
        result.put(entry.getName(), new String(zip.readAllBytes(), StandardCharsets.UTF_8));
    }
    return result;
  }
}
