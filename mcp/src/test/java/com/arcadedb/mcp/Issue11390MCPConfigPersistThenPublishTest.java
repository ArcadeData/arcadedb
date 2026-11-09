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
package com.arcadedb.mcp;

import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Issue #11390: {@code POST /api/v1/mcp/config} applied the payload to the live configuration, then called a {@code save()}
 * that logged a failed write and returned normally, and answered {@code 200} with the new configuration. On a read-only or full
 * configuration directory the change was in force until the next start, which read the earlier file back: a narrowing
 * ({@code allowAdmin false}, a shorter {@code allowedUsers}, {@code enabled false}) reverted to the broader setting. The update
 * is now persisted first and published only once the write succeeded, and a write that fails answers {@code 500} with the
 * configuration in force unchanged.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11390MCPConfigPersistThenPublishTest extends BaseGraphServerTest {

  @Test
  void aNarrowingThatCannotBeWrittenIsRefusedAndNotApplied() throws Exception {
    final MCPConfiguration live = MCPPlugin.of(getServer(0)).getConfiguration();
    final JSONObject permissive = new JSONObject().put("enabled", true).put("allowAdmin", true)
        .put("allowedUsers", new JSONArray().put("root").put("svc"));
    assertThat(post(permissive).statusCode()).isEqualTo(200);
    final String before = get().body();

    final File configDirectory = Paths.get(getServer(0).getConfigPath()).toFile();
    assumeTrue(configDirectory.setWritable(false), "the configuration directory can be made read-only");
    try {
      assumeTrue(!Files.isWritable(configDirectory.toPath()), "the process cannot bypass a read-only directory");

      final HttpResponse<String> refused = post(
          new JSONObject().put("allowAdmin", false).put("allowedUsers", new JSONArray().put("root")));

      assertThat(refused.statusCode()).as("a configuration that could not be persisted is not reported as saved")
          .isEqualTo(500);
      assertThat(new JSONObject(refused.body()).getString("error", "")).contains("mcp-config.json");
      assertThat(get().body()).as("and it is not in force either").isEqualTo(before);
      assertThat(live.isAllowAdmin()).isTrue();
      assertThat(live.getAllowedUsers()).containsExactly("root", "svc");
    } finally {
      configDirectory.setWritable(true);
    }

    // WRITABLE AGAIN, THE SAME NARROWING IS APPLIED AND IS WHAT A RESTART READS BACK
    assertThat(post(new JSONObject().put("allowAdmin", false).put("allowedUsers", new JSONArray().put("root"))).statusCode())
        .isEqualTo(200);
    final MCPConfiguration restarted = new MCPConfiguration(Paths.get(getServer(0).getConfigPath()));
    restarted.load();
    assertThat(restarted.isAllowAdmin()).isFalse();
    assertThat(restarted.getAllowedUsers()).containsExactly("root");
  }

  @Test
  void updateAndSaveOnAnUnwritableDirectoryLeavesTheLiveConfigurationAsItWas(@TempDir final Path tempDir) {
    final Path configDirectory = tempDir.resolve("config");
    final MCPConfiguration config = new MCPConfiguration(configDirectory);
    config.load();
    config.updateAndSave(new JSONObject().put("enabled", true).put("allowAdmin", true));
    final String before = config.toJSON().toString();

    assumeTrue(configDirectory.toFile().setWritable(false), "the configuration directory can be made read-only");
    try {
      assumeTrue(!Files.isWritable(configDirectory), "the process cannot bypass a read-only directory");

      assertThatThrownBy(() -> config.updateAndSave(new JSONObject().put("allowAdmin", false).put("enabled", false)))
          .isInstanceOf(ConfigurationException.class).hasMessageContaining("mcp-config.json");
      assertThat(config.toJSON().toString()).isEqualTo(before);

      assertThatThrownBy(config::save).as("save() reports the failed write instead of logging it away")
          .isInstanceOf(ConfigurationException.class);
    } finally {
      configDirectory.toFile().setWritable(true);
    }

    final MCPConfiguration restarted = new MCPConfiguration(configDirectory);
    restarted.load();
    assertThat(restarted.toJSON().toString()).isEqualTo(before);
  }

  /** A server on a read-only configuration directory still starts: the first-run write of the defaults is best effort. */
  @Test
  void aFirstRunOnAnUnwritableDirectoryStillLoadsTheDefaults(@TempDir final Path tempDir) throws Exception {
    final Path configDirectory = tempDir.resolve("config");
    Files.createDirectories(configDirectory);
    assumeTrue(configDirectory.toFile().setWritable(false), "the configuration directory can be made read-only");
    try {
      assumeTrue(!Files.isWritable(configDirectory), "the process cannot bypass a read-only directory");

      final MCPConfiguration config = new MCPConfiguration(configDirectory);
      config.load();

      assertThat(config.isEnabled()).isFalse();
      assertThat(config.getAllowedUsers()).containsExactly("root");
      assertThat(configDirectory.resolve("mcp-config.json")).doesNotExist();
    } finally {
      configDirectory.toFile().setWritable(true);
    }
  }

  private HttpResponse<String> get() throws Exception {
    return send(request().GET().build());
  }

  private HttpResponse<String> post(final JSONObject payload) throws Exception {
    return send(request().header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(payload.toString(), StandardCharsets.UTF_8)).build());
  }

  private HttpRequest.Builder request() throws Exception {
    return HttpRequest.newBuilder(new URI(getServerHttpUrl(0, "/api/v1/mcp/config"))).header("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
  }

  private static HttpResponse<String> send(final HttpRequest request) throws Exception {
    try (final HttpClient client = HttpClient.newHttpClient()) {
      return client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    }
  }
}
