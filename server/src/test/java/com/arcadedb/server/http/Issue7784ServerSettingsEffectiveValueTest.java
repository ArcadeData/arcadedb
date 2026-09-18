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
package com.arcadedb.server.http;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7784: {@code GET /api/v1/server} published every SCOPE.SERVER setting's value off the process-wide
 * {@code GlobalConfiguration} enum while computing {@code overridden} from the server's own
 * {@link ContextConfiguration}. The two disagree by construction - {@code ContextConfiguration.setValue} writes
 * the overlay and never touches the enum - so every setting the configuration file, the embedding application or
 * {@code SET SERVER SETTING} overrode was rendered at the enum's value next to {@code "overridden": true}, and
 * equal to {@code "default"}: a response that contradicts itself on its own terms, on the very page an operator
 * opens to confirm a live change.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7784ServerSettingsEffectiveValueTest extends BaseGraphServerTest {

  private static final int STARTUP_OVERRIDE = 4242;
  private static final int LIVE_OVERRIDE    = 777;

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    config.setValue(GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS, STARTUP_OVERRIDE);
  }

  @Test
  void theEndpointReportsTheValueTheServerActuallyRunsOn() throws Exception {
    final String key = GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS.getKey();
    final Object defaultValue = GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS.getDefValue();

    assertThat(getServer(0).getConfiguration()
        .getValueAsInteger(GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS))
        .as("precondition: the server runs on the overlay's value")
        .isEqualTo(STARTUP_OVERRIDE);
    assertThat(((Number) defaultValue).intValue())
        .as("precondition: the override must differ from the declared default, or the test proves nothing")
        .isNotEqualTo(STARTUP_OVERRIDE);

    JSONObject setting = settingFromEndpoint(key);
    assertThat(setting.getBoolean("overridden")).isTrue();
    assertThat(setting.getInt("value")).as("the reported value is the effective one").isEqualTo(STARTUP_OVERRIDE);
    assertThat(setting.getInt("default")).as("'default' keeps reporting the enum's declared default")
        .isEqualTo(((Number) defaultValue).intValue());

    // Now change it live, the way an operator does, and read the endpoint back.
    setServerSetting(key, String.valueOf(LIVE_OVERRIDE));
    assertThat(getServer(0).getConfiguration()
        .getValueAsInteger(GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS)).isEqualTo(LIVE_OVERRIDE);

    setting = settingFromEndpoint(key);
    assertThat(setting.getInt("value")).as("a live change must be visible where the operator confirms it")
        .isEqualTo(LIVE_OVERRIDE);
    assertThat(setting.getBoolean("overridden")).isTrue();
  }

  /**
   * The endpoint always redacted these credentials; the guard is here because the routine that does it moved to
   * {@code GlobalConfiguration.publishableValue}, shared with the MCP tool and the SQL {@code schema:database}
   * step, and a consolidation that quietly lost the redaction is exactly what a test has to refuse.
   */
  @Test
  void theCredentialsEmbeddedInDefaultDatabasesAreRedacted() throws Exception {
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_DEFAULT_DATABASES,
        "reportedb[reporter:hunter2];plaindb");
    try {
      final JSONObject setting = settingFromEndpoint(GlobalConfiguration.SERVER_DEFAULT_DATABASES.getKey());

      assertThat(setting.getString("value")).doesNotContain("hunter2");
      assertThat(setting.getString("value")).isEqualTo("reportedb[reporter:*****];plaindb");
    } finally {
      getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_DEFAULT_DATABASES, (Object) null);
    }
  }

  private JSONObject settingFromEndpoint(final String key) throws Exception {
    final JSONArray settings = get("/api/v1/server?mode=default").getJSONArray("settings");
    for (int i = 0; i < settings.length(); i++) {
      final JSONObject setting = settings.getJSONObject(i);
      if (key.equals(setting.getString("key")))
        return setting;
    }
    throw new AssertionError("setting '" + key + "' is not reported by GET /api/v1/server");
  }

  private void setServerSetting(final String key, final String value) throws Exception {
    final JSONObject response = post("/api/v1/server",
        new JSONObject().put("command", "set server setting " + key + " " + value).toString());
    assertThat(response.getString("result")).isEqualTo("ok");
  }

  private JSONObject get(final String path) throws Exception {
    return call("GET", path, null);
  }

  private JSONObject post(final String path, final String body) throws Exception {
    return call("POST", path, body);
  }

  private JSONObject call(final String method, final String path, final String body) throws Exception {
    final int port = getServer(0).getHttpServer().getPort();
    final HttpURLConnection connection = (HttpURLConnection) new URI("http://localhost:" + port + path).toURL()
        .openConnection();
    connection.setRequestMethod(method);
    connection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder()
        .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
    try {
      if (body != null) {
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setDoOutput(true);
        try (final DataOutputStream out = new DataOutputStream(connection.getOutputStream())) {
          out.write(body.getBytes(StandardCharsets.UTF_8));
        }
      }
      final int status = connection.getResponseCode();
      final var stream = status < 400 ? connection.getInputStream() : connection.getErrorStream();
      final String payload = stream == null ? "" : new String(stream.readAllBytes(), StandardCharsets.UTF_8);
      assertThat(status).as(method + " " + path + " -> " + payload).isEqualTo(200);
      return new JSONObject(payload);
    } finally {
      connection.disconnect();
    }
  }
}
