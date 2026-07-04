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
package com.arcadedb.server.security;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GHSA-46hj-24h4-j8gf: {@code GET /api/v1/server} exposed every non-database
 * {@code GlobalConfiguration} value, masking only keys literally containing "password". The HA cluster token
 * ({@code arcadedb.ha.clusterToken}) therefore leaked in clear, which - combined with cluster-forwarded-auth
 * headers - allowed root impersonation. After the fix every setting flagged secret by
 * {@link GlobalConfiguration#isHidden()} (which includes clusterToken) is redacted to {@code *****}.
 */
class GetServerSettingsRedactionIT extends BaseGraphServerTest {

  private static final String SECRET_TOKEN = "s3cr3t-cluster-token-should-never-leak";

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    config.setValue(GlobalConfiguration.HA_CLUSTER_TOKEN, SECRET_TOKEN);
  }

  @Test
  void clusterTokenIsRedactedInServerSettings() throws Exception {
    testEachServer((serverIndex) -> {
      final String body = getServerInfo(serverIndex);

      // The raw secret must not appear anywhere in the response.
      assertThat(body).as("cluster token must not leak in /api/v1/server").doesNotContain(SECRET_TOKEN);

      // And the specific setting entry must be present but masked.
      final JSONArray settings = new JSONObject(body).getJSONArray("settings");
      boolean found = false;
      for (int i = 0; i < settings.length(); i++) {
        final JSONObject entry = settings.getJSONObject(i);
        if ("arcadedb.ha.clusterToken".equals(entry.getString("key"))) {
          found = true;
          assertThat(entry.getString("value")).as("clusterToken value must be masked").isEqualTo("*****");
          assertThat(entry.getString("default")).as("clusterToken default must be masked").isEqualTo("*****");
        }
      }
      assertThat(found).as("clusterToken setting must be present in the exported settings").isTrue();
    });
  }

  private String getServerInfo(final int serverIndex) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/server").openConnection();
    connection.setRequestMethod("GET");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.connect();
    try {
      assertThat(connection.getResponseCode()).isEqualTo(200);
      return new String(connection.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    } finally {
      connection.disconnect();
    }
  }
}
