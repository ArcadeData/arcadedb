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
package com.arcadedb.server.ha;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Scanner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for the cluster health endpoint.
 * Tests that the /api/v1/cluster/health endpoint returns expected health information.
 */
class GetClusterHealthIT extends BaseGraphServerTest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void testClusterHealthEndpoint() throws Exception {
    testEachServer((serverIndex) -> {
      try {
        final String healthUrl = "http://127.0.0.1:248" + serverIndex + "/api/v1/cluster/health";
        final HttpURLConnection conn = (HttpURLConnection) new URL(healthUrl).openConnection();
        conn.setRequestMethod("GET");
        conn.setRequestProperty("Authorization",
            "Basic " + java.util.Base64.getEncoder().encodeToString("root:".getBytes()));

        final int responseCode = conn.getResponseCode();
        assertThat(responseCode).isEqualTo(200);

        // Read response
        final Scanner scanner = new Scanner(conn.getInputStream());
        final StringBuilder response = new StringBuilder();
        while (scanner.hasNextLine()) {
          response.append(scanner.nextLine());
        }
        scanner.close();

        // Parse JSON response
        final JSONObject health = new JSONObject(response.toString());

        // Verify required fields
        assertThat(health.has("serverName")).isTrue();
        assertThat(health.has("role")).isTrue();
        assertThat(health.has("configuredServers")).isTrue();
        assertThat(health.has("onlineServers")).isTrue();
        assertThat(health.has("onlineReplicas")).isTrue();
        assertThat(health.has("quorumAvailable")).isTrue();
        assertThat(health.has("electionStatus")).isTrue();

        // Verify values make sense
        assertThat(health.getString("role")).isIn("Leader", "Replica");
        assertThat(health.getInt("configuredServers")).isEqualTo(3);
        assertThat(health.getInt("onlineServers")).isGreaterThan(0);
        assertThat(health.getInt("onlineReplicas")).isGreaterThanOrEqualTo(0);

        // Leader should have replica statuses
        if (health.getString("role").equals("Leader")) {
          assertThat(health.has("replicaStatuses")).isTrue();
        }

      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }
}
