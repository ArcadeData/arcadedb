/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;

import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4791: the five cluster-management HTTP endpoints must require the
 * root user, not merely some authenticated user. A non-root tenant user must be rejected with
 * HTTP 403 before any cluster mutation can take place, otherwise a tenant could remove peers,
 * force election churn, transfer leadership, or evict a node.
 */
class ClusterManagementAuthorizationIT extends BaseRaftHATest {

  private static final String TENANT_USER     = "tenant4791";
  private static final String TENANT_PASSWORD = "tenantpw1234";

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Test
  void nonRootRejectedOnAllClusterManagementEndpoints() throws Exception {
    // A non-root user that is otherwise a database admin: this is the privilege-escalation scenario.
    getServer(0).getSecurity().createUser(TENANT_USER, TENANT_PASSWORD);

    // Every cluster-management endpoint must reject the non-root tenant with 403. Because the root
    // check runs first, none of these calls mutates the cluster, so the order here is irrelevant.
    assertThat(call(0, "POST", "/api/v1/cluster/peer",
        new JSONObject().put("peerId", "localhost_9999").put("address", "localhost:9999"),
        TENANT_USER, TENANT_PASSWORD))
        .as("POST add-peer as non-root").isEqualTo(403);

    assertThat(call(0, "DELETE", "/api/v1/cluster/peer/" + peerIdForIndex(1), null,
        TENANT_USER, TENANT_PASSWORD))
        .as("DELETE peer as non-root").isEqualTo(403);

    assertThat(call(0, "POST", "/api/v1/cluster/leader", new JSONObject(), TENANT_USER, TENANT_PASSWORD))
        .as("POST transfer-leader as non-root").isEqualTo(403);

    assertThat(call(0, "POST", "/api/v1/cluster/stepdown", new JSONObject(), TENANT_USER, TENANT_PASSWORD))
        .as("POST stepdown as non-root").isEqualTo(403);

    assertThat(call(0, "POST", "/api/v1/cluster/leave", new JSONObject(), TENANT_USER, TENANT_PASSWORD))
        .as("POST leave as non-root").isEqualTo(403);
  }

  @Test
  void rootPassesRootCheck() throws Exception {
    // The root user must pass the root check: a malformed add-peer request reaches validation and
    // returns 400 (missing required fields) rather than 403. This proves the fix does not over-block
    // root, and that the failure for non-root above is the authorization gate, not generic rejection.
    final int status = call(0, "POST", "/api/v1/cluster/peer", new JSONObject(), "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    assertThat(status).as("root add-peer with empty body must pass the root check (400, not 403)")
        .isEqualTo(400);
  }

  private int call(final int serverIndex, final String method, final String path, final JSONObject body,
      final String user, final String password) throws Exception {
    final int port = getServer(serverIndex).getHttpServer().getPort();
    final HttpURLConnection conn = (HttpURLConnection) new URI(
        "http://localhost:" + port + path).toURL().openConnection();
    conn.setRequestMethod(method);
    conn.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8)));
    try {
      if (body != null) {
        conn.setDoOutput(true);
        conn.setRequestProperty("Content-Type", "application/json");
        final byte[] payload = body.toString().getBytes(StandardCharsets.UTF_8);
        conn.getOutputStream().write(payload);
      }
      conn.connect();
      return conn.getResponseCode();
    } finally {
      conn.disconnect();
    }
  }
}
