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

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GHSA-c23x-pqcj-7hfm: the GHSA-x8mg fix added a coarse database-level
 * {@code canAccessToDatabase} gate to the batch/time-series/Prometheus/Grafana handlers, but those handlers
 * do not extend {@code DatabaseAbstractHandler} and never bound the authenticated principal onto the worker
 * thread's {@code DatabaseContext}. The engine's fine-grained per-type ACL layer
 * ({@code LocalDatabase.checkPermissionsOnFile}) is a no-op when no principal is bound, so a user with
 * database access but only per-type rights on some types could read/write types it was not entitled to
 * through those handlers.
 * <p>
 * The user in this test is granted createRecord on everything EXCEPT the {@code Secret} type. On the
 * vulnerable build a batch POST of a {@code Secret} vertex was written (null principal → per-type check
 * skipped). With the principal bound, the per-type CREATE_RECORD gate fires and the write is rejected with
 * HTTP 403, while the same user's write to an authorized type still succeeds.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class UngatedHandlerPerTypeAclIT extends BaseGraphServerTest {

  private static final String SCOPED_USER = "pertype-scoped-user";
  private static final String SCOPED_PWD  = "pertypeuser1";
  private static final String PUBLIC_TYPE = "PublicDoc";
  private static final String SECRET_TYPE = "SecretDoc";

  @Test
  void perTypeAclEnforcedOnBatchHandler() throws Exception {
    testEachServer((serverIndex) -> {
      // Types must exist BEFORE the scoped user's per-type ACL map is built, so the map segments Secret.
      command(serverIndex, "CREATE VERTEX TYPE " + PUBLIC_TYPE);
      command(serverIndex, "CREATE VERTEX TYPE " + SECRET_TYPE);

      createScopedUser(serverIndex);
      try {
        final String auth = basicAuth(SCOPED_USER, SCOPED_PWD);

        // Attack: write a forbidden type through the batch handler. On the vulnerable build this returned a
        // 2xx and created the record; with the principal bound the per-type CREATE_RECORD ACL rejects it (403).
        final int secretStatus = postBatch(serverIndex, getDatabaseName(), auth,
            "{\"@type\":\"vertex\",\"@class\":\"" + SECRET_TYPE + "\",\"x\":1}\n");
        assertThat(secretStatus).as("batch write to a per-type forbidden type must be rejected").isEqualTo(403);

        // Positive control: the same user may write an authorized type - proving the 403 above is per-type
        // authorization, not a blanket denial of the batch handler.
        final int publicStatus = postBatch(serverIndex, getDatabaseName(), auth,
            "{\"@type\":\"vertex\",\"@class\":\"" + PUBLIC_TYPE + "\",\"x\":1}\n");
        assertThat(publicStatus).as("batch write to an authorized type must succeed").isEqualTo(200);

        // Decisive check (as root): no Secret record may have been written. On the vulnerable build one exists.
        final long secretCount = countRecords(serverIndex, SECRET_TYPE);
        assertThat(secretCount).as("no forbidden-type record may have been written").isZero();

        final long publicCount = countRecords(serverIndex, PUBLIC_TYPE);
        assertThat(publicCount).as("the authorized-type record must have been written").isEqualTo(1);
      } finally {
        deleteUser(serverIndex, SCOPED_USER);
      }
    });
  }

  private long countRecords(final int serverIndex, final String typeName) throws Exception {
    final String response = command(serverIndex, "SELECT count(*) AS c FROM " + typeName);
    final JSONObject json = new JSONObject(response);
    final JSONArray result = json.getJSONArray("result");
    if (result.isEmpty())
      return 0;
    return result.getJSONObject(0).getLong("c");
  }

  private void createScopedUser(final int serverIndex) throws Exception {
    final ServerSecurity security = getServer(serverIndex).getSecurity();

    // Grant readRecord/createRecord on every type via the "*" default, but explicitly REVOKE all access on
    // SECRET_TYPE. A listed type overrides the "*" default in the per-type map resolution.
    security.getDatabaseGroupsConfiguration(getDatabaseName()).put("perTypeScoped",
        new JSONObject().put("access", new JSONArray())
            .put("types", new JSONObject()
                .put("*", new JSONObject().put("access",
                    new JSONArray().put("readRecord").put("createRecord").put("updateRecord")))
                .put(SECRET_TYPE, new JSONObject().put("access", new JSONArray()))));
    security.saveGroups();

    if (security.existsUser(SCOPED_USER))
      security.dropUser(SCOPED_USER);

    final JSONObject payload = new JSONObject()
        .put("name", SCOPED_USER)
        .put("password", SCOPED_PWD)
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put("perTypeScoped")));

    final HttpURLConnection connection = openPost(serverIndex, "/api/v1/server/users",
        basicAuth("root", DEFAULT_PASSWORD_FOR_TESTS));
    connection.setDoOutput(true);
    connection.setRequestProperty("Content-Type", "application/json");
    connection.getOutputStream().write(payload.toString().getBytes());
    connection.connect();
    try {
      assertThat(connection.getResponseCode()).isEqualTo(201);
    } finally {
      connection.disconnect();
    }
  }

  private void deleteUser(final int serverIndex, final String name) throws Exception {
    final HttpURLConnection connection = openPost(serverIndex,
        "/api/v1/server/users?name=" + URLEncoder.encode(name, StandardCharsets.UTF_8),
        basicAuth("root", DEFAULT_PASSWORD_FOR_TESTS));
    connection.setRequestMethod("DELETE");
    connection.connect();
    try {
      connection.getResponseCode();
    } finally {
      connection.disconnect();
    }
  }

  private int postBatch(final int serverIndex, final String db, final String auth, final String jsonl) throws Exception {
    final HttpURLConnection connection = openPost(serverIndex, "/api/v1/batch/" + db, auth);
    connection.setDoOutput(true);
    connection.setRequestProperty("Content-Type", "application/x-ndjson");
    connection.getOutputStream().write(jsonl.getBytes(StandardCharsets.UTF_8));
    connection.connect();
    try {
      return connection.getResponseCode();
    } finally {
      connection.disconnect();
    }
  }

  private HttpURLConnection openPost(final int serverIndex, final String path, final String auth) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:248" + serverIndex + path).openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", auth);
    return connection;
  }

  private String basicAuth(final String user, final String password) {
    return "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes());
  }
}
