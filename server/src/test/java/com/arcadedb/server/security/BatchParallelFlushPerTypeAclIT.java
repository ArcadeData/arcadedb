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
 * Regression test for the incomplete fix of GHSA-c23x-pqcj-7hfm: the c23x fix bound the authenticated
 * principal on the batch handler's HTTP worker thread and gated {@code LocalBucket.createRecordsBulk} for the
 * edge-record creation phase, but left {@code DatabaseAsyncTransaction} - the task type
 * {@link com.arcadedb.graph.GraphBatch}'s parallel edge-connect phase submits to the async executor when
 * {@code parallelFlush} is left at its default {@code true} - binding no principal at all. The engine's
 * per-type ACL ({@code LocalDatabase.checkPermissionsOnFile}) is a no-op when no principal is bound, so a user
 * with {@code CREATE_RECORD} on an edge type but {@code CREATE_RECORD}/{@code UPDATE_RECORD} revoked on the
 * target vertex type could still durably connect edges to that vertex type's buckets through the parallel
 * connect phase, while the same write was already correctly rejected with {@code parallelFlush=false}.
 * <p>
 * The user in this test may create edges of {@code PfEdge} but has only {@code readRecord} on {@code PfVertex}.
 * On the vulnerable build the parallel-flush batch below durably connected the edge (2xx); with the principal
 * bound on the async worker, the per-type {@code CREATE_RECORD}/{@code UPDATE_RECORD} gate rejects it (403),
 * matching the {@code parallelFlush=false} behaviour that was already correct.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class BatchParallelFlushPerTypeAclIT extends BaseGraphServerTest {

  private static final String SCOPED_USER = "parallelflush-scoped-user";
  private static final String SCOPED_PWD  = "parallelflushuser1";
  private static final String VERTEX_TYPE = "PfVertex";
  private static final String EDGE_TYPE   = "PfEdge";

  @Test
  void perTypeAclEnforcedOnParallelFlushEdgeConnect() throws Exception {
    testEachServer((serverIndex) -> {
      // Types must exist BEFORE the scoped user's per-type ACL map is built, so the map segments PfVertex.
      command(serverIndex, "CREATE VERTEX TYPE " + VERTEX_TYPE);
      command(serverIndex, "CREATE EDGE TYPE " + EDGE_TYPE);

      final String v1 = createVertex(serverIndex);
      final String v2 = createVertex(serverIndex);
      final String v3 = createVertex(serverIndex);
      final String v4 = createVertex(serverIndex);

      // Seed one edge per attack target BEFORE the ACL is narrowed, as root: this durably allocates v1's and
      // v3's out-edge chunk. GraphBatch only defers a vertex's chunk head update to the async connect phase
      // when the chunk already exists with spare room (a brand-new vertex's first chunk is instead created
      // synchronously on the bound thread, which the earlier GHSA-c23x fix already gates) - so without a seed
      // edge here the attack below would only re-exercise the already-fixed path.
      command(serverIndex, "CREATE EDGE " + EDGE_TYPE + " FROM " + v1 + " TO " + v2);
      command(serverIndex, "CREATE EDGE " + EDGE_TYPE + " FROM " + v3 + " TO " + v2);

      createScopedUser(serverIndex);
      try {
        final String auth = basicAuth(SCOPED_USER, SCOPED_PWD);

        // Attack: connect a further edge from a per-type-forbidden vertex with an EXISTING, ROOMY edge chunk,
        // through the batch handler's default (parallelFlush=true) edge-connect phase. On the vulnerable build
        // this was durably written despite the revoked ACL, because the async worker that runs the connect
        // phase binds no principal.
        final String parallelEdgeLine =
            "{\"@type\":\"edge\",\"@class\":\"" + EDGE_TYPE + "\",\"@from\":\"" + v1 + "\",\"@to\":\"" + v2 + "\"}\n";
        final int parallelStatus = postBatch(serverIndex, getDatabaseName(), auth, parallelEdgeLine, null);
        assertThat(parallelStatus).as("parallel-flush edge connect to a per-type forbidden vertex type must be rejected")
            .isEqualTo(403);

        // Differential control: the identical write (on the OTHER seeded vertex, so it starts from the same
        // one-edge state) with parallelFlush=false runs the connect phase inline on the bound HTTP thread
        // (already gated since GHSA-c23x). Asserting it here proves the 403 above is per-type authorization
        // enforced consistently on both paths, not some unrelated failure.
        final String sequentialEdgeLine =
            "{\"@type\":\"edge\",\"@class\":\"" + EDGE_TYPE + "\",\"@from\":\"" + v3 + "\",\"@to\":\"" + v4 + "\"}\n";
        final int sequentialStatus = postBatch(serverIndex, getDatabaseName(), auth, sequentialEdgeLine, "parallelFlush=false");
        assertThat(sequentialStatus).as("sequential edge connect to a per-type forbidden vertex type must be rejected")
            .isEqualTo(403);

        // Decisive check (as root): each attack target's adjacency must still show only the one seeded edge -
        // the exact bucket the advisory says a bypass leaves durably mutated. On the vulnerable build the
        // parallel-flush attempt connected a second edge to v1 despite the revoked ACL. (A flush that fails
        // after its edge record was already committed on the bound thread, as here, may still leave that
        // lone, UNCONNECTED PfEdge record behind if the user also lacks deleteRecord to let GraphBatch reclaim
        // it - that is a data-hygiene side effect of the correctly-rejected write, not a re-opening of the ACL
        // bypass, so it is not what this assertion checks.)
        assertThat(countConnectedEdges(serverIndex, v1)).as("no forbidden-type edge connection may have been added via the parallel path")
            .isEqualTo(1);
        assertThat(countConnectedEdges(serverIndex, v3)).as("no forbidden-type edge connection may have been added via the sequential path")
            .isEqualTo(1);
      } finally {
        deleteUser(serverIndex, SCOPED_USER);
      }
    });
  }

  private String createVertex(final int serverIndex) throws Exception {
    final String response = command(serverIndex, "CREATE VERTEX " + VERTEX_TYPE);
    final JSONObject json = new JSONObject(response);
    return json.getJSONArray("result").getJSONObject(0).getString("@rid");
  }

  private long countConnectedEdges(final int serverIndex, final String vertexRid) throws Exception {
    final String response = command(serverIndex, "SELECT bothE('" + EDGE_TYPE + "').size() AS c FROM " + vertexRid);
    final JSONObject json = new JSONObject(response);
    return json.getJSONArray("result").getJSONObject(0).getLong("c");
  }

  private void createScopedUser(final int serverIndex) throws Exception {
    final ServerSecurity security = getServer(serverIndex).getSecurity();

    // Grant readRecord/createRecord/updateRecord on every type via the "*" default (covers PfEdge), but
    // narrow PfVertex down to readRecord only - createRecord/updateRecord REVOKED, mirroring the attack
    // precondition. A listed type overrides the "*" default in the per-type map resolution.
    security.getDatabaseGroupsConfiguration(getDatabaseName()).put("parallelFlushScoped",
        new JSONObject().put("access", new JSONArray())
            .put("types", new JSONObject()
                .put("*", new JSONObject().put("access",
                    new JSONArray().put("readRecord").put("createRecord").put("updateRecord")))
                .put(VERTEX_TYPE, new JSONObject().put("access", new JSONArray().put("readRecord")))));
    security.saveGroups();

    if (security.existsUser(SCOPED_USER))
      security.dropUser(SCOPED_USER);

    final JSONObject payload = new JSONObject()
        .put("name", SCOPED_USER)
        .put("password", SCOPED_PWD)
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put("parallelFlushScoped")));

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

  private int postBatch(final int serverIndex, final String db, final String auth, final String jsonl,
      final String extraQueryParam) throws Exception {
    final String query = extraQueryParam != null ? "?" + extraQueryParam : "";
    final HttpURLConnection connection = openPost(serverIndex, "/api/v1/batch/" + db + query, auth);
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
