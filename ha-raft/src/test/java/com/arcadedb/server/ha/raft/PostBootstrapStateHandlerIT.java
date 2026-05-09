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

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * IT for the pre-bootstrap RPC ({@code POST /api/v1/cluster/bootstrap-state}, issue #4147 phase 3).
 * Confirms the wire shape every peer must produce so the bootstrap leader can collect state and
 * pick a source. Phase 6 will add WAL retention; until then {@code oldestRetainedTxId} is always
 * {@code -1} (NO_DELTA_AVAILABLE), which makes followers fall through to the existing
 * leader-shipped snapshot path.
 */
class PostBootstrapStateHandlerIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Test
  void rpcReturnsPerDatabaseStateWithExpectedShape() throws Exception {
    final JSONObject response = postBootstrapState(0);

    assertThat(response.has("peerId")).isTrue();
    assertThat(response.getString("peerId")).startsWith("localhost_");

    final JSONArray dbs = response.getJSONArray("databases");
    // The base test fixture creates a "graph" database; we don't assert on count to stay robust
    // against the fixture changing, only on the shape of each entry.
    assertThat(dbs.length()).isGreaterThanOrEqualTo(1);

    for (int i = 0; i < dbs.length(); i++) {
      final JSONObject db = dbs.getJSONObject(i);
      assertThat(db.getString("name")).isNotEmpty();
      // Reserved ".raft" db must never appear: it's an internal control database.
      assertThat(db.getString("name")).doesNotStartWith(".");
      assertThat(db.getString("fingerprint")).hasSize(64); // SHA-256 hex
      assertThat(db.getLong("lastTxId")).isGreaterThanOrEqualTo(-1L);
      // Phase 3 always reports NO_DELTA_AVAILABLE (-1). Phase 6 will fill this in.
      assertThat(db.getLong("oldestRetainedTxId")).isEqualTo(PostBootstrapStateHandler.NO_DELTA_AVAILABLE);
    }
  }

  @Test
  void rpcRejectsMissingAuth() throws Exception {
    final int httpPort = 2480;
    final URL url = new URL("http://localhost:" + httpPort + "/api/v1/cluster/bootstrap-state");
    final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("POST");
    conn.setDoOutput(true);
    conn.getOutputStream().write("{}".getBytes(StandardCharsets.UTF_8));
    try {
      // No Authorization, no X-ArcadeDB-Cluster-Token header → 401.
      assertThat(conn.getResponseCode()).isEqualTo(401);
    } finally {
      conn.disconnect();
    }
  }

  /** Two peers should agree on the fingerprint of an identical, just-created database. */
  @Test
  void allPeersReportSameFingerprintForReplicatedDatabase() throws Exception {
    final JSONObject r0 = postBootstrapState(0);
    final JSONObject r1 = postBootstrapState(1);

    final JSONObject db0 = findDatabase(r0, getDatabaseName());
    final JSONObject db1 = findDatabase(r1, getDatabaseName());
    assertThat(db0).as("database '%s' present on peer 0", getDatabaseName()).isNotNull();
    assertThat(db1).as("database '%s' present on peer 1", getDatabaseName()).isNotNull();

    // After Raft replication settles both peers should hold identical bytes; fingerprint must
    // match. (Failure here would suggest the fingerprint is computing over an unstable surface
    // such as a WAL file that legitimately rotates between peers.)
    assertThat(db0.getString("fingerprint")).isEqualTo(db1.getString("fingerprint"));
  }

  private static JSONObject findDatabase(final JSONObject response, final String name) {
    final JSONArray dbs = response.getJSONArray("databases");
    for (int i = 0; i < dbs.length(); i++) {
      final JSONObject db = dbs.getJSONObject(i);
      if (name.equals(db.getString("name")))
        return db;
    }
    return null;
  }

  private JSONObject postBootstrapState(final int serverIndex) throws Exception {
    final int httpPort = 2480 + serverIndex;
    final URL url = new URL("http://localhost:" + httpPort + "/api/v1/cluster/bootstrap-state");
    final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("POST");
    conn.setDoOutput(true);
    conn.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(
            ("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
    conn.setRequestProperty("Content-Type", "application/json");
    conn.getOutputStream().write("{}".getBytes(StandardCharsets.UTF_8));

    try {
      assertThat(conn.getResponseCode()).isEqualTo(200);
      final String body = new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
      return new JSONObject(body);
    } finally {
      conn.disconnect();
    }
  }
}
