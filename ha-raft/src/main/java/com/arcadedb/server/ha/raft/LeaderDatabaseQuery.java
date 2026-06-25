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

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Lists the (user) databases a peer holds, by calling its {@code POST /api/v1/cluster/bootstrap-state} RPC
 * (issue #4727). Reuses the same cluster-token + forwarded-user authentication as every other peer-to-peer
 * cluster RPC (see {@link PostBootstrapStateHandler}). The handler already excludes reserved internal
 * databases (names starting with {@code .}), so the returned set is the operator-visible database list.
 * <p>
 * Used by:
 * <ul>
 *   <li>the join-time reconcile in {@code ArcadeStateMachine.notifyInstallSnapshotFromLeader} to learn which
 *       databases the leader holds and pull the ones this node is missing;</li>
 *   <li>the optional presence fan-out in {@code GetClusterHandler} to build the Studio per-node/per-database
 *       presence matrix.</li>
 * </ul>
 */
public final class LeaderDatabaseQuery {

  /** A single database entry reported by a peer. {@code lastTxId} is {@code -1} when unknown/unreadable. */
  public record DatabaseInfo(String name, long lastTxId) {
  }

  private static final HttpClient HTTP = HttpClient.newBuilder()
      .connectTimeout(Duration.ofSeconds(5))
      .build();

  private LeaderDatabaseQuery() {
  }

  /**
   * Synchronously queries {@code httpAddr} for the list of databases it holds.
   *
   * @param httpAddr     the peer HTTP address ({@code host:port}).
   * @param clusterToken the inter-node cluster token, may be {@code null}/blank if not configured.
   * @param timeoutMs    per-request timeout in milliseconds.
   * @return the databases reported by the peer (never {@code null}).
   * @throws IOException          on transport error or a non-200 response.
   * @throws InterruptedException if the calling thread is interrupted while waiting.
   */
  public static List<DatabaseInfo> fetch(final String httpAddr, final String clusterToken, final long timeoutMs)
      throws IOException, InterruptedException {
    final String url = "http://" + httpAddr + "/api/v1/cluster/bootstrap-state";
    final HttpRequest.Builder builder = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(Duration.ofMillis(timeoutMs))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString("{}"));
    if (clusterToken != null && !clusterToken.isBlank())
      builder.header("X-ArcadeDB-Cluster-Token", clusterToken);
    builder.header("X-ArcadeDB-Forwarded-User", RaftHAServer.FORWARDED_ROOT_USER);

    final HttpResponse<String> resp = HTTP.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    if (resp.statusCode() != 200)
      throw new IOException("bootstrap-state query to " + httpAddr + " returned HTTP " + resp.statusCode());

    final JSONObject json = new JSONObject(resp.body());
    final JSONArray dbs = json.getJSONArray("databases");
    final List<DatabaseInfo> out = new ArrayList<>(dbs.length());
    for (int i = 0; i < dbs.length(); i++) {
      final JSONObject db = dbs.getJSONObject(i);
      out.add(new DatabaseInfo(db.getString("name"), db.getLong("lastTxId", -1L)));
    }
    return out;
  }
}
