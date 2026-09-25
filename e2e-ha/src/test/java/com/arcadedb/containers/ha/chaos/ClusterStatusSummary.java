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

package com.arcadedb.containers.ha.chaos;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * One-line summary of a node's {@code GET /api/v1/cluster} answer for failure messages: who it thinks leads, its Raft
 * state and position, whether it reports itself stuck at a stale term, the alerts it raises and, on the leader, each
 * follower's replication state. Fields an older server does not send are left out.
 */
final class ClusterStatusSummary {
  private ClusterStatusSummary() {
  }

  static String describe(final JSONObject cluster) {
    final StringBuilder line = new StringBuilder();
    line.append("isLeader=").append(cluster.getBoolean("isLeader", false));
    line.append(" leader=").append(cluster.isNull("leaderHttpAddress") ? "none" : cluster.getString("leaderHttpAddress"));
    if (cluster.has("raftState"))
      line.append(" raftState=").append(cluster.getString("raftState"));
    if (cluster.has("localAppliedIndex"))
      line.append(" applied=").append(cluster.getLong("localAppliedIndex", -1));
    if (cluster.has("localCommitIndex"))
      line.append(" commit=").append(cluster.getLong("localCommitIndex", -1));
    if (cluster.getBoolean("localStuckAtStaleTerm", false))
      line.append(" STUCK_AT_STALE_TERM");

    final JSONArray peers = cluster.has("peers") ? cluster.getJSONArray("peers") : new JSONArray();
    final List<String> followers = new ArrayList<>();
    for (int i = 0; i < peers.length(); i++) {
      final JSONObject peer = peers.getJSONObject(i);
      if (!peer.has("replicaStatus"))
        continue;
      followers.add(peer.getString("id", "?") + " " + peer.getString("replicaStatus", "?") + " lag="
          + peer.getLong("replicationLag", -1) + " lastContact=" + peer.getLong("lastContactMs", -1) + "ms");
    }
    if (!followers.isEmpty())
      line.append(" followers=").append(followers);

    final JSONArray alerts = cluster.has("alerts") ? cluster.getJSONArray("alerts") : new JSONArray();
    final List<String> alertIds = new ArrayList<>();
    for (int i = 0; i < alerts.length(); i++)
      alertIds.add(alerts.getJSONObject(i).getString("id", "?"));
    if (!alertIds.isEmpty())
      line.append(" alerts=").append(alertIds);
    return line.toString();
  }
}
