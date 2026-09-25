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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ClusterStatusSummaryTest {

  @Test
  void leaderListsEachFollowersReplicationState() {
    final JSONObject cluster = new JSONObject().put("isLeader", true).put("leaderHttpAddress", "proxy:8672")
        .put("localPeerId", "proxy_8662").put("raftState", "RUNNING").put("localAppliedIndex", 238018L)
        .put("localCommitIndex", 238018L).put("localStuckAtStaleTerm", false)
        .put("peers", new JSONArray()
            .put(new JSONObject().put("id", "proxy_8660").put("role", "FOLLOWER").put("replicaStatus", "STALLED")
                .put("replicationLag", 238019L).put("lastContactMs", 75017L))
            .put(new JSONObject().put("id", "proxy_8661").put("role", "FOLLOWER").put("replicaStatus", "CATCHING_UP")
                .put("replicationLag", 9387L).put("lastContactMs", 0L))
            .put(new JSONObject().put("id", "proxy_8662").put("role", "LEADER")))
        .put("alerts", new JSONArray());
    assertThat(ClusterStatusSummary.describe(cluster)).isEqualTo(
        "isLeader=true leader=proxy:8672 raftState=RUNNING applied=238018 commit=238018"
            + " followers=[proxy_8660 STALLED lag=238019 lastContact=75017ms, proxy_8661 CATCHING_UP lag=9387 lastContact=0ms]");
  }

  @Test
  void stuckFollowerAndAlertsAreNamed() {
    final JSONObject cluster = new JSONObject().put("isLeader", false).put("leaderHttpAddress", "proxy:8672")
        .put("raftState", "RUNNING").put("localAppliedIndex", 228631L).put("localCommitIndex", 228631L)
        .put("localStuckAtStaleTerm", true)
        .put("alerts", new JSONArray().put(new JSONObject().put("id", "follower-stuck-at-stale-term").put("severity", "CRITICAL")));
    assertThat(ClusterStatusSummary.describe(cluster)).isEqualTo(
        "isLeader=false leader=proxy:8672 raftState=RUNNING applied=228631 commit=228631 STUCK_AT_STALE_TERM"
            + " alerts=[follower-stuck-at-stale-term]");
  }

  @Test
  void missingFieldsFromAnOlderServerAreTolerated() {
    assertThat(ClusterStatusSummary.describe(new JSONObject().put("isLeader", false))).isEqualTo(
        "isLeader=false leader=none");
  }
}
