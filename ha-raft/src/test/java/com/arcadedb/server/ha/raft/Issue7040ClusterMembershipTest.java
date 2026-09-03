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
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for issue #7040 (follow-up to #5275): the reconciliation of the declared peer list against the live
 * Raft configuration, and the alert built from their divergence. The end-to-end shape of {@code /api/v1/cluster}
 * after a peer removal is covered by {@link Issue7040RemovedPeerStatusIT}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7040ClusterMembershipTest {

  private static final RaftPeer A = peer("a");
  private static final RaftPeer B = peer("b");
  private static final RaftPeer C = peer("c");
  private static final RaftPeer D = peer("d");

  @Test
  void identicalListsAreNotDiverged() {
    final ClusterMembership membership = ClusterMembership.of(List.of(A, B, C), List.of(C, A, B));

    assertThat(membership.diverged()).isFalse();
    assertThat(membership.notInConfiguration()).isEmpty();
    assertThat(membership.notInServerList()).isEmpty();
    assertThat(membership.peers()).as("declaration order is kept, whatever order the configuration reports")
        .containsExactly(A, B, C);
    for (final RaftPeer peer : List.of(A, B, C))
      assertThat(membership.isInConfiguration(peer.getId())).isTrue();
  }

  @Test
  void removedPeerStaysListedButIsMarkedOutOfConfiguration() {
    final ClusterMembership membership = ClusterMembership.of(List.of(A, B, C), List.of(A, B));

    assertThat(membership.diverged()).isTrue();
    assertThat(membership.peers()).as("the removed peer is still reported, never silently dropped").containsExactly(A, B, C);
    assertThat(membership.isInConfiguration(C.getId())).isFalse();
    assertThat(membership.isInConfiguration(A.getId())).isTrue();
    assertThat(membership.notInConfiguration()).containsExactly("c");
    assertThat(membership.notInServerList()).isEmpty();
  }

  @Test
  void dynamicallyAddedPeerIsAppendedAfterTheDeclaredOnes() {
    final ClusterMembership membership = ClusterMembership.of(List.of(A, B), List.of(D, A, B));

    assertThat(membership.peers()).containsExactly(A, B, D);
    assertThat(membership.isInConfiguration(D.getId())).isTrue();
    assertThat(membership.notInServerList()).containsExactly("d");
    assertThat(membership.notInConfiguration()).isEmpty();
  }

  @Test
  void configuredPeersDropTheRemovedAndKeepTheRuntimeAdded() {
    final ClusterMembership membership = ClusterMembership.of(List.of(A, B, C), List.of(A, D, B));

    assertThat(membership.configuredPeers()).as("declared order first, runtime-added last, removed dropped")
        .containsExactly(A, B, D);
    assertThat(membership.peers()).containsExactly(A, B, C, D);
  }

  @Test
  void theLiveEntryWinsForAPeerPresentInBothLists() {
    final RaftPeer rejoined = RaftPeer.newBuilder().setId(B.getId()).setAddress("b-new:2434").build();
    final ClusterMembership membership = ClusterMembership.of(List.of(A, B), List.of(A, rejoined));

    assertThat(membership.peers()).containsExactly(A, rejoined);
    assertThat(membership.peers().get(1).getAddress()).as("the address the cluster committed, not the declared one")
        .isEqualTo("b-new:2434");
    assertThat(membership.diverged()).isFalse();
  }

  @Test
  void alertNamesTheDeclaredPeersMissingFromTheConfiguration() {
    final JSONArray alerts = new JSONArray();
    ClusterAlerts.addMembershipDivergenceAlert(List.of("c"), List.of(), "a", alerts);

    assertThat(alerts.length()).isEqualTo(1);
    final JSONObject alert = alerts.getJSONObject(0);
    assertThat(alert.getString("id")).isEqualTo("peers-not-in-configuration");
    assertThat(alert.getString("severity")).isEqualTo(ClusterAlerts.SEVERITY_WARNING);
    assertThat(alert.getString("message")).contains("c");
    assertThat(alert.getJSONObject("details").getJSONArray("peers").toList()).containsExactly("c");
  }

  @Test
  void alertIsCriticalWhenThisNodeIsTheOneMissing() {
    final JSONArray alerts = new JSONArray();
    ClusterAlerts.addMembershipDivergenceAlert(List.of("a", "c"), List.of(), "a", alerts);

    final JSONObject alert = alerts.getJSONObject(0);
    assertThat(alert.getString("severity")).isEqualTo(ClusterAlerts.SEVERITY_CRITICAL);
    assertThat(alert.getString("title")).contains("This node");
  }

  @Test
  void undeclaredMemberIsOnlyInformational() {
    final JSONArray alerts = new JSONArray();
    ClusterAlerts.addMembershipDivergenceAlert(List.of(), List.of("d"), "a", alerts);

    assertThat(alerts.length()).isEqualTo(1);
    final JSONObject alert = alerts.getJSONObject(0);
    assertThat(alert.getString("id")).isEqualTo("peers-not-in-server-list");
    assertThat(alert.getString("severity")).isEqualTo(ClusterAlerts.SEVERITY_INFO);
  }

  @Test
  void noAlertWithoutDivergence() {
    final JSONArray alerts = new JSONArray();
    ClusterAlerts.addMembershipDivergenceAlert(List.of(), List.of(), "a", alerts);
    ClusterAlerts.addMembershipDivergenceAlert(null, null, null, alerts);

    assertThat(alerts.length()).isZero();
  }

  private static RaftPeer peer(final String id) {
    return RaftPeer.newBuilder().setId(RaftPeerId.valueOf(id)).setAddress(id + ":2434").build();
  }
}
