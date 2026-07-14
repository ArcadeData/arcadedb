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

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #5275: a Kubernetes pod recreation silently removed the peer from the
 * committed Raft configuration (shutdown auto-leave) and a node restarting with persisted Raft storage
 * was never re-added (the boot-time auto-join only ran when storage was missing), permanently shrinking
 * a 3-node group to 2 with no operator-visible signal.
 * <p>
 * The fix has two halves, both covered here:
 * <ol>
 *   <li>a graceful node stop must NOT shrink the committed Raft configuration - shutdown means
 *       "temporarily unreachable", not "gone"; membership changes are explicit-only;</li>
 *   <li>the {@link KubernetesAutoJoin} probe doubles as a startup membership self-check: a node whose
 *       persisted storage claims membership but which the live configuration no longer contains must
 *       re-add itself instead of waiting forever for a leader that will never dial a non-member.
 *       {@code HA_K8S} cannot be enabled in-process (it requires a Kubernetes pod identity), so the
 *       probe is exercised directly, exactly as the k8s start path now invokes it on every boot.</li>
 * </ol>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue5275MembershipSelfHealIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected boolean persistentRaftStorage() {
    // The restarting node must resume its persisted Raft storage: the issue-#5275 strand only exists
    // because persisted storage skips the (pre-fix) auto-join.
    return true;
  }

  @Test
  void gracefulStopDoesNotShrinkRaftConfiguration() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    final int followerIndex = (leaderIndex + 1) % getServerCount();

    final RaftHAServer leaderRaft = getRaftPlugin(leaderIndex).getRaftHAServer();
    assertThat(leaderRaft.getLivePeers()).hasSize(3);

    // Graceful stop of a follower (what a pod deletion's SIGTERM produces).
    getServer(followerIndex).stop();

    // The committed configuration must still contain all 3 peers: the stopped node is unreachable,
    // not removed. Assert it holds (not just transiently) for a few seconds.
    for (int i = 0; i < 5; i++) {
      assertThat(leaderRaft.getLivePeers())
          .as("committed Raft configuration must not shrink on a graceful member stop")
          .hasSize(3);
      Thread.sleep(1_000);
    }

    // And the node resumes as a member on restart, no re-add needed.
    getServer(followerIndex).start();
    Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> {
          final RaftHAPlugin plugin = getRaftPlugin(followerIndex);
          assertThat(plugin).isNotNull();
          assertThat(plugin.getRaftHAServer()).isNotNull();
          assertThat(plugin.getRaftHAServer().getLeaderId()).isNotNull();
          assertThat(leaderRaft.getLivePeers()).hasSize(3);
        });
  }

  @Test
  void removedPeerWithPersistedStorageSelfHealsViaAutoJoinProbe() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    final String followerPeerId = peerIdForIndex(followerIndex);

    final RaftHAServer leaderRaft = getRaftPlugin(leaderIndex).getRaftHAServer();
    assertThat(leaderRaft.getLivePeers()).hasSize(3);

    // Reproduce the reported state: the peer is dropped from the committed configuration while it is
    // down (as the pre-#5275 shutdown auto-leave did on every pod recreation).
    getServer(followerIndex).stop();
    leaderRaft.removePeer(followerPeerId, true);
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> assertThat(leaderRaft.getLivePeers()).hasSize(2));

    // The node restarts with persisted Raft storage still claiming membership of the 3-node group.
    getServer(followerIndex).start();
    final RaftHAServer followerRaft = getRaftPlugin(followerIndex).getRaftHAServer();
    assertThat(followerRaft).isNotNull();

    // Startup membership self-check (what the k8s start path now runs on every boot): the probe must
    // detect this node is absent from the live configuration and atomically re-add it.
    new KubernetesAutoJoin(getServer(followerIndex), followerRaft.getRaftGroup(),
        followerRaft.getLocalPeerId(), followerRaft.getRaftProperties()).tryAutoJoin();

    Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> {
          assertThat(leaderRaft.getLivePeers())
              .as("removed peer must be re-added to the committed configuration by the self-check probe")
              .hasSize(3);
          assertThat(followerRaft.getLeaderId()).isNotNull();
        });
  }
}
