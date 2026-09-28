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

import org.apache.ratis.protocol.RaftPeerId;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8480 on a real 3-node cluster: {@code transferLeadership(timeoutMs)} with no target
 * returned {@code true} as soon as Ratis had stepped the leader down at the same term. At that moment the cluster had
 * no leader at all, the followers still named the ex-leader for a whole election timeout (5-10 s), and the election
 * that finally ran re-elected the same node in 7 rounds out of 8.
 * <p>
 * A {@code true} must mean leadership is already on another node when the call returns, and the whole cluster then
 * agrees on that node.
 */
@Tag("slow")
class Issue8480NoTargetTransferIsARealHandoffIT extends BaseRaftHATest {

  private static final int ROUNDS = 3;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void aNoTargetTransferHandsLeadershipToAnotherNode() {
    for (int round = 0; round < ROUNDS; round++) {
      final int exLeaderIndex = findLeaderIndex();
      assertThat(exLeaderIndex).as("round %d: a Raft leader must be elected", round).isGreaterThanOrEqualTo(0);
      final RaftHAServer exLeader = getRaftPlugin(exLeaderIndex).getRaftHAServer();
      final RaftPeerId exLeaderId = exLeader.getLocalPeerId();

      assertThat(exLeader.transferLeadership(10_000))
          .as("round %d: the no-target transfer must report a handoff", round).isTrue();

      // Checked the instant the call returns. The bare step-down it used to be left this null (no leader) here.
      final RaftPeerId newLeaderId = exLeader.getLeaderId();
      assertThat(newLeaderId).as("round %d: a leader must already be in place when the transfer returns true", round)
          .isNotNull();
      assertThat(newLeaderId).as("round %d: leadership must have moved to another node", round).isNotEqualTo(exLeaderId);

      // Every node converges on that same leader: the one the transfer handed off to, not an election that may pick
      // the ex-leader again.
      final int round0 = round;
      Awaitility.await("round " + round0 + ": every node names the new leader")
          .atMost(30, TimeUnit.SECONDS).pollInterval(50, TimeUnit.MILLISECONDS).until(() -> {
            for (int i = 0; i < getServerCount(); i++)
              if (!newLeaderId.equals(getRaftPlugin(i).getRaftHAServer().getLeaderId()))
                return false;
            return true;
          });
      assertThat(findLeaderIndex()).as("round %d: the ex-leader must not be the leader again", round)
          .isNotEqualTo(exLeaderIndex);
    }
  }
}
