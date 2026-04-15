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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class LeaveClusterTest extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void transferLeadershipToSpecificPeer() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    final RaftHAServer leaderRaft = getRaftPlugin(leaderIndex).getRaftHAServer();
    final int targetIndex = leaderIndex == 0 ? 1 : 0;
    final String targetPeerId = peerIdForIndex(targetIndex);

    leaderRaft.transferLeadership(targetPeerId, 10_000);

    try {
      Thread.sleep(3_000);
    } catch (final InterruptedException ignored) {
    }

    final int newLeaderIndex = findLeaderIndex();
    assertThat(newLeaderIndex).isEqualTo(targetIndex);
  }

  @Test
  void stepDownTransfersToAnotherPeer() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    final RaftHAServer leaderRaft = getRaftPlugin(leaderIndex).getRaftHAServer();
    leaderRaft.stepDown();

    try {
      Thread.sleep(3_000);
    } catch (final InterruptedException ignored) {
    }

    final int newLeaderIndex = findLeaderIndex();
    assertThat(newLeaderIndex).isNotEqualTo(leaderIndex);
  }
}
