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

import com.arcadedb.server.CallLog;
import com.arcadedb.server.TestServerHelper;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The bootstrap election's transfer to the elected source is capped to a hand-off candidate's slice and only issued
 * to a source proven reachable (issue #8714).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8714BootstrapTransferBudgetTest {

  @Test
  void transferIsCappedToACandidateSlice() {
    final FakeRaftHAServer ha = FakeRaftHAServer.detached();
    ha.returns("followerContactPeers", Set.of("peer-b"));
    final BootstrapElection election = new BootstrapElection(ha, TestServerHelper.unstartedServer());

    election.transferToElectedSource("peer-b", 120_000L);

    assertThat(ha.calls("transferLeadership")).containsOnlyOnce(Arrays.asList("peer-b", RaftClusterManager.candidateTransferBudgetMs(120_000L, 120_000L)));
  }

  @Test
  void aSourceThatBecomesReachableAfterAFewPollsIsTransferredTo() {
    final FakeRaftHAServer ha = FakeRaftHAServer.detached();
    ha.on("followerContactPeers", CallLog.inOrder(Set.of(), Set.of(), Set.of("peer-b")));
    final BootstrapElection election = new BootstrapElection(ha, TestServerHelper.unstartedServer());

    election.transferToElectedSource("peer-b", 120_000L, 5_000L);

    assertThat(ha.calls("transferLeadership")).containsOnlyOnce(Arrays.asList("peer-b", RaftClusterManager.candidateTransferBudgetMs(120_000L, 120_000L)));
  }

  @Test
  void unreachableSourceIsNeverTransferredTo() {
    final FakeRaftHAServer ha = FakeRaftHAServer.detached();
    ha.returns("followerContactPeers", Set.of("peer-c"));
    final BootstrapElection election = new BootstrapElection(ha, TestServerHelper.unstartedServer());

    final AtomicBoolean announced = new AtomicBoolean();
    assertThatThrownBy(() -> election.transferToElectedSource("peer-b", 120_000L, 150L, () -> announced.set(true)))
        .isInstanceOf(IllegalStateException.class);
    assertThat(announced).as("the hold is not replaced when no transfer is issued").isFalse();

    assertThat(ha.calls("transferLeadership")).isEmpty();
  }
}
