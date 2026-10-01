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

import com.arcadedb.server.ArcadeDBServer;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The bootstrap election's transfer to the elected source is capped to a hand-off candidate's slice and only issued
 * to a source proven reachable (issue #8714).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8714BootstrapTransferBudgetTest {

  @Test
  void transferIsCappedToACandidateSlice() {
    final RaftHAServer ha = mock(RaftHAServer.class);
    when(ha.followerContactPeers()).thenReturn(Set.of("peer-b"));
    final BootstrapElection election = new BootstrapElection(ha, mock(ArcadeDBServer.class));

    election.transferToElectedSource("peer-b", 120_000L);

    verify(ha).transferLeadership("peer-b", RaftClusterManager.candidateTransferBudgetMs(120_000L, 120_000L));
    assertThat(RaftClusterManager.candidateTransferBudgetMs(120_000L, 120_000L)).isLessThan(120_000L);
  }

  @Test
  void unreachableSourceIsNeverTransferredTo() {
    final RaftHAServer ha = mock(RaftHAServer.class);
    when(ha.followerContactPeers()).thenReturn(Set.of("peer-c"));
    final BootstrapElection election = new BootstrapElection(ha, mock(ArcadeDBServer.class));

    assertThatThrownBy(() -> election.transferToElectedSource("peer-b", 120_000L, 150L)).isInstanceOf(IllegalStateException.class);

    verify(ha, never()).transferLeadership(anyString(), anyLong());
  }
}
