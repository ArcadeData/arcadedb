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

import com.arcadedb.exception.ConfigurationException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for the Raft membership quorum guard (issue #4796): removing a peer (or leaving the
 * cluster) must not drop the configuration below {@code floor(total/2)+1} voters unless forced.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RaftQuorumGuardTest {

  @Test
  void quorumMath() {
    assertThat(RaftClusterManager.quorumOf(1)).isEqualTo(1);
    assertThat(RaftClusterManager.quorumOf(2)).isEqualTo(2);
    assertThat(RaftClusterManager.quorumOf(3)).isEqualTo(2);
    assertThat(RaftClusterManager.quorumOf(4)).isEqualTo(3);
    assertThat(RaftClusterManager.quorumOf(5)).isEqualTo(3);
    assertThat(RaftClusterManager.quorumOf(7)).isEqualTo(4);
  }

  @Test
  void allowsRemovalThatKeepsQuorum() {
    // 3 -> 2 keeps a majority (quorum of 3 is 2)
    assertThatCode(() -> RaftClusterManager.ensureQuorumPreserved("peer3", 3, 2, false))
        .doesNotThrowAnyException();
    // 5 -> 4 and 5 -> 3 both keep quorum (quorum of 5 is 3)
    assertThatCode(() -> RaftClusterManager.ensureQuorumPreserved("peer5", 5, 4, false))
        .doesNotThrowAnyException();
    assertThatCode(() -> RaftClusterManager.ensureQuorumPreserved("peer4", 5, 3, false))
        .doesNotThrowAnyException();
  }

  @Test
  void refusesRemovalThatBreaksQuorum() {
    // 2 -> 1 drops below quorum (quorum of 2 is 2)
    assertThatThrownBy(() -> RaftClusterManager.ensureQuorumPreserved("peer2", 2, 1, false))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("Refusing to remove peer peer2")
        .hasMessageContaining("force=true");

    // 3 -> 1 (e.g. removing two in one step) drops below quorum
    assertThatThrownBy(() -> RaftClusterManager.ensureQuorumPreserved("peerX", 3, 1, false))
        .isInstanceOf(ConfigurationException.class);

    // 1 -> 0 leaves no voters
    assertThatThrownBy(() -> RaftClusterManager.ensureQuorumPreserved("peer1", 1, 0, false))
        .isInstanceOf(ConfigurationException.class);
  }

  @Test
  void forceBypassesGuard() {
    assertThatCode(() -> RaftClusterManager.ensureQuorumPreserved("peer2", 2, 1, true))
        .doesNotThrowAnyException();
    assertThatCode(() -> RaftClusterManager.ensureQuorumPreserved("peer1", 1, 0, true))
        .doesNotThrowAnyException();
  }
}
