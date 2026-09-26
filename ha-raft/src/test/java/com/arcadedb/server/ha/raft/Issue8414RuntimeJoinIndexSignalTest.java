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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #8414 (#8382): the security-convergence readiness window is per join, and what tells the gate a new join
 * happened is {@code HAServerPlugin.getRuntimeJoinIndex()}. These tests pin the Raft implementation of that signal:
 * it is the detector's join index, it moves forward on a re-add and on a snapshot install that moves the join
 * boundary, and a plugin without a Raft server reports none.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue8414RuntimeJoinIndexSignalTest {

  private static final RaftPeerId SELF = RaftPeerId.valueOf("arcadedb-3");

  @TempDir
  File tempDir;

  /** Before any join the server reports none; the join and the re-add each move it forward. */
  @Test
  void theServerReportsTheJoinIndexAndARemoveAndReAddMovesItForward() {
    final RaftHAServer server = detachedServer();
    final RuntimeJoinDetector detector = server.getStateMachine().getRuntimeJoinDetector();
    assertThat(server.getRuntimeJoinIndex()).as("no join yet").isEqualTo(-1L);

    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-2"), List.of(), 5L);
    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-2", "arcadedb-3"),
        peers("arcadedb-0", "arcadedb-1", "arcadedb-2"), 10L);
    assertThat(server.getRuntimeJoinIndex()).as("the first join").isEqualTo(10L);

    // Removed, then re-added: the re-add's joint entry moves the join, which is what reopens the gate's window.
    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-2"), List.of(), 15L);
    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-2", "arcadedb-3"),
        peers("arcadedb-0", "arcadedb-1", "arcadedb-2"), 20L);
    assertThat(server.getRuntimeJoinIndex()).as("the re-add").isEqualTo(20L);
  }

  /** A snapshot install past the join is a join boundary of its own (issue #8353), so it moves the index too. */
  @Test
  void aSnapshotInstallThatMovesTheJoinBoundaryMovesTheIndex() {
    final RaftHAServer server = detachedServer();
    final RuntimeJoinDetector detector = server.getStateMachine().getRuntimeJoinDetector();
    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-2"), List.of(), 5L);
    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-2", "arcadedb-3"),
        peers("arcadedb-0", "arcadedb-1", "arcadedb-2"), 10L);

    detector.onSnapshotInstalledFromLeader(50L);

    assertThat(server.getRuntimeJoinIndex()).isEqualTo(49L);
  }

  /**
   * No Raft server, no join index. The gate reads this only on an armed reading, and counts only a join index that
   * moves forward, so {@code -1} can never restart its bound.
   */
  @Test
  void aPluginWithoutARaftServerReportsNoJoinIndex() {
    assertThat(new RaftHAPlugin().getRuntimeJoinIndex()).isEqualTo(-1L);
  }

  // -----------------------------------------------------------------------------------------------------------

  /** A {@link RaftHAServer} whose constructor has run but whose Ratis server was never started. */
  private RaftHAServer detachedServer() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, "arcadedb-0:2434:2480");
    config.setValue(GlobalConfiguration.HA_RAFT_STORAGE_DIRECTORY, tempDir.getAbsolutePath());
    config.setValue(GlobalConfiguration.HA_RAFT_PERSIST_STORAGE, false);
    final ArcadeDBServer arcadeServer = mock(ArcadeDBServer.class);
    when(arcadeServer.getServerName()).thenReturn("arcadedb-0");
    return new RaftHAServer(arcadeServer, config);
  }

  private static List<RaftPeerId> peers(final String... ids) {
    final List<RaftPeerId> peers = new ArrayList<>(ids.length);
    for (final String id : ids)
      peers.add(RaftPeerId.valueOf(id));
    return peers;
  }
}
