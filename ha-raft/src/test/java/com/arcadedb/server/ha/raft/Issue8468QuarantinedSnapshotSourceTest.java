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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for issue #8468: the quarantine a snapshot-serving node consults before it serves a database.
 * {@code Issue8468QuarantinedSnapshotSourceIT} drives the refusal through the HTTP handler on a real cluster.
 */
class Issue8468QuarantinedSnapshotSourceTest {

  private static final String DB = "db-a";

  @Test
  void aHealthyDatabaseHasNoQuarantineCause(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newStateMachine(tempDir);
    try {
      assertThat(sm.quarantineCause(DB)).isNull();
      assertThat(sm.quarantineCause(null)).isNull();
    } finally {
      sm.close();
    }
  }

  @Test
  void aQuarantinedDatabaseReportsItsCauseUntilTheResyncClearsIt(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newStateMachine(tempDir);
    try {
      sm.markStateDiverged(DB, DivergenceCause.WAL_VERSION_GAP);
      assertThat(sm.quarantineCause(DB)).isEqualTo(DivergenceCause.WAL_VERSION_GAP);
      assertThat(sm.quarantineCause("db-b")).as("a quarantine is per database").isNull();

      sm.clearDivergedDatabase(DB);
      assertThat(sm.quarantineCause(DB)).isNull();
    } finally {
      sm.close();
    }
  }

  @Test
  void aDatabaseAnInstallGaveUpOnIsReportedAsQuarantined(@TempDir final Path tempDir) throws Exception {
    // Its copy is behind the snapshot index the node's applied index already reached, the same overstatement.
    final ArcadeStateMachine sm = newStateMachine(tempDir);
    try {
      sm.settleDivergedStateAfterInstall(Set.of(DB), 100L);
      assertThat(sm.quarantineCause(DB)).isEqualTo(DivergenceCause.SNAPSHOT_INSTALL_INCOMPLETE);
    } finally {
      sm.close();
    }
  }

  @Test
  void aQuarantineRestoredFromDiskIsSeenByTheFirstCaller(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine before = newStateMachine(tempDir);
    try {
      before.markStateDiverged(DB, DivergenceCause.APPLY_ERROR);
    } finally {
      before.close();
    }
    final ArcadeStateMachine after = newStateMachine(tempDir);
    try {
      assertThat(after.quarantineCause(DB)).as("a restart must not serve the copy it quarantined")
          .isEqualTo(DivergenceCause.APPLY_ERROR);
    } finally {
      after.close();
    }
  }

  @Test
  void aNodeWithoutRaftHasNothingToRefuse() {
    assertThat(SnapshotHttpHandler.servedDatabaseQuarantine(null, DB)).isNull();
  }

  private static ArcadeStateMachine newStateMachine(final Path tempDir) {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, tempDir.resolve("databases").toString());
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(new ArcadeDBServer(config));
    return sm;
  }
}
