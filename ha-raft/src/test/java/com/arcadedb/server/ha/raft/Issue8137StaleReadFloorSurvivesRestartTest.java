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
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #8137: the per-database stale read floor a snapshot install publishes for a database it
 * gave up on (#6760) was in-memory only. The quarantine beside it became durable with #7735, so a restart brought the
 * node back unready for that database - but with its LINEARIZABLE / read-your-writes reads unclamped, served from the
 * copy the install had left behind to any client reaching the node directly.
 * <p>
 * The invariant: <b>a floor lives exactly as long as its quarantine, across restarts too.</b>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8137StaleReadFloorSurvivesRestartTest {
  private static final String STALE   = "orders";
  private static final String HEALTHY = "invoices";

  @Test
  void theFloorOfAnIncompleteInstallSurvivesARestart(@TempDir final Path tempDir) throws IOException {
    final ArcadeStateMachine before = newStateMachine(tempDir);
    try {
      before.writePersistedAppliedIndex(40L, STALE);
      before.writePersistedAppliedIndex(90L, HEALTHY);
      before.markDatabasesNotAtSnapshotIndex(Set.of(STALE), 100L);
      assertThat(before.getDatabaseAppliedFloor(STALE)).isEqualTo(40L);
    } finally {
      before.close();
    }

    final ArcadeStateMachine after = newStateMachine(tempDir);
    try {
      assertThat(after.getDatabaseAppliedFloor(STALE)).as("the restart must keep clamping the reads of the stale copy")
          .isEqualTo(40L);
      assertThat(after.getDatabaseAppliedFloor(HEALTHY)).as("and only its reads").isEqualTo(-1L);
      assertThat(after.isDatabaseDiverged(STALE)).isTrue();
    } finally {
      after.close();
    }
  }

  @Test
  void theFloorAndTheQuarantineAreWrittenTogether(@TempDir final Path tempDir) throws IOException {
    final ArcadeStateMachine sm = newStateMachine(tempDir);
    try {
      sm.writePersistedAppliedIndex(40L, STALE);
      sm.markDatabasesNotAtSnapshotIndex(Set.of(STALE), 100L);
    } finally {
      sm.close();
    }

    final JSONObject persisted = new JSONObject(Files.readString(appliedIndexFile(tempDir)));
    assertThat(persisted.getJSONObject("quarantine").getString(STALE, null))
        .isEqualTo(DivergenceCause.SNAPSHOT_INSTALL_INCOMPLETE.name());
    assertThat(persisted.getJSONObject("floors").getLong(STALE, -1)).isEqualTo(40L);
  }

  @Test
  void aDatabaseAlreadyQuarantinedStillGetsItsFloorWritten(@TempDir final Path tempDir) throws IOException {
    // The quarantine write is skipped when nothing new is quarantined: the floor must not be skipped with it
    final ArcadeStateMachine before = newStateMachine(tempDir);
    try {
      before.writePersistedAppliedIndex(40L, STALE);
      before.markStateDiverged(STALE, DivergenceCause.WAL_VERSION_GAP);
      before.markDatabasesNotAtSnapshotIndex(Set.of(STALE), 100L);
    } finally {
      before.close();
    }

    final ArcadeStateMachine after = newStateMachine(tempDir);
    try {
      assertThat(after.getDatabaseAppliedFloor(STALE)).isEqualTo(40L);
      assertThat(after.getLocalResyncState().divergenceCauses()).containsEntry(STALE, DivergenceCause.WAL_VERSION_GAP);
    } finally {
      after.close();
    }
  }

  @Test
  void aTargetedResyncDropsTheFloorForTheNextStart(@TempDir final Path tempDir) throws IOException {
    final ArcadeStateMachine before = newStateMachine(tempDir);
    try {
      before.writePersistedAppliedIndex(40L, STALE);
      before.writePersistedAppliedIndex(50L, HEALTHY);
      before.markDatabasesNotAtSnapshotIndex(Set.of(STALE, HEALTHY), 100L);
      before.clearDivergedDatabase(STALE);
    } finally {
      before.close();
    }

    final ArcadeStateMachine after = newStateMachine(tempDir);
    try {
      assertThat(after.getDatabaseAppliedFloor(STALE)).as("the resync healed it").isEqualTo(-1L);
      assertThat(after.getDatabaseAppliedFloor(HEALTHY)).isEqualTo(50L);
    } finally {
      after.close();
    }
  }

  @Test
  void aFullResyncLeavesNoFloorBehind(@TempDir final Path tempDir) throws IOException {
    final ArcadeStateMachine before = newStateMachine(tempDir);
    try {
      before.writePersistedAppliedIndex(40L, STALE);
      before.markDatabasesNotAtSnapshotIndex(Set.of(STALE), 100L);
      before.clearDivergedState();
    } finally {
      before.close();
    }

    assertThat(new JSONObject(Files.readString(appliedIndexFile(tempDir))).has("floors")).isFalse();
    final ArcadeStateMachine after = newStateMachine(tempDir);
    try {
      assertThat(after.getDatabaseAppliedFloor(STALE)).isEqualTo(-1L);
      assertThat(after.isResyncInProgress()).isFalse();
    } finally {
      after.close();
    }
  }

  @Test
  void aQuarantineWrittenBeforeFloorsWerePersistedIsClampedAtItsAppliedPosition(@TempDir final Path tempDir)
      throws IOException {
    // What a build with #7735 but without this fix leaves on disk after an incomplete install
    writeAppliedIndexFile(tempDir, "{\"global\":100,\"db\":{\"" + STALE + "\":40,\"" + HEALTHY + "\":100},"
        + "\"quarantine\":{\"" + STALE + "\":\"SNAPSHOT_INSTALL_INCOMPLETE\"}}");

    final ArcadeStateMachine sm = newStateMachine(tempDir);
    try {
      assertThat(sm.getDatabaseAppliedFloor(STALE)).isEqualTo(40L);
      assertThat(sm.getDatabaseAppliedFloor(HEALTHY)).isEqualTo(-1L);
    } finally {
      sm.close();
    }
  }

  @Test
  void aFloorWithoutItsQuarantineIsIgnored(@TempDir final Path tempDir) throws IOException {
    writeAppliedIndexFile(tempDir, "{\"global\":100,\"db\":{},\"floors\":{\"" + STALE + "\":40}}");

    final ArcadeStateMachine sm = newStateMachine(tempDir);
    try {
      assertThat(sm.getDatabaseAppliedFloor(STALE)).isEqualTo(-1L);
    } finally {
      sm.close();
    }
  }

  @Test
  void aHealthyNodeWritesNoFloorsKey(@TempDir final Path tempDir) throws IOException {
    final ArcadeStateMachine sm = newStateMachine(tempDir);
    try {
      sm.writePersistedAppliedIndex(11L, STALE);
      assertThat(new JSONObject(Files.readString(appliedIndexFile(tempDir))).has("floors")).isFalse();
    } finally {
      sm.close();
    }
  }

  private static ArcadeStateMachine newStateMachine(final Path tempDir) {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, tempDir.resolve("databases").toString());
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(new ArcadeDBServer(config));
    return sm;
  }

  private static Path appliedIndexFile(final Path tempDir) {
    return tempDir.resolve("databases").resolve(".raft").resolve("applied-index");
  }

  private static void writeAppliedIndexFile(final Path tempDir, final String content) throws IOException {
    final Path file = appliedIndexFile(tempDir);
    Files.createDirectories(file.getParent());
    Files.writeString(file, content);
  }
}
