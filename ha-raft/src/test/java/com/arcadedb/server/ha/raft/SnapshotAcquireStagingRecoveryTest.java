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
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests crash recovery of an interrupted <i>new-database acquisition</i> (issue #4727). When a node crashes
 * while downloading a database it has never seen, the partial download lives under the reserved staging dir
 * {@code databases/.acquire-<name>/}. On the next startup {@link SnapshotInstaller#recoverPendingSnapshotSwaps}
 * must delete it without leaving a half-written non-reserved {@code databases/<name>/} that the boot scan would
 * try to open. No HTTP downloads or Raft clusters are involved.
 */
class SnapshotAcquireStagingRecoveryTest {

  @Test
  void acquireStagingPrefixIsReserved() {
    // The startup scan (ArcadeDBServer.loadDatabases) skips reserved ('.'-prefixed) directories, which is what
    // keeps a half-written acquisition from being opened as a database. Guard that invariant.
    assertThat(ArcadeDBServer.isReservedDatabaseName(SnapshotInstaller.ACQUIRE_STAGING_PREFIX + "mydb")).isTrue();
  }

  @Test
  void recoveryDeletesInterruptedAcquireStaging(@TempDir final Path databasesDir) throws Exception {
    // Simulate a crash mid-download of a never-seen database: only the reserved staging dir exists, with
    // partial content. There is no final databases/mydb directory.
    final Path staging = databasesDir.resolve(SnapshotInstaller.ACQUIRE_STAGING_PREFIX + "mydb");
    Files.createDirectories(staging);
    Files.writeString(staging.resolve("partial.bucket"), "incomplete-download");

    SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir);

    // The staging dir is cleaned up and no non-reserved database directory was published.
    assertThat(staging).doesNotExist();
    assertThat(databasesDir.resolve("mydb")).doesNotExist();
  }

  @Test
  void recoveryLeavesCompletedDatabasesUntouched(@TempDir final Path databasesDir) throws Exception {
    // A fully-acquired database (final name, no staging) sits next to a leftover staging dir from a different,
    // interrupted acquisition. Recovery must clean only the staging dir.
    final Path liveDb = databasesDir.resolve("livedb");
    Files.createDirectories(liveDb);
    Files.writeString(liveDb.resolve("data.dat"), "live-data");

    final Path staging = databasesDir.resolve(SnapshotInstaller.ACQUIRE_STAGING_PREFIX + "otherdb");
    Files.createDirectories(staging);
    Files.writeString(staging.resolve("partial.bucket"), "incomplete");

    SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir);

    assertThat(staging).doesNotExist();
    assertThat(liveDb.resolve("data.dat")).exists();
    assertThat(Files.readString(liveDb.resolve("data.dat"))).isEqualTo("live-data");
  }

  @Test
  void recoveryHandlesAcquireStagingAndPendingSwapTogether(@TempDir final Path databasesDir) throws Exception {
    // An interrupted acquisition (reserved staging) and an interrupted in-place refresh (pending swap with a
    // retained backup) coexist. Each must be recovered independently in a single pass.
    final Path staging = databasesDir.resolve(SnapshotInstaller.ACQUIRE_STAGING_PREFIX + "newdb");
    Files.createDirectories(staging);
    Files.writeString(staging.resolve("partial.bucket"), "incomplete");

    final Path refreshDb = databasesDir.resolve("refreshdb");
    final Path snapshotNew = refreshDb.resolve(".snapshot-new");
    final Path snapshotBackup = refreshDb.resolve(".snapshot-backup");
    Files.createDirectories(snapshotNew);
    Files.createDirectories(snapshotBackup);
    Files.writeString(refreshDb.resolve(".snapshot-pending"), "");
    Files.writeString(snapshotNew.resolve(".snapshot-complete"), "");
    Files.writeString(snapshotNew.resolve("data.dat"), "new-data");
    Files.writeString(snapshotBackup.resolve("data.dat"), "old-data");

    SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir);

    // Acquisition staging cleaned, no phantom newdb.
    assertThat(staging).doesNotExist();
    assertThat(databasesDir.resolve("newdb")).doesNotExist();
    // In-place refresh swap completed.
    assertThat(Files.readString(refreshDb.resolve("data.dat"))).isEqualTo("new-data");
    assertThat(snapshotBackup).doesNotExist();
    assertThat(refreshDb.resolve(".snapshot-pending")).doesNotExist();
  }
}
