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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A snapshot swap interrupted by a binary that recorded no phase (issue #8304) used to be preserved and never
 * recovered: the live directory was a partial mix that would not open, the pending marker kept the database from
 * loading, and the retained backup and marker made every later install refuse to run.
 * <p>
 * The layouts are built from real databases, the way the old binary leaves them: phase 1 moves each original into
 * {@code .snapshot-backup}, phase 2 moves each staged file over the live directory, and a kill between two moves
 * leaves whatever had moved so far.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8304LegacySwapRecoveryTest {

  /** Phase 2 killed part-way: every original is in the backup, some snapshot files are live, the rest still staged. */
  @Test
  void legacyKillBetweenTwoPhase2MovesRollsForwardToTheSnapshot(@TempDir final Path root) throws Exception {
    final Path databases = root.resolve("databases");
    final Path db = databases.resolve("mydb");
    final Path staged = db.resolve(".snapshot-new");
    final Path backup = db.resolve(".snapshot-backup");
    createDatabase(backup, "old");
    createDatabase(staged, "new");
    Files.writeString(db.resolve(".snapshot-pending"), "");
    Files.writeString(staged.resolve(".snapshot-complete"), "");
    final List<String> names = fileNames(staged);
    assertThat(names).hasSizeGreaterThan(2);
    // The first files move to the live directory, as in the report: the bucket, the configuration, the dictionary.
    for (final String name : names)
      if (!name.startsWith(".snapshot") && !name.startsWith("schema") && !name.startsWith("last-tx-id")
          && !name.startsWith("statistics"))
        Files.move(staged.resolve(name), db.resolve(name));
    assertThat(fileNames(db)).as("the layout under test has live snapshot files").anyMatch(n -> !n.startsWith(".snapshot"));
    assertThat(staged.resolve("schema.json")).exists();

    SnapshotInstaller.recoverPendingSnapshotSwaps(databases);
    SnapshotInstaller.recoverPendingSnapshotSwaps(databases);

    assertDatabaseValue(db, "new");
    assertThat(db.resolve(".snapshot-pending")).doesNotExist();
    assertThat(staged).doesNotExist();
    assertThat(backup).doesNotExist();
    assertThat(db.resolve(".snapshot-swap-state")).doesNotExist();
  }

  /** Phase 1 killed part-way: the backup holds the originals that moved, the live directory those that did not. */
  @Test
  void legacyKillBetweenTwoPhase1MovesRestoresTheOriginalDatabase(@TempDir final Path root) throws Exception {
    final Path databases = root.resolve("databases");
    final Path db = databases.resolve("mydb");
    final Path staged = db.resolve(".snapshot-new");
    final Path backup = db.resolve(".snapshot-backup");
    createDatabase(db, "old");
    createDatabase(staged, "new");
    Files.createDirectories(backup);
    Files.writeString(db.resolve(".snapshot-pending"), "");
    Files.writeString(staged.resolve(".snapshot-complete"), "");
    final List<String> originals = fileNames(db);
    int moved = 0;
    for (final String name : originals) {
      if (name.startsWith(".snapshot"))
        continue;
      if (moved++ % 2 == 0)
        Files.move(db.resolve(name), backup.resolve(name), StandardCopyOption.REPLACE_EXISTING);
    }
    assertThat(fileNames(backup)).isNotEmpty();
    assertThat(fileNames(db)).as("the layout under test keeps some originals live").anyMatch(n -> !n.startsWith(".snapshot"));

    SnapshotInstaller.recoverPendingSnapshotSwaps(databases);
    SnapshotInstaller.recoverPendingSnapshotSwaps(databases);

    assertDatabaseValue(db, "old");
    assertThat(db.resolve(".snapshot-pending")).doesNotExist();
    assertThat(staged).doesNotExist();
    assertThat(backup).doesNotExist();
  }

  /**
   * Disjoint names that are really a phase-2 crash: the only snapshot file that moved is one no original had. The
   * originals are restored and the node opens on them; the snapshot-only bucket, whose file id collides with an
   * original's, is moved aside to {@code .snapshot-orphans} because it would stop the database opening.
   */
  @Test
  void disjointLayoutFromAPhase2CrashStillRestoresTheOriginalDatabase(@TempDir final Path root) throws Exception {
    final Path databases = root.resolve("databases");
    final Path db = databases.resolve("mydb");
    final Path staged = db.resolve(".snapshot-new");
    final Path backup = db.resolve(".snapshot-backup");
    createDatabase(backup, "old");
    createDatabase(staged, "new");
    Files.writeString(db.resolve(".snapshot-pending"), "");
    Files.writeString(staged.resolve(".snapshot-complete"), "");
    // A real orphan: the bucket of a type only the snapshot has, moved over before the crash.
    createDatabase(root.resolve("other"), "other", "SnapshotOnly");
    for (final String name : fileNames(root.resolve("other")))
      if (name.startsWith("SnapshotOnly_"))
        Files.move(root.resolve("other").resolve(name), db.resolve(name));
    assertThat(fileNames(db)).anyMatch(n -> n.startsWith("SnapshotOnly_"));

    SnapshotInstaller.recoverPendingSnapshotSwaps(databases);

    assertDatabaseValue(db, "old");
    assertThat(fileNames(db)).noneMatch(n -> n.startsWith("SnapshotOnly_"));
    assertThat(fileNames(db.resolve(".snapshot-orphans"))).anyMatch(n -> n.startsWith("SnapshotOnly_"));
    assertThat(db.resolve(".snapshot-pending")).doesNotExist();
    assertThat(staged).doesNotExist();
    assertThat(backup).doesNotExist();
  }

  /** A crash after the backup was consumed but before the orphan quarantine still gets the orphan moved aside. */
  @Test
  void crashBetweenTheRestoreAndTheQuarantineIsResumed(@TempDir final Path root) throws Exception {
    final Path databases = root.resolve("databases");
    final Path db = databases.resolve("mydb");
    final Path staged = db.resolve(".snapshot-new");
    final Path backup = db.resolve(".snapshot-backup");
    createDatabase(backup, "old");
    createDatabase(staged, "new");
    Files.writeString(db.resolve(".snapshot-pending"), "");
    Files.writeString(staged.resolve(".snapshot-complete"), "");
    moveSnapshotOnlyBucketToLive(root, db);
    final AtomicBoolean crashed = new AtomicBoolean();
    SnapshotInstaller.swapProgressForTesting = point -> {
      if (point.equals("RESTORED") && crashed.compareAndSet(false, true))
        throw new SimulatedCrash();
    };
    try {
      assertThatThrownBy(() -> SnapshotInstaller.recoverPendingSnapshotSwaps(databases))
          .isInstanceOf(SimulatedCrash.class);
    } finally {
      SnapshotInstaller.swapProgressForTesting = null;
    }
    assertThat(crashed).isTrue();

    SnapshotInstaller.recoverPendingSnapshotSwaps(databases);

    assertDatabaseValue(db, "old");
    assertThat(fileNames(db)).noneMatch(n -> n.startsWith("SnapshotOnly_"));
    assertThat(db.resolve(".snapshot-pending")).doesNotExist();
    assertThat(db.resolve(".snapshot-quarantine")).doesNotExist();
  }

  /** The original database caught mid schema rewrite names its buckets in schema.prev.json only. */
  @Test
  void orphanQuarantineReadsSchemaPrevWhenSchemaIsAbsent(@TempDir final Path root) throws Exception {
    final Path databases = root.resolve("databases");
    final Path db = databases.resolve("mydb");
    final Path staged = db.resolve(".snapshot-new");
    final Path backup = db.resolve(".snapshot-backup");
    createDatabase(backup, "old");
    Files.move(backup.resolve("schema.json"), backup.resolve("schema.prev.json"), StandardCopyOption.REPLACE_EXISTING);
    createDatabase(staged, "new");
    Files.writeString(db.resolve(".snapshot-pending"), "");
    Files.writeString(staged.resolve(".snapshot-complete"), "");
    moveSnapshotOnlyBucketToLive(root, db);

    SnapshotInstaller.recoverPendingSnapshotSwaps(databases);

    assertDatabaseValue(db, "old");
    assertThat(fileNames(db)).noneMatch(n -> n.startsWith("SnapshotOnly_"));
    assertThat(db.resolve(".snapshot-pending")).doesNotExist();
  }

  private static void moveSnapshotOnlyBucketToLive(final Path root, final Path db) throws IOException {
    createDatabase(root.resolve("other"), "other", "SnapshotOnly");
    for (final String name : fileNames(root.resolve("other")))
      if (name.startsWith("SnapshotOnly_"))
        Files.move(root.resolve("other").resolve(name), db.resolve(name));
    assertThat(fileNames(db)).anyMatch(n -> n.startsWith("SnapshotOnly_"));
  }

  /** A node that crashes again part-way through the rollback finishes it on the next recovery. */
  @Test
  void rollbackInterruptedMidRestoreIsResumedByTheNextRecovery(@TempDir final Path root) throws Exception {
    final Path databases = root.resolve("databases");
    final Path db = databases.resolve("mydb");
    final Path staged = db.resolve(".snapshot-new");
    final Path backup = db.resolve(".snapshot-backup");
    createDatabase(db, "old");
    createDatabase(staged, "new");
    Files.createDirectories(backup);
    Files.writeString(db.resolve(".snapshot-pending"), "");
    Files.writeString(staged.resolve(".snapshot-complete"), "");
    createDatabase(root.resolve("other"), "other", "SnapshotOnly");
    for (final String name : fileNames(root.resolve("other")))
      if (name.startsWith("SnapshotOnly_"))
        Files.move(root.resolve("other").resolve(name), db.resolve(name));
    boolean first = true;
    for (final String name : fileNames(db)) {
      if (name.startsWith(".snapshot") || name.startsWith("SnapshotOnly_"))
        continue;
      if (first || name.startsWith("schema"))
        Files.move(db.resolve(name), backup.resolve(name));
      first = false;
    }
    final AtomicBoolean crashed = new AtomicBoolean();
    SnapshotInstaller.swapProgressForTesting = point -> {
      if (point.startsWith("RESTORING:") && crashed.compareAndSet(false, true))
        throw new SimulatedCrash();
    };
    try {
      assertThatThrownBy(() -> SnapshotInstaller.recoverPendingSnapshotSwaps(databases))
          .isInstanceOf(SimulatedCrash.class);
    } finally {
      SnapshotInstaller.swapProgressForTesting = null;
    }
    assertThat(crashed).isTrue();

    SnapshotInstaller.recoverPendingSnapshotSwaps(databases);

    assertDatabaseValue(db, "old");
    assertThat(fileNames(db)).noneMatch(n -> n.startsWith("SnapshotOnly_"));
    assertThat(db.resolve(".snapshot-pending")).doesNotExist();
    assertThat(backup).doesNotExist();
  }

  // Bypasses IOException handling, like process death.
  private static final class SimulatedCrash extends Error {
  }

  /** A restore that does not produce a database keeps the pending marker, so nothing is accepted as healthy. */
  @Test
  void rollbackThatRecoversNoDatabaseKeepsThePendingMarker(@TempDir final Path root) throws Exception {
    final Path databases = root.resolve("databases");
    final Path db = databases.resolve("mydb");
    final Path staged = db.resolve(".snapshot-new");
    final Path backup = db.resolve(".snapshot-backup");
    Files.createDirectories(staged);
    Files.createDirectories(backup);
    Files.writeString(db.resolve(".snapshot-pending"), "");
    Files.writeString(staged.resolve(".snapshot-complete"), "");
    Files.writeString(staged.resolve("C.0.bucket"), "new-C");
    Files.writeString(backup.resolve("A.0.bucket"), "old-A");
    Files.writeString(db.resolve("B.0.bucket"), "old-B");

    SnapshotInstaller.recoverPendingSnapshotSwaps(databases);

    assertThat(db.resolve("A.0.bucket")).hasContent("old-A");
    assertThat(db.resolve("B.0.bucket")).hasContent("old-B");
    assertThat(db.resolve(".snapshot-pending")).exists();
  }

  private static List<String> fileNames(final Path dir) throws IOException {
    final List<String> names = new ArrayList<>();
    try (final DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
      for (final Path entry : stream)
        names.add(entry.getFileName().toString());
    }
    return names;
  }

  private static void createDatabase(final Path path, final String value) {
    createDatabase(path, value, "Item");
  }

  private static void createDatabase(final Path path, final String value, final String typeName) {
    try (final DatabaseFactory factory = new DatabaseFactory(path.toString()); final Database db = factory.create()) {
      db.transaction(() -> {
        db.getSchema().createDocumentType(typeName, 1);
        db.newDocument(typeName).set("value", value).save();
      });
    }
  }

  private static void assertDatabaseValue(final Path path, final String value) {
    try (final DatabaseFactory factory = new DatabaseFactory(path.toString()); final Database db = factory.open()) {
      assertThat(db.countType("Item", false)).isEqualTo(1);
      assertThat(db.iterateType("Item", false).next().asDocument().getString("value")).isEqualTo(value);
    }
  }
}
