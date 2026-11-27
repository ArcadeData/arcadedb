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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression for issue #13618: on NFS/EFS a directory walk that runs while its entries are renamed or deleted can skip
 * entries. Every step of the snapshot swap moved or deleted entries in ONE walk of the directory it was reading, took the
 * source as drained, and deleted it recursively, so the entries the walk skipped were lost: staged snapshot files on the
 * install, and originals of the retained backup on a rollback (62 bucket files and a stale dictionary in the report).
 * <p>
 * The skipping filesystem is modelled with {@link SnapshotInstaller#directoryListingForTesting}, which drops entries from
 * the listings the swap takes.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class SnapshotSwapSkippingListingTest {

  private static final int BUCKETS = 30;

  @BeforeEach
  void snapshotsOpen() {
    SnapshotInstaller.snapshotOpensForTesting = path -> true;
  }

  @AfterEach
  void clearHooks() {
    SnapshotInstaller.snapshotOpensForTesting = null;
    SnapshotInstaller.directoryListingForTesting = null;
    SnapshotInstaller.swapProgressForTesting = null;
  }

  /** The first listing of each directory drops every third entry, the way a readdir walk under renames can on NFS. */
  private static void skipEveryThirdEntryOnTheFirstListingOfEachDirectory() {
    final Set<Path> listedOnce = new HashSet<>();
    SnapshotInstaller.directoryListingForTesting = (dir, entries) -> {
      if (!listedOnce.add(dir))
        return entries;
      final List<Path> kept = new ArrayList<>();
      for (int i = 0; i < entries.size(); i++)
        if (i % 3 != 1)
          kept.add(entries.get(i));
      return kept;
    };
  }

  private static Map<String, String> originals() {
    final Map<String, String> files = new LinkedHashMap<>();
    files.put("schema.json", "{\"old\":true}");
    files.put("dictionary.0.327680.v0.dict", "old-dictionary");
    for (int i = 0; i < BUCKETS; i++)
      files.put("Link" + i + "_0." + (100 + i) + ".65536.v0.bucket", "old-" + i);
    return files;
  }

  private static Map<String, String> snapshot() {
    final Map<String, String> files = new LinkedHashMap<>();
    files.put("schema.json", "{\"new\":true}");
    files.put("dictionary.0.65536.v1.dict", "new-dictionary");
    for (int i = 0; i < BUCKETS; i++)
      files.put("Link" + i + "_0." + (100 + i) + ".65536.v0.bucket", "new-" + i);
    return files;
  }

  private static void write(final Path dir, final Map<String, String> files) throws IOException {
    Files.createDirectories(dir);
    for (final Map.Entry<String, String> file : files.entrySet())
      Files.writeString(dir.resolve(file.getKey()), file.getValue());
  }

  private static Map<String, String> databaseFiles(final Path dir) throws IOException {
    final Map<String, String> files = new HashMap<>();
    try (final var stream = Files.list(dir)) {
      for (final Path entry : stream.toList())
        if (Files.isRegularFile(entry) && !SnapshotInstaller.isSnapshotMachineryEntryName(entry.getFileName().toString()))
          files.put(entry.getFileName().toString(), Files.readString(entry));
    }
    return files;
  }

  // Regression for #13618: phase 1 backs up every original and phase 2 installs every staged file
  @Test
  void swapMovesEveryEntryWhenTheFirstListingSkipsSome(@TempDir final Path databasesDir) throws IOException {
    final Path dbDir = databasesDir.resolve("drill");
    final Path snapshotNew = dbDir.resolve(SnapshotInstaller.SNAPSHOT_NEW_DIR);
    final Path snapshotBackup = dbDir.resolve(SnapshotInstaller.SNAPSHOT_BACKUP_DIR);
    write(dbDir, originals());
    write(snapshotNew, snapshot());
    Files.writeString(snapshotNew.resolve(SnapshotInstaller.SNAPSHOT_COMPLETE_FILE), "");

    skipEveryThirdEntryOnTheFirstListingOfEachDirectory();
    SnapshotInstaller.atomicSwap(dbDir, snapshotNew, snapshotBackup);

    assertThat(databaseFiles(dbDir)).as("every staged file is live, and no original is left among them").isEqualTo(snapshot());
    assertThat(databaseFiles(snapshotBackup)).as("every original is in the retained backup").isEqualTo(originals());
    assertThat(snapshotNew).as("the staging was drained before it was removed").doesNotExist();
  }

  // Regression for #13618: the rollback clears every snapshot file and restores every original before the backup goes
  @Test
  void rollbackRestoresEveryOriginalWhenTheFirstListingSkipsSome(@TempDir final Path databasesDir) throws IOException {
    final Path dbDir = databasesDir.resolve("drill");
    final Path snapshotBackup = dbDir.resolve(SnapshotInstaller.SNAPSHOT_BACKUP_DIR);
    write(dbDir, snapshot());
    write(snapshotBackup, originals());
    Files.writeString(dbDir.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE), "");
    Files.writeString(dbDir.resolve(SnapshotInstaller.SNAPSHOT_SWAP_STATE_FILE), "ROLLING_BACK");

    skipEveryThirdEntryOnTheFirstListingOfEachDirectory();
    SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir);

    assertThat(databaseFiles(dbDir)).as("every original is back and no snapshot-only file (the v1 dictionary) is left")
        .isEqualTo(originals());
    assertThat(snapshotBackup).doesNotExist();
    assertThat(dbDir.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE)).doesNotExist();
  }

  // Regression for #13618: a backup that still holds an original is never deleted, and the recovery state is kept
  @Test
  void backupThatStillHoldsAnEntryIsNeverDeleted(@TempDir final Path databasesDir) throws IOException {
    final Path dbDir = databasesDir.resolve("drill");
    final Path snapshotBackup = dbDir.resolve(SnapshotInstaller.SNAPSHOT_BACKUP_DIR);
    write(dbDir, snapshot());
    write(snapshotBackup, originals());
    Files.writeString(dbDir.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE), "");
    Files.writeString(dbDir.resolve(SnapshotInstaller.SNAPSHOT_SWAP_STATE_FILE), "ROLLING_BACK");

    // Every listing of the backup misses one original: no number of fresh listings finds it.
    final String hidden = "Link7_0.107.65536.v0.bucket";
    SnapshotInstaller.directoryListingForTesting = (dir, entries) -> dir.equals(snapshotBackup) ?
        entries.stream().filter(entry -> !entry.getFileName().toString().equals(hidden)).toList() :
        entries;
    SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir);

    assertThat(snapshotBackup.resolve(hidden)).as("the original the listings missed is still in the backup").exists();
    assertThat(Files.readString(snapshotBackup.resolve(hidden))).isEqualTo("old-7");
    assertThat(dbDir.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE)).as("the recovery hook is kept").exists();
    assertThat(dbDir.resolve(SnapshotInstaller.SNAPSHOT_SWAP_STATE_FILE)).exists();

    // Once the backup lists completely again, the next recovery finishes the restore.
    SnapshotInstaller.directoryListingForTesting = null;
    SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir);
    assertThat(databaseFiles(dbDir)).isEqualTo(originals());
    assertThat(snapshotBackup).doesNotExist();
    assertThat(dbDir.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE)).doesNotExist();
  }

  // Regression for #13618: a staged file the listings never show is kept, not deleted with the staging
  @Test
  void stagingThatStillHoldsAnEntryIsNotDeleted(@TempDir final Path databasesDir) throws IOException {
    final Path dbDir = databasesDir.resolve("drill");
    final Path snapshotNew = dbDir.resolve(SnapshotInstaller.SNAPSHOT_NEW_DIR);
    final Path snapshotBackup = dbDir.resolve(SnapshotInstaller.SNAPSHOT_BACKUP_DIR);
    write(dbDir, originals());
    write(snapshotNew, snapshot());
    Files.writeString(snapshotNew.resolve(SnapshotInstaller.SNAPSHOT_COMPLETE_FILE), "");

    final String hidden = "Link4_0.104.65536.v0.bucket";
    SnapshotInstaller.directoryListingForTesting = (dir, entries) -> dir.equals(snapshotNew) ?
        entries.stream().filter(entry -> !entry.getFileName().toString().equals(hidden)).toList() :
        entries;
    SnapshotInstaller.atomicSwap(dbDir, snapshotNew, snapshotBackup);

    assertThat(snapshotNew.resolve(hidden)).as("the staged file the listings missed is kept for the validation to see").exists();
    final Map<String, Long> expected = new HashMap<>();
    for (final Map.Entry<String, String> file : snapshot().entrySet())
      expected.put(file.getKey(), (long) file.getValue().length());
    assertThat(SnapshotInstaller.installedFileMismatches(expected, dbDir)).contains("'" + hidden + "' is missing");
  }

  // Regression for #13618: a stale listing (a name already moved) is skipped rather than failing the swap
  @Test
  void staleListedEntryIsSkipped(@TempDir final Path databasesDir) throws IOException {
    final Path dbDir = databasesDir.resolve("drill");
    final Path snapshotNew = dbDir.resolve(SnapshotInstaller.SNAPSHOT_NEW_DIR);
    final Path snapshotBackup = dbDir.resolve(SnapshotInstaller.SNAPSHOT_BACKUP_DIR);
    write(dbDir, originals());
    write(snapshotNew, snapshot());
    Files.writeString(snapshotNew.resolve(SnapshotInstaller.SNAPSHOT_COMPLETE_FILE), "");

    final Set<Path> listedOnce = new HashSet<>();
    SnapshotInstaller.directoryListingForTesting = (dir, entries) -> {
      if (!dir.equals(snapshotNew) || !listedOnce.add(dir))
        return entries;
      final List<Path> withGhost = new ArrayList<>(entries);
      withGhost.add(dir.resolve("Ghost_0.999.65536.v0.bucket"));
      return withGhost;
    };
    SnapshotInstaller.atomicSwap(dbDir, snapshotNew, snapshotBackup);

    assertThat(databaseFiles(dbDir)).isEqualTo(snapshot());
    assertThat(snapshotNew).doesNotExist();
  }

  // Regression for #13618: a listing that never stops naming a gone entry fails the swap, which restores the originals
  @Test
  void listingThatKeepsNamingAGoneEntryFailsInsteadOfLooping(@TempDir final Path databasesDir) throws IOException {
    final Path dbDir = databasesDir.resolve("drill");
    final Path snapshotNew = dbDir.resolve(SnapshotInstaller.SNAPSHOT_NEW_DIR);
    final Path snapshotBackup = dbDir.resolve(SnapshotInstaller.SNAPSHOT_BACKUP_DIR);
    write(dbDir, originals());
    write(snapshotNew, snapshot());
    Files.writeString(snapshotNew.resolve(SnapshotInstaller.SNAPSHOT_COMPLETE_FILE), "");

    SnapshotInstaller.directoryListingForTesting = (dir, entries) -> {
      if (!dir.equals(snapshotNew))
        return entries;
      final List<Path> withGhost = new ArrayList<>(entries);
      withGhost.add(dir.resolve("Ghost_0.999.65536.v0.bucket"));
      return withGhost;
    };

    assertThatThrownBy(() -> SnapshotInstaller.atomicSwap(dbDir, snapshotNew, snapshotBackup))
        .isInstanceOf(IOException.class)
        .hasRootCauseMessage("Cannot drain " + snapshotNew + ": 1 listed entries no longer exist after 5 fresh listings "
            + "(first: 'Ghost_0.999.65536.v0.bucket')");
    assertThat(databaseFiles(dbDir)).as("the originals are restored").isEqualTo(originals());
  }

  // Regression for #13618: originals the backup phase never listed are moved to the backup, not overwritten or left live
  @Test
  void originalsTheBackupPhaseMissedAreMovedToTheBackup(@TempDir final Path databasesDir) throws IOException {
    final Path dbDir = databasesDir.resolve("drill");
    final Path snapshotNew = dbDir.resolve(SnapshotInstaller.SNAPSHOT_NEW_DIR);
    final Path snapshotBackup = dbDir.resolve(SnapshotInstaller.SNAPSHOT_BACKUP_DIR);
    write(dbDir, originals());
    write(snapshotNew, snapshot());
    SnapshotInstaller.writeInstalledFilesManifest(snapshotNew, SnapshotInstaller.expectedInstalledFiles(snapshotNew));
    Files.writeString(snapshotNew.resolve(SnapshotInstaller.SNAPSHOT_COMPLETE_FILE), "");

    // Every listing of the live directory taken by the backup phase misses one original the snapshot also ships and one
    // it does not (the v0 dictionary): fresh listings of the live directory see them again only once phase 2 started.
    final Set<String> hidden = Set.of("Link3_0.103.65536.v0.bucket", "dictionary.0.327680.v0.dict");
    final AtomicBoolean installing = new AtomicBoolean();
    SnapshotInstaller.directoryListingForTesting = (dir, entries) -> {
      if (dir.equals(snapshotNew))
        installing.set(true);
      if (!dir.equals(dbDir) || installing.get())
        return entries;
      return entries.stream().filter(entry -> !hidden.contains(entry.getFileName().toString())).toList();
    };
    SnapshotInstaller.atomicSwap(dbDir, snapshotNew, snapshotBackup);

    assertThat(databaseFiles(snapshotBackup)).as("every original is in the retained backup, the missed ones included")
        .isEqualTo(originals());
    assertThat(databaseFiles(dbDir)).as("no original is left live next to the snapshot").isEqualTo(snapshot());
  }

  // Regression for #13618: a swap that fails in phase 2 rescues the originals phase 1 missed before its rollback clears
  @Test
  void failedInstallRestoresTheOriginalsTheBackupPhaseMissed(@TempDir final Path databasesDir) throws IOException {
    final Path dbDir = databasesDir.resolve("drill");
    final Path snapshotNew = dbDir.resolve(SnapshotInstaller.SNAPSHOT_NEW_DIR);
    final Path snapshotBackup = dbDir.resolve(SnapshotInstaller.SNAPSHOT_BACKUP_DIR);
    write(dbDir, originals());
    write(snapshotNew, snapshot());
    SnapshotInstaller.writeInstalledFilesManifest(snapshotNew, SnapshotInstaller.expectedInstalledFiles(snapshotNew));
    Files.writeString(snapshotNew.resolve(SnapshotInstaller.SNAPSHOT_COMPLETE_FILE), "");

    // Phase 1 misses an original the snapshot does not ship (the v0 dictionary) and one it does (Link3), whose staged copy
    // the staging listings never show either; and the staging listings keep naming a gone entry, so phase 2 fails.
    final String missedSnapshotOnly = "dictionary.0.327680.v0.dict";
    final String missedStaged = "Link3_0.103.65536.v0.bucket";
    final AtomicBoolean installing = new AtomicBoolean();
    SnapshotInstaller.directoryListingForTesting = (dir, entries) -> {
      if (dir.equals(snapshotNew)) {
        installing.set(true);
        final List<Path> listed = new ArrayList<>(
            entries.stream().filter(entry -> !entry.getFileName().toString().equals(missedStaged)).toList());
        listed.add(dir.resolve("Ghost_0.999.65536.v0.bucket"));
        return listed;
      }
      if (!dir.equals(dbDir) || installing.get())
        return entries;
      return entries.stream()
          .filter(entry -> !entry.getFileName().toString().equals(missedSnapshotOnly) && !entry.getFileName().toString()
              .equals(missedStaged)).toList();
    };

    assertThatThrownBy(() -> SnapshotInstaller.atomicSwap(dbDir, snapshotNew, snapshotBackup)).isInstanceOf(IOException.class);

    assertThat(databaseFiles(dbDir)).as("every original is back, the two phase 1 missed included").isEqualTo(originals());
    assertThat(snapshotBackup).doesNotExist();
  }

  // Regression for #13618: an original no listing showed before the rollback's clear is kept by the clear itself
  @Test
  void rollbackClearKeepsAnOriginalOnlyItsOwnListingShows(@TempDir final Path databasesDir) throws IOException {
    final Path dbDir = databasesDir.resolve("drill");
    final Path snapshotNew = dbDir.resolve(SnapshotInstaller.SNAPSHOT_NEW_DIR);
    final Path snapshotBackup = dbDir.resolve(SnapshotInstaller.SNAPSHOT_BACKUP_DIR);
    write(dbDir, originals());
    write(snapshotNew, snapshot());
    SnapshotInstaller.writeInstalledFilesManifest(snapshotNew, SnapshotInstaller.expectedInstalledFiles(snapshotNew));
    Files.writeString(snapshotNew.resolve(SnapshotInstaller.SNAPSHOT_COMPLETE_FILE), "");

    // The v0 dictionary is missing from every listing of the live directory (the backup phase's and the rescue's) until
    // ROLLING_BACK is published, so the clear is the first to see it; the staging listings make phase 2 fail.
    final String missed = "dictionary.0.327680.v0.dict";
    final AtomicBoolean rollingBack = new AtomicBoolean();
    SnapshotInstaller.swapProgressForTesting = point -> {
      if ("ROLLING_BACK".equals(point))
        rollingBack.set(true);
    };
    SnapshotInstaller.directoryListingForTesting = (dir, entries) -> {
      if (dir.equals(snapshotNew)) {
        final List<Path> listed = new ArrayList<>(entries);
        listed.add(dir.resolve("Ghost_0.999.65536.v0.bucket"));
        return listed;
      }
      if (!dir.equals(dbDir) || rollingBack.get())
        return entries;
      return entries.stream().filter(entry -> !entry.getFileName().toString().equals(missed)).toList();
    };
    try {
      assertThatThrownBy(() -> SnapshotInstaller.atomicSwap(dbDir, snapshotNew, snapshotBackup)).isInstanceOf(IOException.class);
    } finally {
      SnapshotInstaller.swapProgressForTesting = null;
    }

    assertThat(databaseFiles(dbDir)).as("the original only the clear saw is restored, not deleted").isEqualTo(originals());
  }

  // Regression for #13618: recovery never accepts an install whose staging still holds staged files
  @Test
  void recoveryRollsBackAnInstallWhoseStagingStillHoldsFiles(@TempDir final Path databasesDir) throws IOException {
    final Path dbDir = databasesDir.resolve("drill");
    final Path snapshotNew = dbDir.resolve(SnapshotInstaller.SNAPSHOT_NEW_DIR);
    final Path snapshotBackup = dbDir.resolve(SnapshotInstaller.SNAPSHOT_BACKUP_DIR);
    // INSTALLED over a staging that kept one bucket and the manifest (both missed by the listings): no live manifest to
    // compare with, and the copy opens.
    final Map<String, String> live = new LinkedHashMap<>(snapshot());
    final String kept = "Link5_0.105.65536.v0.bucket";
    live.remove(kept);
    write(dbDir, live);
    write(snapshotNew, Map.of(kept, "new-5"));
    SnapshotInstaller.writeInstalledFilesManifest(snapshotNew, Map.of(kept, 5L));
    Files.writeString(snapshotNew.resolve(SnapshotInstaller.SNAPSHOT_COMPLETE_FILE), "");
    write(snapshotBackup, originals());
    Files.writeString(dbDir.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE), "");
    Files.writeString(dbDir.resolve(SnapshotInstaller.SNAPSHOT_SWAP_STATE_FILE), "INSTALLED");

    SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir);

    assertThat(databaseFiles(dbDir)).as("the incomplete install is rolled back to the originals").isEqualTo(originals());
    assertThat(snapshotBackup).doesNotExist();
    assertThat(dbDir.resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE)).doesNotExist();
  }

  // Regression for #13618: the mismatch report counts every file and says why each could not be matched
  @Test
  void mismatchReportNamesEveryFileWithItsReason(@TempDir final Path dbDir) throws IOException {
    Files.writeString(dbDir.resolve("a.bucket"), "12345");
    Files.writeString(dbDir.resolve("b.bucket"), "123");
    final Map<String, Long> expected = new HashMap<>();
    expected.put("a.bucket", 5L);
    expected.put("b.bucket", 5L);
    expected.put("c.bucket", 5L);
    expected.put("d.bucket", 5L);

    assertThat(SnapshotInstaller.installedFileMismatches(expected, dbDir)).isEqualTo(
        "3 of 4 files do not match: 'b.bucket' is 3 bytes instead of 5, 'c.bucket' is missing, 'd.bucket' is missing");

    expected.remove("c.bucket");
    expected.remove("d.bucket");
    assertThat(SnapshotInstaller.installedFileMismatches(expected, dbDir)).isEqualTo("'b.bucket' is 3 bytes instead of 5");

    expected.put("b.bucket", 3L);
    assertThat(SnapshotInstaller.installedFileMismatches(expected, dbDir)).isNull();
  }
}
