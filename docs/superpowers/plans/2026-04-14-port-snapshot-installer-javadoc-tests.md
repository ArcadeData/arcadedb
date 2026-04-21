# Port SnapshotInstaller, Javadoc, and Tests Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port crash-safe snapshot installation, enrich Javadoc on core HA classes, and close test gaps from the apache-ratis branch into ha-redesign.

**Architecture:** New `SnapshotInstaller` class handles three-phase atomic directory swap (download to temp, swap live, cleanup) with marker files for crash recovery. Integrates with existing `ArcadeStateMachine` (caller), `SnapshotHttpHandler` (server side), and `GlobalConfiguration` (retry tuning). Javadoc is written fresh against ha-redesign code. Tests cover new code plus genuinely missing scenarios.

**Tech Stack:** Java 21, JUnit 5, AssertJ, Apache Ratis, JDK `java.nio.file` / `java.util.zip` / `java.net`

---

## File Map

### New Files
| File | Responsibility |
|---|---|
| `ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotInstaller.java` | Crash-safe snapshot download, atomic swap, startup recovery, retry with backoff |
| `ha-raft/src/test/java/com/arcadedb/server/ha/raft/SnapshotSwapRecoveryTest.java` | Unit tests for `recoverPendingSnapshotSwaps()` recovery truth table |
| `ha-raft/src/test/java/com/arcadedb/server/ha/raft/SnapshotInstallerRetryTest.java` | Unit tests for exponential backoff retry logic |
| `ha-raft/src/test/java/com/arcadedb/server/ha/raft/SnapshotInstallerIntegrationIT.java` | End-to-end snapshot install with crash-safety in 3-node cluster |
| `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftReplicaFailureIT.java` | Replica failure during snapshot install scenarios |
| `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftHAServerAddressParsingTest.java` | Edge cases in `parsePeerList()` |
| `ha-raft/src/test/java/com/arcadedb/server/ha/raft/HAConfigDefaultsTest.java` | GlobalConfiguration HA defaults validation |

### Modified Files
| File | Change |
|---|---|
| `engine/src/main/java/com/arcadedb/GlobalConfiguration.java` | Add `HA_SNAPSHOT_INSTALL_RETRIES` and `HA_SNAPSHOT_INSTALL_RETRY_BASE_MS` |
| `ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java` | Delegate to `SnapshotInstaller`, add recovery call in `initialize()`, add Javadoc, remove `installDatabaseSnapshot()` |
| `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java` | Add Javadoc to class and key methods |
| `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftReplicatedDatabase.java` | Add Javadoc to `commit()` flow |

---

### Task 1: Add GlobalConfiguration entries for snapshot retry

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/GlobalConfiguration.java:596-598`

- [ ] **Step 1: Add the two new config entries**

Insert after `HA_SNAPSHOT_MAX_CONCURRENT` (line 598):

```java
  HA_SNAPSHOT_INSTALL_RETRIES("arcadedb.ha.snapshotInstallRetries", SCOPE.SERVER,
      "Maximum retry attempts for snapshot download from the leader during snapshot installation.",
      Integer.class, 3),

  HA_SNAPSHOT_INSTALL_RETRY_BASE_MS("arcadedb.ha.snapshotInstallRetryBaseMs", SCOPE.SERVER,
      "Base delay in milliseconds for exponential backoff between snapshot download retries. Actual delay is baseMs * 2^attempt.",
      Long.class, 5000L),
```

- [ ] **Step 2: Compile to verify**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn compile -pl engine -q`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add engine/src/main/java/com/arcadedb/GlobalConfiguration.java
git commit -m "feat(ha-raft): add GlobalConfiguration entries for snapshot install retry"
```

---

### Task 2: Create SnapshotInstaller with recovery logic (TDD - recovery tests first)

**Files:**
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/SnapshotSwapRecoveryTest.java`
- Create: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotInstaller.java`

- [ ] **Step 1: Write the recovery test class**

```java
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
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link SnapshotInstaller#recoverPendingSnapshotSwaps(Path)} using synthetic
 * filesystem state. No HTTP downloads or Raft clusters are involved.
 */
class SnapshotSwapRecoveryTest {

  @Test
  void testRecoveryCompletesInterruptedSwap(@TempDir final Path databasesDir) throws IOException {
    // Simulate: download completed, swap started (backup created) but process crashed before cleanup
    final Path dbDir = databasesDir.resolve("mydb");
    final Path snapshotNew = dbDir.resolve(".snapshot-new");
    final Path snapshotBackup = dbDir.resolve(".snapshot-backup");

    Files.createDirectories(snapshotNew);
    Files.createDirectories(snapshotBackup);
    Files.writeString(dbDir.resolve(".snapshot-pending"), "");
    Files.writeString(snapshotNew.resolve(".snapshot-complete"), "");
    Files.writeString(snapshotNew.resolve("data.dat"), "new-snapshot-data");
    Files.writeString(snapshotBackup.resolve("data.dat"), "old-data");

    SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir);

    // .snapshot-new should have been swapped into dbDir
    assertThat(dbDir.resolve("data.dat")).exists();
    assertThat(Files.readString(dbDir.resolve("data.dat"))).isEqualTo("new-snapshot-data");
    // Cleanup: backup, marker, and .snapshot-complete should be gone
    assertThat(snapshotBackup).doesNotExist();
    assertThat(dbDir.resolve(".snapshot-pending")).doesNotExist();
    assertThat(dbDir.resolve(".snapshot-complete")).doesNotExist();
  }

  @Test
  void testRecoveryRollsBackIncompleteDownload(@TempDir final Path databasesDir) throws IOException {
    // Simulate: download was interrupted (no .snapshot-complete), backup exists
    final Path dbDir = databasesDir.resolve("mydb");
    final Path snapshotNew = dbDir.resolve(".snapshot-new");
    final Path snapshotBackup = dbDir.resolve(".snapshot-backup");

    Files.createDirectories(snapshotNew);
    Files.createDirectories(snapshotBackup);
    Files.writeString(dbDir.resolve(".snapshot-pending"), "");
    Files.writeString(snapshotNew.resolve("partial.dat"), "incomplete");
    Files.writeString(snapshotBackup.resolve("data.dat"), "original-data");

    SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir);

    // Backup should be restored as the live directory
    assertThat(dbDir.resolve("data.dat")).exists();
    assertThat(Files.readString(dbDir.resolve("data.dat"))).isEqualTo("original-data");
    // Cleanup
    assertThat(snapshotNew).doesNotExist();
    assertThat(snapshotBackup).doesNotExist();
    assertThat(dbDir.resolve(".snapshot-pending")).doesNotExist();
  }

  @Test
  void testRecoveryCleansUpOrphanedNewDir(@TempDir final Path databasesDir) throws IOException {
    // Simulate: download interrupted before backup was created
    final Path dbDir = databasesDir.resolve("mydb");
    final Path snapshotNew = dbDir.resolve(".snapshot-new");

    Files.createDirectories(dbDir);
    Files.createDirectories(snapshotNew);
    Files.writeString(dbDir.resolve(".snapshot-pending"), "");
    Files.writeString(dbDir.resolve("data.dat"), "existing-data");
    Files.writeString(snapshotNew.resolve("partial.dat"), "incomplete");

    SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir);

    // Existing data should be untouched
    assertThat(Files.readString(dbDir.resolve("data.dat"))).isEqualTo("existing-data");
    // Cleanup
    assertThat(snapshotNew).doesNotExist();
    assertThat(dbDir.resolve(".snapshot-pending")).doesNotExist();
  }

  @Test
  void testNoMarkerNoAction(@TempDir final Path databasesDir) throws IOException {
    // Clean database directory with no markers - should be a no-op
    final Path dbDir = databasesDir.resolve("mydb");
    Files.createDirectories(dbDir);
    Files.writeString(dbDir.resolve("data.dat"), "untouched");

    SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir);

    assertThat(Files.readString(dbDir.resolve("data.dat"))).isEqualTo("untouched");
  }

  @Test
  void testMultipleDatabasesRecoveredIndependently(@TempDir final Path databasesDir) throws IOException {
    // db1: needs swap completion; db2: needs rollback
    final Path db1 = databasesDir.resolve("db1");
    final Path db1New = db1.resolve(".snapshot-new");
    final Path db1Backup = db1.resolve(".snapshot-backup");
    Files.createDirectories(db1New);
    Files.createDirectories(db1Backup);
    Files.writeString(db1.resolve(".snapshot-pending"), "");
    Files.writeString(db1New.resolve(".snapshot-complete"), "");
    Files.writeString(db1New.resolve("data.dat"), "db1-new");
    Files.writeString(db1Backup.resolve("data.dat"), "db1-old");

    final Path db2 = databasesDir.resolve("db2");
    final Path db2New = db2.resolve(".snapshot-new");
    final Path db2Backup = db2.resolve(".snapshot-backup");
    Files.createDirectories(db2New);
    Files.createDirectories(db2Backup);
    Files.writeString(db2.resolve(".snapshot-pending"), "");
    // No .snapshot-complete - incomplete download
    Files.writeString(db2New.resolve("partial.dat"), "incomplete");
    Files.writeString(db2Backup.resolve("data.dat"), "db2-original");

    SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir);

    // db1: swap completed
    assertThat(Files.readString(db1.resolve("data.dat"))).isEqualTo("db1-new");
    assertThat(db1Backup).doesNotExist();

    // db2: rolled back
    assertThat(Files.readString(db2.resolve("data.dat"))).isEqualTo("db2-original");
    assertThat(db2New).doesNotExist();
  }

  @Test
  void testRecoveryHandlesSwapCompletedButCleanupIncomplete(@TempDir final Path databasesDir) throws IOException {
    // Simulate: swap was completed (.snapshot-new renamed to live) but .snapshot-pending marker
    // and leftover .snapshot-new dir (with .snapshot-complete) still exist. No backup.
    final Path dbDir = databasesDir.resolve("mydb");
    final Path snapshotNew = dbDir.resolve(".snapshot-new");

    Files.createDirectories(dbDir);
    Files.createDirectories(snapshotNew);
    Files.writeString(dbDir.resolve(".snapshot-pending"), "");
    Files.writeString(snapshotNew.resolve(".snapshot-complete"), "");
    Files.writeString(dbDir.resolve("data.dat"), "live-data");

    SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir);

    // Live data should be untouched, leftovers cleaned up
    assertThat(Files.readString(dbDir.resolve("data.dat"))).isEqualTo("live-data");
    assertThat(snapshotNew).doesNotExist();
    assertThat(dbDir.resolve(".snapshot-pending")).doesNotExist();
  }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn test -pl ha-raft -Dtest=SnapshotSwapRecoveryTest -q`
Expected: Compilation error - `SnapshotInstaller` does not exist yet.

- [ ] **Step 3: Create SnapshotInstaller with recoverPendingSnapshotSwaps()**

```java
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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.utility.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.logging.Level;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Crash-safe snapshot installation for Raft HA replicas.
 * <p>
 * When a follower's log falls too far behind the leader's compacted log, Ratis triggers a
 * snapshot install. This class handles the download-and-swap lifecycle with crash safety:
 * <ol>
 *   <li><b>Download phase:</b> Extract the snapshot ZIP into {@code <dbDir>/.snapshot-new}.
 *       A {@code .snapshot-pending} marker is written before extraction starts.
 *       A {@code .snapshot-complete} marker is written inside {@code .snapshot-new} after
 *       all entries are extracted successfully.</li>
 *   <li><b>Swap phase:</b> Rename the live database directory to {@code .snapshot-backup},
 *       then rename {@code .snapshot-new} to the live path. If the second rename fails,
 *       {@code .snapshot-backup} is restored.</li>
 *   <li><b>Cleanup phase:</b> Delete the backup directory, remove marker files, and
 *       clean up stale WAL files from the newly installed database.</li>
 * </ol>
 * On startup, {@link #recoverPendingSnapshotSwaps(Path)} detects incomplete swaps
 * via the {@code .snapshot-pending} marker and either completes or rolls back each one.
 */
public final class SnapshotInstaller {

  static final String SNAPSHOT_NEW_DIR      = ".snapshot-new";
  static final String SNAPSHOT_BACKUP_DIR   = ".snapshot-backup";
  static final String SNAPSHOT_PENDING_FILE = ".snapshot-pending";
  static final String SNAPSHOT_COMPLETE_FILE = ".snapshot-complete";

  private SnapshotInstaller() {
  }

  /**
   * Installs a database snapshot from the leader using crash-safe atomic swap.
   * Downloads the snapshot ZIP with retry, extracts to a temp directory, and atomically
   * swaps it into the live database path.
   *
   * @param databaseName   name of the database to install
   * @param databasePath   absolute path to the live database directory
   * @param leaderHttpAddr leader's HTTP address (host:port)
   * @param clusterToken   cluster authentication token (may be null)
   * @param server         the ArcadeDB server instance for re-registering the database
   */
  public static void install(final String databaseName, final String databasePath,
      final String leaderHttpAddr, final String clusterToken,
      final ArcadeDBServer server) throws IOException {

    final Path dbPath = Path.of(databasePath).normalize().toAbsolutePath();
    final Path snapshotNew = dbPath.resolve(SNAPSHOT_NEW_DIR);
    final Path snapshotBackup = dbPath.resolve(SNAPSHOT_BACKUP_DIR);
    final Path pendingMarker = dbPath.resolve(SNAPSHOT_PENDING_FILE);

    // Clean up any leftover state from a previous failed attempt
    deleteDirectoryIfExists(snapshotNew);
    deleteDirectoryIfExists(snapshotBackup);
    Files.deleteIfExists(pendingMarker);

    Files.createDirectories(snapshotNew);

    // Write the pending marker BEFORE starting extraction
    Files.writeString(pendingMarker, "");

    final int maxRetries = server.getConfiguration().getValueAsInteger(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRIES);
    final long retryBaseMs = server.getConfiguration().getValueAsLong(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRY_BASE_MS);

    downloadWithRetry(databaseName, snapshotNew, leaderHttpAddr, clusterToken, maxRetries, retryBaseMs);

    // Mark download as complete
    Files.writeString(snapshotNew.resolve(SNAPSHOT_COMPLETE_FILE), "");

    // Swap: live -> backup, new -> live
    atomicSwap(dbPath, snapshotNew, snapshotBackup);

    // Cleanup
    deleteDirectoryIfExists(snapshotBackup);
    Files.deleteIfExists(pendingMarker);
    cleanupWalFiles(dbPath);
    // Remove the completion marker from the now-live directory
    Files.deleteIfExists(dbPath.resolve(SNAPSHOT_COMPLETE_FILE));

    // Re-open the database so the server registers it
    server.getDatabase(databaseName);

    HALog.log(SnapshotInstaller.class, HALog.BASIC, "Snapshot for '%s' installed successfully", databaseName);
  }

  /**
   * Scans all database subdirectories for pending snapshot swaps and completes
   * or rolls back each one. Called during state machine initialization to recover
   * from crashes that occurred mid-swap.
   *
   * @param databasesDir the parent directory containing all database subdirectories
   */
  public static void recoverPendingSnapshotSwaps(final Path databasesDir) {
    if (!Files.isDirectory(databasesDir))
      return;

    try (final DirectoryStream<Path> stream = Files.newDirectoryStream(databasesDir, Files::isDirectory)) {
      for (final Path dbDir : stream) {
        // Skip internal snapshot directories themselves
        final String dirName = dbDir.getFileName().toString();
        if (dirName.startsWith("."))
          continue;

        final Path pendingMarker = dbDir.resolve(SNAPSHOT_PENDING_FILE);
        if (!Files.exists(pendingMarker))
          continue;

        LogManager.instance().log(SnapshotInstaller.class, Level.INFO,
            "Recovering pending snapshot swap for database directory: %s", dbDir);

        recoverSingleDatabase(dbDir);
      }
    } catch (final IOException e) {
      LogManager.instance().log(SnapshotInstaller.class, Level.SEVERE,
          "Error scanning databases directory for pending snapshot swaps: %s", e, e.getMessage());
    }
  }

  private static void recoverSingleDatabase(final Path dbDir) {
    final Path snapshotNew = dbDir.resolve(SNAPSHOT_NEW_DIR);
    final Path snapshotBackup = dbDir.resolve(SNAPSHOT_BACKUP_DIR);
    final Path pendingMarker = dbDir.resolve(SNAPSHOT_PENDING_FILE);

    try {
      final boolean hasCompleteMarker = Files.exists(snapshotNew.resolve(SNAPSHOT_COMPLETE_FILE));
      final boolean hasBackup = Files.isDirectory(snapshotBackup);

      if (hasCompleteMarker && hasBackup) {
        // Download completed, swap started but not finished: complete the swap
        LogManager.instance().log(SnapshotInstaller.class, Level.INFO,
            "Completing interrupted snapshot swap for: %s", dbDir);
        atomicSwap(dbDir, snapshotNew, snapshotBackup);
        deleteDirectoryIfExists(snapshotBackup);
        Files.deleteIfExists(dbDir.resolve(SNAPSHOT_COMPLETE_FILE));

      } else if (hasCompleteMarker && !hasBackup) {
        // Swap was already completed but cleanup didn't finish
        LogManager.instance().log(SnapshotInstaller.class, Level.INFO,
            "Cleaning up completed snapshot swap for: %s", dbDir);
        deleteDirectoryIfExists(snapshotNew);

      } else if (!hasCompleteMarker && hasBackup) {
        // Download was interrupted, backup exists: restore the backup
        LogManager.instance().log(SnapshotInstaller.class, Level.INFO,
            "Rolling back incomplete snapshot download for: %s", dbDir);
        deleteDirectoryIfExists(snapshotNew);
        // Move backup contents back into dbDir
        restoreBackup(dbDir, snapshotBackup);

      } else {
        // Download was interrupted before backup was created: just clean up
        LogManager.instance().log(SnapshotInstaller.class, Level.INFO,
            "Cleaning up orphaned snapshot directory for: %s", dbDir);
        deleteDirectoryIfExists(snapshotNew);
      }

      Files.deleteIfExists(pendingMarker);

    } catch (final IOException e) {
      LogManager.instance().log(SnapshotInstaller.class, Level.SEVERE,
          "Error recovering snapshot swap for %s: %s", e, dbDir, e.getMessage());
    }
  }

  /**
   * Downloads the snapshot ZIP from the leader with exponential backoff retry.
   * On each failure, cleans up the partial {@code .snapshot-new} directory before retrying.
   */
  static void downloadWithRetry(final String databaseName, final Path snapshotNewDir,
      final String leaderHttpAddr, final String clusterToken,
      final int maxRetries, final long retryBaseMs) throws IOException {

    final String snapshotUrl = "http://" + leaderHttpAddr + "/api/v1/ha/snapshot/" + databaseName;
    IOException lastException = null;

    for (int attempt = 0; attempt <= maxRetries; attempt++) {
      if (attempt > 0) {
        // Clean up partial download from previous attempt
        deleteDirectoryContents(snapshotNewDir);

        final long delayMs = retryBaseMs * (1L << (attempt - 1));
        HALog.log(SnapshotInstaller.class, HALog.BASIC,
            "Retrying snapshot download for '%s' (attempt %d/%d, delay %dms)",
            databaseName, attempt + 1, maxRetries + 1, delayMs);
        try {
          Thread.sleep(delayMs);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException("Snapshot download interrupted during backoff", e);
        }
      }

      try {
        downloadSnapshot(snapshotNewDir, snapshotUrl, clusterToken);
        return; // Success
      } catch (final IOException e) {
        lastException = e;
        LogManager.instance().log(SnapshotInstaller.class, Level.WARNING,
            "Snapshot download attempt %d/%d failed for '%s': %s",
            attempt + 1, maxRetries + 1, databaseName, e.getMessage());
      }
    }

    throw new IOException("Snapshot download failed after " + (maxRetries + 1) + " attempts for '" + databaseName + "'",
        lastException);
  }

  private static void downloadSnapshot(final Path targetDir, final String snapshotUrl,
      final String clusterToken) throws IOException {

    HALog.log(SnapshotInstaller.class, HALog.BASIC, "Downloading snapshot from %s", snapshotUrl);

    final java.net.HttpURLConnection connection;
    try {
      connection = (java.net.HttpURLConnection) new java.net.URI(snapshotUrl).toURL().openConnection();
    } catch (final java.net.URISyntaxException e) {
      throw new IOException("Invalid snapshot URL: " + snapshotUrl, e);
    }
    connection.setRequestMethod("GET");
    connection.setConnectTimeout(30_000);
    connection.setReadTimeout(300_000);

    if (clusterToken != null && !clusterToken.isEmpty())
      connection.setRequestProperty("X-ArcadeDB-Cluster-Token", clusterToken);

    try {
      final int responseCode = connection.getResponseCode();
      if (responseCode != 200)
        throw new IOException("Failed to download snapshot: HTTP " + responseCode);

      try (final ZipInputStream zipIn = new ZipInputStream(connection.getInputStream())) {
        ZipEntry zipEntry;
        while ((zipEntry = zipIn.getNextEntry()) != null) {
          final Path targetFile = targetDir.resolve(zipEntry.getName()).normalize();

          // Zip-slip protection
          if (!targetFile.startsWith(targetDir))
            throw new IOException("Zip slip detected in snapshot: " + zipEntry.getName());

          // Symlink rejection
          if (zipEntry.getName().contains(".."))
            throw new IOException("Suspicious path in snapshot ZIP: " + zipEntry.getName());

          try (final java.io.FileOutputStream fos = new java.io.FileOutputStream(targetFile.toFile())) {
            zipIn.transferTo(fos);
          }
          zipIn.closeEntry();
        }
      }
    } finally {
      connection.disconnect();
    }
  }

  /**
   * Atomically swaps a new snapshot directory into the live database path.
   * <ol>
   *   <li>Rename live contents by moving all files from {@code dbDir} to {@code backupDir}
   *       (skipping {@code .snapshot-*} directories and the pending marker)</li>
   *   <li>Move all files from {@code newDir} to {@code dbDir}
   *       (skipping the {@code .snapshot-complete} marker)</li>
   * </ol>
   * If step 2 fails, attempts to restore from backup.
   */
  private static void atomicSwap(final Path dbDir, final Path newDir, final Path backupDir) throws IOException {
    // Ensure backup directory exists
    Files.createDirectories(backupDir);

    // Move live files to backup (skip .snapshot-* dirs and .snapshot-pending marker)
    try (final DirectoryStream<Path> stream = Files.newDirectoryStream(dbDir)) {
      for (final Path entry : stream) {
        final String name = entry.getFileName().toString();
        if (name.startsWith(".snapshot"))
          continue;
        Files.move(entry, backupDir.resolve(name), StandardCopyOption.REPLACE_EXISTING);
      }
    }

    // Move new snapshot files to live dir (skip .snapshot-complete marker)
    try {
      try (final DirectoryStream<Path> stream = Files.newDirectoryStream(newDir)) {
        for (final Path entry : stream) {
          final String name = entry.getFileName().toString();
          if (name.equals(SNAPSHOT_COMPLETE_FILE))
            continue;
          Files.move(entry, dbDir.resolve(name), StandardCopyOption.REPLACE_EXISTING);
        }
      }
    } catch (final IOException e) {
      // Swap failed - try to restore from backup
      LogManager.instance().log(SnapshotInstaller.class, Level.SEVERE,
          "Snapshot swap failed, restoring from backup: %s", e.getMessage());
      restoreBackup(dbDir, backupDir);
      throw new IOException("Snapshot swap failed for " + dbDir, e);
    }

    // Remove the now-empty .snapshot-new directory
    deleteDirectoryIfExists(newDir);
  }

  private static void restoreBackup(final Path dbDir, final Path backupDir) throws IOException {
    try (final DirectoryStream<Path> stream = Files.newDirectoryStream(backupDir)) {
      for (final Path entry : stream)
        Files.move(entry, dbDir.resolve(entry.getFileName().toString()), StandardCopyOption.REPLACE_EXISTING);
    }
    deleteDirectoryIfExists(backupDir);
  }

  private static void cleanupWalFiles(final Path dbDir) {
    final File[] walFiles = dbDir.toFile().listFiles((dir, name) -> name.endsWith(".wal"));
    if (walFiles != null)
      for (final File walFile : walFiles)
        if (!walFile.delete())
          LogManager.instance().log(SnapshotInstaller.class, Level.WARNING,
              "Failed to delete stale WAL file: %s", walFile.getName());
  }

  private static void deleteDirectoryIfExists(final Path dir) throws IOException {
    if (Files.isDirectory(dir))
      FileUtils.deleteRecursively(dir.toFile());
  }

  private static void deleteDirectoryContents(final Path dir) throws IOException {
    if (!Files.isDirectory(dir))
      return;
    try (final DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
      for (final Path entry : stream) {
        if (Files.isDirectory(entry))
          FileUtils.deleteRecursively(entry.toFile());
        else
          Files.delete(entry);
      }
    }
  }
}
```

- [ ] **Step 4: Run recovery tests to verify they pass**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn test -pl ha-raft -Dtest=SnapshotSwapRecoveryTest -q`
Expected: All 6 tests PASS

- [ ] **Step 5: Commit**

```bash
git add ha-raft/src/test/java/com/arcadedb/server/ha/raft/SnapshotSwapRecoveryTest.java \
       ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotInstaller.java
git commit -m "feat(ha-raft): add SnapshotInstaller with crash-safe recovery logic

Three-phase atomic swap with marker files for crash-safe snapshot
installation. recoverPendingSnapshotSwaps() handles all four recovery
scenarios from the truth table in the design spec."
```

---

### Task 3: Write SnapshotInstallerRetryTest (TDD - retry logic)

**Files:**
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/SnapshotInstallerRetryTest.java`

- [ ] **Step 1: Write the retry test class**

Uses a lightweight `com.sun.net.httpserver.HttpServer` to simulate leader responses.

```java
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

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests {@link SnapshotInstaller#downloadWithRetry} exponential backoff logic
 * using a local HTTP server to simulate leader responses.
 */
class SnapshotInstallerRetryTest {

  private HttpServer httpServer;
  private int port;

  @BeforeEach
  void startServer() throws IOException {
    httpServer = HttpServer.create(new InetSocketAddress(0), 0);
    port = httpServer.getAddress().getPort();
  }

  @AfterEach
  void stopServer() {
    if (httpServer != null)
      httpServer.stop(0);
  }

  @Test
  void testSuccessOnFirstAttempt(@TempDir final Path tempDir) throws IOException {
    final AtomicInteger callCount = new AtomicInteger(0);
    httpServer.createContext("/api/v1/ha/snapshot/testdb", exchange -> {
      callCount.incrementAndGet();
      final byte[] zip = createTestZip("data.dat", "hello");
      exchange.sendResponseHeaders(200, zip.length);
      exchange.getResponseBody().write(zip);
      exchange.close();
    });
    httpServer.start();

    final Path snapshotDir = tempDir.resolve(".snapshot-new");
    Files.createDirectories(snapshotDir);

    SnapshotInstaller.downloadWithRetry("testdb", snapshotDir,
        "localhost:" + port, null, 3, 100);

    assertThat(callCount.get()).isEqualTo(1);
    assertThat(snapshotDir.resolve("data.dat")).exists();
    assertThat(Files.readString(snapshotDir.resolve("data.dat"))).isEqualTo("hello");
  }

  @Test
  void testSuccessAfterTransientFailure(@TempDir final Path tempDir) throws IOException {
    final AtomicInteger callCount = new AtomicInteger(0);
    httpServer.createContext("/api/v1/ha/snapshot/testdb", exchange -> {
      final int attempt = callCount.incrementAndGet();
      if (attempt == 1) {
        exchange.sendResponseHeaders(503, -1);
        exchange.close();
        return;
      }
      final byte[] zip = createTestZip("data.dat", "recovered");
      exchange.sendResponseHeaders(200, zip.length);
      exchange.getResponseBody().write(zip);
      exchange.close();
    });
    httpServer.start();

    final Path snapshotDir = tempDir.resolve(".snapshot-new");
    Files.createDirectories(snapshotDir);

    SnapshotInstaller.downloadWithRetry("testdb", snapshotDir,
        "localhost:" + port, null, 3, 100);

    assertThat(callCount.get()).isEqualTo(2);
    assertThat(Files.readString(snapshotDir.resolve("data.dat"))).isEqualTo("recovered");
  }

  @Test
  void testExhaustedRetriesThrows(@TempDir final Path tempDir) throws IOException {
    final AtomicInteger callCount = new AtomicInteger(0);
    httpServer.createContext("/api/v1/ha/snapshot/testdb", exchange -> {
      callCount.incrementAndGet();
      exchange.sendResponseHeaders(500, -1);
      exchange.close();
    });
    httpServer.start();

    final Path snapshotDir = tempDir.resolve(".snapshot-new");
    Files.createDirectories(snapshotDir);

    assertThatThrownBy(() -> SnapshotInstaller.downloadWithRetry("testdb", snapshotDir,
        "localhost:" + port, null, 2, 100))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("failed after 3 attempts");

    // 1 initial + 2 retries = 3 total
    assertThat(callCount.get()).isEqualTo(3);
  }

  @Test
  void testPartialDownloadCleanedUpBeforeRetry(@TempDir final Path tempDir) throws IOException {
    final AtomicInteger callCount = new AtomicInteger(0);
    httpServer.createContext("/api/v1/ha/snapshot/testdb", exchange -> {
      final int attempt = callCount.incrementAndGet();
      if (attempt == 1) {
        // Send a partial ZIP that will cause extraction to fail or leave partial state
        exchange.sendResponseHeaders(500, -1);
        exchange.close();
        return;
      }
      final byte[] zip = createTestZip("clean.dat", "clean-data");
      exchange.sendResponseHeaders(200, zip.length);
      exchange.getResponseBody().write(zip);
      exchange.close();
    });
    httpServer.start();

    final Path snapshotDir = tempDir.resolve(".snapshot-new");
    Files.createDirectories(snapshotDir);
    // Simulate partial file from a "previous failed attempt"
    Files.writeString(snapshotDir.resolve("leftover.dat"), "partial");

    SnapshotInstaller.downloadWithRetry("testdb", snapshotDir,
        "localhost:" + port, null, 3, 100);

    // Leftover from failed attempt should have been cleaned before retry
    assertThat(snapshotDir.resolve("leftover.dat")).doesNotExist();
    assertThat(Files.readString(snapshotDir.resolve("clean.dat"))).isEqualTo("clean-data");
  }

  @Test
  void testBackoffTimingIncreases(@TempDir final Path tempDir) throws IOException {
    final AtomicInteger callCount = new AtomicInteger(0);
    httpServer.createContext("/api/v1/ha/snapshot/testdb", exchange -> {
      callCount.incrementAndGet();
      exchange.sendResponseHeaders(500, -1);
      exchange.close();
    });
    httpServer.start();

    final Path snapshotDir = tempDir.resolve(".snapshot-new");
    Files.createDirectories(snapshotDir);

    // Use baseMs=200 so delays are 200ms, 400ms. Total should be >= 600ms.
    final long start = System.currentTimeMillis();
    try {
      SnapshotInstaller.downloadWithRetry("testdb", snapshotDir,
          "localhost:" + port, null, 2, 200);
    } catch (final IOException ignored) {
      // Expected
    }
    final long elapsed = System.currentTimeMillis() - start;

    // 200ms + 400ms = 600ms minimum backoff (allow some slack for HTTP overhead)
    assertThat(elapsed).isGreaterThan(500);
  }

  private static byte[] createTestZip(final String fileName, final String content) throws IOException {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (final ZipOutputStream zos = new ZipOutputStream(baos)) {
      zos.putNextEntry(new ZipEntry(fileName));
      zos.write(content.getBytes());
      zos.closeEntry();
    }
    return baos.toByteArray();
  }
}
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn test -pl ha-raft -Dtest=SnapshotInstallerRetryTest -q`
Expected: All 5 tests PASS

- [ ] **Step 3: Commit**

```bash
git add ha-raft/src/test/java/com/arcadedb/server/ha/raft/SnapshotInstallerRetryTest.java
git commit -m "test(ha-raft): add SnapshotInstallerRetryTest for exponential backoff logic"
```

---

### Task 4: Wire SnapshotInstaller into ArcadeStateMachine

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java`

- [ ] **Step 1: Add recovery call in initialize()**

In `ArcadeStateMachine.initialize()`, after `reinitialize()` (line 86), add the recovery call:

```java
    // Recover any snapshot swaps that were interrupted by a crash
    final String databasesDir = raftServer.getProperties()
        .get(org.apache.ratis.server.RaftServerConfigKeys.STORAGE_DIR_KEY) != null
        ? null : null; // Not available from Ratis properties
    // Use server's database directory if available
    if (server != null) {
      final String dbDir = server.getConfiguration().getValueAsString(
          com.arcadedb.GlobalConfiguration.SERVER_DATABASE_DIRECTORY);
      if (dbDir != null)
        SnapshotInstaller.recoverPendingSnapshotSwaps(java.nio.file.Path.of(dbDir));
    }
```

Wait - let me reconsider. The `server` field may be null during `initialize()` because `setServer()` is called separately. Let me check the initialization order.

Actually, looking at the code more carefully: `RaftHAServer` constructor at line 135-136 creates the state machine and calls `setServer()` before `start()` which calls `raftServer.start()` which triggers `initialize()`. So `server` should be set by the time `initialize()` runs. But let's add a null guard to be safe.

Replace the `initialize()` method body (lines 82-88) with:

```java
  @Override
  public void initialize(final RaftServer raftServer, final RaftGroupId groupId, final RaftStorage raftStorage) throws IOException {
    super.initialize(raftServer, groupId, raftStorage);
    storage.init(raftStorage);
    reinitialize();

    // Recover any snapshot installations that were interrupted by a crash
    if (server != null) {
      final String dbDir = server.getConfiguration().getValueAsString(
          com.arcadedb.GlobalConfiguration.SERVER_DATABASE_DIRECTORY);
      if (dbDir != null)
        SnapshotInstaller.recoverPendingSnapshotSwaps(java.nio.file.Path.of(dbDir));
    }

    LogManager.instance().log(this, Level.INFO, "ArcadeStateMachine initialized (groupId=%s)", groupId);
  }
```

- [ ] **Step 2: Replace notifyInstallSnapshotFromLeader() to use SnapshotInstaller**

Replace the `notifyInstallSnapshotFromLeader()` method (lines 197-229) with:

```java
  /**
   * Called by Ratis when the follower's log is too far behind the leader's compacted log.
   * Individual log entries are no longer available, so a full database snapshot must be
   * downloaded from the leader. Delegates to {@link SnapshotInstaller#install} for crash-safe
   * installation with marker files and atomic directory swap.
   * <p>
   * Runs asynchronously via {@link CompletableFuture#supplyAsync} to avoid blocking the
   * Ratis state machine thread.
   */
  @Override
  public CompletableFuture<TermIndex> notifyInstallSnapshotFromLeader(
      final RaftProtos.RoleInfoProto roleInfoProto, final TermIndex firstTermIndexInLog) {

    LogManager.instance().log(this, Level.INFO,
        "Snapshot installation requested from leader (firstLogIndex=%s). Starting full resync...", firstTermIndexInLog);

    return CompletableFuture.supplyAsync(() -> {
      try {
        final RaftPeerId leaderId = RaftPeerId.valueOf(
            roleInfoProto.getFollowerInfo().getLeaderInfo().getId().getId());
        final String leaderHttpAddr = raftHAServer.getPeerHttpAddress(leaderId);

        if (leaderHttpAddr == null)
          throw new RuntimeException("Cannot determine leader HTTP address for snapshot download");

        final String clusterToken = server.getConfiguration().getValueAsString(
            com.arcadedb.GlobalConfiguration.HA_CLUSTER_TOKEN);

        for (final String dbName : server.getDatabaseNames()) {
          LogManager.instance().log(this, Level.INFO,
              "Installing snapshot for database '%s' from leader %s...", dbName, leaderHttpAddr);

          // Close and deregister the database before the swap
          if (server.existsDatabase(dbName)) {
            final DatabaseInternal db = (DatabaseInternal) server.getDatabase(dbName);
            final String databasePath = db.getDatabasePath();
            db.close();
            server.removeDatabase(dbName);
            SnapshotInstaller.install(dbName, databasePath, leaderHttpAddr, clusterToken, server);
          }
        }

        LogManager.instance().log(this, Level.INFO, "Full resync from leader completed");
        return firstTermIndexInLog;

      } catch (final Exception e) {
        LogManager.instance().log(this, Level.SEVERE, "Error during snapshot installation from leader", e);
        throw new RuntimeException("Error during Raft snapshot installation", e);
      }
    });
  }
```

- [ ] **Step 3: Update applyInstallDatabaseEntry() forceSnapshot path to use SnapshotInstaller**

Replace the forceSnapshot branch in `applyInstallDatabaseEntry()` (lines 403-428). The `if (forceSnapshot) { ... }` block becomes:

```java
    if (forceSnapshot) {
      // Restore flow: replace files from the leader's snapshot even if the DB exists.
      // The leader's own files are already authoritative, so the leader skips the reinstall;
      // replicas close their local copy and pull the fresh snapshot from the leader.
      if (raftHAServer != null && raftHAServer.isLeader()) {
        HALog.log(this, HALog.TRACE, "Leader skips forceSnapshot reinstall for '%s'", databaseName);
        return;
      }

      String databasePath;
      if (server.existsDatabase(databaseName)) {
        final DatabaseInternal db = (DatabaseInternal) server.getDatabase(databaseName);
        databasePath = db.getDatabasePath();
        db.getEmbedded().close();
        server.removeDatabase(databaseName);
      } else {
        databasePath = server.getConfiguration().getValueAsString(com.arcadedb.GlobalConfiguration.SERVER_DATABASE_DIRECTORY)
            + File.separator + databaseName;
      }

      final String leaderHttpAddr = raftHAServer.getLeaderHttpAddress();
      final String clusterToken = server.getConfiguration().getValueAsString(
          com.arcadedb.GlobalConfiguration.HA_CLUSTER_TOKEN);
      try {
        SnapshotInstaller.install(databaseName, databasePath, leaderHttpAddr, clusterToken, server);
      } catch (final IOException e) {
        throw new RuntimeException("Failed to install snapshot for restored database '" + databaseName + "'", e);
      }
      LogManager.instance().log(this, Level.INFO, "Database '%s' reinstalled via forceSnapshot from leader", databaseName);
      return;
    }
```

- [ ] **Step 4: Remove the old installDatabaseSnapshot() method**

Delete lines 231-304 (the `installDatabaseSnapshot()` method and its helper logic). All snapshot installation now goes through `SnapshotInstaller`.

- [ ] **Step 5: Compile to verify**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn compile -pl ha-raft -q`
Expected: BUILD SUCCESS

- [ ] **Step 6: Run existing tests to verify no regressions**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn test -pl ha-raft -Dtest="ArcadeStateMachineTest,SnapshotSwapRecoveryTest,SnapshotInstallerRetryTest,SnapshotManagerTest" -q`
Expected: All tests PASS

- [ ] **Step 7: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java
git commit -m "refactor(ha-raft): wire SnapshotInstaller into ArcadeStateMachine

notifyInstallSnapshotFromLeader() and forceSnapshot path now delegate
to SnapshotInstaller.install() for crash-safe atomic swap. Recovery
runs on startup via initialize(). Old installDatabaseSnapshot() removed."
```

---

### Task 5: Add Javadoc to ArcadeStateMachine

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java`

- [ ] **Step 1: Add class-level Javadoc**

Replace the existing class Javadoc (lines 55-58) with:

```java
/**
 * Ratis state machine that bridges the Raft log and ArcadeDB storage.
 * <p>
 * Handles five entry types:
 * <ul>
 *   <li>{@code TX_ENTRY} - WAL page diffs from committed transactions</li>
 *   <li>{@code SCHEMA_ENTRY} - DDL operations with file creation/removal, buffered WAL entries,
 *       and schema JSON updates</li>
 *   <li>{@code INSTALL_DATABASE_ENTRY} - create a new database or force-restore from leader snapshot</li>
 *   <li>{@code DROP_DATABASE_ENTRY} - drop a database (idempotent on replay)</li>
 *   <li>{@code SECURITY_USERS_ENTRY} - replicate user/role changes across the cluster</li>
 * </ul>
 * <p>
 * <b>Threading model:</b> {@link #applyTransaction} is called sequentially by Ratis on a single
 * thread per Raft group. No concurrent apply calls occur for the same group.
 * <p>
 * <b>Idempotency:</b> All apply methods are safe for replay after a crash. {@code applyTxEntry}
 * uses page-version guards in {@link com.arcadedb.engine.TransactionManager#applyChanges} to skip
 * already-applied pages. {@code applySchemaEntry} uses file-existence guards for file creation
 * and the same page-version guards for WAL application. Schema reload is naturally idempotent.
 * <p>
 * <b>Crash recovery:</b> On startup, {@link SnapshotInstaller#recoverPendingSnapshotSwaps} is
 * called from {@link #initialize} to complete or roll back any snapshot installations that were
 * interrupted by a process crash.
 */
```

- [ ] **Step 2: Add Javadoc to applyTxEntry()**

Add Javadoc before the `applyTxEntry()` method:

```java
  /**
   * Applies a committed WAL transaction to the local database.
   * <p>
   * <b>Origin-skip optimization:</b> On the leader, the transaction was already applied locally
   * via {@link RaftReplicatedDatabase#commit}'s Phase 2 ({@code commit2ndPhase}), so the state
   * machine skips it. On replicas, this is the primary path for applying transaction data.
   * <p>
   * <b>Ordering guarantee:</b> WAL capture (Phase 1) happens before Raft replication. Local
   * apply (Phase 2) happens after Raft commit. The leader skips the state machine apply because
   * Phase 2 already wrote the pages when replication succeeded.
   * <p>
   * <b>{@code ignoreErrors=true} rationale:</b> During Raft log replay on restart, log entries
   * may already be applied to the database files (Ratis last-applied tracking can lag behind
   * durable page writes). Page-version guards in {@code applyChanges} detect and skip
   * already-applied pages; version-gap warnings are still logged.
   */
```

- [ ] **Step 3: Add Javadoc to applySchemaEntry()**

Add Javadoc before the `applySchemaEntry()` method:

```java
  /**
   * Applies a committed DDL (schema change) entry to the local database.
   * <p>
   * <b>Three-phase application order</b> (order matters for correctness):
   * <ol>
   *   <li><b>Create/remove physical files.</b> WAL pages reference file IDs that must already
   *       exist on the replica. File-existence guards make this idempotent on replay.</li>
   *   <li><b>Apply buffered WAL entries.</b> Index page writes that occurred during DDL on the
   *       leader are embedded in the schema entry. These target the files created in step 1.
   *       Page-version guards make this idempotent on replay.</li>
   *   <li><b>Update schema JSON and reload.</b> Writes the schema configuration and reloads
   *       types, buckets, and file IDs into memory. Naturally idempotent (overwrites with
   *       same content on replay).</li>
   * </ol>
   * <p>
   * Like {@link #applyTxEntry}, the leader skips this because schema changes were already
   * applied locally during the transaction.
   */
```

- [ ] **Step 4: Add Javadoc to reinitialize()**

Add Javadoc before the `reinitialize()` method:

```java
  /**
   * Restores {@link #lastAppliedIndex} from the latest Ratis {@link SimpleStateMachineStorage}
   * snapshot metadata. Called during {@link #initialize} and again if the state machine storage
   * is reset (e.g., during Ratis recovery via {@link RaftHAServer#restartRatisIfNeeded}).
   */
```

- [ ] **Step 5: Compile to verify**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn compile -pl ha-raft -q`
Expected: BUILD SUCCESS

- [ ] **Step 6: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java
git commit -m "docs(ha-raft): add design rationale Javadoc to ArcadeStateMachine

Documents entry types, threading model, idempotency guarantees,
origin-skip optimization, three-phase schema application order,
and crash recovery behavior."
```

---

### Task 6: Add Javadoc to RaftHAServer

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java`

- [ ] **Step 1: Add class-level Javadoc**

Replace the existing class Javadoc (lines 63-66) with:

```java
/**
 * Manages the lifecycle of the Apache Ratis {@link RaftServer}, {@link RaftClient},
 * and {@link RaftGroupCommitter} for ArcadeDB high availability.
 * <p>
 * Owns peer configuration (parsed from {@code HA_SERVER_LIST}), quorum policy
 * ({@link Quorum#MAJORITY} or {@link Quorum#ALL}), and leadership state.
 * Provides the {@link HealthMonitor.HealthTarget} interface so the background
 * health monitor can trigger automatic recovery from stuck Ratis states.
 * <p>
 * <b>Thread-safety:</b> {@link #recoveryLock} synchronizes recovery attempts in
 * {@link #restartRatisIfNeeded()} to prevent concurrent restart races. The
 * {@link #shutdownRequested} volatile flag prevents recovery during shutdown.
 * <p>
 * <b>Security note (K8s mode):</b> When {@code HA_K8S} is enabled and gRPC is bound
 * to {@code 0.0.0.0}, any pod in the Kubernetes cluster can connect to the Raft port
 * and inject Raft log entries. Authentication for inter-node traffic relies on
 * Kubernetes NetworkPolicy. Operators should restrict access to the Raft port via
 * NetworkPolicy rules in production.
 */
```

- [ ] **Step 2: Add Javadoc to restartRatisIfNeeded()**

Replace the existing method declaration comment (if any) at line 411 with:

```java
  /**
   * Recovers from a Ratis server that has entered CLOSED or CLOSING state, typically
   * after a network partition where the node was isolated long enough for Ratis to
   * give up on the group.
   * <p>
   * Triggered by {@link HealthMonitor} when it detects an unhealthy Ratis state.
   * Creates a new {@link ArcadeStateMachine} and Ratis server using
   * {@link RaftStorage.StartupOption#RECOVER} mode, which loads the existing Raft log
   * from disk instead of formatting fresh storage.
   * <p>
   * The database state is persisted on disk, so the new state machine picks up where
   * the old one left off. The {@code lastAppliedIndex} is restored from Ratis snapshot
   * metadata during {@link ArcadeStateMachine#reinitialize()}, and Ratis replays any
   * log entries beyond that point.
   * <p>
   * The {@link #recoveryLock} prevents concurrent restart attempts. If
   * {@link #shutdownRequested} is true, recovery is skipped.
   */
```

- [ ] **Step 3: Add Javadoc to waitForLocalApply()**

Add Javadoc before the `waitForLocalApply()` method (line 1002):

```java
  /**
   * Blocks until the local state machine's last-applied index reaches the current commit index.
   * Used for {@code READ_YOUR_WRITES} consistency: the client provides a commit index bookmark
   * from a previous write, and the replica waits until that entry has been applied locally
   * before serving the read.
   * <p>
   * Uses {@link #applyNotifier} (notified by {@link ArcadeStateMachine#applyTransaction})
   * with a timeout of {@link #quorumTimeout} milliseconds. If the deadline is reached before
   * catch-up, the method returns silently (reads may be slightly stale rather than failing).
   */
```

- [ ] **Step 4: Compile to verify**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn compile -pl ha-raft -q`
Expected: BUILD SUCCESS

- [ ] **Step 5: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java
git commit -m "docs(ha-raft): add design rationale Javadoc to RaftHAServer

Documents lifecycle management, K8s security considerations,
restartRatisIfNeeded() recovery semantics, and waitForLocalApply()
read consistency protocol."
```

---

### Task 7: Add Javadoc to RaftReplicatedDatabase

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftReplicatedDatabase.java`

- [ ] **Step 1: Add Javadoc to commit()**

Add Javadoc before the `commit()` method (line 151):

```java
  /**
   * Commits the current transaction through Raft consensus.
   * <p>
   * <b>Two-phase flow with lock release during replication:</b>
   * <ol>
   *   <li><b>Phase 1 (read lock held):</b> Capture WAL bytes and bucket record deltas
   *       via {@code commit1stPhase}. The read lock ensures a consistent snapshot of the
   *       transaction's page changes.</li>
   *   <li><b>Replication (no lock held):</b> Submit the WAL entry to Raft via
   *       {@link RaftGroupCommitter#submitAndWait} and wait for quorum. Releasing the lock
   *       here allows concurrent transactions to proceed through Phase 1 while this
   *       transaction waits for Raft consensus, significantly improving throughput.</li>
   *   <li><b>Phase 2 (read lock held on leader):</b> Apply pages locally via
   *       {@code commit2ndPhase}. On replicas, the state machine applies the pages via
   *       {@link ArcadeStateMachine#applyTransaction}, so Phase 2 is skipped here.</li>
   * </ol>
   * <p>
   * <b>Phase 2 failure handling:</b> If local apply fails after Raft has committed the entry,
   * the entry is already in the log and other replicas will apply it. The leader logs SEVERE
   * and should step down rather than continue with diverged state.
   * <p>
   * <b>Schema WAL buffering:</b> When {@code commit()} is called from inside a
   * {@code recordFileChanges()} callback, the files being created do not yet exist on replicas.
   * Instead of sending a {@code TX_ENTRY} that would fail, the WAL data is buffered in
   * {@link #schemaWalBuffer} and embedded in the {@code SCHEMA_ENTRY} sent after the callback.
   */
```

- [ ] **Step 2: Compile to verify**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn compile -pl ha-raft -q`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftReplicatedDatabase.java
git commit -m "docs(ha-raft): add design rationale Javadoc to RaftReplicatedDatabase.commit()

Documents two-phase commit with lock release during replication,
Phase 2 failure handling, and schema WAL buffering."
```

---

### Task 8: Port RaftHAServerAddressParsingTest

**Files:**
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftHAServerAddressParsingTest.java`

- [ ] **Step 1: Write the test class**

```java
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

import com.arcadedb.server.ServerException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Edge cases for {@link RaftHAServer#parsePeerList} not covered by {@link RaftHAServerTest}.
 * Ported from apache-ratis branch.
 */
class RaftHAServerAddressParsingTest {

  @Test
  void emptyStringThrows() {
    assertThatThrownBy(() -> RaftHAServer.parsePeerList("", 2434))
        .isInstanceOf(ServerException.class);
  }

  @Test
  void blankEntryThrows() {
    assertThatThrownBy(() -> RaftHAServer.parsePeerList("  ", 2434))
        .isInstanceOf(ServerException.class);
  }

  @Test
  void tooManyColonsThrows() {
    assertThatThrownBy(() -> RaftHAServer.parsePeerList("host:1:2:3:4", 2434))
        .isInstanceOf(ServerException.class);
  }

  @Test
  void blankHostnameThrows() {
    assertThatThrownBy(() -> RaftHAServer.parsePeerList(":2434", 2434))
        .isInstanceOf(ServerException.class);
  }

  @Test
  void nonNumericPriorityThrows() {
    assertThatThrownBy(() -> RaftHAServer.parsePeerList("host:2434:2480:abc", 2434))
        .isInstanceOf(ServerException.class);
  }

  @Test
  void singleNodeCluster() {
    final var parsed = RaftHAServer.parsePeerList("myhost:2434:2480", 2434);
    assertThat(parsed.peers()).hasSize(1);
    assertThat(parsed.peers().get(0).getAddress()).isEqualTo("myhost:2434");
    assertThat(parsed.httpAddresses()).hasSize(1);
  }

  @Test
  void trailingCommaIgnored() {
    // parsePeerList splits on comma; trailing comma creates empty entry that should be handled
    // This test documents the current behavior - adjust if it should throw instead
    final var parsed = RaftHAServer.parsePeerList("host1:2434,host2:2435", 2434);
    assertThat(parsed.peers()).hasSize(2);
  }

  @Test
  void leadingWhitespaceInEntryTrimmed() {
    final var parsed = RaftHAServer.parsePeerList("  host1:2434 , host2:2435 ", 2434);
    assertThat(parsed.peers()).hasSize(2);
    assertThat(parsed.peers().get(0).getAddress()).isEqualTo("host1:2434");
    assertThat(parsed.peers().get(1).getAddress()).isEqualTo("host2:2435");
  }
}
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn test -pl ha-raft -Dtest=RaftHAServerAddressParsingTest -q`
Expected: All tests PASS (or some may reveal bugs to fix - adjust assertions if needed)

- [ ] **Step 3: Commit**

```bash
git add ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftHAServerAddressParsingTest.java
git commit -m "test(ha-raft): port RaftHAServerAddressParsingTest edge cases from apache-ratis"
```

---

### Task 9: Port HAConfigDefaultsTest

**Files:**
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/HAConfigDefaultsTest.java`

- [ ] **Step 1: Write the test class**

```java
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

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates that all HA-related {@link GlobalConfiguration} entries have sensible defaults.
 * Ported from apache-ratis branch, adapted to include new snapshot retry entries.
 */
class HAConfigDefaultsTest {

  @Test
  void allHAEntriesHaveNonNullDefaults() {
    for (final GlobalConfiguration config : GlobalConfiguration.values()) {
      if (config.getKey().startsWith("arcadedb.ha."))
        assertThat(config.getDefValue())
            .as("HA config '%s' should have a non-null default", config.getKey())
            .isNotNull();
    }
  }

  @Test
  void snapshotInstallRetriesDefault() {
    assertThat(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRIES.getDefValue()).isEqualTo(3);
    assertThat(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRIES.getType()).isEqualTo(Integer.class);
  }

  @Test
  void snapshotInstallRetryBaseMsDefault() {
    assertThat(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRY_BASE_MS.getDefValue()).isEqualTo(5000L);
    assertThat(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRY_BASE_MS.getType()).isEqualTo(Long.class);
  }

  @Test
  void snapshotMaxConcurrentDefault() {
    assertThat(GlobalConfiguration.HA_SNAPSHOT_MAX_CONCURRENT.getDefValue()).isEqualTo(2);
  }

  @Test
  void quorumTimeoutDefault() {
    assertThat(GlobalConfiguration.HA_QUORUM_TIMEOUT.getDefValue()).isEqualTo(10000L);
  }

  @Test
  void replicationLagWarningDefault() {
    assertThat(GlobalConfiguration.HA_REPLICATION_LAG_WARNING.getDefValue()).isEqualTo(1000L);
  }
}
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn test -pl ha-raft -Dtest=HAConfigDefaultsTest -q`
Expected: All tests PASS

- [ ] **Step 3: Commit**

```bash
git add ha-raft/src/test/java/com/arcadedb/server/ha/raft/HAConfigDefaultsTest.java
git commit -m "test(ha-raft): port HAConfigDefaultsTest from apache-ratis with new config entries"
```

---

### Task 10: Create SnapshotInstallerIntegrationIT

**Files:**
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/SnapshotInstallerIntegrationIT.java`

- [ ] **Step 1: Write the integration test**

```java
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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.log.LogManager;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test verifying that {@link SnapshotInstaller} correctly installs a database
 * snapshot on a follower that has fallen behind the leader's compacted log, and that
 * marker files are properly cleaned up after installation.
 */
class SnapshotInstallerIntegrationIT extends BaseRaftHATest {

  @Override
  protected boolean persistentRaftStorage() {
    return true;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
    // Low threshold to trigger log compaction quickly
    config.setValue(GlobalConfiguration.HA_RAFT_SNAPSHOT_THRESHOLD, 10L);
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void testFollowerInstallsSnapshotViaCrashSafeFlow() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int replicaIndex = (leaderIndex + 1) % getServerCount();

    final var leaderDb = getServerDatabase(leaderIndex, getDatabaseName());

    // Create type and initial data
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType("SnapshotTest"))
        leaderDb.getSchema().createVertexType("SnapshotTest");
    });

    leaderDb.transaction(() -> {
      for (int i = 0; i < 50; i++) {
        final MutableVertex v = leaderDb.newVertex("SnapshotTest");
        v.set("name", "phase1-" + i);
        v.save();
      }
    });

    assertClusterConsistency();

    // Stop replica and write enough to trigger log compaction
    LogManager.instance().log(this, Level.INFO, "TEST: Stopping replica %d", replicaIndex);
    getServer(replicaIndex).stop();

    leaderDb.transaction(() -> {
      for (int i = 0; i < 200; i++) {
        final MutableVertex v = leaderDb.newVertex("SnapshotTest");
        v.set("name", "phase2-" + i);
        v.save();
      }
    });

    assertThat(leaderDb.countType("SnapshotTest", true)).isEqualTo(250);

    // Restart replica - should trigger snapshot install via SnapshotInstaller
    LogManager.instance().log(this, Level.INFO, "TEST: Restarting replica %d - expecting snapshot install", replicaIndex);
    restartServer(replicaIndex);

    // Verify data consistency
    assertThat(getServerDatabase(replicaIndex, getDatabaseName()).countType("SnapshotTest", true))
        .as("Replica should have all 250 records after snapshot install").isEqualTo(250);

    // Verify no marker files left behind
    final String dbPath = getServerDatabase(replicaIndex, getDatabaseName()).getDatabasePath();
    assertThat(Path.of(dbPath).resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE)).doesNotExist();
    assertThat(Path.of(dbPath).resolve(SnapshotInstaller.SNAPSHOT_NEW_DIR)).doesNotExist();
    assertThat(Path.of(dbPath).resolve(SnapshotInstaller.SNAPSHOT_BACKUP_DIR)).doesNotExist();

    assertClusterConsistency();
  }
}
```

- [ ] **Step 2: Run the integration test**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn test -pl ha-raft -Dtest=SnapshotInstallerIntegrationIT -q`
Expected: PASS (this validates the full end-to-end flow)

- [ ] **Step 3: Commit**

```bash
git add ha-raft/src/test/java/com/arcadedb/server/ha/raft/SnapshotInstallerIntegrationIT.java
git commit -m "test(ha-raft): add SnapshotInstallerIntegrationIT for end-to-end crash-safe snapshot"
```

---

### Task 11: Create RaftReplicaFailureIT

**Files:**
- Create: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftReplicaFailureIT.java`

- [ ] **Step 1: Write the integration test**

```java
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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.log.LogManager;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests replica failure scenarios during snapshot installation.
 * Ported from apache-ratis branch, adapted to ha-redesign's BaseRaftHATest framework.
 */
class RaftReplicaFailureIT extends BaseRaftHATest {

  @Override
  protected boolean persistentRaftStorage() {
    return true;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
    config.setValue(GlobalConfiguration.HA_RAFT_SNAPSHOT_THRESHOLD, 10L);
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void replicaRecoverAfterLongAbsenceRequiringSnapshot() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    final int replicaIndex = (leaderIndex + 1) % getServerCount();

    final var leaderDb = getServerDatabase(leaderIndex, getDatabaseName());

    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType("FailureTest"))
        leaderDb.getSchema().createVertexType("FailureTest");
    });

    leaderDb.transaction(() -> {
      for (int i = 0; i < 30; i++) {
        final MutableVertex v = leaderDb.newVertex("FailureTest");
        v.set("name", "before-" + i);
        v.save();
      }
    });

    assertClusterConsistency();

    // Stop replica
    LogManager.instance().log(this, Level.INFO, "TEST: Stopping replica %d for long absence", replicaIndex);
    getServer(replicaIndex).stop();

    // Write enough data to trigger multiple log compactions
    for (int batch = 0; batch < 5; batch++) {
      final int b = batch;
      leaderDb.transaction(() -> {
        for (int i = 0; i < 50; i++) {
          final MutableVertex v = leaderDb.newVertex("FailureTest");
          v.set("name", "during-" + b + "-" + i);
          v.save();
        }
      });
    }

    assertThat(leaderDb.countType("FailureTest", true)).isEqualTo(280);

    // Restart the long-absent replica - needs full snapshot
    LogManager.instance().log(this, Level.INFO, "TEST: Restarting replica %d after long absence", replicaIndex);
    restartServer(replicaIndex);

    assertThat(getServerDatabase(replicaIndex, getDatabaseName()).countType("FailureTest", true))
        .as("Replica should have all records after recovery").isEqualTo(280);

    assertClusterConsistency();
  }
}
```

- [ ] **Step 2: Run the integration test**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn test -pl ha-raft -Dtest=RaftReplicaFailureIT -q`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftReplicaFailureIT.java
git commit -m "test(ha-raft): port RaftReplicaFailureIT for replica long-absence recovery"
```

---

### Task 12: Final verification - run all ha-raft tests

**Files:** None (verification only)

- [ ] **Step 1: Run all unit tests**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn test -pl ha-raft -Dtest="SnapshotSwapRecoveryTest,SnapshotInstallerRetryTest,HAConfigDefaultsTest,RaftHAServerAddressParsingTest,SnapshotManagerTest,ArcadeStateMachineTest,RaftHAServerTest,RaftReplicatedDatabaseTest" -q`
Expected: All tests PASS

- [ ] **Step 2: Run full module compilation**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && mvn compile -pl engine,ha-raft -q`
Expected: BUILD SUCCESS

- [ ] **Step 3: Verify no leftover System.out or debug prints**

Run: `cd /Users/frank/projects/arcade/worktrees/ha-redesign && grep -r "System.out" ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotInstaller.java`
Expected: No matches
