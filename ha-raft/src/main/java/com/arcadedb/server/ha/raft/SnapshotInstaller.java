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
            "Recovering pending snapshot swap for database directory: %s", null, dbDir);

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
            "Completing interrupted snapshot swap for: %s", null, dbDir);
        atomicSwap(dbDir, snapshotNew, snapshotBackup);
        deleteDirectoryIfExists(snapshotBackup);
        Files.deleteIfExists(dbDir.resolve(SNAPSHOT_COMPLETE_FILE));

      } else if (hasCompleteMarker && !hasBackup) {
        // Swap was already completed but cleanup didn't finish
        LogManager.instance().log(SnapshotInstaller.class, Level.INFO,
            "Cleaning up completed snapshot swap for: %s", null, dbDir);
        deleteDirectoryIfExists(snapshotNew);

      } else if (!hasCompleteMarker && hasBackup) {
        // Download was interrupted, backup exists: restore the backup
        LogManager.instance().log(SnapshotInstaller.class, Level.INFO,
            "Rolling back incomplete snapshot download for: %s", null, dbDir);
        deleteDirectoryIfExists(snapshotNew);
        // Move backup contents back into dbDir
        restoreBackup(dbDir, snapshotBackup);

      } else {
        // Download was interrupted before backup was created: just clean up
        LogManager.instance().log(SnapshotInstaller.class, Level.INFO,
            "Cleaning up orphaned snapshot directory for: %s", null, dbDir);
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
            null, attempt + 1, maxRetries + 1, databaseName, e.getMessage());
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
          "Snapshot swap failed, restoring from backup: %s", e, e.getMessage());
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
              "Failed to delete stale WAL file: %s", null, walFile.getName());
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
