/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.log.LogManager;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.server.ArcadeDBServer;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Handles HTTP-based snapshot installation for followers that fall behind the Raft log.
 * This class encapsulates:
 * <ul>
 *   <li>Downloading a database snapshot ZIP from the leader's HTTP endpoint</li>
 *   <li>Crash-safe directory swap using a marker file (safe to interrupt at any point)</li>
 *   <li>Recovery of interrupted swaps on server startup</li>
 *   <li>Stale database cleanup (databases dropped on leader but still present locally)</li>
 * </ul>
 * <p>
 * The Ratis snapshot contains only a small marker file. The actual database data is transferred
 * via HTTP separately, so the snapshot ZIP from the leader may be tens or hundreds of gigabytes
 * for large databases.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SnapshotInstaller {

  private static final int    SNAPSHOT_DOWNLOAD_MAX_RETRIES = 3;
  private static final long[] SNAPSHOT_DOWNLOAD_BACKOFF_MS  = { 5_000, 10_000, 20_000 };

  private final ArcadeDBServer server;
  private final RaftHAServer   raftHA;

  public SnapshotInstaller(final ArcadeDBServer server, final RaftHAServer raftHA) {
    this.server = server;
    this.raftHA = raftHA;
  }

  /**
   * Downloads all databases from the leader. Called when reinitialize() detects a gap between
   * the snapshot index and the persisted applied index, indicating the follower missed data.
   */
  public void installDatabasesFromLeader() throws IOException {
    final String leaderHttpAddr = raftHA.getLeaderHTTPAddress();
    if (leaderHttpAddr == null) {
      LogManager.instance().log(this, Level.WARNING,
          "Cannot determine leader HTTP address for snapshot download, will rely on Raft log replay");
      return;
    }

    LogManager.instance().log(this, Level.INFO,
        "Downloading databases from leader %s during snapshot installation...", leaderHttpAddr);

    final Set<String> leaderDatabases = fetchLeaderDatabaseNames(leaderHttpAddr);

    for (final String dbName : new ArrayList<>(server.getDatabaseNames())) {
      if (!leaderDatabases.isEmpty() && !leaderDatabases.contains(dbName)) {
        LogManager.instance().log(this, Level.INFO,
            "Database '%s' exists on this follower but not on the leader, dropping stale copy", dbName);
        try {
          server.getDatabase(dbName).getEmbedded().drop();
          server.removeDatabase(dbName);
        } catch (final Exception e) {
          LogManager.instance().log(this, Level.WARNING, "Failed to drop stale database '%s': %s", dbName, e.getMessage());
        }
        continue;
      }

      final DatabaseInternal db = server.getDatabase(dbName);
      if (db == null) {
        LogManager.instance().log(this, Level.WARNING, "Database '%s' was dropped during snapshot installation, skipping", dbName);
        continue;
      }
      try {
        LogManager.instance().log(this, Level.INFO, "Installing snapshot for database '%s' from leader %s...", dbName, leaderHttpAddr);
        installDatabaseSnapshot(db, leaderHttpAddr, dbName);
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.WARNING, "Failed to install snapshot for database '%s', skipping: %s", dbName, e.getMessage());
      }
    }

    LogManager.instance().log(this, Level.INFO, "Snapshot installation from leader completed");
  }

  /**
   * Handles the {@code notifyInstallSnapshotFromLeader} Ratis callback by downloading all databases
   * from the identified leader. Returns the {@code firstTermIndexInLog} on success.
   *
   * @param roleInfoProto   Ratis role info containing the leader's peer ID
   * @param leaderHttpAddr  resolved HTTP address of the leader
   */
  public void installFromLeaderNotification(final String leaderHttpAddr) throws IOException {
    final Set<String> leaderDatabases = fetchLeaderDatabaseNames(leaderHttpAddr);

    for (final String dbName : new ArrayList<>(server.getDatabaseNames())) {
      if (!leaderDatabases.isEmpty() && !leaderDatabases.contains(dbName)) {
        LogManager.instance().log(this, Level.INFO,
            "Database '%s' exists on this follower but not on the leader, dropping stale copy", dbName);
        try {
          server.getDatabase(dbName).getEmbedded().drop();
          server.removeDatabase(dbName);
        } catch (final Exception e) {
          LogManager.instance().log(this, Level.WARNING, "Failed to drop stale database '%s': %s", dbName, e.getMessage());
        }
        continue;
      }

      final DatabaseInternal db = server.getDatabase(dbName);
      if (db == null)
        throw new ReplicationException("Database '" + dbName + "' not found during snapshot installation");
      installDatabaseSnapshot(db, leaderHttpAddr, dbName);
    }
  }

  /**
   * Downloads a database snapshot (ZIP) from the leader's HTTP endpoint and atomically replaces
   * the local database directory using a crash-safe marker file and directory swap.
   * <p>
   * Phase 1: Download and extract to a temp directory (database stays open).
   * Phase 2: Close the database, write a marker file, swap directories, reopen.
   */
  public void installDatabaseSnapshot(final DatabaseInternal db, final String leaderHttpAddr,
      final String databaseName) throws IOException {

    final Path dbPath = Path.of(db.getDatabasePath()).normalize().toAbsolutePath();
    final Path tempDir = dbPath.resolveSibling(dbPath.getFileName() + ".snapshot-tmp");
    final Path backupDir = dbPath.resolveSibling(dbPath.getFileName() + ".snapshot-old");

    // Phase 1: Download and extract to temp directory (database stays open and operational).
    downloadSnapshotWithRetry(leaderHttpAddr, tempDir, databaseName);

    // Phase 2: Close database and swap directories using a crash-safe marker file.
    // Close the underlying LocalDatabase directly - ServerDatabase.close() throws
    // UnsupportedOperationException because server-managed databases are shared.
    db.getEmbedded().close();

    final Path markerFile = dbPath.resolveSibling(dbPath.getFileName() + ".snapshot-pending");

    try {
      // Write the pending marker BEFORE any destructive operation.
      // If we crash after this, recoverPendingSnapshotSwaps() will finish the job.
      Files.writeString(markerFile, databaseName);

      if (Files.exists(dbPath)) {
        FileUtils.deleteRecursively(backupDir.toFile());
        Files.move(dbPath, backupDir);
      }
      Files.move(tempDir, dbPath);
      deleteStaleWalFiles(dbPath);

      Files.deleteIfExists(markerFile);
      FileUtils.deleteRecursively(backupDir.toFile());

      LogManager.instance().log(this, Level.INFO, "Database snapshot for '%s' installed successfully", databaseName);

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Snapshot swap failed for '%s', attempting rollback...", databaseName);
      try {
        if (Files.exists(backupDir)) {
          FileUtils.deleteRecursively(dbPath.toFile());
          Files.move(backupDir, dbPath);
        }
      } catch (final Exception rollbackEx) {
        LogManager.instance().log(this, Level.SEVERE,
            "CRITICAL: Failed to rollback snapshot swap for '%s'. Database may be unavailable. Error: %s",
            databaseName, rollbackEx.getMessage());
      }
      Files.deleteIfExists(markerFile);
      FileUtils.deleteRecursively(tempDir.toFile());
      throw new ReplicationException("Snapshot installation failed during directory swap", e);
    } finally {
      // Force the server to drop the old (now-stale) database reference and reopen.
      try {
        server.removeDatabase(databaseName);
        server.getDatabase(databaseName);
        LogManager.instance().log(this, Level.INFO, "Database '%s' reopened after snapshot installation", databaseName);
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.SEVERE,
            "CRITICAL: Failed to reopen database '%s' after snapshot installation. "
                + "This node may need to be restarted to recover. Error: %s",
            databaseName, e.getMessage());
      }
    }
  }

  /**
   * Downloads a snapshot ZIP into the temp directory, retrying on transient failures.
   * On each retry the leader address is refreshed to handle elections during download.
   */
  public void downloadSnapshotWithRetry(final String initialLeaderAddr, final Path tempDir,
      final String databaseName) throws IOException {
    final boolean useSsl = server.getConfiguration().getValueAsBoolean(GlobalConfiguration.NETWORK_USE_SSL);
    final String protocol = useSsl ? "https" : "http";

    for (int attempt = 1; attempt <= SNAPSHOT_DOWNLOAD_MAX_RETRIES; attempt++) {
      String leaderAddr = raftHA.getLeaderHTTPAddress();
      if (leaderAddr == null)
        leaderAddr = initialLeaderAddr;

      final String snapshotUrl = protocol + "://" + leaderAddr + "/api/v1/ha/snapshot/"
          + URLEncoder.encode(databaseName, StandardCharsets.UTF_8);
      try {
        downloadSnapshot(snapshotUrl, tempDir, databaseName);
        return;
      } catch (final IOException | ReplicationException e) {
        FileUtils.deleteRecursively(tempDir.toFile());
        if (attempt == SNAPSHOT_DOWNLOAD_MAX_RETRIES)
          throw e instanceof IOException ? (IOException) e
              : new IOException("Snapshot download failed after " + SNAPSHOT_DOWNLOAD_MAX_RETRIES + " attempts", e);

        final long backoff = SNAPSHOT_DOWNLOAD_BACKOFF_MS[attempt - 1];
        LogManager.instance().log(this, Level.WARNING,
            "Snapshot download from %s failed (attempt %d/%d), retrying in %dms: %s",
            snapshotUrl, attempt, SNAPSHOT_DOWNLOAD_MAX_RETRIES, backoff, e.getMessage());
        try {
          Thread.sleep(backoff);
        } catch (final InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new IOException("Snapshot download interrupted during retry backoff", ie);
        }
      }
    }
  }

  /**
   * Downloads a snapshot ZIP from the leader and extracts it into the given temp directory.
   * The temp directory is created fresh (any leftover from a previous attempt is cleaned up).
   */
  private void downloadSnapshot(final String snapshotUrl, final Path tempDir,
      final String databaseName) throws IOException {
    LogManager.instance().log(this, Level.INFO, "Downloading database snapshot from %s...", snapshotUrl);

    final HttpURLConnection connection;
    try {
      connection = (HttpURLConnection) new URI(snapshotUrl).toURL().openConnection();
    } catch (final URISyntaxException e) {
      throw new IOException("Invalid snapshot URL: " + snapshotUrl, e);
    }

    if (connection instanceof javax.net.ssl.HttpsURLConnection httpsConn) {
      try {
        final javax.net.ssl.SSLContext sslContext = server.getHttpServer().getSSLContext();
        if (sslContext != null)
          httpsConn.setSSLSocketFactory(sslContext.getSocketFactory());
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.WARNING,
            "Could not configure SSL for snapshot download, using default trust store: %s", e.getMessage());
      }
    }

    connection.setRequestMethod("GET");
    connection.setConnectTimeout(30_000);
    connection.setReadTimeout(server.getConfiguration().getValueAsInteger(GlobalConfiguration.HA_SNAPSHOT_DOWNLOAD_TIMEOUT));

    if (raftHA.getClusterToken() != null)
      connection.setRequestProperty("X-ArcadeDB-Cluster-Token", raftHA.getClusterToken());

    try {
      final int responseCode = connection.getResponseCode();
      if (responseCode != 200)
        throw new ReplicationException("Failed to download snapshot from " + snapshotUrl + ": HTTP " + responseCode);

      FileUtils.deleteRecursively(tempDir.toFile());
      Files.createDirectories(tempDir);

      try (final ZipInputStream zipIn = new ZipInputStream(connection.getInputStream())) {
        ZipEntry zipEntry;
        while ((zipEntry = zipIn.getNextEntry()) != null) {
          final Path targetFile = tempDir.resolve(zipEntry.getName()).normalize();

          // Security: prevent zip slip
          if (!targetFile.startsWith(tempDir))
            throw new ReplicationException("Zip slip detected in snapshot: " + zipEntry.getName());

          LogManager.instance().log(this, Level.FINE, "Extracting snapshot file: %s", zipEntry.getName());

          if (zipEntry.isDirectory()) {
            Files.createDirectories(targetFile);
          } else {
            Files.createDirectories(targetFile.getParent());
            try (final FileOutputStream fos = new FileOutputStream(targetFile.toFile())) {
              copyWithLimit(zipIn, fos, 10L * 1024 * 1024 * 1024, zipEntry.getName());
            }
          }
          zipIn.closeEntry();
        }
      } catch (final Exception e) {
        FileUtils.deleteRecursively(tempDir.toFile());
        throw e;
      }
    } finally {
      connection.disconnect();
    }
  }

  /**
   * Queries the leader's HTTP API for its current database list.
   * Returns an empty set on failure (caller skips stale-database removal when leader is unreachable).
   */
  private Set<String> fetchLeaderDatabaseNames(final String leaderHttpAddr) {
    try {
      final boolean useSsl = server.getConfiguration().getValueAsBoolean(GlobalConfiguration.NETWORK_USE_SSL);
      final String url = (useSsl ? "https" : "http") + "://" + leaderHttpAddr + "/api/v1/server";
      final HttpURLConnection conn = (HttpURLConnection) new URI(url).toURL().openConnection();
      conn.setRequestMethod("GET");
      conn.setConnectTimeout(10_000);
      conn.setReadTimeout(10_000);
      if (raftHA.getClusterToken() != null) {
        conn.setRequestProperty("X-ArcadeDB-Cluster-Token", raftHA.getClusterToken());
        conn.setRequestProperty("X-ArcadeDB-Forwarded-User", "root");
      }
      try {
        if (conn.getResponseCode() == 200) {
          final String body = new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
          final JSONObject json = new JSONObject(body);
          if (json.has("databases")) {
            final JSONArray dbs = json.getJSONArray("databases");
            final Set<String> names = new HashSet<>(dbs.length());
            for (int i = 0; i < dbs.length(); i++)
              names.add(dbs.getString(i));
            return names;
          }
        }
      } finally {
        conn.disconnect();
      }
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING,
          "Could not fetch database list from leader %s, skipping stale database cleanup: %s",
          leaderHttpAddr, e.getMessage());
    }
    return Set.of();
  }

  // -- Crash recovery --

  /**
   * Scans the database directory for pending snapshot swap markers and completes or rolls back
   * the swap. Must be called on startup BEFORE opening databases.
   * <p>
   * Recovery logic:
   * <ul>
   *   <li>If the temp snapshot dir exists: complete the swap (move temp to live, clean up backup)</li>
   *   <li>If the temp snapshot is gone but backup exists: rollback (restore backup to live)</li>
   *   <li>If the live path already exists (swap completed, marker not deleted): just clean up</li>
   * </ul>
   */
  public static void recoverPendingSnapshotSwaps(final Path databaseDir) {
    final File[] markerFiles = databaseDir.toFile().listFiles((dir, name) -> name.endsWith(".snapshot-pending"));
    if (markerFiles == null || markerFiles.length == 0)
      return;

    for (final File marker : markerFiles) {
      final String baseName = marker.getName().replace(".snapshot-pending", "");
      final Path livePath = databaseDir.resolve(baseName);
      final Path backupPath = databaseDir.resolve(baseName + ".snapshot-old");
      final Path snapshotPath = databaseDir.resolve(baseName + ".snapshot-tmp");

      LogManager.instance().log(SnapshotInstaller.class, Level.WARNING,
          "Found pending snapshot swap marker for database '%s', recovering...", baseName);

      try {
        if (Files.exists(snapshotPath)) {
          if (Files.exists(livePath)) {
            FileUtils.deleteRecursively(backupPath.toFile());
            Files.move(livePath, backupPath);
          }
          Files.move(snapshotPath, livePath);
          deleteStaleWalFiles(livePath);
          FileUtils.deleteRecursively(backupPath.toFile());
          LogManager.instance().log(SnapshotInstaller.class, Level.INFO,
              "Snapshot swap recovery completed for database '%s'", baseName);

        } else if (Files.exists(backupPath) && !Files.exists(livePath)) {
          Files.move(backupPath, livePath);
          LogManager.instance().log(SnapshotInstaller.class, Level.WARNING,
              "Snapshot swap rolled back for database '%s' (snapshot data was lost)", baseName);

        } else if (Files.exists(livePath)) {
          FileUtils.deleteRecursively(backupPath.toFile());
          LogManager.instance().log(SnapshotInstaller.class, Level.INFO,
              "Snapshot swap already completed for database '%s', cleaning up", baseName);
        }
        marker.delete();
      } catch (final IOException e) {
        // Keep the marker so the next restart can retry. The marker is the only signal that
        // recovery is still needed when the directory is in an intermediate state.
        LogManager.instance().log(SnapshotInstaller.class, Level.SEVERE,
            "CRITICAL: Failed to recover snapshot swap for database '%s'. "
                + "The marker file has been preserved for retry on next startup. Error: %s",
            baseName, e.getMessage());
      }
    }
  }

  public static void deleteStaleWalFiles(final Path dbPath) {
    final File[] walFiles = dbPath.toFile().listFiles((dir, name) -> name.endsWith(".wal"));
    if (walFiles != null)
      for (final File walFile : walFiles)
        if (!walFile.delete())
          LogManager.instance().log(SnapshotInstaller.class, Level.WARNING,
              "Failed to delete stale WAL file: %s", walFile.getName());
  }

  private static void copyWithLimit(final InputStream in, final OutputStream out, final long maxBytes,
      final String entryName) throws IOException {
    final byte[] buf = new byte[8192];
    long total = 0;
    int read;
    while ((read = in.read(buf)) != -1) {
      total += read;
      if (total > maxBytes)
        throw new ReplicationException("Snapshot entry '" + entryName + "' exceeds size limit of " + maxBytes + " bytes");
      out.write(buf, 0, read);
    }
  }
}
