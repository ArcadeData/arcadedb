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

  /** Default cap on uncompressed bytes per ZIP entry extracted from a snapshot. Overridable via
   *  {@link GlobalConfiguration#HA_SNAPSHOT_MAX_ENTRY_SIZE} for deployments whose largest
   *  component file legitimately exceeds this size. Protects the follower from a malicious or
   *  corrupted leader entry that would inflate indefinitely; sized well above the largest
   *  realistic ArcadeDB component file while keeping a memoryless streaming check. The
   *  per-entry compression-ratio guard below is the primary defense against decompression bombs;
   *  this cap is a coarse secondary bound. Package-private for unit testing. */
  static final long DEFAULT_MAX_ZIP_ENTRY_UNCOMPRESSED_BYTES = 10L * 1024 * 1024 * 1024;

  /** Connect timeout for the snapshot HTTP GET. Read timeout is governed separately by
   *  {@link GlobalConfiguration#HA_SNAPSHOT_DOWNLOAD_TIMEOUT} because it must cover the full
   *  transfer of a potentially multi-GB database. */
  private static final int SNAPSHOT_CONNECT_TIMEOUT_MS = 30_000;

  /** Connect and read timeout for the lightweight `/api/v1/server` call used to fetch the
   *  leader's current database list. Kept short because the response is tiny and a slow leader
   *  here only degrades stale-database cleanup, not snapshot installation itself. */
  private static final int LEADER_METADATA_TIMEOUT_MS = 10_000;

  /**
   * Sentinel file written inside the temp snapshot directory after all ZIP entries have been
   * successfully extracted. {@link #recoverPendingSnapshotSwaps(Path)} checks for this marker
   * to distinguish a complete download from one interrupted mid-write (power failure, OOM,
   * kill -9). Without this marker the temp directory is untrusted and will be discarded.
   */
  public static final String SNAPSHOT_COMPLETE_MARKER = ".snapshot-complete";

  /**
   * Outcome of {@link #performSnapshotSwap}. Drives whether the caller should attempt to
   * reopen the database and whether the pending marker must be retained for startup recovery.
   */
  enum SwapOutcome {
    /** Swap completed: {@code dbPath} contains the new snapshot, marker and backup cleaned up. */
    SUCCESS,
    /** Swap failed, but rollback restored {@code dbPath} from the backup; marker cleaned up. */
    ROLLED_BACK,
    /** Swap AND rollback both failed; {@code dbPath} may be missing or partial. The pending
     *  marker has been retained so {@link #recoverPendingSnapshotSwaps(Path)} can retry on
     *  the next startup. Callers must NOT attempt to reopen the database in this state. */
    UNRECOVERABLE
  }

  /**
   * Test seam for the single-argument path rename. Production code passes {@link Files#move}.
   */
  @FunctionalInterface
  interface PathMover {
    void move(Path src, Path dst) throws IOException;
  }

  private static final PathMover DEFAULT_MOVER = Files::move;

  private final ArcadeDBServer server;
  private final RaftHAServer   raftHA;

  public SnapshotInstaller(final ArcadeDBServer server, final RaftHAServer raftHA) {
    this.server = server;
    this.raftHA = raftHA;
  }

  /**
   * Downloads all databases from the leader. Called when reinitialize() detects a gap between
   * the snapshot index and the persisted applied index, indicating the follower missed data.
   * Snapshot failures for individual databases are logged and skipped rather than aborting the whole sync.
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
    syncDatabasesFromLeader(leaderHttpAddr, false);
    LogManager.instance().log(this, Level.INFO, "Snapshot installation from leader completed");
  }

  /**
   * Handles the {@code notifyInstallSnapshotFromLeader} Ratis callback by downloading all databases
   * from the identified leader. Any failure aborts the sync and propagates to the caller.
   *
   * @param leaderHttpAddr  resolved HTTP address of the leader
   */
  public void installFromLeaderNotification(final String leaderHttpAddr) throws IOException {
    syncDatabasesFromLeader(leaderHttpAddr, true);
  }

  /**
   * Iterates over local databases, drops any that no longer exist on the leader, and installs
   * a fresh snapshot for each remaining one.
   *
   * <p>Note: databases present on the leader but absent locally are not created here. Those are
   * handled via Raft log replay (CREATE_DATABASE entries), not snapshot sync.
   *
   * @param leaderHttpAddr  HTTP address of the current leader
   * @param strict          when {@code true}, a missing database or install failure throws
   *                        {@link ReplicationException}; when {@code false}, such cases are
   *                        logged as warnings and skipped
   */
  private void syncDatabasesFromLeader(final String leaderHttpAddr, final boolean strict) throws IOException {
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
        if (strict)
          throw new ReplicationException("Database '" + dbName + "' not found during snapshot installation");
        LogManager.instance().log(this, Level.WARNING, "Database '%s' was dropped during snapshot installation, skipping", dbName);
        continue;
      }

      LogManager.instance().log(this, Level.INFO, "Installing snapshot for database '%s' from leader %s...", dbName, leaderHttpAddr);
      if (strict) {
        installDatabaseSnapshot(db, leaderHttpAddr, dbName);
      } else {
        try {
          installDatabaseSnapshot(db, leaderHttpAddr, dbName);
        } catch (final Exception e) {
          LogManager.instance().log(this, Level.WARNING, "Failed to install snapshot for database '%s', skipping: %s", dbName, e.getMessage());
        }
      }
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
    //
    // Mark the install as in-progress so that any HTTP request that hits the close → swap →
    // reopen window sees its failure translated into 503 + Retry-After by the base HTTP handler,
    // making the transient error client-visible and safely retryable with the idempotency cache.
    final Path markerFile = dbPath.resolveSibling(dbPath.getFileName() + ".snapshot-pending");
    final SwapOutcome outcome;

    server.setSnapshotInstallInProgress(true);
    try {
      db.getEmbedded().close();

      outcome = performSnapshotSwap(dbPath, tempDir, backupDir, markerFile, databaseName, DEFAULT_MOVER);

      // Only reopen when the on-disk state is known-good. On UNRECOVERABLE, dbPath may be missing
      // or partial and server.getDatabase() could silently open a corrupt or auto-created empty
      // database, masking data loss. The retained pending marker lets startup recovery retry.
      if (outcome != SwapOutcome.UNRECOVERABLE) {
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
      } else {
        LogManager.instance().log(this, Level.SEVERE,
            "CRITICAL: database '%s' left in inconsistent state (swap and rollback both failed). "
                + "Path '%s' may be missing or partial. Pending marker '%s' retained for startup recovery. "
                + "Manual recovery or node restart required.",
            databaseName, dbPath, markerFile);
      }
    } finally {
      server.setSnapshotInstallInProgress(false);
    }

    if (outcome != SwapOutcome.SUCCESS)
      throw new ReplicationException(outcome == SwapOutcome.UNRECOVERABLE
          ? "Snapshot installation failed and rollback also failed for database '" + databaseName
              + "'; pending marker '" + markerFile + "' retained for startup recovery"
          : "Snapshot installation failed during directory swap for database '" + databaseName + "' (rolled back)");
  }

  /**
   * Atomically replaces {@code dbPath} with the contents of {@code tempDir} using a pending
   * marker file for crash safety. Package-private and static to allow direct unit testing of
   * the double-failure (swap + rollback) path via an injected {@link PathMover}.
   * <p>
   * Sequence:
   * <ol>
   *   <li>Write {@code markerFile} so a mid-operation crash is detected on startup.</li>
   *   <li>Move {@code dbPath} to {@code backupDir} (if it exists).</li>
   *   <li>Move {@code tempDir} to {@code dbPath} and clean up WAL / completion marker.</li>
   *   <li>Delete {@code markerFile} and {@code backupDir}.</li>
   * </ol>
   * On any failure, attempts to restore {@code dbPath} from {@code backupDir}. The marker file
   * is only deleted on successful completion or successful rollback; in the {@code UNRECOVERABLE}
   * case it is left on disk so {@link #recoverPendingSnapshotSwaps(Path)} can retry.
   *
   * @param mover hook for the {@link Files#move(Path, Path, java.nio.file.CopyOption...)} call
   *              used by every destructive rename; production passes {@link #DEFAULT_MOVER}
   */
  static SwapOutcome performSnapshotSwap(final Path dbPath, final Path tempDir, final Path backupDir,
      final Path markerFile, final String databaseName, final PathMover mover) throws IOException {
    try {
      // Write the pending marker BEFORE any destructive operation.
      // If we crash after this, recoverPendingSnapshotSwaps() will finish the job.
      Files.writeString(markerFile, databaseName);

      if (Files.exists(dbPath)) {
        FileUtils.deleteRecursively(backupDir.toFile());
        mover.move(dbPath, backupDir);
      }
      mover.move(tempDir, dbPath);
      deleteStaleWalFiles(dbPath);
      Files.deleteIfExists(dbPath.resolve(SNAPSHOT_COMPLETE_MARKER));

      Files.deleteIfExists(markerFile);
      FileUtils.deleteRecursively(backupDir.toFile());

      LogManager.instance().log(SnapshotInstaller.class, Level.INFO,
          "Database snapshot for '%s' installed successfully", databaseName);
      return SwapOutcome.SUCCESS;

    } catch (final Exception e) {
      LogManager.instance().log(SnapshotInstaller.class, Level.SEVERE,
          "Snapshot swap failed for '%s', attempting rollback...", databaseName);
      boolean rollbackSucceeded = false;
      try {
        if (Files.exists(backupDir)) {
          FileUtils.deleteRecursively(dbPath.toFile());
          mover.move(backupDir, dbPath);
        }
        rollbackSucceeded = true;
      } catch (final Exception rollbackEx) {
        LogManager.instance().log(SnapshotInstaller.class, Level.SEVERE,
            "CRITICAL: Failed to rollback snapshot swap for '%s'. Database may be unavailable. "
                + "Pending marker '%s' will be retained for startup recovery. Error: %s",
            databaseName, markerFile, rollbackEx.getMessage());
      }

      FileUtils.deleteRecursively(tempDir.toFile());

      if (rollbackSucceeded) {
        // Only safe to drop the marker once dbPath has been restored to a consistent state.
        Files.deleteIfExists(markerFile);
        return SwapOutcome.ROLLED_BACK;
      }
      return SwapOutcome.UNRECOVERABLE;
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
      } catch (final IOException e) {
        FileUtils.deleteRecursively(tempDir.toFile());
        if (attempt == SNAPSHOT_DOWNLOAD_MAX_RETRIES) {
          LogManager.instance().log(this, Level.SEVERE,
              "All %d snapshot download attempts failed for database '%s'", SNAPSHOT_DOWNLOAD_MAX_RETRIES, databaseName);
          throw e;
        }

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
   * <p>
   * Package-private to enable a local-{@code HttpServer}-based integration test of the download
   * path (happy path, zip slip, non-200 response, HTTPS/SSL mismatch) without standing up a
   * full Raft cluster.
   */
  void downloadSnapshot(final String snapshotUrl, final Path tempDir,
      final String databaseName) throws IOException {
    LogManager.instance().log(this, Level.INFO, "Downloading database snapshot from %s...", snapshotUrl);

    final HttpURLConnection connection;
    try {
      connection = (HttpURLConnection) new URI(snapshotUrl).toURL().openConnection();
    } catch (final URISyntaxException e) {
      throw new IOException("Invalid snapshot URL: " + snapshotUrl, e);
    }

    if (connection instanceof javax.net.ssl.HttpsURLConnection httpsConn) {
      final javax.net.ssl.SSLContext sslContext = server.getHttpServer().getSSLContext();
      if (sslContext != null)
        httpsConn.setSSLSocketFactory(sslContext.getSocketFactory());
      // If SSL is enabled, getSSLContext() either returns a valid context or throws.
      // If SSL is disabled, the connection should not be HTTPS. Reaching here with a null
      // sslContext on an HTTPS connection means a URL/config mismatch - fail explicitly.
      else
        throw new ReplicationException(
            "HTTPS snapshot connection but SSL is not enabled in configuration. "
                + "Check arcadedb.network.useSSL and the snapshot URL: " + snapshotUrl);
    }

    connection.setRequestMethod("GET");
    connection.setConnectTimeout(SNAPSHOT_CONNECT_TIMEOUT_MS);
    connection.setReadTimeout(server.getConfiguration().getValueAsInteger(GlobalConfiguration.HA_SNAPSHOT_DOWNLOAD_TIMEOUT));

    final long maxEntrySize = server.getConfiguration().getValueAsLong(GlobalConfiguration.HA_SNAPSHOT_MAX_ENTRY_SIZE);

    if (raftHA.getClusterToken() != null)
      connection.setRequestProperty("X-ArcadeDB-Cluster-Token", raftHA.getClusterToken());

    try {
      final int responseCode = connection.getResponseCode();
      if (responseCode != 200)
        throw new ReplicationException("Failed to download snapshot from " + snapshotUrl + ": HTTP " + responseCode);

      FileUtils.deleteRecursively(tempDir.toFile());
      Files.createDirectories(tempDir);

      // Wrap the raw connection input so we can measure compressed bytes consumed per entry and
      // reject suspicious compression ratios (defense against decompression-bomb snapshots).
      final CountingInputStream rawCounter = new CountingInputStream(connection.getInputStream());
      try (final ZipInputStream zipIn = new ZipInputStream(rawCounter)) {
        ZipEntry zipEntry;
        while ((zipEntry = zipIn.getNextEntry()) != null) {
          final Path targetFile = tempDir.resolve(zipEntry.getName()).normalize();

          // Security: prevent zip slip
          if (!targetFile.startsWith(tempDir))
            throw new ReplicationException("Zip slip detected in snapshot: " + zipEntry.getName());

          LogManager.instance().log(this, Level.FINE, "Extracting snapshot file: %s", zipEntry.getName());

          final long compressedStart = rawCounter.getCount();
          final long uncompressedBytes;
          if (zipEntry.isDirectory()) {
            Files.createDirectories(targetFile);
            uncompressedBytes = 0;
          } else {
            Files.createDirectories(targetFile.getParent());

            // Security: resolve symlinks in parent directories and verify the real path
            // stays within tempDir. The normalize()+startsWith() check above handles ../
            // path traversal, but a symlink in a parent component could redirect outside.
            final Path realParent = targetFile.getParent().toRealPath();
            final Path realTempDir = tempDir.toRealPath();
            if (!realParent.startsWith(realTempDir))
              throw new ReplicationException(
                  "Symlink escape detected in snapshot parent directory: " + zipEntry.getName()
                      + " (resolved to " + realParent + ", expected within " + realTempDir + ")");

            // Also refuse to write through a symlink at the file level
            if (Files.isSymbolicLink(targetFile))
              throw new ReplicationException("Symlink detected in snapshot extraction path: " + zipEntry.getName());

            try (final FileOutputStream fos = new FileOutputStream(targetFile.toFile())) {
              uncompressedBytes = copyWithLimit(zipIn, fos, maxEntrySize, zipEntry.getName());
            }
          }
          zipIn.closeEntry();

          // Decompression-bomb defense. Compute the ratio only when the inflated entry is large
          // enough that a high ratio implies an attack (tiny entries like schema JSON can
          // legitimately inflate 100x+ and pose no memory risk). Uses the delta of the raw
          // counter across the full entry (header + payload + descriptor trailer), which slightly
          // OVER-estimates the compressed payload and therefore UNDER-estimates the ratio -
          // intentional, as we want zero false positives on borderline-compressible data.
          final long compressedBytes = Math.max(1L, rawCounter.getCount() - compressedStart);
          if (uncompressedBytes > MIN_RATIO_CHECK_BYTES
              && uncompressedBytes / compressedBytes > MAX_COMPRESSION_RATIO_PER_ENTRY)
            throw new ReplicationException("Suspicious compression ratio for snapshot entry '"
                + zipEntry.getName() + "': inflated " + uncompressedBytes + " bytes from "
                + compressedBytes + " (ratio > " + MAX_COMPRESSION_RATIO_PER_ENTRY + ":1)");
        }
      } catch (final Exception e) {
        FileUtils.deleteRecursively(tempDir.toFile());
        throw e;
      }

      // Write completion marker AFTER all ZIP entries have been successfully extracted.
      // If the JVM crashes before this point, the temp directory will lack the marker and
      // recoverPendingSnapshotSwaps() will discard it instead of swapping in corrupt data.
      Files.writeString(tempDir.resolve(SNAPSHOT_COMPLETE_MARKER), "");
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
      if (conn instanceof javax.net.ssl.HttpsURLConnection httpsConn) {
        final javax.net.ssl.SSLContext sslContext = server.getHttpServer().getSSLContext();
        if (sslContext != null)
          httpsConn.setSSLSocketFactory(sslContext.getSocketFactory());
      }
      conn.setRequestMethod("GET");
      conn.setConnectTimeout(LEADER_METADATA_TIMEOUT_MS);
      conn.setReadTimeout(LEADER_METADATA_TIMEOUT_MS);
      if (raftHA.getClusterToken() != null) {
        conn.setRequestProperty("X-ArcadeDB-Cluster-Token", raftHA.getClusterToken());
        // "root" is the correct identity here: this call is issued by the follower on its own
        // behalf (to reconcile its local database list against the leader's), NOT on behalf of
        // a user-initiated request. The cluster token already authenticates the hop; the
        // forwarded-user header is redundant for authorization and is included only to satisfy
        // the handler's requirement that every authenticated request name a user.
        // Contrast with PostVerifyDatabaseHandler, which forwards the ORIGINAL caller's identity
        // because it is proxying a user request across the cluster.
        conn.setRequestProperty("X-ArcadeDB-Forwarded-User", "root");
      }
      try {
        if (conn.getResponseCode() == 200) {
          final String body;
          try (final InputStream is = conn.getInputStream()) {
            body = new String(is.readAllBytes(), StandardCharsets.UTF_8);
          }
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
        // Check whether the temp snapshot directory contains a valid, complete download.
        // The completion marker is written as the last step of a successful extraction.
        // If it is missing, the download was interrupted (power failure, OOM, kill -9) and
        // the temp directory may contain truncated or missing files - discard it.
        boolean snapshotValid = false;
        if (Files.exists(snapshotPath)) {
          if (Files.exists(snapshotPath.resolve(SNAPSHOT_COMPLETE_MARKER))) {
            snapshotValid = true;
          } else {
            LogManager.instance().log(SnapshotInstaller.class, Level.WARNING,
                "Incomplete snapshot download for '%s' (no completion marker), discarding partial data", baseName);
            FileUtils.deleteRecursively(snapshotPath.toFile());
          }
        }

        if (snapshotValid) {
          if (Files.exists(livePath)) {
            FileUtils.deleteRecursively(backupPath.toFile());
            Files.move(livePath, backupPath);
          }
          Files.move(snapshotPath, livePath);
          deleteStaleWalFiles(livePath);
          Files.deleteIfExists(livePath.resolve(SNAPSHOT_COMPLETE_MARKER));
          FileUtils.deleteRecursively(backupPath.toFile());
          LogManager.instance().log(SnapshotInstaller.class, Level.INFO,
              "Snapshot swap recovery completed for database '%s'", baseName);

        } else if (Files.exists(backupPath) && !Files.exists(livePath)) {
          Files.move(backupPath, livePath);
          LogManager.instance().log(SnapshotInstaller.class, Level.WARNING,
              "Snapshot swap rolled back for database '%s' (snapshot data was lost or incomplete)", baseName);

        } else if (Files.exists(livePath)) {
          FileUtils.deleteRecursively(backupPath.toFile());
          LogManager.instance().log(SnapshotInstaller.class, Level.INFO,
              "Snapshot swap already completed for database '%s', cleaning up", baseName);
        }
        if (!marker.delete())
          LogManager.instance().log(SnapshotInstaller.class, Level.WARNING,
              "Failed to delete snapshot swap marker file '%s' - it will be re-processed on next startup",
              marker.getName());
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

  /**
   * Copy buffer size for ZIP extraction during snapshot download. Sized for the database
   * transfer described in the class Javadoc (tens to hundreds of GB) rather than the default
   * 8 KB used for small streams - a larger buffer meaningfully reduces the number of
   * read/inflate/write syscalls per snapshot and dominates over the one-time allocation cost.
   */
  private static final int COPY_BUFFER_SIZE = 512 * 1024;

  /**
   * Maximum tolerated uncompressed:compressed size ratio per ZIP entry. 200:1 comfortably
   * accommodates real-world page data (DEFLATE typically compresses 5-20x) while rejecting
   * crafted decompression bombs that inflate 1000:1+. Package-private for unit testing.
   */
  static final int MAX_COMPRESSION_RATIO_PER_ENTRY = 200;

  /**
   * Minimum uncompressed entry size before applying the ratio check. Tiny entries (the
   * completion marker, short schema snippets) naturally have ill-defined or extreme ratios
   * and pose no memory risk, so skipping them avoids false positives without weakening the
   * defense - a decompression bomb must inflate to many megabytes to actually threaten the
   * process.
   */
  static final long MIN_RATIO_CHECK_BYTES = 64L * 1024L;

  /**
   * Streams bytes from {@code in} to {@code out} until EOF, throwing a {@link ReplicationException}
   * if the total exceeds {@code maxBytes}, and returns the number of bytes copied. Guarantees
   * no silent truncation: the size check runs BEFORE each {@code out.write(...)}, so if the cap
   * is exceeded we throw without writing the over-limit chunk, and the caller's {@code try/catch}
   * deletes {@code tempDir} so a partial file cannot be mistaken for a valid extraction.
   * Network-level truncation (connection dropped mid-entry) is a separate concern handled by
   * the surrounding ZIP layer via {@link java.util.zip.ZipInputStream#closeEntry()} (DEFLATE
   * trailer / CRC32 check) and by the final {@code SNAPSHOT_COMPLETE_MARKER} write, which
   * happens only after every entry extracts cleanly.
   * <p>
   * Package-private to enable direct unit testing of the no-silent-truncate contract.
   */
  static long copyWithLimit(final InputStream in, final OutputStream out, final long maxBytes,
      final String entryName) throws IOException {
    final byte[] buf = new byte[COPY_BUFFER_SIZE];
    long total = 0;
    int read;
    while ((read = in.read(buf)) != -1) {
      total += read;
      if (total > maxBytes)
        throw new ReplicationException("Snapshot entry '" + entryName + "' exceeds size limit of " + maxBytes + " bytes");
      out.write(buf, 0, read);
    }
    return total;
  }

  /**
   * FilterInputStream that counts bytes consumed by the downstream reader. Used to measure the
   * compressed bytes a {@link ZipInputStream} reads per entry so we can enforce a per-entry
   * compression-ratio cap. The count intentionally includes the ZIP's local file header and
   * optional data descriptor for each entry (ZipInputStream reads them via the same stream);
   * this over-estimates the pure compressed payload and therefore under-estimates the ratio,
   * which is the safe direction for the check.
   */
  static final class CountingInputStream extends java.io.FilterInputStream {
    private long count;

    CountingInputStream(final InputStream in) {
      super(in);
    }

    long getCount() {
      return count;
    }

    @Override
    public int read() throws IOException {
      final int b = super.read();
      if (b != -1)
        count++;
      return b;
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
      final int n = super.read(b, off, len);
      if (n > 0)
        count += n;
      return n;
    }

    @Override
    public long skip(final long n) throws IOException {
      final long skipped = super.skip(n);
      if (skipped > 0)
        count += skipped;
      return skipped;
    }

    /** {@link ZipInputStream} does not call {@code mark/reset}, but honor the contract. */
    @Override
    public boolean markSupported() {
      return false;
    }
  }
}
