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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.utility.FileUtils;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.KeyStore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
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

  static final String SNAPSHOT_NEW_DIR       = ".snapshot-new";
  static final String SNAPSHOT_BACKUP_DIR    = ".snapshot-backup";
  static final String SNAPSHOT_PENDING_FILE  = ".snapshot-pending";
  static final String SNAPSHOT_COMPLETE_FILE = ".snapshot-complete";

  /**
   * Maximum tolerated uncompressed:compressed size ratio per ZIP entry.
   * ArcadeDB page files (e.g. dictionary pages of 327 680 bytes) are fixed-size and freshly
   * initialised with mostly-zero content, so legitimate DEFLATE ratios can exceed 900:1.
   * 100 000:1 provides comfortable headroom above any real page while still catching
   * crafted decompression bombs; the 10 GB absolute limit is the primary protection.
   * Package-private for unit testing.
   */
  static final int MAX_COMPRESSION_RATIO = 100_000;

  /**
   * Minimum uncompressed entry size before applying the ratio check. Tiny entries (schema JSON,
   * completion marker) naturally have extreme ratios and pose no memory risk, so skipping them
   * avoids false positives without weakening the defense. Package-private for unit testing.
   */
  static final long MIN_RATIO_CHECK_BYTES = 64L * 1024L;

  /**
   * Maximum allowed uncompressed size for a single ZIP entry (10 GB). Entries exceeding this
   * limit trigger a zip-bomb defense exception. Package-private for unit testing.
   */
  static final long MAX_ZIP_ENTRY_UNCOMPRESSED_BYTES = 10L * 1024 * 1024 * 1024;

  /**
   * Logged at most once: warns that SSL is enabled but the snapshot is being downloaded over plain
   * HTTP because no HTTPS endpoint could be resolved for the leader.
   */
  private static final AtomicBoolean PLAIN_HTTP_FALLBACK_WARNED = new AtomicBoolean(false);

  private SnapshotInstaller() {
  }

  /**
   * Installs a database snapshot from the leader using crash-safe atomic swap.
   * Downloads the snapshot ZIP with retry, extracts to a temp directory, and atomically
   * swaps it into the live database path.
   *
   * @param databaseName    name of the database to install
   * @param databasePath    absolute path to the live database directory
   * @param leaderHttpAddr  leader's plain HTTP address (host:httpPort)
   * @param leaderHttpsAddr leader's HTTPS address (host:httpsPort), or {@code null} when no encrypted
   *                        endpoint is known. When SSL is enabled and this is non-null the snapshot is
   *                        downloaded over HTTPS; otherwise it falls back to plain HTTP on
   *                        {@code leaderHttpAddr} (issue #4470).
   * @param clusterToken    cluster authentication token (may be null)
   * @param server          the ArcadeDB server instance for re-registering the database
   */
  public static void install(final String databaseName, final String databasePath,
      final String leaderHttpAddr, final String leaderHttpsAddr, final String clusterToken,
      final ArcadeDBServer server) throws IOException {
    install(databaseName, databasePath, () -> leaderHttpAddr, () -> leaderHttpsAddr, clusterToken, server);
  }

  /**
   * Overload that resolves the leader HTTP/HTTPS addresses on each retry attempt. Use this when the
   * leader may not be known yet at the moment install is invoked (e.g. during bootstrap-mismatch
   * recovery on startup, before Ratis has finished electing a leader). When a supplier returns
   * null on a given attempt, that attempt is treated as a failure and the next retry will resolve
   * again, giving leader election time to complete.
   */
  public static void install(final String databaseName, final String databasePath,
      final Supplier<String> leaderHttpAddrSupplier, final Supplier<String> leaderHttpsAddrSupplier,
      final String clusterToken, final ArcadeDBServer server) throws IOException {

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

    // PHASE 1 - DOWNLOAD into .snapshot-new with the live database STILL OPEN. The historical behaviour
    // closed it up-front, so any download failure (leader unreachable, network blip) left it closed and
    // deregistered with no recovery. Staging first means we touch the live files only on success.
    try {
      downloadWithRetry(databaseName, snapshotNew, leaderHttpAddrSupplier, leaderHttpsAddrSupplier, clusterToken,
          maxRetries, retryBaseMs, server);
    } catch (final IOException e) {
      // Download failed: the live database has not been touched and is still open. Drop the staging
      // directory and rethrow so the caller (or Raft) can retry later without losing availability.
      deleteDirectoryIfExists(snapshotNew);
      Files.deleteIfExists(pendingMarker);
      throw e;
    }

    // Mark download as complete
    Files.writeString(snapshotNew.resolve(SNAPSHOT_COMPLETE_FILE), "");

    // PHASE 2 - SWAP. Set the server-wide flag BEFORE closing the database so HTTP handlers return 503
    // while the files are being moved.
    server.setSnapshotInstallInProgress(true);
    try {
      // Close + deregister the live database now that a complete snapshot is staged on disk. The DB
      // must be closed before the file move so no open handles point at the directory being swapped.
      closeLocalDatabaseIfOpen(server, databaseName);

      // Swap: live -> backup, new -> live. atomicSwap restores the backup itself if the second rename
      // fails, so on an IOException here the live directory already holds the previous copy.
      try {
        atomicSwap(dbPath, snapshotNew, snapshotBackup);
      } catch (final IOException swapEx) {
        // Live files were restored by atomicSwap; bring the previous database back online so the node
        // keeps serving instead of being stuck with a closed, deregistered database.
        reopenQuietly(server, databaseName);
        Files.deleteIfExists(pendingMarker);
        throw swapEx;
      }

      // Swap succeeded: live = new snapshot, .snapshot-backup = previous copy (retained until the new
      // snapshot is confirmed to open). Cleanup then validate the install by reopening.
      cleanupWalFiles(dbPath);
      // Remove the completion marker from the now-live directory
      Files.deleteIfExists(dbPath.resolve(SNAPSHOT_COMPLETE_FILE));

      try {
        // Re-open the database so the server registers it (also validates the snapshot is loadable)
        server.getDatabase(databaseName);
      } catch (final RuntimeException openEx) {
        // The freshly installed snapshot will not open (corrupt/incompatible files). Roll back to the
        // previous local copy and reopen it so the node is never left with a closed database.
        LogManager.instance().log(SnapshotInstaller.class, Level.SEVERE,
            "Installed snapshot for '%s' failed to open; rolling back to the previous local copy", openEx, databaseName);
        rollbackToBackup(dbPath, snapshotBackup);
        reopenQuietly(server, databaseName);
        Files.deleteIfExists(pendingMarker);
        throw new IOException("Snapshot for '" + databaseName
            + "' downloaded but failed to open; rolled back to the previous local copy", openEx);
      }

      // Success: drop the retained backup and clear the pending marker.
      deleteDirectoryIfExists(snapshotBackup);
      Files.deleteIfExists(pendingMarker);

      HALog.log(SnapshotInstaller.class, HALog.BASIC, "Snapshot for '%s' installed successfully", databaseName);
    } finally {
      server.setSnapshotInstallInProgress(false);
    }
  }

  /**
   * Resolves the on-disk path of a database whether or not it is currently open, so callers no longer
   * need to keep the database open just to read its path before an install. When the database is not
   * registered the path is derived from {@link GlobalConfiguration#SERVER_DATABASE_DIRECTORY}.
   */
  public static String resolveDatabasePath(final ArcadeDBServer server, final String databaseName) {
    // Best-effort: the exists/get pair is not atomic, but it only resolves a path before the download
    // phase (no data at risk) and getDatabase returns a valid path even if it has to reopen.
    if (server.existsDatabase(databaseName))
      return ((DatabaseInternal) server.getDatabase(databaseName)).getDatabasePath();
    return server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY)
        + File.separator + databaseName;
  }

  /**
   * Closes and deregisters the local database if it is currently registered. No-op when the database
   * is absent (late joiner with no local copy). Closing through the embedded instance mirrors the
   * resync/bootstrap install paths.
   */
  private static void closeLocalDatabaseIfOpen(final ArcadeDBServer server, final String databaseName) {
    if (server.existsDatabase(databaseName)) {
      final DatabaseInternal db = (DatabaseInternal) server.getDatabase(databaseName);
      db.getEmbedded().close();
      server.removeDatabase(databaseName);
    }
  }

  /**
   * Best-effort reopen used by the rollback paths: a failure here must not mask the original cause,
   * so it only logs. {@link ArcadeDBServer#getDatabase} opens and registers the database from disk
   * when it is not already registered.
   */
  private static void reopenQuietly(final ArcadeDBServer server, final String databaseName) {
    try {
      server.getDatabase(databaseName);
    } catch (final Exception e) {
      LogManager.instance().log(SnapshotInstaller.class, Level.SEVERE,
          "Failed to reopen database '%s' after a snapshot-install rollback; manual intervention may be required",
          e, databaseName);
    }
  }

  /**
   * Rolls the live database directory back to the retained {@code .snapshot-backup} copy after a
   * post-swap failure. Clears the failed snapshot files first so entries present only in the failed
   * snapshot do not linger, then moves the backup contents back into place.
   */
  private static void rollbackToBackup(final Path dbPath, final Path snapshotBackup) {
    try {
      if (!Files.isDirectory(snapshotBackup)) {
        LogManager.instance().log(SnapshotInstaller.class, Level.SEVERE,
            "Cannot roll back snapshot install for %s: backup directory is missing", null, dbPath);
        return;
      }
      clearLiveDatabaseFiles(dbPath);
      restoreBackup(dbPath, snapshotBackup);
    } catch (final IOException e) {
      LogManager.instance().log(SnapshotInstaller.class, Level.SEVERE,
          "Failed to roll back snapshot install for %s: %s", e, dbPath, e.getMessage());
    }
  }

  /**
   * Deletes every non-{@code .snapshot*} entry in the live database directory. Used before restoring
   * a backup so files that exist only in a failed snapshot are not left behind.
   */
  private static void clearLiveDatabaseFiles(final Path dbPath) throws IOException {
    try (final DirectoryStream<Path> stream = Files.newDirectoryStream(dbPath)) {
      for (final Path entry : stream) {
        if (entry.getFileName().toString().startsWith(".snapshot"))
          continue;
        if (Files.isDirectory(entry))
          FileUtils.deleteRecursively(entry.toFile());
        else
          Files.delete(entry);
      }
    }
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
   * Package-private overload used by tests - assumes plain HTTP (no SSL).
   */
  static void downloadWithRetry(final String databaseName, final Path snapshotNewDir,
      final String leaderHttpAddr, final String clusterToken,
      final int maxRetries, final long retryBaseMs) throws IOException {
    downloadWithRetry(databaseName, snapshotNewDir, () -> leaderHttpAddr, () -> null, clusterToken, maxRetries, retryBaseMs,
        null);
  }

  static void downloadWithRetry(final String databaseName, final Path snapshotNewDir,
      final Supplier<String> leaderHttpAddrSupplier, final Supplier<String> leaderHttpsAddrSupplier,
      final String clusterToken, final int maxRetries, final long retryBaseMs, final ArcadeDBServer server)
      throws IOException {

    final boolean useSSL = server != null && server.getConfiguration().getValueAsBoolean(GlobalConfiguration.NETWORK_USE_SSL);
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

      // Resolve the leader endpoint on every attempt: during bootstrap-mismatch recovery the
      // first attempt typically races Ratis leader election and observes a null address.
      // When SSL is enabled, prefer the HTTPS endpoint so the snapshot travels encrypted; the plain
      // HTTP listener and the HTTPS listener bind to *different* ports, so forcing an HTTPS scheme
      // onto the plain HTTP port is what produced "Unsupported or unrecognized SSL message" (#4470).
      boolean https = false;
      String endpoint = null;
      if (useSSL && leaderHttpsAddrSupplier != null) {
        endpoint = leaderHttpsAddrSupplier.get();
        if (endpoint != null)
          https = true;
      }
      if (endpoint == null) {
        endpoint = leaderHttpAddrSupplier.get();
        if (useSSL && endpoint != null && PLAIN_HTTP_FALLBACK_WARNED.compareAndSet(false, true))
          LogManager.instance().log(SnapshotInstaller.class, Level.WARNING,
              "SSL is enabled but no HTTPS endpoint is known for snapshot download of '%s'; falling back to plain HTTP on %s. "
                  + "Declare an httpsPort (the optional 5th field 'host:raftPort:httpPort:priority:httpsPort') in '%s' to "
                  + "transfer snapshots encrypted.",
              null, databaseName, endpoint, GlobalConfiguration.HA_SERVER_LIST.getKey());
      }
      if (endpoint == null) {
        lastException = new IOException("Leader HTTP address not yet known (Raft election in progress)");
        LogManager.instance().log(SnapshotInstaller.class, Level.WARNING,
            "Snapshot download attempt %d/%d failed for '%s': %s",
            null, attempt + 1, maxRetries + 1, databaseName, lastException.getMessage());
        continue;
      }

      final String snapshotUrl = (https ? "https://" : "http://") + endpoint + "/api/v1/ha/snapshot/" + databaseName;
      try {
        downloadSnapshot(snapshotNewDir, snapshotUrl, clusterToken, https, server);
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
      final String clusterToken, final boolean https, final ArcadeDBServer server) throws IOException {

    HALog.log(SnapshotInstaller.class, HALog.BASIC, "Downloading snapshot from %s", snapshotUrl);

    final HttpURLConnection connection;
    try {
      connection = (HttpURLConnection) new URI(snapshotUrl).toURL().openConnection();
    } catch (final URISyntaxException e) {
      throw new IOException("Invalid snapshot URL: " + snapshotUrl, e);
    }

    if (connection instanceof HttpsURLConnection) {
      if (!https)
        throw new ReplicationException("Snapshot URL is HTTPS but plain HTTP was expected: " + snapshotUrl);
      final SSLContext sslContext = buildSSLContext(server);
      ((HttpsURLConnection) connection).setSSLSocketFactory(sslContext.getSocketFactory());
    }

    connection.setRequestMethod("GET");
    connection.setConnectTimeout(30_000);
    connection.setReadTimeout(
        server != null ? server.getConfiguration().getValueAsInteger(GlobalConfiguration.HA_SNAPSHOT_DOWNLOAD_TIMEOUT)
            : 300_000);

    if (clusterToken != null && !clusterToken.isEmpty())
      connection.setRequestProperty("X-ArcadeDB-Cluster-Token", clusterToken);

    try {
      final int responseCode = connection.getResponseCode();
      if (responseCode != 200)
        throw new IOException("Failed to download snapshot: HTTP " + responseCode);

      final CountingInputStream rawCounter = new CountingInputStream(connection.getInputStream());
      try (final ZipInputStream zipIn = new ZipInputStream(rawCounter)) {
        ZipEntry zipEntry;
        while ((zipEntry = zipIn.getNextEntry()) != null) {
          final Path targetFile = targetDir.resolve(zipEntry.getName()).normalize();

          // Zip-slip protection: normalized path must remain inside targetDir
          if (!targetFile.startsWith(targetDir))
            throw new ReplicationException("Zip slip detected in snapshot: " + zipEntry.getName());

          // Reject suspicious path components before touching the filesystem
          if (zipEntry.getName().contains(".."))
            throw new ReplicationException("Suspicious path in snapshot ZIP: " + zipEntry.getName());

          // Create parent directories and perform real-path symlink-escape check
          Files.createDirectories(targetFile.getParent());
          final Path realParent = targetFile.getParent().toRealPath();
          if (!realParent.startsWith(targetDir.toRealPath()))
            throw new ReplicationException(
                "Symlink escape detected in snapshot: entry '" + zipEntry.getName() + "' resolves outside target directory");

          // Reject symlinks at the target file path
          if (Files.isSymbolicLink(targetFile))
            throw new ReplicationException("Symlink detected at extraction target: " + targetFile);

          final long compressedStart = rawCounter.getCount();
          try (final FileOutputStream fos = new FileOutputStream(targetFile.toFile())) {
            final long uncompressedBytes = copyWithLimit(zipIn, fos, MAX_ZIP_ENTRY_UNCOMPRESSED_BYTES, zipEntry.getName());

            // Decompression-bomb defense: check ratio for entries large enough to matter.
            // Uses raw counter delta (compressed bytes including headers) which slightly
            // over-estimates compressed size, under-estimating ratio - safe direction.
            final long compressedBytes = Math.max(1L, rawCounter.getCount() - compressedStart);
            if (uncompressedBytes > MIN_RATIO_CHECK_BYTES
                && uncompressedBytes / compressedBytes > MAX_COMPRESSION_RATIO)
              throw new ReplicationException("Suspicious compression ratio for snapshot entry '"
                  + zipEntry.getName() + "': inflated " + uncompressedBytes + " bytes from "
                  + compressedBytes + " (ratio > " + MAX_COMPRESSION_RATIO + ":1)");
          }
          zipIn.closeEntry();
        }
      }
    } finally {
      connection.disconnect();
    }
  }

  /**
   * Builds the client-side {@link SSLContext} used for encrypted peer-to-peer transfers (snapshot
   * download, cross-node verify). Package-private so other HA peer-to-peer callers validate the peer
   * certificate the same way (against the trust store, not the key store).
   */
  static SSLContext buildSSLContext(final ArcadeDBServer server) throws IOException {
    try {
      if (server == null)
        return SSLContext.getDefault();

      // The client validates the leader's server certificate against its TRUST store only: the key
      // store holds this node's own private key/cert (its identity) and is the wrong source of trust
      // anchors (issue #4470). This mirrors HttpServer.createSSLContext(), which keeps the two stores
      // strictly separate and mandates a trust store whenever SSL is enabled - so a running HTTPS
      // cluster always has one configured here. When no trust store is set, fall back to the JVM
      // default trust store rather than the key store.
      final String storePath = server.getConfiguration().getValueAsString(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE);
      final String storePassword = server.getConfiguration().getValueAsString(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE_PASSWORD);
      if (storePath == null || storePath.isBlank())
        return SSLContext.getDefault();

      final char[] password = storePassword != null ? storePassword.toCharArray() : new char[0];

      final KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
      try (final InputStream is = Files.newInputStream(Path.of(storePath))) {
        ks.load(is, password);
      }
      final TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      tmf.init(ks);
      final SSLContext ctx = SSLContext.getInstance("TLS");
      ctx.init(null, tmf.getTrustManagers(), null);
      return ctx;
    } catch (final IOException e) {
      throw e;
    } catch (final Exception e) {
      throw new IOException("Failed to build SSL context for snapshot download", e);
    }
  }

  static long copyWithLimit(final InputStream in, final OutputStream out,
      final long maxBytes, final String entryName) throws IOException {
    final byte[] buffer = new byte[8192];
    long totalRead = 0;
    int bytesRead;
    while ((bytesRead = in.read(buffer)) != -1) {
      totalRead += bytesRead;
      if (totalRead > maxBytes)
        throw new ReplicationException(
            "Snapshot entry '" + entryName + "' exceeds size limit of " + maxBytes + " bytes (zip-bomb protection)");
      out.write(buffer, 0, bytesRead);
    }
    return totalRead;
  }

  /**
   * FilterInputStream that counts bytes consumed by the downstream reader. Used to measure the
   * compressed bytes a {@link ZipInputStream} reads per entry so we can enforce a per-entry
   * compression-ratio cap. The count intentionally includes the ZIP's local file header and
   * optional data descriptor for each entry; this over-estimates the pure compressed payload
   * and therefore under-estimates the ratio, which is the safe direction for the check.
   * Package-private for unit testing.
   */
  static final class CountingInputStream extends FilterInputStream {
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

    @Override
    public boolean markSupported() {
      return false;
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
          if (SNAPSHOT_COMPLETE_FILE.equals(name))
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
