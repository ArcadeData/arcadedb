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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.utility.FileUtils;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.KeyStore;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;
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
 * <p>
 * <b>Durability ordering (issue #4830).</b> Crash recovery is only sound if the on-disk state it reads
 * back is actually durable, so each boundary is fsynced before the next step depends on it: extracted
 * files are fsynced individually as they are written, the {@code .snapshot-pending}/{@code .snapshot-complete}
 * markers are fsynced together with their parent directory, and the post-swap directory entries are fsynced
 * before the retained backup is deleted. This guarantees the backup is never removed while the newly
 * installed files are still only in the OS page cache, so a power loss can always fall back to a complete
 * copy (either the old one via the backup, or the new one once the swap is durable).
 */
public final class SnapshotInstaller {

  static final String SNAPSHOT_NEW_DIR       = ".snapshot-new";
  static final String SNAPSHOT_BACKUP_DIR    = ".snapshot-backup";
  static final String SNAPSHOT_PENDING_FILE  = ".snapshot-pending";
  static final String SNAPSHOT_COMPLETE_FILE = ".snapshot-complete";

  // Reserved staging directory prefix for acquiring a database the node has NEVER seen (issue #4727).
  // A new-database acquire downloads into databases/.acquire-<name>/ and publishes with a single atomic
  // rename to databases/<name>/. The '.' prefix makes it a reserved name (ArcadeDBServer.isReservedDatabaseName),
  // so the startup scan (ArcadeDBServer.loadDatabases) never tries to open a half-written acquisition.
  static final String ACQUIRE_STAGING_PREFIX = ".acquire-";

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

  /**
   * Absolute database directory paths with an install currently in flight. The lifecycle assumes
   * installs for a given database never overlap (see {@link #closeLocalDatabaseIfOpen}); this set turns
   * a violation of that assumption from a silent double-close into a logged WARNING so it is
   * diagnosable after the fact. Keyed by resolved path (not database name) so two logical servers in
   * the same JVM - which use distinct database directories - never raise a spurious overlap warning.
   */
  private static final Set<String> INSTALLS_IN_FLIGHT = ConcurrentHashMap.newKeySet();

  /**
   * Test-only barrier invoked once inside the registry-locked swap region of {@link #swapAndReopen}, after the
   * live database has been closed/deregistered and before the staged snapshot is moved into place. {@code null}
   * in production (the only cost is a single reference read per install). The issue-#4832 regression test sets it
   * to pause inside the critical section and prove a concurrent {@link ArcadeDBServer#getDatabase} blocks on the
   * registry lock instead of re-opening the database mid-swap.
   */
  static volatile Runnable swapBarrierForTesting = null;

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

    // The lifecycle assumes installs for a given database never overlap (see closeLocalDatabaseIfOpen).
    // If they ever do, log it loudly rather than silently double-closing: this set makes the violation
    // diagnosable. Keyed by resolved path so distinct logical servers do not collide on database name.
    // Tracked across both phases and cleared in the outer finally so a download failure does not leak
    // the entry.
    final String inFlightKey = dbPath.toString();
    if (!INSTALLS_IN_FLIGHT.add(inFlightKey))
      LogManager.instance().log(SnapshotInstaller.class, Level.WARNING,
          """
          Concurrent snapshot install detected for '%s'; the install lifecycle assumes these never overlap \
          for the same database - this may indicate a coordination bug in the HA layer""", null, databaseName);

    try {
      // Clean up any leftover state from a previous failed attempt
      deleteDirectoryIfExists(snapshotNew);
      deleteDirectoryIfExists(snapshotBackup);
      Files.deleteIfExists(pendingMarker);

      Files.createDirectories(snapshotNew);

      // Write the pending marker BEFORE starting extraction, and fsync it together with the parent
      // directory so a crash right after this point still leaves the marker on disk for startup
      // recovery to find (issue #4830).
      writeMarkerDurable(pendingMarker);

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

      // Mark download as complete. The extracted files were each fsynced as they were written
      // (see extractAndVerifySnapshot), so fsyncing this marker and the staging directory now
      // establishes the durability barrier: if this marker is on disk after a crash, every snapshot
      // file it vouches for is on disk too, and the swap can be safely completed by startup recovery.
      writeMarkerDurable(snapshotNew.resolve(SNAPSHOT_COMPLETE_FILE));

      // PHASE 2 - SWAP. Set the server-wide flag BEFORE closing the database so HTTP handlers return 503
      // while the files are being moved.
      server.setSnapshotInstallInProgress(true);
      try {
        swapAndReopen(databaseName, dbPath, snapshotNew, snapshotBackup, pendingMarker, server);
      } finally {
        server.setSnapshotInstallInProgress(false);
      }
    } finally {
      INSTALLS_IN_FLIGHT.remove(inFlightKey);
    }
  }

  /**
   * Closes the live database, atomically swaps the staged snapshot into place, and reopens it - the whole
   * sequence held under the server's database-registry lock ({@link ArcadeDBServer#getDatabasesLock()}).
   * <p>
   * The {@link #atomicSwap} itself moves files entry-by-entry, so for a brief window the on-disk directory holds
   * a mix of old-removed and new-installed files. The {@link ArcadeDBServer#setSnapshotInstallInProgress} flag
   * deflects HTTP clients with a clean 503 during that window, but the engine-internal open paths
   * (reconcile / apply / health-monitor threads calling {@link ArcadeDBServer#getDatabase}) do not consult that
   * flag, and the close here deregisters the database, so a concurrent open could otherwise re-register it from
   * the half-swapped directory. Holding the registry lock across close -&gt; swap -&gt; reopen makes the swap
   * window invisible to <i>every</i> open path: a concurrent open blocks on the lock and, once it proceeds, sees
   * the fully installed snapshot (or, on failure, the restored previous copy) - never an intermediate mix
   * (issue #4832).
   * <p>
   * The database is closed via {@link #closeLocalDatabaseIfOpen} (which closes the embedded instance directly,
   * skipping the HA wrapper's replicated-close semantics: this is a local file swap, not a cluster-wide close).
   * On a swap or reopen failure the previous copy is restored and reopened so the node never stays closed; the
   * caller's {@code .snapshot-pending} marker remains the single startup-recovery hook.
   */
  static void swapAndReopen(final String databaseName, final Path dbPath, final Path snapshotNew,
      final Path snapshotBackup, final Path pendingMarker, final ArcadeDBServer server) throws IOException {
    synchronized (server.getDatabasesLock()) {
      // Close + deregister the live database now that a complete snapshot is staged on disk. The DB
      // must be closed before the file move so no open handles point at the directory being swapped.
      closeLocalDatabaseIfOpen(server, databaseName);

      // Test seam: pause inside the critical section so a regression test can prove a concurrent open blocks
      // on the registry lock rather than re-opening the half-swapped directory. No-op in production.
      final Runnable barrier = swapBarrierForTesting;
      if (barrier != null)
        barrier.run();

      // Swap: live -> backup, new -> live. atomicSwap restores the original live files on a failure in
      // either phase (see its contract), so on an IOException here dbPath holds the previous copy.
      try {
        atomicSwap(dbPath, snapshotNew, snapshotBackup);
      } catch (final IOException swapEx) {
        // Reopen the restored previous database so the node keeps serving. Leave the pending marker in
        // place: if atomicSwap's own restore was interrupted, recoverPendingSnapshotSwaps reconciles
        // dbPath (and removes the leftover .snapshot-new) on the next startup.
        reopenQuietly(server, databaseName);
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
        // The pending marker is intentionally NOT cleared here: it is dropped only on the success path
        // below. If rollbackToBackup succeeds, the next startup's recoverSingleDatabase sees both
        // .snapshot-new and .snapshot-backup gone (!hasCompleteMarker && !hasBackup), logs "orphaned
        // snapshot directory", and clears the marker - so leaving it is harmless and keeps recovery
        // logic in one place. If the rollback was instead interrupted, that same startup pass restores
        // the backup. Either way the marker is the single recovery hook.
        LogManager.instance().log(SnapshotInstaller.class, Level.SEVERE,
            "Installed snapshot for '%s' failed to open; rolling back to the previous local copy", openEx, databaseName);
        rollbackToBackup(dbPath, snapshotBackup);
        reopenQuietly(server, databaseName);
        throw new IOException("Snapshot for '" + databaseName
            + "' downloaded but failed to open; rolled back to the previous local copy", openEx);
      }

      // Success: drop the retained backup and clear the pending marker.
      deleteDirectoryIfExists(snapshotBackup);
      Files.deleteIfExists(pendingMarker);

      HALog.log(SnapshotInstaller.class, HALog.BASIC, "Snapshot for '%s' installed successfully", databaseName);
    }
  }

  /**
   * Acquires a database the local node has <b>never seen</b> on disk, crash-safely (issue #4727).
   * <p>
   * Unlike {@link #install} - which refreshes an <i>existing</i> database in place and can roll back to a
   * retained {@code .snapshot-backup} - a brand-new acquire has no previous copy. The hazard is the startup
   * scan: {@code ArcadeDBServer.loadDatabases} opens every non-reserved {@code databases/<name>/} directory
   * before HA crash recovery runs, so a half-written download left under the final name would be opened as a
   * corrupt database. To avoid that, the download is staged under a <b>reserved</b> directory
   * ({@code databases/.acquire-<name>/}, skipped by the boot scan) and published with a single
   * {@link StandardCopyOption#ATOMIC_MOVE atomic rename}. A crash before the rename leaves only the reserved
   * staging dir (cleaned on the next startup by {@link #recoverPendingSnapshotSwaps}); a crash after it leaves
   * a complete, openable database. No partial non-reserved directory can ever exist.
   * <p>
   * If the database materialises locally while we download (e.g. an {@code INSTALL_DATABASE_ENTRY} is replayed
   * concurrently), this falls back to the in-place {@link #install} refresh so the now-registered database still
   * receives the leader's snapshot.
   * <p>
   * The leader addresses are taken as {@link Supplier}s for symmetry with {@link #install}, whose retry loop
   * re-resolves them per attempt. The reconcile caller passes constant suppliers on purpose: an InstallSnapshot is
   * tied to one specific leader, and if leadership changes mid-download Ratis re-triggers the whole install from
   * the new leader, so re-resolving within a single call would not help.
   */
  public static void acquireNewDatabase(final String databaseName,
      final Supplier<String> leaderHttpAddrSupplier, final Supplier<String> leaderHttpsAddrSupplier,
      final String clusterToken, final ArcadeDBServer server) throws IOException {

    final Path databasesDir = Path.of(
        server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY))
        .normalize().toAbsolutePath();
    final Path dbPath = databasesDir.resolve(databaseName);
    final Path staging = databasesDir.resolve(ACQUIRE_STAGING_PREFIX + databaseName);

    // Raced: the database already exists locally (created/registered concurrently). Refresh in place instead
    // of acquiring, so two creators never race on dbPath. install() manages its own in-flight guard.
    if (server.existsDatabase(databaseName)) {
      install(databaseName, dbPath.toString(), leaderHttpAddrSupplier, leaderHttpsAddrSupplier, clusterToken, server);
      return;
    }

    // Same overlap guard install() uses, but fail-fast: the lifecycle assumes acquisitions for a given database
    // never overlap (Ratis serializes InstallSnapshot per follower). Keyed by the resolved final path so distinct
    // logical servers in one JVM (tests) do not collide. If the invariant is ever violated, abort rather than let
    // two acquisitions share one staging dir and race on the atomic rename; the caller treats this like any other
    // failed install and Ratis re-triggers once the other acquisition has finished and released the key.
    final String inFlightKey = dbPath.toString();
    if (!INSTALLS_IN_FLIGHT.add(inFlightKey))
      throw new IOException("Concurrent acquisition already in progress for '" + databaseName
          + "'; acquisitions for the same database must not overlap (possible HA coordination bug)");

    try {
      // Clean any leftover staging from a previous failed attempt, then create a fresh reserved staging dir.
      deleteDirectoryIfExists(staging);
      Files.createDirectories(staging);

      // PHASE 1 - DOWNLOAD into the reserved staging dir. A crash here cannot leave a half-written
      // databases/<name>/ that the boot scan opens.
      final int maxRetries = server.getConfiguration().getValueAsInteger(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRIES);
      final long retryBaseMs = server.getConfiguration().getValueAsLong(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRY_BASE_MS);
      try {
        downloadWithRetry(databaseName, staging, leaderHttpAddrSupplier, leaderHttpsAddrSupplier, clusterToken,
            maxRetries, retryBaseMs, server);
      } catch (final IOException e) {
        deleteDirectoryIfExists(staging);
        throw e;
      }

      // Re-check right before publishing: the database may have been created concurrently while we downloaded.
      if (server.existsDatabase(databaseName)) {
        deleteDirectoryIfExists(staging);
        // Release our guard before delegating so install()'s own guard does not see a spurious overlap.
        INSTALLS_IN_FLIGHT.remove(inFlightKey);
        install(databaseName, dbPath.toString(), leaderHttpAddrSupplier, leaderHttpsAddrSupplier, clusterToken, server);
        return;
      }

      // PHASE 2 - VALIDATE the downloaded snapshot while it is STILL under the reserved staging name, BEFORE
      // publishing it. A corrupt/incompatible snapshot must never reach databases/<name>/, where the startup
      // scan would try to open it and crash the server: there is no previous copy to roll back to for a
      // never-seen database. Validating in staging means a bad download is simply discarded (the reserved dir
      // is ignored by the boot scan), leaving the database absent - the safe state - to be re-acquired next
      // reconcile. This is stronger than validating after the rename, which on Windows could leave an
      // undeletable corrupt directory under the final name if the failed open leaked a file handle.
      try {
        validateSnapshotOpens(staging, server);
      } catch (final IOException validationEx) {
        LogManager.instance().log(SnapshotInstaller.class, Level.SEVERE,
            """
            Acquired snapshot for new database '%s' failed validation; discarding it and leaving the database \
            absent (it will be re-acquired on the next reconcile): %s""", null, databaseName, validationEx.getMessage());
        deleteDirectoryIfExists(staging);
        throw validationEx;
      }

      // A stale, unregistered databases/<name>/ (e.g. from a pre-upgrade crash) would block the rename. The
      // leader's copy is authoritative for an unseen database, so clear it before publishing.
      // Safety rests on Ratis applying the log single-threaded relative to this snapshot-install path: a
      // concurrent INSTALL_DATABASE_ENTRY replay that creates databases/<name>/ cannot interleave between the
      // re-check above and this delete+rename, so we never blow away a database being created by another path.
      if (Files.exists(dbPath))
        deleteDirectoryIfExists(dbPath);

      // PHASE 3 - PUBLISH the validated snapshot with a single atomic rename, then register it. The open here
      // re-opens files we just validated, so it is not expected to fail; if it somehow does, drop the directory
      // (best effort) and leave the database absent rather than registered-but-broken.
      publishStaging(staging, dbPath);
      try {
        server.getDatabase(databaseName);
      } catch (final RuntimeException openEx) {
        LogManager.instance().log(SnapshotInstaller.class, Level.SEVERE,
            """
            Acquired snapshot for new database '%s' was validated but failed to open after publish; removing it \
            and leaving the database absent""", openEx, databaseName);
        server.removeDatabase(databaseName);
        deleteDirectoryIfExists(dbPath);
        throw new IOException("Acquired snapshot for new database '" + databaseName + "' failed to open after publish", openEx);
      }

      HALog.log(SnapshotInstaller.class, HALog.BASIC, "New database '%s' acquired from leader", databaseName);
    } finally {
      // Idempotent: a no-op if we already released the key before delegating to install() above.
      INSTALLS_IN_FLIGHT.remove(inFlightKey);
      // Defensive: on success the staging dir was renamed away; on failure it was deleted. This is a no-op
      // in both cases but guarantees no reserved staging dir is ever leaked.
      deleteDirectoryIfExists(staging);
    }
  }

  /**
   * Publishes a fully-downloaded acquisition staging directory to its final database directory with a single
   * atomic rename. Same-filesystem directory rename, so the swap is all-or-nothing. Falls back to a plain move
   * only on the rare filesystem that does not support {@link StandardCopyOption#ATOMIC_MOVE}.
   */
  private static void publishStaging(final Path staging, final Path dbPath) throws IOException {
    try {
      Files.move(staging, dbPath, StandardCopyOption.ATOMIC_MOVE);
    } catch (final AtomicMoveNotSupportedException e) {
      Files.move(staging, dbPath);
    }
    // Persist the new directory entry in the parent so a crash right after publish cannot lose the
    // freshly-acquired database (issue #4830). The staged files were already fsynced at extraction.
    fsyncDirectory(dbPath.getParent());
  }

  /**
   * Smoke-tests that a freshly-downloaded snapshot directory is a loadable ArcadeDB database, by opening it
   * read-only and closing it again. Read-only avoids any writes to the staging files. Throws {@link IOException}
   * (not the raw open exception) so callers can treat a corrupt snapshot uniformly with a download failure.
   */
  private static void validateSnapshotOpens(final Path stagingPath, final ArcadeDBServer server) throws IOException {
    // Safe to open under the reserved staging name and then publish: ArcadeDB derives the database name from the
    // directory basename at open time (LocalDatabase computes it from the path; it is not persisted into
    // configuration.json / schema.json), so opening as ".acquire-<name>" and later as "<name>" yields the same DB.
    // A READ_ONLY open performs no writes, so it leaves behind no lock/WAL artifact that the atomic rename would
    // carry into the final directory.
    // The end-to-end acquire IT (SnapshotAcquireNewDatabaseIT) confirms the published database opens cleanly.
    // Use the server's security + auto-transaction so this validation open matches the settings of the eventual
    // live open in ArcadeDBServer.getDatabase, rather than opening under defaults that could diverge.
    try (final DatabaseFactory factory = new DatabaseFactory(stagingPath.toString())
        .setAutoTransaction(true)
        .setSecurity(server.getSecurity());
        final Database db = factory.open(ComponentFile.MODE.READ_ONLY)) {
      // Opening + closing is the validation: it confirms the configuration, schema and component files load.
      if (db == null)
        throw new IOException("snapshot did not open");
    } catch (final RuntimeException e) {
      throw new IOException("downloaded snapshot failed to open: " + e.getMessage(), e);
    }
  }

  /**
   * Resolves the on-disk path of a database, so callers no longer need to keep it open just to read its
   * path before an install. Two cases:
   * <ul>
   *   <li>database registered (or deregistered-but-on-disk, which {@code existsDatabase} reports as
   *       present): returns its live {@code getDatabasePath()}. Note the side effect - {@code getDatabase}
   *       <i>opens and registers</i> a deregistered-but-on-disk database, so do not call this as a pure
   *       read on a database meant to stay closed;</li>
   *   <li>database absent: derives the path from {@link GlobalConfiguration#SERVER_DATABASE_DIRECTORY}
   *       without opening anything (nothing to open).</li>
   * </ul>
   * Package-private and intended only for the install call sites, which invoke it on an open database
   * just before closing it. The conditional open is safe for the install paths only because two installs
   * for the same database are never in flight at once (see {@link #closeLocalDatabaseIfOpen}); a
   * re-register racing a deliberate deregistration elsewhere would be a misuse.
   */
  static String resolveDatabasePath(final ArcadeDBServer server, final String databaseName) {
    // Best-effort: the exists/get pair is not atomic, but it only resolves a path before the download
    // phase (no data at risk) and getDatabase returns a valid path even if it has to reopen.
    if (server.existsDatabase(databaseName))
      return ((DatabaseInternal) server.getDatabase(databaseName)).getDatabasePath();
    return server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY)
        + File.separator + databaseName;
  }

  /**
   * Closes and deregisters the local database if it is currently registered. No-op when the database
   * is absent (late joiner with no local copy).
   * <p>
   * Closes the embedded instance directly ({@code getEmbedded().close()}) rather than the wrapper
   * ({@code db.close()}): the install does a local file swap, so it must close the underlying
   * {@code LocalDatabase} without the replicated-close semantics the HA wrapper would apply. This
   * unifies the previously divergent call sites (the Ratis install path used {@code db.close()}; the
   * resync/bootstrap paths used {@code getEmbedded().close()}) on the form correct for a file swap.
   * <p>
   * The {@code existsDatabase}/{@code getDatabase} pair is not atomic. This relies on the assumption
   * that two snapshot installs for the same database are never in flight at once: the install drivers
   * (Raft apply/bootstrap recovery, the operator-triggered resync, the health-monitor watchdog) are
   * not expected to overlap on a single database. If that assumption is ever broken, two concurrent
   * installs could both pass the {@code existsDatabase} check and the second {@code close} would see an
   * already-closed instance.
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
      // dbPath may be partially cleared here. The caller leaves the .snapshot-pending marker in place,
      // so recoverPendingSnapshotSwaps reconciles dbPath from the retained backup on the next startup -
      // call this out so operators know that marker is the recovery hook to look for.
      LogManager.instance().log(SnapshotInstaller.class, Level.SEVERE,
          """
          Failed to roll back snapshot install for %s: %s. Leaving the .snapshot-pending marker so startup \
          recovery (recoverPendingSnapshotSwaps) restores the backup on the next restart""", e, dbPath, e.getMessage());
    }
  }

  /**
   * Deletes every non-{@code .snapshot*} entry in the live database directory. Used before restoring
   * a backup so files that exist only in a failed snapshot are not left behind. Not transactional: a
   * crash partway leaves dbPath partially cleared, but the caller keeps the {@code .snapshot-pending}
   * marker on failure so {@link #recoverPendingSnapshotSwaps} restores the backup on the next startup.
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
        final String dirName = dbDir.getFileName().toString();

        // Clean up an interrupted new-database acquisition staging dir (databases/.acquire-<name>, issue #4727).
        // A completed acquire atomically renames the staging dir to its final database name, so any surviving
        // .acquire-* dir is an interrupted download and is safe to delete; the node re-acquires it on the next
        // reconcile. These are reserved ('.'-prefixed) so the boot scan never opened them.
        if (dirName.startsWith(ACQUIRE_STAGING_PREFIX)) {
          try {
            LogManager.instance().log(SnapshotInstaller.class, Level.INFO,
                "Cleaning up interrupted new-database acquisition staging dir: %s", null, dbDir);
            deleteDirectoryIfExists(dbDir);
          } catch (final IOException e) {
            LogManager.instance().log(SnapshotInstaller.class, Level.WARNING,
                "Could not delete acquisition staging dir %s: %s", e, dbDir, e.getMessage());
          }
          continue;
        }

        // Skip internal snapshot directories themselves
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
              """
              SSL is enabled but no HTTPS endpoint is known for snapshot download of '%s'; falling back to plain HTTP on %s. \
              Declare an httpsPort (the optional 5th field 'host:raftPort:httpPort:priority:httpsPort') in '%s' to \
              transfer snapshots encrypted.""",
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

    if (connection instanceof HttpsURLConnection lConnection) {
      if (!https)
        throw new ReplicationException("Snapshot URL is HTTPS but plain HTTP was expected: " + snapshotUrl);
      final SSLContext sslContext = buildSSLContext(server);
      lConnection.setSSLSocketFactory(sslContext.getSocketFactory());
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

      // A leader on issue #4831 or later advertises a completeness manifest via this header; when present
      // the manifest becomes mandatory, so a truncated download (manifest dropped) fails loudly. A leader
      // predating #4831 omits the header, and the follower keeps the legacy "ZipInputStream reached EOF"
      // acceptance for backward compatibility during a rolling upgrade.
      final boolean manifestRequired = "1".equals(connection.getHeaderField(SnapshotManager.MANIFEST_HEADER));

      final CountingInputStream rawCounter = new CountingInputStream(connection.getInputStream());
      InputStream source = rawCounter;
      final boolean progressLogging = server == null
          || server.getConfiguration().getValueAsBoolean(GlobalConfiguration.HA_RESYNC_PROGRESS_LOGGING);
      if (progressLogging) {
        final String dbName = targetDir.getFileName() != null ? targetDir.getFileName().toString() : "snapshot";
        final long intervalMs = server != null
            ? server.getConfiguration().getValueAsLong(GlobalConfiguration.HA_RESYNC_PROGRESS_INTERVAL)
            : 5000L;
        source = new ProgressReportingInputStream(rawCounter, new SnapshotDownloadProgressMeter(dbName, intervalMs));
      }
      extractAndVerifySnapshot(source, rawCounter, targetDir, manifestRequired);
    } finally {
      connection.disconnect();
    }
  }

  /**
   * Maximum bytes the manifest entry is allowed to occupy uncompressed (8 MB). The manifest is a small JSON
   * document (one record per database file), so this is a generous ceiling that still caps a hostile or
   * corrupt stream claiming a huge manifest. Package-private for unit testing.
   */
  static final long MAX_MANIFEST_BYTES = 8L * 1024 * 1024;

  /**
   * Extracts every entry of the snapshot ZIP read from {@code source} into {@code targetDir} and, when a
   * {@link SnapshotManager#MANIFEST_ENTRY_NAME manifest} is present (or {@code manifestRequired}), verifies
   * the transfer is complete: every file the manifest lists must have been extracted with a matching size
   * and CRC32 (issue #4831).
   * <p>
   * The manifest entry itself is read into memory and never written to disk. Because the leader writes it
   * last, a download truncated at any ZIP-entry boundary loses it; with {@code manifestRequired} the install
   * then fails (and the caller retries) instead of opening a structurally-incomplete database.
   * <p>
   * Package-private and decoupled from the HTTP connection so the verification can be unit-tested by feeding
   * a {@link ByteArrayInputStream} of a hand-built (and deliberately truncated) ZIP.
   *
   * @param source           the snapshot byte stream (possibly wrapped for progress reporting)
   * @param rawCounter       the underlying byte counter, used for the per-entry compression-ratio check
   * @param targetDir        the staging directory the entries are extracted into
   * @param manifestRequired when true, a missing manifest is treated as a truncated download and rejected
   */
  static void extractAndVerifySnapshot(final InputStream source, final CountingInputStream rawCounter,
      final Path targetDir, final boolean manifestRequired) throws IOException {
    // Records the size+CRC32 of each file actually extracted, used to verify against the manifest.
    final Map<String, long[]> extracted = new HashMap<>();
    byte[] manifestBytes = null;

    try (final ZipInputStream zipIn = new ZipInputStream(source)) {
      ZipEntry zipEntry;
      while ((zipEntry = zipIn.getNextEntry()) != null) {
        final String entryName = zipEntry.getName();

        // The manifest is metadata, not a database file: read it into memory (capped) and never write it
        // to the staging directory, so it is not carried into the live database by the swap.
        if (SnapshotManager.MANIFEST_ENTRY_NAME.equals(entryName)) {
          final ByteArrayOutputStream buf = new ByteArrayOutputStream();
          copyWithLimit(zipIn, buf, MAX_MANIFEST_BYTES, entryName);
          manifestBytes = buf.toByteArray();
          zipIn.closeEntry();
          continue;
        }

        final Path targetFile = targetDir.resolve(entryName).normalize();

        // Zip-slip protection: normalized path must remain inside targetDir
        if (!targetFile.startsWith(targetDir))
          throw new ReplicationException("Zip slip detected in snapshot: " + entryName);

        // Reject suspicious path components before touching the filesystem
        if (entryName.contains(".."))
          throw new ReplicationException("Suspicious path in snapshot ZIP: " + entryName);

        // Create parent directories and perform real-path symlink-escape check
        Files.createDirectories(targetFile.getParent());
        final Path realParent = targetFile.getParent().toRealPath();
        if (!realParent.startsWith(targetDir.toRealPath()))
          throw new ReplicationException(
              "Symlink escape detected in snapshot: entry '" + entryName + "' resolves outside target directory");

        // Reject symlinks at the target file path
        if (Files.isSymbolicLink(targetFile))
          throw new ReplicationException("Symlink detected at extraction target: " + targetFile);

        final long compressedStart = rawCounter.getCount();
        final CRC32 crc = new CRC32();
        try (final FileOutputStream fos = new FileOutputStream(targetFile.toFile());
            final CheckedOutputStream cos = new CheckedOutputStream(fos, crc)) {
          final long uncompressedBytes = copyWithLimit(zipIn, cos, MAX_ZIP_ENTRY_UNCOMPRESSED_BYTES, entryName);

          // Decompression-bomb defense: check ratio for entries large enough to matter.
          // Uses raw counter delta (compressed bytes including headers) which slightly
          // over-estimates compressed size, under-estimating ratio - safe direction.
          final long compressedBytes = Math.max(1L, rawCounter.getCount() - compressedStart);
          if (uncompressedBytes > MIN_RATIO_CHECK_BYTES
              && uncompressedBytes / compressedBytes > MAX_COMPRESSION_RATIO)
            throw new ReplicationException("Suspicious compression ratio for snapshot entry '"
                + entryName + "': inflated " + uncompressedBytes + " bytes from "
                + compressedBytes + " (ratio > " + MAX_COMPRESSION_RATIO + ":1)");

          // Force this file's bytes to stable storage before it is later renamed into the live
          // database. Without this the extracted data lingers in the OS page cache and a power loss
          // after the swap (when the backup is already gone) would expose torn/partial files with no
          // way to roll back (issue #4830).
          cos.flush();
          fos.getFD().sync();

          extracted.put(entryName, new long[] { uncompressedBytes, crc.getValue() });
        }
        zipIn.closeEntry();
      }
    }

    verifyManifest(manifestBytes, extracted, manifestRequired);
  }

  /**
   * Validates the extracted snapshot against its manifest (issue #4831).
   * <ul>
   *   <li>manifest absent + not required: legacy leader, nothing to verify;</li>
   *   <li>manifest absent + required: the leader advertised a manifest but it never arrived - the download
   *       was truncated before the final entry, so reject;</li>
   *   <li>manifest present: every listed file must have been extracted with a matching size and CRC32.</li>
   * </ul>
   */
  private static void verifyManifest(final byte[] manifestBytes, final Map<String, long[]> extracted,
      final boolean manifestRequired) throws IOException {
    if (manifestBytes == null) {
      if (manifestRequired)
        throw new IOException("Snapshot transfer incomplete: the leader advertised a completeness manifest but it was "
            + "not received - the download was truncated before completion (" + extracted.size() + " file(s) extracted)");
      return;
    }

    final List<SnapshotManager.ManifestEntry> manifest = SnapshotManager.parseManifest(
        new String(manifestBytes, StandardCharsets.UTF_8));
    for (final SnapshotManager.ManifestEntry entry : manifest) {
      final long[] got = extracted.get(entry.name());
      if (got == null)
        throw new IOException("Snapshot transfer incomplete: file '" + entry.name()
            + "' is listed in the manifest but was not received (truncated download)");
      if (got[0] != entry.size())
        throw new IOException("Snapshot file '" + entry.name() + "' size mismatch: manifest declares "
            + entry.size() + " bytes but " + got[0] + " were received (truncated or corrupt download)");
      if (got[1] != entry.crc())
        throw new IOException("Snapshot file '" + entry.name() + "' CRC32 mismatch: manifest declares "
            + entry.crc() + " but received content hashes to " + got[1] + " (corrupt download)");
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
   * Wraps the snapshot download stream to feed the cumulative byte count to a progress meter and log
   * any due progress line. Reports compressed bytes read off the wire (the meter measures download, not
   * decompression). No-op when the meter is null (resync logging disabled).
   */
  private static final class ProgressReportingInputStream extends FilterInputStream {
    private final SnapshotDownloadProgressMeter meter;
    private       long                          total;

    ProgressReportingInputStream(final InputStream in, final SnapshotDownloadProgressMeter meter) {
      super(in);
      this.meter = meter;
    }

    private void reportProgress() {
      final String line = meter.lineIfDue(total, System.currentTimeMillis());
      if (line != null)
        // Log at INFO (not HALog.BASIC, which HA_LOG_VERBOSE gates off by default) so snapshot
        // download progress is visible alongside the rest of the resync narrative.
        LogManager.instance().log(SnapshotInstaller.class, Level.INFO, line);
    }

    @Override
    public int read() throws IOException {
      final int b = super.read();
      if (b != -1)
        total++; // single-byte path: only count; the bulk path samples the clock and reports progress
      return b;
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
      final int n = super.read(b, off, len);
      if (n > 0) {
        total += n;
        reportProgress();
      }
      return n;
    }
  }

  /**
   * Swaps a new snapshot directory into the live database path:
   * <ol>
   *   <li>move live contents from {@code dbDir} to {@code backupDir} (skipping {@code .snapshot-*});</li>
   *   <li>move the new files from {@code newDir} to {@code dbDir} (skipping {@code .snapshot-complete}).</li>
   * </ol>
   * "Atomic" here is from the live database's perspective: the swap either fully completes or the
   * original live files are restored, never a half-installed mix. It is <i>not</i> crash-atomic - a
   * process crash mid-swap is reconciled on startup by {@link #recoverPendingSnapshotSwaps} via the
   * pending marker the caller leaves in place.
   * <p>
   * Guarantee on failure: if a move in <i>either</i> phase throws, the live directory is restored to its
   * original contents before the exception propagates, so {@code dbDir} is never left in an intermediate
   * state - a phase-1 failure leaves the un-moved originals in place and moves the backed-up ones back; a
   * phase-2 failure clears the partially-installed new files first, then restores the originals. If the
   * in-catch restore itself throws (e.g. {@link #clearLiveDatabaseFiles} fails), the IOException
   * propagates with dbDir partially swapped and the caller's pending marker still present, so
   * {@link #recoverPendingSnapshotSwaps} finishes the reconciliation on the next startup.
   */
  private static void atomicSwap(final Path dbDir, final Path newDir, final Path backupDir) throws IOException {
    Files.createDirectories(backupDir);

    boolean liveMovedToBackup = false;
    try {
      // Phase 1: move live files to backup (skip .snapshot-* dirs and the pending marker).
      try (final DirectoryStream<Path> stream = Files.newDirectoryStream(dbDir)) {
        for (final Path entry : stream) {
          if (entry.getFileName().toString().startsWith(".snapshot"))
            continue;
          Files.move(entry, backupDir.resolve(entry.getFileName().toString()), StandardCopyOption.REPLACE_EXISTING);
        }
      }
      // All originals are now in backup; the catch below uses this to decide whether dbDir holds
      // partially-installed new files (must be cleared) or un-moved originals (must be kept).
      liveMovedToBackup = true;

      // Phase 2: move new snapshot files to the live dir (skip the .snapshot-complete marker).
      try (final DirectoryStream<Path> stream = Files.newDirectoryStream(newDir)) {
        for (final Path entry : stream) {
          final String name = entry.getFileName().toString();
          if (SNAPSHOT_COMPLETE_FILE.equals(name))
            continue;
          Files.move(entry, dbDir.resolve(name), StandardCopyOption.REPLACE_EXISTING);
        }
      }

      // Make the rename directory entries durable before the caller deletes the retained backup. The
      // file data itself was already fsynced at extraction time; this fsync persists the directory
      // entries that now point at it, so a crash after the backup is gone cannot lose the swap (#4830).
      fsyncDirectory(dbDir);
    } catch (final IOException e) {
      // Log the root cause FIRST, before any restore step can throw and mask it.
      LogManager.instance().log(SnapshotInstaller.class, Level.SEVERE,
          "Snapshot swap failed, restoring live database from backup: %s", e, e.getMessage());
      // If phase 2 had started, partially-installed new files are now in dbDir and must be cleared
      // before restoring the originals; if phase 1 failed partway, the un-moved originals are still in
      // dbDir, so restoreBackup just moves the backed-up ones back to reconstruct the full set.
      try {
        if (liveMovedToBackup)
          clearLiveDatabaseFiles(dbDir);
        restoreBackup(dbDir, backupDir);
      } catch (final IOException restoreEx) {
        // Catch-inside-a-catch: the restore itself failed. Attach the root cause so it is never lost,
        // then propagate. The originals are still safe in backupDir and the caller's .snapshot-pending
        // marker is intact, so recoverPendingSnapshotSwaps completes the restore on the next startup.
        restoreEx.addSuppressed(e);
        throw restoreEx;
      }
      throw new IOException("Snapshot swap failed for " + dbDir, e);
    }

    // Remove the now-empty .snapshot-new directory
    deleteDirectoryIfExists(newDir);
  }

  private static void restoreBackup(final Path dbDir, final Path backupDir) throws IOException {
    try (final DirectoryStream<Path> stream = Files.newDirectoryStream(backupDir)) {
      for (final Path entry : stream)
        // REPLACE_EXISTING is required for the phase-1 partial-failure path: some originals may never
        // have left dbDir, so the backed-up copies must overwrite whatever partial state is there to
        // reconstruct the exact original set without leaving stale files behind.
        Files.move(entry, dbDir.resolve(entry.getFileName().toString()), StandardCopyOption.REPLACE_EXISTING);
    }
    // Persist the restored directory entries before the backup is deleted so a crash during rollback
    // recovery cannot lose the originals we just moved back (issue #4830).
    fsyncDirectory(dbDir);
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

  /**
   * Writes an empty marker file and forces both the file and its parent directory to stable storage.
   * Used for the {@code .snapshot-pending} and {@code .snapshot-complete} markers so the crash-recovery
   * state machine never reads back a marker whose creation was still buffered in the OS page cache
   * (issue #4830).
   */
  private static void writeMarkerDurable(final Path marker) throws IOException {
    Files.writeString(marker, "");
    try (final FileChannel channel = FileChannel.open(marker, StandardOpenOption.WRITE)) {
      channel.force(true);
    }
    fsyncDirectory(marker.getParent());
  }

  /**
   * Best-effort fsync of a directory so that file creations, renames and deletions within it survive a
   * power loss. Opening a directory as a {@link FileChannel} and forcing it is the POSIX way to persist
   * directory entries, but it is not supported on every platform (notably Windows, where opening a
   * directory throws). A failure here is therefore logged at FINE and ignored rather than aborting the
   * install: the snapshot file data itself is always fsynced individually, so the worst case on such a
   * platform is the pre-existing behaviour, not a regression. Package-private for unit testing.
   */
  static void fsyncDirectory(final Path dir) {
    if (dir == null)
      return;
    try (final FileChannel channel = FileChannel.open(dir, StandardOpenOption.READ)) {
      channel.force(true);
    } catch (final IOException e) {
      LogManager.instance().log(SnapshotInstaller.class, Level.FINE,
          "Directory fsync not supported or failed for %s: %s", null, dir, e.getMessage());
    }
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
