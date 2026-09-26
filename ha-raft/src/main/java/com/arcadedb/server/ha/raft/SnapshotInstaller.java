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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.ProtocolContext;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.WALFile;
import com.arcadedb.exception.DatabaseNotAvailableException;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.backup.BackupCoordinator;
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
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.KeyStore;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
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
 *   <li><b>Swap phase:</b> Move live files into {@code .snapshot-backup}, then move the staged
 *       files into the live directory. {@code .snapshot-swap-state} records each durable phase,
 *       so recovery never mistakes already-installed snapshot files for originals to back up.
 *       An I/O failure starts a separately recorded rollback.</li>
 *   <li><b>Cleanup phase:</b> Clean stale WAL files and reopen the database, clear and sync the
 *       pending marker, then delete the retained backup and swap state.</li>
 * </ol>
 * On startup, {@link #recoverPendingSnapshotSwaps(Path, ArcadeDBServer)} detects incomplete swaps
 * via the {@code .snapshot-pending} marker and either completes or rolls back each one, taking the same
 * per-database maintenance slot {@link #install} takes while it does (issue #7449).
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
  static final String SNAPSHOT_PENDING_FILE  = ArcadeDBServer.SNAPSHOT_PENDING_FILE;
  static final String SNAPSHOT_COMPLETE_FILE = ".snapshot-complete";
  static final String SNAPSHOT_SWAP_STATE_FILE = ".snapshot-swap-state";
  static final String SNAPSHOT_SWAP_STATE_TMP_FILE = SNAPSHOT_SWAP_STATE_FILE + ".tmp";

  private enum SwapPhase {
    BACKING_UP, INSTALLING, INSTALLED, ROLLING_BACK, RESTORING
  }

  /**
   * Test-only hook invoked after each durable swap-phase transition ({@code <PHASE>_UNPUBLISHED}, then
   * {@code <PHASE>}) and after each file move ({@code <PHASE>:<file>}). A test throws an {@link Error} or halts
   * the JVM from it to model a crash at that boundary without running the IOException rollback. {@code null} in
   * production.
   */
  static volatile Consumer<String> swapProgressForTesting = null;

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
   * Default maximum allowed uncompressed size for a single ZIP entry (10 GB), used when
   * {@link GlobalConfiguration#HA_SNAPSHOT_MAX_ENTRY_SIZE} is unset or not positive. Entries exceeding the limit
   * trigger a zip-bomb defense exception. Package-private for unit testing.
   */
  static final long MAX_ZIP_ENTRY_UNCOMPRESSED_BYTES = 10L * 1024 * 1024 * 1024;

  /**
   * Logged at most once: warns that SSL is enabled but the snapshot is being downloaded over plain
   * HTTP because no HTTPS endpoint could be resolved for the leader.
   */
  private static final AtomicBoolean PLAIN_HTTP_FALLBACK_WARNED = new AtomicBoolean(false);

  /**
   * Absolute database directory paths with installs currently in flight, and how many. The lifecycle assumes
   * installs for a given database never overlap (see {@link #closeLocalDatabaseIfOpen}); registering here turns
   * a violation of that assumption from a silent double-close into a logged WARNING so it is diagnosable after
   * the fact. Keyed by resolved path (not database name) so two logical servers in the same JVM - which use
   * distinct database directories - never raise a spurious overlap warning.
   * <p>
   * Since issue #7444 the per-database maintenance slot {@link #install} takes usually prevents the overlap
   * outright rather than only reporting it: a second install of the same database on the same server waits for the
   * first. Registration here still earns its keep, because that wait is bounded - an install that outlasts
   * {@code arcadedb.ha.snapshotInstallBackupWaitMs} can still be joined by a second one, and that is exactly the
   * case worth a WARNING.
   * <p>
   * <b>Reference-counted rather than a plain presence set (issue #7622).</b> The #7128 recovery-pass skip guards
   * below key off this map to know whether it is safe to delete a directory an install might still be touching,
   * so the guard has to stay true for as long as ANY overlapping install of that database is still running - not
   * just the first one to register. A plain set had no owner: two overlapping installs shared one entry, and the
   * {@code finally} of whichever finished FIRST removed it unconditionally, clearing the guard while the other
   * was still running and leaving the recovery pass free to delete its staging directory. Counting registrations
   * and removing the key only when the count reaches zero keeps the guard correct regardless of finishing order.
   * {@link #acquireNewDatabase} needs the opposite, exclusive discipline - two acquisitions of the same
   * never-seen database must never share one staging directory - so it registers through
   * {@link #tryAcquireInstallInFlightExclusive} instead of {@link #registerInstallInFlight}.
   */
  private static final Map<String, Integer> INSTALLS_IN_FLIGHT = new ConcurrentHashMap<>();

  /**
   * The {@link #INSTALLS_IN_FLIGHT} key for {@code dbDir}, normalized and absolutized exactly like the two
   * {@code inFlightKey} derivations in {@link #installHoldingMaintenanceSlot} and {@link #acquireNewDatabase}
   * (both {@code Path.of(...).normalize().toAbsolutePath().toString()}), so a path arriving from a directory
   * listing - as {@link #recoverPendingSnapshotSwaps(Path, ArcadeDBServer)} does - matches the key an in-flight
   * install registered from its own, differently-constructed {@code Path} (issue #7128).
   */
  private static String resolvedInFlightKey(final Path dbDir) {
    return dbDir.normalize().toAbsolutePath().toString();
  }

  /**
   * Registers one more install in flight for {@code inFlightKey} and returns whether another install already
   * held it (an overlap worth a WARNING at the call site). Always paired with exactly one
   * {@link #releaseInstallInFlight} call, from a {@code finally} block.
   */
  private static boolean registerInstallInFlight(final String inFlightKey) {
    final boolean[] overlapped = { false };
    INSTALLS_IN_FLIGHT.compute(inFlightKey, (key, count) -> {
      overlapped[0] = count != null;
      return count == null ? 1 : count + 1;
    });
    return overlapped[0];
  }

  /**
   * The exclusive form {@link #acquireNewDatabase} uses: registers {@code inFlightKey} only when no install -
   * of any kind - already holds it, atomically with the check. Returns {@code false}, registering nothing, when
   * one already does, so the caller can refuse rather than share a staging directory with it.
   */
  private static boolean tryAcquireInstallInFlightExclusive(final String inFlightKey) {
    return INSTALLS_IN_FLIGHT.putIfAbsent(inFlightKey, 1) == null;
  }

  /**
   * Releases one registration taken by {@link #registerInstallInFlight} or
   * {@link #tryAcquireInstallInFlightExclusive}, removing {@code inFlightKey} once every install that
   * registered it has released its own - see the reference-counting note on {@link #INSTALLS_IN_FLIGHT}.
   * Idempotent: releasing a key nothing holds (already released, e.g. by the caller's own early release before
   * delegating) is a no-op rather than an error.
   */
  private static void releaseInstallInFlight(final String inFlightKey) {
    INSTALLS_IN_FLIGHT.compute(inFlightKey, (key, count) -> count == null || count <= 1 ? null : count - 1);
  }

  /**
   * Whether ANY install is in flight in this JVM (issue #8363). The cheap first half of
   * {@link #isInstallInFlight(String)}: {@code RaftReplicatedDatabase} asks it on every client request, and the
   * overwhelmingly common answer is no, so that answer must cost one map read and no path arithmetic.
   */
  static boolean hasInstallsInFlight() {
    return !INSTALLS_IN_FLIGHT.isEmpty();
  }

  /**
   * Whether an install is replacing the database directory at {@code databasePath} right now, from the moment
   * {@link #install(String, String, Supplier, Supplier, String, ArcadeDBServer)} registers it - before the
   * download, which is the long part and runs with the live copy still open - until the swap is done or has been
   * rolled back (issue #8363). Keyed by resolved path, like the registry itself, so two logical servers in one JVM
   * never answer for each other's copy of a same-named database.
   */
  static boolean isInstallInFlight(final String databasePath) {
    return !INSTALLS_IN_FLIGHT.isEmpty() && INSTALLS_IN_FLIGHT.containsKey(resolvedInFlightKey(Path.of(databasePath)));
  }

  /**
   * Test-only: registers {@code dbDir} as having an install in flight, so a test can exercise the issue #7128
   * skip guard in {@link #recoverPendingSnapshotSwaps(Path, ArcadeDBServer)} without driving a real download.
   * Always paired with {@link #clearInstallInFlightForTesting} once the test is done with it.
   */
  static void markInstallInFlightForTesting(final Path dbDir) {
    registerInstallInFlight(resolvedInFlightKey(dbDir));
  }

  /** Test-only: undoes {@link #markInstallInFlightForTesting}. */
  static void clearInstallInFlightForTesting(final Path dbDir) {
    releaseInstallInFlight(resolvedInFlightKey(dbDir));
  }

  /**
   * Test-only barrier invoked once inside the registry-locked swap region of {@link #swapAndReopen}, after the
   * live database has been closed/deregistered and before the staged snapshot is moved into place. {@code null}
   * in production (the only cost is a single reference read per install). The issue-#4832 regression test sets it
   * to pause inside the critical section and prove a concurrent {@link ArcadeDBServer#getDatabase} blocks on the
   * registry lock instead of re-opening the database mid-swap.
   */
  static volatile Runnable swapBarrierForTesting = null;

  /**
   * Test-only barrier invoked at the head of {@link #recoverSingleDatabase}, with the per-database maintenance slot
   * already taken by {@link #recoverSingleDatabaseHoldingMaintenanceSlot} when there is a coordinator to take it
   * from. {@code null} in production (the only cost is a single reference read per recovered database). The
   * issue-#7449 regression test sets it to pause inside the repair and prove a concurrent backup of that database is
   * refused while its files are being moved.
   */
  static volatile Runnable recoveryBarrierForTesting = null;

  /**
   * Test-only barrier invoked inside {@link #acquireNewDatabase}, right after the early
   * {@code releaseInstallInFlight} of the existsDatabase race and before delegating to {@link #install}.
   * {@code null} in production (the only cost is a single reference read per acquisition that hits that
   * race). The PR-#7650-review regression test sets it to register a different, concurrent install on the
   * same key at exactly that point, proving the outer {@code finally} does not release that install's own
   * registration too.
   */
  static volatile Runnable existsDatabaseRaceBarrierForTesting = null;

  /**
   * Test-only barrier invoked with the database name once the leader's snapshot is fully staged in
   * {@code .snapshot-new} and before {@link #swapAndReopen} takes the registry lock, i.e. while the live copy is
   * still open. {@code null} in production (one reference read per install). The issue-#7958 regression test
   * pauses here to commit a write on the leader in the window between the leader serving the snapshot and this
   * node swapping it in, and proves the entry is not applied to the copy about to be discarded.
   */
  static volatile Consumer<String> snapshotStagedForTesting = null;

  /**
   * The effective per-entry cap. {@code arcadedb.ha.snapshotMaxEntrySize} declared and documented exactly this
   * limit but had no reader anywhere in the tree, so the only way to change it was to recompile this class
   * (issue #7121). A non-positive configured value falls back to the compiled default rather than disabling the
   * defense - a zip-bomb guard that an operator can switch off by typing 0 is not a guard.
   * <p>
   * Read from the SERVER's {@link ContextConfiguration} rather than from the {@link GlobalConfiguration}
   * enum, as the sibling reads in this class do. The enum is populated by {@code readConfiguration()} alone, which
   * consults {@code System.getProperty} and {@code System.getenv}: the server configuration file, {@code SET SERVER
   * SETTING} and the MCP {@code set_server_setting} tool all write into the overlay and never touch it, so an enum
   * read silently ignores every channel this {@code SCOPE.SERVER} setting advertises except a raw {@code -D}
   * (issue #7226). The overlay falls back to the enum for a key nobody set, so {@code -D} keeps working through it.
   *
   * @param configuration the server's configuration overlay; {@code null} in unit tests and in the non-Raft install
   *                      callers, which then see the enum (and therefore {@code -D}) alone
   */
  static long maxZipEntryUncompressedBytes(final ContextConfiguration configuration) {
    final long configured = configuration != null
        ? configuration.getValueAsLong(GlobalConfiguration.HA_SNAPSHOT_MAX_ENTRY_SIZE)
        : GlobalConfiguration.HA_SNAPSHOT_MAX_ENTRY_SIZE.getValueAsLong();
    return configured > 0 ? configured : MAX_ZIP_ENTRY_UNCOMPRESSED_BYTES;
  }

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

    // An install REPLACES this node's copy of the database - it closes the live one, swaps its directory for the
    // leader's snapshot and reopens it - so it is a restore of this node's copy, and it takes the same per-database
    // maintenance slot a restore takes (issue #7384). Without this, a backup running on THIS node had its directory
    // reinstalled underneath it: the restore that produced the entry ran on the leader, a different JVM, so the
    // leader's slot could not be the one that excluded it (issue #7444).
    //
    // Taken here rather than at any call site because this method is the choke point every install driver shares:
    // the forceSnapshot arm of applyInstallDatabaseEntry, the bootstrap installs, the reconciler's resync, and
    // acquireNewDatabase on both arms where it finds the database already registered and delegates here.
    //
    // The wait is bounded, and expiry is not fatal. An install applies a committed Raft entry and a follower that
    // declines to apply one diverges, so an in-flight backup can delay the install but must not veto it. The default
    // is proportionate rather than cautious: the download below runs on this same thread and takes minutes for a
    // large database, so the wait is small beside what the caller is already committed to.
    //
    // Bounding it also keeps one narrow circular wait from becoming a deadlock. A leader-side restore holds this
    // database's RESTORE slot on its request thread while replicateRestoredDatabase blocks waiting for the entry to
    // commit and apply; applyInstallDatabaseEntry normally returns early on a leader and never reaches here, but a
    // node that lost leadership between the submit and the apply does reach here, on the apply thread, with its own
    // request thread still holding the slot and waiting on it. The reservation is not reentrant and these are two
    // different threads, so an unbounded wait would be a cycle. Bounded, it costs the timeout and a warning.
    //
    // A null coordinator is not a production state - ArcadeDBServer's field is final and initialised inline - but
    // the unit tests that drive this method directly hand it a partially-stubbed server,
    // which is the same reason downloadSnapshot and purgeRaftLogBeforeInstall below already tolerate one. No
    // coordinator means no slot to take, and therefore none to release.
    final BackupCoordinator coordinator = server.getBackupCoordinator();

    // Registered before the maintenance-slot wait/acquisition below, not after: recoverPendingSnapshotSwaps
    // (issue #7128) reads this set to skip a database an install already owns, and a prior revision of that
    // fix registered this entry only once installHoldingMaintenanceSlot started - after coordinator.begin()
    // below had already run. That left a narrow but real window (coordinator.begin() returning through to
    // this add()) in which the recovery pass's check would not see this install yet, proceed into its own
    // coordinator.begin() for the same RESTORE slot, find it held, wait out its own bounded timeout, and
    // - by the same "an expired wait proceeds rather than skips" contract recoverSingleDatabaseHoldingMaintenanceSlot
    // documents for the #7449 backup race - delete the staging directory anyway (review finding on PR #7605).
    // Registering here first means the recovery pass sees this install the instant it starts, before it has
    // touched the coordinator or the filesystem at all.
    final Path dbPath = Path.of(databasePath).normalize().toAbsolutePath();
    final String inFlightKey = dbPath.toString();

    // The install is the engine's work, whatever thread drives it (issue #8363). The operator resync runs it on the
    // HTTP worker that received POST /api/v1/cluster/resync, which is tagged as a client request, and registering
    // this install below is exactly what makes RaftReplicatedDatabase refuse client requests on this database: left
    // tagged, the install would be refused by the gate it opens the moment anything it drives - the reopen at the
    // end of the swap included - went through the wrapper. Restored in the outer finally, so the caller's own
    // request goes on being what it was once the install has returned.
    final String callerProtocol = ProtocolContext.get();
    ProtocolContext.set(ProtocolContext.INTERNAL);
    // The lifecycle assumes installs for a given database never overlap (see closeLocalDatabaseIfOpen). If
    // they ever do, log it loudly rather than silently double-closing: registering here makes the violation
    // diagnosable, and counted rather than a plain flag so the guard below survives however many overlap
    // (issue #7622). Keyed by resolved path so distinct logical servers do not collide on database name.
    if (registerInstallInFlight(inFlightKey))
      LogManager.instance().log(SnapshotInstaller.class, Level.WARNING,
          "Concurrent snapshot install detected for '%s'; the install lifecycle assumes these never overlap "
              + "for the same database - this may indicate a coordination bug in the HA layer", null, databaseName);

    try {
      final BackupCoordinator.Operation refusedBy = coordinator == null ? null
          : coordinator.begin(databaseName, BackupCoordinator.Operation.RESTORE,
              server.getConfiguration().getValueAsLong(GlobalConfiguration.HA_SNAPSHOT_INSTALL_BACKUP_WAIT_MS));
      final boolean slotHeld = coordinator != null && refusedBy == null;
      if (refusedBy != null)
        LogManager.instance().log(SnapshotInstaller.class, Level.WARNING,
            "Reinstalling database '%s' from the leader while %s of it is still running on this node: the install "
                + "applies a committed Raft entry and cannot be declined, so it proceeds and that operation will fail "
                + "or produce an incomplete result. Raise '%s' to give it longer to finish", null,
            databaseName, refusedBy.phrase(), GlobalConfiguration.HA_SNAPSHOT_INSTALL_BACKUP_WAIT_MS.getKey());

      try {
        installHoldingMaintenanceSlot(databaseName, dbPath, leaderHttpAddrSupplier, leaderHttpsAddrSupplier,
            clusterToken, server);
      } finally {
        // Only what was actually reserved: a wait that expired took nothing, and releasing then would drop the
        // reservation the operation still in flight is holding.
        if (slotHeld)
          coordinator.end(databaseName, BackupCoordinator.Operation.RESTORE);
      }
    } finally {
      releaseInstallInFlight(inFlightKey);
      ProtocolContext.set(callerProtocol);
    }
  }

  /**
   * The install itself, with this node's per-database maintenance slot already held (or deliberately given up on)
   * and its {@link #INSTALLS_IN_FLIGHT} entry already registered, both by
   * {@link #install(String, String, Supplier, Supplier, String, ArcadeDBServer)}.
   */
  private static void installHoldingMaintenanceSlot(final String databaseName, final Path dbPath,
      final Supplier<String> leaderHttpAddrSupplier, final Supplier<String> leaderHttpsAddrSupplier,
      final String clusterToken, final ArcadeDBServer server) throws IOException {

    final Path snapshotNew = dbPath.resolve(SNAPSHOT_NEW_DIR);
    final Path snapshotBackup = dbPath.resolve(SNAPSHOT_BACKUP_DIR);
    final Path pendingMarker = dbPath.resolve(SNAPSHOT_PENDING_FILE);

    // A .snapshot-backup left behind together with the pending marker is NOT leftover junk: rollbackToBackup's
    // failure exit deliberately keeps both, and at that point the backup is the only intact copy of the
    // database (see its javadoc). Deleting it here destroyed that copy before this attempt had downloaded
    // anything, so a second failure - and the conditions that cause the first, a full volume, are exactly the
    // ones that cause the second - left the node with a torn dbPath and nothing to restore from (issue #7139).
    // Reconcile that state first, through the same startup-recovery routine, and refuse to start if the
    // reconciliation cannot complete.
    reconcileRetainedBackup(databaseName, dbPath, snapshotBackup, pendingMarker, server);

    // A marker that survives reconciliation over a directory that is not a loadable database is the torn state
    // recovery refuses to bless (#7139): nothing to reconcile from, so a fresh snapshot is the only cure and the
    // install proceeds. But if its download fails, the marker is the one thing stopping the torn directory from
    // being opened and served, so that failure must leave it in place (review finding on PR #8318, issue #7670).
    final boolean keepMarkerIfDownloadFails = Files.exists(pendingMarker) && !looksLikeADatabaseDirectory(dbPath);

    // Clean up any leftover state from a previous failed attempt. Reaching here means the backup (if there was
    // one) has been reconciled away, so these deletes only ever drop genuinely disposable state.
    deleteDirectoryIfExists(snapshotNew);
    deleteDirectoryIfExists(snapshotBackup);
    Files.deleteIfExists(pendingMarker);
    deleteSwapState(dbPath);

    Files.createDirectories(snapshotNew);

    // Write the pending marker BEFORE starting extraction, and fsync it together with the parent
    // directory so a crash right after this point still leaves the marker on disk for startup
    // recovery to find (issue #4830).
    writeMarkerDurable(pendingMarker);

    final int maxRetries = server.getConfiguration().getValueAsInteger(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRIES);
    final long retryBaseMs = server.getConfiguration().getValueAsLong(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRY_BASE_MS);

    // PHASE 0 - MAKE ROOM (issue #7037). This install is the self-heal for a diverged follower, and the volume
    // it writes onto may be the one the Raft log just filled: when that volume is under pressure, purge the local
    // log first so the segments below the applied index are reclaimable, then let the download refuse up front
    // (see downloadSnapshot) rather than fail with "No space left on device" halfway through the extraction.
    purgeRaftLogBeforeInstall(databaseName, server);

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
      if (keepMarkerIfDownloadFails)
        LogManager.instance().log(SnapshotInstaller.class, Level.SEVERE,
            "Snapshot download for '%s' failed over a database directory that holds no loadable database; keeping "
                + "its '%s' marker so the directory stays unopenable until a snapshot install succeeds", null,
            databaseName, SNAPSHOT_PENDING_FILE);
      else
        Files.deleteIfExists(pendingMarker);
      throw e;
    }

    // Mark download as complete. The extracted files were each fsynced as they were written
    // (see extractAndVerifySnapshot), so fsyncing this marker and the staging directory now
    // establishes the durability barrier: if this marker is on disk after a crash, every snapshot
    // file it vouches for is on disk too, and the swap can be safely completed by startup recovery.
    writeMarkerDurable(snapshotNew.resolve(SNAPSHOT_COMPLETE_FILE));

    final Consumer<String> staged = snapshotStagedForTesting;
    if (staged != null)
      staged.accept(databaseName);

    // PHASE 2 - SWAP. Set the server-wide flag BEFORE closing the database so HTTP handlers return 503
    // while the files are being moved.
    server.setSnapshotInstallInProgress(true);
    try {
      swapAndReopen(databaseName, dbPath, snapshotNew, snapshotBackup, pendingMarker, server);
    } finally {
      server.setSnapshotInstallInProgress(false);
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
        // Re-open the database so the server registers it (also validates the snapshot is loadable). The pending
        // marker is still on disk at this point - it is cleared only once this open succeeds - and it is what
        // stops every other caller from opening the directory, so this one reopen has to say it owns the marker
        // (issue #7129).
        server.reopenDatabaseUnderSnapshotRecovery(databaseName);
      } catch (final RuntimeException openEx) {
        // The freshly installed snapshot will not open (corrupt/incompatible files). Roll back to the
        // previous local copy and reopen it so the node is never left with a closed database.
        // The pending marker is intentionally NOT cleared here: it is dropped only on the success path
        // below. If rollbackToBackup succeeds, the recorded RESTORING phase tells the next recovery pass that
        // only cleanup remains, and it clears the marker - so leaving it is harmless and keeps recovery logic
        // in one place. If the rollback was instead interrupted, that same pass resumes it from the recorded
        // phase. Either way the marker is the single recovery hook.
        LogManager.instance().log(SnapshotInstaller.class, Level.SEVERE,
            "Installed snapshot for '%s' failed to open; rolling back to the previous local copy", openEx, databaseName);
        rollbackToBackup(dbPath, snapshotBackup);
        reopenQuietly(server, databaseName);
        throw new IOException("Snapshot for '" + databaseName
            + "' downloaded but failed to open; rolled back to the previous local copy", openEx);
      }

      // Success. Clear the pending marker FIRST, then drop the retained backup - the order matters because the
      // marker is what every reconciliation path keys on. Deleting the backup first and failing (or crashing)
      // before the marker leaves marker+backup, which reconcileRetainedBackup and startup recovery both read as
      // "the swap was interrupted" and would answer by restoring the OLD database over the new one that is
      // already correctly installed. Marker-first leaves at worst a stale backup directory with no marker, which
      // every path ignores and the next install deletes as leftover.
      completeSwapRecovery(dbPath);

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
    if (!tryAcquireInstallInFlightExclusive(inFlightKey))
      throw new IOException("Concurrent acquisition already in progress for '" + databaseName
          + "'; acquisitions for the same database must not overlap (possible HA coordination bug)");
    // Whether THIS invocation still owns the registration just taken. Set false the moment it releases early
    // (the existsDatabase race below), so the outer finally does not release again: a plain "idempotent,
    // no-op if already released" release is only actually a no-op if nothing else has registered inFlightKey
    // in the meantime - which an early release does not guarantee. A second install starting in that window
    // registers its own entry, and an unconditional second release here would decrement THAT install's count
    // instead of finding the key already gone, clearing the guard while it is still writing its staging
    // directory (review finding on PR #7650).
    boolean ownsRegistration = true;

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
        // Release our guard before delegating so install()'s own guard does not see a spurious overlap, and
        // record that we no longer own it so the outer finally does not release it a second time.
        releaseInstallInFlight(inFlightKey);
        ownsRegistration = false;
        // Test-only: lets a test register a DIFFERENT install on inFlightKey right here, in the window this
        // release just opened, to prove the outer finally below does not clear that install's own guard.
        if (existsDatabaseRaceBarrierForTesting != null)
          existsDatabaseRaceBarrierForTesting.run();
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
            "Acquired snapshot for new database '%s' failed validation; discarding it and leaving the database "
                + "absent (it will be re-acquired on the next reconcile): %s", null, databaseName, validationEx.getMessage());
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
            "Acquired snapshot for new database '%s' was validated but failed to open after publish; removing it "
                + "and leaving the database absent", openEx, databaseName);
        server.removeDatabase(databaseName);
        deleteDirectoryIfExists(dbPath);
        throw new IOException("Acquired snapshot for new database '" + databaseName + "' failed to open after publish", openEx);
      }

      HALog.log(SnapshotInstaller.class, HALog.BASIC, "New database '%s' acquired from leader", databaseName);
    } finally {
      // Only what THIS invocation still owns: the existsDatabase branch above already released its own
      // registration, and by now a different install may hold inFlightKey - releasing again would decrement
      // that install's count instead of being the no-op it looks like (review finding on PR #7650).
      if (ownsRegistration)
        releaseInstallInFlight(inFlightKey);
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
        .setSecurity(server.getDatabaseSecurityManager());
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
   *       read on a database meant to stay closed. A registered entry that is not open and cannot be reopened
   *       because its {@code .snapshot-pending} marker is on disk falls through to the next case (issue #7670);</li>
   *   <li>database absent: derives the path from {@link GlobalConfiguration#SERVER_DATABASE_DIRECTORY}
   *       without opening anything (nothing to open).</li>
   * </ul>
   * Package-private and intended only for the install call sites, which invoke it on an open database
   * just before closing it. The conditional open is safe for the install paths only because two installs
   * for the same database are never in flight at once (see {@link #closeLocalDatabaseIfOpen}); a
   * re-register racing a deliberate deregistration elsewhere would be a misuse.
   * <p>
   * <b>{@code server} must not be null.</b> Both branches dereference it - the first for
   * {@code existsDatabase}, the second for {@code getConfiguration} - so there is no path that could
   * usefully tolerate a null: every caller reaches this from a state machine that {@code createStateMachine}
   * wired before its reference escaped, so nothing can apply an entry against a half-wired one. Stated here
   * rather than enforced with a check, because
   * a null would mean the caller is unwired, and an unwired caller has nowhere to install a snapshot to:
   * the loud dereference is the correct outcome, and only the precondition was missing.
   */
  static String resolveDatabasePath(final ArcadeDBServer server, final String databaseName) {
    // Best-effort: the exists/get pair is not atomic, but it only resolves a path before the download
    // phase (no data at risk) and getDatabase returns a valid path even if it has to reopen.
    if (server.existsDatabase(databaseName)) {
      try {
        return ((DatabaseInternal) server.getDatabase(databaseName)).getDatabasePath();
      } catch (final DatabaseNotAvailableException e) {
        // Registered but not open, with the .snapshot-pending marker on disk: getDatabase refuses to open it, and
        // refusing here too failed every install driver before the install could reconcile that marker - the one
        // thing that makes the database openable again (issue #7670). The configured path below is the directory
        // getDatabase would have opened, so it is the same answer the lookup would have given.
        LogManager.instance().log(SnapshotInstaller.class, Level.WARNING,
            "Database '%s' is registered but did not resolve while locating it for a snapshot install (%s); "
                + "using its configured directory", null, databaseName, e.getMessage());
      }
    }
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
   * <p>
   * Both callers - {@link #swapAndReopen} and {@link #reconcileRetainedBackup} - run with the
   * {@code .snapshot-pending} marker on disk, which is the one state in which a registered entry that is
   * <i>not open</i> cannot be resolved at all: {@link ArcadeDBServer#getDatabase} throws
   * {@code DatabaseNotAvailableException} for it. So this resolves the instance through the same guarded lookup the
   * crash-recovery pass uses, {@link #closeRegisteredDatabaseForRepair}, and ignores whether anything was closed:
   * an entry that failed to resolve is already closed, so there is nothing left for this call to close, and the
   * callers' own reopen - {@code swapAndReopen} always, {@code reconcileRetainedBackup} once the marker is gone -
   * replaces the stale entry. Letting the lookup throw instead failed the whole install before it had moved a file,
   * and left the follower unable to resync from the leader until a restart (issue #7670).
   */
  private static void closeLocalDatabaseIfOpen(final ArcadeDBServer server, final String databaseName) {
    closeRegisteredDatabaseForRepair(server, databaseName);
  }

  /**
   * Closes a resolved local instance and takes it out of the server registry - the mutating half of
   * {@link #closeLocalDatabaseIfOpen}, shared with {@link #closeRegisteredDatabaseForRepair}.
   * <p>
   * Closes the embedded instance ({@code getEmbedded().close()}) rather than the wrapper: every caller is doing a
   * local file swap or repair, which must not carry the HA wrapper's replicated-close semantics.
   */
  private static void closeAndDeregister(final ArcadeDBServer server, final DatabaseInternal db,
      final String databaseName) {
    db.getEmbedded().close();
    server.removeDatabase(databaseName);
  }

  /**
   * Best-effort reopen used by the rollback paths: a failure here must not mask the original cause,
   * so it only logs. {@link ArcadeDBServer#reopenDatabaseUnderSnapshotRecovery} opens and registers the database
   * from disk when it is not already registered, looking past the {@code .snapshot-pending} marker these paths
   * deliberately retain.
   */
  private static void reopenQuietly(final ArcadeDBServer server, final String databaseName) {
    try {
      // Every caller of this is mid-reconciliation with the pending marker still on disk, so it reopens through
      // the installer-only entry point rather than the one that refuses a marked directory (issue #7129).
      server.reopenDatabaseUnderSnapshotRecovery(databaseName);
    } catch (final Exception e) {
      LogManager.instance().log(SnapshotInstaller.class, Level.SEVERE,
          "Failed to reopen database '%s' after a snapshot-install rollback; manual intervention may be required",
          e, databaseName);
    }
  }

  /**
   * Reconciles a retained backup or recorded swap phase before a new install is allowed to touch anything
   * (issues #7139 and #7769). The phase remains relevant even after a rollback consumed the backup.
   * <p>
   * A backup present ALONGSIDE the {@code .snapshot-pending} marker means the previous attempt did not finish:
   * either its swap was interrupted, or its rollback failed and left {@code dbPath} partially cleared, with the
   * backup as the only intact copy. Both are exactly what {@link #recoverPendingSnapshotSwaps} reconciles at
   * startup, so this runs the same routine rather than a second implementation of it - with the database closed
   * and the registry lock held, mirroring {@link #swapAndReopen}, because the reconciliation moves files under a
   * directory the server may have registered.
   * <p>
   * If the backup or the pending marker is still there afterwards the reconciliation failed, and the install is
   * refused: whatever it could not resolve stays on disk for the next attempt and for an operator, which is strictly
   * better than proceeding into a download that may fail for the same reason (a full volume) and leave nothing
   * behind at all. For the same reason the database is reopened only once the marker is gone.
   */
  private static void reconcileRetainedBackup(final String databaseName, final Path dbPath, final Path snapshotBackup,
      final Path pendingMarker, final ArcadeDBServer server) throws IOException {
    if (!Files.exists(pendingMarker)
        || (!Files.isDirectory(snapshotBackup) && !Files.exists(dbPath.resolve(SNAPSHOT_SWAP_STATE_FILE))
        && !Files.exists(dbPath.resolve(SNAPSHOT_SWAP_STATE_TMP_FILE))
        && !Files.exists(dbPath.resolve(SNAPSHOT_NEW_DIR).resolve(SNAPSHOT_COMPLETE_FILE))))
      return;

    LogManager.instance().log(SnapshotInstaller.class, Level.SEVERE,
        "A previous snapshot install for '%s' left unfinished recovery state and its pending marker in place, "
            + "so the live database directory may be incomplete. Reconciling that state before starting a new install",
        null, databaseName);

    // Same 503 window PHASE 2 opens around swapAndReopen: this moves files under the live database directory
    // too, and the engine-internal open paths do not consult the registry lock's meaning, only HTTP does.
    // Without it a client could be served from the directory mid-restore. Cleared in a finally so a failed
    // reconciliation cannot leave the server wedged in "install in progress".
    server.setSnapshotInstallInProgress(true);
    try {
      synchronized (server.getDatabasesLock()) {
        closeLocalDatabaseIfOpen(server, databaseName);
        recoverSingleDatabase(dbPath);
        if (!Files.exists(pendingMarker))
          reopenQuietly(server, databaseName);
      }
    } finally {
      server.setSnapshotInstallInProgress(false);
    }

    // recoverSingleDatabase reports its own failures and returns, so the caller has to read the outcome off the
    // filesystem. Two things have to be true, not one: the backup is gone AND what it was restored into is a
    // database. A restore that consumed the backup but left a directory that still cannot be opened would
    // otherwise pass this check, and the install would overwrite the questionable state with a fresh download -
    // destroying the one signal this whole mechanism exists to preserve.
    if (Files.isDirectory(snapshotBackup))
      throw new IOException("Refusing to install a snapshot for '" + databaseName
          + "': a retained .snapshot-backup from a previous failed install could not be reconciled into "
          + dbPath + ". It is the only intact copy of this database on this node and will not be deleted; "
          + "resolve the underlying problem (typically a full or read-only volume) and retry");

    if (Files.exists(pendingMarker))
      throw new IOException("Refusing to overwrite unresolved snapshot swap state for '" + databaseName + "'");

    if (!looksLikeADatabaseDirectory(dbPath))
      throw new IOException("Refusing to install a snapshot for '" + databaseName + "': snapshot swap recovery "
          + "finished but " + dbPath + " still does not hold a loadable database. Its state is preserved as-is "
          + "for inspection rather than overwritten by a fresh download");
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
      writeSwapPhase(dbPath, SwapPhase.ROLLING_BACK);
      resumeRollback(dbPath, snapshotBackup);
    } catch (final IOException e) {
      // dbPath may be partially cleared here. The caller leaves the .snapshot-pending marker in place,
      // so recoverPendingSnapshotSwaps reconciles dbPath from the retained backup on the next startup -
      // call this out so operators know that marker is the recovery hook to look for.
      LogManager.instance().log(SnapshotInstaller.class, Level.SEVERE,
          "Failed to roll back snapshot install for %s: %s. Leaving the .snapshot-pending marker so startup "
              + "recovery (recoverPendingSnapshotSwaps) restores the backup on the next restart", e, dbPath, e.getMessage());
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
    recoverPendingSnapshotSwaps(databasesDir, null);
  }

  /**
   * Scans all database subdirectories for pending snapshot swaps and completes or rolls back each one, with the
   * repaired database closed, its maintenance slot held, the registry lock held and the node's 503 window open
   * while it does.
   * <p>
   * The repair moves files into and out of the live database directory - {@code atomicSwap}, {@code restoreBackup}
   * and {@code clearLiveDatabaseFiles} all run against it - which is what #7444 set out to exclude a backup from
   * when it gave {@link #install} the slot. This driver of the same file movement was left out, and could not take
   * the slot as written: its signature had no {@link ArcadeDBServer} to reach a {@link BackupCoordinator} through
   * (issue #7449).
   * <p>
   * It is not a startup-only path, which is what makes the exclusion worth having rather than a formality:
   * {@code RaftHAServer.restartRatis} builds a new state machine and starts a new Ratis server while this server is
   * ONLINE, so {@code ArcadeStateMachine.initialize()} - and this pass - runs again with the auto-backup scheduler
   * started and the databases registered.
   * <p>
   * The slot is taken per database rather than once for the pass, because the repair is per directory: a backup of
   * one database must not hold up the repair of another. The wait is bounded by
   * {@code arcadedb.ha.snapshotInstallBackupWaitMs}, the same setting {@link #install} uses, and expiry is not
   * fatal for the same reason it is not there: a directory left half-swapped is worse than a backup that reads a
   * torn one, and {@code ArcadeDBServer.loadDatabases} keeps the database unopened until the marker clears.
   * <p>
   * The slot excludes a backup and nothing else, which left the repair renaming files under whatever was reading
   * the database through {@link ArcadeDBServer#getDatabase} - a reachable state, not a hypothetical one. See
   * {@link #recoverSingleDatabaseExcludingReaders} for how that is closed, and for why its reopen is conditional
   * where {@link #reconcileRetainedBackup}'s is not (issue #7530).
   *
   * @param databasesDir the parent directory containing all database subdirectories
   * @param server       the server whose {@link BackupCoordinator} admits the repair and whose registry the repair
   *                     closes the database out of, or {@code null} when there is none - an embedded caller, or a
   *                     unit test driving the pass directly - in which case the repair runs unreserved and
   *                     unsynchronised, exactly as it did before any of this existed
   */
  public static void recoverPendingSnapshotSwaps(final Path databasesDir, final ArcadeDBServer server) {
    if (!Files.isDirectory(databasesDir))
      return;

    try (final DirectoryStream<Path> stream = Files.newDirectoryStream(databasesDir, Files::isDirectory)) {
      for (final Path dbDir : stream) {
        final String dirName = dbDir.getFileName().toString();

        // Clean up an interrupted new-database acquisition staging dir (databases/.acquire-<name>, issue #4727).
        // A completed acquire atomically renames the staging dir to its final database name, so any surviving
        // .acquire-* dir is normally an interrupted download and safe to delete; the node re-acquires it on the
        // next reconcile. These are reserved ('.'-prefixed) so the boot scan never opened them.
        //
        // "Normally" - not always - because this pass is not startup-only (see the class javadoc above on
        // #7449): RaftHAServer.restartRatis rebuilds the state machine, and ArcadeStateMachine.initialize() -
        // and this pass with it - runs again while the server stays ONLINE, so acquireNewDatabase() can already
        // be downloading into this very staging dir. It registers under the FINAL database path (not the
        // staging path) in INSTALLS_IN_FLIGHT before it touches the staging dir at all, so translate the
        // staging name back to that final path and check it the same way the pending-marker branch below does
        // (review finding on PR #7605 - the first cut of the #7128 fix only guarded that branch, leaving this
        // one, an equally real deletion of an in-flight install's staging directory, unguarded).
        if (dirName.startsWith(ACQUIRE_STAGING_PREFIX)) {
          final Path finalDbPath = databasesDir.resolve(dirName.substring(ACQUIRE_STAGING_PREFIX.length()));
          if (INSTALLS_IN_FLIGHT.containsKey(resolvedInFlightKey(finalDbPath))) {
            LogManager.instance().log(SnapshotInstaller.class, Level.FINE,
                "Skipping acquisition-staging cleanup for %s this pass: an acquisition is already in flight for it",
                null, dbDir);
            continue;
          }
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

        // Same not-startup-only reasoning as the .acquire-* branch above, for a database install() is
        // refreshing in place rather than acquiring fresh. install() registers dbDir's resolved path in
        // INSTALLS_IN_FLIGHT before it does anything else - including acquiring the maintenance slot or
        // writing the pending marker below - specifically so this check sees it (issue #7128; tightened
        // further per the PR #7605 review to close a narrow window between the old registration point and
        // the maintenance-slot wait). Skipping it is safe either way the in-flight install ends: success
        // completes the swap and clears the marker itself; failure deletes its own staging directory and
        // marker in its catch block. A crash takes INSTALLS_IN_FLIGHT - an in-memory set - down with it, so
        // the next actual process restart finds the marker with nothing in-flight to race and reconciles it
        // normally.
        if (INSTALLS_IN_FLIGHT.containsKey(resolvedInFlightKey(dbDir))) {
          LogManager.instance().log(SnapshotInstaller.class, Level.FINE,
              "Skipping snapshot recovery for %s this pass: an install is already in flight for it", null, dbDir);
          continue;
        }

        LogManager.instance().log(SnapshotInstaller.class, Level.INFO,
            "Recovering pending snapshot swap for database directory: %s", null, dbDir);

        // The directory name IS the database name: ArcadeDBServer resolves databases/<name> both when it opens
        // them (loadDatabases) and when it looks one up (getDatabase), so it is the key the coordinator - and
        // every backup entry point that consults it - uses for this database.
        //
        // One database's repair must not end the scan. recoverSingleDatabase reports its own IOExceptions and
        // returns, so before #7530 this loop was effectively immune to a bad directory - every file-moving helper
        // it reaches declares only IOException. Closing the database first (#7530) put an UNCHECKED failure on
        // this path for the first time: LocalDatabase.close() declares no checked exception, and a close that
        // throws is most likely in exactly the disk-pressure and crash conditions that leave a marker behind. Left
        // unguarded it would escape this loop, leaving every other pending marker unrepaired this pass, and then
        // escape recoverPendingSnapshotSwaps into ArcadeStateMachine.initialize(), which does not catch it either
        // - turning one database's close failure into a failed Ratis start for the node (review finding on PR
        // #7631).
        //
        // So it is caught per database, loudly, and the marker is left on disk for the next pass. Error is not
        // caught: an OutOfMemoryError is not a per-database problem and the node has bigger trouble than a marker.
        try {
          recoverSingleDatabaseHoldingMaintenanceSlot(dirName, dbDir, server);
        } catch (final RuntimeException e) {
          LogManager.instance().log(SnapshotInstaller.class, Level.SEVERE,
              "Could not repair the interrupted snapshot swap of database '%s': %s. Its '%s' marker is left on disk, "
                  + "so the database stays unavailable until the next recovery pass reconciles it or an operator "
                  + "intervenes; the scan continues with the remaining databases", e, dirName, e.getMessage(),
              SNAPSHOT_PENDING_FILE);
        }
      }
    } catch (final IOException e) {
      LogManager.instance().log(SnapshotInstaller.class, Level.SEVERE,
          "Error scanning databases directory for pending snapshot swaps: %s", e, e.getMessage());
    }
  }

  /**
   * Runs {@link #recoverSingleDatabase} with this node's per-database maintenance slot held, so a backup of the
   * database cannot read a directory whose files are being moved (issue #7449). Excluding everything <i>else</i>
   * from that directory - queries, the registry, HTTP clients - is {@link #recoverSingleDatabaseExcludingReaders},
   * which this wraps (issue #7530).
   * <p>
   * A null coordinator is not a production state - {@code ArcadeDBServer}'s field is final and initialised inline -
   * but this pass also runs with no server at all (the {@code null} contract on
   * {@link #recoverPendingSnapshotSwaps(Path, ArcadeDBServer)}), and the {@code install} path above tolerates a
   * partially-stubbed server for the same reason. No coordinator means no slot to take, and therefore none to
   * release.
   * <p>
   * An expired wait proceeds rather than skipping the database. The repair is the only thing that clears the
   * {@code .snapshot-pending} marker, and until it is cleared {@code ArcadeDBServer.loadDatabases} refuses to open
   * the database at all - so skipping it would trade a backup that reads a torn directory for a database that stays
   * unavailable until the next restart.
   */
  private static void recoverSingleDatabaseHoldingMaintenanceSlot(final String databaseName, final Path dbDir,
      final ArcadeDBServer server) {
    final BackupCoordinator coordinator = server == null ? null : server.getBackupCoordinator();
    if (coordinator == null) {
      recoverSingleDatabaseExcludingReaders(databaseName, dbDir, server);
      return;
    }

    final BackupCoordinator.Operation refusedBy = coordinator.begin(databaseName, BackupCoordinator.Operation.RESTORE,
        server.getConfiguration().getValueAsLong(GlobalConfiguration.HA_SNAPSHOT_INSTALL_BACKUP_WAIT_MS));
    final boolean slotHeld = refusedBy == null;
    if (!slotHeld)
      LogManager.instance().log(SnapshotInstaller.class, Level.WARNING,
          "Repairing the interrupted snapshot swap of database '%s' while %s of it is still running on this node: "
              + "the repair is what clears the '%s' marker that keeps the database from being opened, so it proceeds "
              + "and that operation will fail or produce an incomplete result. Raise '%s' to give it longer to "
              + "finish", null, databaseName, refusedBy.phrase(), SNAPSHOT_PENDING_FILE,
          GlobalConfiguration.HA_SNAPSHOT_INSTALL_BACKUP_WAIT_MS.getKey());

    try {
      recoverSingleDatabaseExcludingReaders(databaseName, dbDir, server);
    } finally {
      // Only what was actually reserved: a wait that expired took nothing, and releasing then would drop the
      // reservation the operation still in flight is holding.
      if (slotHeld)
        coordinator.end(databaseName, BackupCoordinator.Operation.RESTORE);
    }
  }

  /**
   * Runs {@link #recoverSingleDatabase} with every reader of the database excluded from it, the way
   * {@link #reconcileRetainedBackup} already runs the identical call: the node-wide 503 window open, the registry
   * lock held, and any registered instance closed first (issue #7530).
   * <p>
   * The maintenance slot {@link #recoverSingleDatabaseHoldingMaintenanceSlot} takes around this excludes a
   * <i>backup</i>. It excludes nothing else, and the repair moves files into and out of the live database
   * directory - {@code atomicSwap}, {@code restoreBackup}, {@code clearLiveDatabaseFiles} - so a query served
   * meanwhile reads a directory whose files are being renamed. The {@code .snapshot-pending} marker does not stop
   * that on its own: {@link ArcadeDBServer#getDatabase} serves an already-registered, open database from its
   * lock-free fast path without consulting the marker at all, and {@link #swapAndReopen}'s two failure arms
   * deliberately produce exactly that state - marker retained, database reopened - so the node keeps serving. The
   * pass then runs again on the next {@code RaftHAServer.restartRatis}, with the node ONLINE.
   * <p>
   * <b>The reopen is conditional on what this pass closed, and that is the difference from
   * {@link #reconcileRetainedBackup}.</b> An install wants the database registered and serving when it is done, so
   * it reopens whenever its reconciliation cleared the marker. This pass also runs
   * at cold start, where {@code ArcadeDBServer.loadDatabases(false)} has <i>deferred</i> the marked directory on
   * purpose and the second {@code loadDatabases(true)} pass - which runs after this one - is what is supposed to
   * pick it up. Reopening unconditionally would register it from inside Ratis's state-machine initialization
   * instead. So the repair puts the registry back exactly as it found it: it reopens what it closed, and nothing
   * else.
   * <p>
   * The 503 window is taken per database rather than once for the whole pass, which keeps each window as short as
   * the repair of one directory. That flips the node-wide flag once per repaired database, which is safe only
   * because {@link ArcadeDBServer#setSnapshotInstallInProgress} counts holders rather than storing a boolean - an
   * {@code install} of another database can be holding the same window on another thread.
   * <p>
   * A {@code null} server is the documented contract of {@link #recoverPendingSnapshotSwaps(Path, ArcadeDBServer)}:
   * no server means no registry to lock, no HTTP clients to deflect and no registered database to close, so the
   * repair runs exactly as it did before this existed.
   */
  private static void recoverSingleDatabaseExcludingReaders(final String databaseName, final Path dbDir,
      final ArcadeDBServer server) {
    if (server == null) {
      recoverSingleDatabase(dbDir);
      return;
    }

    server.setSnapshotInstallInProgress(true);
    try {
      synchronized (server.getDatabasesLock()) {
        final boolean closedByThisRepair = closeRegisteredDatabaseForRepair(server, databaseName);
        try {
          recoverSingleDatabase(dbDir);
        } finally {
          // In a finally so a RuntimeException escaping the repair cannot skip the decision below.
          // recoverSingleDatabase reports its own IOExceptions and returns, so the outcome is read off the
          // filesystem rather than from a return value - the same way reconcileRetainedBackup reads its own.
          if (closedByThisRepair)
            reopenIfReconciled(server, databaseName, dbDir);
        }
      }
    } finally {
      server.setSnapshotInstallInProgress(false);
    }
  }

  /**
   * Reopens the database this repair closed, but <b>only if the repair actually reconciled the directory</b> -
   * i.e. only if the {@code .snapshot-pending} marker is gone (review finding on PR #7631).
   * <p>
   * This is deliberately unlike {@link #swapAndReopen}'s failure arms, which reopen unconditionally. They are
   * rescuing a database from a close <i>they</i> performed in order to serve it again, over a marker <i>they</i>
   * wrote moments ago on a directory that was healthy before they touched it. This pass is in the opposite position: the marker predates it, the repair has just failed to make sense of the
   * directory, and nothing is waiting on the handle - the next pass, or an operator, will deal with it.
   * <p>
   * Reopening anyway would recreate exactly the state this whole issue is about. A marked directory is refused by
   * {@link ArcadeDBServer#getDatabase}'s locked path and by {@code loadDatabases}, so a registered, open entry over
   * a still-marked directory buys nothing except the lock-free fast path - which serves it without ever consulting
   * the marker. That fast path is the hole #7530 exists to close; ending the repair by reopening into it would be
   * this pass putting the hole back.
   * <p>
   * Read off the filesystem rather than from a success flag because that is the fact that matters and the one every
   * other reconciliation path keys on, {@link #reconcileRetainedBackup} included: what the next
   * {@code loadDatabases} and the next repair will see, not what this call believes it did.
   */
  private static void reopenIfReconciled(final ArcadeDBServer server, final String databaseName, final Path dbDir) {
    if (!Files.exists(dbDir.resolve(SNAPSHOT_PENDING_FILE))) {
      reopenQuietly(server, databaseName);
      return;
    }

    LogManager.instance().log(SnapshotInstaller.class, Level.SEVERE,
        "The interrupted snapshot swap of database '%s' could not be reconciled, so its '%s' marker is still in "
            + "place and it is NOT being reopened: its directory is neither the previous database nor the installed "
            + "snapshot, and serving it would be worse than leaving it unavailable. It stays closed until a later "
            + "recovery pass or a new snapshot install reconciles the directory, or an operator does", null,
        databaseName, SNAPSHOT_PENDING_FILE);
  }

  /**
   * {@link #closeLocalDatabaseIfOpen} for the crash-recovery pass, which cannot let one database that will not
   * <i>resolve</i> abort the scan of every other one (issue #7530). The install paths reach it too, through
   * {@code closeLocalDatabaseIfOpen}, because they run with the same marker on disk (issue #7670).
   * <p>
   * {@code closeLocalDatabaseIfOpen} resolves the instance through {@link ArcadeDBServer#getDatabase}, and that
   * throws {@code DatabaseNotAvailableException} for a database that is registered but <i>closed</i> while the
   * {@code .snapshot-pending} marker is on disk - the marker refusal lives in the slow path, which only a
   * {@code null} or closed entry falls into. Letting that propagate would abandon the pass, and with it every
   * marker it had not reached yet: those databases stay unopenable until the next restart, which is worse than
   * the state being repaired.
   * <p>
   * Proceeding past a failed <b>lookup</b> is safe rather than merely expedient, and the reasoning is what bounds
   * this catch to the lookup alone. A registered, open entry is returned without the lookup ever consulting the
   * marker or the filesystem: by the lock-free fast path when the server is ONLINE, and otherwise by the locked
   * path, whose {@code db == null || !db.isOpen()} guard it fails. So a throwing lookup means the entry is not
   * open, and an entry that is not open holds no handles in the directory about to be repaired. This call holds
   * {@link ArcadeDBServer#getDatabasesLock()} throughout, so nothing can open it meanwhile, and
   * {@code loadDatabases(true)} and the next {@code getDatabase} pick the stale entry up once the marker is gone.
   * <p>
   * Only {@code DatabaseNotAvailableException} is caught, not {@code Exception}: that is the one the reasoning
   * above is built on, and anything else from the lookup is a bug worth seeing rather than a routine "not
   * resolved" (review finding on PR #7631). It is survivable to let it through because
   * {@link #recoverPendingSnapshotSwaps} guards each database's repair and continues the scan.
   * <p>
   * A failure of the <b>close</b> itself is deliberately NOT caught here. That one leaves a database registered
   * and open, and swallowing it would put this pass back to renaming files under exactly that - the defect this
   * whole change exists to remove. It leaves this database's marker on disk instead, and the per-database guard in
   * {@link #recoverPendingSnapshotSwaps} keeps it from taking the rest of the scan - or Ratis initialization -
   * down with it.
   *
   * @return {@code true} only when this call closed and deregistered a live instance, i.e. when the caller owes a
   * matching reopen
   */
  private static boolean closeRegisteredDatabaseForRepair(final ArcadeDBServer server, final String databaseName) {
    if (!server.existsDatabase(databaseName))
      return false;

    final DatabaseInternal db;
    try {
      db = (DatabaseInternal) server.getDatabase(databaseName);
    } catch (final DatabaseNotAvailableException e) {
      LogManager.instance().log(SnapshotInstaller.class, Level.WARNING,
          "Database '%s' is registered but did not resolve before its directory is swapped or repaired: %s. Only "
              + "an entry that is not open can fail to resolve, so it holds no files in that directory; the swap or "
              + "repair proceeds under the registry lock and leaves the entry for the next open to pick up once the "
              + "'%s' marker is cleared", e, databaseName, e.getMessage(), SNAPSHOT_PENDING_FILE);
      return false;
    }

    closeAndDeregister(server, db, databaseName);
    return true;
  }

  private static void recoverSingleDatabase(final Path dbDir) {
    final Runnable barrier = recoveryBarrierForTesting;
    if (barrier != null)
      barrier.run();

    final Path snapshotNew = dbDir.resolve(SNAPSHOT_NEW_DIR);
    final Path snapshotBackup = dbDir.resolve(SNAPSHOT_BACKUP_DIR);
    final Path pendingMarker = dbDir.resolve(SNAPSHOT_PENDING_FILE);

    try {
      final SwapPhase phase = readSwapPhase(dbDir);
      if (phase != null) {
        switch (phase) {
        case BACKING_UP, INSTALLING -> atomicSwap(dbDir, snapshotNew, snapshotBackup);
        case ROLLING_BACK, RESTORING -> {
          resumeRollback(dbDir, snapshotBackup);
          deleteDirectoryIfExists(snapshotNew);
        }
        case INSTALLED -> {
          // Every snapshot file is live, but the reopen that validates it never completed (a completed one clears
          // the marker before deleting anything). Roll forward: the leader's snapshot is authoritative, and
          // restoring the backup here would only trigger another install.
        }
        default -> throw new IOException("Unhandled snapshot swap phase " + phase + " in " + dbDir);
        }
        requireRecoveredDatabase(dbDir);
        completeSwapRecovery(dbDir);
        return;
      }

      final boolean hasCompleteMarker = Files.exists(snapshotNew.resolve(SNAPSHOT_COMPLETE_FILE));
      final boolean hasBackup = Files.isDirectory(snapshotBackup);

      if (hasCompleteMarker && hasBackup) {
        // Old versions recorded no phase. They moved staged files only after every original was in the backup,
        // so an empty staging directory means phase 2 finished and only cleanup remains.
        if (!hasStagedSnapshotFiles(snapshotNew)) {
          LogManager.instance().log(SnapshotInstaller.class, Level.INFO,
              "Cleaning up completed legacy snapshot swap for: %s", null, dbDir);
          requireRecoveredDatabase(dbDir);
          completeSwapRecovery(dbDir);
          return;
        }
        // Otherwise a live file may be an un-moved original OR an already-installed snapshot file. Re-running
        // phase 1 would destroy the latter (#7769); leave ambiguous layouts intact.
        if (hasLiveDatabaseFiles(dbDir)) {
          LogManager.instance().log(SnapshotInstaller.class, Level.SEVERE,
              "Cannot determine the phase of the legacy snapshot swap for %s. Preserving the live files, "
                  + "staging, backup and pending marker for manual recovery", null, dbDir);
          return;
        }
        // No live files: phase 1 finished, so complete the swap from the staging, as recovery always has. (A legacy
        // rollback that cleared the installed files and failed before restoring any leaves the same layout with a
        // partial staging; the old binary recorded nothing that could tell the two apart.)
        LogManager.instance().log(SnapshotInstaller.class, Level.INFO,
            "Completing interrupted snapshot swap for: %s", null, dbDir);
        atomicSwap(dbDir, snapshotNew, snapshotBackup);
        requireRecoveredDatabase(dbDir);
        completeSwapRecovery(dbDir);
        return;

      } else if (hasCompleteMarker && !hasBackup) {
        // Every version creates the backup before moving anything, so the live directory is intact: either a
        // legacy swap completed and only its cleanup was interrupted, or the swap never started (a crash or an
        // I/O failure before the first published phase; readSwapPhase ignores that unpublished write). After a
        // failure the node reopened and kept applying on the live database, so a leftover staging may be stale:
        // never install it here, drop it and let the next install fetch a current snapshot.
        LogManager.instance().log(SnapshotInstaller.class, Level.INFO,
            "Discarding the staging of a snapshot swap that did not start, or cleaning up a completed one, for: %s",
            null, dbDir);
        requireRecoveredDatabase(dbDir);
        completeSwapRecovery(dbDir);
        return;

      } else if (!hasCompleteMarker && hasBackup) {
        // Download was interrupted, backup exists: restore the backup
        LogManager.instance().log(SnapshotInstaller.class, Level.INFO,
            "Rolling back incomplete snapshot download for: %s", null, dbDir);
        deleteDirectoryIfExists(snapshotNew);
        // Move backup contents back into dbDir
        restoreBackup(dbDir, snapshotBackup);

      } else {
        // No completion marker and no backup. "The download was interrupted before the backup was created" is
        // only ONE way to reach this state, and it leaves dbDir intact. The other is "a backup was created, used
        // for a failed rollback, and is now gone", which leaves dbDir TORN - and this branch used to bless both,
        // deleting the marker and letting the node open a directory that is neither the old database nor the new
        // one, with nothing in the log saying so (issue #7139). So prove the premise before acting on it.
        if (!looksLikeADatabaseDirectory(dbDir)) {
          LogManager.instance().log(SnapshotInstaller.class, Level.SEVERE,
              "Snapshot recovery for %s found no completion marker and no backup to restore from, and the database "
                  + "directory does not hold a loadable schema - it is incomplete, not intact. Leaving it and the "
                  + ".snapshot-pending marker untouched for inspection rather than accepting it as a healthy "
                  + "database; the node will reacquire this database from the leader", null, dbDir);
          return;
        }

        // Download was interrupted before backup was created: just clean up
        LogManager.instance().log(SnapshotInstaller.class, Level.INFO,
            "Cleaning up orphaned snapshot directory for: %s", null, dbDir);
        deleteDirectoryIfExists(snapshotNew);
      }

      Files.deleteIfExists(pendingMarker);
      fsyncDirectory(dbDir);

    } catch (final IOException e) {
      LogManager.instance().log(SnapshotInstaller.class, Level.SEVERE,
          "Error recovering snapshot swap for %s: %s", e, dbDir, e.getMessage());
    }
  }

  private static void requireRecoveredDatabase(final Path dbDir) throws IOException {
    if (!looksLikeADatabaseDirectory(dbDir))
      throw new IOException("Recovered snapshot has no loadable schema in " + dbDir
          + "; retaining the pending marker and any remaining copies");
  }

  /** Clears the recovery trigger durably before deleting any retained copy. Cleanup can then be retried safely. */
  private static void completeSwapRecovery(final Path dbDir) throws IOException {
    Files.deleteIfExists(dbDir.resolve(SNAPSHOT_PENDING_FILE));
    fsyncDirectory(dbDir);
    deleteDirectoryIfExists(dbDir.resolve(SNAPSHOT_NEW_DIR));
    deleteDirectoryIfExists(dbDir.resolve(SNAPSHOT_BACKUP_DIR));
    deleteSwapState(dbDir);
  }

  private static void deleteSwapState(final Path dbDir) throws IOException {
    Files.deleteIfExists(dbDir.resolve(SNAPSHOT_SWAP_STATE_FILE));
    Files.deleteIfExists(dbDir.resolve(SNAPSHOT_SWAP_STATE_TMP_FILE));
    fsyncDirectory(dbDir);
  }

  /**
   * Returns the published swap phase, or {@code null} when the swap has not moved anything yet. An unpublished
   * temporary file without a published predecessor can only be the first BACKING_UP write, possibly torn by the
   * crash or an I/O failure, and that write precedes the backup directory and every move: with a complete staging
   * directory and no backup it is ignored, and recovery treats the swap as never started. Any other unpublished or
   * unreadable state is refused.
   */
  private static SwapPhase readSwapPhase(final Path dbDir) throws IOException {
    final Path state = dbDir.resolve(SNAPSHOT_SWAP_STATE_FILE);
    if (!Files.exists(state)) {
      final Path temporary = dbDir.resolve(SNAPSHOT_SWAP_STATE_TMP_FILE);
      if (!Files.exists(temporary) || (!Files.isDirectory(dbDir.resolve(SNAPSHOT_BACKUP_DIR))
          && Files.exists(dbDir.resolve(SNAPSHOT_NEW_DIR).resolve(SNAPSHOT_COMPLETE_FILE))))
        return null;
      throw new IOException("Cannot safely resume unpublished swap state in " + temporary + "; preserving all files");
    }
    try {
      return SwapPhase.valueOf(Files.readString(state).strip());
    } catch (final IllegalArgumentException e) {
      throw new IOException("Unrecognized snapshot swap state in " + state + "; preserving all files", e);
    }
  }

  private static boolean hasStagedSnapshotFiles(final Path snapshotNew) throws IOException {
    try (final DirectoryStream<Path> staged = Files.newDirectoryStream(snapshotNew,
        entry -> !entry.getFileName().toString().equals(SNAPSHOT_COMPLETE_FILE))) {
      return staged.iterator().hasNext();
    }
  }

  private static boolean hasLiveDatabaseFiles(final Path dbDir) throws IOException {
    try (final DirectoryStream<Path> live = Files.newDirectoryStream(dbDir,
        entry -> !entry.getFileName().toString().startsWith(".snapshot"))) {
      return live.iterator().hasNext();
    }
  }

  /** Publish either the previous phase or the next one, never a truncated state file after a crash. */
  private static void writeSwapPhase(final Path dbDir, final SwapPhase phase) throws IOException {
    final Path temporary = dbDir.resolve(SNAPSHOT_SWAP_STATE_TMP_FILE);
    writeFileForced(temporary, phase.name());
    snapshotSwapProgress(phase.name() + "_UNPUBLISHED");
    // No non-atomic fallback: an unsupported filesystem must refuse the swap rather than lose its phase.
    Files.move(temporary, dbDir.resolve(SNAPSHOT_SWAP_STATE_FILE),
        StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    fsyncDirectory(dbDir);
    snapshotSwapProgress(phase.name());
  }

  private static void snapshotSwapProgress(final String point) {
    final Consumer<String> progress = swapProgressForTesting;
    if (progress != null)
      progress.accept(point);
  }

  /** Once originals start moving back, retries must never clear the live directory again. */
  private static void resumeRollback(final Path dbDir, final Path backupDir) throws IOException {
    if (readSwapPhase(dbDir) == SwapPhase.ROLLING_BACK) {
      if (!Files.isDirectory(backupDir))
        throw new IOException("Cannot clear snapshot files without the retained backup in " + dbDir);
      clearLiveDatabaseFiles(dbDir);
      fsyncDirectory(dbDir);
      writeSwapPhase(dbDir, SwapPhase.RESTORING);
    }
    if (Files.isDirectory(backupDir))
      restoreBackup(dbDir, backupDir);
  }

  /**
   * Whether {@code dbDir} holds something that can be opened as a database, used by the recovery branch that has
   * no marker and no backup to reason from (issue #7139). A rollback that tore the directory took the schema
   * file with everything else, so its absence is the cheapest reliable evidence of a torn directory rather than
   * an untouched one.
   * <p>
   * Delegates to {@link DatabaseFactory#exists()} rather than testing {@code schema.json} directly: that is the
   * engine's own definition, and it also accepts {@code schema.prev.json}, the marker {@code LocalSchema} leaves
   * mid-rewrite. A database interrupted during a schema rewrite is exactly the kind of state this path exists to
   * reason about, and testing only the final name would misread it as torn.
   */
  private static boolean looksLikeADatabaseDirectory(final Path dbDir) {
    try (final DatabaseFactory factory = new DatabaseFactory(dbDir.toString())) {
      return factory.exists();
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
        downloadSnapshot(databaseName, snapshotNewDir, snapshotUrl, clusterToken, https, server);
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

  private static void downloadSnapshot(final String databaseName, final Path targetDir, final String snapshotUrl,
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

      // A leader on issue #4831 or later advertises a completeness manifest via this header; when present
      // the manifest becomes mandatory, so a truncated download (manifest dropped) fails loudly. A leader
      // predating #4831 omits the header, and the follower keeps the legacy "ZipInputStream reached EOF"
      // acceptance for backward compatibility during a rolling upgrade.
      final boolean manifestRequired = "1".equals(connection.getHeaderField(SnapshotManager.MANIFEST_HEADER));

      // A leader on #7037 or later says how many bytes the archive inflates to. Refuse before reading the body
      // when the target volume cannot hold them: the extraction would otherwise fail on the last file it could
      // not fit, after the whole transfer, and leave a follower already short of space with a partial staging
      // directory to clean up. A leader predating #7037 omits the header and the check is skipped.
      checkUsableSpace(targetDir, parseUncompressedBytes(connection.getHeaderField(SnapshotManager.UNCOMPRESSED_BYTES_HEADER)),
          databaseName);

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
      extractAndVerifySnapshot(source, rawCounter, targetDir, manifestRequired, server);
    } finally {
      connection.disconnect();
    }
  }

  /**
   * Asks the local Raft server to snapshot and purge its log before a database snapshot is written (issue #7037),
   * when its storage volume is under pressure. No-op without Raft HA (unit tests, the non-Raft install callers) and
   * best-effort otherwise: the purge only makes the space reclaimable, it is not a precondition of the install.
   */
  private static void purgeRaftLogBeforeInstall(final String databaseName, final ArcadeDBServer server) {
    if (server != null && server.getHA() instanceof RaftHAPlugin plugin && plugin.getRaftHAServer() != null)
      plugin.getRaftHAServer().compactRaftLogBeforeSnapshotInstall(databaseName);
  }

  /** Parses the {@link SnapshotManager#UNCOMPRESSED_BYTES_HEADER} value; absent or malformed reads as unknown ({@code -1}). */
  static long parseUncompressedBytes(final String header) {
    if (header == null || header.isEmpty())
      return -1L;
    try {
      return Long.parseLong(header.trim());
    } catch (final NumberFormatException e) {
      return -1L;
    }
  }

  /**
   * Refuses an install whose files cannot fit on the volume hosting {@code targetDir} (issue #7037). The volume is
   * the target directory or its nearest existing ancestor, because {@code getUsableSpace()} answers 0 for a path
   * that does not exist yet. An unknown size ({@code <= 0}) or an unresolvable volume never refuses: the check
   * turns a certain failure into a clear early one, it does not add a new way to fail. Package-private for tests.
   */
  static void checkUsableSpace(final Path targetDir, final long requiredBytes, final String databaseName) throws IOException {
    if (requiredBytes <= 0)
      return;
    final File volume = nearestExistingAncestor(targetDir.toAbsolutePath().toFile());
    if (volume == null)
      return;
    final long usable = volume.getUsableSpace();
    final long needed = withAllocationReserve(requiredBytes);
    if (usable < needed)
      throw new IOException("Insufficient space to install the snapshot of '" + databaseName + "': it inflates to "
          + requiredBytes + " bytes (" + needed + " with the allocation reserve) but the volume of '"
          + volume.getAbsolutePath() + "' has " + usable + " usable. Free space on the volume (the Raft log is purged "
          + "before an install when its volume is under pressure; see arcadedb.ha.snapshotInterval and "
          + "arcadedb.ha.raftStorageMinFreeSpacePerc) and the install is retried");
  }

  /**
   * {@code path} itself when it exists, otherwise its nearest existing ancestor, or {@code null} when none does.
   * {@code File.getUsableSpace()} and {@code getTotalSpace()} answer 0 for a path that does not exist, which would
   * read as "disk full" for a directory that is about to be created (a staging directory, the Raft storage
   * directory before Ratis creates it), so every free-space probe resolves its volume through this one walk.
   * The walk runs on the absolute form: a relative path's parent chain ends at {@code null} where its components
   * run out, before ever reaching the working directory that exists.
   */
  static File nearestExistingAncestor(final File path) {
    File dir = path.getAbsoluteFile();
    while (dir != null && !dir.exists())
      dir = dir.getParentFile();
    return dir;
  }

  /**
   * Reserve added on top of the advertised payload before it is compared with the usable space: the payload is the
   * logical size, and the extraction also pays per-file block rounding, directory metadata and the durability
   * markers, and the swap keeps the previous copy in {@code .snapshot-backup} until it is dropped. A payload the
   * volume can hold only to the byte would still run out mid-extraction, which is the failure the check exists to
   * turn into an early, clear one. Package-private for tests.
   */
  static final int  SPACE_RESERVE_PERCENT   = 5;
  static final long SPACE_RESERVE_MIN_BYTES = 16L * 1024 * 1024;

  /** {@code requiredBytes} plus the allocation reserve, saturating at {@link Long#MAX_VALUE}. */
  static long withAllocationReserve(final long requiredBytes) {
    final long reserve = Math.max(requiredBytes / 100L * SPACE_RESERVE_PERCENT, SPACE_RESERVE_MIN_BYTES);
    final long needed = requiredBytes + reserve;
    return needed < requiredBytes ? Long.MAX_VALUE : needed;
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
   * @param server           the server whose configuration carries the per-entry cap; may be {@code null}
   */
  static void extractAndVerifySnapshot(final InputStream source, final CountingInputStream rawCounter,
      final Path targetDir, final boolean manifestRequired, final ArcadeDBServer server) throws IOException {
    // Records the size+CRC32 of each file actually extracted, used to verify against the manifest.
    final Map<String, long[]> extracted = new HashMap<>();
    byte[] manifestBytes = null;
    // Read once for the whole install rather than per entry: the limit must not change mid-extraction, and a
    // snapshot with many small entries should not pay a configuration lookup for each of them.
    final long maxEntryBytes = maxZipEntryUncompressedBytes(server != null ? server.getConfiguration() : null);

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
          final long uncompressedBytes = copyWithLimit(zipIn, cos, maxEntryBytes, entryName);

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

    verifyManifest(manifestBytes, extracted, manifestRequired, targetDir);
  }

  /**
   * Validates the extracted snapshot against its manifest (issue #4831).
   * <ul>
   *   <li>manifest absent + not required: legacy leader, nothing to verify;</li>
   *   <li>manifest absent + required: the leader advertised a manifest but it never arrived - the download
   *       was truncated before the final entry, so reject;</li>
   *   <li>manifest present: every listed file must have been extracted with a matching size and CRC32, AND
   *       must still be present on disk under {@code targetDir} with that same size.</li>
   * </ul>
   * <p>
   * The on-disk re-stat (issue #7128) exists because {@code extracted} is a record of what this call
   * <i>streamed</i>, taken as each entry was written and fsynced - it says nothing about whether the file is
   * still there by the time this method runs. A concurrent boot-time recovery pass re-entering through a
   * runtime Ratis restart can read this same staging directory as "orphaned" (no completion marker yet) and
   * delete it out from under an extraction already in flight; every entry extracted before that deletion is
   * gone from disk yet still recorded in {@code extracted} with a matching size and CRC, so the map-only check
   * verified a directory that no longer existed. Re-stating turns that silent bad install into a loud one.
   * Existence and size only, not a CRC re-read: the CRC was already computed from the exact bytes written in
   * this call, immediately before the fsync that made them durable, so it cannot itself have been corrupted by
   * a concurrent deletion the way "is the file still there" can - and re-reading every file a second time to
   * recompute it would double this method's I/O for a check the in-memory record already answers correctly.
   */
  static void verifyManifest(final byte[] manifestBytes, final Map<String, long[]> extracted,
      final boolean manifestRequired, final Path targetDir) throws IOException {
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

      final Path onDisk = targetDir.resolve(entry.name());
      final long sizeOnDisk;
      try {
        sizeOnDisk = Files.size(onDisk);
      } catch (final NoSuchFileException e) {
        throw new IOException("Snapshot file '" + entry.name() + "' was extracted and verified but is no longer "
            + "present on disk under " + targetDir + " - the staging directory was modified concurrently while "
            + "this install was in progress (possible concurrent recovery pass, issue #7128)", e);
      } catch (final IOException e) {
        // A narrower catch than NoSuchFileException above would miss it: not every filesystem/JDK combination
        // is guaranteed to raise that specific subtype for a missing file. But a message asserting concurrent
        // modification as the cause is only earned for that specific, common case; any other I/O failure here
        // (a permission error, a disk-level fault) gets a neutral message instead of a misdiagnosis, with the
        // wrapped exception carrying the real cause either way.
        throw new IOException("Snapshot file '" + entry.name() + "' was extracted and verified but could not be "
            + "re-verified on disk under " + targetDir + ": " + e.getMessage(), e);
      }
      if (sizeOnDisk != entry.size())
        throw new IOException("Snapshot file '" + entry.name() + "' was extracted and verified but now measures "
            + sizeOnDisk + " bytes on disk instead of the manifest's " + entry.size()
            + " - the staging directory was modified concurrently while this install was in progress "
            + "(possible concurrent recovery pass, issue #7128)");
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
   * pending marker and durable swap phase. Recovery resumes INSTALLING without repeating BACKING_UP.
   * INSTALLED survives staging cleanup, and RESTORING prevents a repeated rollback from clearing
   * original files that have already been moved back.
   * <p>
   * Guarantee on failure: if a move in <i>either</i> phase throws, the live directory is restored to its
   * original contents before the exception propagates, so {@code dbDir} is never left in an intermediate
   * state - a phase-1 failure leaves the un-moved originals in place and moves the backed-up ones back; a
   * phase-2 failure clears the partially-installed new files first, then restores the originals. If the
   * in-catch restore itself throws (e.g. {@link #clearLiveDatabaseFiles} fails), the IOException
   * propagates with dbDir partially swapped and the caller's pending marker still present, so
   * {@link #recoverPendingSnapshotSwaps} finishes the reconciliation on the next startup.
   */
  static void atomicSwap(final Path dbDir, final Path newDir, final Path backupDir) throws IOException {
    SwapPhase phase = readSwapPhase(dbDir);
    if (phase == null) {
      writeSwapPhase(dbDir, SwapPhase.BACKING_UP);
      phase = SwapPhase.BACKING_UP;
    }
    if (phase != SwapPhase.BACKING_UP && phase != SwapPhase.INSTALLING)
      throw new IOException("Cannot start a snapshot swap in phase " + phase + " for " + dbDir);
    if (phase == SwapPhase.INSTALLING && !Files.isDirectory(backupDir))
      throw new IOException("Cannot resume snapshot installation without its retained backup in " + dbDir);

    Files.createDirectories(backupDir);
    boolean liveMovedToBackup = phase == SwapPhase.INSTALLING;
    try {
      if (!liveMovedToBackup) {
        // Only BACKING_UP may move originals. INSTALLING's live files belong to the new snapshot.
        try (final DirectoryStream<Path> stream = Files.newDirectoryStream(dbDir)) {
          for (final Path entry : stream) {
            if (entry.getFileName().toString().startsWith(".snapshot"))
              continue;
            Files.move(entry, backupDir.resolve(entry.getFileName().toString()), StandardCopyOption.REPLACE_EXISTING);
            snapshotSwapProgress("BACKING_UP:" + entry.getFileName());
          }
        }
        fsyncDirectory(backupDir);
        fsyncDirectory(dbDir);
        writeSwapPhase(dbDir, SwapPhase.INSTALLING);
        liveMovedToBackup = true;
      }

      // Phase 2: move new snapshot files to the live dir (skip the .snapshot-complete marker).
      try (final DirectoryStream<Path> stream = Files.newDirectoryStream(newDir)) {
        for (final Path entry : stream) {
          final String name = entry.getFileName().toString();
          if (SNAPSHOT_COMPLETE_FILE.equals(name))
            continue;
          Files.move(entry, dbDir.resolve(name), StandardCopyOption.REPLACE_EXISTING);
          snapshotSwapProgress("INSTALLING:" + entry.getFileName());
        }
      }

      // Make the rename directory entries durable before the caller deletes the retained backup. The
      // file data itself was already fsynced at extraction time; this fsync persists the directory
      // entries that now point at it, so a crash after the backup is gone cannot lose the swap (#4830).
      fsyncDirectory(dbDir);
      fsyncDirectory(newDir);
      writeSwapPhase(dbDir, SwapPhase.INSTALLED);
    } catch (final IOException e) {
      // Log the root cause FIRST, before any restore step can throw and mask it.
      LogManager.instance().log(SnapshotInstaller.class, Level.SEVERE,
          "Snapshot swap failed, restoring live database from backup: %s", e, e.getMessage());
      // If phase 2 had started, partially-installed new files are now in dbDir and must be cleared
      // before restoring the originals; if phase 1 failed partway, the un-moved originals are still in
      // dbDir, so restoreBackup just moves the backed-up ones back to reconstruct the full set.
      try {
        writeSwapPhase(dbDir, liveMovedToBackup ? SwapPhase.ROLLING_BACK : SwapPhase.RESTORING);
        resumeRollback(dbDir, backupDir);
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
      for (final Path entry : stream) {
        // REPLACE_EXISTING is required for the phase-1 partial-failure path: some originals may never
        // have left dbDir, so the backed-up copies must overwrite whatever partial state is there to
        // reconstruct the exact original set without leaving stale files behind.
        Files.move(entry, dbDir.resolve(entry.getFileName().toString()), StandardCopyOption.REPLACE_EXISTING);
        snapshotSwapProgress("RESTORING:" + entry.getFileName());
      }
    }
    // Persist the restored directory entries before the backup is deleted so a crash during rollback
    // recovery cannot lose the originals we just moved back (issue #4830).
    fsyncDirectory(dbDir);
    deleteDirectoryIfExists(backupDir);
  }

  /**
   * Removes the WAL files left in a freshly swapped-in snapshot directory, through the same lock-aware sweep the
   * engine's own close-time cleanup uses (issue #7505).
   * <p>
   * This used to delete every {@code *.wal} in the directory by name alone, which is the failure class #7479
   * reported and PR #7504 fixed in {@code TransactionManager.close()} - a second, unprotected copy of it. Under
   * normal HA operation there is no live local instance holding these files at this point ({@code swapAndReopen}
   * has already closed the local database), so the lock always succeeds and the outcome is unchanged; on a
   * database directory shared with another live process - the case #7479 described - the sweep now leaves that
   * instance's files alone instead of deleting them out from under it.
   * <p>
   * A skipped file is reported LOUDER here than in the engine sweep, and deliberately. There, a surviving orphan
   * is inert. Here the directory is about to be reopened as the installed snapshot, and a WAL file that outlives
   * the swap would be replayed on top of pages it was never written against - so an operator has to hear that the
   * install completed with one still in place.
   */
  // @VisibleForTesting
  static void cleanupWalFiles(final Path dbDir) {
    final File[] walFiles = dbDir.toFile().listFiles((dir, name) -> name.endsWith(".wal"));
    if (walFiles == null)
      return;
    for (final File walFile : walFiles)
      switch (WALFile.deleteIfNotHeldByAnotherInstance(walFile)) {
      case DELETED -> {
        // Nothing to say: the file this snapshot replaces is gone, which is the expected outcome.
      }
      case SKIPPED_LOCKED -> LogManager.instance().log(SnapshotInstaller.class, Level.SEVERE,
          "WAL file '%s' is still open by another database instance and was left in place while installing the "
              + "snapshot into '%s'. The database directory appears to be shared with another live process, which "
              + "is not supported: that WAL may be replayed against the installed snapshot's pages",
          null, walFile.getName(), dbDir);
      case ERROR -> LogManager.instance().log(SnapshotInstaller.class, Level.WARNING,
          "Failed to delete stale WAL file: %s", null, walFile.getName());
      }
  }

  /**
   * Writes an empty marker file and forces both the file and its parent directory to stable storage.
   * Used for the {@code .snapshot-pending} and {@code .snapshot-complete} markers so the crash-recovery
   * state machine never reads back a marker whose creation was still buffered in the OS page cache
   * (issue #4830).
   */
  private static void writeMarkerDurable(final Path marker) throws IOException {
    writeFileForced(marker, "");
    fsyncDirectory(marker.getParent());
  }

  private static void writeFileForced(final Path file, final String content) throws IOException {
    Files.writeString(file, content);
    try (final FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE)) {
      channel.force(true);
    }
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
