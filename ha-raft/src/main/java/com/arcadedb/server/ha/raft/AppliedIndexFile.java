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
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import java.io.IOException;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

/**
 * The applied-index file of this node: the Raft log index each database has applied up to, and the quarantine and stale
 * read floors persisted beside it, so a restart resumes from what was made durable rather than from the Ratis snapshot
 * marker alone.
 * <p>
 * Split out of {@link ArcadeStateMachine}, which updates these positions as it applies entries.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class AppliedIndexFile {
  private final ArcadeStateMachine sm;

  // Persisted applied-index bookkeeping. One ArcadeStateMachine multiplexes every database onto a
  // single Raft group, so a single global scalar cannot answer a per-database question: a co-located
  // database advancing the shared log past another database's entry would make the global value
  // overstate that other database's progress (issue #4824). We keep BOTH: a global Raft-log position
  // (the highest applied index across all databases, used by reinitialize()'s snapshot-gap check,
  // which compares against the inherently global Ratis snapshot index) AND a per-database map (used by
  // the per-database bootstrap replay-skip). The values live in memory so the hot apply path never
  // reads the file back; the file is parsed once lazily on first access and serialised on each write.
  // globalAppliedIndex tracks the same value as lastAppliedIndex (the AtomicLong above) on the apply
  // path. They are seeded independently (this one from the persisted file on load, lastAppliedIndex
  // from the Ratis snapshot in reinitialize()) and can briefly differ after reinitialize() - e.g. when
  // there is no snapshot lastAppliedIndex is -1 while globalAppliedIndex may hold the persisted value -
  // but every applyTransaction advances both to the same index, reconverging them.
  final Map<String, Long>         appliedIndexByDb     = new ConcurrentHashMap<>();

  // The per-database positions as this instance found them on disk (issue #11946): how far the previous session of this node
  // applied each database's entries, i.e. what the copies it left behind already hold. appliedIndexByDb moves on with every
  // entry this session applies, the replay included, so it cannot tell a replayed entry from a new one. A database dropped or
  // created in this session leaves the map: its copy is not the one the position describes.
  private final Map<String, Long> appliedIndexByDbOfPreviousSession = new ConcurrentHashMap<>();

  volatile long                      globalAppliedIndex   = -1;

  private volatile boolean                   appliedIndexLoaded   = false;

  final Object                    appliedIndexFileLock = new Object();

  // The index of the INSTALL_DATABASE_ENTRY that created the copy of each database this node holds (issue #11919), or for
  // a copy acquired from the leader the index after the one the leader served it at. An entry naming the database at a
  // LOWER index belongs to an incarnation a drop removed or is already in the copy: a replay from a snapshot marker older
  // than the drop must not apply it to the copy the later create made. Kept in a file of its own, .raft/created-index,
  // fsynced on every change: the applied-index file is rewritten without an fsync on every entry, and a power loss can
  // leave that rewrite empty. Guarded by appliedIndexFileLock and evicted by a drop with the per-database applied index.
  private final Map<String, Long> createdIndexByDb = new ConcurrentHashMap<>();

  // Whether the last change of createdIndexByDb could not be written (issue #11919): the next applied entry writes it again
  private volatile boolean createdIndexFilePending = false;

  // Which writes of the applied-index file are fsynced (issue #11517), both guarded by appliedIndexFileLock: whether
  // the file on disk carries a quarantine or a read floor (so the write that clears them must be durable too), and
  // whether this instance has made one durable write yet (the file's existence is what the offline-bootstrap gate reads)
  private          boolean                   appliedIndexFileCarriesQuarantine = false;

  private          boolean                   appliedIndexFileWrittenDurably    = false;

  AppliedIndexFile(final ArcadeStateMachine sm) {
    this.sm = sm;
  }

  /**
   * Returns the GLOBAL persisted applied index: the highest Raft-log index applied across all
   * databases multiplexed on this state machine, or {@code -1} if none was persisted. This is a
   * Raft-log position, not a per-database guarantee; {@link ArcadeStateMachine#reinitialize()} compares it against the
   * (inherently global) Ratis snapshot index. Package-private for tests.
   */
  long readPersistedAppliedIndex() {
    ensureAppliedIndexLoaded();
    return globalAppliedIndex;
  }

  /**
   * The in-memory applied counter {@link ArcadeStateMachine#takeSnapshot()} checkpoints from. This - not
   * {@link #readPersistedAppliedIndex()} - is the value that decides what a restarted node replays, because the
   * replay position comes solely from the snapshot marker and the marker comes from here. Package-private for
   * tests; see {@code ha-raft/CLAUDE.md} on why the two must not be confused.
   */
  // @VisibleForTesting
  long readAppliedIndexCounter() {
    return sm.lastAppliedIndex.get();
  }

  /**
   * Returns the persisted applied index for a single {@code dbName}, or {@code -1} when there is no
   * per-database evidence that this database was advanced (issue #4824). A legacy plain-number file
   * carries only the global value and therefore yields {@code -1} here: per-database decisions never
   * fall back to the global value, so a co-located database can never falsely satisfy them.
   * Package-private for tests.
   */
  long readPersistedAppliedIndex(final String dbName) {
    if (dbName == null)
      return -1;
    ensureAppliedIndexLoaded();
    final Long v = appliedIndexByDb.get(dbName);
    return v != null ? v : -1;
  }

  /**
   * Whether the entry at {@code entryIndex} of {@code dbName} was applied by the previous session of this node to the copy on
   * disk (issue #11946): a restart replays the log from the snapshot marker, so every entry up to the position that session
   * reached comes back. The copy already holds what such an entry did, and possibly more: a schema document it carries is OLDER
   * than the one on disk. {@code -1} positions and databases this session dropped or created never match.
   * <p>
   * A targeted resync of a quarantined copy does not forget the position: the copy it installs is the leader's, which holds
   * every entry up to the position and more, so an entry at or below it is still already applied.
   */
  boolean appliedByPreviousSession(final String dbName, final long entryIndex) {
    if (dbName == null)
      return false;
    ensureAppliedIndexLoaded();
    final Long applied = appliedIndexByDbOfPreviousSession.get(dbName);
    return applied != null && entryIndex <= applied;
  }

  /** The copy of {@code dbName} is dropped or created by this session: the position the previous session reached is not its. */
  void forgetPositionOfPreviousSession(final String dbName) {
    if (dbName == null)
      return;
    ensureAppliedIndexLoaded();
    appliedIndexByDbOfPreviousSession.remove(dbName);
  }

  /**
   * Records {@code index} as the global applied position and, when {@code dbName} is non-null, as the
   * per-database applied position for that database, then serialises the bookkeeping to disk.
   * Package-private for tests.
   * <p>
   * Synchronised on {@link #appliedIndexFileLock}: the apply thread and the snapshot-install thread
   * (see {@link #writePersistedAppliedIndexForAllDatabases}) are the two writers, and the lock keeps
   * the in-memory update and the temp-file write+rename atomic with respect to each other so the
   * shared {@code applied-index.tmp} is never raced.
   */
  void writePersistedAppliedIndex(final long index, final String dbName) {
    synchronized (appliedIndexFileLock) {
      ensureAppliedIndexLoaded();
      globalAppliedIndex = index;
      if (dbName != null)
        appliedIndexByDb.put(dbName, index);
      persistAppliedIndexFile();
    }
  }

  /**
   * Records {@code index} against {@code dbName} only (or evicts it, for a drop), leaving the global applied position
   * where it is (issue #8651): for an entry below a Raft install's boundary, whose global position is the install's.
   */
  void writePersistedDatabaseAppliedIndex(final long index, final String dbName, final boolean drop) {
    if (dbName == null || dbName.isEmpty())
      return;
    synchronized (appliedIndexFileLock) {
      ensureAppliedIndexLoaded();
      if (drop) {
        appliedIndexByDb.remove(dbName);
        evictCreatedIndex(dbName);
      } else
        appliedIndexByDb.put(dbName, index);
      persistAppliedIndexFile();
    }
  }

  /**
   * Advances the global applied position to {@code index} and, in the SAME serialised write, evicts
   * {@code dbName} from the per-database map. Used for a {@code DROP_DATABASE_ENTRY}: the database is
   * gone, so its per-database entry must not linger and grow the map/persisted JSON for the node
   * lifetime (issue #4824). Folding the global advance and the eviction into one atomic write avoids
   * a crash window that could leave a stale per-database entry for a database that no longer exists.
   * Package-private for tests.
   */
  void writePersistedAppliedIndexDroppingDatabase(final long index, final String dbName) {
    synchronized (appliedIndexFileLock) {
      ensureAppliedIndexLoaded();
      globalAppliedIndex = index;
      if (dbName != null) {
        appliedIndexByDb.remove(dbName);
        // The copy the create made is gone: a create of the same name starts the next incarnation (issue #11919)
        evictCreatedIndex(dbName);
      }
      persistAppliedIndexFile();
    }
  }

  /**
   * Records that the {@code INSTALL_DATABASE_ENTRY} at {@code index} made the copy of {@code dbName} this node holds (issue
   * #11919), the boundary {@link #isOfEarlierIncarnation} tells a replayed entry of a dropped incarnation by. A replay of
   * the same create records nothing new. Written durably to {@code .raft/created-index}, which no per-entry write touches:
   * the boundary has to survive whatever restart replays the drop before it. A failed write keeps the boundary in memory,
   * where it already protects this run, and is written again by the next applied entry (see
   * {@link #retryCreatedIndexFile()}).
   */
  void recordDatabaseCreated(final long index, final String dbName) {
    synchronized (appliedIndexFileLock) {
      ensureAppliedIndexLoaded();
      final Long previous = createdIndexByDb.get(dbName);
      if (previous != null && previous >= index)
        return;
      createdIndexByDb.put(dbName, index);
      persistCreatedIndexFile();
    }
  }

  /** Forgets the creation boundary of a dropped database (issue #11919), on disk too. Called under appliedIndexFileLock. */
  private void evictCreatedIndex(final String dbName) {
    if (createdIndexByDb.remove(dbName) != null)
      persistCreatedIndexFile();
  }

  /** Writes the creation boundaries again after a failed write (issue #11919). */
  void retryCreatedIndexFile() {
    synchronized (appliedIndexFileLock) {
      if (createdIndexFilePending)
        persistCreatedIndexFile();
    }
  }

  /** Whether a change of the creation boundaries is still to be written (issue #11919). */
  boolean isCreatedIndexFilePending() {
    return createdIndexFilePending;
  }

  /**
   * Whether the entry at {@code index} naming {@code dbName} is below the creation boundary of the copy this node holds
   * (issue #11919): of an incarnation a drop removed before the create that made the copy, or, for a copy acquired from
   * the leader, already in it. A replay of the log from a snapshot marker older than that drop gets here with such an
   * entry, and so do the entries between an acquired copy's install boundary and the index the leader served it at.
   */
  boolean isOfEarlierIncarnation(final String dbName, final long index) {
    ensureAppliedIndexLoaded();
    final Long created = createdIndexByDb.get(dbName);
    return created != null && index < created;
  }

  /** The creation boundary of {@code dbName}, or null when none is recorded (issue #11919). */
  Long createdIndex(final String dbName) {
    return createdIndexByDb.get(dbName);
  }

  /**
   * Writes {@link #createdIndexByDb} to {@code .raft/created-index} with an fsync (issue #11919). Called under
   * appliedIndexFileLock. A failure is logged at WARNING and leaves {@link #createdIndexFilePending} set: until a write
   * succeeds, a crash loses the boundaries recorded since the last good one, and a restart replaying a drop older than
   * them falls back to applying the dropped incarnation's entries, which the later create then rebuilds the copy over.
   */
  private void persistCreatedIndexFile() {
    if (sm.closed)
      return; // see close(): a task that outlived the shutdown must not recreate the file
    final Path file = getCreatedIndexFile();
    if (file == null)
      return;
    final JSONObject json = new JSONObject();
    for (final Map.Entry<String, Long> entry : createdIndexByDb.entrySet())
      json.put(entry.getKey(), entry.getValue().longValue());
    try {
      Files.createDirectories(file.getParent());
      FileUtils.atomicWriteFile(file.toFile(), json.toString());
      createdIndexFilePending = false;
    } catch (final Exception e) {
      if (!createdIndexFilePending)
        LogManager.instance().log(sm, Level.WARNING,
            "Could not write %s: %s. Retried with every applied entry; until it succeeds, a restart that replays the log "
                + "from before a drop may apply the dropped incarnation's entries to the database created after it",
            file, e.getMessage());
      createdIndexFilePending = true;
    }
  }

  /**
   * Records {@code index} as the global applied position and as the per-database position for every
   * database currently present on this node, then serialises once. Used by the full state-machine
   * snapshot install, after which every present database is at {@code index}. Synchronised on
   * {@link #appliedIndexFileLock} so it never races the apply-thread writer on the in-memory state or
   * the shared temp file.
   */
  void writePersistedAppliedIndexForAllDatabases(final long index) {
    writePersistedAppliedIndexForAllDatabases(index, Set.of());
  }

  /**
   * Same, minus {@code excluded}: the databases a snapshot install gave up on are not at {@code index}, so recording
   * them there would make {@link ArcadeStateMachine#reinitialize()} skip exactly the replay that would have caught them up on the next
   * restart, and would silently launder a stale copy into "applied" (issue #6760). The GLOBAL position still
   * advances: it is the Raft-log position Ratis is being told about, and Ratis has been told.
   */
  void writePersistedAppliedIndexForAllDatabases(final long index, final Set<String> excluded) {
    synchronized (appliedIndexFileLock) {
      ensureAppliedIndexLoaded();
      globalAppliedIndex = index;
      if (sm.server != null)
        for (final String dbName : sm.server.getDatabaseNames())
          if (!excluded.contains(dbName))
            appliedIndexByDb.put(dbName, index);
      // A resync's position is rare and what the next reinitialize() compares the marker against (issue #11517)
      persistAppliedIndexFile(true);
    }
  }

  /**
   * Lazily parses the persisted applied-index file once into the in-memory cache. Accepts both the
   * new JSON document ({@code {"global": n, "db": {"name": n, ...}}}) and a legacy plain-number file
   * (read as the global value with an empty per-database map). A missing/unreadable file simply means
   * "nothing persisted yet" (-1) and still latches the cache as loaded.
   * <p>
   * When the file path cannot yet be resolved (no server wired, so {@code getAppliedIndexFile()} is
   * {@code null}) the cache is NOT latched, so a later call retries once the server is available and
   * a persisted file is no longer masked. In the current wiring {@code setServer(...)} always runs
   * before the first read, so this only guards against a future reordering.
   */
  void ensureAppliedIndexLoaded() {
    if (appliedIndexLoaded)
      return;
    synchronized (appliedIndexFileLock) {
      if (appliedIndexLoaded)
        return;
      final Path file = getAppliedIndexFile();
      if (file == null)
        return; // server not wired yet: do not latch, retry once the path is resolvable
      removeStrayAppliedIndexTempFiles(file);
      try {
        if (Files.exists(file)) {
          final String content = Files.readString(file).trim();
          if (!content.isEmpty()) {
            if (content.charAt(0) == '{') {
              final JSONObject json = new JSONObject(content);
              globalAppliedIndex = json.getLong("global", -1);
              final JSONObject perDb = json.getJSONObject("db", new JSONObject());
              for (final String name : perDb.keySet())
                appliedIndexByDb.put(name, perDb.getLong(name, -1));
              restorePersistedQuarantine(json.getJSONObject("quarantine", new JSONObject()));
              restorePersistedFloors(json.getJSONObject("floors", new JSONObject()));
              // Not for a copy the previous session quarantined or left behind an install: its position says what the log
              // reached, not what the copy holds
              for (final Map.Entry<String, Long> entry : appliedIndexByDb.entrySet())
                if (!sm.divergedDatabases.containsKey(entry.getKey()) && !sm.staleDatabaseAppliedFloors.containsKey(entry.getKey()))
                  appliedIndexByDbOfPreviousSession.put(entry.getKey(), entry.getValue());
              // The write that clears what was restored must be durable, or a power loss brings it back (issue #11517)
              appliedIndexFileCarriesQuarantine = !sm.divergedDatabases.isEmpty() || !sm.staleDatabaseAppliedFloors.isEmpty();
            } else
              // Legacy format: a single plain number is the global Raft-log position.
              globalAppliedIndex = Long.parseLong(content);
          }
        }
      } catch (final Exception e) {
        LogManager.instance().log(sm, Level.FINE, "Could not read persisted applied index: %s", e.getMessage());
      } finally {
        loadCreatedIndexFile();
        // The path was resolvable and we attempted a read: latch even on a parse failure so a corrupt
        // file is not re-read on every apply (it degrades to -1, re-running the idempotent verification).
        // Deliberate coupling: a corrupt file leaving globalAppliedIndex at -1 also makes
        // reinitialize()'s snapshot-gap check (persistedApplied >= 0 && ...) evaluate false, i.e. it
        // suppresses the "snapshot ahead, download from leader" path. This matches the pre-change
        // behavior (a parse failure already returned -1), so it is intentional, not a regression.
        appliedIndexLoaded = true;
      }
    }
  }

  /**
   * Deletes the {@code applied-index.<random>.tmp} files a crash inside {@link FileUtils#atomicWriteFile(File, String)}
   * leaves behind (issue #11517). Called once, from {@link #ensureAppliedIndexLoaded()} under {@link #appliedIndexFileLock},
   * which every writer of the file holds, so no write of this instance is in flight. Best effort: a temporary that cannot
   * be listed or deleted is harmless, nothing reads it.
   */
  private void removeStrayAppliedIndexTempFiles(final Path file) {
    final Path dir = file.getParent();
    if (dir == null || !Files.isDirectory(dir))
      return;
    try (final DirectoryStream<Path> stray = Files.newDirectoryStream(dir, file.getFileName() + ".*.tmp")) {
      for (final Path tmp : stray)
        Files.deleteIfExists(tmp);
    } catch (final IOException | DirectoryIteratorException e) {
      LogManager.instance().log(sm, Level.FINE, "Could not remove stray applied-index temporary files: %s", e.getMessage());
    }
  }

  /**
   * Seeds {@link ArcadeStateMachine#divergedDatabases} from the {@code quarantine} object of the persisted applied-index file
   * (issue #7735). Called from {@link #ensureAppliedIndexLoaded()} while it holds {@link #appliedIndexFileLock},
   * which is the same lock every quarantine mutation takes, so a mark or a clear can neither race the restore nor
   * be undone by it.
   * <p>
   * <b>Why it has to be persisted at all.</b> A quarantine deliberately does NOT advance
   * {@link ArcadeStateMachine#lastAppliedIndex} for the entry that tripped it - {@code applyTransaction} returns a failed future
   * before the {@code getAndSet} - while every later entry does. The in-memory mark was the only thing standing
   * between that and a node that restarts, forgets, reports {@code alerts: []} and serves a database permanently
   * missing a committed mutation.
   * <p>
   * {@code putIfAbsent}, mirroring the bootstrap-baseline loader. Every quarantine mutation loads before it
   * mutates, so in practice the restored cause is already in the map when a later mark arrives and the FIRST
   * cause wins across the restart exactly as it does within a run (issue #7741): the restored one describes
   * the failure that quarantined the database, while a mark after it comes from an entry that hit the same
   * wall on a database already waiting for a resync.
   * <p>
   * An unrecognised cause name - written by a newer build - degrades to {@link DivergenceCause#APPLY_ERROR}
   * rather than dropping the entry: the cause only changes what the alert SAYS, while dropping it would
   * reinstate exactly the silent divergence this exists to prevent.
   */
  private void restorePersistedQuarantine(final JSONObject quarantine) {
    for (final String name : quarantine.keySet()) {
      final String causeName = quarantine.getString(name, null);
      DivergenceCause cause = DivergenceCause.APPLY_ERROR;
      if (causeName != null)
        try {
          cause = DivergenceCause.valueOf(causeName);
        } catch (final IllegalArgumentException e) {
          LogManager.instance().log(sm, Level.FINE,
              "Unknown divergence cause '%s' persisted for database '%s'; keeping the quarantine under %s",
              causeName, name, cause);
        }
      if (sm.divergedDatabases.putIfAbsent(name, cause) == null)
        LogManager.instance().log(sm, Level.SEVERE,
            "Database '%s' is still quarantined from a previous run (%s): this node refuses readiness and will "
                + "resync it from the leader rather than serve a copy that is missing a committed entry (issue #7735)",
            name, cause.getDescription());
    }
  }

  /**
   * Seeds {@link ArcadeStateMachine#staleDatabaseAppliedFloors} from the {@code floors} object of the persisted applied-index file
   * (issue #8137). Called from {@link #ensureAppliedIndexLoaded()} right after the quarantine is restored, under
   * {@link #appliedIndexFileLock}.
   * <p>
   * A floor only means something beside its quarantine, so one whose database is no longer quarantined is ignored.
   * A database quarantined by an incomplete snapshot install but carrying no floor - a file written by a build that
   * persisted the quarantine (#7735) but not yet the floor - is clamped at its own persisted applied position, which
   * is where {@code settleDivergedStateAfterInstall} put the floor in the first place: the install deliberately
   * left that position behind for it.
   */
  private void restorePersistedFloors(final JSONObject floors) {
    for (final String name : floors.keySet())
      if (sm.divergedDatabases.containsKey(name))
        sm.staleDatabaseAppliedFloors.putIfAbsent(name, Math.max(0L, floors.getLong(name, 0L)));

    for (final Map.Entry<String, DivergenceCause> entry : sm.divergedDatabases.entrySet())
      if (entry.getValue() == DivergenceCause.SNAPSHOT_INSTALL_INCOMPLETE) {
        final Long applied = appliedIndexByDb.get(entry.getKey());
        sm.staleDatabaseAppliedFloors.putIfAbsent(entry.getKey(), Math.max(0L, applied != null ? applied : -1L));
      }

    for (final Map.Entry<String, Long> entry : sm.staleDatabaseAppliedFloors.entrySet())
      LogManager.instance().log(sm, Level.SEVERE,
          "Database '%s' is still behind a snapshot install from a previous run: clamping its LINEARIZABLE / "
              + "read-your-writes reads at appliedIndex=%d until a resync succeeds (issue #8137)",
          entry.getKey(), entry.getValue());
  }

  /**
   * Serialises the in-memory applied-index bookkeeping to {@code .raft/applied-index} via a temp file
   * and atomic rename, so a crash mid-write never leaves a corrupt file.
   * <p>
   * Called once per applied entry (the file was already rewritten every apply before this change).
   * The per-database map is tiny (one entry per co-located database) and the small JSON it allocates
   * is dominated by the {@code createDirectories} + {@code writeString} + atomic {@code move} syscalls
   * that already ran every apply, so the extra allocation is negligible on the apply path.
   * <p>
   * <b>Which writes are fsynced (issue #11517).</b> The rename is atomic against a process crash, not against a power
   * loss: without an fsync of the content and of the directory the file can come back empty (a Jepsen LazyFS run found it
   * at 0 bytes) or as an older copy. An fsync per write would put two fsyncs per Raft entry on the apply thread, so only
   * the writes a restart must not lose are durable, through {@link FileUtils#atomicWriteFile(File, byte[])}:
   * <ul>
   *   <li>every write while a quarantine or a read floor stands, and the write that clears the last of them. Every write
   *   carries the whole quarantine, and an unsynced rename over the durable copy can leave an empty file in its place, so
   *   one durable mark is not enough. Once the cleared state is durable, what a later unsynced write can lose is the
   *   position alone;</li>
   *   <li>the first write of this instance, so the file's existence, which {@link ArcadeStateMachine#hasNeverAppliedApplicationEntry()}
   *   reads, survives;</li>
   *   <li>{@code forceSync}: the positions recorded by a snapshot install, a resync and the operator's accept, and the write
   *   {@link ArcadeStateMachine#takeSnapshot()} makes before its marker.</li>
   * </ul>
   * Every other write - the per-entry position of a healthy node - stays best effort. What a power loss leaves of it is an
   * older position, which only makes {@link ArcadeStateMachine#reinitialize()} resync or re-verify more than it needed to, or an empty or
   * torn file, which {@link #ensureAppliedIndexLoaded()} reads as nothing persisted (-1, the marker is trusted) while its
   * existence still keeps the offline-bootstrap gate closed.
   */
  boolean persistAppliedIndexFile() {
    return persistAppliedIndexFile(false);
  }

  boolean persistAppliedIndexFile(final boolean forceSync) {
    if (sm.closed)
      return false; // see close(): a task that outlived the shutdown must not recreate the file
    try {
      final Path file = getAppliedIndexFile();
      if (file == null)
        return false;
      final JSONObject json = new JSONObject();
      json.put("global", globalAppliedIndex);
      final JSONObject perDb = new JSONObject();
      for (final Map.Entry<String, Long> entry : appliedIndexByDb.entrySet())
        perDb.put(entry.getKey(), entry.getValue());
      json.put("db", perDb);
      // The quarantine travels in the SAME atomic write as the applied position it qualifies (issue #7735), so a
      // crash can never leave a file that says "applied up to N" without saying "and database X was skipped on the
      // way". Omitted when nothing is quarantined, which is the overwhelmingly common case, so the file shape an
      // older build reads back is byte-for-byte what it wrote.
      if (!sm.divergedDatabases.isEmpty()) {
        final JSONObject quarantine = new JSONObject();
        for (final Map.Entry<String, DivergenceCause> entry : sm.divergedDatabases.entrySet())
          quarantine.put(entry.getKey(), entry.getValue().name());
        json.put("quarantine", quarantine);
      }
      // The read floors qualify the quarantine and go with it (issue #8137); omitted when none is outstanding
      if (!sm.staleDatabaseAppliedFloors.isEmpty()) {
        final JSONObject floors = new JSONObject();
        for (final Map.Entry<String, Long> entry : sm.staleDatabaseAppliedFloors.entrySet())
          floors.put(entry.getKey(), entry.getValue().longValue());
        json.put("floors", floors);
      }

      final boolean carriesQuarantine = !sm.divergedDatabases.isEmpty() || !sm.staleDatabaseAppliedFloors.isEmpty();
      if (forceSync || carriesQuarantine || appliedIndexFileCarriesQuarantine || !appliedIndexFileWrittenDurably) {
        FileUtils.atomicWriteFile(file.toFile(), json.toString());
        appliedIndexFileWrittenDurably = true;
      } else {
        Files.createDirectories(file.getParent());
        final Path tmp = file.resolveSibling("applied-index.tmp");
        Files.writeString(tmp, json.toString());
        Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
      }
      // Reached only when the write succeeded: a throw above leaves both flags as they were, so the next write is still
      // made durable whenever this one had to be
      appliedIndexFileCarriesQuarantine = carriesQuarantine;
      return true;
    } catch (final Exception e) {
      LogManager.instance().log(sm, Level.FINE, "Could not write persisted applied index: %s", e.getMessage());
      return false;
    }
  }

  /**
   * Rewrites the applied-index file with an fsync, for {@link ArcadeStateMachine#takeSnapshot()} (issue #11517). Nothing is written while no
   * position was ever recorded: creating the file is what closes the offline-bootstrap gate, and a checkpoint is not an
   * applied application entry.
   */
  void syncAppliedIndexFileBeforeMarker() {
    synchronized (appliedIndexFileLock) {
      ensureAppliedIndexLoaded();
      if (globalAppliedIndex >= 0)
        persistAppliedIndexFile(true);
    }
  }

  Path getAppliedIndexFile() {
    final Path raftDir = getRaftDir();
    return raftDir != null ? raftDir.resolve("applied-index") : null;
  }

  private Path getCreatedIndexFile() {
    final Path raftDir = getRaftDir();
    return raftDir != null ? raftDir.resolve("created-index") : null;
  }

  /**
   * Reads {@code .raft/created-index} into {@link #createdIndexByDb} (issue #11919), from {@link #ensureAppliedIndexLoaded()}
   * under appliedIndexFileLock. A missing, empty or unreadable file records no boundary: nothing is skipped for a database
   * until its next create, as before the file existed.
   */
  private void loadCreatedIndexFile() {
    final Path file = getCreatedIndexFile();
    if (file == null)
      return;
    removeStrayAppliedIndexTempFiles(file);
    try {
      if (!Files.exists(file))
        return;
      final String content = Files.readString(file).trim();
      if (content.isEmpty())
        return;
      final JSONObject json = new JSONObject(content);
      for (final String name : json.keySet())
        createdIndexByDb.put(name, json.getLong(name, -1));
    } catch (final Exception e) {
      LogManager.instance().log(sm, Level.WARNING, "Could not read %s: %s", file, e.getMessage());
    }
  }

  /**
   * The {@code .raft} state directory under the server database directory, or {@code null} when it
   * cannot yet be resolved (no server wired or no configured database directory). Shared by the
   * applied-index and bootstrap-baseline files so the two are guaranteed to be co-located.
   */
  Path getRaftDir() {
    if (sm.server == null)
      return null;
    final String dbDir = sm.server.getConfiguration().getValueAsString(
        GlobalConfiguration.SERVER_DATABASE_DIRECTORY);
    if (dbDir == null)
      return null;
    return Path.of(dbDir, ".raft");
  }
}
