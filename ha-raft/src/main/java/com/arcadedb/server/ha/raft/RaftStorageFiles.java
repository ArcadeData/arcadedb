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
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import java.io.File;
import java.util.logging.Level;
import org.apache.ratis.util.LifeCycle;

/**
 * The Raft storage of this node on disk: where it lives, whether it is kept across restarts, the marker files written
 * beside it (runtime join, snapshot-install hold, crash-loop escalation, cluster-token fingerprint), and the room left
 * on its volume, with the local snapshot that compacts the log when the volume runs short before a snapshot install.
 * <p>
 * Split out of {@link RaftHAServer}, which formats, starts and restarts Ratis on this storage.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class RaftStorageFiles {
  private final RaftHAServer ha;

  /**
   * Suffix of the file that records a crash-loop escalation across process restarts (issue #7736). It is a sibling
   * of this peer's Raft storage directory, named after it ({@code <raft-storage-dir>.crash-loop-escalated}), never a
   * file inside it: the divergence reformat of {@link RatisInPlaceRestart#restartRatis(boolean)} deletes that directory wholesale, and
   * reaches it automatically from the health monitor while an escalation record exists, so a record inside it was
   * silently lost with the storage and the next process start walked the whole restart/reformat ladder again
   * (issue #8380). The record describes this node's crash-loop history, not its log, so it has exactly three
   * deleters: {@link RaftHAServer#clearPersistedCrashLoopEscalation()}; {@link #discardNonPersistentRaftStorage(File)}, which
   * wipes it together with a storage that is not kept across starts ({@code arcadedb.ha.raftPersistStorage=false});
   * and an operator, by hand, to re-arm the automatic crash-loop recovery. See
   * {@link #crashLoopEscalationMarkerFile(File)}.
   */
  static final String CRASH_LOOP_ESCALATION_MARKER = "crash-loop-escalated";

  /** Budget of the log purge that precedes a database snapshot install (issue #7037): "not now" beats waiting. */
  private static final long PRE_INSTALL_SNAPSHOT_REQUEST_TIMEOUT_MS = 5_000L;

  private volatile boolean                   legacyRaftStorageWarningLogged = false;

  RaftStorageFiles(final RaftHAServer ha) {
    this.ha = ha;
  }

  /**
   * Two segments of room, or unknown: a volume reporting no total space does not implement {@code statfs} (a FUSE
   * mount such as LazyFS answers 0 for both numbers), so its "0 usable" is not "full" (issue #11487).
   */
  static boolean hasRoomForSegments(final long usable, final long total, final long segmentSize) {
    return total <= 0 || usable >= 2L * segmentSize;
  }

  /**
   * Whether free space on the Raft storage volume is below {@code arcadedb.ha.raftStorageMinFreeSpacePerc}. The
   * same arithmetic as the compaction scheduler's disk-pressure probe; {@code false} when the check is disabled
   * (0) or the volume cannot be sized, so pressure is never guessed.
   */
  boolean isRaftStorageUnderPressure() {
    final int minFreeSpacePerc = ha.configuration.getValueAsInteger(GlobalConfiguration.HA_RAFT_STORAGE_MIN_FREE_SPACE_PERC);
    if (minFreeSpacePerc <= 0)
      return false;
    final File volume = raftStorageVolume();
    if (volume == null)
      return false;
    final long total = volume.getTotalSpace();
    final long usable = volume.getUsableSpace();
    return total > 0 && usable >= 0 && (double) usable / (double) total * 100.0 < minFreeSpacePerc;
  }

  /**
   * Forces a local Raft snapshot with the smallest creation gap Ratis accepts, so every log segment below the
   * applied index becomes purgeable before a database snapshot is written onto the same volume (issue #7037).
   * Best-effort: a refused or timed-out request only means the segments stay until the next periodic tick.
   * <p>
   * Only when the Raft storage volume is actually under pressure (below {@code arcadedb.ha.raftStorageMinFreeSpacePerc},
   * the same threshold the periodic compaction escalates on): an ordinary resync on a volume with room must not pay a
   * round trip to the {@code StateMachineUpdater} for a purge that has nothing to reclaim.
   * <p>
   * A snapshot request is fulfilled by the {@code StateMachineUpdater} thread, and only while the state machine is
   * {@code RUNNING}, so it is skipped when it could not be served promptly: on the apply thread itself (a request
   * issued from an entry apply would wait on itself), while the state machine is paused or reloading (a
   * leader-driven install, which purges the log by itself once it lands), and it carries a short budget rather than
   * the scheduler's 30s one, because the install it precedes must not queue behind a long entry apply.
   */
  void compactRaftLogBeforeSnapshotInstall(final String databaseName) {
    if (!isRaftStorageUnderPressure())
      return;
    final ArcadeStateMachine sm = ha.stateMachine;
    // An install holding the #7958 install lock can have the apply thread - the one that serves this request - waiting
    // on it; the install purged before taking the lock (ArcadeStateMachine.installLeaderCopy).
    if (sm == null || sm.isApplyThread() || sm.isHoldingInstallApplyGate()
        || sm.getLifeCycleState() != LifeCycle.State.RUNNING) {
      LogManager.instance().log(ha, Level.FINE,
          "Skipping the Raft log purge before installing '%s': the state machine cannot serve a snapshot request now",
          databaseName);
      return;
    }
    final long index = ha.takeLocalSnapshot(RaftLogCompactionScheduler.DISK_PRESSURE_CREATION_GAP,
        PRE_INSTALL_SNAPSHOT_REQUEST_TIMEOUT_MS);
    if (index >= 0)
      HALog.log(ha, HALog.BASIC, "Raft log purged up to index %d before installing the snapshot of '%s'", index,
          databaseName);
  }

  /**
   * Where the runtime-join marker of the peer whose Raft storage is {@code raftStorageDir} lives: a sibling file of
   * that directory, never inside it, so the divergence reformat of {@link RatisInPlaceRestart#restartRatis(boolean)} - which deletes
   * the directory and rejoins the same membership - cannot delete the marker along with it (issue #8329). Ratis is
   * handed the storage directory itself ({@code RaftServerConfigKeys.setStorageDir}), not its parent.
   */
  static File runtimeJoinMarkerFile(final File raftStorageDir) {
    return new File(raftStorageDir.getAbsoluteFile().getParentFile(), raftStorageDir.getName() + ".joined-at-runtime");
  }

  /**
   * Where the snapshot-install hold of a node that did NOT join at runtime is kept while it is open (issue #8465): a
   * sibling of the Raft storage directory for the same reason as {@link #runtimeJoinMarkerFile}, and a file of its own
   * so reading it back never arms a static member (issue #7819).
   */
  static File snapshotInstallHoldMarkerFile(final File raftStorageDir) {
    return new File(raftStorageDir.getAbsoluteFile().getParentFile(),
        raftStorageDir.getName() + ".snapshot-install-hold");
  }

  /**
   * Where the crash-loop escalation record of the peer whose Raft storage is {@code raftStorageDir} lives (issues
   * #7736, #8380): a sibling file of that directory, never inside it, for the same reason as
   * {@link #runtimeJoinMarkerFile}: the divergence reformat of {@link RatisInPlaceRestart#restartRatis(boolean)} deletes the directory,
   * and the health monitor's in-memory view of the record ({@code crashLoopEscalationPersisted}) cannot see that.
   */
  static File crashLoopEscalationMarkerFile(final File raftStorageDir) {
    return new File(raftStorageDir.getAbsoluteFile().getParentFile(),
        raftStorageDir.getName() + "." + CRASH_LOOP_ESCALATION_MARKER);
  }

  /**
   * Where the fingerprint of the cluster token a node derived from its root password file is recorded (issue #10601): a
   * sibling of the Raft storage directory, like {@link #runtimeJoinMarkerFile}, so the divergence reformat of
   * {@link RatisInPlaceRestart#restartRatis(boolean)} does not take the record of the token this node ran with along with the log.
   */
  static File clusterTokenFingerprintFile(final File raftStorageDir) {
    return new File(raftStorageDir.getAbsoluteFile().getParentFile(), raftStorageDir.getName() + ".cluster-token-fingerprint");
  }

  /**
   * Wipes the Raft storage of a node that does not persist it across starts ({@code arcadedb.ha.raftPersistStorage}
   * false), together with the crash-loop escalation record kept beside it: that record describes a crash loop on the
   * storage being discarded, and the fresh log this start formats has not crash-looped yet. Before issue #8380 the
   * record lived inside the directory and went with it; moving it out must not make it outlive the storage here.
   */
  static void discardNonPersistentRaftStorage(final File raftStorageDir) {
    if (raftStorageDir.exists())
      deleteRecursive(raftStorageDir);
    // The token fingerprint describes the previous run of a node that keeps nothing across starts (issue #10601).
    final File fingerprint = clusterTokenFingerprintFile(raftStorageDir);
    if (fingerprint.exists() && !fingerprint.delete() && fingerprint.exists())
      LogManager.instance().log(RaftHAServer.class, Level.WARNING,
          "Cannot delete the cluster token fingerprint '%s' of the discarded Raft storage", fingerprint.getAbsolutePath());
    final File record = crashLoopEscalationMarkerFile(raftStorageDir);
    if (record.exists() && !record.delete() && record.exists())
      LogManager.instance().log(RaftHAServer.class, Level.WARNING,
          "Cannot delete the crash-loop escalation record '%s' of the discarded Raft storage: delete it by hand, or "
              + "this server will not re-arm the automatic crash-loop recovery (issue #8380)", record.getAbsolutePath());
  }

  File crashLoopEscalationMarker() {
    return crashLoopEscalationMarkerFile(cachedRaftStorageDir());
  }

  boolean hasExistingRaftStorage() {
    final File storageDir = getRaftStorageDir();
    if (!storageDir.exists())
      return false;
    final File[] subdirs = storageDir.listFiles(f -> f.isDirectory() && !ArcadeDBServer.FILESYSTEM_RECOVERY_DIRECTORY.equals(f.getName()));
    return subdirs != null && subdirs.length > 0;
  }

  File getRaftStorageDir() {
    final File dir = resolveRaftStorageDir(
        ha.configuration.getValueAsString(GlobalConfiguration.HA_RAFT_STORAGE_DIRECTORY),
        ha.configuration.getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY),
        ha.arcadeServer.getRootPath(), ha.localPeerId.toString());
    warnIfLegacyRaftStorage(dir);
    return dir;
  }

  /**
   * Resolves the on-disk Raft storage directory for a peer. Kept as a package-visible static so tests
   * and the server agree on the default location (issue #5272).
   * <p>
   * Precedence:
   * <ol>
   *   <li>an explicit {@code arcadedb.ha.raftStorageDirectory} is always honored - absolute decoupling,
   *       required for read-only-root-filesystem deployments;</li>
   *   <li>otherwise the default is {@code <databaseDirectory>/.raft-storage/raft-storage-<peerId>} so
   *       that persisting the database directory (which every durable deployment already does) also
   *       persists the Raft log. The pre-#5272 default was the server root path, a <i>sibling</i> of the
   *       database directory, which on Kubernetes lands on ephemeral disk while only {@code databases/}
   *       is on the PersistentVolume - so a pod recreation silently discarded all Raft state;</li>
   *   <li>backward compatibility: if the new default does not yet exist but a legacy
   *       {@code <serverRootPath>/raft-storage-<peerId>} directory does, the legacy location is reused, so
   *       an in-place upgrade of a deployment that persisted the server root path does not FORMAT a fresh
   *       empty log and self-inflict the very divergence this fix prevents. The caller emits a one-time
   *       WARNING recommending migration or an explicit {@code raftStorageDirectory}.</li>
   * </ol>
   */
  static File resolveRaftStorageDir(final String configuredRaftDir, final String databaseDirectory,
      final String serverRootPath, final String peerId) {
    final String subdir = "raft-storage-" + peerId;

    if (configuredRaftDir != null && !configuredRaftDir.isBlank())
      return new File(configuredRaftDir, subdir);

    final File newDefault = databaseDirectory != null && !databaseDirectory.isBlank()
        ? new File(new File(databaseDirectory, ".raft-storage"), subdir)
        : new File(serverRootPath, subdir);

    if (!newDefault.exists()) {
      final File legacy = new File(serverRootPath, subdir);
      if (legacy.exists() && !legacy.equals(newDefault))
        return legacy;
    }
    return newDefault;
  }

  /**
   * Emits a one-time WARNING when the resolved Raft storage directory is the legacy pre-#5272 location
   * (directly under the server root path) rather than the new default under the database directory.
   */
  private void warnIfLegacyRaftStorage(final File resolvedDir) {
    if (legacyRaftStorageWarningLogged)
      return;
    if (ha.configuration.getValueAsString(GlobalConfiguration.HA_RAFT_STORAGE_DIRECTORY) != null
        && !ha.configuration.getValueAsString(GlobalConfiguration.HA_RAFT_STORAGE_DIRECTORY).isBlank())
      return;
    final File rootPath = new File(ha.arcadeServer.getRootPath());
    final File parent = resolvedDir.getParentFile();
    if (parent != null && parent.equals(rootPath)) {
      legacyRaftStorageWarningLogged = true;
      LogManager.instance().log(ha, Level.WARNING,
          "Raft storage is using the legacy location '%s' (under the server root path). Set '%s' explicitly "
              + "or move it under the database directory ('%s/.raft-storage') so Raft state is persisted together "
              + "with the databases (issue #5272).",
          resolvedDir.getAbsolutePath(), GlobalConfiguration.HA_RAFT_STORAGE_DIRECTORY.getKey(),
          ha.configuration.getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY));
    }
  }

  /**
   * Decides whether the Raft storage directory must be preserved across server restarts instead of
   * being wiped and re-FORMATted on every {@link RaftHAServer#start()} (issue #4835).
   * <p>
   * {@link GlobalConfiguration#HA_RAFT_PERSIST_STORAGE} defaults to {@code true} (durable): wiping the
   * Raft log on restart turns a follower that was merely lagging into a permanently diverged node on a
   * full-cluster cold restart, and on a single-seed cluster it can silently re-form a fresh empty
   * single-node cluster (data loss / split brain). Persisting was previously the default only under
   * Kubernetes (where a PersistentVolume made it essential - issue #4835); it is now the default
   * everywhere, so the Kubernetes special case has collapsed into the global default.
   * <p>
   * An explicit {@code raftPersistStorage=false} is always honored for a throwaway/test cluster that
   * really wants ephemeral storage. A JVM system property is read directly because a
   * {@link ContextConfiguration} built after {@link GlobalConfiguration} was initialised does not
   * necessarily reflect a late {@code System.setProperty}.
   */
  static boolean resolvePersistStorage(final ContextConfiguration configuration) {
    if (isExplicitlyConfigured(configuration, GlobalConfiguration.HA_RAFT_PERSIST_STORAGE)) {
      final String sysProp = System.getProperty(GlobalConfiguration.HA_RAFT_PERSIST_STORAGE.getKey());
      if (sysProp != null) {
        // Parsed STRICTLY, through the same entry point every other reader of raw configuration text uses
        // (issue #7296). Boolean.parseBoolean, which this replaced, maps everything that is not "true" to
        // false - so `-Darcadedb.ha.raftPersistStorage=yes`, an operator affirming that the Raft log must
        // survive a restart, silently made the storage ephemeral instead, which on a full-cluster cold restart
        // is what turns a merely-lagging follower into a permanently diverged one. Refusing to guess is the
        // rule #7222 established for exactly this shape; here it also happens to fail safe, the default being
        // the durable side.
        final Object coerced = GlobalConfiguration.HA_RAFT_PERSIST_STORAGE.coerceFromConfigurationSource(sysProp,
            "system property '" + GlobalConfiguration.HA_RAFT_PERSIST_STORAGE.getKey() + "'");
        if (coerced instanceof Boolean b)
          return b;
        // Refused: keep the compiled-in default. Read WITHOUT a cast, and biased to the durable side rather than
        // to whatever a non-Boolean default would coerce to - a cast here would only turn a future change to this
        // setting's declared default into a ClassCastException on a recovery path, and "not explicitly false" is
        // the reading that cannot answer "wipe the Raft log" to a question nobody managed to ask.
        return !Boolean.FALSE.equals(GlobalConfiguration.HA_RAFT_PERSIST_STORAGE.getDefValue());
      }
      return configuration.getValueAsBoolean(GlobalConfiguration.HA_RAFT_PERSIST_STORAGE);
    }
    return GlobalConfiguration.HA_RAFT_PERSIST_STORAGE.getValueAsBoolean();
  }

  /**
   * Returns {@code true} when the operator explicitly provided a value for the given configuration
   * key, either through this {@link ContextConfiguration} (config file, programmatic set, or server
   * settings) or via a JVM system property. Used by {@link #resolvePersistStorage} to distinguish a
   * deliberate {@code raftPersistStorage=false} from the implicit default.
   */
  static boolean isExplicitlyConfigured(final ContextConfiguration configuration, final GlobalConfiguration key) {
    return configuration.hasValue(key.getKey()) || System.getProperty(key.getKey()) != null;
  }

  /**
   * The Raft storage directory, or its nearest existing ancestor. {@code File.getUsableSpace()}
   * returns 0 for a path that does not exist, which would otherwise read as "disk full" during the
   * window between server start and Ratis creating the directory.
   * <p>
   * The configured directory itself is resolved once and cached: {@code resolveRaftStorageDir} is
   * config-derived and constant for the server's lifetime, and re-running its {@code exists()} probes
   * on every tick is pointless I/O. The ancestor walk stays per-call because its result legitimately
   * changes once Ratis creates the directory. Shared by the compaction scheduler's disk-pressure probe and
   * the log-writer recovery's free-space gate (issue #7037), so the two cannot disagree on the volume; the walk
   * itself is {@link SnapshotInstaller#nearestExistingAncestor}, the same one the snapshot space check uses.
   */
  File raftStorageVolume() {
    return SnapshotInstaller.nearestExistingAncestor(cachedRaftStorageDir());
  }

  /** The configured Raft storage directory, resolved once: config-derived and constant for this server's lifetime. */
  private File cachedRaftStorageDir() {
    File dir = ha.cachedRaftStorageDir;
    if (dir == null) {
      dir = getRaftStorageDir();
      ha.cachedRaftStorageDir = dir;
    }
    return dir;
  }

  static void deleteRecursive(final File file) {
    if (file.isDirectory()) {
      final File[] children = file.listFiles();
      if (children != null)
        for (final File child : children)
          deleteRecursive(child);
    }
    file.delete();
  }
}
