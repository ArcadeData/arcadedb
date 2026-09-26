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

import com.arcadedb.log.LogManager;
import org.apache.ratis.protocol.RaftPeerId;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.logging.Level;

/**
 * Records whether THIS node was added to the Raft configuration by a configuration change it applied, as opposed
 * to having been a member from the first configuration it observed (issue #7819).
 * <p>
 * That is the event the security-convergence readiness gate of issue #7532 needs to tell apart two nodes that
 * look identical from their security documents alone: a peer admitted at runtime whose admission seed has not
 * landed, and a member of a cluster that has simply never replicated a security document. Both hold no
 * replicated fingerprint. Only the first one joined a configuration that did not contain it.
 * <p>
 * <b>Why it does not read the configuration the node was created with.</b> A peer about to be added is started
 * with the group its own {@code arcadedb.ha.serverList} declares, and on a StatefulSet scale-up or a
 * {@code connect cluster} target that list usually names the node itself - so "was I in my starting group" says
 * yes for the joiner and for a static member alike, and a gate armed on it would never fire. What does differ is
 * the configuration LOG: the cluster a peer joins already has configuration entries without it, and the entry that
 * adds it is a Ratis joint-consensus entry whose {@code oldPeers} is the membership before the change. Ratis
 * routes every {@code setConfiguration} - {@code addPeer}, {@code connect cluster}, the {@code Mode.ADD} of
 * {@code KubernetesAutoJoin} - through {@code LeaderStateImpl.applyOldNewConf}, so that entry always exists and
 * every member, the joiner included, applies it. So a node arms on either of two observations, both of which are
 * read from applied configurations and neither from the starting group:
 * <ul>
 *   <li>a joint entry that lists this node in its new peers and not in its non-empty old peers - the change that
 *       added it, self-describing, which needs no baseline at all; or</li>
 *   <li>a configuration that contains this node, following one observed earlier in this process that did not -
 *       the fallback for a joiner that fell behind and caught up by a snapshot install carrying the final
 *       configuration rather than the joint entry.</li>
 * </ul>
 * The first configuration a node observes never arms it through the second rule, so a member of a freshly formed
 * cluster - whose first observed configuration is the leader's startup entry and names it - stays unarmed, as does
 * a statically configured node that restarts: every configuration it replays names it.
 * <p>
 * <b>Replay on restart is inert, not suppressed.</b> A node that was added at runtime and later restarts replays
 * the joint entry that added it, for as long as that entry has not been compacted into a snapshot, and arms again.
 * The gate that reads this waits for security documents installed by entries after that joint entry, and a log
 * that still holds the joint entry also holds every entry after it - the seed that converged the node the first
 * time included - so the replay converges it again by the time it has caught up. One that never converged is held
 * again, which is the honest answer for it: it is still enforcing its own copy.
 * Suppressing the replay instead would need the log index this process started from, which Ratis does not hand
 * the state machine before it starts applying.
 * <p>
 * <b>Persisted, because the replay does not always happen (issue #8329).</b> A joiner that restarts AFTER the entry
 * that added it was compacted into a snapshot observes only configurations containing itself, so neither rule can
 * fire again. When constructed with a marker file, the detector writes it the moment it arms and a later process
 * reading it back starts armed. The marker also carries the join index and each document's install index (see
 * below), rewritten on every change of either, because a restart after compaction replays neither the join nor the
 * installs that converged it: without them, a converged joiner would be held for the whole readiness window on every
 * restart. {@link RaftHAServer} places the marker NEXT TO the Raft storage directory rather
 * than inside it, so the divergence reformat of {@code RaftHAServer.restartRatis(true)} - which deletes that
 * directory - does not take the marker with it. A marker that outlives convergence is inert, because the install
 * indexes it carries follow its join index. A marker that could not be
 * written is logged and leaves the in-memory arm in place; a restart after compaction then comes back unarmed,
 * which is the behaviour before issue #8329, not a new failure.
 * <p>
 * <b>Ordering between the two callers.</b> The monitor makes each observation atomic, not the two Ratis call
 * sites ordered with respect to each other, so the snapshot-install callback can land a configuration older than
 * one the apply loop has already delivered. That cannot produce a false arm on a static member, because no
 * configuration it has ever been part of lacks it; and it cannot undo an arm, because arming latches. The worst
 * it can do is let the second rule see "without me" last and arm on the next configuration that names this node -
 * which is only ever true of a node that really was outside the configuration, i.e. a joiner.
 * <p>
 * Never cleared. Once a node has joined at runtime the gate is armed for the rest of the process - and, with a
 * marker, for the life of that marker - and what releases it is convergence (or the gate's own bounded window),
 * not a later configuration.
 * <p>
 * <b>Convergence is measured from the join, not from the fingerprints (issue #8317).</b> A recorded replicated
 * fingerprint only says that the cluster installed a document here at some point. A node removed from the cluster
 * and re-added with its config volume retained holds one for every document, from its PREVIOUS membership, and
 * would pass a gate that asked nothing else while enforcing a user dropped, a group narrowed or a token revoked
 * while it was out. So each arming also records the log index of the configuration that added this node, the
 * state machine reports the index of every security document it installs from the log, and a document counts as
 * converged only when it was installed by an entry AFTER that join index - which, since the leader seeds a peer
 * only once the configuration adding it has committed (issue #7531), is exactly the seed or a later change. A
 * re-add moves the join index forward even though the node is already armed, so installs from the previous
 * membership stop counting at that point. The index only moves forward: a snapshot-install callback delivering
 * an older configuration cannot pull it back.
 * <p>
 * <b>A leader-driven snapshot install is a join boundary of its own (issue #8353).</b> Both rules above read the
 * configuration log, and a node removed from the cluster WHILE IT WAS DOWN, then re-added with its config volume
 * retained, can reach the leader again without observing either: it never received the removal entry, so every
 * configuration it replays after its restart names it, and the joint entry that re-added it lies below the leader's
 * compaction point, so it is never applied - the snapshot install delivers only the final configuration, which names
 * it too. The join index then stays at its FIRST join and the previous membership's installs keep counting. A node
 * cannot tell that case apart from a member that merely lagged past the compaction point, so every leader-driven
 * install on an armed node is treated as one: {@link #onSnapshotInstalledFromLeader(long)} moves the join index to
 * just below the installed snapshot index and forgets every install recorded before it. That is sound for both: no
 * security document travels in a snapshot, so a lagging member that skipped the entries really may be enforcing a
 * stale copy too. What releases it is the same evidence as a seeded joiner: a seed or later change applied from the
 * log, or the leader-confirmed match {@code SecurityCatchUp} asks for right after every install, read at an applied
 * index at or past the snapshot.
 * <p>
 * Owned by {@link RaftHAServer} rather than by a state machine, so the answer survives the in-place Ratis restart
 * of {@code RaftHAServer.restartRatis}, which builds a new {@link ArcadeStateMachine}.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
public final class RuntimeJoinDetector {

  /** The security documents whose installs are tracked, as {@link #onSecurityDocumentInstalled} takes them. */
  public static final int USERS      = 0;
  public static final int GROUPS     = 1;
  public static final int API_TOKENS = 2;

  /**
   * The names the readiness gate reports, in the order {@code ServerSecurity.unconvergedClusterSecurityDocuments()}
   * reports its own, indexed by the constants above.
   */
  private static final String[] DOCUMENT_NAMES = { "users", "groups", "API tokens" };

  /** Stands for "no index known": no join recorded, or no install of that document observed. */
  private static final long NO_INDEX = -1L;

  /** The marker keys of the join index and of each document's install index, in {@link #DOCUMENT_NAMES} order. */
  private static final String   JOIN_INDEX_KEY     = "joinIndex";
  private static final String[] INSTALLED_KEYS     = { "installed.users", "installed.groups", "installed.apiTokens" };

  /** Where the armed state is persisted; {@code null} keeps it in memory only. */
  private final    File    marker;
  /** Serializes marker writes, which run outside this instance's monitor. */
  private final    Object  persistLock        = new Object();
  /** Bumped under the monitor on every change the marker records; the writer skips a snapshot older than it wrote. */
  private          long    stateVersion;
  /** The {@link #stateVersion} of the last marker written; guarded by {@link #persistLock}. */
  private          long    persistedVersion   = -1L;
  /** The peer id the marker names, set when this detector arms. */
  private volatile String  armedPeer;
  /** Whether the last configuration observed contained this node; {@code null} before the first one. */
  private          Boolean lastObservedMembership;
  private volatile boolean joinedAtRuntime;
  /** The log index of the latest configuration that added this node; guarded by this instance's monitor. */
  private          long    joinIndex          = NO_INDEX;
  /** Per document, the highest log index it was installed at; guarded by this instance's monitor. */
  private final    long[]  lastInstalledIndex = { NO_INDEX, NO_INDEX, NO_INDEX };

  /**
   * {@link #onConfiguration(RaftPeerId, Collection, Collection, long)} for a configuration whose log index is not
   * known. The join it records has no position, so every install this detector records counts as following it.
   */
  public boolean onConfiguration(final RaftPeerId self, final Collection<RaftPeerId> peers,
      final Collection<RaftPeerId> oldPeers) {
    return onConfiguration(self, peers, oldPeers, NO_INDEX);
  }

  /** A detector whose armed state lives only as long as this instance. */
  public RuntimeJoinDetector() {
    this(null, false);
  }

  /**
   * A detector that persists its armed state in {@code marker} (issue #8329).
   *
   * @param marker  the file written when this detector arms; {@code null} keeps the state in memory only
   * @param restore {@code true} to start armed when {@code marker} already exists - the process restart of a node
   *                that joined at runtime. {@code false} discards an existing marker instead: the owner is starting
   *                from Raft state it does not keep across restarts, so nothing it recorded about a previous
   *                membership still describes this node
   */
  public RuntimeJoinDetector(final File marker, final boolean restore) {
    this.marker = marker;
    if (marker == null || !marker.exists())
      return;

    if (restore) {
      joinedAtRuntime = true;
      restoreIndexes(marker);
      LogManager.instance().log(this, Level.INFO,
          "This peer joined the Raft configuration at runtime in an earlier run (%s): readiness waits for the cluster "
              + "security documents to reach it (arcadedb.ha.securityConvergenceReadinessTimeout)",
          marker.getAbsolutePath());
    } else if (!marker.delete())
      LogManager.instance().log(this, Level.WARNING, "Could not delete the stale runtime-join marker %s",
          marker.getAbsolutePath());
  }

  /**
   * Called for every configuration this node applies. Never throws and never blocks beyond this instance's own
   * monitor: it runs on a Ratis callback thread, and the two Ratis call sites (the apply loop and a
   * leader-initiated snapshot install) can arrive concurrently.
   *
   * @param self     this node's peer id; nothing is recorded when it is {@code null} (a state machine that has
   *                 not been initialized by Ratis yet cannot say which of the peers is itself)
   * @param peers    the peers of the applied configuration
   * @param oldPeers the old peers of a joint-consensus configuration, empty for a final one
   * @param index    the log index of the configuration, which becomes the join index when it added this node
   *
   * @return {@code true} when THIS call armed the detector, or re-armed it on a later join (a re-add)
   */
  public boolean onConfiguration(final RaftPeerId self, final Collection<RaftPeerId> peers,
      final Collection<RaftPeerId> oldPeers, final long index) {
    if (self == null)
      return false;

    final boolean member = peers.contains(self);
    final boolean addedByThisEntry = member && !oldPeers.isEmpty() && !oldPeers.contains(self);

    final boolean armedNow;
    final boolean rearmedNow;
    synchronized (this) {
      final boolean addedSinceLastObservation = member && Boolean.FALSE.equals(lastObservedMembership);
      lastObservedMembership = member;
      final boolean added = addedByThisEntry || addedSinceLastObservation;
      armedNow = added && !joinedAtRuntime;
      // A re-add of a node that is already armed: the installs it holds from before this index belong to its
      // previous membership (issue #8317). Only ever forward, so an older configuration cannot undo it.
      rearmedNow = added && joinedAtRuntime && index > joinIndex;
      if (armedNow) {
        joinedAtRuntime = true;
        joinIndex = index;
      } else if (rearmedNow)
        joinIndex = index;
      if (armedNow || rearmedNow)
        stateVersion++;
    }

    if (armedNow)
      LogManager.instance().log(this, Level.INFO,
          "Peer %s was added to the Raft configuration while running: readiness waits for the cluster security "
              + "documents to reach it (arcadedb.ha.securityConvergenceReadinessTimeout)", self);
    else if (rearmedNow)
      LogManager.instance().log(this, Level.INFO,
          "Peer %s was added to the Raft configuration again at index %d: the security documents it installed "
              + "before no longer count, and readiness waits for the cluster's current ones to reach it "
              + "(arcadedb.ha.securityConvergenceReadinessTimeout)", self, index);
    if (armedNow || rearmedNow) {
      armedPeer = self.toString();
      // Outside the monitor: the readiness probe reads it, and must not wait on a SYNC write.
      persist();
    }
    return armedNow || rearmedNow;
  }

  /**
   * Records that the state machine installed {@code document} from the replicated log entry at {@code index}.
   * Called on the apply thread for every install, armed or not, so a join observed later still judges it by its
   * index. Cheap and non-blocking beyond this instance's monitor, except on a node that joined at runtime and has a
   * marker: there the new index is persisted, so a restart after the entry is compacted still knows the document
   * converged. Security documents change rarely, so that write is not on any hot path.
   *
   * @param document one of {@link #USERS}, {@link #GROUPS}, {@link #API_TOKENS}
   */
  public void onSecurityDocumentInstalled(final int document, final long index) {
    final boolean changed;
    synchronized (this) {
      changed = index > lastInstalledIndex[document];
      if (changed) {
        lastInstalledIndex[document] = index;
        if (joinedAtRuntime)
          stateVersion++;
      }
    }
    if (changed && joinedAtRuntime)
      persist();
  }

  /**
   * Records that the leader compared this node's security documents, read at applied index {@code appliedIndex},
   * against its own live ones and found all three equal (issue #8346). Recorded as an install of every document at
   * that index, so it counts toward convergence exactly when an install there would: only when it follows the join.
   * <p>
   * This is what releases a re-added node that caught up by snapshot install PAST its re-admission seed while holding
   * documents that already equal the cluster's. The seed entries are never applied on it, no snapshot carries the
   * security documents, and the leader - finding nothing to change - writes nothing, so no install after the join
   * would ever be observed and the gate would hold the node for its whole window before reporting documents that
   * "never reached" a node that had them all along.
   * <p>
   * Why the applied index and not the join index or "now": the caller reads the index BEFORE reading the fingerprints
   * it sends, and the apply loop installs a document before it advances the applied index, so the documents compared
   * include every entry up to that index. Equal to the leader's live documents, they are the cluster's as of at least
   * that position - the same claim an install at that index makes. A match read at or before the join therefore does
   * not count (it may be the previous membership's copy, compared before the change the join started), and a later
   * re-add discards it like any other install. Recording it against a position also keeps it correct when the
   * snapshot-install callback delivers the configuration that re-added this node only after the match was recorded.
   *
   * @param appliedIndex the applied index read before the compared fingerprints were; negative (unknown) records
   *                     nothing
   */
  public void onSecurityDocumentsMatchedLeader(final long appliedIndex) {
    if (appliedIndex < 0)
      return;
    final boolean changed;
    synchronized (this) {
      boolean any = false;
      for (int i = 0; i < lastInstalledIndex.length; i++)
        if (appliedIndex > lastInstalledIndex[i]) {
          lastInstalledIndex[i] = appliedIndex;
          any = true;
        }
      changed = any;
      if (changed && joinedAtRuntime)
        stateVersion++;
    }
    if (changed && joinedAtRuntime)
      persist();
  }

  /**
   * Records that a leader-driven snapshot install brought this node to {@code snapshotIndex} without applying the
   * log entries up to it (issue #8353). On a node that joined at runtime, every install recorded so far is
   * forgotten and the join index moves forward to {@code snapshotIndex - 1}, so only a seed or later change applied
   * from the log after the snapshot, or a leader-confirmed match read at the snapshot index or later, counts toward
   * convergence from here.
   * <p>
   * Why a boundary at all: the skipped range may hold the entry that removed this node and the joint entry that
   * re-added it, neither of which it will ever apply, so neither rule of {@link #onConfiguration} can see the re-add.
   * The installs are forgotten rather than only judged by index because they were all recorded BEFORE the install,
   * from the log this node had, which is precisely what the skipped range may have invalidated; judging by index
   * alone would keep one whose index happens to lie past the boundary - an entry of a log the divergence reformat of
   * {@code RaftHAServer.restartRatis(true)} since threw away, for one.
   * <p>
   * Why {@code snapshotIndex - 1} and not the snapshot index: the boundary must still let through the match
   * {@code SecurityCatchUp.afterSnapshotInstall} asks for right after the install, which is read at an applied index
   * of at least {@code snapshotIndex}. The documents compared there are this node's after the install, set against
   * the leader's live ones, so the match describes the current membership whatever index the re-add was at. A
   * boundary AT the snapshot index would refuse it and hold a node whose documents already equal the cluster's for
   * the whole readiness window - the case issue #8346 fixed. A match read concurrently at an applied index below the
   * snapshot, by the once-per-start catch-up, is at or below the boundary and does not count: the caller moves the
   * boundary before it advances the applied index.
   * <p>
   * Only forward, like every other move of the join index, and a no-op on a node that did not join at runtime: the
   * readiness gate is not armed there (issue #7819), and a snapshot install is no evidence of a runtime join.
   * Persisted, so a restart right after the install stays held until the documents are confirmed.
   *
   * @param snapshotIndex the log index the install brought this node to; not positive records nothing
   *
   * @return {@code true} when this changed what counts as converged
   */
  public boolean onSnapshotInstalledFromLeader(final long snapshotIndex) {
    if (snapshotIndex <= 0)
      return false;
    final long boundary = snapshotIndex - 1;
    boolean changed = false;
    synchronized (this) {
      if (joinedAtRuntime) {
        if (boundary > joinIndex) {
          joinIndex = boundary;
          changed = true;
        }
        for (int i = 0; i < lastInstalledIndex.length; i++)
          if (lastInstalledIndex[i] != NO_INDEX) {
            lastInstalledIndex[i] = NO_INDEX;
            changed = true;
          }
        if (changed)
          stateVersion++;
      }
    }
    if (changed) {
      LogManager.instance().log(this, Level.INFO,
          "Peer %s caught up by a snapshot install to index %d, which skips every log entry up to it, including any "
              + "that removed and re-added this peer: the security documents it installed before no longer count, "
              + "and readiness waits for the cluster's current ones to be confirmed "
              + "(arcadedb.ha.securityConvergenceReadinessTimeout)", armedPeer, snapshotIndex);
      persist();
    }
    return changed;
  }

  /**
   * The security documents this node has not installed from an entry after the configuration that (last) added
   * it, in the order users, groups, API tokens (issue #8317). Empty when this node did not join at runtime - the
   * gate is not armed there, see {@link #hasJoinedAtRuntime()}.
   */
  public List<String> securityDocumentsNotInstalledSinceJoin() {
    if (!joinedAtRuntime)
      return List.of();
    final List<String> awaited = new ArrayList<>(DOCUMENT_NAMES.length);
    synchronized (this) {
      for (int i = 0; i < DOCUMENT_NAMES.length; i++)
        if (lastInstalledIndex[i] == NO_INDEX || lastInstalledIndex[i] <= joinIndex)
          awaited.add(DOCUMENT_NAMES[i]);
    }
    return awaited;
  }

  /** The names {@link #securityDocumentsNotInstalledSinceJoin()} reports, all of them. */
  public static List<String> allSecurityDocumentNames() {
    return List.of(DOCUMENT_NAMES);
  }

  /**
   * The log index of the configuration that last added this node - or the join boundary a snapshot install moved it
   * to - {@code -1} when none did. Only ever moves forward. The readiness gate opens a fresh security-convergence
   * window when it does (issue #8414).
   */
  synchronized long joinIndex() {
    return joinIndex;
  }

  /**
   * Writes the marker: the arm, the join index and each document's install index. A failure is logged, never
   * thrown: this runs on a Ratis callback thread, and the in-memory state stays in place for the rest of the process
   * either way.
   * <p>
   * The content goes to a temporary file written with {@code SYNC} and is then renamed over the marker, so a restart
   * reads either the previous marker or the new one, never a torn one. The parent directory is not fsynced, so an OS
   * crash in the instant after a write can lose it; the restart then reads the previous marker, or none, which is
   * the behaviour before issue #8329.
   */
  private void persist() {
    if (marker == null)
      return;
    synchronized (persistLock) {
      final long version;
      final long join;
      final long[] installed;
      synchronized (this) {
        version = stateVersion;
        join = joinIndex;
        installed = lastInstalledIndex.clone();
      }
      // A concurrent caller already wrote this state or a newer one.
      if (version <= persistedVersion)
        return;

      final StringBuilder content = new StringBuilder(160);
      content.append("peer=").append(armedPeer).append('\n');
      content.append("armedAt=").append(System.currentTimeMillis()).append('\n');
      content.append(JOIN_INDEX_KEY).append('=').append(join).append('\n');
      for (int i = 0; i < INSTALLED_KEYS.length; i++)
        content.append(INSTALLED_KEYS[i]).append('=').append(installed[i]).append('\n');

      try {
        final File parent = marker.getAbsoluteFile().getParentFile();
        if (parent != null && !parent.isDirectory() && !parent.mkdirs() && !parent.isDirectory())
          throw new IOException("cannot create directory " + parent);
        final File temp = new File(marker.getAbsolutePath() + ".tmp");
        Files.writeString(temp.toPath(), content, StandardCharsets.UTF_8, StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE, StandardOpenOption.SYNC);
        Files.move(temp.toPath(), marker.toPath(), StandardCopyOption.REPLACE_EXISTING,
            StandardCopyOption.ATOMIC_MOVE);
        persistedVersion = version;
      } catch (final IOException | RuntimeException e) {
        LogManager.instance().log(this, Level.WARNING,
            "Could not persist the runtime-join marker %s: a restart after the Raft log is compacted past the entry "
                + "that added this peer will not hold readiness for the cluster security documents, or will hold it "
                + "for documents that already converged (%s)", marker.getAbsolutePath(), e.toString());
      }
    }
  }

  /**
   * Reads back the join index and the install indexes a previous run persisted (issues #8317, #8329). A marker that
   * predates them, or a value that does not parse, leaves that index unknown, which awaits the document: the safe
   * answer, bounded by the gate's own window.
   */
  private void restoreIndexes(final File marker) {
    final List<String> lines;
    try {
      lines = Files.readAllLines(marker.toPath(), StandardCharsets.UTF_8);
    } catch (final IOException | RuntimeException e) {
      LogManager.instance().log(this, Level.WARNING,
          "Could not read the runtime-join marker %s: readiness waits for every cluster security document (%s)",
          marker.getAbsolutePath(), e.toString());
      return;
    }
    synchronized (this) {
      for (final String line : lines) {
        final int eq = line.indexOf('=');
        if (eq <= 0)
          continue;
        final String key = line.substring(0, eq);
        final String value = line.substring(eq + 1);
        if ("peer".equals(key))
          armedPeer = value;
        else if (JOIN_INDEX_KEY.equals(key))
          joinIndex = parseIndex(value);
        else
          for (int i = 0; i < INSTALLED_KEYS.length; i++)
            if (INSTALLED_KEYS[i].equals(key))
              lastInstalledIndex[i] = parseIndex(value);
      }
      // What is on disk is the state as restored: nothing to rewrite until it changes.
      persistedVersion = stateVersion;
    }
  }

  private static long parseIndex(final String value) {
    try {
      return Long.parseLong(value.trim());
    } catch (final NumberFormatException e) {
      return NO_INDEX;
    }
  }

  /** Whether this node was added to the Raft configuration by a change it applied while running. */
  public boolean hasJoinedAtRuntime() {
    return joinedAtRuntime;
  }
}
