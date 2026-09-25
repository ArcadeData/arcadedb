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
import java.nio.file.StandardOpenOption;
import java.util.Collection;
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
 * The gate that reads this also requires an absent replicated fingerprint, and the fingerprint is on disk
 * ({@code ReplicatedSecurityFingerprintRepository}), so a node that did converge is not held for a single probe.
 * One that never converged is held again, which is the honest answer for it: it is still enforcing its own copy.
 * Suppressing the replay instead would need the log index this process started from, which Ratis does not hand
 * the state machine before it starts applying.
 * <p>
 * <b>Persisted, because the replay does not always happen (issue #8329).</b> A joiner that restarts AFTER the entry
 * that added it was compacted into a snapshot observes only configurations containing itself, so neither rule can
 * fire again. When constructed with a marker file, the detector writes it the moment it arms and a later process
 * reading it back starts armed. {@link RaftHAServer} places the marker NEXT TO the Raft storage directory rather
 * than inside it, so the divergence reformat of {@code RaftHAServer.restartRatis(true)} - which deletes that
 * directory - does not take the marker with it. A marker that outlives convergence is inert for the same reason
 * the replay re-arm is: the gate also requires an absent replicated fingerprint. A marker that could not be
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
 * Owned by {@link RaftHAServer} rather than by a state machine, so the answer survives the in-place Ratis restart
 * of {@code RaftHAServer.restartRatis}, which builds a new {@link ArcadeStateMachine}.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
public final class RuntimeJoinDetector {

  /** Where the armed state is persisted; {@code null} keeps it in memory only. */
  private final    File    marker;
  /** Whether the last configuration observed contained this node; {@code null} before the first one. */
  private          Boolean lastObservedMembership;
  private volatile boolean joinedAtRuntime;

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
   *
   * @return {@code true} when THIS call armed the detector
   */
  public boolean onConfiguration(final RaftPeerId self, final Collection<RaftPeerId> peers,
      final Collection<RaftPeerId> oldPeers) {
    if (self == null)
      return false;

    final boolean member = peers.contains(self);
    final boolean addedByThisEntry = member && !oldPeers.isEmpty() && !oldPeers.contains(self);

    final boolean armedNow;
    synchronized (this) {
      final boolean addedSinceLastObservation = member && Boolean.FALSE.equals(lastObservedMembership);
      lastObservedMembership = member;
      armedNow = (addedByThisEntry || addedSinceLastObservation) && !joinedAtRuntime;
      if (armedNow)
        joinedAtRuntime = true;
    }

    if (armedNow) {
      LogManager.instance().log(this, Level.INFO,
          "Peer %s was added to the Raft configuration while running: readiness waits for the cluster security "
              + "documents to reach it (arcadedb.ha.securityConvergenceReadinessTimeout)", self);
      // Outside the monitor, and only by the one call that armed, so there is a single writer.
      persist(self);
    }
    return armedNow;
  }

  /**
   * Writes the marker. A failure is logged, never thrown: this runs on a Ratis callback thread, and the in-memory
   * arm stays in place for the rest of the process either way.
   * <p>
   * The file content is written with {@code SYNC}, but the parent directory is not fsynced, so an OS crash in the
   * instant after the arm can lose the new directory entry. That restart then comes back unarmed, which is the
   * behaviour before issue #8329.
   */
  private void persist(final RaftPeerId self) {
    if (marker == null)
      return;
    try {
      final File parent = marker.getAbsoluteFile().getParentFile();
      if (parent != null && !parent.isDirectory() && !parent.mkdirs() && !parent.isDirectory())
        throw new IOException("cannot create directory " + parent);
      Files.writeString(marker.toPath(), "peer=" + self + "\narmedAt=" + System.currentTimeMillis() + "\n",
          StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
          StandardOpenOption.WRITE, StandardOpenOption.SYNC);
    } catch (final IOException | RuntimeException e) {
      LogManager.instance().log(this, Level.WARNING,
          "Could not persist the runtime-join marker %s: a restart after the Raft log is compacted past the entry "
              + "that added this peer will not hold readiness for the cluster security documents (%s)",
          marker.getAbsolutePath(), e.toString());
    }
  }

  /** Whether this node was added to the Raft configuration by a change it applied while running. */
  public boolean hasJoinedAtRuntime() {
    return joinedAtRuntime;
  }
}
