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
 * The converse is a known gap: the armed state is not persisted, so a joiner that restarts AFTER the entry that
 * added it was compacted into a snapshot observes only configurations containing itself and comes back unarmed
 * (issue #8329).
 * <p>
 * <b>Ordering between the two callers.</b> The monitor makes each observation atomic, not the two Ratis call
 * sites ordered with respect to each other, so the snapshot-install callback can land a configuration older than
 * one the apply loop has already delivered. That cannot produce a false arm on a static member, because no
 * configuration it has ever been part of lacks it; and it cannot undo an arm, because arming latches. The worst
 * it can do is let the second rule see "without me" last and arm on the next configuration that names this node -
 * which is only ever true of a node that really was outside the configuration, i.e. a joiner.
 * <p>
 * Never cleared. Once a node has joined at runtime the gate is armed for the rest of the process, and what
 * releases it is convergence (or the gate's own bounded window), not a later configuration.
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
    return armedNow || rearmedNow;
  }

  /**
   * Records that the state machine installed {@code document} from the replicated log entry at {@code index}.
   * Called on the apply thread for every install, armed or not, so a join observed later still judges it by its
   * index. Cheap and non-blocking beyond this instance's monitor.
   *
   * @param document one of {@link #USERS}, {@link #GROUPS}, {@link #API_TOKENS}
   */
  public void onSecurityDocumentInstalled(final int document, final long index) {
    synchronized (this) {
      if (index > lastInstalledIndex[document])
        lastInstalledIndex[document] = index;
    }
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

  /** The log index of the configuration that last added this node, {@code -1} when none did. For tests. */
  synchronized long joinIndex() {
    return joinIndex;
  }

  /** Whether this node was added to the Raft configuration by a change it applied while running. */
  public boolean hasJoinedAtRuntime() {
    return joinedAtRuntime;
  }
}
