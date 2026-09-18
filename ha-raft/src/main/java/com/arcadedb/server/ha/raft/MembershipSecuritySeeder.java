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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import java.util.logging.Level;

/**
 * Seeds the cluster security documents from the <b>leader</b> whenever a peer enters the committed Raft
 * configuration, whichever admission path put it there (issue #7531).
 * <p>
 * {@code server-users.jsonl}, {@code server-groups.json} and {@code server-api-tokens.json} live under
 * {@code <server-root>/config/}, outside the database directory, so a Raft snapshot install carries none of
 * them: a peer that is not explicitly seeded runs on whatever its own config directory holds until the next
 * cluster-wide change of each kind. On a fresh volume that is the bootstrap {@code root} and the default
 * groups, which authorize almost nothing; on a retained volume it is the security state from whenever that
 * node last converged - a user dropped since, a group narrowed since or a token revoked since is still good
 * on that one node.
 * <p>
 * <b>Why the leader and not the admitting node.</b> Issue #7521 put the seed on the admitting side of an
 * admission, which covers {@code POST /api/v1/cluster/peer} and {@code connect cluster} because both run on a
 * node that issued the membership change for somebody else. {@code KubernetesAutoJoin} has no admitting node:
 * on a StatefulSet scale-up the new pod probes a peer and issues {@code Mode.ADD} for <b>itself</b>, and
 * nothing on the receiving side ran any ArcadeDB code that knew a peer had joined. Hooking the configuration
 * change instead covers all three, because every one of them ends in a configuration entry the leader applies.
 * <p>
 * The joining node applies that same entry and must <b>not</b> seed from it: its documents are the stale ones.
 * That is what the leader gate is for, not merely an optimization to avoid N redundant submits.
 *
 * <h2>Why a configuration that adds a peer, rather than every configuration</h2>
 * The baseline is reliable, which is the only reason the diff is safe. Ratis appends a configuration entry
 * carrying the <i>current</i> membership at the start of every leader's term
 * ({@code LeaderStateImpl.start()} -&gt; {@code StartupLogEntry}, ratis-server 3.3.0) and rejects client
 * requests with {@code LeaderNotReadyException} until that entry is applied, so a leader has always observed
 * the pre-change membership before it can process a {@code setConfiguration}. A follower keeps its baseline
 * current the same way, from the entries it applies.
 * <p>
 * The <b>first</b> configuration observed is therefore recorded as the baseline and seeds nothing: on a leader
 * that first observation is its own startup entry, and on a follower it is a replay or a snapshot install.
 * A configuration that only removes a peer adds nobody and seeds nothing. A peer removed and later re-added is
 * seeded again, because the baseline follows the configuration rather than accumulating.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
public class MembershipSecuritySeeder implements AutoCloseable {

  private final BooleanSupplier  isLeader;
  private final LongSupplier     retryBudgetMs;
  private final SecuritySeed     seed;
  private final Executor         executor;
  /** Non-null only when this instance built its own executor, and is then the same object as {@link #executor}. */
  private final ExecutorService  ownedExecutor;
  /** Peers of the last configuration observed, {@code null} until the first one. Guarded by {@code this}. */
  private       Set<RaftPeerId>  knownPeers;

  /**
   * Production form: seeds on a dedicated single daemon worker.
   * <p>
   * Not one of the JVM-wide {@code DedicatedThreadPool}s and, per the rule those exist to enforce, not the JDK
   * common {@code ForkJoinPool} either - this is per-server security state rather than engine parallelism, and
   * it has to stay serialised so two seeds cannot interleave their submits. The shape is
   * {@code ServerSecurity.permissionsRefreshExecutor}'s, for the same reason: core 0 so an idle server carries
   * no thread, max 1, daemon, one queue slot.
   * <p>
   * <b>It has to be off the caller's thread.</b> The caller is a Ratis callback thread - the state-machine
   * apply loop, or the one serving a leader-initiated snapshot install - and the seed submits Raft entries and
   * waits for them to commit, which is the applying the first of those two threads does.
   * <p>
   * <b>A refused seed is dropped on purpose, and that is coalescing rather than loss.</b> A task reads the
   * documents when it RUNS rather than being handed a snapshot, so a task already queued and not yet started
   * covers every change dropped behind it. The handler's other caller is shutdown, where dropping is what
   * stopping means.
   */
  public MembershipSecuritySeeder(final BooleanSupplier isLeader, final LongSupplier retryBudgetMs,
      final SecuritySeed seed) {
    final ThreadPoolExecutor owned = createSeedExecutor();
    this.isLeader = isLeader;
    this.retryBudgetMs = retryBudgetMs;
    this.seed = seed;
    this.executor = owned;
    this.ownedExecutor = owned;
  }

  /**
   * Test seam: runs the seed on the supplied executor, which this instance does not own and does not shut
   * down. See the public constructor for what the production executor is and why.
   */
  MembershipSecuritySeeder(final BooleanSupplier isLeader, final LongSupplier retryBudgetMs,
      final SecuritySeed seed, final Executor executor) {
    this.isLeader = isLeader;
    this.retryBudgetMs = retryBudgetMs;
    this.seed = seed;
    this.executor = executor;
    this.ownedExecutor = null;
  }

  private static ThreadPoolExecutor createSeedExecutor() {
    return new ThreadPoolExecutor(0, 1, 30L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(1), r -> {
      final Thread thread = new Thread(r, "arcadedb-raft-security-seed");
      thread.setDaemon(true);
      return thread;
    }, (rejected, executor) -> LogManager.instance().log(MembershipSecuritySeeder.class, Level.FINE,
        "A cluster security seed is already queued or the node is stopping; this one is coalesced into it"));
  }

  /**
   * Called for every Raft configuration entry this node applies. Records the membership, and - on the leader
   * only, and only when the configuration brought in a peer that the previous one did not have - schedules the
   * security seed.
   * <p>
   * Runs on a Ratis callback thread - the state-machine apply loop, or the thread serving a leader-initiated
   * snapshot install - and therefore never blocks and never throws: the seed itself goes to {@link #executor},
   * and a failure to schedule it is logged rather than propagated back into Ratis. The membership update below
   * is done under this instance's monitor because those two callers can arrive concurrently.
   *
   * @param term  term of the configuration entry, for the log line only
   * @param index index of the configuration entry, for the log line only
   * @param peers the peers of the new configuration
   */
  public void onConfigurationChanged(final long term, final long index, final Collection<RaftPeerId> peers) {
    final Set<RaftPeerId> current = new LinkedHashSet<>(peers);
    final List<RaftPeerId> added;

    synchronized (this) {
      final Set<RaftPeerId> previous = knownPeers;
      knownPeers = current;

      if (previous == null) {
        LogManager.instance().log(this, Level.FINE,
            "Raft membership baseline recorded at term=%d index=%d: %s", term, index, current);
        return;
      }

      added = new ArrayList<>(1);
      for (final RaftPeerId peer : current)
        if (!previous.contains(peer))
          added.add(peer);
    }

    if (added.isEmpty())
      return;

    if (!isLeader.getAsBoolean()) {
      // Including the joining node itself, whose own documents are the stale ones this seed exists to replace.
      LogManager.instance().log(this, Level.FINE,
          "Peer(s) %s joined the Raft configuration at term=%d index=%d; the leader issues the security seed",
          added, term, index);
      return;
    }

    LogManager.instance().log(this, Level.INFO,
        "Peer(s) %s joined the Raft configuration at term=%d index=%d: seeding the cluster security documents",
        added, term, index);

    try {
      executor.execute(() -> runSeed(added));
    } catch (final RejectedExecutionException e) {
      LogManager.instance().log(this, Level.FINE,
          "A cluster security seed is already queued or the node is stopping; the one for %s is coalesced into it",
          added);
    }
  }

  /**
   * The seed itself, off the Ratis thread.
   * <p>
   * Leadership is deliberately <b>not</b> re-checked here. The submit routes to whoever is leader at the time,
   * so a leadership change between the configuration entry and this call still delivers the documents, whereas
   * a re-check would silently drop the seed - the new leader's own startup configuration entry adds no peer and
   * so would not issue one of its own.
   */
  private void runSeed(final List<RaftPeerId> added) {
    try {
      final List<String> failed = seed.seed(retryBudgetMs.getAsLong());
      if (failed.isEmpty())
        // "Committed", not "the peer now holds them": what the submit waits for is a Raft commit, which a
        // quorum satisfies. The joining peer applies the entries when it catches up, and nothing here observes
        // that - claiming otherwise in a log line an operator reads would be claiming more than was checked.
        LogManager.instance().log(this, Level.INFO,
            "Cluster security documents committed for the newly-joined peer(s) %s", added);
      else
        LogManager.instance().log(this, Level.SEVERE,
            "Peer(s) %s joined the cluster but these security documents could not be seeded to them: %s. They are "
                + "cluster members serving requests against their own copy of them - which for a node re-added "
                + "after time out of the cluster can still hold a user dropped since, a group narrowed since or a "
                + "token revoked since. Reissue the change to retry it. Two causes look alike from here: no quorum "
                + "at this instant, for which raising arcadedb.ha.securitySeedRetryTimeout is the answer, and the "
                + "capability gate on the group and API-token entries refusing because the peer that just joined "
                + "has not answered a capability probe yet (issue #7511), for which it is not - that one clears "
                + "itself once the peer answers, and arcadedb.ha.securityEntryCapabilityGate is the override",
            added, String.join(", ", failed));
    } catch (final Throwable t) {
      // Nothing here may escape: on the owned executor an escaping throwable kills the worker, and the next
      // membership change would then be seeded by a pool that has to build a new thread for it.
      LogManager.instance().log(this, Level.SEVERE,
          "Peer(s) %s joined the cluster but the security seed could not be run at all: %s. They are cluster "
              + "members serving requests against their own security documents", t, added, t.getMessage());
    }
  }

  /** The peers of the last configuration observed, or {@code null} before the first one. Test seam. */
  synchronized Set<RaftPeerId> knownPeersForTest() {
    return knownPeers == null ? null : Set.copyOf(knownPeers);
  }

  /**
   * Stops the owned worker, if this instance built one. {@code shutdownNow()} rather than {@code shutdown()}:
   * a seed in flight is parked waiting for a Raft commit that a node being torn down is not going to produce.
   */
  @Override
  public void close() {
    if (ownedExecutor != null)
      ownedExecutor.shutdownNow();
  }

  /**
   * Submits the three security documents and reports the ones that did not commit. In production this is
   * {@code ServerSecurity.seedSecurityStateClusterWide(long)}, which reads each document under the security
   * monitor and retries the failing ones within {@code retryBudgetMs}.
   */
  @FunctionalInterface
  public interface SecuritySeed {
    /**
     * @param retryBudgetMs how long to keep retrying the documents that fail
     *
     * @return the names of the documents that could not be seeded, empty when all of them committed
     */
    List<String> seed(long retryBudgetMs);
  }
}
