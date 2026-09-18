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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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

  /**
   * How recently a finished seed may have finished to answer a request instead of a new seed being run.
   * <p>
   * Sized as the round trip the duplicate comes from: the admitting node issues its request as soon as
   * {@code addPeer} returns, so the gap it has to cover is one HTTP call on a cluster network. Generous enough
   * to absorb a slow one, short enough that an unrelated request gets a fresh seed rather than a stale answer.
   */
  static final long REUSE_WINDOW_MS = 5_000L;

  private final BooleanSupplier  isLeader;
  private final LongSupplier     retryBudgetMs;
  private final SecuritySeed     seed;
  private final Executor         executor;
  /** Non-null only when this instance built its own executor, and is then the same object as {@link #executor}. */
  private final ExecutorService  ownedExecutor;
  /** Peers of the last configuration observed, {@code null} until the first one. Guarded by {@code this}. */
  private       Set<RaftPeerId>  knownPeers;
  /**
   * The seed that is queued or running, or {@code null}/done when none is (issue #7834). Guarded by
   * {@code this}.
   * <p>
   * It is what makes this class the cluster's <b>single</b> seeder rather than one of several. An admission
   * used to be seeded twice - once by the node that admitted the peer, once here - from two different JVMs,
   * each holding only its own {@code ServerSecurity} monitor, so a revocation committing between the two could
   * be undone by whichever submit landed second. The admitting node now asks the leader for a seed through
   * {@link #seedNowAndReport} instead of running one, and this field is where that request meets the one the
   * membership change already scheduled: a request that arrives while a seed is outstanding takes that seed's
   * result instead of adding a second.
   * <p>
   * Folding into an outstanding seed is sound because the payload does not depend on the admission. A seed
   * carries the security documents as they are when it RUNS - membership is not one of them - so any run that
   * commits after the peer became a member delivers exactly what a run started later would have. For the
   * admission that scheduled it that is always true: the configuration callback runs on the apply thread, so a
   * seed it schedules reads AFTER the membership entry was applied.
   * <p>
   * The one case that does not hold is a SECOND admission folding into a seed the first one started, whose
   * entry can then be ordered before the second peer's configuration entry. That peer still receives it by
   * ordinary log replay, and if the log has been purged and it catches up by snapshot install instead, by the
   * catch-up request of issue #7833.
   */
  private       CompletableFuture<List<String>> outstandingSeed;
  /**
   * The seed that finished most recently, and when, or {@code null} before the first one. Guarded by
   * {@code this}.
   * <p>
   * The fold above only catches a request that arrives while a seed is still outstanding. An admitting node's
   * request can also arrive just AFTER the membership change's seed finished - it is a round trip behind the
   * configuration entry - and that would schedule a second seed for the same admission. A seed that finished
   * moments ago is reported instead.
   * <p>
   * Reusing it is sound for the same reason the fold is, and {@link #REUSE_WINDOW_MS} bounds how far back that
   * argument may reach. Only a caller that asks may reuse it: see {@link #seedNowAndReport}.
   */
  private       CompletableFuture<List<String>> lastCompletedSeed;
  private       long                            lastCompletedSeedAt;
  /**
   * Set by {@link #close()}, and never cleared: a seed still running then may finish afterwards, and its
   * result must not repopulate the slot this node has just abandoned.
   */
  private       boolean                          closed;
  /** What {@link #runSeed} was last entered for; read back by a test that the reason is not hardcoded. */
  private       String                           lastRunReason;

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
   * <b>A second seed folds into the outstanding one rather than being queued behind it, and that is coalescing
   * rather than loss.</b> A task reads the documents when it RUNS rather than being handed a snapshot, so the
   * outstanding task covers every change folded into it - and the caller that folded in gets that task's
   * result, which is what lets an admission report a seed it did not itself run (issue #7834).
   * <p>
   * <b>Not registered with {@code PoolMetrics}.</b> That binder is a {@code MeterBinder} over the engine's
   * process-wide singletons, bound once through their {@code getInstance()} accessors; it has no surface for
   * a per-server pool such as this one, which belongs to a state-machine instance. The module's other
   * housekeeping executor is excluded from that binder too ({@code RaftLogCompactionScheduler}), but on a
   * different ground and not a precedent for this one: that pool is excluded because it is uninteresting - one
   * bounded task every few minutes, no queue to back up - whereas this one has a one-slot queue and drops on
   * saturation, so its drop count is exactly what an operator would want and the obstacle is structural rather
   * than editorial. Surfacing this pool and {@code ServerSecurity.permissionsRefreshExecutor} - the pool this
   * one is shaped after, and which is not surfaced either - is tracked as issue #7856.
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
    // AbortPolicy, not a discarding handler (issue #7834). Coalescing now happens one level up, in schedule(),
    // where the folded-in caller gets the outstanding seed's FUTURE and therefore its outcome; a handler that
    // silently dropped the task here would leave that future uncompleted and every reporting caller waiting
    // out its timeout for a seed that was never going to run. What reaches this policy now is only a submit to
    // an executor that has been shut down, which schedule() reports as "the node is stopping".
    return new ThreadPoolExecutor(0, 1, 30L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(1), r -> {
      final Thread thread = new Thread(r, "arcadedb-raft-security-seed");
      thread.setDaemon(true);
      return thread;
    }, new ThreadPoolExecutor.AbortPolicy());
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
   * <p>
   * <b>{@code term} and {@code index} are for the log lines only, and a stale one is deliberately not
   * rejected.</b> The two callers really are different threads, so the monitor is not merely cheap insurance,
   * but an index guard on top of it would be wrong in one direction and unnecessary in the other:
   * <ul>
   *   <li><b>Unnecessary for the seed decision.</b> Only the leader seeds, and a leader never reaches the
   *       snapshot-install call site: {@code SnapshotInstallationHandler} answers {@code NOT_LEADER} unless
   *       {@code state.recognizeLeader} accepts the sender, and then calls
   *       {@code changeToFollowerAndPersistMetadata} before installing. So on the node that decides to seed,
   *       this method is driven only by {@code applyLogToStateMachine}, i.e. the single apply loop delivering
   *       committed entries in index order, and no stale update can regress the baseline that decision reads.</li>
   *   <li><b>Wrong for the baseline.</b> The snapshot path fires only when the installed configuration differs
   *       from the one this node holds, immediately after {@code state.truncate(proto.getIndex())} - it is the
   *       authoritative membership replacing one this node took from entries now being truncated away, and its
   *       index is legitimately lower. Rejecting it would leave the baseline holding a membership the log no
   *       longer contains.</li>
   * </ul>
   * <p>
   * <b>The baseline advances before the seed is dispatched, and is not rolled back if the dispatch does not
   * happen.</b> That is deliberate - the baseline tracks what Ratis committed, not what this seeder managed to
   * act on, and rolling it back would make the NEXT unrelated configuration change re-seed for a peer that has
   * nothing to do with it. The consequence is that a peer whose seed is lost here is not retried by a later
   * configuration change that adds some other peer; only removing and re-adding it seeds it again. Three ways
   * the dispatch can be skipped, and what each costs:
   * <ul>
   *   <li>{@code isLeader} answers {@code false} on a follower - by design, nothing is lost: the leader is
   *       running this same callback and dispatches there.</li>
   *   <li>The submit is refused by the one-slot queue - nothing is lost either: the queued task re-reads the
   *       documents when it runs, so it covers this change too (see the constructor).</li>
   *   <li>{@code isLeader} answers {@code false} on the actual leader because the role read degraded, or the
   *       dispatch path throws an {@code Error} that {@code ArcadeStateMachine.notifyConfigurationChanged}
   *       logs and swallows. This is the one case where a seed is genuinely dropped. Both are logged, and the
   *       admitting-side seed of issue #7521 still covers the two paths that have an admitting node; only a
   *       {@code KubernetesAutoJoin} self-join would be left unseeded, with the same symptom and the same
   *       remedy as issue #7833.</li>
   * </ul>
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

      // Sized for one, not capped at one: a setConfiguration may add several peers at once and the list grows
      // if it does. Every production issuer adds exactly one, and this runs on a Ratis callback thread, so the
      // common case allocates the smallest backing array rather than the default sixteen.
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

    schedule("the peer(s) " + added + " joining the Raft configuration", false);
  }

  /**
   * Runs a seed and reports what it could not commit, for a caller that has to answer an operator (issue
   * #7834).
   * <p>
   * This is the half of {@code POST /api/v1/cluster/peer}'s and {@code connect cluster}'s contract that issue
   * #7521 made operator-facing: the route answers 503 with a {@code failedSeeds} array, and the verb logs
   * SEVERE naming the documents. That contract is why those two paths could not simply stop seeding when the
   * leader-side seeder arrived - the leader-side seed is asynchronous and has no caller to report to. So the
   * report comes back through here instead: the admitting node asks the LEADER for the seed and reads the
   * outcome, and there is one seeder again.
   *
   * @param reason    what the seed is FOR, in the caller's own words, because it is what the log lines this
   *                  run writes will name. Hardcoding an admission's phrasing here made the issue #7833
   *                  catch-up - the one caller with no concurrent membership seed to fold into, so the one
   *                  whose string actually reaches the log - report itself as a peer admission
   * @param timeoutMs how long to wait for the seed to finish before giving up on REPORTING it; the seed itself
   *                  is not cancelled, since it is the work the joining peer needs either way
   * @param mayReuseRecentSeed whether a seed that finished moments ago may ANSWER this request instead of a new
   *                  one being run. True for an admission, whose request is a round trip behind the membership
   *                  change that already seeded for it. <b>False for anything that has established it needs a
   *                  seed</b> - a catch-up whose fingerprints did not match has just been told this node is out
   *                  of step, and answering it from an unrelated recent result would leave it that way, which
   *                  is the failure issue #7833 exists to repair
   *
   * @return the names of the documents that could not be seeded, empty when all of them committed
   *
   * @throws IllegalStateException when no seed could be run or awaited at all, so a caller never reads
   *                               "nothing failed" from a seed that never happened
   */
  public List<String> seedNowAndReport(final String reason, final long timeoutMs,
      final boolean mayReuseRecentSeed) {
    final CompletableFuture<List<String>> seed = schedule(reason, mayReuseRecentSeed);
    if (seed == null)
      throw new IllegalStateException("the security seed could not be scheduled; this node may be stopping");

    return report(seed, timeoutMs);
  }

  /** Waits for {@code seed} and turns every way that can fail into the one exception this class reports. */
  private static List<String> report(final CompletableFuture<List<String>> seed, final long timeoutMs) {
    try {
      return seed.get(Math.max(1L, timeoutMs), TimeUnit.MILLISECONDS);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted while waiting for the cluster security seed", e);
    } catch (final TimeoutException e) {
      throw new IllegalStateException("the cluster security seed did not finish within " + timeoutMs
          + "ms; it is still running on this node and its outcome will be logged there", e);
    } catch (final CompletionException | ExecutionException e) {
      throw new IllegalStateException("the cluster security seed failed: "
          + (e.getCause() != null ? e.getCause().getMessage() : e.getMessage()),
          e.getCause() != null ? e.getCause() : e);
    }
  }

  /**
   * Schedules a seed, hands back the one already outstanding, or - only when the caller allows it - the one
   * that finished moments ago. {@code null} when the executor refused the task, which means the node is
   * stopping.
   */
  private CompletableFuture<List<String>> schedule(final String reason, final boolean mayReuseRecentSeed) {
    final CompletableFuture<List<String>> seed;
    // Fold, reuse and install are ONE decision under ONE acquisition. Split across two, a caller could pass
    // the reuse check while the outstanding seed was still running, pause, and install a second seed after
    // that one finished - the duplicate the reuse exists to prevent.
    synchronized (this) {
      // A DONE future does not block the next schedule, so the slot never has to be cleared: whoever comes
      // next simply installs their own. That is what lets the refusal below answer rather than undo.
      if (outstandingSeed != null && !outstandingSeed.isDone()) {
        LogManager.instance().log(this, Level.FINE,
            "A cluster security seed is already outstanding; the one for %s folds into it", reason);
        return outstandingSeed;
      }
      if (mayReuseRecentSeed && lastCompletedSeed != null
          && System.currentTimeMillis() - lastCompletedSeedAt <= REUSE_WINDOW_MS) {
        LogManager.instance().log(this, Level.FINE,
            "A cluster security seed finished %dms ago; %s reports its outcome instead of running a second one",
            System.currentTimeMillis() - lastCompletedSeedAt, reason);
        return lastCompletedSeed;
      }
      seed = new CompletableFuture<>();
      outstandingSeed = seed;
    }

    try {
      // Outside the monitor: runSeed submits Raft entries and waits for them to commit, and holding this
      // monitor across that would block onConfigurationChanged, which runs on a Ratis callback thread.
      executor.execute(() -> runSeed(reason, seed));
    } catch (final RejectedExecutionException e) {
      // Completed rather than left dangling: a caller that folded into this future before the refusal is owed
      // an answer, and a future nobody will ever complete is a caller parked until its own timeout.
      seed.completeExceptionally(new IllegalStateException(
          "this node is stopping; the cluster security seed for " + reason + " was not scheduled"));
      LogManager.instance().log(this, Level.FINE,
          "The cluster security seed for %s was refused by the executor; the node is stopping", reason);
      return null;
    }
    return seed;
  }

  /**
   * The seed itself, off the Ratis thread.
   * <p>
   * Leadership is deliberately <b>not</b> re-checked here. The submit routes to whoever is leader at the time,
   * so a leadership change between the configuration entry and this call still delivers the documents, whereas
   * a re-check would silently drop the seed - the new leader's own startup configuration entry adds no peer and
   * so would not issue one of its own.
   *
   * @param result completed with the outcome, for {@link #seedNowAndReport}'s caller
   */
  private void runSeed(final String reason, final CompletableFuture<List<String>> result) {
    synchronized (this) {
      lastRunReason = reason;
    }
    try {
      final List<String> failed = seed.seed(retryBudgetMs.getAsLong());
      // Recorded BEFORE the future is completed: a caller that sees the outstanding seed done must also see
      // the result it is allowed to reuse, or it installs a second seed for work that just finished.
      recordCompletion(result);
      result.complete(List.copyOf(failed));
      if (failed.isEmpty())
        // "Committed", not "the peer now holds them": what the submit waits for is a Raft commit, which a
        // quorum satisfies. The joining peer applies the entries when it catches up, and nothing here observes
        // that - claiming otherwise in a log line an operator reads would be claiming more than was checked.
        LogManager.instance().log(this, Level.INFO,
            "Cluster security documents committed; the seed was run for %s", reason);
      else
        LogManager.instance().log(this, Level.SEVERE,
            "The security seed run for %s could not commit these documents: %s. The peer(s) it was for are "
                + "cluster members serving requests against their own copy of them - which for a node re-added "
                + "after time out of the cluster can still hold a user dropped since, a group narrowed since or a "
                + "token revoked since. Reissue the change to retry it. Two causes look alike from here: no quorum "
                + "at this instant, for which raising arcadedb.ha.securitySeedRetryTimeout is the answer, and the "
                + "capability gate on the group and API-token entries refusing because the peer that just joined "
                + "has not answered a capability probe yet (issue #7511), for which it is not - that one clears "
                + "itself once the peer answers, and arcadedb.ha.securityEntryCapabilityGate is the override",
            reason, String.join(", ", failed));
    } catch (final Throwable t) {
      // Nothing here may escape: on the owned executor an escaping throwable kills the worker, and the next
      // membership change would then be seeded by a pool that has to build a new thread for it. The future is
      // completed with it rather than only logged, because seedNowAndReport has a caller that must not read
      // "nothing failed" from a seed that threw. Deliberately NOT recorded for reuse: an admission must not be
      // answered by a seed that threw.
      result.completeExceptionally(t);
      LogManager.instance().log(this, Level.SEVERE,
          "The security seed run for %s could not be run at all: %s. The peer(s) it was for are cluster "
              + "members serving requests against their own security documents", t, reason, t.getMessage());
    }
  }

  /** Publishes a finished seed for {@link #REUSE_WINDOW_MS}, so a request a round trip behind it is answered. */
  private synchronized void recordCompletion(final CompletableFuture<List<String>> result) {
    if (closed)
      return;
    lastCompletedSeed = result;
    lastCompletedSeedAt = System.currentTimeMillis();
  }

  /**
   * Schedules a seed and hands back its future WITHOUT waiting, so a test can observe the fold itself rather
   * than infer it from a race. Package-private and test-only, the seam {@link #knownPeersForTest} is.
   */
  // @VisibleForTesting
  CompletableFuture<List<String>> scheduleForTest(final String reason) {
    return schedule(reason, false);
  }

  /** {@link #scheduleForTest(String)} for the reuse path, which only an admission is allowed to take. */
  // @VisibleForTesting
  CompletableFuture<List<String>> scheduleReusingForTest(final String reason) {
    return schedule(reason, true);
  }

  /**
   * Drops the retained result, so the next request runs a fresh seed. Test seam: it is what
   * {@link #REUSE_WINDOW_MS} passing does, without the test waiting for it.
   */
  // @VisibleForTesting
  synchronized void forgetCompletedSeedForTest() {
    lastCompletedSeed = null;
  }

  /** The reason the last run was scheduled under, which is what its log lines name. Test seam. */
  // @VisibleForTesting
  synchronized String lastRunReasonForTest() {
    return lastRunReason;
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

    // Any reporting caller parked on the outstanding seed is told it is not coming, rather than being left to
    // wait out its own timeout on a worker that has just been interrupted.
    final CompletableFuture<List<String>> pending;
    synchronized (this) {
      closed = true;
      pending = outstandingSeed;
      outstandingSeed = null;
      lastCompletedSeed = null;
    }
    if (pending != null)
      pending.completeExceptionally(
          new IllegalStateException("this node is stopping; the security seed was abandoned"));
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
