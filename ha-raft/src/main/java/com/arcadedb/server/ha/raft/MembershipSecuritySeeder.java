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
import com.arcadedb.server.monitor.PoolMetrics;
import com.arcadedb.utility.DedicatedThreadPool.PoolStats;
import org.apache.ratis.protocol.RaftPeerId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
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

  static final String THREAD_NAME = "arcadedb-raft-security-seed";

  /**
   * How many times a seed that could not commit every document is run again by {@link #rerunOwedSeedIfDue} before
   * this node stops and leaves it to an operator (issue #13447).
   * <p>
   * A re-run is only started once no peer withholds the security entry capabilities, so one that fails again failed
   * for another reason, usually a quorum lost at that instant. A handful of tries, one capability round apart at the
   * least, rides that out without turning a cluster that cannot commit into a seed every five seconds for ever.
   */
  static final int OWED_SEED_RERUNS = 5;

  /** For a seeder that is told nothing about the capability gate: no peer is ever reported as withholding. */
  private static final Supplier<List<String>> NO_PEER_WITHHOLDING = List::of;

  /** The executor row of a pool that has no worker to report on: no thread, nothing queued, the one slot free. */
  static final PoolStats EMPTY_POOL_STATS = new PoolStats(0, 0, 0, 1, 0L, 0L, 0L);

  private final BooleanSupplier  isLeader;
  private final LongSupplier     retryBudgetMs;
  private final SecuritySeed     seed;
  /**
   * The peers the capability gate of the group and API-token entries is refusing for right now (issue #7511), empty
   * when it would let a seed through. Read to say WHICH peer a failed seed was waiting for, and to decide when a seed
   * that is owed can be run again (issue #13447). Answers from the capability registry the monitor fills: it must not
   * probe, because it is asked on the capability monitor's own thread.
   */
  private final Supplier<List<String>> peersWithholdingSecurityEntries;
  private final Executor         executor;
  /** Non-null only when this instance built its own executor, and is then the same object as {@link #executor}. */
  private final ExecutorService  ownedExecutor;
  /**
   * The owned executor's current worker, recorded by its thread factory, so {@link #awaitTermination(long)} can tell
   * by identity that it is running on it (issue #8364). One worker at a time, and one running a task is never replaced.
   */
  private volatile Thread        ownedWorker;
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
  /**
   * Seed requests that did not get a run of their own (issue #7856): folded into the outstanding seed, answered by
   * one that finished moments ago, or refused because the node is stopping. The {@code tasks.coalesced} column of
   * the {@code pool=security_seed} executor row.
   */
  private final AtomicLong                       coalescedSeeds = new AtomicLong();
  /** What {@link #runSeed} was last entered for; read back by a test that the reason is not hardcoded. */
  private       String                           lastRunReason;
  /**
   * What the seed that could not commit every document was for, or {@code null} when none is owed (issue #13447).
   * Guarded by {@code this}.
   * <p>
   * A seed whose retry budget ran out used to be forgotten: the SEVERE line said the capability gate's refusal "clears
   * itself once the peer answers", which is true of the gate and was never true of the seed it refused. During a
   * rolling restart that is the common case rather than a corner: a node's catch-up asks for a seed while the next peer
   * is still booting and has not advertised its capabilities, and the peer takes longer to come up than
   * {@code arcadedb.ha.securitySeedRetryTimeout} lasts. So the seed is kept here and {@link #rerunOwedSeedIfDue} runs
   * it again once every peer advertises the capabilities.
   * <p>
   * One reason is enough for any number of failed seeds, because a seed's payload does not depend on who asked for it:
   * it carries the three documents as they are when it runs, so one run that commits pays every seed that was owed.
   * <p>
   * In memory only, and only on the node that ran the seed. A leader that restarts or is replaced forgets it; the
   * node that asked covers that case itself, because its own catch-up asks again at the next leader change
   * ({@code SecurityCatchUp.outcomeOfAnswer}). A membership seed has no such second asker, which is the gap issue
   * #7833's once-per-start catch-up of the joined node is left to cover.
   */
  private       String                           owedSeedReason;
  /** How many times {@link #rerunOwedSeedIfDue} has run the owed seed again. Guarded by {@code this}. */
  private       int                              owedSeedReruns;
  /**
   * Which debt {@link #owedSeedReason} is, counted up every time a seed becomes owed. Guarded by {@code this}.
   * <p>
   * {@link #rerunOwedSeedIfDue} reads the debt, leaves the monitor to ask Ratis and the capability registry, and comes
   * back to clear it or to run it again. The reason text cannot tell it whether the debt it finds then is the one it
   * read: two catch-ups of the same node carry the same reason, so a debt dropped and owed again in between (the
   * leadership lost and regained, and a new seed refused) would be cleared by the check that saw the old one, and never
   * run again.
   */
  private       long                             owedSeedGeneration;

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
   * <b>Published as the {@code pool=security_seed} executor row</b> (issue #7856), through
   * {@code PoolMetrics.bindInstancePool} - the registration path for a pool that belongs to an instance rather
   * than to the JVM. {@code RaftHAPlugin} registers it and resolves this seeder through the live state machine on
   * every scrape, see {@link #getPoolStats()} and {@link #getCoalescedSeeds()}.
   */
  public MembershipSecuritySeeder(final BooleanSupplier isLeader, final LongSupplier retryBudgetMs,
      final SecuritySeed seed) {
    this(isLeader, retryBudgetMs, seed, NO_PEER_WITHHOLDING);
  }

  /**
   * The production form, told which peers the capability gate of the security entries is refusing for (issue
   * #13447): see {@link #peersWithholdingSecurityEntries}.
   */
  public MembershipSecuritySeeder(final BooleanSupplier isLeader, final LongSupplier retryBudgetMs,
      final SecuritySeed seed, final Supplier<List<String>> peersWithholdingSecurityEntries) {
    final ThreadPoolExecutor owned = createSeedExecutor(this);
    this.isLeader = isLeader;
    this.retryBudgetMs = retryBudgetMs;
    this.seed = seed;
    this.peersWithholdingSecurityEntries = peersWithholdingSecurityEntries;
    this.executor = owned;
    this.ownedExecutor = owned;
  }

  /**
   * Test seam: runs the seed on the supplied executor, which this instance does not own and does not shut
   * down. See the public constructor for what the production executor is and why.
   */
  MembershipSecuritySeeder(final BooleanSupplier isLeader, final LongSupplier retryBudgetMs,
      final SecuritySeed seed, final Executor executor) {
    this(isLeader, retryBudgetMs, seed, NO_PEER_WITHHOLDING, executor);
  }

  /** The test seam above, with the peers the capability gate is refusing for (issue #13447). */
  MembershipSecuritySeeder(final BooleanSupplier isLeader, final LongSupplier retryBudgetMs,
      final SecuritySeed seed, final Supplier<List<String>> peersWithholdingSecurityEntries, final Executor executor) {
    this.isLeader = isLeader;
    this.retryBudgetMs = retryBudgetMs;
    this.seed = seed;
    this.peersWithholdingSecurityEntries = peersWithholdingSecurityEntries;
    this.executor = executor;
    this.ownedExecutor = null;
  }

  private static ThreadPoolExecutor createSeedExecutor(final MembershipSecuritySeeder owner) {
    // AbortPolicy, not a discarding handler (issue #7834). Coalescing now happens one level up, in schedule(),
    // where the folded-in caller gets the outstanding seed's FUTURE and therefore its outcome; a handler that
    // silently dropped the task here would leave that future uncompleted and every reporting caller waiting
    // out its timeout for a seed that was never going to run. What reaches this policy now is only a submit to
    // an executor that has been shut down, which schedule() reports as "the node is stopping".
    return new ThreadPoolExecutor(0, 1, 30L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(1), r -> {
      final Thread thread = new Thread(r, THREAD_NAME);
      owner.ownedWorker = thread;
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

  /**
   * {@link #seedNowAndReport} for a request that has just changed what a seed would observe, and so must be answered
   * by a seed that STARTED after that change (issue #8689).
   * <p>
   * The admission of a peer declared on a follower is the case: the membership change already scheduled a seed here,
   * and that seed's capability probes dialled a derived address because the declared one had not reached this node
   * yet. Folding into it, or reusing its result, would report the failure the declared address was sent to prevent.
   * So a seed still in flight is waited out - its outcome is not this request's - and a fresh one is run after it,
   * with no reuse. A seed that a third caller starts meanwhile is folded into, which is sound: it started after the
   * change too.
   * <p>
   * "In flight" includes a seed that is queued but not yet running: it is waited for the same way, which costs nothing
   * more, since it would read the new address when it ran.
   * <p>
   * One deadline covers both waits, so the caller's own timeout, which is sized for one seed plus a margin, still
   * bounds the whole answer.
   *
   * @throws IllegalStateException when the in-flight seed did not finish within the deadline, or when no fresh seed
   *                               could be run or awaited at all
   */
  public List<String> seedAfterInFlightAndReport(final String reason, final long timeoutMs) {
    final long deadline = System.currentTimeMillis() + Math.max(1L, timeoutMs);
    final CompletableFuture<List<String>> inFlight;
    synchronized (this) {
      inFlight = outstandingSeed;
    }
    if (inFlight != null && !inFlight.isDone())
      try {
        inFlight.get(Math.max(1L, deadline - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
      } catch (final ExecutionException | CancellationException ignored) {
        // Its outcome is not this request's: whatever it failed with, the fresh seed below is the answer.
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("interrupted while waiting for the cluster security seed in flight", e);
      } catch (final TimeoutException e) {
        throw new IllegalStateException("the cluster security seed already in flight did not finish within " + timeoutMs
            + "ms, so no seed could be run for " + reason, e);
      }
    return seedNowAndReport(reason, Math.max(1L, deadline - System.currentTimeMillis()), false);
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
        coalescedSeeds.incrementAndGet();
        LogManager.instance().log(this, Level.FINE,
            "A cluster security seed is already outstanding; the one for %s folds into it", reason);
        return outstandingSeed;
      }
      if (mayReuseRecentSeed && lastCompletedSeed != null
          && System.currentTimeMillis() - lastCompletedSeedAt <= REUSE_WINDOW_MS) {
        coalescedSeeds.incrementAndGet();
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
      coalescedSeeds.incrementAndGet();
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
      // What the outcome owes or pays is settled BEFORE the future is completed too (issue #13447): a caller that
      // sees the seed done must also see whether it is still owed. The peers are read first and outside this
      // monitor, because that reads the capability registry.
      final boolean committed = failed.isEmpty();
      final List<String> withholding = committed ? List.of() : peersWithholdingSecurityEntries();
      final boolean wasOwed = committed && settleOwedSeed();
      final Owed owed = committed ? null : oweSeed(reason);
      // Recorded BEFORE the future is completed: a caller that sees the outstanding seed done must also see
      // the result it is allowed to reuse, or it installs a second seed for work that just finished.
      recordCompletion(result);
      result.complete(List.copyOf(failed));
      if (committed) {
        // "Committed", not "the peer now holds them": what the submit waits for is a Raft commit, which a
        // quorum satisfies. The joining peer applies the entries when it catches up, and nothing here observes
        // that - claiming otherwise in a log line an operator reads would be claiming more than was checked.
        LogManager.instance().log(this, Level.INFO,
            "Cluster security documents committed; the seed was run for %s%s", reason,
            wasOwed ? ". That also pays the seed that could not commit earlier (issue #13447)" : "");
      } else {
        // SEVERE when the seed becomes owed and when it is given up on; a WARNING in between, where the node that
        // asked is retrying and each of its attempts fails here for the reason the first line already gave.
        LogManager.instance().log(this, owed == Owed.STILL ? Level.WARNING : Level.SEVERE,
            "The security seed run for %s could not commit these documents: %s. The node(s) it was for are "
                + "cluster members serving requests against their own copy of them - which for a node that was "
                + "away from the cluster can still hold a user dropped since, a group narrowed since or a "
                + "token revoked since. %s %s", reason, String.join(", ", failed), causeOf(withholding),
            remedyOf(owed != Owed.DROPPED));
      }
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

  /**
   * What most likely stopped a seed, said from what the capability registry holds at the moment it failed.
   * <p>
   * The two causes cannot be told apart from the document names the seed reports, and the line used to describe both
   * and blame "the peer that just joined" for the second. The peer the gate waits for is whichever one has not
   * advertised the capability, which during a rolling restart is the NEXT peer restarting, not the node the seed
   * was for (issue #13447). So the peers are named, from the same registry the gate reads.
   */
  // @VisibleForTesting
  static String causeOf(final List<String> withholding) {
    if (withholding.isEmpty())
      return "Every peer advertises the security entry capabilities at this moment, so the likely cause is that there "
          + "was no quorum while the seed ran, for which raising arcadedb.ha.securitySeedRetryTimeout is the answer.";
    return "Peer(s) " + withholding + " have not advertised the capabilities the group and API-token entries need, "
        + "so the capability gate refuses to replicate those two documents (issue #7511). That is the usual state of "
        + "a peer that is restarting, and it need not be the node this seed was for. Raising "
        + "arcadedb.ha.securitySeedRetryTimeout does not help with it; arcadedb.ha.securityEntryCapabilityGate is "
        + "the override.";
  }

  /** What happens next to a seed that failed, and what an operator can do about it. */
  // @VisibleForTesting
  static String remedyOf(final boolean owed) {
    return owed
        ? "This node runs the seed again, for as long as it leads, as soon as every peer advertises those "
        + "capabilities, at most " + OWED_SEED_RERUNS + " times, and logs the outcome. To retry sooner, re-POST the "
        + "node to /api/v1/cluster/peer on any other member, or reissue the change"
        : "This node has run it again " + OWED_SEED_RERUNS + " times and stops here, or is stopping. Re-POST the "
            + "node to /api/v1/cluster/peer on any other member, or reissue the change, to retry it";
  }

  /** The peers the gate is refusing for, never {@code null} and never a throw: it is read to write a log line. */
  private List<String> peersWithholdingSecurityEntries() {
    try {
      final List<String> withholding = peersWithholdingSecurityEntries.get();
      return withholding != null ? withholding : List.of();
    } catch (final RuntimeException e) {
      LogManager.instance().log(this, Level.FINE,
          "Could not read which peers withhold the security entry capabilities: %s", e.getMessage());
      return List.of();
    }
  }

  /** What recording a failed seed did: see {@link #oweSeed}. */
  private enum Owed {
    /** Nothing was owed before: this failure is the one {@link #rerunOwedSeedIfDue} will run again. */
    NEW,
    /** A seed was owed already and still is. */
    STILL,
    /** Run again {@link #OWED_SEED_RERUNS} times already, or this node is stopping: nothing is owed any longer. */
    DROPPED
  }

  /** Records that a seed could not commit every document, so {@link #rerunOwedSeedIfDue} runs it again. */
  private synchronized Owed oweSeed(final String reason) {
    if (closed)
      return Owed.DROPPED;
    if (owedSeedReason == null) {
      owedSeedReason = reason;
      owedSeedReruns = 0;
      owedSeedGeneration++;
      return Owed.NEW;
    }
    if (owedSeedReruns < OWED_SEED_RERUNS)
      // Already owed: the first reason stays, since one run pays them all, and so does the count, so a caller that
      // keeps asking cannot keep the re-runs going for ever.
      return Owed.STILL;
    owedSeedReason = null;
    owedSeedReruns = 0;
    return Owed.DROPPED;
  }

  /** A seed committed every document, so nothing is owed any longer. Answers whether something was. */
  private synchronized boolean settleOwedSeed() {
    final boolean wasOwed = owedSeedReason != null;
    owedSeedReason = null;
    owedSeedReruns = 0;
    return wasOwed;
  }

  /**
   * Runs again the seed that could not commit, once nothing stands in its way (issue #13447). Called after every
   * scheduled capability round, which is the moment the leader learns that a peer advertises its capabilities
   * again; cheap when nothing is owed, which is nearly always.
   * <p>
   * Three conditions, each of them a reason the re-run would be wrong or wasted:
   * <ul>
   *   <li><b>no seed is queued or running</b>: its own outcome decides whether anything is still owed;</li>
   *   <li><b>this node leads</b>: a seed publishes the leader's documents, which are the cluster's reference. A node
   *       that no longer leads drops what it owed instead of keeping it for a leadership it may regain much later;
   *       the node that asked is asking the new leader itself (see {@link #owedSeedReason});</li>
   *   <li><b>no peer withholds the security entry capabilities</b>: the gate would refuse the same two documents
   *       again, and the seed would spend its retry budget probing a peer that is still down.</li>
   * </ul>
   * Never blocks and never throws: the seed itself goes to {@link #executor}, like every other.
   *
   * @return whether a re-run was started
   */
  boolean rerunOwedSeedIfDue() {
    final String reason;
    final long generation;
    synchronized (this) {
      if (owedSeedReason == null || closed)
        return false;
      if (outstandingSeed != null && !outstandingSeed.isDone())
        return false;
      reason = owedSeedReason;
      generation = owedSeedGeneration;
    }

    // Both read Ratis or the capability registry, so neither runs under this monitor. What they decide is applied
    // only to the debt read above (see owedSeedGeneration): one owed since is left for the next round.
    if (!isLeader.getAsBoolean()) {
      final boolean dropped;
      synchronized (this) {
        dropped = owedSeedReason != null && generation == owedSeedGeneration;
        if (dropped) {
          owedSeedReason = null;
          owedSeedReruns = 0;
        }
      }
      if (dropped)
        LogManager.instance().log(this, Level.INFO,
            "This node no longer leads, so it will not run again the security seed for %s that could not commit; "
                + "the node it was for asks the new leader itself (issue #13447)", reason);
      return false;
    }
    final List<String> withholding = peersWithholdingSecurityEntries();
    if (!withholding.isEmpty()) {
      LogManager.instance().log(this, Level.FINE,
          "The security seed for %s is still owed: peer(s) %s have not advertised the security entry capabilities",
          reason, withholding);
      return false;
    }

    final int rerun;
    final boolean spent;
    synchronized (this) {
      if (owedSeedReason == null || generation != owedSeedGeneration || closed)
        return false;
      // Checked here as well as in oweSeed: a re-run that THROWS reports no documents, so it never reaches oweSeed
      // and would otherwise be started again on every capability round.
      spent = owedSeedReruns >= OWED_SEED_RERUNS;
      if (spent) {
        owedSeedReason = null;
        owedSeedReruns = 0;
        rerun = 0;
      } else
        rerun = ++owedSeedReruns;
    }
    if (spent) {
      LogManager.instance().log(this, Level.SEVERE,
          "The security seed for %s could not be run to the end in %d re-runs and is not run again. %s", reason,
          OWED_SEED_RERUNS, remedyOf(false));
      return false;
    }
    LogManager.instance().log(this, Level.INFO,
        "Every peer advertises the security entry capabilities: running again the security seed for %s, which "
            + "could not commit before (re-run %d of %d, issue #13447)", reason, rerun, OWED_SEED_RERUNS);
    return schedule(reason, false) != null;
  }

  /** What the owed seed was for, or {@code null} when none is owed. Test seam. */
  // @VisibleForTesting
  synchronized String owedSeedReasonForTest() {
    return owedSeedReason;
  }

  /**
   * Load of the owned worker, for the {@code pool=security_seed} executor row (issue #7856). All zeros when this
   * instance runs on an executor it was handed rather than one it built - the test seam - since that executor's
   * load is its owner's to report, not this seeder's.
   */
  public PoolStats getPoolStats() {
    return ownedExecutor instanceof ThreadPoolExecutor owned ? PoolMetrics.statsOf(owned) : EMPTY_POOL_STATS;
  }

  /** Cumulative seed requests that did not get a run of their own; see {@link #coalescedSeeds}. */
  public long getCoalescedSeeds() {
    return coalescedSeeds.get();
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
   * Does NOT wait for it - see {@link #awaitTermination(long)}.
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
      owedSeedReason = null;
      pending = outstandingSeed;
      outstandingSeed = null;
      lastCompletedSeed = null;
    }
    if (pending != null)
      pending.completeExceptionally(
          new IllegalStateException("this node is stopping; the security seed was abandoned"));
  }

  /**
   * Waits, until {@code deadlineNanos}, for the seed {@link #close()} interrupted (issue #8364). A seed past its last
   * interruption point goes on submitting security documents cluster-wide, so without this wait it can still do so
   * after the state machine that owns it has returned from its own close. Returns at once for an executor this
   * instance does not own, which it never shut down.
   *
   * @return false if the caller was interrupted while waiting, for the caller to restore the flag
   */
  boolean awaitTermination(final long deadlineNanos) {
    if (ownedExecutor == null)
      return true;
    return ExecutorTermination.await(this, ownedExecutor, ownedWorker, THREAD_NAME, deadlineNanos,
        "submit security documents cluster-wide");
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
