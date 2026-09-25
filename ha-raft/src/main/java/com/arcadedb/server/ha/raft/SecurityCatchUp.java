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
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.HAServerPlugin;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

/**
 * Puts a node that came back <b>still a Raft member</b> back in step with the cluster's security documents
 * (issue #7833).
 * <p>
 * <b>Only a follower asks.</b> Both triggers below end in {@link #run}, which returns without asking anything
 * when this node leads - see the note there for why that is accepted rather than closed, and why it is now
 * logged.
 * <p>
 * <b>The once-per-start request is spent by asking, not by trying</b> (issue #8034). The latch is taken when a
 * request is submitted, but every exit that answers without having put the question to a peer releases it
 * again through {@link #settle}, so the request survives to be made at the first moment somebody can answer
 * it. The arm that makes this matter is the one where this node leads by the time the task runs: the window
 * between taking the latch on the REPLICA branch and running the task is the jitter plus
 * {@code RaftHAServer.waitForLocalApply()} plus the transport backoffs, and a node that restarts during a
 * failover and then wins the election lands in it routinely. Leading is not an answer, because nothing about
 * winning an election pushes this node's {@code <server-root>/config/} documents at its peers: a seed of them
 * is always something somebody ASKED for - an admission route, a configuration change that adds a peer, or
 * another node's own catch-up - and a step-down is none of those. So without the release that node stayed the
 * odd one out until the next cluster-wide security change of each kind.
 * <p>
 * Issue #7531 closed the case where a pod is ABSENT from the committed configuration and adds itself: the
 * leader notices the configuration change and seeds. A pod that restarts while it is still a member issues no
 * configuration change at all - {@code KubernetesAutoJoin} answers {@code Outcome.ALREADY_MEMBER} and no
 * {@code setConfiguration} is written - so no membership hook fires and nothing seeds it. That is the normal
 * outcome of a StatefulSet rolling restart with a retained PVC, of a node drain and reschedule, and of every
 * pod whose ordinal is inside the static {@code arcadedb.ha.serverList}: a configuration member from birth,
 * which the membership hook never sees added.
 *
 * <h2>Why it is not always benign, and which trigger answers which half</h2>
 * While the node was down the cluster may have committed security entries. If they are still in the leader's
 * retained log, catch-up replays them and the node converges on its own - nothing here is needed. If the log
 * has been purged and the node catches up by <b>snapshot install</b> instead, it does not converge: the three
 * documents live under {@code <server-root>/config/}, outside the database directory, and no snapshot carries
 * them. The node then serves requests against the security state it had when it went down - a user dropped
 * since, a group narrowed since, a token revoked since.
 * <ul>
 * <li>{@link #afterSnapshotInstall} is that case exactly, and it is the trigger that is strictly necessary: the
 * snapshot install is the one catch-up path that skips the entries.</li>
 * <li>{@link #onFirstLeaderObserved} is the belt and braces, once per node start, for every other way a node
 * can come back holding documents nobody will send it again - a config directory restored from a backup, a
 * volume re-created under a member that is still in the configuration, a node whose own snapshot index was
 * ahead of the entries. It is nearly free: see below.</li>
 * </ul>
 *
 * <h2>Why the common case writes nothing</h2>
 * The request carries this node's own fingerprints and the leader compares them against its own, so a rolling
 * restart of a cluster whose security state did not change while the pods were down costs one HTTP round trip
 * per pod and <b>no Raft entries</b>. Only a genuine mismatch is seeded, and it is seeded the ordinary way -
 * the leader replicates the documents to the whole cluster, under its own {@code ServerSecurity} monitor -
 * rather than being installed locally from a pulled copy. That is deliberate: a document installed on one node
 * only would leave that node's replicated-fingerprint baseline out of step with everyone else's, and the
 * baseline is what issue #7509's concurrency check is decided from on every node (issue #7693).
 *
 * <h2>Ordering</h2>
 * The once-per-start request waits, bounded, for this node's applied index to reach the commit index before
 * reading its own fingerprints, through {@code RaftHAServer.waitForLocalApply}: asking before catch-up
 * finishes would compare documents this node is about to be sent anyway, and answer a mismatch that the next
 * entry was going to fix. The snapshot-install trigger does
 * not wait - the install has just advanced the applied index to the snapshot point, which is the definition of
 * caught up for the documents a snapshot carries, and the ones it does not carry are precisely what is being
 * asked for.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class SecurityCatchUp implements AutoCloseable {

  /**
   * How many times a request that failed for a transient reason is retried, and the wait before the first
   * retry (doubled each time).
   * <p>
   * Without this the once-per-start request was spent whether or not it SUCCEEDED (CodeRabbit on PR #7854):
   * the latch was taken before the work ran, so a leader that was not resolvable for a moment, or a connection
   * refused while the leader was still binding its listener, burned the attempt for the lifetime of the node.
   * On a quiet cluster - one where no security change and no snapshot install follows - that leaves a member
   * serving the users, groups and tokens it came back with, which is the whole of issue #7833.
   */
  private static final int  TRANSIENT_ATTEMPTS    = 4;
  private static final long TRANSIENT_BACKOFF_MS  = 2_000L;
  /**
   * How long the once-per-start request spreads itself over, chosen per node.
   * <p>
   * A full-cluster restart elects one leader and every follower observes it at once, so without this they all
   * dial within milliseconds of each other (code review on PR #7854). Each request is cheap - a bounded poll
   * and three string comparisons - so the burst is unlikely to matter at the cluster sizes this targets, but
   * the leader is also the node everything else is waiting on at exactly that moment, and spreading the
   * arrivals costs nothing. Same reflex as {@code KubernetesAutoJoin}'s own join jitter.
   */
  private static final long START_JITTER_MS      = 3_000L;

  static final String THREAD_NAME = "arcadedb-raft-security-catchup";

  private final AtomicBoolean      requestedSinceStart = new AtomicBoolean(false);
  private final ThreadPoolExecutor executor;
  /**
   * The executor's current worker, recorded by its thread factory, so {@link #awaitTermination(long)} can tell by
   * identity that it is running on it (issue #8364). One worker at a time, and one running a task is never replaced.
   */
  private volatile Thread          worker;

  SecurityCatchUp() {
    // Same shape as MembershipSecuritySeeder's worker and for the same reasons: core 0 so an idle server carries
    // no thread, max 1 so two catch-ups cannot interleave, daemon, one queue slot. A task dropped for want of
    // room is still safe for the WORK - the one already queued reads the fingerprints when it RUNS and therefore
    // covers the one dropped behind it - but it is not safe for the once-per-start LATCH: the queued task that
    // covers the dropped one may itself settle on an arm that releases the latch (NOBODY_TO_ASK), leaving the
    // dropped request both never made and recorded as made. Rearming unconditionally on every rejection closes
    // that gap: whichever task runs last leaves the latch telling the truth about whether anybody was asked
    // (issue #8087).
    this.executor = new ThreadPoolExecutor(0, 1, 30L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(1), r -> {
      final Thread thread = new Thread(r, THREAD_NAME);
      worker = thread;
      thread.setDaemon(true);
      return thread;
    }, (r, exec) -> rearm());
  }

  /**
   * The first leader this node observes since it started, and only that one: the request is about this node
   * having been away, not about who leads now. A re-election on a running node changes nothing about the
   * documents this node holds, and firing on every one of them would put an HTTP round trip on a path that
   * already has an election to get through.
   * <p>
   * Called from the REPLICA branch of the leader-change callback, so a node that restarts and immediately wins
   * the election never asks. That is the same case {@link #run} describes when it finds this node leading, and
   * while it leads it has the same answer: there is nobody to ask. It is not the same answer afterwards, which
   * is why that arm releases the request again (issue #8034) - and this method is the trigger that then makes
   * it, on the leader change that puts this node back under somebody who can answer.
   */
  void onFirstLeaderObserved(final ArcadeDBServer server, final RaftHAServer raft) {
    if (requestedSinceStart.compareAndSet(false, true))
      submit(server, raft, "this node rejoining the cluster as an existing member", true);
  }


  /**
   * Re-arms the once-per-start request, so the NEXT leader this node observes asks again.
   * <p>
   * Called when the request could not be completed for a transient reason after its own retries are spent, and
   * from {@link #settle} for every attempt that ended without asking anybody. The latch exists to stop a
   * re-election on a healthy node from dialling, not to make one unlucky moment permanent.
   */
  private void rearm() {
    requestedSinceStart.set(false);
  }

  /**
   * What one attempt did, as the once-per-start latch sees it (issue #8034).
   * <p>
   * The distinction the latch needs is not success versus failure - a leader that answers "you are three
   * documents behind, and two of them would not commit" has answered - but <b>asked</b> versus <b>did not
   * ask</b>. Only {@link #ASKED} spends the request.
   */
  enum Outcome {
    /**
     * A peer answered, whether it seeded anything or reported failures. The request is spent: retrying it here
     * would submit the same documents again.
     */
    ASKED,
    /**
     * Nothing was asked, so nothing was answered: this node leads and is itself the reference, there is no Raft
     * plugin to ask through, or the attempt was interrupted before it dialled. The request is released, so the
     * next leader this node observes makes it.
     */
    NOBODY_TO_ASK,
    /**
     * The dial failed for a reason a later attempt might not hit. The latch is left alone here - {@link #run}
     * owns it across the retry budget and releases it once the budget is spent.
     */
    TRANSIENT_FAILURE
  }

  /**
   * Applies an attempt's outcome to the once-per-start latch, and answers whether the attempt is finished.
   * <p>
   * Package-private rather than private so the latch discipline can be driven without a cluster.
   *
   * @return {@code true} when there is nothing left to retry, {@code false} when the caller should back off and
   * try again
   */
  boolean settle(final Outcome outcome) {
    if (outcome == Outcome.TRANSIENT_FAILURE)
      return false;
    if (outcome == Outcome.NOBODY_TO_ASK)
      // Nothing was asked, so the once-per-start request was not made. Releasing it here is what stops a node
      // that led through its own catch-up from staying out of step for good (issue #8034).
      rearm();
    return true;
  }

  /** Whether the once-per-start request has been made and not released. Visible for testing. */
  boolean hasRequestedSinceStart() {
    return requestedSinceStart.get();
  }

  /**
   * Puts the latch back to its at-start value, so a test can drive a trigger from a known state rather than
   * from whatever the server's own startup left behind. Same shape, and the same reason, as
   * {@code PlainHttpFallbackNotice.rearmForTests()}.
   */
  void rearmForTests() {
    rearm();
  }

  /**
   * Takes the once-per-start latch directly, bypassing {@link #onFirstLeaderObserved}/{@link #submit} entirely
   * - which, since issue #8087, release it again immediately when there is nothing to queue. Lets a test put the
   * latch in its taken state to drive {@link #settle} on its own, the same way {@link #rearmForTests} lets one
   * put it back.
   */
  void takeRequestForTests() {
    requestedSinceStart.set(true);
  }

  /**
   * A leader-initiated snapshot install has just finished. This is the catch-up path that provably skips the
   * security entries, so the request is made every time rather than once.
   */
  void afterSnapshotInstall(final ArcadeDBServer server, final RaftHAServer raft) {
    // It also counts as the once-per-start request: a node that has just been reinstalled from a snapshot has
    // no earlier state left worth asking about separately. Taken here rather than after the answer, for the
    // same reason onFirstLeaderObserved takes it at submit time - it is what stops a second trigger dialling
    // while this one is in flight - and it is safe to take up front because every arm of the attempt that ends
    // without asking anybody releases it again (issue #8034), this trigger's own leader arm included.
    requestedSinceStart.set(true);
    submit(server, raft, "a snapshot install, which carries no security document", false);
  }

  private void submit(final ArcadeDBServer server, final RaftHAServer raft, final String reason,
      final boolean waitForCatchUp) {
    if (server == null || raft == null) {
      // Nothing to submit: both callers take the once-per-start latch before calling this (or unconditionally,
      // for afterSnapshotInstall), on the assumption that a task is about to run and eventually settle it. With
      // nothing to run behind it, releasing it here is what stops the latch from being taken for the life of
      // the node with no attempt ever having been made (issue #8087).
      rearm();
      return;
    }
    executor.execute(() -> run(server, raft, reason, waitForCatchUp));
  }

  private void run(final ArcadeDBServer server, final RaftHAServer raft, final String reason,
      final boolean waitForCatchUp) {
    long backoffMs = TRANSIENT_BACKOFF_MS;
    for (int attempt = 1; ; attempt++) {
      if (settle(attemptOnce(server, raft, reason, waitForCatchUp && attempt == 1)))
        return;

      if (attempt >= TRANSIENT_ATTEMPTS) {
        // Out of attempts, and the node is still a follower holding documents nobody is going to send it. The
        // once-per-start latch is released so the next leader this node sees asks again, rather than the gap
        // lasting until a snapshot install, a replicated change, or an operator notices.
        rearm();
        LogManager.instance().log(this, Level.WARNING,
            "Gave up asking the leader to check this node's security documents after %s (%d attempts); the next "
                + "leader change will try again", reason, TRANSIENT_ATTEMPTS);
        return;
      }

      try {
        Thread.sleep(backoffMs);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        rearm();
        return;
      }
      backoffMs *= 2;
    }
  }

  /**
   * One attempt, classified for {@link #settle}: did it put the question to a peer, find nobody to put it to,
   * or fail for a reason a later attempt might not hit?
   */
  private Outcome attemptOnce(final ArcadeDBServer server, final RaftHAServer raft, final String reason,
      final boolean waitForCatchUp) {
    try {
      if (waitForCatchUp) {
        // Before the catch-up wait, not after: the point is to spread the arrivals at the leader, and the wait
        // below ends when this node is caught up - which on a simultaneous restart is the same instant for all
        // of them.
        Thread.sleep(ThreadLocalRandom.current().nextLong(START_JITTER_MS));
        // The module's own notify-based wait rather than a poll loop of this class's invention
        // (code review on PR #7854): it is woken by notifyApplied on every path that advances the index,
        // including the snapshot install, and it already knows about the stale-snapshot floor of issue #6111.
        // Best-effort by contract, which is what this caller wants - a request made slightly early costs a
        // comparison against documents that were about to arrive.
        raft.waitForLocalApply();
      }

      final HAServerPlugin ha = server.getHA();
      if (!(ha instanceof final RaftHAPlugin plugin))
        // No Raft plugin to ask through, so nothing was asked. Both triggers are called from the Raft state
        // machine itself, so this is the plugin being torn down or swapped under a task already queued.
        return Outcome.NOBODY_TO_ASK;
      if (plugin.isLeader()) {
        // The leader IS the reference this request compares against, so there is nobody to ask - including in
        // the window this catches: a node that finished a snapshot install, or restarted, and then won the
        // election before this task ran.
        //
        // For as long as it leads, that is a residual risk rather than an oversight, and it is not closable
        // from here: asking a FOLLOWER would invert the trust model, and the documents this node holds are the
        // ones a Raft election guarantees nothing about, since they live outside the log and outside the
        // snapshot. What the design relies on is that its state came from the same replicated entries everyone
        // else applied - which holds unless it was restored out of band (a hand-edited or backup-restored
        // config directory).
        //
        // So it is said out loud rather than skipped silently (code review on PR #7854): an operator who DID
        // restore that directory by hand has one line in the log naming the node that was not checked.
        //
        // What is NOT accepted is the risk outliving the leadership (issue #8034), and the line used to say
        // the opposite - that these documents were "the cluster's reference from here", as though leading
        // published them. It does not: a cluster-wide seed of the three only ever runs because something asked
        // for one - MembershipSecuritySeeder on a configuration change that ADDS a peer, an admission route, or
        // another node's own catch-up request - and simply leading asks for none of them. The peers keep
        // theirs, which in the scenario this class exists for are the NEWER ones. So the outcome is
        // NOBODY_TO_ASK rather than ASKED: the request is kept, and made on the leader change that puts this
        // node back under somebody who can answer it.
        LogManager.instance().log(this, Level.INFO,
            "This node leads the cluster by the time its security catch-up ran (after %s), so there was no peer "
                + "to validate its %s, %s and %s against and none was asked. Leading does not publish this "
                + "node's copies to the cluster, so the check is kept and made again the next time this node "
                + "observes a different leader. Re-issue the security changes if this node's config directory "
                + "was restored out of band", reason,
            "server-users.jsonl", "server-groups.json", "server-api-tokens.json");
        return Outcome.NOBODY_TO_ASK;
      }

      // Read BEFORE the fingerprints the request carries, so a match proves the documents are the cluster's as of
      // at least this position (issue #8346). See RuntimeJoinDetector.onSecurityDocumentsMatchedLeader.
      final long appliedBeforeRead = raft.getLastAppliedIndex();
      final ClusterSecuritySeedQuery.SeedAnswer answer = ClusterSecuritySeedQuery.seedForCatchUpAnswer(server, plugin,
          reason);
      if (answer.upToDate())
        // The leader compared and submitted nothing. On a runtime joiner that caught up by snapshot install past its
        // seed, this is the only evidence of convergence it will ever get: no seed entry is applied here and no
        // later install is coming, so without it the readiness gate holds for its whole window (issue #8346).
        raft.onSecurityDocumentsMatchedLeader(appliedBeforeRead);
      final List<String> failed = answer.failedSeeds();
      if (failed.isEmpty())
        LogManager.instance().log(this, Level.FINE,
            "Cluster security documents are in step after %s", reason);
      else
        LogManager.instance().log(this, Level.SEVERE,
            "After %s this node asked the leader to re-seed the cluster security documents and these did not "
                + "commit: %s. Until they do, this node serves requests against its own copy of them - which can "
                + "still hold a user dropped, a group narrowed or a token revoked while it was away. Re-POST this "
                + "node to %s on any member to retry", reason, String.join(", ", failed), "/api/v1/cluster/peer");
      // Answered, whether or not every document committed: a partial failure is the leader's report, not a
      // transport failure, and retrying it here would submit the same documents again.
      return Outcome.ASKED;
    } catch (final InterruptedException e) {
      // Interrupted in the jitter or in the catch-up wait, so the dial never happened. Nothing is retried on a
      // thread that is being shut down, but the request must not be recorded as made either (issue #8034).
      Thread.currentThread().interrupt();
      return Outcome.NOBODY_TO_ASK;
    } catch (final Exception e) {
      // Transient by assumption - an unresolvable leader, a refused connection, a timeout - so the caller
      // retries. What is NOT retried is a leader that answered: see the return above.
      LogManager.instance().log(this, Level.FINE,
          "Could not ask the leader to bring this node's security documents back in step after %s: %s", reason,
          e.getMessage());
      return Outcome.TRANSIENT_FAILURE;
    }
  }

  /**
   * Stops the worker: drops the queued request and interrupts the running one, and does NOT wait for it - see
   * {@link #awaitTermination(long)}.
   */
  @Override
  public void close() {
    executor.shutdownNow();
  }

  /**
   * Waits, until {@code deadlineNanos}, for the catch-up {@link #close()} interrupted (issue #8364). An attempt past its
   * last interruption point - the HTTP round trip to the leader - goes on applying the security documents it is
   * answered with, so without this wait it can still change this node's security state after the state machine that
   * owns it has returned from its own close.
   *
   * @return false if the caller was interrupted while waiting, for the caller to restore the flag
   */
  boolean awaitTermination(final long deadlineNanos) {
    return ExecutorTermination.await(this, executor, worker, THREAD_NAME, deadlineNanos,
        "apply security documents to this node");
  }
}
