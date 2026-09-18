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
 * reading its own fingerprints: asking before catch-up finishes would compare documents this node is about to
 * be sent anyway, and answer a mismatch that the next entry was going to fix. The snapshot-install trigger does
 * not wait - the install has just advanced the applied index to the snapshot point, which is the definition of
 * caught up for the documents a snapshot carries, and the ones it does not carry are precisely what is being
 * asked for.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class SecurityCatchUp implements AutoCloseable {

  /** How long the once-per-start request waits for this node to finish catching up before asking anyway. */
  private static final long CATCH_UP_WAIT_MS      = 30_000L;
  /** Poll period of that wait. Short: it is a local read of two longs. */
  private static final long CATCH_UP_POLL_MS      = 200L;

  private final AtomicBoolean      requestedSinceStart = new AtomicBoolean(false);
  private final ThreadPoolExecutor executor;

  SecurityCatchUp() {
    // Same shape as MembershipSecuritySeeder's worker and for the same reasons: core 0 so an idle server carries
    // no thread, max 1 so two catch-ups cannot interleave, daemon, one queue slot. DiscardPolicy here rather than
    // AbortPolicy, because unlike a seed nobody is waiting on the outcome and two catch-ups are the same work:
    // the one already queued reads the fingerprints when it RUNS and therefore covers the one dropped behind it.
    this.executor = new ThreadPoolExecutor(0, 1, 30L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(1), r -> {
      final Thread thread = new Thread(r, "arcadedb-raft-security-catchup");
      thread.setDaemon(true);
      return thread;
    }, new ThreadPoolExecutor.DiscardPolicy());
  }

  /**
   * The first leader this node observes since it started, and only that one: the request is about this node
   * having been away, not about who leads now. A re-election on a running node changes nothing about the
   * documents this node holds, and firing on every one of them would put an HTTP round trip on a path that
   * already has an election to get through.
   * <p>
   * Called from the REPLICA branch of the leader-change callback, so a node that restarts and immediately wins
   * the election never asks. That is the same case {@link #run} describes when it finds this node leading, and
   * it has the same answer: there is nobody to ask.
   */
  void onFirstLeaderObserved(final ArcadeDBServer server, final RaftHAServer raft) {
    if (requestedSinceStart.compareAndSet(false, true))
      submit(server, raft, "this node rejoining the cluster as an existing member", true);
  }

  /**
   * A leader-initiated snapshot install has just finished. This is the catch-up path that provably skips the
   * security entries, so the request is made every time rather than once.
   */
  void afterSnapshotInstall(final ArcadeDBServer server, final RaftHAServer raft) {
    // It also counts as the once-per-start request: a node that has just been reinstalled from a snapshot has
    // no earlier state left worth asking about separately.
    requestedSinceStart.set(true);
    submit(server, raft, "a snapshot install, which carries no security document", false);
  }

  private void submit(final ArcadeDBServer server, final RaftHAServer raft, final String reason,
      final boolean waitForCatchUp) {
    if (server == null || raft == null)
      return;
    executor.execute(() -> run(server, raft, reason, waitForCatchUp));
  }

  private void run(final ArcadeDBServer server, final RaftHAServer raft, final String reason,
      final boolean waitForCatchUp) {
    try {
      if (waitForCatchUp)
        awaitCatchUp(raft);

      final HAServerPlugin ha = server.getHA();
      if (!(ha instanceof final RaftHAPlugin plugin))
        return;
      if (plugin.isLeader()) {
        // The leader IS the reference this request compares against, so there is nobody to ask - including in
        // the window this catches: a node that finished a snapshot install, or restarted, and then won the
        // election before this task ran. Its documents become the cluster's by fiat.
        //
        // That is a residual risk rather than an oversight, and it is not closable from here: asking a FOLLOWER
        // would invert the trust model, and the documents this node holds are the ones a Raft election
        // guarantees nothing about, since they live outside the log and outside the snapshot. What the design
        // relies on is that its state came from the same replicated entries everyone else applied - which holds
        // unless it was restored out of band (a hand-edited or backup-restored config directory).
        //
        // So it is said out loud rather than skipped silently (claude-review on PR #7854): an operator who DID
        // restore that directory by hand has one line in the log naming the node whose copy the cluster is now
        // about to converge on.
        LogManager.instance().log(this, Level.INFO,
            "This node leads the cluster by the time its security catch-up ran (after %s), so there is no peer to "
                + "validate its %s, %s and %s against - they are the cluster's reference from here. Re-issue the "
                + "security changes if this node's config directory was restored out of band", reason,
            "server-users.jsonl", "server-groups.json", "server-api-tokens.json");
        return;
      }

      final List<String> failed = ClusterSecuritySeedQuery.seedForCatchUp(server, plugin, reason);
      if (failed.isEmpty())
        LogManager.instance().log(this, Level.FINE,
            "Cluster security documents are in step after %s", reason);
      else
        LogManager.instance().log(this, Level.SEVERE,
            "After %s this node asked the leader to re-seed the cluster security documents and these did not "
                + "commit: %s. Until they do, this node serves requests against its own copy of them - which can "
                + "still hold a user dropped, a group narrowed or a token revoked while it was away. Re-POST this "
                + "node to %s on any member to retry", reason, String.join(", ", failed), "/api/v1/cluster/peer");
    } catch (final Exception e) {
      // Best effort by contract: this runs on a background worker with no caller, and a node that cannot reach
      // the leader right now is a node whose next restart - or next replicated security change - covers it.
      LogManager.instance().log(this, Level.WARNING,
          "Could not ask the leader to bring this node's security documents back in step after %s: %s", reason,
          e.getMessage());
    }
  }

  /**
   * Waits, bounded, for this node's state machine to reach the commit index it knows about. Best-effort by
   * contract, exactly like {@code HAServerPlugin.awaitLocalApply}: when the deadline passes the request is made
   * anyway, and the worst that costs is a comparison against documents that were about to arrive.
   */
  private static void awaitCatchUp(final RaftHAServer raft) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + CATCH_UP_WAIT_MS;
    while (System.currentTimeMillis() < deadline) {
      final long commitIndex = raft.getCommitIndex();
      if (commitIndex <= 0 || raft.getLastAppliedIndex() >= commitIndex)
        return;
      Thread.sleep(CATCH_UP_POLL_MS);
    }
    LogManager.instance().log(SecurityCatchUp.class, Level.FINE,
        "The security catch-up did not see this node reach the commit index within %dms; asking the leader anyway",
        CATCH_UP_WAIT_MS);
  }

  @Override
  public void close() {
    executor.shutdownNow();
  }
}
