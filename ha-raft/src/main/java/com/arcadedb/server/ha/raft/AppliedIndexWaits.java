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

import com.arcadedb.database.Database;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.log.LogManager;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.logging.Level;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.exceptions.NotLeaderException;

/**
 * The waits for the local state machine to reach an index: a read-your-writes or linearizable read waits until this node
 * has applied the index it must see, bounded by the quorum timeout, and gives up early - with a refusal that says why -
 * when this node is removed from the configuration or a stale read floor stands in the way.
 * <p>
 * Split out of {@link RaftHAServer}, whose read-consistency entry points wait through it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class AppliedIndexWaits {
  private final RaftHAServer ha;

  // Upper bound on a single applyNotifier.wait(...) call before the loop re-checks the apply-index
  // condition on its own, even without an intervening notifyApplied() call. notifyApplied() has a
  // single call site (ArcadeStateMachine.applyTransaction()); any other path that advances the
  // applied index - reinitialize() after a snapshot install and notifyInstallSnapshotFromLeader() are
  // the two known ones (issue #5846) - now also calls notifyApplied() explicitly, but that call graph
  // is not enforced by the compiler. This bound caps the cost of a future missed notification at this
  // interval instead of the full quorumTimeout.
  private static final long                  APPLY_WAIT_RECHECK_INTERVAL_MS = 1000L;

  // Bumped on every Raft configuration change this node observes (issue #9606): a consistent read waiting in
  // awaitAppliedIndex() asks whether this node is still a member only when it moved, or once per recheck interval
  private final    AtomicLong                membershipChanges     = new AtomicLong();

  AppliedIndexWaits(final RaftHAServer ha) {
    this.ha = ha;
  }

  /**
   * Wakes every consistent read blocked in {@link RaftHAServer#waitForAppliedIndexForRead} so it asks again whether this node is still a
   * member of the Raft configuration (issue #9606). Called where this node learns of a configuration change: the state
   * machine's configuration callback and a removal this node submitted, its own {@code leaveCluster()} included.
   * <p>
   * Not the only way a waiting read learns of a removal, and it cannot be: a removed node is usually cut off before it
   * applies the entry that removes it, so its state machine is never told. The read also asks once per
   * {@link #APPLY_WAIT_RECHECK_INTERVAL_MS}, which bounds the wait there instead of the quorum timeout.
   */
  public void notifyMembershipChanged() {
    membershipChanges.incrementAndGet();
    synchronized (ha.applyNotifier) {
      ha.applyNotifier.notifyAll();
    }
  }

  /**
   * The refusal of a consistent read on a node that is no longer a member of the Raft configuration (issues #9590, #9606):
   * retryable, because a member of the cluster can serve it.
   */
  static NeedRetryException readRefusedWhileRemovedFromConfiguration(final String databaseName,
      final Database.READ_CONSISTENCY consistency) {
    return new NeedRetryException("Database '" + databaseName + "' cannot serve a " + consistency + " read on this server: "
        + "it is not a member of the Raft cluster's configuration any more (it left or was removed), so it receives no "
        + "entries and cannot reach the index this read needs. Send the request to a server of the cluster, use EVENTUAL "
        + "consistency, or add this server back to it");
  }

  /**
   * Waits until the condition holds, re-evaluating it whenever the state machine applies an entry.
   *
   * @return {@code false} when the timeout elapsed first
   */
  public boolean awaitApplied(final BooleanSupplier condition, final long timeoutMs) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + timeoutMs;
    // The condition may read a page (a disk read on a cache miss), so it is evaluated OUTSIDE the monitor that the
    // apply thread takes after every entry: the monitor is only used to park between evaluations.
    while (!condition.getAsBoolean()) {
      final long remaining = deadline - System.currentTimeMillis();
      if (remaining <= 0)
        return false;
      synchronized (ha.applyNotifier) {
        ha.applyNotifier.wait(Math.min(remaining, 50L));
      }
    }
    return true;
  }

  /**
   * The apply wait behind both public forms.
   *
   * @param readConsistency the consistency of the read waiting, which then gives up on a removal from the configuration;
   *                        {@code null} for any other waiter
   */
  void awaitAppliedIndex(final String databaseName, final long targetIndex, final boolean throwOnTimeout,
      final Database.READ_CONSISTENCY readConsistency) {
    if (targetIndex <= 0)
      return;
    try {
      final long deadline = System.currentTimeMillis() + ha.quorumTimeout;
      // The configuration is read on entry, then whenever it changed, and once per recheck interval: not on every entry
      // applied, which wakes every waiter
      long membershipSeen = membershipChanges.get();
      long nextMembershipCheck = 0L;
      while (true) {
        // Outside the monitor: the configuration is read from the Ratis division, and holding applyNotifier across that
        // read would stall notifyApplied() - the apply thread - and every other waiter behind it. The applied index is
        // still re-checked under the monitor before each wait, so no notification can be missed between the two
        if (readConsistency != null && ha.getTrustedAppliedIndex(databaseName) < targetIndex) {
          final long now = System.currentTimeMillis();
          final long membership = membershipChanges.get();
          if (membership != membershipSeen || now >= nextMembershipCheck) {
            membershipSeen = membership;
            nextMembershipCheck = now + APPLY_WAIT_RECHECK_INTERVAL_MS;
            if (ha.isRemovedFromConfiguration()) {
              LogManager.instance().log(ha, Level.INFO,
                  "%s read on database '%s' refused while waiting for index %d (applied=%d): this server is no longer a "
                      + "member of the Raft configuration", readConsistency, databaseName, targetIndex,
                  ha.getTrustedAppliedIndex(databaseName));
              throw readRefusedWhileRemovedFromConfiguration(databaseName, readConsistency);
            }
          }
        }
        synchronized (ha.applyNotifier) {
          // The one normal exit: the index is applied. Every other way out returns or throws below
          if (ha.getTrustedAppliedIndex(databaseName) >= targetIndex)
            break;
          if (!throwOnTimeout && (getStaleSnapshotAppliedFloor() >= 0 || getDatabaseAppliedFloor(databaseName) >= 0)) {
            // No explicit "targetIndex > floor" test is needed, and adding one would be dead code:
            // reaching this line means the loop condition held, i.e. getTrustedAppliedIndex() (pinned at
            // the floor for the whole life of an outstanding gap, since Ratis' own counter starts at the
            // marker index - above the floor by construction - and only grows) is below targetIndex.
            //
            // That invariant is established in ArcadeStateMachine.reinitialize()'s snapshot-gap branch:
            // it publishes the floor as persistedApplied and only when
            // snapshotIndex > persistedApplied + HA_SNAPSHOT_GAP_TOLERANCE, so marker > floor always
            // holds while the floor is outstanding. Anything that changes what that branch publishes
            // must revisit this reasoning.
            //
            // The same holds for a per-database floor (issue #6760): it is published exactly when the install
            // could not bring THIS database to the snapshot index, so only a later resync can close it.
            //
            // READ_YOUR_WRITES / bookmark: the target therefore sits inside a gap that only the pending
            // snapshot resync can fill, so waiting out the quorum timeout cannot change the outcome.
            // Degrade now with the same contract the timeout branch below applies (issue #6111).
            LogManager.instance().log(ha, Level.WARNING,
                "READ_YOUR_WRITES target %d is inside a pending snapshot resync gap on database '%s' "
                    + "(trusted applied=%d): consistency degraded to EVENTUAL without waiting",
                targetIndex, databaseName, ha.getTrustedAppliedIndex(databaseName));
            return;
          }
          final long remaining = deadline - System.currentTimeMillis();
          if (remaining <= 0) {
            if (throwOnTimeout) {
              LogManager.instance().log(ha, Level.WARNING,
                  "LINEARIZABLE read failed: local apply timeout applied=%d < readIndex=%d after %dms "
                      + "(staleSnapshotFloor=%d, database '%s' floor=%d, failing read)",
                  ha.getTrustedAppliedIndex(databaseName), targetIndex, ha.quorumTimeout, getStaleSnapshotAppliedFloor(),
                  databaseName, getDatabaseAppliedFloor(databaseName));
              throw new ReplicationException(
                  "LINEARIZABLE read timed out waiting for local apply: applied=" + ha.getTrustedAppliedIndex(databaseName)
                      + " < readIndex=" + targetIndex);
            }
            LogManager.instance().log(ha, Level.WARNING,
                "READ_YOUR_WRITES consistency timeout: applied=%d < target=%d (consistency degraded to EVENTUAL)",
                ha.getTrustedAppliedIndex(databaseName), targetIndex);
            return;
          }
          ha.applyNotifier.wait(Math.min(remaining, APPLY_WAIT_RECHECK_INTERVAL_MS));
        }
      }
      HALog.log(ha, HALog.TRACE, "Apply wait complete: applied >= target=%d", targetIndex);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      if (throwOnTimeout)
        throw new ReplicationException("LINEARIZABLE read interrupted while waiting for local apply", e);
    }
  }

  /**
   * Blocks until the local state machine's last-applied index reaches the current commit index.
   * Used for {@code READ_YOUR_WRITES} consistency: the client provides a commit index bookmark
   * from a previous write, and the replica waits until that entry has been applied locally
   * before serving the read.
   * <p>
   * Uses {@link RaftHAServer#applyNotifier}, notified explicitly by every code path that advances the applied
   * index (see {@link RaftHAServer#notifyApplied()}) and re-checked on its own at least every
   * {@link #APPLY_WAIT_RECHECK_INTERVAL_MS} regardless, with an overall timeout of
   * {@link RaftHAServer#quorumTimeout} milliseconds. If the deadline is reached before catch-up, the method
   * returns silently (reads may be slightly stale rather than failing).
   */
  public void waitForLocalApply() {
    try {
      final long commitIndex = ha.getCommitIndex();
      if (commitIndex <= 0)
        return;

      final long deadline = System.currentTimeMillis() + ha.quorumTimeout;
      synchronized (ha.applyNotifier) {
        while (ha.getTrustedAppliedIndex() < commitIndex) {
          if (getStaleSnapshotAppliedFloor() >= 0) {
            // Same reasoning as waitForAppliedIndex: the loop condition already proves commitIndex sits
            // above the floor, so no explicit comparison is needed. Only the pending snapshot resync can
            // close the gap; this waiter is best-effort by contract, so degrade now instead of burning
            // the whole quorum timeout (issue #6111).
            LogManager.instance().log(ha, Level.WARNING,
                "waitForLocalApply: commit=%d is inside a pending snapshot resync gap (trusted applied=%d), "
                    + "returning without waiting (reads may be stale)",
                commitIndex, ha.getTrustedAppliedIndex());
            return;
          }
          final long remaining = deadline - System.currentTimeMillis();
          if (remaining <= 0) {
            HALog.log(ha, HALog.DETAILED, "waitForLocalApply timed out: applied=%d < commit=%d",
                ha.getTrustedAppliedIndex(), commitIndex);
            return;
          }
          ha.applyNotifier.wait(Math.min(remaining, APPLY_WAIT_RECHECK_INTERVAL_MS));
        }
      }
      HALog.log(ha, HALog.TRACE, "Local apply caught up: applied=%d >= commit=%d",
          ha.getTrustedAppliedIndex(), commitIndex);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (final Exception e) {
      HALog.log(ha, HALog.DETAILED, "waitForLocalApply failed: %s", e.getMessage());
    }
  }

  /**
   * The state machine's stale-snapshot read floor, or {@code -1} when there is none or the state machine
   * is not wired yet. See {@link ArcadeStateMachine#getStaleSnapshotAppliedFloor()} (issue #6111).
   */
  long getStaleSnapshotAppliedFloor() {
    final ArcadeStateMachine sm = ha.stateMachine;
    return sm == null ? -1 : sm.getStaleSnapshotAppliedFloor();
  }

  /**
   * The read floor of a single database, or {@code -1} when it has none, the state machine is not wired yet, or no
   * database was named. See {@link ArcadeStateMachine#getDatabaseAppliedFloor(String)} (issue #6760).
   */
  long getDatabaseAppliedFloor(final String databaseName) {
    if (databaseName == null)
      return -1;
    final ArcadeStateMachine sm = ha.stateMachine;
    return sm == null ? -1 : sm.getDatabaseAppliedFloor(databaseName);
  }

  /**
   * Sends a ReadIndex RPC to the Raft leader and returns the confirmed commit index.
   * The leader replies only after confirming its lease (or a majority heartbeat round)
   * AND after committing at least one entry in its own current term, guaranteeing that
   * the returned index covers every write that committed before this call - including
   * writes committed under a prior leader/term (Raft §6.4, the new-leader hazard handled
   * by Ratis {@code LeaderStateImpl.getReadIndex} / startup no-op entry).
   * <p>
   * IMPORTANT (issue: stale LINEARIZABLE follower read): in Apache Ratis 3.2.x a read-only
   * reply does NOT carry the read index in {@link RaftClientReply#getLogIndex()} - that
   * field is only populated for write replies and defaults to {@code 0} for reads
   * ({@code RaftServerImpl.processQueryFuture} builds the reply without {@code setLogIndex}).
   * Relying on {@code getLogIndex()} therefore yielded {@code 0}, which made
   * {@link RaftHAServer#waitForAppliedIndex(long, boolean)} a no-op and let a follower serve arbitrarily
   * stale local state. Instead we read the leader's quorum-confirmed commit index out of the
   * reply's {@code CommitInfoProto} list (every reply carries the commit index of the server
   * that produced it, which for a leader-routed read is the leader itself). The reply is only
   * produced after the leader's internal ReadIndex protocol confirmed leadership, so this
   * commit index is a safe linearizable read index.
   *
   * @param expectSelfIsLeader if true and the reply indicates this node is not leader,
   *                           a {@link ReplicationException} is thrown with leader info
   */
  public long fetchReadIndex(final boolean expectSelfIsLeader) {
    try {
      final RaftClientReply reply = ha.raftClient.io().sendReadOnly(Message.EMPTY);
      if (reply.isSuccess())
        return extractLeaderCommitIndex(reply);

      final NotLeaderException nle = reply.getNotLeaderException();
      if (nle != null) {
        final var suggestedLeader = nle.getSuggestedLeader();
        if (expectSelfIsLeader) {
          final String leaderAddr = suggestedLeader != null ? suggestedLeader.getId().toString() : null;
          throw new ReplicationException(
              "Lost leadership during ReadIndex" + (leaderAddr != null ? ", new leader: " + leaderAddr : ""));
        }
        throw new ReplicationException("ReadIndex failed: leader unavailable");
      }
      throw new ReplicationException("ReadIndex failed: " + reply);
    } catch (final ReplicationException e) {
      throw e;
    } catch (final IOException e) {
      throw new ReplicationException("ReadIndex RPC failed: " + e.getMessage(), e);
    }
  }

  /**
   * Extracts the quorum-confirmed commit index of the server that produced {@code reply}
   * (for a leader-routed read-only request, that is the leader) from the reply's
   * {@code CommitInfoProto} list. See {@link #fetchReadIndex(boolean)} for why we cannot use
   * {@link RaftClientReply#getLogIndex()} on a read reply (it is always {@code 0} in Ratis 3.2.x).
   *
   * @throws ReplicationException if the reply does not carry a commit index for its own server,
   *                              which must NOT be silently treated as index 0 (that would
   *                              re-introduce the stale-read bug).
   */
  private long extractLeaderCommitIndex(final RaftClientReply reply) {
    final long commitIndex = RaftHAServer.reportedCommitIndexOf(reply.getCommitInfos(), reply.getServerId());
    if (commitIndex != RaftHAServer.NO_COMMIT_INFO)
      return commitIndex;
    // The read succeeded but the leader's commit index is missing from the reply. We cannot
    // prove linearizability, so fail loudly rather than serve a possibly-stale read.
    throw new ReplicationException(
        "ReadIndex reply did not include the leader commit index; cannot guarantee linearizable read");
  }
}
