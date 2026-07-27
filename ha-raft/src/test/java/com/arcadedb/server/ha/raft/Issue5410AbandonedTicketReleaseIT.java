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
import com.arcadedb.database.Database;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.log.LogManager;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5410, end-to-end through the real {@code applyTxEntry}.
 * <p>
 * A #4790 dispatched-timeout leaves a locally-originated entry abandoned by the leader's phase 2,
 * and the #5407 guard deliberately keeps that entry's phase-2 ticket held so the entry stays inside
 * the Raft replay window. When the entry then reaches quorum and the state machine applies it
 * locally - the normal, non-crash resolution of a #4790 timeout - its pages become durable, but
 * before this fix nothing released the ticket. The snapshot checkpoint stayed clamped to that
 * entry's replay floor, so Ratis could not purge the Raft log past it for the rest of the node's
 * uptime (feeding #5345, unbounded Raft log growth).
 * <p>
 * The fix carries the ticket on the abandoned marker and releases it on the branch that actually
 * applies the transaction. This test reuses the #4790 fault injection to produce a real abandoned
 * entry, and asserts the leader stops pinning the checkpoint once the entry converges.
 *
 * @see Issue4790PhantomCommitOriginSkipIT for the lost-write behaviour this builds on
 */
@Tag("slow")
class Issue5410AbandonedTicketReleaseIT extends BaseRaftHATest {

  private static final String VERTEX_TYPE = "AbandonedTicket";

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @AfterEach
  void clearHooks() {
    RaftGroupCommitter.TEST_FORCE_DISPATCHED_TIMEOUT = null;
  }

  @Test
  void abandonedEntryReleasesItsPhase2TicketOnceApplied() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    final ArcadeStateMachine leaderStateMachine = getRaftPlugin(leaderIndex).getRaftHAServer().getStateMachine();

    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType(VERTEX_TYPE))
        leaderDb.getSchema().createVertexType(VERTEX_TYPE, 1);
    });

    leaderDb.transaction(() -> {
      final MutableVertex v = leaderDb.newVertex(VERTEX_TYPE);
      v.set("name", "baseline");
      v.save();
    });
    assertClusterConsistency();

    // A cleanly committed write must leave nothing pinned: this is the baseline the faulted commit
    // is compared against, and it also proves the assertion below can actually observe a release.
    assertThat(awaitNoPendingPhase2(leaderStateMachine))
        .as("a cleanly replicated commit must not pin the snapshot checkpoint")
        .isTrue();

    // Arm the one-shot #4790 fault: the entry is dispatched to Ratis for real (so it WILL commit on
    // the followers) but the quorum wait abandons with ReplicationDispatchedTimeoutException.
    final AtomicBoolean faultFired = new AtomicBoolean(false);
    RaftGroupCommitter.TEST_FORCE_DISPATCHED_TIMEOUT = entry -> {
      try {
        final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(ByteString.copyFrom(entry));
        if (decoded.type() == RaftLogEntryType.TX_ENTRY
            && getDatabaseName().equals(decoded.databaseName())
            && faultFired.compareAndSet(false, true)) {
          LogManager.instance().log(this, Level.INFO,
              "TEST: forcing dispatched-timeout for a TX_ENTRY on db=%s", decoded.databaseName());
          return true;
        }
      } catch (final Exception ignore) {
        // Not a decodable/target entry; never force the timeout for it.
      }
      return false;
    };

    boolean threw = false;
    try {
      leaderDb.begin();
      final MutableVertex v = leaderDb.newVertex(VERTEX_TYPE);
      v.set("name", "abandoned");
      v.save();
      leaderDb.commit();
    } catch (final ReplicationDispatchedTimeoutException expected) {
      threw = true;
    } finally {
      if (leaderDb.isTransactionActive())
        leaderDb.rollback();
    }

    assertThat(faultFired.get()).as("The dispatched-timeout fault must have fired").isTrue();
    assertThat(threw).as("commit() must surface the indeterminate replication error").isTrue();

    // The abandoned entry reaches quorum and every node - including the leader, via the
    // abandonedLocalTransactions path - converges on both vertices (the #4790 contract).
    final long deadline = System.currentTimeMillis() + 30_000;
    boolean converged = false;
    while (System.currentTimeMillis() < deadline && !converged) {
      for (int i = 0; i < getServerCount(); i++)
        waitForReplicationIsCompleted(i);
      converged = true;
      for (int i = 0; i < getServerCount(); i++)
        if (getServerDatabase(i, getDatabaseName()).countType(VERTEX_TYPE, true) != 2L) {
          converged = false;
          break;
        }
      if (!converged)
        Thread.sleep(250);
    }

    for (int i = 0; i < getServerCount(); i++)
      assertThat(getServerDatabase(i, getDatabaseName()).countType(VERTEX_TYPE, true))
          .as("Node %d must hold both vertices", i)
          .isEqualTo(2L);

    // The #5410 assertion: the entry's pages are on disk here, so the ticket taken before the
    // abandoned replication must have been released. Before the fix this stayed at 1 until restart.
    assertThat(awaitNoPendingPhase2(leaderStateMachine))
        .as("applying the abandoned entry must release its phase-2 ticket, unpinning log compaction")
        .isTrue();

    // With nothing pinned the checkpoint is free to move again, which is what Ratis needs before it
    // can purge the Raft log - the operational symptom the issue reports.
    assertThat(leaderStateMachine.lowestPendingLocalPhase2ReplayFloor())
        .as("no replay floor may remain pinned once the abandoned entry is applied")
        .isEqualTo(-1L);
    assertThat(leaderStateMachine.oldestPendingLocalPhase2HeldMs())
        .as("no ticket age may remain once the abandoned entry is applied")
        .isZero();

    assertClusterConsistency();
  }

  /**
   * Waits for the leader to stop holding phase-2 tickets. Polled rather than asserted outright
   * because the release happens on the Raft apply thread, asynchronously from the client commit that
   * observed the timeout.
   */
  private static boolean awaitNoPendingPhase2(final ArcadeStateMachine stateMachine) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + 30_000;
    while (System.currentTimeMillis() < deadline) {
      if (stateMachine.pendingLocalPhase2Count() == 0)
        return true;
      Thread.sleep(100);
    }
    return false;
  }
}
