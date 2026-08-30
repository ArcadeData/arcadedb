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
 * Regression test for issue #6848: the #4790 abandoned-entry mark used to be published AFTER the
 * entry had already been dispatched, so the state machine could apply the entry on the leader
 * BEFORE the mark existed - and origin-skip it, silently losing the write on the leader.
 * <p>
 * {@link Issue5410AbandonedTicketReleaseIT} exercises the same fault but wins the race by accident:
 * its predicate returns immediately, so the client thread publishes the mark in microseconds while
 * the group-commit flusher still has to hand the entry to Ratis. That made the class pass only when
 * a long-running IT had warmed the JVM before it (issue #6848); on a cold JVM the apply thread got
 * there first and the leader ended up one vertex short.
 * <p>
 * Here the ordering is made deterministic instead of hoped for: the injected predicate sleeps before
 * returning {@code true}, which parks the committing thread past the point where Ratis has committed
 * and applied the entry on the leader. Before the fix the leader origin-skips it and never converges.
 *
 * @see Issue5410AbandonedTicketReleaseIT for the ticket-release half of the #4790/#5410 contract
 */
@Tag("slow")
class Issue6848AbandonedMarkRaceIT extends BaseRaftHATest {

  private static final String VERTEX_TYPE = "AbandonedMarkRace";

  /**
   * How long the committing thread is parked after its entry has been enqueued for dispatch. It only
   * has to exceed a local Raft round trip (single-digit ms on loopback); two seconds is generous so
   * the ordering is decided by the fix and not by the machine the test runs on.
   */
  private static final long PARK_COMMITTER_MS = 2_000;

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
  void abandonedEntryIsAppliedOnTheLeaderEvenWhenTheApplyBeatsTheMark() throws Exception {
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
      v.set("id", 1);
      v.set("name", "baseline");
      v.save();
    });
    assertClusterConsistency();

    // Arm the one-shot #4790 fault. The predicate runs on the committing thread AFTER the entry has
    // been handed to the group-commit queue, so sleeping here lets the flusher dispatch it, Ratis
    // commit it and the state machine apply it on this very leader - all before the committing
    // thread gets to record that its phase 2 was abandoned.
    final AtomicBoolean faultFired = new AtomicBoolean(false);
    RaftGroupCommitter.TEST_FORCE_DISPATCHED_TIMEOUT = entry -> {
      try {
        final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(ByteString.copyFrom(entry));
        if (decoded.type() == RaftLogEntryType.TX_ENTRY
            && getDatabaseName().equals(decoded.databaseName())
            && faultFired.compareAndSet(false, true)) {
          LogManager.instance().log(this, Level.INFO,
              "TEST: forcing dispatched-timeout for a TX_ENTRY on db=%s after parking the committer for %d ms",
              decoded.databaseName(), PARK_COMMITTER_MS);
          Thread.sleep(PARK_COMMITTER_MS);
          return true;
        }
      } catch (final InterruptedException ie) {
        Thread.currentThread().interrupt();
      } catch (final Exception ignore) {
        // Not a decodable/target entry; never force the timeout for it.
      }
      return false;
    };

    boolean threw = false;
    try {
      leaderDb.begin();
      final MutableVertex v = leaderDb.newVertex(VERTEX_TYPE);
      v.set("id", 2);
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

    // The whole point: the LEADER must hold the abandoned entry too. Before the fix it origin-skipped
    // the entry (the mark did not exist yet when applyTxEntry ran) and stayed one vertex behind the
    // followers for the rest of its uptime.
    for (int i = 0; i < getServerCount(); i++)
      assertThat(awaitCountOn(i, VERTEX_TYPE, 2L))
          .as("Node %d must hold both vertices", i)
          .isEqualTo(2L);

    // And the #5410 contract still holds: whoever applied the entry released its phase-2 ticket.
    assertThat(awaitNoPendingPhase2(leaderStateMachine))
        .as("applying the abandoned entry must release its phase-2 ticket, unpinning log compaction")
        .isTrue();
    assertThat(leaderStateMachine.lowestPendingLocalPhase2ReplayFloor())
        .as("no replay floor may remain pinned once the abandoned entry is applied")
        .isEqualTo(-1L);

    assertClusterConsistency();
  }

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
