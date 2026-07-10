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
 * Regression test for issue #4790: the leader silently loses a committed write when replication
 * times out <b>after</b> the entry has already been dispatched to Ratis (phantom commit + origin-skip).
 * <p>
 * <b>The bug:</b> on the leader, {@code RaftReplicatedDatabase.commit()} captures the WAL (phase 1),
 * dispatches it to Ratis and waits for quorum, then applies it locally (phase 2). To avoid
 * double-applying, {@code ArcadeStateMachine.applyTxEntry} origin-skips entries that this node
 * originated. If the quorum wait times out <i>after</i> the entry was dispatched, the old code rolled
 * back and threw without running phase 2 - yet Ratis later reached quorum and the entry was applied
 * on every follower while the leader's own state machine origin-skipped it. The write landed on all
 * followers but never on the leader: a silent, permanent divergence.
 * <p>
 * <b>The fix:</b> the dispatched-but-timed-out path now surfaces a dedicated
 * {@link ReplicationDispatchedTimeoutException}; {@code commit()} marks the transaction as
 * abandoned-but-possibly-committed, and if the entry does commit, {@code applyTxEntry} applies it
 * locally instead of origin-skipping it - so the leader converges with the followers.
 * <p>
 * The test uses {@link RaftGroupCommitter#TEST_FORCE_DISPATCHED_TIMEOUT} to deterministically force
 * the "entry dispatched, quorum wait timed out" window for a single transaction: the entry is still
 * dispatched and committed for real (so the followers apply it), but {@code submitAndWait} abandons
 * the wait with the same exception the production grace-expiry path raises.
 */
@Tag("slow")
class Issue4790PhantomCommitOriginSkipIT extends BaseRaftHATest {

  private static final String VERTEX_TYPE = "PhantomCommit";

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
  void leaderAppliesWriteWhenReplicationTimesOutAfterDispatch() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());

    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType(VERTEX_TYPE))
        leaderDb.getSchema().createVertexType(VERTEX_TYPE, 1);
    });

    // Baseline: one vertex, replicated cleanly to every node.
    leaderDb.transaction(() -> {
      final MutableVertex v = leaderDb.newVertex(VERTEX_TYPE);
      v.set("name", "baseline");
      v.save();
    });
    assertClusterConsistency();
    for (int i = 0; i < getServerCount(); i++)
      assertThat(getServerDatabase(i, getDatabaseName()).countType(VERTEX_TYPE, true))
          .as("Baseline count on node %d", i).isEqualTo(1L);

    // Arm a one-shot fault: the next TX_ENTRY for this database is dispatched to Ratis for real, but
    // submitAndWait abandons the quorum wait with ReplicationDispatchedTimeoutException - exactly the
    // production "dispatched, then timed out" window.
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

    // The faulted commit dispatches the entry (it WILL commit on the followers) but reports an
    // indeterminate replication result to the client.
    boolean threw = false;
    try {
      leaderDb.begin();
      final MutableVertex v = leaderDb.newVertex(VERTEX_TYPE);
      v.set("name", "phantom");
      v.save();
      leaderDb.commit();
    } catch (final ReplicationDispatchedTimeoutException expected) {
      threw = true;
      LogManager.instance().log(this, Level.INFO,
          "TEST: commit reported indeterminate replication as expected: %s", expected.getMessage());
    } finally {
      if (leaderDb.isTransactionActive())
        leaderDb.rollback();
    }

    assertThat(faultFired.get()).as("The dispatched-timeout fault must have fired").isTrue();
    assertThat(threw).as("commit() must surface the indeterminate replication error").isTrue();

    // The entry was dispatched and will reach quorum: every node - INCLUDING the leader - must
    // converge to the second vertex. Before the fix the leader stayed at 1 (origin-skip dropped it).
    final long deadline = System.currentTimeMillis() + 30_000;
    boolean converged = false;
    while (System.currentTimeMillis() < deadline) {
      for (int i = 0; i < getServerCount(); i++)
        waitForReplicationIsCompleted(i);
      converged = true;
      for (int i = 0; i < getServerCount(); i++) {
        if (getServerDatabase(i, getDatabaseName()).countType(VERTEX_TYPE, true) != 2L) {
          converged = false;
          break;
        }
      }
      if (converged)
        break;
      Thread.sleep(250);
    }

    for (int i = 0; i < getServerCount(); i++)
      assertThat(getServerDatabase(i, getDatabaseName()).countType(VERTEX_TYPE, true))
          .as("Node %d must hold both vertices (leader must not silently lose the write)", i)
          .isEqualTo(2L);

    // Pages must be byte-identical across the cluster.
    assertClusterConsistency();
  }
}
