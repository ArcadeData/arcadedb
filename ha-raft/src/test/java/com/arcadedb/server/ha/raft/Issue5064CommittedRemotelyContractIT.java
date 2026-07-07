/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.TransactionCommittedRemotelyException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.log.LogManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * #5064: the user-visible contract for a post-quorum local commit failure. When the Raft quorum durably
 * committed the transaction but the leader's LOCAL phase-2 apply fails, the application must receive
 * {@link TransactionCommittedRemotelyException} ("committed cluster-wide, do NOT retry") instead of a
 * generic commit failure - and the identities of records created in the transaction must remain valid (they
 * are the identities the cluster committed), so an application-level retry no longer INSERTS DUPLICATES of
 * already-committed records.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5064CommittedRemotelyContractIT extends BaseRaftHATest {

  private static final String VERTEX_TYPE = "Contract5064";

  @AfterEach
  void disarmHooks() {
    RaftReplicatedDatabase.TEST_PHASE2_COMMIT_FAULT = null;
  }

  @Test
  void postQuorumLocalFailureSurfacesTheCommittedRemotelyContract() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType(VERTEX_TYPE))
        leaderDb.getSchema().createVertexType(VERTEX_TYPE, 1);
    });
    // Baseline vertex: registers the property name in the dictionary so its INTERNAL transaction (fired
    // lazily from save() -> Dictionary.getIdByName) does not consume the single-shot fault below.
    leaderDb.transaction(() -> leaderDb.newVertex(VERTEX_TYPE).set("k", "baseline").save());
    assertClusterConsistency();

    // Arm a single-shot phase-2 fault: replication reaches quorum, then the local apply throws.
    final AtomicBoolean faultFired = new AtomicBoolean(false);
    RaftReplicatedDatabase.TEST_PHASE2_COMMIT_FAULT = dbName -> {
      if (!faultFired.compareAndSet(false, true))
        return;
      LogManager.instance().log(Issue5064CommittedRemotelyContractIT.class, Level.INFO,
          "TEST: injecting phase-2 commit fault for db=%s on leader %d", dbName, leaderIndex);
      throw new ConcurrentModificationException("TEST fault injection: simulated post-quorum local failure (#5064)");
    };

    final MutableVertex held;
    leaderDb.begin();
    held = leaderDb.newVertex(VERTEX_TYPE);
    held.set("k", "committed-remotely");
    held.save();
    final Object identityAtSave = held.getIdentity();
    assertThat(identityAtSave).as("save() assigns the optimistic identity").isNotNull();

    try {
      leaderDb.commit();
      fail("the faulted commit must not report plain success");
    } catch (final TransactionCommittedRemotelyException e) {
      // The contract: distinct type, actionable message.
      assertThat(e.getMessage()).contains("committed cluster-wide").contains("Do NOT retry");
    }

    assertThat(faultFired.get()).as("the phase-2 fault hook must have fired").isTrue();

    // #4940-composition: the user-held record keeps the identity the cluster committed - it must NOT have
    // been reset to provisional (that reset is what turned application retries into duplicate inserts).
    assertThat(held.getIdentity())
        .as("the identity of a remotely-committed record must survive the local failure (#5064)")
        .isEqualTo(identityAtSave);

    // The data is genuinely committed: a post-step-down write drives the log forward (same pattern as
    // Issue4740Phase2ReconcileIT), then every node must serve BOTH vertices - the baseline and the
    // remotely-committed one whose local apply failed.
    final int newLeaderIndex = findLeaderIndex();
    assertThat(newLeaderIndex).as("A leader must be elected after the phase-2 step-down").isGreaterThanOrEqualTo(0);
    final Database newLeaderDb = getServerDatabase(newLeaderIndex, getDatabaseName());
    newLeaderDb.transaction(() -> newLeaderDb.newVertex(VERTEX_TYPE).set("k", "post-stepdown").save());

    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    assertClusterConsistency();
    for (int i = 0; i < getServerCount(); i++) {
      final Database db = getServerDatabase(i, getDatabaseName());
      assertThat(db.countType(VERTEX_TYPE, true))
          .as("Node %d must hold the baseline, the remotely-committed and the post-stepdown vertices", i)
          .isEqualTo(3L);
    }
  }
}
