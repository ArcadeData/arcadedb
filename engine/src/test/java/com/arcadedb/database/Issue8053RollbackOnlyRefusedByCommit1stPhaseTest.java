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
package com.arcadedb.database;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8053: the rollback-only marker of issue #7467 was read at ONE of the two commit entry points.
 * <p>
 * {@link TransactionContext#setRollbackOnly} exists so a transaction whose half-written record could not be
 * freed cannot be published - "committing it would publish a record no index entry points at". Its only read
 * was at the top of {@link TransactionContext#commit()}, and {@code commit()} is not the method an HA node
 * calls: {@code RaftReplicatedDatabase.commit()} takes the two phases apart itself so it can put the WAL bytes
 * on the wire between them, and drives {@link TransactionContext#commit1stPhase(boolean)} directly. So on the
 * deployment where the consequence is worst - the state is not merely committed locally but shipped as a
 * {@code TX_ENTRY} and applied on every follower - the marker did nothing.
 * <p>
 * The refusal now lives in {@code commit1stPhase}, the method every commit path converges on and the one that
 * already decides whether there is anything to publish. {@code commit()} keeps it by calling that method, and
 * no second copy of the guard can drift away from the first - a guard with two copies is the shape that
 * produced this gap.
 * <p>
 * The {@code commit1stPhase} rows here are the engine-level pin; the replicated wrapper's own two call sites
 * are pinned in {@code Issue8053RollbackOnlyRefusedOnTheReplicatedCommitPathTest} in the {@code ha-raft}
 * module.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue8053RollbackOnlyRefusedByCommit1stPhaseTest extends TestHelper {
  private static final String TYPE   = "Keyed8053";
  private static final String REASON = "record #1:0 could not be taken back after its indexing refused it";

  @Override
  protected void beginTest() {
    final DocumentType type = database.getSchema().createDocumentType(TYPE);
    type.createProperty("name", Type.STRING);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, TYPE, "name");
  }

  /**
   * The gap itself, on the leader's own argument value: the HA wrapper asks for phase 1 and never for
   * {@code commit()}, so phase 1 is what has to refuse. Before the fix this returned the WAL bytes.
   */
  @Test
  void phase1RefusesARollbackOnlyTransactionOnTheLeader() {
    assertPhase1Refuses(true);
  }

  /**
   * The same on a replica ORIGINATING its own write: {@code RaftReplicatedDatabase.commit()} passes its own
   * {@code isLeader()} through to phase 1, so {@code false} is a real production argument value and not a
   * variant invented for the test.
   */
  @Test
  void phase1RefusesARollbackOnlyTransactionOnAReplica() {
    assertPhase1Refuses(false);
  }

  /**
   * The standalone path keeps the behaviour issue #7467 gave it, now by inheritance rather than by its own
   * copy of the test: {@code LocalDatabase.commit()} reaches {@code commit()}, which reaches phase 1.
   */
  @Test
  void theStandaloneCommitStillRefusesIt() {
    database.begin();
    try {
      database.newDocument(TYPE).set("name", "in-flight").save();
      ((DatabaseInternal) database).getTransaction().setRollbackOnly(REASON);

      assertThatThrownBy(database::commit)
          .isInstanceOf(TransactionException.class)
          .hasMessageContaining("could not be taken back")
          .hasMessageContaining("Roll it back");
    } finally {
      if (database.isTransactionActive())
        database.rollback();
    }

    assertThat(database.countType(TYPE, false)).as("nothing of the refused transaction may be durable").isZero();
  }

  /** The guard must not cost a healthy transaction its commit: phase 1 still publishes when nothing marked it. */
  @Test
  void anUnmarkedTransactionStillCommitsThroughPhase1() {
    database.begin();
    final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
    database.newDocument(TYPE).set("name", "healthy").save();

    final TransactionContext.TransactionPhase1 phase1 = tx.commit1stPhase(true);
    assertThat(phase1).as("an unmarked transaction still produces the WAL bytes to publish").isNotNull();
    tx.commit2ndPhase(phase1);

    assertThat(database.countType(TYPE, false)).isEqualTo(1);
  }

  /**
   * Drives phase 1 the way {@code RaftReplicatedDatabase.commit()} drives it and asserts the three things the
   * caller needs: it is refused, it is refused BEFORE the transaction leaves {@code BEGUN} so the caller's own
   * error handling can still roll it back, and nothing of it is durable afterwards.
   */
  private void assertPhase1Refuses(final boolean isLeader) {
    database.begin();
    try {
      database.newDocument(TYPE).set("name", "in-flight").save();

      final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
      tx.setRollbackOnly(REASON);

      assertThatThrownBy(() -> tx.commit1stPhase(isLeader))
          .as("the WAL bytes of a transaction that cannot be published must never be produced")
          .isInstanceOf(TransactionException.class)
          .hasMessageContaining("could not be taken back")
          .hasMessageContaining("Roll it back");

      assertThat(tx.getStatus())
          .as("refused before the phase starts, so the caller can still roll it back")
          .isEqualTo(TransactionContext.STATUS.BEGUN);
      assertThat(tx.getRollbackOnlyReason()).isEqualTo(REASON);
    } finally {
      if (database.isTransactionActive())
        database.rollback();
    }

    assertThat(database.countType(TYPE, false)).as("nothing of the refused transaction may be durable").isZero();

    // The refusal belonged to that transaction, not to the context the next begin() reuses.
    database.transaction(() -> database.newDocument(TYPE).set("name", "after-" + isLeader).save());
    assertThat(database.countType(TYPE, false)).isEqualTo(1);
  }
}
