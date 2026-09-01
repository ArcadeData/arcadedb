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
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6964, unit-level pin: {@code TransactionContext.commit1stPhase(boolean isLeader)} must replay the
 * transaction's queued index operations regardless of {@code isLeader}. This is the exact defect - on a Raft
 * replica originating its own commit, {@code isLeader} is {@code false} while the record itself still needs to be
 * committed, and the index replay was wrongly gated on that flag - but it is pinned here directly on
 * {@code TransactionContext}, without spinning up a Raft cluster, so a regression fails fast in the ordinary
 * engine unit-test lane instead of only in the slower {@code ha-raft} integration test
 * ({@code ReplicaInsertUniqueIndexInvisibleIT}) that exercises the same invariant end to end over a real cluster.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6964NonLeaderCommit1stPhaseIndexReplayTest extends TestHelper {

  private static final String TYPE = "Issue6964Singleton";

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createDocumentType(TYPE).createProperty("name", Type.STRING);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, TYPE, "name");
    });
  }

  @Test
  void nonLeaderCommit1stPhaseStillReplaysQueuedIndexChanges() {
    database.begin();
    database.newDocument(TYPE).set("name", "value").save();

    final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
    final TransactionContext.TransactionPhase1 phase1 = tx.commit1stPhase(false);

    assertThat(tx.isIndexChangesReplayed())
        .as("#6964: index replay must not depend on isLeader - it is what makes a replica's own WAL bytes carry "
            + "its index pages")
        .isTrue();

    tx.commit2ndPhase(phase1);

    database.begin();
    assertThat(database.lookupByKey(TYPE, "name", "value").hasNext())
        .as("the index entry queued before commit1stPhase(false) must be visible after commit")
        .isTrue();
    database.commit();
  }
}
