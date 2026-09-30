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
import com.arcadedb.index.Index;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #8686: a replica transaction prepared BEFORE the replica applied a committed
 * {@code CREATE INDEX} used to be accepted by the leader after the DDL, and every node applied its WAL as it was, which
 * carries no page changes for the new index. The record was then in the type and not in the index on every node
 * (records N+1, index N).
 * <p>
 * Made deterministic rather than raced: the replica opens a transaction and saves its record (the index changes are staged
 * as the record is saved, under the schema the replica holds then), the leader creates the index, the replica applies it,
 * and only then does the replica commit. The leader must refuse that commit with a retryable conflict; the same insert
 * repeated in a new transaction, prepared under the new schema, must be accepted and land in the index everywhere.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8686StaleSchemaReplicaTransactionIT extends BaseRaftHATest {
  private static final String TYPE_NAME = "Issue8686Doc";

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Test
  void aReplicaTransactionPreparedBeforeTheIndexWasAppliedIsRefusedAndTheRetryIsIndexed() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    final int replicaIndex = leaderIndex == 0 ? 1 : 0;

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> leaderDb.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create()
        .getOrCreateProperty("id", Type.LONG));
    leaderDb.transaction(() -> {
      for (int i = 0; i < 10; i++)
        leaderDb.newDocument(TYPE_NAME).set("id", (long) i).save();
    });
    waitForReplicationIsCompleted(replicaIndex);

    // The capability negotiation is what lets the replica state the index; its background monitor may not have finished its
    // first round yet, and without it the entry carries no index and is (by design) never refused.
    final RaftHAServer replicaRaft = ((RaftHAPlugin) getServer(replicaIndex).getHA()).getRaftHAServer();
    final long deadline = System.currentTimeMillis() + 60_000L;
    while (!replicaRaft.allPeersSupport(PeerCapabilities.TX_PREPARED_AT_INDEX) && System.currentTimeMillis() < deadline) {
      replicaRaft.peersMissingCapabilityNow(PeerCapabilities.TX_PREPARED_AT_INDEX);
      Thread.sleep(100);
    }
    assertThat(replicaRaft.allPeersSupport(PeerCapabilities.TX_PREPARED_AT_INDEX)).isTrue();

    final Database replicaDb = getServerDatabase(replicaIndex, getDatabaseName());

    // Prepared under the old schema: the record is saved (and its index changes staged) before the index exists.
    replicaDb.begin();
    replicaDb.newDocument(TYPE_NAME).set("id", 100L).save();

    // The leader creates the index, and the replica applies it before it commits.
    leaderDb.getSchema().getOrCreateTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, TYPE_NAME, "id");
    waitForReplicationIsCompleted(replicaIndex);
    assertThat(replicaDb.getSchema().getIndexByName(TYPE_NAME + "[id]")).as("the replica applied the DDL").isNotNull();

    assertThatThrownBy(replicaDb::commit)
        .as("accepted, this is the record that ends up in no index on any node")
        .isInstanceOf(ConcurrentModificationException.class);
    if (replicaDb.isTransactionActive())
      replicaDb.rollback();

    // The retry is prepared under the new schema.
    replicaDb.transaction(() -> replicaDb.newDocument(TYPE_NAME).set("id", 100L).save(), false, 0);
    waitForReplicationIsCompleted(replicaIndex);

    final Database leaderCheck = getServerDatabase(leaderIndex, getDatabaseName());
    final Database replicaCheck = getServerDatabase(replicaIndex, getDatabaseName());
    assertThat(leaderCheck.countType(TYPE_NAME, true)).isEqualTo(11);
    assertThat(replicaCheck.countType(TYPE_NAME, true)).isEqualTo(11);
    for (final Index leaderIndexDef : leaderCheck.getSchema().getType(TYPE_NAME).getAllIndexes(true)) {
      final Index replicaIndexDef = replicaCheck.getSchema().getIndexByName(leaderIndexDef.getName());
      assertThat(leaderIndexDef.countEntries()).as("index %s on the leader", leaderIndexDef.getName()).isEqualTo(11);
      assertThat(replicaIndexDef.countEntries()).as("index %s on the replica", leaderIndexDef.getName()).isEqualTo(11);
    }
    assertClusterConsistency();
  }
}
