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
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.index.Index;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Regression test for issue #12122: the #8686 refusal of {@code StaleSchemaReplicaTransactionIT}, with the third node
 * of the cluster DOWN.
 * <p>
 * A replica states the index its transaction was prepared at only while every peer advertises {@code tx-prepared-at-index},
 * and the capability registry used to drop a peer's advertisement at its first failed probe. So stopping one follower switched
 * the refusal off on every other node: the replica transaction prepared before it applied {@code CREATE INDEX} was accepted, and
 * its record never entered the index on any live node (records N+1, index N). A peer that only stopped answering now keeps,
 * for the framed transaction sections, what it last advertised.
 */
class StaleSchemaReplicaTransactionWithPeerDownIT extends BaseRaftHATest {
  private static final String TYPE_NAME = "Issue12122Doc";

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected int[] getServerToCheck() {
    return startedServers();
  }

  // Regression for #12122: the #8686 refusal of StaleSchemaReplicaTransactionIT, with the third node of the cluster DOWN.
  @Test
  @Timeout(300)
  void aReplicaTransactionPreparedBeforeTheIndexIsRefusedWhileAPeerIsDown() {
    final int leaderIndex = requireLeaderIndex();
    final int replicaIndex = firstFollowerOf(leaderIndex);
    int downIndex = -1;
    for (int i = 0; i < getServerCount(); i++)
      if (i != leaderIndex && i != replicaIndex)
        downIndex = i;

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> leaderDb.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create()
        .getOrCreateProperty("id", Type.LONG));
    leaderDb.transaction(() -> {
      for (int i = 0; i < 10; i++)
        leaderDb.newDocument(TYPE_NAME).set("id", (long) i).save();
    });
    waitForAllServers();

    // Every peer has advertised the section before the outage, as on any healthy cluster
    final RaftHAServer replicaRaft = getRaftPlugin(replicaIndex).getRaftHAServer();
    await().atMost(60, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS)
        .until(() -> replicaRaft.peersMissingCapabilityNow(PeerCapabilities.TX_PREPARED_AT_INDEX).isEmpty());

    // One follower goes down, and the replica's capability monitor sees its probe fail
    getServer(downIndex).stop();
    final String downPeer = peerIdForIndex(downIndex);
    await().atMost(60, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS)
        .until(() -> replicaRaft.getPeerCapabilityRegistry().unknownKindOf(downPeer) == PeerCapabilityRegistry.UnknownKind.UNREACHABLE);
    replicaRaft.invalidateTxPreparedAtCapability();
    assertThat(replicaRaft.canStateTxPreparedAt()).as("a peer that is only down still counts for tx-prepared-at-index").isTrue();

    final Database replicaDb = getServerDatabase(replicaIndex, getDatabaseName());

    // Prepared under the old schema: the record is saved (and its index changes staged) before the index exists
    replicaDb.begin();
    replicaDb.newDocument(TYPE_NAME).set("id", 100L).save();

    // The leader creates the index, and the replica applies it before it commits
    leaderDb.getSchema().getOrCreateTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, TYPE_NAME, "id");
    waitForReplicationIsCompleted(replicaIndex);
    assertThat(replicaDb.getSchema().getIndexByName(TYPE_NAME + "[id]")).as("the replica applied the DDL").isNotNull();

    assertThatThrownBy(replicaDb::commit)
        .as("accepted, this is the record that ends up in no index on any live node")
        .isInstanceOf(ConcurrentModificationException.class);
    if (replicaDb.isTransactionActive())
      replicaDb.rollback();

    // The retry is prepared under the new schema
    replicaDb.transaction(() -> replicaDb.newDocument(TYPE_NAME).set("id", 100L).save(), false, 0);
    waitForAllServers();

    for (final int serverIndex : new int[] { leaderIndex, replicaIndex }) {
      final Database db = getServerDatabase(serverIndex, getDatabaseName());
      assertThat(db.countType(TYPE_NAME, true)).as("records on server %d", serverIndex).isEqualTo(11);
      for (final Index index : db.getSchema().getType(TYPE_NAME).getAllIndexes(true))
        assertThat(index.countEntries()).as("entries of %s on server %d", index.getName(), serverIndex).isEqualTo(11);
    }
    assertClusterConsistency();
  }
}
