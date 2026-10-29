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
import com.arcadedb.database.RID;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10856: a float array is written in its fixed-width form, which a build that predates it cannot read, and the record
 * bytes reach every node in the pages a transaction ships. So while a peer has not advertised
 * {@link PeerCapabilities#FIXED_WIDTH_FLOAT_ARRAYS} (a node on an older build during a rolling upgrade) the leader writes the
 * varint form every build reads, and once every peer advertises it the fixed-width form is written, with no operator step.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10856MixedVersionFloatArrayIT extends BaseRaftHATest {
  private static final int DIM = 256;

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Override
  protected void populateDatabase() {
  }

  private RaftHAServer raftServerOf(final int serverIndex) {
    return getRaftPlugin(serverIndex).getRaftHAServer();
  }

  @Test
  void theVarintFormIsWrittenUntilEveryPeerReadsTheFixedOne() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int replicaIndex = leaderIndex == 0 ? 1 : 0;
    final Database leader = getServerDatabase(leaderIndex, getDatabaseName());

    leader.command("sql", "CREATE DOCUMENT TYPE Doc");
    leader.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    // The replica now behaves like a node whose build predates the fixed-width form
    raftServerOf(replicaIndex).setAdvertisedCapabilities(Set.of());
    final RaftHAServer leaderRaft = raftServerOf(leaderIndex);
    Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(250, TimeUnit.MILLISECONDS)
        .until(() -> !leaderRaft.peersMissingCapability(PeerCapabilities.FIXED_WIDTH_FLOAT_ARRAYS).isEmpty()
            && !leaderRaft.canWriteFixedWidthFloatArrays());

    final float[] before = vector(1);
    final RID varint = insert(leader, before);
    assertThat(storedSize(replicaIndex, varint)).as("the varint form: 5 bytes per float").isGreaterThanOrEqualTo(5 * DIM);
    assertThat(readBack(replicaIndex, varint)).containsExactly(before);

    // The rolling upgrade finishes: no restart, no setting
    raftServerOf(replicaIndex).setAdvertisedCapabilities(PeerCapabilities.LOCAL);
    Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(250, TimeUnit.MILLISECONDS)
        .until(() -> leaderRaft.peersMissingCapability(PeerCapabilities.FIXED_WIDTH_FLOAT_ARRAYS).isEmpty()
            && leaderRaft.canWriteFixedWidthFloatArrays());

    final float[] after = vector(2);
    final RID fixed = insert(leader, after);
    assertThat(storedSize(replicaIndex, fixed)).as("the fixed-width form: 4 bytes per float").isLessThan(4 * DIM + 64);
    assertThat(readBack(replicaIndex, fixed)).containsExactly(after);
    assertThat(readBack(replicaIndex, varint)).as("the record written before is still read").containsExactly(before);
  }

  private RID insert(final Database leader, final float[] vector) {
    final AtomicReference<RID> rid = new AtomicReference<>();
    leader.transaction(() -> rid.set(leader.newDocument("Doc").set("embedding", vector).save().getIdentity()));
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);
    return rid.get();
  }

  private int storedSize(final int serverIndex, final RID rid) {
    final Database database = getServerDatabase(serverIndex, getDatabaseName());
    return database.getSchema().getBucketById(rid.getBucketId()).getRecord(rid).size();
  }

  private float[] readBack(final int serverIndex, final RID rid) {
    return (float[]) getServerDatabase(serverIndex, getDatabaseName()).lookupByRID(rid, true).asDocument().get("embedding");
  }

  private static float[] vector(final int seed) {
    final float[] vector = new float[DIM];
    for (int i = 0; i < DIM; i++)
      vector[i] = (float) Math.sin(seed * 31 + i) / 16;
    return vector;
  }
}
