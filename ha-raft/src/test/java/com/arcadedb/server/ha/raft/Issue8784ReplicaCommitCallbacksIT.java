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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.TransactionContext;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8784, end to end on a real cluster: a transaction committed on a REPLICA - whose pages that replica's state
 * machine publishes from the entry's WAL bytes (#5503) - must end there as a commit: after-commit callbacks fired, the
 * commit counted, the saved record clean. Before, the replica's commit tail ended it with a bare {@code reset()}.
 */
class Issue8784ReplicaCommitCallbacksIT extends BaseRaftHATest {
  private static final String TYPE_NAME = "Issue8784Probe";

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void populateDatabase() {
    // The schema is created in the test, through the leader.
  }

  @Test
  void aCommitOnAReplicaFiresItsAfterCommitCallbacks() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.command("sql", "CREATE DOCUMENT TYPE " + TYPE_NAME);
    leaderDb.command("sql", "CREATE PROPERTY " + TYPE_NAME + ".name STRING");
    waitForAllServers();

    final int replicaIndex = leaderIndex == 0 ? 1 : 0;
    final Database replicaDb = getServerDatabase(replicaIndex, getDatabaseName());

    // A write transaction: the replica's commit tail.
    final AtomicInteger fired = new AtomicInteger();
    replicaDb.begin();
    final MutableDocument doc = replicaDb.newDocument(TYPE_NAME).set("name", "from-replica");
    doc.save();
    final TransactionContext tx = ((DatabaseInternal) replicaDb).getTransaction();
    tx.addAfterCommitCallback(fired::incrementAndGet);
    final long commitCountBefore = tx.getCommitCount();
    replicaDb.commit();

    assertThat(fired.get()).as("the after-commit callback of a replica commit fires on the replica").isEqualTo(1);
    assertThat(tx.getCommitCount()).as("the replica commit is counted").isEqualTo(commitCountBefore + 1);
    assertThat(doc.isDirty()).as("the committed record is clean on the replica").isFalse();

    // A transaction that wrote nothing: the read-only arm, on the replica.
    final AtomicInteger firedReadOnly = new AtomicInteger();
    replicaDb.begin();
    ((DatabaseInternal) replicaDb).getTransaction().addAfterCommitCallback(firedReadOnly::incrementAndGet);
    replicaDb.commit();
    assertThat(firedReadOnly.get()).as("the after-commit callback of a read-only replica commit fires").isEqualTo(1);

    waitForAllServers();
    for (int i = 0; i < getServerCount(); i++)
      assertThat(getServerDatabase(i, getDatabaseName()).countType(TYPE_NAME, true)).as("count of node %d", i).isEqualTo(1L);

    assertClusterConsistency();
  }
}
