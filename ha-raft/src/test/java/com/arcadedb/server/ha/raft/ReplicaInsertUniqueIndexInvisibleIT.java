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
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #6964: a document INSERTED on a Raft replica through the embedded transaction API commits, and the record
 * replicates cluster-wide - a full scan finds it on every node - but its UNIQUE index entry used to be silently
 * lost everywhere: {@code lookupByKey} missed the document on the replica that wrote it AND on the leader, with no
 * error raised anywhere.
 * <p>
 * Root cause: {@code TransactionContext.commit1stPhase(boolean isLeader)} only replayed the transaction's queued
 * index operations into WAL-bound pages when {@code isLeader} was {@code true}. {@link RaftReplicatedDatabase#commit()}
 * passes the current Raft-leadership flag to that method, so a replica originating its own commit (issue #5503's
 * "replica commits its own transaction" path) skipped the replay entirely - the WAL bytes it shipped through Raft
 * never contained the index pages, so no node, including the replica itself, ever got them.
 * <p>
 * Expected behaviour: a committed, replicated insert is visible through its unique index on every node - or the
 * commit fails. The control insert on the LEADER shows the baseline works: the leader-originated document is
 * index-visible on both nodes within seconds. Only the replica-originated insert lost its index entry.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class ReplicaInsertUniqueIndexInvisibleIT extends BaseRaftHATest {

  private static final String TYPE_NAME = "Singleton";

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Test
  void replicaOriginatedInsertMustBeVisibleThroughItsUniqueIndexClusterWide() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a leader must be elected before the test starts").isGreaterThanOrEqualTo(0);
    final int replicaIndex = (leaderIndex + 1) % getServerCount();

    final Database leaderDb = getServer(leaderIndex).getDatabase(getDatabaseName());
    leaderDb.command("sql", "CREATE DOCUMENT TYPE " + TYPE_NAME);
    leaderDb.command("sql", "CREATE PROPERTY " + TYPE_NAME + ".name STRING");
    leaderDb.command("sql", "CREATE INDEX ON " + TYPE_NAME + " (name) UNIQUE");
    waitForAllServers();

    // Control document from the LEADER: proves the type, the index and the replication path all work for a
    // leader-originated insert.
    insertSingleton(leaderDb, "control");
    waitForAllServers();
    assertThat(awaitValue(1, () -> indexVisible(getServerDatabase(replicaIndex, getDatabaseName()), "control")))
        .as("the leader's control document must be index-visible on the replica")
        .isEqualTo(1);

    // The same insert pattern, executed on the REPLICA.
    final Database replicaDb = getServer(replicaIndex).getDatabase(getDatabaseName());
    insertSingleton(replicaDb, "from-replica");

    // The record itself replicates: a full scan (count(@rid) forces one, unlike the cached count(*)) finds it on
    // both nodes. Not BaseRaftHATest's awaitCountOn/countOn: those count(id), a property this type does not have.
    assertThat(awaitValue(2, () -> scanCount(getServerDatabase(replicaIndex, getDatabaseName()))))
        .as("the replica's document must be scan-visible on the replica")
        .isEqualTo(2);
    assertThat(awaitValue(2, () -> scanCount(getServerDatabase(leaderIndex, getDatabaseName()))))
        .as("the replica's document must be scan-visible on the leader")
        .isEqualTo(2);

    // The document must also become visible through the unique index it is registered in, on either node.
    assertThat(awaitValue(1, () -> indexVisible(getServerDatabase(replicaIndex, getDatabaseName()), "from-replica")))
        .as("a committed, replicated insert must be visible through its unique index on the writing node")
        .isEqualTo(1);
    assertThat(awaitValue(1, () -> indexVisible(getServerDatabase(leaderIndex, getDatabaseName()), "from-replica")))
        .as("a committed, replicated insert must be visible through its unique index on the leader")
        .isEqualTo(1);

    // The index does not just exist - it must ENFORCE uniqueness. An unguarded second insert of the same key
    // (bypassing insertSingleton's own lookup-then-insert guard) on the replica must be rejected, and must not
    // leave a duplicate or phantom record behind on either node.
    assertThatThrownBy(() -> replicaDb.transaction(() -> replicaDb.newDocument(TYPE_NAME).set("name", "from-replica").save()))
        .isInstanceOf(DuplicatedKeyException.class);
    assertThat(scanCount(getServerDatabase(replicaIndex, getDatabaseName())))
        .as("a rejected duplicate insert must not leave a phantom record on the replica")
        .isEqualTo(2);
    assertThat(scanCount(getServerDatabase(leaderIndex, getDatabaseName())))
        .as("a rejected duplicate insert must not leave a phantom record on the leader")
        .isEqualTo(2);
  }

  /**
   * The classic find-or-create-singleton pattern of an embedded application: one transaction, the pessimistic
   * type lock, an index lookup, and the insert when the lookup misses.
   */
  private void insertSingleton(final Database database, final String name) {
    database.transaction(() -> {
      database.acquireLock().type(TYPE_NAME).lock();
      if (!database.lookupByKey(TYPE_NAME, "name", name).hasNext())
        database.newDocument(TYPE_NAME).set("name", name).save();
    }, false, 1);
  }

  private long indexVisible(final Database database, final String name) {
    try {
      return database.lookupByKey(TYPE_NAME, "name", name).hasNext() ? 1 : 0;
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.FINE, "Index lookup of '%s' failed, retrying", e, name);
      return 0;
    }
  }

  /**
   * count(@rid) rather than count(*): the latter reads a cached per-bucket counter, the wrong tool when the
   * question is whether the pages themselves arrived. Not BaseRaftHATest's countOn(), which counts a property
   * named "id" that this type does not have.
   */
  private long scanCount(final Database database) {
    try (final ResultSet resultSet = database.query("sql", "SELECT count(@rid) AS c FROM " + TYPE_NAME)) {
      return resultSet.next().<Number>getProperty("c").longValue();
    }
  }
}
