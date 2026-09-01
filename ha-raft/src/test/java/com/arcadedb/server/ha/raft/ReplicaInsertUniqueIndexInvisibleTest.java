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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.HAReplicatedDatabase;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6964: a document INSERTED on a Raft replica through the embedded transaction API commits, and the record
 * replicates cluster-wide - a full scan finds it on every node - but its UNIQUE index entry used to be silently
 * lost everywhere: {@code lookupByKey} missed the document on the replica that wrote it AND on the leader, with no
 * error raised anywhere.
 * <p>
 * Root cause: {@code TransactionContext.commit1stPhase(boolean isLeader)} only replayed the transaction's queued
 * index operations into WAL-bound pages when {@code isLeader} was {@code true}. {@link RaftReplicatedDatabase#commit()}
 * passes the CURRENT RAFT LEADERSHIP flag to that method, so a replica originating its own commit (issue #5503's
 * "replica commits its own transaction" path) skipped the replay entirely - the WAL bytes it shipped through Raft
 * never contained the index pages, so no node, including the replica itself, ever got them.
 * <p>
 * Expected behaviour: a committed, replicated insert is visible through its unique index on every node - or the
 * commit fails. The control insert on the LEADER shows the baseline works: the leader-originated document is
 * index-visible on both nodes within seconds. Only the replica-originated insert lost its index entry.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ReplicaInsertUniqueIndexInvisibleTest {

  private static final String ROOT_PATH_PREFIX = "target/replica-insert-repro-";
  private static final String DATABASE_NAME    = "insertrepro";
  private static final String TYPE_NAME        = "Singleton";
  private static final int    BASE_RAFT_PORT   = 7134;
  private static final int    BASE_HTTP_PORT   = 7180;
  private static final int    SERVER_COUNT     = 2;

  private final List<ArcadeDBServer> servers = new ArrayList<>();

  @BeforeEach
  void setUp() {
    for (int i = 0; i < SERVER_COUNT; i++)
      FileUtils.deleteRecursively(new File(ROOT_PATH_PREFIX + i));
  }

  @AfterEach
  void tearDown() {
    for (final ArcadeDBServer server : servers)
      try {
        server.stop();
      } catch (final Exception e) {
        // Best-effort teardown.
      }
    servers.clear();
    for (int i = 0; i < SERVER_COUNT; i++)
      FileUtils.deleteRecursively(new File(ROOT_PATH_PREFIX + i));
  }

  @Test
  void replicaOriginatedInsertMustBeVisibleThroughItsUniqueIndexClusterWide() throws Exception {
    startCluster();
    final ArcadeDBServer leader = awaitLeader(60);
    final ArcadeDBServer replica = otherThan(leader);

    // 1. Database + schema on the leader, pushed to the replica.
    final ServerDatabase leaderDb = leader.getOrCreateDatabase(DATABASE_NAME);
    ((HAReplicatedDatabase) leaderDb.getWrappedDatabaseInstance()).createInReplicas();
    final Schema schema = leaderDb.getSchema();
    final DocumentType type = schema.getOrCreateDocumentType(TYPE_NAME);
    type.getOrCreateProperty("name", Type.STRING);
    schema.getOrCreateTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, TYPE_NAME, "name");

    // 2. Control document from the LEADER: proves the type, the index and the
    // replication path all work for a leader-originated insert.
    insertSingleton(leaderDb, "control");
    awaitTrue("the replica acquires the database", 60, () -> replica.existsDatabase(DATABASE_NAME));
    awaitTrue("the leader's control document is index-visible on the replica", 60,
        () -> indexVisible(replica.getDatabase(DATABASE_NAME), "control"));

    // 3. The same insert pattern, executed on the REPLICA.
    final Database replicaDb = replica.getDatabase(DATABASE_NAME);
    insertSingleton(replicaDb, "from-replica");

    // 4. The record itself replicates: a full scan finds it on both nodes.
    awaitTrue("the replica's document is scan-visible on the replica", 60,
        () -> scanVisible(replicaDb, "from-replica"));
    awaitTrue("the replica's document is scan-visible on the leader", 60,
        () -> scanVisible(leaderDb, "from-replica"));

    // 5. The document must also become visible through the unique index it is registered in, on either node.
    awaitTrue("the replica's document is index-visible on the replica", 30,
        () -> indexVisible(replicaDb, "from-replica"));
    awaitTrue("the replica's document is index-visible on the leader", 30,
        () -> indexVisible(leaderDb, "from-replica"));

    assertThat(indexVisible(replicaDb, "from-replica"))
        .as("a committed, replicated insert must be visible through its unique index on the writing node")
        .isTrue();
    assertThat(indexVisible(leaderDb, "from-replica"))
        .as("a committed, replicated insert must be visible through its unique index on the leader")
        .isTrue();
  }

  /**
   * The classic find-or-create-singleton pattern of an embedded application: one
   * transaction, the pessimistic type lock, an index lookup, and the insert when
   * the lookup misses.
   */
  private void insertSingleton(final Database database, final String name) {
    database.transaction(() -> {
      database.acquireLock().type(TYPE_NAME).lock();
      if (!database.lookupByKey(TYPE_NAME, "name", name).hasNext())
        database.newDocument(TYPE_NAME).set("name", name).save();
    }, false, 1);
  }

  private boolean indexVisible(final Database database, final String name) {
    try {
      return database.lookupByKey(TYPE_NAME, "name", name).hasNext();
    } catch (final Exception e) {
      return false;
    }
  }

  private boolean scanVisible(final Database database, final String name) {
    // Full scan on purpose: no WHERE on the indexed property, so the query
    // cannot be answered from the (diverged) index.
    try (final ResultSet resultSet = database.query("sql", "select from " + TYPE_NAME)) {
      while (resultSet.hasNext())
        if (name.equals(resultSet.next().getProperty("name")))
          return true;
    } catch (final Exception e) {
      // Fall through: not visible yet.
    }
    return false;
  }

  private void startCluster() {
    final StringBuilder serverList = new StringBuilder();
    for (int i = 0; i < SERVER_COUNT; i++) {
      if (i > 0)
        serverList.append(",");
      serverList.append("localhost:").append(BASE_RAFT_PORT + i).append(":").append(BASE_HTTP_PORT + i);
    }
    for (int i = 0; i < SERVER_COUNT; i++) {
      final ContextConfiguration configuration = new ContextConfiguration();
      final String rootPath = new File(ROOT_PATH_PREFIX + i).getAbsolutePath();
      configuration.setValue(GlobalConfiguration.SERVER_NAME, "insertrepro-" + i);
      configuration.setValue(GlobalConfiguration.SERVER_ROOT_PATH, rootPath);
      configuration.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, rootPath + "/databases");
      configuration.setValue(GlobalConfiguration.SERVER_LOGS_DIRECTORY, rootPath + "/log");
      configuration.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "insertrepro-root");
      configuration.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT, String.valueOf(BASE_HTTP_PORT + i));
      configuration.setValue(GlobalConfiguration.HA_ENABLED, true);
      configuration.setValue(GlobalConfiguration.HA_SERVER_LIST, serverList.toString());
      configuration.setValue(GlobalConfiguration.HA_RAFT_PORT, BASE_RAFT_PORT + i);
      configuration.setValue(GlobalConfiguration.HA_HEALTH_CHECK_INTERVAL, 0L);
      final ArcadeDBServer server = new ArcadeDBServer(configuration);
      servers.add(server);
      server.start();
    }
  }

  private ArcadeDBServer awaitLeader(final int seconds) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + seconds * 1000L;
    while (System.currentTimeMillis() < deadline) {
      for (final ArcadeDBServer server : servers)
        if (server.getHA() != null && server.getHA().isLeader())
          return server;
      Thread.sleep(250);
    }
    throw new AssertionError("No leader elected within " + seconds + " seconds");
  }

  private ArcadeDBServer otherThan(final ArcadeDBServer server) {
    for (final ArcadeDBServer candidate : servers)
      if (candidate != server)
        return candidate;
    throw new AssertionError("No second server");
  }

  private interface Check {
    boolean holds() throws Exception;
  }

  private void awaitTrue(final String what, final int seconds, final Check check) throws Exception {
    final long deadline = System.currentTimeMillis() + seconds * 1000L;
    while (System.currentTimeMillis() < deadline) {
      if (check.holds())
        return;
      Thread.sleep(250);
    }
    throw new AssertionError("Timed out after " + seconds + " seconds waiting until: " + what);
  }

}
