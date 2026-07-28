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
package com.arcadedb.remote;

import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #5279, over the wire: the reporter's own test. Several remote clients open a transaction each, create ONE
 * vertex of a single-bucket type and only then commit, one after the other. Every client used to be handed the SAME
 * RID - the first free slot of the bucket's last page - so all the commits but the first failed with a page-level
 * ConcurrentModificationException that no application-level retry could resolve.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5279RemoteConcurrentInsertIT extends BaseGraphServerTest {
  private static final String DATABASE_NAME    = "remote-database-5279";
  private static final int    CONCURRENT_USERS = 10;

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @Test
  void concurrentRemoteTransactionsInsertingInTheSameBucketAllCommit() throws Exception {
    testEachServer(serverIndex -> {
      final int port = 2480 + serverIndex;

      final RemoteDatabase schemaOwner = remoteDatabase(port);
      schemaOwner.command("sql", "create vertex type SimpleVertexEx if not exists buckets 1");
      // Materialise the bucket's first page, so every client below inserts into a REUSED page - the case the issue
      // is about (on a still empty bucket each transaction gets a brand-new page of its own and never collides).
      schemaOwner.command("sql", "insert into SimpleVertexEx set svex = 'seed'");

      final List<RemoteDatabase> clients = new ArrayList<>();
      final List<RID> rids = new ArrayList<>();

      for (int i = 0; i < CONCURRENT_USERS; i++) {
        final RemoteDatabase client = remoteDatabase(port);
        client.begin();
        clients.add(client);

        final MutableVertex vertex = client.newVertex("SimpleVertexEx");
        vertex.set("svex", "concurrent test" + i);
        vertex.save();
        rids.add(vertex.getIdentity());
      }

      assertThat(new HashSet<>(rids)).as("every open transaction must get its own RID").hasSize(CONCURRENT_USERS);

      // NO RETRY: inserting different records never conflicts, whoever commits first.
      for (final RemoteDatabase client : clients)
        client.commit();

      try (final ResultSet result = schemaOwner.query("sql", "select count(*) as total from SimpleVertexEx")) {
        assertThat(result.next().<Number>getProperty("total").longValue()).isEqualTo(CONCURRENT_USERS + 1L);
      }

      for (final RID rid : rids)
        try (final ResultSet result = schemaOwner.query("sql", "select svex from " + rid)) {
          assertThat(result.next().<String>getProperty("svex")).startsWith("concurrent test");
        }

      for (final RemoteDatabase client : clients)
        client.close();
      schemaOwner.close();
    });
  }

  private RemoteDatabase remoteDatabase(final int port) {
    return new RemoteDatabase("127.0.0.1", port, DATABASE_NAME, "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
  }

  @BeforeEach
  public void beginTest() {
    super.beginTest();
    final RemoteServer server = new RemoteServer("127.0.0.1", 2480, "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    if (!server.exists(DATABASE_NAME))
      server.create(DATABASE_NAME);
  }

  @AfterEach
  public void endTest() {
    final RemoteServer server = new RemoteServer("127.0.0.1", 2480, "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    if (server.exists(DATABASE_NAME))
      server.drop(DATABASE_NAME);
    super.endTest();
  }
}
