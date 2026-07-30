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
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #5279 (2nd round), over the wire: the reporter's testPageConcurrentModificationException2(). Several remote
 * clients open a transaction each, update a DIFFERENT already-committed vertex of a single-bucket type - all of them
 * living on the same page - and only then commit, one after the other. A client-managed remote transaction spans
 * several HTTP calls, so the server-side auto-retry never fires and the raw page-level
 * ConcurrentModificationException used to reach the client.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5279RemoteConcurrentUpdateIT extends BaseGraphServerTest {
  private static final String DATABASE_NAME    = "remote-database-5279-update";
  private static final int    CONCURRENT_USERS = 10;

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @Test
  void concurrentRemoteTransactionsUpdatingRecordsOfTheSamePageAllCommit() throws Exception {
    testEachServer(serverIndex -> {
      final int port = 2480 + serverIndex;

      final RemoteDatabase schemaOwner = remoteDatabase(port);
      schemaOwner.command("sql", "create vertex type SimpleVertexEx if not exists buckets 1");

      final List<RID> rids = new ArrayList<>();
      for (int i = 0; i < CONCURRENT_USERS; i++)
        try (final ResultSet result = schemaOwner.command("sql",
            "insert into SimpleVertexEx set svex = 'concurrent test" + i + "'")) {
          rids.add(result.next().getIdentity().get());
        }

      final List<RemoteDatabase> clients = new ArrayList<>();
      for (int i = 0; i < CONCURRENT_USERS; i++) {
        final RemoteDatabase client = remoteDatabase(port);
        client.begin();
        clients.add(client);

        // A LONGER value than the committed one: the record grows in place, on a page every other client is
        // updating too.
        final MutableVertex record = ((Vertex) client.lookupByRID(rids.get(i), true)).modify();
        record.set("svex", "concurrent modification " + i);
        record.save();
      }

      // NO RETRY: updating different records never conflicts, whoever commits first.
      for (final RemoteDatabase client : clients)
        client.commit();

      for (int i = 0; i < CONCURRENT_USERS; i++)
        try (final ResultSet result = schemaOwner.query("sql", "select svex from " + rids.get(i))) {
          assertThat(result.next().<String>getProperty("svex")).isEqualTo("concurrent modification " + i);
        }

      try (final ResultSet result = schemaOwner.command("sql", "check database")) {
        while (result.hasNext()) {
          final Result row = result.next();
          assertThat(numberProperty(row, "totalErrors")).as("check database: " + row.toJSON()).isZero();
          assertThat(numberProperty(row, "autoFix")).as("check database: " + row.toJSON()).isZero();
        }
      }

      for (final RemoteDatabase client : clients)
        client.close();
      schemaOwner.close();
    });
  }

  /** Null-tolerant read of a numeric check-database property, so a missing field fails clearly instead of NPE. */
  private static long numberProperty(final Result row, final String name) {
    final Object value = row.getProperty(name);
    return value == null ? 0L : ((Number) value).longValue();
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
