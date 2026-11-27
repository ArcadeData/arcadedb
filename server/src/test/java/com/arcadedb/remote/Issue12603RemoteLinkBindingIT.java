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

import com.arcadedb.database.DatabaseRID;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12603: a link read back by the HTTP client was a plain {@link RID}, not bound to the remote database, so
 * {@code rid.asDocument()} failed on a thread that has no embedded database active, while the identity of the record, already a
 * {@link DatabaseRID}, worked.
 * <p>
 * Every link is followed from a NEW thread: the test thread also hosts the server, and a database the server left active on it
 * would resolve the bare RID and hide the defect.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue12603RemoteLinkBindingIT extends BaseGraphServerTest {
  private RemoteDatabase  db;
  private ExecutorService freshThread;

  @Override
  protected String getDatabaseName() {
    return "issue12603";
  }

  @BeforeEach
  void connect() {
    freshThread = Executors.newSingleThreadExecutor();
    db = new RemoteDatabase("127.0.0.1", getServerHttpPort(), getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS);
    db.command("sql", "CREATE VERTEX TYPE P12603");
    db.command("sql", "CREATE EDGE TYPE K12603");
    db.command("sql", "CREATE PROPERTY P12603.dlink LINK");
    db.command("sql", "CREATE PROPERTY P12603.dlinks LIST OF LINK");
    db.command("sql", "CREATE PROPERTY P12603.dmapl MAP OF LINK");
  }

  @AfterEach
  void disconnect() throws InterruptedException {
    if (db != null)
      db.close();
    if (freshThread != null) {
      freshThread.shutdownNow();
      freshThread.awaitTermination(10, TimeUnit.SECONDS);
    }
  }

  @Test
  void linksOfARecordAreBoundToTheRemoteDatabase() throws Exception {
    final RID other = db.command("sql", "INSERT INTO P12603 SET name = 'other'").next().getIdentity().get();
    final RID me = insertLinking(other);

    assertLinksResolve(onFreshThread(() -> db.lookupByRID(me, true).asDocument()));
    try (final ResultSet rs = db.query("sql", "SELECT FROM P12603 WHERE name = 'me'")) {
      final Document doc = rs.next().getElement().get();
      assertLinksResolve(doc);
    }
  }

  @Test
  void projectedLinksAreBoundToTheRemoteDatabase() throws Exception {
    final RID other = db.command("sql", "INSERT INTO P12603 SET name = 'other'").next().getIdentity().get();
    final RID me = insertLinking(other);

    final Result row = db.query("sql", "SELECT friend, friends, byName, dlink, @rid FROM " + me).next();
    assertResolves(row.getProperty("friend"));
    assertResolves(row.<List<Object>>getProperty("friends").getFirst());
    assertResolves(row.<Map<String, Object>>getProperty("byName").get("x"));
    assertResolves(row.getProperty("dlink"));
    assertThat(onFreshThread(() -> row.getIdentity().get().asDocument().getString("name"))).isEqualTo("me");
  }

  @Test
  void edgeEndpointsAreBoundToTheRemoteDatabase() throws Exception {
    final RID a = db.command("sql", "INSERT INTO P12603 SET name = 'a'").next().getIdentity().get();
    final RID b = db.command("sql", "INSERT INTO P12603 SET name = 'b'").next().getIdentity().get();
    db.command("sql", "CREATE EDGE K12603 FROM " + a + " TO " + b);

    final Edge edge = db.query("sql", "SELECT FROM K12603").next().getEdge().get();
    assertThat(onFreshThread(() -> edge.getOut().asVertex().getString("name"))).isEqualTo("a");
    assertThat(onFreshThread(() -> edge.getIn().asVertex().getString("name"))).isEqualTo("b");
    assertThat(edge.getOut()).isInstanceOf(DatabaseRID.class);
  }

  private RID insertLinking(final RID other) {
    return db.command("sql", "INSERT INTO P12603 SET name = 'me', friend = " + other + ", friends = [" + other + "], byName = {'x': "
            + other + "}, dlink = " + other + ", dlinks = [" + other + "], dmapl = {'x': " + other + "}").next().getIdentity()
        .get();
  }

  private void assertLinksResolve(final Document doc) throws Exception {
    // undeclared: a link, a list of links and a map of links, read through the @props hint
    assertResolves(doc.get("friend"));
    assertResolves(doc.getList("friends").getFirst());
    assertResolves(doc.getMap("byName").get("x"));
    // declared in the schema: LINK, LIST OF LINK, MAP OF LINK
    assertResolves(doc.get("dlink"));
    assertResolves(doc.getList("dlinks").getFirst());
    assertResolves(doc.getMap("dmapl").get("x"));
  }

  private void assertResolves(final Object link) throws Exception {
    final Identifiable identifiable = (Identifiable) link;
    assertThat(onFreshThread(() -> identifiable.asDocument().getString("name"))).isEqualTo("other");
    assertThat(onFreshThread(() -> identifiable.asVertex().getString("name"))).isEqualTo("other");
    assertThat(onFreshThread(() -> identifiable.getRecord().asDocument().getString("name"))).isEqualTo("other");
    assertThat(link).isInstanceOf(DatabaseRID.class);
  }

  private <T> T onFreshThread(final Callable<T> callable) throws Exception {
    try {
      return freshThread.submit(callable).get(30, TimeUnit.SECONDS);
    } catch (final ExecutionException e) {
      if (e.getCause() instanceof Exception cause)
        throw cause;
      throw e;
    }
  }
}
