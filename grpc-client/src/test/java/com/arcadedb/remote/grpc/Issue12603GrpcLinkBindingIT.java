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
package com.arcadedb.remote.grpc;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseRID;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
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
 * Issue #12603: the gRPC client decoded a link as a plain {@link RID}, not bound to the remote database, so {@code rid.asDocument()}
 * failed on a thread that has no embedded database active.
 * <p>
 * Every link is followed from a NEW thread: the test thread also hosts the server, and a database the server left active on it
 * would resolve the bare RID and hide the defect.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue12603GrpcLinkBindingIT extends BaseGrpcClientServerTest {
  private RemoteGrpcServer   grpcServer;
  private RemoteGrpcDatabase grpc;
  private ExecutorService    freshThread;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GRPC:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void open() {
    freshThread = Executors.newSingleThreadExecutor();
    grpcServer = new RemoteGrpcServer("localhost", getServerGrpcPort(), "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
    grpc = new RemoteGrpcDatabase(grpcServer, "localhost", getServerGrpcPort(), getServerHttpPort(), getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS);
    grpc.command("sql", "CREATE VERTEX TYPE P12603 IF NOT EXISTS");
    grpc.command("sql", "CREATE EDGE TYPE K12603 IF NOT EXISTS");
  }

  @AfterEach
  @Override
  public void endTest() {
    if (grpc != null)
      grpc.close();
    if (grpcServer != null)
      grpcServer.close();
    if (freshThread != null)
      freshThread.shutdownNow();
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @Test
  void linksOfARecordAreBoundToTheRemoteDatabase() throws Exception {
    final RID other = grpc.command("sql", "INSERT INTO P12603 SET name = 'other'").next().getIdentity().get();
    final RID me = insertLinking(other);

    assertLinksResolve(grpc.lookupByRID(me, true).asDocument());
    try (final ResultSet rs = grpc.query("sql", "SELECT FROM P12603 WHERE name = 'me'")) {
      assertLinksResolve(rs.next().getElement().get());
    }
    try (final ResultSet rs = grpc.queryStream("sql", "SELECT FROM P12603 WHERE name = 'me'")) {
      assertLinksResolve(rs.next().getElement().get());
    }
  }

  @Test
  void projectedLinksAreBoundToTheRemoteDatabase() throws Exception {
    final RID other = grpc.command("sql", "INSERT INTO P12603 SET name = 'other'").next().getIdentity().get();
    final RID me = insertLinking(other);

    final Result row = grpc.query("sql", "SELECT friend, friends, byName FROM " + me).next();
    assertResolves(row.getProperty("friend"));
    assertResolves(row.<List<Object>>getProperty("friends").getFirst());
    assertResolves(row.<Map<String, Object>>getProperty("byName").get("x"));
  }

  @Test
  void edgeEndpointsAreBoundToTheRemoteDatabase() throws Exception {
    final RID a = grpc.command("sql", "INSERT INTO P12603 SET name = 'a'").next().getIdentity().get();
    final RID b = grpc.command("sql", "INSERT INTO P12603 SET name = 'b'").next().getIdentity().get();
    grpc.command("sql", "CREATE EDGE K12603 FROM " + a + " TO " + b);

    final Edge edge = grpc.query("sql", "SELECT FROM K12603").next().getEdge().get();
    assertThat(onFreshThread(() -> edge.getOut().asVertex().getString("name"))).isEqualTo("a");
    assertThat(onFreshThread(() -> edge.getIn().asVertex().getString("name"))).isEqualTo("b");
    assertThat(edge.getOut()).isInstanceOf(DatabaseRID.class);
    assertThat(edge.getIdentity()).isInstanceOf(DatabaseRID.class);
  }

  private RID insertLinking(final RID other) {
    return grpc.command("sql",
        "INSERT INTO P12603 SET name = 'me', friend = " + other + ", friends = [" + other + "], byName = {'x': " + other + "}").next()
        .getIdentity().get();
  }

  private void assertLinksResolve(final Document doc) throws Exception {
    assertResolves(doc.get("friend"));
    assertResolves(doc.getList("friends").getFirst());
    assertResolves(doc.getMap("byName").get("x"));
  }

  private void assertResolves(final Object link) throws Exception {
    final Identifiable identifiable = (Identifiable) link;
    assertThat(onFreshThread(() -> identifiable.asDocument().getString("name"))).isEqualTo("other");
    assertThat(onFreshThread(() -> identifiable.asVertex().getString("name"))).isEqualTo("other");
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
