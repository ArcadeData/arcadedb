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
package com.arcadedb.server.gremlin;

import com.arcadedb.database.Database;
import com.arcadedb.gremlin.io.ArcadeIoRegistry;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.security.ServerSecurity;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.server.op.OpLoader;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * A Gremlin session (the remote {@code g.tx()} of the TinkerPop drivers) runs its requests on a thread of its own, outside
 * the pool that binds the authenticated user into the engine. Sessions are therefore reserved to the server administrator,
 * like the other capabilities whose reach is wider than one database; every other user keeps sessionless traversals, which
 * run under the type access of their group.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class GremlinSessionRequestsIT extends AbstractGremlinServerIT {
  private static final String REFUSAL         = SessionRestrictingOpProcessor.REFUSAL;
  private static final String DENIED_USER     = "gremlinSessionDenied";
  private static final String DENIED_PASSWORD = "gremlinSessionDenied1";
  private static final String READER_USER     = "gremlinSessionReader";
  private static final String READER_PASSWORD = "gremlinSessionReader1";
  private static final String DENIED_GROUP    = "gremlinSessionNoTypeAccess";
  private static final String READER_GROUP    = "gremlinSessionReadOnly";

  @TempDir
  Path tempDir;

  @Test
  void sessionTraversalHonoursTheTypeAccessOfTheUser() {
    createUsers();

    final Cluster cluster = createCluster(DENIED_USER, DENIED_PASSWORD);
    try {
      final GraphTraversalSource g = AnonymousTraversalSource.traversal()
          .withRemote(DriverRemoteConnection.using(cluster, getDatabaseName()));

      // the type access of the group applies to a sessionless traversal ...
      assertThat(catchThrowable(() -> g.V().hasLabel("Probe").values("name").toList()))
          .as("A sessionless traversal must honour the type access of the group").isNotNull();

      // ... and to the one that runs in a session
      final Transaction tx = g.tx();
      final GraphTraversalSource gtx = tx.begin();
      assertThat(catchThrowable(() -> gtx.V().hasLabel("Probe").values("name").toList()))
          .as("A session traversal must honour the type access of the group").hasStackTraceContaining(REFUSAL);
      assertThat(catchThrowable(() -> gtx.addV("Probe").property("name", "added").iterate()))
          .as("A session traversal must not write a type the group has no access to").hasStackTraceContaining(REFUSAL);
    } finally {
      cluster.close();
    }

    assertProbeCount(1L);
  }

  @Test
  void sessionTraversalOfAReadOnlyUserCannotWrite() {
    createUsers();

    final Cluster cluster = createCluster(READER_USER, READER_PASSWORD);
    try {
      final GraphTraversalSource g = AnonymousTraversalSource.traversal()
          .withRemote(DriverRemoteConnection.using(cluster, getDatabaseName()));

      assertThat(g.V().hasLabel("Probe").count().next()).as("a sessionless read keeps working").isEqualTo(1L);

      final Transaction tx = g.tx();
      final GraphTraversalSource gtx = tx.begin();
      assertThat(catchThrowable(() -> gtx.addV("Probe").property("name", "added").iterate()))
          .as("A read-only user must not write from a session").hasStackTraceContaining(REFUSAL);
      assertThat(catchThrowable(tx::commit)).as("nothing to commit").isNotNull();
    } finally {
      cluster.close();
    }

    assertProbeCount(1L);
  }

  @Test
  void sessionIoStepIsRefusedForOtherUsers() {
    createUsers();
    final Path target = tempDir.resolve("session-export.xml");

    final Cluster cluster = createCluster(READER_USER, READER_PASSWORD);
    try {
      final GraphTraversalSource g = AnonymousTraversalSource.traversal()
          .withRemote(DriverRemoteConnection.using(cluster, getDatabaseName()));

      final Transaction tx = g.tx();
      final GraphTraversalSource gtx = tx.begin();
      assertThat(catchThrowable(() -> gtx.io(target.toString()).write().iterate()))
          .as("The io() step is not served in a session to a user other than the administrator").hasStackTraceContaining(REFUSAL);
    } finally {
      cluster.close();
    }

    assertThat(target).doesNotExist();
  }

  @Test
  void sessionTransactionOfTheServerAdministratorStillWorks() {
    createUsers();

    final Cluster cluster = createCluster("root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    try {
      final GraphTraversalSource g = AnonymousTraversalSource.traversal()
          .withRemote(DriverRemoteConnection.using(cluster, getDatabaseName()));

      final Transaction tx = g.tx();
      final GraphTraversalSource gtx = tx.begin();
      gtx.addV("Probe").property("name", "added").next();
      tx.commit();

      assertThat(g.V().hasLabel("Probe").count().next()).isEqualTo(2L);
    } finally {
      cluster.close();
    }

    assertProbeCount(2L);
  }

  @Test
  void sessionProcessorIsTheRestrictedOne() {
    assertThat(OpLoader.getProcessor("session")).containsInstanceOf(SessionRestrictingOpProcessor.class);
  }

  private void assertProbeCount(final long expected) {
    final Database db = getServerDatabase(0, getDatabaseName());
    assertThat(db.query("sql", "SELECT count(*) as c FROM Probe").next().<Long>getProperty("c")).isEqualTo(expected);
  }

  private void createUsers() {
    final Database db = getServerDatabase(0, getDatabaseName());
    db.command("sqlscript", "CREATE VERTEX TYPE Probe IF NOT EXISTS;\nINSERT INTO Probe SET name = 'probe';");

    final ServerSecurity security = getServer(0).getSecurity();
    security.saveGroup(getDatabaseName(), DENIED_GROUP, new JSONObject()
        .put("access", new JSONArray())
        .put("types", new JSONObject().put("*", new JSONObject().put("access", new JSONArray()))));
    security.saveGroup(getDatabaseName(), READER_GROUP, new JSONObject()
        .put("access", new JSONArray())
        .put("types", new JSONObject().put("*", new JSONObject().put("access", new JSONArray().put("readRecord")))));

    if (security.getUser(DENIED_USER) == null)
      security.createUser(new JSONObject()
          .put("name", DENIED_USER)
          .put("password", security.encodePassword(DENIED_PASSWORD))
          .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put(DENIED_GROUP))));
    if (security.getUser(READER_USER) == null)
      security.createUser(new JSONObject()
          .put("name", READER_USER)
          .put("password", security.encodePassword(READER_PASSWORD))
          .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put(READER_GROUP))));
  }

  private Cluster createCluster(final String user, final String password) {
    final GraphBinaryMessageSerializerV1 serializer = new GraphBinaryMessageSerializerV1(
        new TypeSerializerRegistry.Builder().addRegistry(new ArcadeIoRegistry()));

    return Cluster.build().enableSsl(false).addContactPoint("localhost").port(getGremlinPort())
        .credentials(user, password).serializer(serializer).create();
  }

  @AfterEach
  @Override
  public void endTest() {
    try {
      final ServerSecurity security = getServer(0).getSecurity();
      if (security != null) {
        if (security.getUser(DENIED_USER) != null)
          security.dropUser(DENIED_USER);
        if (security.getUser(READER_USER) != null)
          security.dropUser(READER_USER);
        security.deleteGroup(getDatabaseName(), DENIED_GROUP);
        security.deleteGroup(getDatabaseName(), READER_GROUP);
      }
    } catch (final Exception e) {
      // IGNORE: server may already be stopped
    }
    super.endTest();
  }
}
