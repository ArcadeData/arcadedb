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
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.RequestOptions;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #13087: the {@code resultSetLimit} of a security group caps the rows a traversal answers over the Gremlin Server wire
 * protocol, where a request never goes through the {@code gremlin} query engine: a bytecode request and a script both build their
 * traversal from the server's traversal source. Since #12888 removed the truncation of every bucket scan, nothing bounded them.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class Issue13087GremlinServerResultSetLimitIT extends AbstractGremlinServerIT {
  private static final String CAPPED_USER     = "cappedGremlinUser";
  private static final String CAPPED_PASSWORD = "cappedPassword1";
  private static final String CAPPED_GROUP    = "gremlinCapped";
  private static final int    RECORDS         = 40;
  private static final int    QUOTA           = 2;

  @Test
  void aBytecodeTraversalAnswersTheQuotaComputedOverEveryRecord() {
    seed();
    createCappedUser();

    final Cluster cluster = createCluster(CAPPED_USER, CAPPED_PASSWORD);
    try {
      final GraphTraversalSource g = AnonymousTraversalSource.traversal()
          .withRemote(DriverRemoteConnection.using(cluster, getDatabaseName()));

      assertThat(g.V().hasLabel("Capped").toList()).hasSize(QUOTA);
      // THE RECORD SITS PAST THE FIRST TWO OF ITS BUCKET, AND OF THE TYPE
      assertThat(g.V().hasLabel("Capped").has("k", RECORDS).values("k").toList()).containsExactly(RECORDS);
      assertThat(g.V().hasLabel("Capped").count().next()).isEqualTo((long) RECORDS);
      assertThat(g.V().hasLabel("Capped").order().by("k", Order.desc).values("k").toList()).containsExactly(RECORDS, RECORDS - 1);
    } finally {
      cluster.close();
    }
  }

  @Test
  void aScriptAnswersTheQuotaComputedOverEveryRecord() throws Exception {
    seed();
    createCappedUser();

    final Cluster cluster = createCluster(CAPPED_USER, CAPPED_PASSWORD);
    try {
      // No alias: 'g' is the global traversal source of the default database, which is the test database. gremlin-lang, as Groovy
      // is reserved to the server administrator
      final Client client = cluster.connect();
      final RequestOptions gremlinLang = RequestOptions.build().language("gremlin-lang").create();
      assertThat(client.submit("g.V().hasLabel('Capped')", gremlinLang).all().get()).hasSize(QUOTA);
      assertThat(client.submit("g.V().hasLabel('Capped').count()", gremlinLang).all().get().getFirst().getLong()).isEqualTo(RECORDS);
      assertThat(client.submit("g.V().hasLabel('Capped').has('k', " + RECORDS + ").values('k')", gremlinLang).all().get())
          .hasSize(1);
    } finally {
      cluster.close();
    }
  }

  @Test
  void aUserWithoutAQuotaReadsEveryRecord() {
    seed();

    final Cluster cluster = createCluster("root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    try {
      final GraphTraversalSource g = AnonymousTraversalSource.traversal()
          .withRemote(DriverRemoteConnection.using(cluster, getDatabaseName()));
      assertThat(g.V().hasLabel("Capped").toList()).hasSize(RECORDS);
    } finally {
      cluster.close();
    }
  }

  private void seed() {
    final Database db = getServerDatabase(0, getDatabaseName());
    db.command("sql", "CREATE VERTEX TYPE Capped IF NOT EXISTS BUCKETS 4");
    db.transaction(() -> {
      for (int k = 1; k <= RECORDS; k++)
        db.newVertex("Capped").set("k", k).save();
    });
  }

  private void createCappedUser() {
    final ServerSecurity security = getServer(0).getSecurity();
    security.saveGroup(getDatabaseName(), CAPPED_GROUP, new JSONObject()
        .put("access", new JSONArray())
        .put("resultSetLimit", QUOTA)
        .put("types", new JSONObject().put("*", new JSONObject().put("access", new JSONArray().put("readRecord")))));
    security.createUser(new JSONObject()
        .put("name", CAPPED_USER)
        .put("password", security.encodePassword(CAPPED_PASSWORD))
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put(CAPPED_GROUP))));
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
      if (security != null && security.getUser(CAPPED_USER) != null)
        security.dropUser(CAPPED_USER);
    } catch (final Exception e) {
      // IGNORE: server may already be stopped
    }
    super.endTest();
  }
}
