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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.gremlin.io.ArcadeIoRegistry;
import com.arcadedb.server.BaseGraphServerTest;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.RequestOptions;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * https://github.com/ArcadeData/arcadedb/issues/9147
 * <p>
 * A Gremlin plugin started on a server with no database never bound {@code g} for script requests, even after a database
 * was created: the script engines fix their global bindings when the executor is built.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9147ScriptGBindingNoDatabaseAtStartIT extends BaseGraphServerTest {
  private int gremlinPort;

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    gremlinPort = GremlinTestPorts.assign(config);
  }

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GremlinServer:com.arcadedb.server.gremlin.GremlinServerPlugin");
  }

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @Test
  void scriptSeesGForADatabaseCreatedAfterTheStart() throws Exception {
    assertThat(getServer(0).getDatabaseNames()).as("the scenario needs a server that starts with no database").isEmpty();

    final Database db = getServer(0).getOrCreateDatabase("mydb");
    db.command("sql", "CREATE VERTEX TYPE Person").close();
    db.transaction(() -> db.newVertex("Person").set("name", "a").save());

    final Cluster cluster = Cluster.build().enableSsl(false).addContactPoint("127.0.0.1").port(gremlinPort)
        .credentials("root", DEFAULT_PASSWORD_FOR_TESTS)
        .serializer(new GraphBinaryMessageSerializerV1(new TypeSerializerRegistry.Builder().addRegistry(new ArcadeIoRegistry()))).create();
    try {
      final Client client = cluster.connect();
      final String query = "g.V().hasLabel('Person').count()";

      final List<Result> groovy = client.submit(query).all().get();
      assertThat(groovy).hasSize(1);
      assertThat(groovy.get(0).getLong()).isEqualTo(1L);

      final List<Result> lang = client.submit(query, RequestOptions.build().language("gremlin-lang").create()).all().get();
      assertThat(lang).hasSize(1);
      assertThat(lang.get(0).getLong()).isEqualTo(1L);
    } finally {
      cluster.close();
    }
  }

  @Test
  void aDroppedDatabaseIsNoLongerBoundForScripts() throws Exception {
    final Database db = getServer(0).getOrCreateDatabase("mydb");
    db.command("sql", "CREATE VERTEX TYPE Person").close();

    final Cluster cluster = Cluster.build().enableSsl(false).addContactPoint("127.0.0.1").port(gremlinPort)
        .credentials("root", DEFAULT_PASSWORD_FOR_TESTS)
        .serializer(new GraphBinaryMessageSerializerV1(new TypeSerializerRegistry.Builder().addRegistry(new ArcadeIoRegistry()))).create();
    try {
      final Client client = cluster.connect();
      final RequestOptions lang = RequestOptions.build().language("gremlin-lang").create();
      assertThat(client.submit("g.V().count()", lang).all().get().get(0).getLong()).isZero();

      ((DatabaseInternal) getServer(0).getDatabase("mydb")).getEmbedded().drop();
      getServer(0).removeDatabase("mydb");

      assertThatThrownBy(() -> client.submit("g.V().count()", lang).all().get()).isInstanceOf(ExecutionException.class);
    } finally {
      cluster.close();
    }
  }
}
