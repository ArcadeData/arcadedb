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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.gremlin.io.ArcadeIoRegistry;
import com.arcadedb.test.BaseGraphServerTest;
import com.arcadedb.utility.FileUtils;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Regression test for issue #3661: the default {@code graph} database referenced in
 * {@code gremlin-server.properties} must be created automatically when the Gremlin plugin
 * starts and the database does not yet exist (e.g. a fresh Docker container).
 */
class GremlinDefaultGraphCreationIT extends BaseGraphServerTest {

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();

    new File("./target/config").mkdirs();

    try {
      FileUtils.writeFile(new File("./target/config/gremlin-server.yaml"),
          FileUtils.readStreamAsString(getClass().getClassLoader().getResourceAsStream("gremlin-server.yaml"), "utf8"));

      FileUtils.writeFile(new File("./target/config/gremlin-server.properties"),
          FileUtils.readStreamAsString(getClass().getClassLoader().getResourceAsStream("gremlin-server.properties"), "utf8"));

      FileUtils.writeFile(new File("./target/config/gremlin-server.groovy"),
          FileUtils.readStreamAsString(getClass().getClassLoader().getResourceAsStream("gremlin-server.groovy"), "utf8"));

      GlobalConfiguration.SERVER_PLUGINS.setValue("GremlinServer:com.arcadedb.server.gremlin.GremlinServerPlugin");
      GlobalConfiguration.TYPE_DEFAULT_BUCKETS.setValue(Runtime.getRuntime().availableProcessors());

    } catch (final IOException e) {
      fail("", e);
    }
  }

  /**
   * Do NOT pre-create the database — this is the scenario that was broken: a fresh start
   * where only the Gremlin properties file references the database directory.
   */
  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @BeforeEach
  @Override
  public void beginTest() {
    super.beginTest();
    // no explicit database creation — the Gremlin plugin must handle it
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    GlobalConfiguration.TYPE_DEFAULT_BUCKETS.setValue(1);
    super.endTest();
  }

  /**
   * Verifies that starting the Gremlin plugin with a {@code graphs:} section in
   * {@code gremlin-server.yaml} causes the referenced database to be created automatically,
   * even when no {@code arcadedb.server.defaultDatabases} setting is provided.
   * <p>
   * Regression test for: https://github.com/ArcadeData/arcadedb/issues/3661
   */
  @Test
  void defaultGraphDatabaseCreatedOnGremlinPluginStart() {
    // The server is up and the Gremlin plugin has started. The "graph" database must
    // exist because GremlinServerPlugin.initPreConfiguredDatabases() should have created it.
    assertThat(getServer(0).existsDatabase(getDatabaseName()))
        .as("database '%s' should have been auto-created by the Gremlin plugin from gremlin-server.properties", getDatabaseName())
        .isTrue();
  }

  @Test
  void canConnectViaGremlinToAutoCreatedDatabase() {
    final GraphBinaryMessageSerializerV1 serializer = new GraphBinaryMessageSerializerV1(
        new TypeSerializerRegistry.Builder().addRegistry(new ArcadeIoRegistry()));

    final Cluster cluster = Cluster.build()
        .enableSsl(false)
        .addContactPoint("localhost")
        .port(8182)
        .credentials("root", DEFAULT_PASSWORD_FOR_TESTS)
        .serializer(serializer)
        .create();

    try {
      final GraphTraversalSource g = AnonymousTraversalSource.traversal()
          .withRemote(DriverRemoteConnection.using(cluster, getDatabaseName()));

      // The database is freshly created, so it must be empty.
      assertThat(g.V().count().next()).isZero();
    } finally {
      cluster.close();
    }
  }
}
