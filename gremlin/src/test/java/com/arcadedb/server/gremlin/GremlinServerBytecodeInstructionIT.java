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
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.IO;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * A bytecode request is a list of instructions, each naming a method of the traversal. The wire authorizer accepts only the
 * instructions a client driver records, so a request that names any other method of the traversal is refused before it is
 * translated, and a user is held to the permissions of its type grants for every request it sends.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class GremlinServerBytecodeInstructionIT extends AbstractGremlinServerIT {
  private static final String TYPED_USER     = "typedGremlinUser";
  private static final String TYPED_PASSWORD = "typedPassword1";
  private static final String TYPED_GROUP    = "gremlinTypedAccess";

  private final File exportTarget = new File("./target/gremlin-instruction-probe.json");

  @Test
  void driverTraversalsKeepWorkingForATypedUser() {
    seedTypes();
    createTypedUser();

    final Cluster cluster = createCluster(TYPED_USER, TYPED_PASSWORD);
    try {
      final GraphTraversalSource g = AnonymousTraversalSource.traversal().withRemote(DriverRemoteConnection.using(cluster, getDatabaseName()));

      assertThat(g.V().hasLabel("Granted").count().next()).isEqualTo(1L);
      // A terminal call of the driver is recorded as a step of its own
      g.V().hasLabel("Granted").iterate();
      assertThat(g.V().hasLabel("Granted").values("name").toList()).containsExactly("granted");

      // The type grants still hold for a request built by the driver
      assertThat(catchThrowable(() -> g.addV("Restricted").property("name", "added").iterate())).isNotNull();
    } finally {
      cluster.close();
    }

    assertThat(countOf("Restricted")).isEqualTo(1L);
  }

  @Test
  void sourceOptionsAndCardinalityOfDriversKeepWorking() {
    seedTypes();

    final Cluster cluster = createCluster("root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    try {
      final GraphTraversalSource g = AnonymousTraversalSource.traversal().withRemote(DriverRemoteConnection.using(cluster, getDatabaseName()));

      assertThat(g.withSideEffect("x", 5).withSack(1).withBulk(true).withPath().V().hasLabel("Granted").count().next()).isEqualTo(1L);
      g.addV("Restricted").property(VertexProperty.Cardinality.single, "name", "single").iterate();
      assertThat(g.V().hasLabel("Restricted").has("name", "single").count().next()).isEqualTo(1L);
    } finally {
      cluster.close();
    }
  }

  @Test
  void nonStepInstructionIsRefusedForATypedUser() {
    seedTypes();
    createTypedUser();

    final Cluster cluster = createCluster(TYPED_USER, TYPED_PASSWORD);
    try {
      final Client client = cluster.connect().alias(getDatabaseName());

      final Bytecode write = withInstruction(EmptyGraph.instance().traversal().addV("Restricted").property("name", "added"));
      final Throwable thrown = catchThrowable(() -> client.submit(write).all().get());
      assertThat(thrown).as("A request that names a method of the traversal that is not a step must be refused").isNotNull();
      assertThat(thrown).hasStackTraceContaining("is not supported");

      final Bytecode create = withInstruction(EmptyGraph.instance().traversal().addV("Created").property("name", "added"));
      assertThat(catchThrowable(() -> client.submit(create).all().get())).isNotNull();
    } finally {
      cluster.close();
    }

    assertThat(countOf("Restricted")).isEqualTo(1L);
    assertThat(getServerDatabase(0, getDatabaseName()).getSchema().existsType("Created")).isFalse();
  }

  @Test
  void nonStepInstructionNestedInATraversalArgumentIsRefused() {
    seedTypes();
    createTypedUser();

    final Cluster cluster = createCluster(TYPED_USER, TYPED_PASSWORD);
    try {
      final Client client = cluster.connect().alias(getDatabaseName());

      final Bytecode nested = new Bytecode();
      nested.addStep("addV", "Restricted");
      nested.addStep("iterate");
      final Bytecode bytecode = new Bytecode();
      bytecode.addStep("V");
      bytecode.addStep("union", nested);

      assertThat(catchThrowable(() -> client.submit(bytecode).all().get())).isNotNull();
    } finally {
      cluster.close();
    }

    assertThat(countOf("Restricted")).isEqualTo(1L);
  }

  @Test
  void nonStepInstructionLeavesNoExportFile() {
    seedTypes();
    createTypedUser();

    final Cluster cluster = createCluster(TYPED_USER, TYPED_PASSWORD);
    try {
      final Client client = cluster.connect().alias(getDatabaseName());

      final Bytecode export = withInstruction(
          EmptyGraph.instance().traversal().io(exportTarget.getAbsolutePath()).with(IO.writer, IO.graphson).write());
      assertThat(catchThrowable(() -> client.submit(export).all().get())).isNotNull();
    } finally {
      cluster.close();
    }

    assertThat(exportTarget).doesNotExist();
  }

  @Test
  void nonStepInstructionIsRefusedForTheAdministratorToo() {
    seedTypes();

    final Cluster cluster = createCluster("root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    try {
      final Client client = cluster.connect().alias(getDatabaseName());

      final Bytecode write = withInstruction(EmptyGraph.instance().traversal().addV("Restricted").property("name", "added"));
      assertThat(catchThrowable(() -> client.submit(write).all().get())).isNotNull();

      // The same traversal as a driver builds it is served
      final GraphTraversalSource g = AnonymousTraversalSource.traversal().withRemote(DriverRemoteConnection.using(cluster, getDatabaseName()));
      g.addV("Restricted").property("name", "added by the driver").iterate();
    } finally {
      cluster.close();
    }

    assertThat(countOf("Restricted")).isEqualTo(2L);
  }

  /** The request a driver would send for {@code traversal}, with one more instruction that is not a step. */
  private static Bytecode withInstruction(final Traversal<?, ?> traversal) {
    final Bytecode bytecode = traversal.asAdmin().getBytecode().clone();
    bytecode.addStep("iterate");
    return bytecode;
  }

  private long countOf(final String type) {
    return getServerDatabase(0, getDatabaseName()).query("sql", "SELECT count(*) AS c FROM " + type).next().<Long>getProperty("c");
  }

  private void seedTypes() {
    final Database db = getServerDatabase(0, getDatabaseName());
    db.command("sqlscript", """
        CREATE VERTEX TYPE Granted IF NOT EXISTS;
        CREATE VERTEX TYPE Restricted IF NOT EXISTS;
        INSERT INTO Granted SET name = 'granted';
        INSERT INTO Restricted SET name = 'restricted';""");
  }

  private void createTypedUser() {
    final ServerSecurity security = getServer(0).getSecurity();
    // Reads one type and nothing else
    security.saveGroup(getDatabaseName(), TYPED_GROUP, new JSONObject()
        .put("access", new JSONArray())
        .put("types", new JSONObject().put("Granted", new JSONObject().put("access", new JSONArray().put("readRecord")))));
    if (security.getUser(TYPED_USER) == null)
      security.createUser(new JSONObject()
          .put("name", TYPED_USER)
          .put("password", security.encodePassword(TYPED_PASSWORD))
          .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put(TYPED_GROUP))));
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
        if (security.getUser(TYPED_USER) != null)
          security.dropUser(TYPED_USER);
        security.deleteGroup(getDatabaseName(), TYPED_GROUP);
      }
    } catch (final Exception e) {
      // IGNORE: server may already be stopped
    }
    exportTarget.delete();
    super.endTest();
  }
}
