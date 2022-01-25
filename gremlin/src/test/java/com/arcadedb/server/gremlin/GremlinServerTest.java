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
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.utility.FileUtils;
import org.apache.tinkerpop.gremlin.arcadedb.structure.io.ArcadeIoRegistry;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.driver.ser.GraphBinaryMessageSerializerV1;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

public class GremlinServerTest extends BaseGraphServerTest {

  @Test
  public void getAllVertices() {
    final GraphTraversalSource g = traversal();
    var vertices = g.V().limit(3).toList();
    Assertions.assertEquals(3, vertices.size());
  }

  @Override
  protected boolean isPopulateDatabase() {
    return true;
  }

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();

    // COPY GREMLIN SERVER FILES BEFORE STARTING THE GREMLIN SERVER
    new File("./target/config").mkdirs();

    try {
      FileUtils.writeFile(new File("./target/config/gremlin-server.yaml"),
          FileUtils.readStreamAsString(getClass().getClassLoader().getResourceAsStream("gremlin-server.yaml"), "utf8"));

      FileUtils.writeFile(new File("./target/config/gremlin-server.properties"),
          FileUtils.readStreamAsString(getClass().getClassLoader().getResourceAsStream("gremlin-server.properties"), "utf8"));

      FileUtils.writeFile(new File("./target/config/gremlin-server.groovy"),
          FileUtils.readStreamAsString(getClass().getClassLoader().getResourceAsStream("gremlin-server.groovy"), "utf8"));

      GlobalConfiguration.SERVER_PLUGINS.setValue("GremlinServer:com.arcadedb.server.gremlin.GremlinServerPlugin");

    } catch (IOException e) {
      Assertions.fail(e);
    }
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  private Cluster createCluster() {
    final GraphBinaryMessageSerializerV1 serializer = new GraphBinaryMessageSerializerV1(
        new TypeSerializerRegistry.Builder().addRegistry(new ArcadeIoRegistry()));

    return Cluster.build().enableSsl(false).addContactPoint("localhost").port(8182).credentials("root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).serializer(serializer)
        .create();
  }

  private GraphTraversalSource traversal() {
    return AnonymousTraversalSource.traversal().withRemote(DriverRemoteConnection.using(createCluster(), "graph"));
  }
}
