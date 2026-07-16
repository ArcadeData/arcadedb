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
import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.gremlin.io.ArcadeIoRegistry;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.server.BaseGraphServerTest;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reproduces issue #5309: a traversal result that carries a raw RID (e.g. the value of a LINK property)
 * failed to be serialized by the Gremlin Server with GraphBinary
 * ("Serializer for type com.arcadedb.database.DatabaseRID not found").
 */
class GremlinServerRIDSerializationTest extends AbstractGremlinServerIT {

  @Test
  void linkPropertyValueIsSerialized() {
    final RID friendRid = createGraphWithLink();

    final GraphTraversalSource g = traversal();
    final Object value = g.V().has("PersonNode", "pid", 1).values("friend").next();

    assertThat(value).isNotNull();
    assertThat(value).isInstanceOf(RID.class);
    assertThat(value.toString()).isEqualTo(friendRid.toString());
  }

  @Test
  void vertexWithLinkPropertyIsSerialized() {
    final RID friendRid = createGraphWithLink();

    final GraphTraversalSource g = traversal();
    final Map<Object, Object> valueMap = g.V().has("PersonNode", "pid", 1).valueMap().next();

    final Object friend = valueMap.get("friend");
    assertThat(friend).isNotNull();
    // valueMap() wraps property values in a list
    final Object first = friend instanceof List<?> list ? list.get(0) : friend;
    assertThat(first).isInstanceOf(RID.class);
    assertThat(first.toString()).isEqualTo(friendRid.toString());
  }

  private RID createGraphWithLink() {
    final Database db = getServerDatabase(0, getDatabaseName());
    final RID[] ridHolder = new RID[1];
    db.transaction(() -> {
      if (!db.getSchema().existsType("PersonNode")) {
        final VertexType type = db.getSchema().createVertexType("PersonNode");
        type.createProperty("pid", Type.INTEGER);
      }
      final MutableVertex v2 = db.newVertex("PersonNode").set("pid", 2);
      v2.save();
      final MutableVertex v1 = db.newVertex("PersonNode").set("pid", 1).set("friend", v2.getIdentity());
      v1.save();
      ridHolder[0] = v2.getIdentity();
    });
    return ridHolder[0];
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

    return Cluster.build().enableSsl(false).addContactPoint("localhost").port(8182)
        .credentials("root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).serializer(serializer).create();
  }

  private GraphTraversalSource traversal() {
    return AnonymousTraversalSource.traversal().withRemote(DriverRemoteConnection.using(createCluster(), getDatabaseName()));
  }
}
