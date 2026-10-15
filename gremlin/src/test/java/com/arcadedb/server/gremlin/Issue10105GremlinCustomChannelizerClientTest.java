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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.gremlin.io.ArcadeIoRegistry;
import com.arcadedb.query.RunningQuery;
import com.arcadedb.server.BaseGraphServerTest;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.server.channel.WsAndHttpChannelizer;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Issue #10105, end to end: a Gremlin Server configured with a channelizer other than
 * {@link ConcealingWebSocketChannelizer} (legal outside production mode) listed every statement with no client, because
 * the connection was published only by that channelizer's own handler. The client now travels with the connection's
 * authenticated user, so the entry names it whatever channelizer carries the request.
 * <p>
 * The same test pins the other half of the issue as it stands: a Gremlin session request never reaches the point where
 * a client would be missing, because the authorizer refuses it (the session processor evaluates on an executor of its own
 * that the principal binding does not cover, GHSA-c287-v325-j5jx).
 * <p>
 * The traversal walks every path of length five on a complete graph of 60 vertices, which runs for minutes when left alone.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10105GremlinCustomChannelizerClientTest extends AbstractGremlinServerIT {
  private static final int    VERTICES       = 60;
  private static final String LONG_TRAVERSAL = "g.V().hasLabel('Node10105').out().out().out().out().out().path().count()";

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.SERVER_MODE, "development");
    config.setValue("gremlin.channelizer", WsAndHttpChannelizer.class.getName());
  }

  @BeforeEach
  void createGraph() {
    final Database database = getServerDatabase(0, getDatabaseName());
    if (database.getSchema().existsType("Node10105"))
      return;
    database.getSchema().createVertexType("Node10105");
    database.getSchema().createEdgeType("Link10105");
    database.transaction(() -> {
      final MutableVertex[] vertices = new MutableVertex[VERTICES];
      for (int i = 0; i < VERTICES; i++)
        vertices[i] = database.newVertex("Node10105").set("i", i).save();
      for (int i = 0; i < VERTICES; i++)
        for (int j = 0; j < VERTICES; j++)
          if (i != j)
            vertices[i].newEdge("Link10105", vertices[j]);
    });
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @Test
  void aRequestIsListedWithItsClientWhateverTheChannelizer() throws Exception {
    final Cluster cluster = createCluster();
    try {
      final Client client = cluster.connect();
      final CompletableFuture<List<Result>> running = CompletableFuture.supplyAsync(() -> {
        try {
          return client.submit(LONG_TRAVERSAL).all().get();
        } catch (final Exception e) {
          throw new RuntimeException(e);
        }
      });

      final RunningQuery entry = awaitRunning();
      assertThat(getServer(0).getConfiguration().getValueAsString(GlobalConfiguration.SERVER_MODE)).isEqualTo("development");
      assertThat(entry.getConnectionId()).as("the connection of the request, with a channelizer that publishes nothing").isNotBlank();
      assertThat(entry.getClientAddress()).as("its peer").isNotBlank().isNotEqualTo("null").contains("127.0.0.1");

      entry.terminate("root");
      assertThatThrownBy(() -> running.get(60, TimeUnit.SECONDS)).hasStackTraceContaining("terminated");
      assertThat(entry.awaitEnd(30_000)).isTrue();
    } finally {
      cluster.close();
    }
  }

  @Test
  void aSessionRequestIsRefusedBeforeAnyClientCouldBeMissing() throws Exception {
    final Cluster cluster = createCluster();
    try {
      final Client session = cluster.connect("session10105");
      assertThatThrownBy(() -> session.submit("g.V().count()").all().get(30, TimeUnit.SECONDS))
          .hasStackTraceContaining("sessions are not supported");
      assertThat(getServer(0).getRunningQueries().getRunning()).as("nothing was registered for it")
          .noneMatch(q -> "gremlin".equals(q.getProtocol()));
    } finally {
      cluster.close();
    }
  }

  private RunningQuery awaitRunning() {
    final RunningQuery[] found = new RunningQuery[1];
    await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(20)).until(() -> {
      for (final RunningQuery q : getServer(0).getRunningQueries().getRunning())
        if ("gremlin".equals(q.getProtocol()) && LONG_TRAVERSAL.equals(q.getText())) {
          found[0] = q;
          return true;
        }
      return false;
    });
    return found[0];
  }

  private Cluster createCluster() {
    final GraphBinaryMessageSerializerV1 serializer = new GraphBinaryMessageSerializerV1(
        new TypeSerializerRegistry.Builder().addRegistry(new ArcadeIoRegistry()));
    return Cluster.build().enableSsl(false).addContactPoint("localhost").port(getGremlinPort())
        .credentials("root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).serializer(serializer).create();
  }
}
