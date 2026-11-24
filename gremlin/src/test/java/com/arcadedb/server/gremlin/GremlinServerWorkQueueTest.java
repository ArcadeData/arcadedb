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
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.util.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * The work queue of the Gremlin execution pool honours {@code maxWorkQueueSize}: with every worker busy and the queue full, a
 * request is answered with the "too many requests" status instead of being kept in memory, and the server keeps serving once
 * the workers are free again.
 * <p>
 * The server runs with one worker and a queue of one. The worker is held by a traversal that walks every path of length five on
 * a complete graph of 60 vertices, which runs for minutes when left alone and is ended through its registry entry.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GremlinServerWorkQueueTest extends AbstractGremlinServerIT {
  private static final int    VERTICES       = 60;
  private static final String HELD_TRAVERSAL = "g.V().hasLabel('NodeQueued').out().out().out().out().out().path().count()";
  private static final String COUNT          = "g.V().hasLabel('NodeQueued').count()";
  private static final int    EXTRA_REQUESTS = 6;

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue("gremlin.gremlinPool", 1);
    config.setValue("gremlin.maxWorkQueueSize", 1);
  }

  @BeforeEach
  void createGraph() {
    final Database database = getServerDatabase(0, getDatabaseName());
    if (database.getSchema().existsType("NodeQueued"))
      return;
    database.getSchema().createVertexType("NodeQueued");
    database.getSchema().createEdgeType("LinkQueued");
    database.transaction(() -> {
      final MutableVertex[] vertices = new MutableVertex[VERTICES];
      for (int i = 0; i < VERTICES; i++)
        vertices[i] = database.newVertex("NodeQueued").set("i", i).save();
      for (int i = 0; i < VERTICES; i++)
        for (int j = 0; j < VERTICES; j++)
          if (i != j)
            vertices[i].newEdge("LinkQueued", vertices[j]);
    });
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @Test
  void aFullWorkQueueRefusesTheRequestsThatDoNotFitAndTheServerKeepsServing() throws Exception {
    final Cluster cluster = createCluster();
    RunningQuery held = null;
    try {
      final Client client = cluster.connect();
      final CompletableFuture<List<Result>> heldResult = client.submit(HELD_TRAVERSAL).all();
      held = awaitRunning();

      // The only worker is busy and the queue holds one request: of the requests that follow, one waits and the others are refused
      final List<CompletableFuture<List<Result>>> extra = new ArrayList<>(EXTRA_REQUESTS);
      for (int i = 0; i < EXTRA_REQUESTS; i++)
        extra.add(client.submit(COUNT).all());

      await().atMost(Duration.ofSeconds(20)).untilAsserted(
          () -> assertThat(extra.stream().filter(CompletableFuture::isCompletedExceptionally).count()).as("refused requests")
              .isEqualTo(EXTRA_REQUESTS - 1));
      for (final CompletableFuture<List<Result>> request : extra)
        if (request.isCompletedExceptionally())
          assertThat(responseStatusOf(request)).isEqualTo(ResponseStatusCode.TOO_MANY_REQUESTS);

      // The worker is freed: the request that waited runs, and so does the one that comes after it
      held.terminate("root");
      assertThat(held.awaitEnd(30_000)).isTrue();
      assertThatThrownBy(() -> heldResult.get(30, TimeUnit.SECONDS)).hasStackTraceContaining("terminated");
      for (final CompletableFuture<List<Result>> request : extra)
        if (!request.isCompletedExceptionally())
          assertThat(request.get(30, TimeUnit.SECONDS).getFirst().getLong()).isEqualTo(VERTICES);
      assertThat(client.submit(COUNT).all().get(30, TimeUnit.SECONDS).getFirst().getLong()).isEqualTo(VERTICES);
    } finally {
      if (held != null) {
        held.terminate("root");
        held.awaitEnd(30_000);
      }
      cluster.close();
    }
  }

  private static ResponseStatusCode responseStatusOf(final CompletableFuture<?> request) {
    try {
      request.get();
    } catch (final ExecutionException e) {
      Throwable cause = e;
      while (cause != null && !(cause instanceof ResponseException))
        cause = cause.getCause();
      assertThat(cause).as("the failure of a refused request: %s", e).isNotNull();
      return ((ResponseException) cause).getResponseStatusCode();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return null;
  }

  private RunningQuery awaitRunning() {
    final RunningQuery[] found = new RunningQuery[1];
    await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(20)).until(() -> {
      for (final RunningQuery q : getServer(0).getRunningQueries().getRunning())
        if ("gremlin".equals(q.getProtocol()) && HELD_TRAVERSAL.equals(q.getText())) {
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
