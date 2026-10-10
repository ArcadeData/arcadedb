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
import com.arcadedb.exception.QueryTerminatedException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.gremlin.io.ArcadeIoRegistry;
import com.arcadedb.query.RunningQuery;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.BaseGraphServerTest;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Issue #9689 for Gremlin: a traversal is a statement like any other - listed by {@code list queries}, and stopped by a
 * terminate wherever it spends its time, including the adjacency steps that never reach a SQL scan's check - whether it
 * arrives over HTTP (which registers it) or through Gremlin Server (which now registers it too). Before, a terminate
 * only reached a traversal at a {@code g.V()} scan, and a Gremlin Server request was not listed at all.
 * <p>
 * The traversal walks every path of length five on a complete graph of 60 vertices: about 750 million paths, which
 * {@code path()} keeps from being bulked together, so it runs for minutes when left alone.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9689GremlinTerminateTest extends AbstractGremlinServerIT {
  private static final int    VERTICES       = 60;
  private static final String LONG_TRAVERSAL = "g.V().hasLabel('Node9689').out().out().out().out().out().path().count()";

  @BeforeEach
  void createGraph() {
    final Database database = getServerDatabase(0, getDatabaseName());
    if (database.getSchema().existsType("Node9689"))
      return;
    database.getSchema().createVertexType("Node9689");
    database.getSchema().createEdgeType("Link9689");
    database.transaction(() -> {
      final MutableVertex[] vertices = new MutableVertex[VERTICES];
      for (int i = 0; i < VERTICES; i++)
        vertices[i] = database.newVertex("Node9689").set("i", i).save();
      for (int i = 0; i < VERTICES; i++)
        for (int j = 0; j < VERTICES; j++)
          if (i != j)
            vertices[i].newEdge("Link9689", vertices[j]);
    });
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @Test
  void aGremlinServerRequestIsListedAndTerminated() throws Exception {
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

      final RunningQuery entry = awaitRunning("gremlin");
      assertThat(entry.getUser()).isEqualTo("root");
      assertThat(entry.getLanguage()).isEqualTo("gremlin");

      entry.terminate("root");
      assertThatThrownBy(() -> running.get(60, TimeUnit.SECONDS)).hasStackTraceContaining("terminated");
      assertThat(entry.awaitEnd(30_000)).isTrue();
      assertThat(entry.getOutcome()).isEqualTo(RunningQuery.Outcome.TERMINATED);

      // The worker serves the next request
      assertThat(client.submit("g.V().hasLabel('Node9689').count()").all().get().getFirst().getLong()).isEqualTo(VERTICES);
    } finally {
      cluster.close();
    }
  }

  @Test
  void aTraversalStopsInItsAdjacencyStepsWhenTerminated() throws Exception {
    // The HTTP path: the request's entry is published on the thread that runs the traversal
    final Database database = getServerDatabase(0, getDatabaseName());
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final AtomicReference<RunningQuery> entry = new AtomicReference<>();
    final Thread runner = new Thread(() -> {
      try (final RunningQuery q = getServer(0).getRunningQueries().register(getDatabaseName(), "root", "http", null, null)) {
        q.setStatement("gremlin", LONG_TRAVERSAL);
        entry.set(q);
        try (final ResultSet rs = database.command("gremlin", LONG_TRAVERSAL)) {
          while (rs.hasNext())
            rs.next();
        }
      } catch (final Throwable e) {
        failure.set(e);
      }
    }, "issue-9689-gremlin");
    runner.start();

    await().atMost(Duration.ofSeconds(30)).until(() -> entry.get() != null);
    // Well into the walk, past the one g.V() scan whose check alone used to see a terminate
    Thread.sleep(500);
    entry.get().terminate("root");
    assertThat(entry.get().awaitEnd(60_000)).as("the traversal must end once terminated").isTrue();
    runner.join(10_000);

    Throwable t = failure.get();
    while (t != null && !(t instanceof QueryTerminatedException))
      t = t.getCause();
    assertThat(t).as("the traversal must fail with the termination, got: %s", failure.get()).isNotNull();
    assertThat(entry.get().getOutcome()).isEqualTo(RunningQuery.Outcome.TERMINATED);
  }

  private RunningQuery awaitRunning(final String protocol) {
    final RunningQuery[] found = new RunningQuery[1];
    await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(20)).until(() -> {
      for (final RunningQuery q : getServer(0).getRunningQueries().getRunning())
        if (protocol.equals(q.getProtocol()) && LONG_TRAVERSAL.equals(q.getText())) {
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
