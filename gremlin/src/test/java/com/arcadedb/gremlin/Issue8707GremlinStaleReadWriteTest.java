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
package com.arcadedb.gremlin;

import com.arcadedb.database.BasicDatabase;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8707: the #8610 stale-read refusal had no exemption on the Gremlin element API, so a literal
 * {@code property()} write over a record another client changed since the traversal read it failed with a retryable
 * conflict the Gremlin driver never retries. The key and the value come from the traversal, so the write is accepted,
 * like over the WebSocket insert route and gRPC {@code updateRecord}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8707GremlinStaleReadWriteTest {
  private static final String DB_PATH = "./target/databases/test-issue-8707";

  private ArcadeGraph graph;

  @BeforeEach
  void setUp() {
    try (final DatabaseFactory databaseFactory = new DatabaseFactory(DB_PATH)) {
      if (databaseFactory.exists())
        databaseFactory.open().drop();
    }
    graph = ArcadeGraph.open(DB_PATH);
    graph.getDatabase().getSchema().getOrCreateVertexType("Node8707");
    graph.getDatabase().getSchema().getOrCreateEdgeType("Link8707");
  }

  @AfterEach
  void tearDown() {
    if (graph != null)
      graph.drop();
  }

  @Test
  void vertexPropertyWriteOverAConcurrentCommitIsAccepted() {
    final RID rid = createNode();

    graph.tx().readWrite();
    final Vertex read = graph.vertices(rid.toString()).next();
    assertThat((Integer) read.value("n")).isEqualTo(1);
    commitConcurrently(rid, 5);

    read.property("m", "literal");
    graph.tx().commit();

    final com.arcadedb.graph.Vertex stored = graph.getDatabase().lookupByRID(rid, true).asVertex();
    assertThat(stored.getString("m")).isEqualTo("literal");
    // The write is the property's own, so the concurrent change to ANOTHER property must survive it
    assertThat(stored.getInteger("n")).as("the concurrent commit is not reverted").isEqualTo(5);
  }

  @Test
  void vertexPropertyRemoveOverAConcurrentCommitIsAccepted() {
    final RID rid = createNode();

    graph.tx().readWrite();
    final Vertex read = graph.vertices(rid.toString()).next();
    read.value("n");
    commitConcurrently(rid, 5);

    read.property("n").remove();
    graph.tx().commit();

    assertThat(graph.getDatabase().lookupByRID(rid, true).asVertex().has("n")).isFalse();
  }

  private RID createNode() {
    graph.tx().readWrite();
    final Vertex v = graph.addVertex(T.label, "Node8707", "n", 1);
    final RID rid = new RID(v.id().toString());
    graph.tx().commit();
    return rid;
  }

  private void commitConcurrently(final RID rid, final int value) {
    final BasicDatabase database = graph.getDatabase();
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread concurrent = new Thread(() -> {
      try {
        database.transaction(() -> rid.asVertex().modify().set("n", value).save());
      } catch (final Throwable t) {
        failure.set(t);
      }
    });
    concurrent.start();
    join(concurrent);
    assertThat(failure.get()).isNull();
  }

  private static void join(final Thread thread) {
    try {
      thread.join();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
