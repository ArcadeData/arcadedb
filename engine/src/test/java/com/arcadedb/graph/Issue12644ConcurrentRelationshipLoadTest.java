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
package com.arcadedb.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12644: writers that create relationships between shared vertices on one edge bucket per direction, so every chunk is on
 * one page and a transaction that appended to a chunk holds a copy of the page that the commits of the others outdate. Every
 * relationship a transaction committed is reachable from both its ends, and none is lost, whichever of the transactions retried.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue12644ConcurrentRelationshipLoadTest extends TestHelper {
  private static final int  VERTICES       = 300;
  private static final int  WRITERS        = 8;
  private static final int  TX_PER_WRITER  = 30;
  private static final int  EDGES_PER_TX   = 10;
  private static final int  ATTEMPTS       = 50;
  private static final long TIMEOUT_SECOND = 120;

  private final RID[] vertices = new RID[VERTICES];

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("V", 1);
      database.getSchema().createEdgeType("E", 1);
      for (int i = 0; i < VERTICES; i++)
        vertices[i] = database.newVertex("V").save().getIdentity();
    });
  }

  @Test
  void everyCommittedRelationshipIsReachableFromBothEnds() throws Exception {
    final AtomicLong committedEdges = new AtomicLong();
    final AtomicLong gaveUp = new AtomicLong();

    final ExecutorService executor = Executors.newFixedThreadPool(WRITERS);
    try {
      final List<Future<?>> futures = new ArrayList<>();
      for (int w = 0; w < WRITERS; w++) {
        final long seed = 12644L + w;
        futures.add(executor.submit(() -> {
          final Random random = new Random(seed);
          for (int t = 0; t < TX_PER_WRITER; t++) {
            final int[] from = new int[EDGES_PER_TX];
            final int[] to = new int[EDGES_PER_TX];
            for (int e = 0; e < EDGES_PER_TX; e++) {
              from[e] = random.nextInt(VERTICES);
              to[e] = random.nextInt(VERTICES);
            }
            boolean committed = false;
            for (int attempt = 0; attempt < ATTEMPTS && !committed; attempt++) {
              database.begin();
              try {
                for (int e = 0; e < EDGES_PER_TX; e++)
                  database.lookupByRID(vertices[from[e]], true).asVertex().newEdge("E", database.lookupByRID(vertices[to[e]], true).asVertex());
                database.commit();
                committed = true;
              } catch (final ConcurrentModificationException e) {
                if (database.isTransactionActive())
                  database.rollback();
              }
            }
            if (committed)
              committedEdges.addAndGet(EDGES_PER_TX);
            else
              gaveUp.incrementAndGet();
          }
          return null;
        }));
      }
      for (final Future<?> future : futures)
        future.get(TIMEOUT_SECOND, TimeUnit.SECONDS);
    } finally {
      executor.shutdownNow();
    }

    assertThat(gaveUp.get()).as("transactions that ran out of attempts").isZero();
    assertThat(committedEdges.get()).isEqualTo((long) WRITERS * TX_PER_WRITER * EDGES_PER_TX);

    database.transaction(() -> {
      assertThat(edgeCount()).isEqualTo(committedEdges.get());

      long outgoing = 0;
      long incoming = 0;
      for (final RID rid : vertices) {
        final Vertex vertex = database.lookupByRID(rid, true).asVertex();
        outgoing += vertex.countEdges(Vertex.DIRECTION.OUT, "E");
        incoming += vertex.countEdges(Vertex.DIRECTION.IN, "E");
      }
      assertThat(outgoing).as("relationships reached from their source").isEqualTo(committedEdges.get());
      assertThat(incoming).as("relationships reached from their target").isEqualTo(committedEdges.get());
    });
  }

  private long edgeCount() {
    try (final ResultSet result = database.query("sql", "SELECT count(@rid) AS c FROM E")) {
      return result.next().<Number>getProperty("c").longValue();
    }
  }
}
