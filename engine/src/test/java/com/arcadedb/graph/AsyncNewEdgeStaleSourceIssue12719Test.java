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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #12719: {@code async().newEdge()} and {@code newEdgeByKeys()} handed the worker the vertex they had read
 * on the caller's thread, and the worker used that instance as it was, although earlier tasks of its open batch had already rewritten
 * the vertex (an edge ending at it, an earlier edge from it). The head of the edge list that instance names was not the head of the
 * vertex in the worker's transaction any more, so the stale-head guard refused the append with a {@code ConcurrentModificationException}
 * and the whole batch was rolled back: edges whose callback had already reported success were gone, and on a bidirectional type the
 * IN entry of a cross-slot edge was appended for an edge the rollback had discarded. Random edges among a handful of vertices keep
 * every vertex both the source and the end of edges inside one batch, which the synchronous API creates without any error.
 * <p>
 * The vertices are read before the first edge is submitted and a small {@code commitEvery} ends a batch every few tasks, so the
 * instances the tasks carry are stale by the time the worker runs them, whatever the speed of the worker: the same state a caller
 * reaches by reading each vertex at submission while the worker is behind.
 */
class AsyncNewEdgeStaleSourceIssue12719Test extends TestHelper {
  private static final int VERTICES     = 6;
  private static final int EDGES        = 600;
  private static final int COMMIT_EVERY = 5;

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE V BUCKETS 4");
    database.command("sql", "CREATE PROPERTY V.id LONG");
    database.command("sql", "CREATE INDEX ON V (id) UNIQUE");
    database.command("sql", "CREATE EDGE TYPE E");
  }

  @Test
  @Timeout(120)
  void oneWorkerCreatesEveryEdgeWithoutAnError() {
    assertEveryEdgeIsCreated(1, false);
  }

  @Test
  @Timeout(120)
  void twoWorkersCreateEveryEdgeAndBothSidesOfIt() {
    assertEveryEdgeIsCreated(2, false);
  }

  @Test
  @Timeout(120)
  void fourWorkersCreateEveryEdgeAndBothSidesOfIt() {
    assertEveryEdgeIsCreated(4, false);
  }

  @Test
  @Timeout(120)
  void lightweightEdgesAreAllWiredToo() {
    assertEveryEdgeIsCreated(2, true);
  }

  private void assertEveryEdgeIsCreated(final int workers, final boolean light) {
    final AtomicInteger errors = new AtomicInteger();
    final AtomicInteger created = new AtomicInteger();
    database.async().setParallelLevel(workers);
    database.async().setCommitEvery(COMMIT_EVERY);
    database.async().onError(e -> errors.incrementAndGet());
    for (long id = 0; id < VERTICES; id++)
      database.async().createRecord(database.newVertex("V").set("id", id), record -> {
      });
    database.async().waitCompletion();

    final Vertex[] vertices = new Vertex[VERTICES];
    for (int i = 0; i < VERTICES; i++)
      vertices[i] = database.lookupByKey("V", "id", (long) i).next().asVertex(true);

    final Random random = new Random(1);
    for (int i = 0; i < EDGES; i++)
      database.async().newEdge(vertices[random.nextInt(VERTICES)], "E", vertices[random.nextInt(VERTICES)].getIdentity(), light,
          (edge, createdSource, createdDestination) -> created.incrementAndGet());
    database.async().waitCompletion();

    final String what = workers + " workers, light=" + light;
    assertThat(errors.get()).as("errors, " + what).isZero();
    assertThat(created.get()).as("callbacks, " + what).isEqualTo(EDGES);

    long out = 0, in = 0;
    try (final ResultSet resultSet = database.query("sql", "SELECT FROM V")) {
      while (resultSet.hasNext()) {
        final Vertex vertex = resultSet.next().getVertex().get();
        out += vertex.countEdges(Vertex.DIRECTION.OUT, "E");
        in += vertex.countEdges(Vertex.DIRECTION.IN, "E");
      }
    }
    assertThat(out).as("OUT entries, " + what).isEqualTo(EDGES);
    assertThat(in).as("IN entries, " + what).isEqualTo(EDGES);
    if (!light)
      assertThat(database.countType("E", true)).as("edge records, " + what).isEqualTo(EDGES);

    try (final ResultSet resultSet = database.command("sql", "CHECK DATABASE")) {
      final Result check = resultSet.next();
      assertThat(check.<Number>getProperty("edgesMissingInReference").longValue()).as("edgesMissingInReference, " + what).isZero();
      assertThat(check.<Number>getProperty("edgesMissingOutReference").longValue()).as("edgesMissingOutReference, " + what).isZero();
      assertThat(check.<Number>getProperty("invalidLinks").longValue()).as("invalidLinks, " + what).isZero();
    }
  }
}
