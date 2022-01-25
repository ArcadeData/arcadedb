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

import com.arcadedb.NullLogger;
import com.arcadedb.TestHelper;
import com.arcadedb.database.async.ErrorCallback;
import com.arcadedb.database.bucketselectionstrategy.PartitionedBucketSelectionStrategy;
import com.arcadedb.engine.WALFile;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.logging.Level;

public class InsertGraphIndexTest extends TestHelper {
  private static final int    VERTICES         = 1_000;
  private static final int    EDGES_PER_VERTEX = 1_000;
  private static final String VERTEX_TYPE_NAME = "Person";
  private static final String EDGE_TYPE_NAME   = "Friend";
  private static final int    PARALLEL         = 3;

  @Test
  public void testGraph() {
    // PHASE 1
    {
      createSchema();
      createVertices();
      loadVertices();
      createEdges();
    }

    // PHASE 2
    {
      Vertex[] cachedVertices = loadVertices();
      checkGraph(cachedVertices);
    }

    database.close();
  }

  @Override
  protected String getPerformanceProfile() {
    LogManager.instance().setLogger(NullLogger.INSTANCE);

    return "high-performance";
  }

  private void createEdges() {
    //System.out.println("Creating " + EDGES_PER_VERTEX + " edges per vertex on all " + VERTICES + " vertices");

    database.begin();
    final long begin = System.currentTimeMillis();
    try {
      int sourceIndex = 0;
      for (; sourceIndex < VERTICES; ++sourceIndex) {
        int edges = 0;

        final Vertex sourceVertex = (Vertex) database.lookupByKey(VERTEX_TYPE_NAME, new String[] { "id" }, new Object[] { sourceIndex }).next().getRecord();

        for (int destinationIndex = 0; destinationIndex < VERTICES; destinationIndex++) {
          final Vertex destinationVertex = (Vertex) database.lookupByKey(VERTEX_TYPE_NAME, new String[] { "id" }, new Object[] { destinationIndex }).next()
              .getRecord();

          sourceVertex.newEdge(EDGE_TYPE_NAME, destinationVertex, true);
          if (++edges > EDGES_PER_VERTEX)
            break;
        }

        if (sourceIndex % 100 == 0) {
          database.commit();
          database.begin();
        }
      }
      //System.out.println("Created " + EDGES_PER_VERTEX + " edges per vertex in " + sourceIndex + " vertices in " + (System.currentTimeMillis() - begin) + "ms");

    } finally {
      database.commit();
      final long elapsed = System.currentTimeMillis() - begin;
      //System.out.println("Creation of edges finished in " + elapsed + "ms");
    }
  }

  private Vertex[] loadVertices() {
    //System.out.println("Start inserting " + EDGES_PER_VERTEX + " edges per vertex...");

    final Vertex[] cachedVertices = new Vertex[VERTICES];

    //System.out.println("Loading " + VERTICES + " in RAM...");
    database.transaction(() -> {
      final long begin = System.currentTimeMillis();
      try {
        int counter = 0;
        for (; counter < VERTICES; ++counter) {
          final IndexCursor cursor = database.lookupByKey(VERTEX_TYPE_NAME, new String[] { "id" }, new Object[] { counter });
          if (!cursor.hasNext()) {
            //System.out.println("Vertex with id " + counter + " was not found");
            continue;
          }

          cachedVertices[counter] = (Vertex) cursor.next().getRecord();
        }
        //System.out.println("Loaded " + counter + " vertices in " + (System.currentTimeMillis() - begin) + "ms");
      } finally {
        final long elapsed = System.currentTimeMillis() - begin;
        //System.out.println("Loaded all vertices in RAM in " + elapsed + "ms -> " + (VERTICES / (elapsed / 1000F)) + " ops/sec");
      }
    });

    Assertions.assertEquals(VERTICES, cachedVertices.length);

    return cachedVertices;
  }

  private void createVertices() {
    //System.out.println("Start inserting " + VERTICES + " vertices...");

    long startOfTest = System.currentTimeMillis();

    try {
      //database.setEdgeListSize(256);
      database.async().setParallelLevel(PARALLEL);
      database.async().setTransactionUseWAL(false);
      database.async().setTransactionSync(WALFile.FLUSH_TYPE.NO);
      database.async().setCommitEvery(5000);
      database.async().onError(new ErrorCallback() {
        @Override
        public void call(Throwable exception) {
          LogManager.instance().log(this, Level.SEVERE, "ERROR: " + exception, exception);
          System.exit(1);
        }
      });

      long vertexIndex = 0;
      for (; vertexIndex < VERTICES; ++vertexIndex) {
        final MutableVertex vertex = database.newVertex(VERTEX_TYPE_NAME);

        vertex.set("id", vertexIndex);

        database.async().createRecord(vertex, null);
      }

      //System.out.println("Inserted " + vertexIndex + " vertices in " + (System.currentTimeMillis() - startOfTest) + "ms");

      database.async().waitCompletion();

      Assertions.assertEquals(VERTICES, database.countType(VERTEX_TYPE_NAME, true));

    } finally {
      final long elapsed = System.currentTimeMillis() - startOfTest;
      //System.out.println("Insertion finished in " + elapsed + "ms -> " + (VERTICES / (elapsed / 1000F)) + " ops/sec");
    }
  }

  private void createSchema() {
    final VertexType vertex = database.getSchema().createVertexType(VERTEX_TYPE_NAME, PARALLEL);
    vertex.createProperty("id", Integer.class);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, VERTEX_TYPE_NAME, "id");
    vertex.setBucketSelectionStrategy(new PartitionedBucketSelectionStrategy(new String[] { "id" }));

    database.getSchema().createEdgeType(EDGE_TYPE_NAME, PARALLEL);
  }

  private void checkGraph(Vertex[] cachedVertices) {
    //System.out.println("Checking graph with " + VERTICES + " vertices");

    final int expectedEdges = Math.min(VERTICES, EDGES_PER_VERTEX);

    database.begin();
    final long begin = System.currentTimeMillis();
    try {
      int i = 0;
      for (; i < VERTICES; ++i) {
        int edges = 0;
        final long outEdges = cachedVertices[i].countEdges(Vertex.DIRECTION.OUT, EDGE_TYPE_NAME);
        Assertions.assertEquals(expectedEdges, outEdges);

        final long inEdges = cachedVertices[i].countEdges(Vertex.DIRECTION.IN, EDGE_TYPE_NAME);
        Assertions.assertEquals(expectedEdges, inEdges);

        if (++edges > EDGES_PER_VERTEX)
          break;
      }
      //System.out.println("Checked " + expectedEdges + " edges per vertex in " + i + " vertices in " + (System.currentTimeMillis() - begin) + "ms");

    } finally {
      database.commit();
      final long elapsed = System.currentTimeMillis() - begin;
      //System.out.println("Check of graph finished in " + elapsed + "ms");
    }
  }
}
