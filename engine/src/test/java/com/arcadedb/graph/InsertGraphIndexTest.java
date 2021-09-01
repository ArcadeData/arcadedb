/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.async.ErrorCallback;
import com.arcadedb.engine.WALFile;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
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
    final InsertGraphIndexTest test = new InsertGraphIndexTest();

    // PHASE 1
    {
      test.createSchema();
      test.createVertices();
      test.loadVertices();
      test.createEdges();
    }

    // PHASE 2
    {
      Vertex[] cachedVertices = test.loadVertices();
      test.checkGraph(cachedVertices);
    }

    test.database.close();
  }

  @Override
  protected String getPerformanceProfile() {
    LogManager.instance().setLogger(new Logger() {
      @Override
      public void log(Object iRequester, Level iLevel, String iMessage, Throwable iException, String context, Object arg1, Object arg2, Object arg3,
          Object arg4, Object arg5, Object arg6, Object arg7, Object arg8, Object arg9, Object arg10, Object arg11, Object arg12, Object arg13, Object arg14,
          Object arg15, Object arg16, Object arg17) {
      }

      @Override
      public void log(Object iRequester, Level iLevel, String iMessage, Throwable iException, String context, Object... args) {
      }

      @Override
      public void flush() {
      }
    });

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
    database.transaction((tx) -> {
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
    database.begin();

    final VertexType type = database.getSchema().createVertexType(VERTEX_TYPE_NAME, PARALLEL);
    type.createProperty("id", Long.class);

    database.getSchema().createEdgeType(EDGE_TYPE_NAME, PARALLEL);

    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, VERTEX_TYPE_NAME, new String[] { "id" }, 5000000);

    database.commit();
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
