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
package performance;

import com.arcadedb.NullLogger;
import com.arcadedb.TestHelper;
import com.arcadedb.database.async.ErrorCallback;
import com.arcadedb.database.bucketselectionstrategy.PartitionedBucketSelectionStrategy;
import com.arcadedb.engine.WALFile;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Assertions;

import java.io.File;
import java.util.logging.Level;

/**
 * Inserts a graph. Configurations:
 * - 100M edges on 10,000 vertices with respectively 10,000 to all the other nodes and itself. 10,000 * 10,000 = 100M edges
 * - 1B edges on 31,623 vertices with respectively 31,623 edges to all the other nodes and itself. 31,623 * 31,623 = 1B edges
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PerformanceInsertGraphIndexTest extends TestHelper {
  private static final boolean CREATEDB         = true;
  private static final int     VERTICES         = 5_000_000; //31_623;
  private static final int     EDGES_PER_VERTEX = 20; //31_623;
  private static final String  VERTEX_TYPE_NAME = "Person";
  private static final String  EDGE_TYPE_NAME   = "Friend";
  private static final int     PARALLEL         = 6;
  private static final boolean USE_WAL          = true;
  private static final boolean EDGE_IDS         = true;

  public static void main(String[] args) {
    final long begin = System.currentTimeMillis();

    if (CREATEDB)
      FileUtils.deleteRecursively(new File(PerformanceTest.DATABASE_PATH));

    final PerformanceInsertGraphIndexTest test = new PerformanceInsertGraphIndexTest();

    test.database.setReadYourWrites(false);

    try {
      // PHASE 1
      if (CREATEDB) {
        test.createSchema();
        test.createVertices();
        test.loadVertices();
        test.createEdges();
      } else
        test.loadDatabase();

      // PHASE 2
      {
        test.countEdges(test.loadVertices());
        test.checkEdgeIds();
      }

    } finally {
      test.database.close();
    }

    final long elapsedSecs = (System.currentTimeMillis() - begin) / 1000;
    System.out.println("TEST completed in " + elapsedSecs + " secs = " + (elapsedSecs / 60) + " mins");
  }

  private void loadDatabase() {
    final VertexType vertex = database.getSchema().getOrCreateVertexType(VERTEX_TYPE_NAME);
    vertex.setBucketSelectionStrategy(new PartitionedBucketSelectionStrategy(new String[] { "id" }));

    final EdgeType edge = database.getSchema().getOrCreateEdgeType(EDGE_TYPE_NAME);
    edge.setBucketSelectionStrategy(new PartitionedBucketSelectionStrategy(new String[] { "id" }));
  }

  protected PerformanceInsertGraphIndexTest() {
    super(false);
  }

  private void createEdges() {
    System.out.println("Creating " + EDGES_PER_VERTEX + " edges per vertex on all " + VERTICES + " vertices");

    database.begin();

    if (!USE_WAL) {
      database.setUseWAL(false);
      database.setWALFlush(WALFile.FLUSH_TYPE.NO);
    }

    final long begin = System.currentTimeMillis();
    int edgeSerial = 0;
    try {
      int vertexIndex = 0;
      for (; vertexIndex < VERTICES; ++vertexIndex) {

        final Vertex sourceVertex = (Vertex) database.lookupByKey(VERTEX_TYPE_NAME, new String[] { "id" }, new Object[] { vertexIndex }).next().getRecord();

        int edgesPerVertex = 0;
        for (int destinationIndex = 0; destinationIndex < VERTICES; destinationIndex++) {
          final Vertex destinationVertex = (Vertex) database.lookupByKey(VERTEX_TYPE_NAME, new String[] { "id" }, new Object[] { destinationIndex }).next()
              .getRecord();

          if (EDGE_IDS) {
            sourceVertex.newEdge(EDGE_TYPE_NAME, destinationVertex, true, "id", edgeSerial++);
          } else
            sourceVertex.newEdge(EDGE_TYPE_NAME, destinationVertex, true);

          if (++edgesPerVertex >= EDGES_PER_VERTEX)
            break;
        }

        if (vertexIndex % 10_000 == 0) {
          final long elapsed = System.currentTimeMillis() - begin;
          System.out.println("Created " + edgesPerVertex + " edges per vertex in " + vertexIndex + " vertices in " + elapsed + "ms total (" + (
              vertexIndex * edgesPerVertex / elapsed * 1000) + " edges/sec)");
        }

        if (vertexIndex % 1000 == 0) {
          database.commit();
          database.begin();
          if (!USE_WAL) {
            database.setUseWAL(false);
            database.setWALFlush(WALFile.FLUSH_TYPE.NO);
          }
        }
      }

      final long elapsed = System.currentTimeMillis() - begin;
      System.out.println("Created " + EDGES_PER_VERTEX + " edges per vertex in " + vertexIndex + " vertices in " + elapsed + "ms total (" + (
          vertexIndex * EDGES_PER_VERTEX / elapsed * 1000) + " edges/sec)");

    } finally {
      if (database.isTransactionActive()) {
        database.commit();
        final long elapsed = System.currentTimeMillis() - begin;
        System.out.println("Creation of edges finished in " + elapsed + "ms");
      }
    }
  }

  private Vertex[] loadVertices() {
    final Vertex[] cachedVertices = new Vertex[VERTICES];

    System.out.println("Loading " + VERTICES + " vertices in RAM...");
    database.transaction(() -> {
      final long begin = System.currentTimeMillis();
      try {
        int counter = 0;
        for (; counter < VERTICES; ++counter) {
          final IndexCursor cursor = database.lookupByKey(VERTEX_TYPE_NAME, new String[] { "id" }, new Object[] { counter });
          if (!cursor.hasNext()) {
            System.out.println("Vertex with id " + counter + " was not found");
            continue;
          }

          cachedVertices[counter] = (Vertex) cursor.next().getRecord();
        }
        System.out.println("Loaded " + counter + " vertices in " + (System.currentTimeMillis() - begin) + "ms");
      } finally {
        final long elapsed = System.currentTimeMillis() - begin;
        System.out.println("Loaded all vertices in RAM in " + elapsed + "ms -> " + (VERTICES / (elapsed / 1000F)) + " ops/sec");
      }
    });
    return cachedVertices;
  }

  private void createVertices() {
    System.out.println("Start inserting " + VERTICES + " vertices...");

    long startOfTest = System.currentTimeMillis();

    try {
      //database.setEdgeListSize(256);

      //database.async().setBackPressure(0);
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

      int counter = 0;
      for (; counter < VERTICES; ++counter) {
        final MutableVertex vertex = database.newVertex(VERTEX_TYPE_NAME);

        vertex.set("id", counter);

        database.async().createRecord(vertex, null);

        if (counter % 1_000_000 == 0)
          System.out.println("Inserted " + counter + " vertices in " + (System.currentTimeMillis() - startOfTest) + "ms");
      }

      System.out.println("Inserted " + counter + " vertices in " + (System.currentTimeMillis() - startOfTest) + "ms");

    } finally {
      database.async().waitCompletion();
      final long elapsed = System.currentTimeMillis() - startOfTest;
      System.out.println("Insertion finished in " + elapsed + "ms -> " + (VERTICES / (elapsed / 1000F)) + " ops/sec");
    }
  }

  private void createSchema() {
    final VertexType vertex = database.getSchema().createVertexType(VERTEX_TYPE_NAME, PARALLEL);
    vertex.createProperty("id", Integer.class);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, VERTEX_TYPE_NAME, "id");
    vertex.setBucketSelectionStrategy(new PartitionedBucketSelectionStrategy(new String[] { "id" }));

    final EdgeType edge = database.getSchema().createEdgeType(EDGE_TYPE_NAME, PARALLEL);
    if (EDGE_IDS) {
      edge.createProperty("id", Integer.class);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, EDGE_TYPE_NAME, "id");
      edge.setBucketSelectionStrategy(new PartitionedBucketSelectionStrategy(new String[] { "id" }));
    }
  }

  private void checkEdgeIds() {
    if (!EDGE_IDS)
      return;

    System.out.println("Checking ids on edges");

    final long begin = System.currentTimeMillis();

    try {
      final int expectedTotalEdges = VERTICES * EDGES_PER_VERTEX;

      final Index index = database.getSchema().getIndexByName(EDGE_TYPE_NAME + "[id]");

      int j = 0;
      for (; j < expectedTotalEdges; j++) {
        IndexCursor edgeCursor = index.get(new Object[] { j });
        Assertions.assertTrue(edgeCursor.hasNext());
        final Edge e = edgeCursor.next().asEdge(true);
        Assertions.assertNotNull(e);
        Assertions.assertEquals(j, e.get("id"));

        if (j % 1_000_000 == 0) {
          final long elapsed = System.currentTimeMillis() - begin;
          System.out.println("Checked ids for " + j + " edges in " + elapsed + "ms total (" + (j / elapsed * 1000) + " edges/sec)");
        }
      }

      final long elapsed = System.currentTimeMillis() - begin;
      System.out.println("Checked ids for " + j + " edges in " + elapsed + "ms total (" + (j / elapsed * 1000) + " edges/sec)");

    } finally {
      final long elapsed = System.currentTimeMillis() - begin;
      System.out.println("Check of edge ids finished in " + elapsed + "ms");
    }
  }

  private void countEdges(Vertex[] cachedVertices) {
    System.out.println("Checking graph with " + VERTICES + " vertices");

    database.begin();
    final long begin = System.currentTimeMillis();

    try {
      int i = 0;
      int outEdges = 0;
      int inEdges = 0;
      for (; i < VERTICES; ++i) {
        for (Edge e : cachedVertices[i].getEdges(Vertex.DIRECTION.OUT, EDGE_TYPE_NAME)) {
          if (EDGE_IDS)
            Assertions.assertNotNull(e.get("id"));
          ++outEdges;
        }

        for (Edge e : cachedVertices[i].getEdges(Vertex.DIRECTION.IN, EDGE_TYPE_NAME)) {
          if (EDGE_IDS)
            Assertions.assertNotNull(e.get("id"));
          ++inEdges;
        }

        // HELP THE GC
        cachedVertices[i] = null;

        if (i % 100_000 == 0) {
          final long elapsed = System.currentTimeMillis() - begin;
          System.out.println(
              "Checked " + outEdges + " outgoing edges and " + inEdges + " incoming edges per vertex in " + i + " vertices in " + elapsed + "ms total (" + (
                  (outEdges + inEdges) / elapsed * 1000) + " edges/sec)");
        }
      }

      final long elapsed = System.currentTimeMillis() - begin;
      System.out.println(
          "Checked " + outEdges + " outgoing edges and " + inEdges + " incoming edges per vertex in " + i + " vertices in " + elapsed + "ms total (" + (
              (outEdges + inEdges) / elapsed * 1000) + " edges/sec)");

      final int expectedTotalEdges = VERTICES * EDGES_PER_VERTEX;

      Assertions.assertEquals(expectedTotalEdges, inEdges);
      Assertions.assertEquals(expectedTotalEdges, outEdges);

    } finally {
      database.commit();
      final long elapsed = System.currentTimeMillis() - begin;
      System.out.println("Check of graph finished in " + elapsed + "ms");
    }
  }

  @Override
  protected String getDatabasePath() {
    return PerformanceTest.DATABASE_PATH;
  }

  @Override
  protected String getPerformanceProfile() {
    LogManager.instance().setLogger(NullLogger.INSTANCE);

    return "high-performance";
  }
}
