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
package com.arcadedb.integration.importer.graph;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9575, discussion #9559: the edge pass of a 209M-vertex import stopped for 15 minutes every 500,000 edges, reading
 * the vertex record of every distinct endpoint of a flush to find an edge list that the importer knew to be empty: it had
 * created those vertices itself, without edges. The importer now tells the edge batch so, the first time it hands it a
 * vertex in each direction, and only then.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9575GraphImporterEdgePassReadsTest {
  private static final String DB_PATH  = "target/databases/issue-9575-edge-pass-reads";
  private static final int    VERTICES = 2_000;
  private static final int    EDGES    = 30_000;

  private Database database;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();
    database.transaction(() -> {
      database.getSchema().createVertexType("WORK").createProperty("id", Type.LONG);
      database.getSchema().createEdgeType("CITE");
      database.getSchema().createEdgeType("REFERS");
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      if (database.isTransactionActive())
        database.rollbackAllNested();
      if (database.isOpen())
        database.drop();
      database = null;
    }
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void theEdgePassReadsNoVertexItCreated() throws Exception {
    final int[][] cite = new int[2][VERTICES];
    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("WORK", works(), v -> {
          v.id("id");
          v.longProperty("id", "id");
        })
        .edgeSource("CITE", edges(1, cite), e -> {
          e.from("from", "WORK");
          e.to("to", "WORK");
        })
        .build()) {
      importer.run();
      assertThat(importer.getEdgeCount()).isEqualTo(EDGES);
      assertThat(importer.getVertexHeadReads()).isZero();
    }
    assertDegrees("CITE", cite);
  }

  /**
   * The case a wrong hint would break: a second edge type over the same vertices appends to the edge lists the first one
   * wrote, so it must find them - by reading the vertices - instead of starting new lists that would orphan them.
   */
  @Test
  void aSecondEdgeTypeOverTheSameVerticesKeepsTheEdgesOfTheFirst() throws Exception {
    final int[][] cite = new int[2][VERTICES];
    final int[][] refers = new int[2][VERTICES];
    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("WORK", works(), v -> {
          v.id("id");
          v.longProperty("id", "id");
        })
        .edgeSource("CITE", edges(1, cite), e -> {
          e.from("from", "WORK");
          e.to("to", "WORK");
        })
        .edgeSource("REFERS", edges(2, refers), e -> {
          e.from("from", "WORK");
          e.to("to", "WORK");
        })
        .build()) {
      importer.run();
      assertThat(importer.getEdgeCount()).isEqualTo(2L * EDGES);
      // the second type's batch read the vertices the first one had given edges
      assertThat(importer.getVertexHeadReads()).isPositive();
    }
    assertDegrees("CITE", cite);
    assertDegrees("REFERS", refers);
  }

  /**
   * The hints cover only the vertices the running import created: a second import into the same database, of the same
   * types, starts edge lists for its own vertices and leaves those of the first import's vertices as they were.
   */
  @Test
  void aSecondImportIntoTheSameDatabaseKeepsTheEdgesOfTheFirst() throws Exception {
    final int[][] first = new int[2][2 * VERTICES];
    final int[][] second = new int[2][2 * VERTICES];
    importWorks(0, 1, first);
    importWorks(VERTICES, 2, second);

    for (int i = 0; i < 2 * VERTICES; i++) {
      first[0][i] += second[0][i];
      first[1][i] += second[1][i];
    }
    assertDegrees("CITE", first);
  }

  private void importWorks(final int firstId, final long seed, final int[][] degrees) throws Exception {
    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("WORK", works(firstId), v -> {
          v.id("id");
          v.longProperty("id", "id");
        })
        .edgeSource("CITE", edges(firstId, seed, degrees), e -> {
          e.from("from", "WORK");
          e.to("to", "WORK");
        })
        .build()) {
      importer.run();
      assertThat(importer.getVertexHeadReads()).isZero();
    }
  }

  private static GraphImporter.RecordSource works() {
    return works(0);
  }

  private static GraphImporter.RecordSource works(final int firstId) {
    return visitor -> {
      final Map<String, String> row = new HashMap<>();
      final GraphImporter.RecordReader reader = row::get;
      for (int i = firstId; i < firstId + VERTICES; i++) {
        row.put("id", String.valueOf(i));
        visitor.visit(reader);
      }
    };
  }

  private static GraphImporter.RecordSource edges(final long seed, final int[][] degrees) {
    return edges(0, seed, degrees);
  }

  /**
   * Random edges between the VERTICES ids from {@code firstId}, counted into {@code degrees[0]} (out) and {@code degrees[1]}
   * (in) as they are generated.
   */
  private static GraphImporter.RecordSource edges(final int firstId, final long seed, final int[][] degrees) {
    return visitor -> {
      final Random rnd = new Random(seed);
      final Map<String, String> row = new HashMap<>();
      final GraphImporter.RecordReader reader = row::get;
      for (int i = 0; i < EDGES; i++) {
        final int from = firstId + rnd.nextInt(VERTICES);
        final int to = firstId + rnd.nextInt(VERTICES);
        degrees[0][from]++;
        degrees[1][to]++;
        row.put("from", String.valueOf(from));
        row.put("to", String.valueOf(to));
        visitor.visit(reader);
      }
    };
  }

  private void assertDegrees(final String edgeType, final int[][] degrees) {
    int checked = 0;
    try (final ResultSet rs = database.query("sql", "select from WORK")) {
      while (rs.hasNext()) {
        final Vertex work = rs.next().getVertex().get();
        final int id = (int) work.getLong("id").longValue();
        assertThat(work.countEdges(Vertex.DIRECTION.OUT, edgeType)).as("%s out of %d", edgeType, id).isEqualTo(degrees[0][id]);
        assertThat(work.countEdges(Vertex.DIRECTION.IN, edgeType)).as("%s in of %d", edgeType, id).isEqualTo(degrees[1][id]);
        ++checked;
      }
    }
    assertThat(checked).isEqualTo(degrees[0].length);
  }
}
