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
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

import static com.arcadedb.integration.importer.graph.GraphImporter.MAX_ARRAY_LENGTH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9575, reported as discussion #9559: {@link GraphImporter} over 209M vertices and 2.26B edges with a 16 GB
 * heap logged "Pass 1" and then nothing for 22 hours. The vertex pass wrote a 2 KB edge segment for every vertex, an
 * edge source was held in memory as 8 bytes an edge before any edge was written - in arrays that cannot grow past
 * 2^30 entries - and nothing said how far a pass had got.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9575GraphImporterLargeLoadTest {

  private static final String DB_PATH  = "target/databases/issue-9575-large-load";
  private static final String BASE_DIR = "target/issue-9575-sources";

  private Database database;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    FileUtils.deleteRecursively(new File(BASE_DIR));
    new File(BASE_DIR).mkdirs();

    database = new DatabaseFactory(DB_PATH).create();
    database.transaction(() -> {
      database.getSchema().createVertexType("WORK").createProperty("id", Type.LONG);
      database.getSchema().createEdgeType("CITE");
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
    FileUtils.deleteRecursively(new File(BASE_DIR));
  }

  /**
   * The vertex pass used to give every vertex an OUT edge segment of the batch's 2 KB initial size, edges or not: at
   * 209M vertices that is ~400 GB of segments, most of each one never filled. A vertex now gets a segment, sized for
   * its edges, only when it has one.
   */
  @Test
  void aVertexWithoutEdgesGetsNoEdgeSegment() throws Exception {
    write("works.csv", "id", "1", "2", "3");
    write("cites.csv", "from_id,to_id", "1,2");

    importWorks(CsvRowSource.from(BASE_DIR, "works.csv"), CsvRowSource.from(BASE_DIR, "cites.csv"));

    final Map<Long, VertexInternal> works = loadWorks();
    // the source of the edge has an OUT list and no IN list, its target the other way round
    assertThat(works.get(1L).getOutEdgesHeadChunk()).isNotNull();
    assertThat(works.get(1L).getInEdgesHeadChunk()).isNull();
    assertThat(works.get(2L).getOutEdgesHeadChunk()).isNull();
    assertThat(works.get(2L).getInEdgesHeadChunk()).isNotNull();
    // the vertex no edge touches has neither
    assertThat(works.get(3L).getOutEdgesHeadChunk()).isNull();
    assertThat(works.get(3L).getInEdgesHeadChunk()).isNull();

    assertThat(works.get(1L).countEdges(Vertex.DIRECTION.OUT, "CITE")).isEqualTo(1);
    assertThat(works.get(2L).countEdges(Vertex.DIRECTION.IN, "CITE")).isEqualTo(1);

    // one segment per direction for the one edge, and nothing for the vertices it does not touch
    assertThat(countEdgeSegments("out_edges")).isEqualTo(1);
    assertThat(countEdgeSegments("in_edges")).isEqualTo(1);
  }

  /**
   * An edge source is streamed into its batch: by the time the source has handed over more rows than one batch
   * buffers, edges are already on the disk. They used to be collected in full first, so nothing was written until
   * the source had been read to its end.
   */
  @Test
  void anEdgeSourceIsWrittenWhileItIsStillBeingRead() throws Exception {
    final int vertices = 1_000;
    final String[] works = new String[vertices + 1];
    works[0] = "id";
    for (int i = 0; i < vertices; i++)
      works[i + 1] = String.valueOf(i);
    write("works.csv", works);

    // one more row than the edge batch buffers before it flushes
    final int rows = 500_001;
    final long[] edgesOnDiskWhileReading = {-1};
    final GraphImporter.RecordSource edges = visitor -> {
      final Map<String, String> row = new HashMap<>();
      final GraphImporter.RecordReader reader = row::get;
      for (int i = 0; i < rows; i++) {
        row.put("from", String.valueOf(i % vertices));
        row.put("to", String.valueOf((i * 7 + 1) % vertices));
        visitor.visit(reader);
      }
      // still inside the source: the rows are all handed over, the source has not returned yet
      edgesOnDiskWhileReading[0] = database.countType("CITE", false);
    };

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("WORK", CsvRowSource.from(BASE_DIR, "works.csv"), v -> {
          v.id("id");
          v.longProperty("id", "id");
        })
        .edgeSource("CITE", edges, e -> {
          e.from("from", "WORK");
          e.to("to", "WORK");
        })
        .build()) {
      importer.run();
      assertThat(importer.getEdgeCount()).isEqualTo(rows);
    }

    assertThat(edgesOnDiskWhileReading[0]).isPositive();
    assertThat(database.countType("CITE", false)).isEqualTo(rows);

    long out = 0;
    long in = 0;
    for (final VertexInternal work : loadWorks().values()) {
      out += work.countEdges(Vertex.DIRECTION.OUT, "CITE");
      in += work.countEdges(Vertex.DIRECTION.IN, "CITE");
    }
    assertThat(out).isEqualTo(rows);
    assertThat(in).isEqualTo(rows);
  }

  /**
   * A row that cannot be read still aborts the import, as it always did, but the edges of the rows before it are now
   * written by then. They must be complete edges, reachable from both of their vertices, and the import must leave no
   * transaction behind.
   */
  @Test
  void aFailingEdgeRowLeavesTheEdgesBeforeItConnected() throws Exception {
    write("works.csv", "id", "1", "2", "3");
    write("cites.csv", "from_id,to_id,weight", "1,2,10", "2,3,20", "3,1,not-a-number");
    database.transaction(() -> database.getSchema().getType("CITE").createProperty("weight", Type.INTEGER));

    assertThatThrownBy(() -> {
      try (final GraphImporter importer = GraphImporter.builder(database)
          .vertex("WORK", CsvRowSource.from(BASE_DIR, "works.csv"), v -> {
            v.id("id");
            v.longProperty("id", "id");
          })
          .edgeSource("CITE", CsvRowSource.from(BASE_DIR, "cites.csv"), e -> {
            e.from("from_id", "WORK");
            e.to("to_id", "WORK");
            e.intProperty("weight", "weight");
          })
          .build()) {
        importer.run();
      }
    }).hasMessageContaining("not-a-number");

    assertThat(database.isTransactionActive()).isFalse();
    assertThat(database.countType("CITE", false)).isEqualTo(2);

    final Map<Long, VertexInternal> works = loadWorks();
    assertThat(works.get(1L).countEdges(Vertex.DIRECTION.OUT, "CITE")).isEqualTo(1);
    assertThat(works.get(2L).countEdges(Vertex.DIRECTION.IN, "CITE")).isEqualTo(1);
    assertThat(works.get(2L).countEdges(Vertex.DIRECTION.OUT, "CITE")).isEqualTo(1);
    assertThat(works.get(3L).countEdges(Vertex.DIRECTION.IN, "CITE")).isEqualTo(1);
    assertThat(works.get(1L).countEdges(Vertex.DIRECTION.IN, "CITE")).isZero();

    try (final ResultSet rs = database.query("sql", "SELECT sum(weight) AS total FROM CITE")) {
      assertThat(rs.next().<Number>getProperty("total").intValue()).isEqualTo(30);
    }
  }

  /**
   * A list doubled its length on growth, which past 2^30 entries is a negative length: an edge collector holding
   * more than a billion edges ended the import with a {@code NegativeArraySizeException}. It now grows by half, up to
   * the largest array the JVM allocates, and refuses to go further with a message that says what to do.
   */
  @Test
  void aListGrowsByHalfAndStopsAtTheLargestArray() {
    assertThat(GraphImporter.grownCapacity(1 << 30)).isEqualTo((1 << 30) + (1 << 29) + 16);
    assertThat(GraphImporter.grownCapacity(1_500_000_000)).isEqualTo(MAX_ARRAY_LENGTH);
    assertThat(GraphImporter.grownCapacity(0)).isPositive();
    assertThatThrownBy(() -> GraphImporter.grownCapacity(MAX_ARRAY_LENGTH))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("edge source");

    // the identity maps' tables grow the same way, with the same ceiling
    assertThat(GraphImporter.grownTableCapacity(1 << 30)).isEqualTo((1 << 30) + (1 << 29) + 1);
    assertThat(GraphImporter.grownTableCapacity(1_500_000_000)).isEqualTo(MAX_ARRAY_LENGTH);
    assertThatThrownBy(() -> GraphImporter.grownTableCapacity(MAX_ARRAY_LENGTH)).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void aVertexRidPacksIntoOneLong() {
    final GraphImporter.TypeState ts = new GraphImporter.TypeState();
    final RID[] rids = { new RID(0, 0), new RID(17, 123_456_789_012L), new RID((1 << 23) - 1, (1L << 40) - 1) };
    ts.rids = new long[rids.length];
    for (int i = 0; i < rids.length; i++)
      ts.rids[i] = GraphImporter.packRID(rids[i]);
    for (int i = 0; i < rids.length; i++)
      assertThat(ts.rid(i)).isEqualTo(rids[i]);

    assertThatThrownBy(() -> GraphImporter.packRID(new RID(0, 1L << 40))).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> GraphImporter.packRID(new RID(1 << 23, 0))).isInstanceOf(IllegalStateException.class);
  }

  /**
   * What the progress line reports as the share of an interval spent collecting garbage, the number its heap
   * warning is decided on.
   */
  @Test
  void theGcShareIsAPercentageOfTheInterval() {
    assertThat(GraphImporter.gcSharePercent(0, 30_000)).isZero();
    assertThat(GraphImporter.gcSharePercent(15_000, 30_000)).isEqualTo(50);
    assertThat(GraphImporter.gcSharePercent(29_700, 30_000)).isEqualTo(99);
    // a collector on several threads can report more pause time than wall clock went by
    assertThat(GraphImporter.gcSharePercent(90_000, 30_000)).isEqualTo(100);
    assertThat(GraphImporter.gcSharePercent(10, 0)).isZero();
    assertThat(GraphImporter.gcPauseMillis()).isNotNegative();
  }

  private void importWorks(final GraphImporter.RecordSource works, final GraphImporter.RecordSource cites) throws Exception {
    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("WORK", works, v -> {
          v.id("id");
          v.longProperty("id", "id");
        })
        .edgeSource("CITE", cites, e -> {
          e.from("from_id", "WORK");
          e.to("to_id", "WORK");
        })
        .build()) {
      importer.run();
    }
  }

  private Map<Long, VertexInternal> loadWorks() {
    final Map<Long, VertexInternal> works = new HashMap<>();
    database.iterateType("WORK", false).forEachRemaining(r -> {
      final Vertex v = r.asVertex();
      works.put(v.getLong("id"), (VertexInternal) v);
    });
    return works;
  }

  /** Records in the edge-list buckets of WORK of one direction, however many buckets the type was created with. */
  private long countEdgeSegments(final String suffix) {
    long total = 0;
    for (final Bucket bucket : database.getSchema().getBuckets())
      if (bucket.getName().startsWith("WORK_") && bucket.getName().endsWith("_" + suffix))
        total += database.countBucket(bucket.getName());
    return total;
  }

  private void write(final String fileName, final String... lines) throws Exception {
    Files.writeString(new File(BASE_DIR, fileName).toPath(), String.join("\n", lines) + "\n", StandardCharsets.UTF_8);
  }
}
