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
package com.arcadedb.integration.importer;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.integration.importer.graph.CsvRowSource;
import com.arcadedb.integration.importer.graph.GraphImporter;
import com.arcadedb.integration.importer.graph.JsonlRowSource;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.StallAwareStopwatch;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7432: a {@link GraphImporter} load into a type carrying an {@code LSM_VECTOR} index
 * ended with no graph, twice over.
 * <p>
 * {@code GraphBatch} suspends the index's inactivity rebuild while it is open (issue #7357), but the importer is
 * not one batch: the vertex batch closes, the edge sources are read for their topology with no batch open, then
 * one edge batch per edge type opens. The first close lifted the only suspension and armed the timer, which fired
 * in that gap and started a full build that then ran alongside the whole edge pass. And the one build the load is
 * worth was scheduled by the timer, asynchronously, a window after the load - so the {@code database.close()} that
 * follows every completed import cancelled it. The reporter lost a 20-minute build over 4.2M vectors five seconds
 * after "Import complete".
 * <p>
 * The fixture is the reporter's shape at toy scale: a JSONL vertex file with an embedding per row, a tab-separated
 * edge file, a unique index on the id and an INT8 vector index storing its vectors in the graph.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7432GraphImporterVectorGraphTest {
  private static final String DB_PATH    = "target/databases/issue-7432-graph-importer-vector-graph";
  private static final String DATA_DIR   = "target/test-data/issue-7432";
  private static final int    DIMENSIONS = 16;
  private static final int    VERTICES   = 300;
  private static final int    TIMEOUT_MS = 300;

  private DatabaseFactory factory;
  private Database        database;

  @BeforeEach
  void setUp() throws IOException {
    FileUtils.deleteRecursively(new File(DB_PATH));
    writeFixture();
    factory = new DatabaseFactory(DB_PATH);
    database = factory.create();
    createSchema(database);
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
    else
      FileUtils.deleteRecursively(new File(DB_PATH));
    if (factory != null)
      factory.close();
    FileUtils.deleteRecursively(new File(DATA_DIR));
  }

  /**
   * The edge source stalls for several inactivity windows before yielding its first row. In the importer that
   * stall lands exactly between the vertex batch's close and the edge batch's open, which is the gap the rebuild
   * used to fire in. One build for the whole load, run by the importer itself before {@code run()} returns, and
   * a database closed the moment the import returns reopens with that graph on disk.
   */
  @Test
  void theRebuildWaitsForTheWholeImportAndTheGraphIsBuiltBeforeRunReturns() throws Exception {
    final LSMVectorIndex index = vectorIndex(database);

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("WORK", new JsonlRowSource(DATA_DIR + "/vertices.jsonl"), v -> {
          v.id("id");
          v.longProperty("id", "id");
          v.floatArrayProperty("embedding", "embedding");
        })
        .edgeSource("CITE", new StallingSource(new CsvRowSource(DATA_DIR + "/edges.tsv", '\t', 0), TIMEOUT_MS * 6L, () -> {
          // Observed from inside the gap, where the vertex batch is closed and no edge batch is open yet. Without
          // the importer's own suspension the timer has fired by now and, on a graph this small, built
          // synchronously on its own thread - which the count after run() cannot tell apart from the importer's
          // build, because that one finds nothing pending and skips.
          final Map<String, Long> midGap = index.getStats();
          assertThat(midGap.get("graphRebuildCount"))
              .as("no rebuild fires in the gap between the vertex pass and the edge pass (issue #7432)").isZero();
          assertThat(midGap.get("asyncRebuildInProgress")).as("nor is one running").isZero();
          assertThat(midGap.get("backgroundMaintenanceSuspensions"))
              .as("the importer's suspension is what holds the timer off while no batch is open").isEqualTo(1L);
        }), e -> {
          e.from("from_id", "WORK");
          e.to("to_id", "WORK");
        })
        .build()) {

      importer.run();
      assertThat(importer.getVertexCount()).isEqualTo(VERTICES);
      assertThat(importer.getEdgeCount()).isEqualTo(VERTICES * 2L);
    }

    final Map<String, Long> stats = index.getStats();
    assertThat(stats.get("graphRebuildCount"))
        .as("exactly one build for the whole load: the importer's own, and none from the inactivity timer in the "
            + "gap between the vertex pass and the edge pass (issue #7432)")
        .isEqualTo(1L);
    assertThat(stats.get("graphNodeCount"))
        .as("and it covers every vector the load wrote")
        .isEqualTo((long) VERTICES);
    assertThat(stats.get("mutationsSinceRebuild"))
        .as("nothing is left for a background rebuild the caller cannot see")
        .isZero();
    assertThat(stats.get("asyncRebuildInProgress"))
        .as("the build ran on the importer's thread, so run() returned with it complete")
        .isZero();
    assertThat(stats.get("backgroundMaintenanceSuspensions"))
        .as("the importer lifted its own suspension on the way out")
        .isZero();

    // The reporter's next line: close the database as soon as the import returns. The graph must be on disk.
    database.close();
    database = factory.open();
    final LSMVectorIndex reopened = vectorIndex(database);
    assertThat(reopened.getStats().get("persistedGraphNodeCount"))
        .as("the graph built before run() returned survived the close that followed it")
        .isEqualTo((long) VERTICES);
    assertThat(reopened.getStats().get("mutationsSinceRebuild"))
        .as("no vector is outside the persisted graph on reopen")
        .isZero();

    assertThat(nearestId(database, 42)).as("and the reopened index answers from that graph").isEqualTo(1_000_042L);
  }

  /**
   * The per-index loop: a second vector index on a type the import CONFIGURED a source for but wrote nothing to
   * (the source is filtered down to no rows) keeps its own pending vectors and is not built - the import never
   * touched it, and the pending work is somebody else's - and an index declared on a PARENT type is built when the
   * import writes only to a subtype. The importer matches on the type each bucket index names, and the subtype's
   * bucket index names the subtype - pinned here because it is what makes the match correct without a walk of the
   * hierarchy (PR #7433 review).
   */
  @Test
  void aParentTypeIndexIsBuiltAndAnIndexNothingWasWrittenToIsLeftAlone() throws Exception {
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 600_000);
    database.command("sqlscript", """
        CREATE VERTEX TYPE PAPER EXTENDS WORK;
        CREATE VERTEX TYPE NOTE;
        CREATE PROPERTY NOTE.embedding ARRAY_OF_FLOATS;
        CREATE INDEX ON NOTE (embedding) LSM_VECTOR METADATA { "dimensions": %d, "similarity": "COSINE" };
        """.formatted(DIMENSIONS));
    database.transaction(() -> {
      for (int i = 0; i < 5; i++)
        database.newVertex("NOTE").set("embedding", embedding(i)).save();
    });
    final LSMVectorIndex noteIndex = (LSMVectorIndex) database.getSchema().getType("NOTE")
        .getPolymorphicIndexByProperties("embedding").getIndexesOnBuckets()[0];
    assertThat(noteIndex.getStats().get("mutationsSinceRebuild")).as("precondition: NOTE has work pending").isEqualTo(5L);

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("PAPER", new JsonlRowSource(DATA_DIR + "/vertices.jsonl"), v -> {
          v.id("id");
          v.longProperty("id", "id");
          v.floatArrayProperty("embedding", "embedding");
        })
        // Configured, so NOTE is a type this import knows about, but the filter admits no row: nothing is written.
        .vertex("NOTE", new JsonlRowSource(DATA_DIR + "/vertices.jsonl"), v -> {
          v.id("id");
          v.filter("id", "no such id");
          v.floatArrayProperty("embedding", "embedding");
        })
        .build()) {
      importer.run();
      assertThat(importer.getVertexCount()).as("PAPER's rows only; NOTE's source admitted none").isEqualTo(VERTICES);
    }

    // The index on WORK covers PAPER's bucket through its own bucket index, which names PAPER as its type.
    final int paperBucket = database.getSchema().getType("PAPER").getBuckets(false).get(0).getFileId();
    final LSMVectorIndex paperIndex = (LSMVectorIndex) Arrays.stream(database.getSchema().getType("PAPER")
            .getPolymorphicIndexByProperties("embedding").getIndexesOnBuckets())
        .filter(i -> i.getAssociatedBucketId() == paperBucket).findFirst().orElseThrow();
    assertThat(paperIndex.getTypeName()).as("the shape the type match relies on: the subtype's bucket index names the subtype")
        .isEqualTo("PAPER");
    assertThat(paperIndex.getStats().get("graphRebuildCount"))
        .as("the parent-type index the import wrote into through the subtype is built").isEqualTo(1L);
    assertThat(paperIndex.getStats().get("graphNodeCount")).isEqualTo((long) VERTICES);
    assertThat(paperIndex.getStats().get("mutationsSinceRebuild")).isZero();

    assertThat(noteIndex.getStats().get("graphRebuildCount"))
        .as("an index on a type the import wrote nothing to is left to its own rebuild, configured source or not")
        .isZero();
    assertThat(noteIndex.getStats().get("mutationsSinceRebuild")).isEqualTo(5L);
  }

  /**
   * The JSON form of the opt-out, for a loader that keeps the database open and prefers the index's background
   * rebuild. The load leaves its vectors pending and the index unsuspended, and nothing has been built on the
   * importer's thread.
   */
  @Test
  void optingOutLeavesTheBuildToTheIndex() throws Exception {
    // Well beyond the test: a background rebuild must not start before the assertions below read the counters.
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 600_000);
    final LSMVectorIndex index = vectorIndex(database);

    final JSONObject config = new JSONObject()
        .put("vectorGraphBuild", false)
        .put("vertices", new JSONArray().put(new JSONObject()
            .put("type", "WORK").put("file", "vertices.jsonl").put("id", "id")
            .put("properties", new JSONObject().put("id", "long:id").put("embedding", "vector:embedding"))))
        .put("edgeSources", new JSONArray().put(new JSONObject()
            .put("edge", "CITE").put("file", "edges.tsv").put("format", "csv").put("delimiter", "\t")
            .put("from", "from_id:WORK").put("to", "to_id:WORK")));

    try (final GraphImporter importer = GraphImporter.fromJSON(database, config, DATA_DIR)) {
      importer.run();
      assertThat(importer.getVertexCount()).isEqualTo(VERTICES);
      assertThat(importer.getEdgeCount()).isEqualTo(VERTICES * 2L);
    }

    final Map<String, Long> stats = index.getStats();
    assertThat(stats.get("graphRebuildCount")).as("opted out: the importer built nothing").isZero();
    assertThat(stats.get("mutationsSinceRebuild"))
        .as("every vector is still pending, served by the delta scan until the index rebuilds on its own")
        .isEqualTo((long) VERTICES);
    assertThat(stats.get("backgroundMaintenanceSuspensions"))
        .as("and the index is free to do so: the importer lifted its suspension")
        .isZero();
  }

  /**
   * Wraps a source so that its first row comes only after {@code stallMs} of running time: what an LSM compaction
   * or a GC pause looks like from inside the vector index, placed where the importer has no batch open. Runs
   * {@code afterStall} before the first row, which is where the test looks at what the stall did.
   */
  private static final class StallingSource implements GraphImporter.RecordSource {
    private final GraphImporter.RecordSource delegate;
    private final long                       stallMs;
    private final Runnable                   afterStall;

    private StallingSource(final GraphImporter.RecordSource delegate, final long stallMs, final Runnable afterStall) {
      this.delegate = delegate;
      this.stallMs = stallMs;
      this.afterStall = afterStall;
    }

    @Override
    public void forEach(final GraphImporter.RecordVisitor visitor) throws Exception {
      // Running time, not wall clock: a stop-the-world pause covering the stall would leave the timer no CPU to
      // fire on, and "nothing rebuilt" would then hold because nothing ran rather than because the suspension held.
      final StallAwareStopwatch stall = StallAwareStopwatch.start();
      while (stall.effectiveMs() < stallMs)
        Thread.sleep(25);
      afterStall.run();
      delegate.forEach(visitor);
    }

    @Override
    public Character fieldSeparator() {
      return delegate.fieldSeparator();
    }
  }

  private static void createSchema(final Database db) {
    db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, TIMEOUT_MS);
    db.command("sqlscript", """
        CREATE VERTEX TYPE WORK;
        CREATE PROPERTY WORK.id LONG;
        CREATE PROPERTY WORK.embedding ARRAY_OF_FLOATS;
        CREATE INDEX ON WORK (id) UNIQUE;
        CREATE INDEX ON WORK (embedding) LSM_VECTOR METADATA {
          "dimensions": %d, "similarity": "COSINE", "quantization": "INT8", "storeVectorsInGraph": true
        };
        CREATE EDGE TYPE CITE;
        """.formatted(DIMENSIONS));
  }

  private static LSMVectorIndex vectorIndex(final Database db) {
    return (LSMVectorIndex) db.getSchema().getType("WORK")
        .getPolymorphicIndexByProperties("embedding").getIndexesOnBuckets()[0];
  }

  private static long nearestId(final Database db, final int id) {
    try (final ResultSet rs = db.query("sql",
        "SELECT id FROM (SELECT expand(vectorNeighbors('WORK[embedding]', ?, 1)))", (Object) embedding(id))) {
      assertThat(rs.hasNext()).isTrue();
      return rs.next().getProperty("id");
    }
  }

  private static void writeFixture() throws IOException {
    final File dir = new File(DATA_DIR);
    FileUtils.deleteRecursively(dir);
    assertThat(dir.mkdirs()).isTrue();

    final StringBuilder vertices = new StringBuilder();
    for (int i = 0; i < VERTICES; i++)
      // Arrays.toString() spells a float[] as a JSON array.
      vertices.append("{\"id\":").append(1_000_000L + i).append(",\"embedding\":").append(Arrays.toString(embedding(i)))
          .append("}\n");
    Files.writeString(new File(dir, "vertices.jsonl").toPath(), vertices.toString(), StandardCharsets.UTF_8);

    final StringBuilder edges = new StringBuilder("from_id\tto_id\n");
    for (int i = 0; i < VERTICES; i++) {
      edges.append(1_000_000L + i).append('\t').append(1_000_000L + (i + 1) % VERTICES).append('\n');
      edges.append(1_000_000L + i).append('\t').append(1_000_000L + (i * 7) % VERTICES).append('\n');
    }
    Files.writeString(new File(dir, "edges.tsv").toPath(), edges.toString(), StandardCharsets.UTF_8);
  }

  /** Deterministic per-id embedding, so a fixture is reproducible run to run. */
  private static float[] embedding(final int id) {
    final Random random = new Random(0x7432L * 17 + id);
    final float[] vector = new float[DIMENSIONS];
    for (int d = 0; d < DIMENSIONS; d++)
      vector[d] = random.nextFloat();
    return vector;
  }
}
