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
package com.arcadedb.index.sparsevector;

import com.arcadedb.TestHelper;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8536: an {@code LSM_SPARSE_VECTOR} index created (or rebuilt) over a type that already holds rows reported
 * every row in {@code totalIndexed} while {@code vector.sparseNeighbors} answered nothing. The build delegated to the
 * underlying LSM-Tree, which indexed each scanned record into ITSELF - a registration shell postings never enter -
 * so the sparse engine the search reads was left empty.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8536SparseIndexBuildOverExistingRowsTest extends TestHelper {

  private static final String ROWS = """
      [{name:"a",tokens:[1,5,9],weights:[0.9,0.5,0.1]}, {name:"b",tokens:[1,6],weights:[0.8,0.6]},
       {name:"c",tokens:[2,7],weights:[0.7,0.3]},       {name:"d",tokens:[3,8],weights:[0.2,0.3]},
       {name:"e",tokens:[4,10],weights:[0.2,0.3]},      {name:"f",tokens:[11,12],weights:[0.2,0.3]},
       {name:"g",tokens:[13,14],weights:[0.2,0.3]},     {name:"h",tokens:[15,16],weights:[0.2,0.3]},
       {name:"i",tokens:[17,18],weights:[0.2,0.3]},     {name:"j",tokens:[19,20],weights:[0.2,0.3]}]""";

  /** The control from the issue: the index exists before the rows, so every row reaches it through a live put. */
  @Test
  void indexCreatedBeforeTheRowsAnswers() {
    createType("SpA");
    database.command("sql", "CREATE INDEX ON SpA (tokens, weights) LSM_SPARSE_VECTOR METADATA { dimensions: 100, modifier: 'IDF' }");
    database.transaction(() -> database.command("sql", "INSERT INTO SpA CONTENT " + ROWS));

    assertThat(neighbors("SpA", new int[] { 1, 5 }, new float[] { 0.9f, 0.5f }, 3)).containsExactly("a", "b");
  }

  /** The reported case: the index is created over rows already stored. */
  @Test
  void indexCreatedOverExistingRowsAnswers() {
    createType("SpD");
    database.transaction(() -> database.command("sql", "INSERT INTO SpD CONTENT " + ROWS));

    try (final ResultSet rs = database.command("sql",
        "CREATE INDEX ON SpD (tokens, weights) LSM_SPARSE_VECTOR METADATA { dimensions: 100, modifier: 'IDF' }")) {
      assertThat(rs.next().<Number>getProperty("totalIndexed").longValue()).isEqualTo(10L);
    }

    assertThat(neighbors("SpD", new int[] { 1, 5 }, new float[] { 0.9f, 0.5f }, 3)).containsExactly("a", "b");
    assertThat(sparseIndex("SpD").countEntries()).as("every non-zero weight of the 10 rows is a posting").isEqualTo(21L);
  }

  /** Same without the IDF modifier, which the issue also reported broken. */
  @Test
  void indexCreatedOverExistingRowsAnswersWithoutIdf() {
    createType("SpN");
    database.transaction(() -> database.command("sql", "INSERT INTO SpN CONTENT " + ROWS));
    database.command("sql", "CREATE INDEX ON SpN (tokens, weights) LSM_SPARSE_VECTOR METADATA { dimensions: 100 }");

    assertThat(neighbors("SpN", new int[] { 1, 5 }, new float[] { 0.9f, 0.5f }, 3)).containsExactly("a", "b");
  }

  /** REBUILD INDEX drops and recreates the index, then takes the same build path: it must repopulate the engine. */
  @Test
  void rebuildIndexRepopulatesTheEngine() {
    createType("SpR");
    database.command("sql", "CREATE INDEX ON SpR (tokens, weights) LSM_SPARSE_VECTOR METADATA { dimensions: 100 }");
    database.transaction(() -> database.command("sql", "INSERT INTO SpR CONTENT " + ROWS));
    assertThat(neighbors("SpR", new int[] { 1, 5 }, new float[] { 0.9f, 0.5f }, 3)).containsExactly("a", "b");

    try (final ResultSet rs = database.command("sql", "REBUILD INDEX `SpR[tokens,weights]`")) {
      assertThat(rs.next().<Number>getProperty("totalIndexed").longValue()).isEqualTo(10L);
    }

    assertThat(neighbors("SpR", new int[] { 1, 5 }, new float[] { 0.9f, 0.5f }, 3)).containsExactly("a", "b");
    assertThat(sparseIndex("SpR").countEntries()).as("a rebuild must not double the postings").isEqualTo(21L);
  }

  /**
   * A build that spans many commit batches and more than one bucket, then survives a reopen: the postings have to be
   * committed through the transaction like live inserts are, so they are durable and replicated, not parked in a
   * memtable nobody replays.
   */
  @Test
  void buildAcrossBatchesAndBucketsSurvivesReopen() {
    database.command("sql", "CREATE DOCUMENT TYPE SpB BUCKETS 3");
    database.command("sql", "CREATE PROPERTY SpB.tokens ARRAY_OF_INTEGERS");
    database.command("sql", "CREATE PROPERTY SpB.weights ARRAY_OF_FLOATS");
    database.command("sql", "CREATE PROPERTY SpB.name STRING");

    final int docs = 2_000;
    database.transaction(() -> {
      for (int i = 0; i < docs; i++)
        // Every document carries dim 0 at a weight growing with i, plus its own private dim, so the top hit of a
        // query on the private dim is exactly that document.
        database.newDocument("SpB").set("name", "d" + i, "tokens", new int[] { 0, 1 + i },
            "weights", new float[] { 0.001f * (i + 1), 1.0f }).save();
    });

    final TypeIndex typeIndex = database.getSchema().buildTypeIndex("SpB", new String[] { "tokens", "weights" })
        .withType(Schema.INDEX_TYPE.LSM_SPARSE_VECTOR).withBatchSize(97).create();
    assertThat(typeIndex.getIndexesOnBuckets()).hasSize(3);

    assertThat(neighbors("SpB", new int[] { 0 }, new float[] { 1.0f }, 3)).containsExactly("d1999", "d1998", "d1997");
    assertThat(neighbors("SpB", new int[] { 1 + 1234 }, new float[] { 1.0f }, 1)).containsExactly("d1234");

    reopenDatabase();

    assertThat(neighbors("SpB", new int[] { 0 }, new float[] { 1.0f }, 3)).containsExactly("d1999", "d1998", "d1997");
    assertThat(neighbors("SpB", new int[] { 1 + 1234 }, new float[] { 1.0f }, 1)).containsExactly("d1234");
    long postings = 0;
    for (final Index idx : ((TypeIndex) database.getSchema().getIndexByName("SpB[tokens,weights]")).getIndexesOnBuckets())
      postings += idx.countEntries();
    assertThat(postings).isEqualTo(2L * docs);
  }

  private void createType(final String type) {
    database.command("sql", "CREATE DOCUMENT TYPE " + type);
    database.command("sql", "CREATE PROPERTY " + type + ".tokens ARRAY_OF_INTEGERS");
    database.command("sql", "CREATE PROPERTY " + type + ".weights ARRAY_OF_FLOATS");
    database.command("sql", "CREATE PROPERTY " + type + ".name STRING");
  }

  private LSMSparseVectorIndex sparseIndex(final String type) {
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName(type + "[tokens,weights]");
    return (LSMSparseVectorIndex) typeIndex.getIndexesOnBuckets()[0];
  }

  private List<String> neighbors(final String type, final int[] tokens, final float[] weights, final int k) {
    final List<String> names = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT expand(`vector.sparseNeighbors`(?, ?, ?, ?))",
        type + "[tokens,weights]", tokens, weights, k)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        names.add(r.getProperty("name"));
      }
    }
    return names;
  }
}
