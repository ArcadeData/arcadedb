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
package com.arcadedb.function.sql.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the {@code groupBy} / {@code groupSize} options on {@code vector.neighbors} and
 * {@code vector.sparseNeighbors} (issue #4067).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SQLFunctionVectorGroupByTest extends TestHelper {

  private static final String TYPE_NAME  = "GroupedDoc";
  private static final String DENSE_IDX  = "GroupedDoc[embedding]";
  private static final String SPARSE_IDX = "GroupedDoc[tokens,weights]";

  private static final int    GROUPS   = 10;
  private static final int    PER_GROUP = 10;

  private void buildSchema() {
    database.transaction(() -> {
      final DocumentType t = database.getSchema().createDocumentType(TYPE_NAME);
      t.createProperty("source_file", Type.STRING);
      t.createProperty("embedding", Type.ARRAY_OF_FLOATS);
      t.createProperty("tokens", Type.ARRAY_OF_INTEGERS);
      t.createProperty("weights", Type.ARRAY_OF_FLOATS);

      database.getSchema()
          .buildTypeIndex(TYPE_NAME, new String[] { "embedding" })
          .withLSMVectorType()
          .withDimensions(8)
          .withSimilarity("COSINE")
          .create();

      database.getSchema()
          .buildTypeIndex(TYPE_NAME, new String[] { "tokens", "weights" })
          .withSparseVectorType()
          .withDimensions(50)
          .create();
    });
  }

  /**
   * Inserts {@code GROUPS x PER_GROUP} = 100 docs across 10 source files. Each doc has a random
   * 8-dim dense embedding and a 3-nnz sparse vector. The {@code source_file} is the group key.
   */
  private void seed100Across10Files() {
    database.transaction(() -> {
      final Random rnd = new Random(0xCAFEL);
      for (int g = 0; g < GROUPS; g++) {
        for (int j = 0; j < PER_GROUP; j++) {
          final float[] dense = new float[8];
          for (int d = 0; d < 8; d++) dense[d] = rnd.nextFloat();

          final int[]   indices = new int[3];
          final float[] values  = new float[3];
          final HashSet<Integer> picked = new HashSet<>(3);
          for (int z = 0; z < 3; z++) {
            int dim;
            do {
              dim = rnd.nextInt(50);
            } while (!picked.add(dim));
            indices[z] = dim;
            values[z] = 0.1f + rnd.nextFloat();
          }

          final MutableDocument d = database.newDocument(TYPE_NAME);
          d.set("source_file", "file_" + g);
          d.set("embedding", dense);
          d.set("tokens", indices);
          d.set("weights", values);
          d.save();
        }
      }
    });
  }

  @Test
  void denseGroupByReturnsDistinctGroupsAtGroupSize1() {
    buildSchema();
    seed100Across10Files();

    final float[] queryVec = new float[8];
    queryVec[0] = 1.0f;

    final ResultSet rs = database.query("sql",
        "SELECT expand(`vector.neighbors`(?, ?, ?, { groupBy: 'source_file', groupSize: 1 }))",
        DENSE_IDX, queryVec, 10);

    final Set<String> groups = new HashSet<>();
    int rowCount = 0;
    while (rs.hasNext()) {
      final Result row = rs.next();
      assertThat(groups.add((String) row.getProperty("source_file")))
          .as("each source_file appears at most once").isTrue();
      rowCount++;
    }
    assertThat(rowCount).isEqualTo(10);
    assertThat(groups).hasSize(10);
  }

  @Test
  void denseGroupSize2CapsRowsPerGroup() {
    buildSchema();
    seed100Across10Files();

    final float[] queryVec = new float[8];
    queryVec[0] = 1.0f;

    final ResultSet rs = database.query("sql",
        "SELECT expand(`vector.neighbors`(?, ?, ?, { groupBy: 'source_file', groupSize: 2 }))",
        DENSE_IDX, queryVec, 10);

    final HashMap<String, Integer> seen = new HashMap<>();
    while (rs.hasNext()) {
      final Result row = rs.next();
      seen.merge((String) row.getProperty("source_file"), 1, Integer::sum);
    }

    assertThat(seen.size()).as("group count <= limit").isLessThanOrEqualTo(10);
    for (final Map.Entry<String, Integer> e : seen.entrySet())
      assertThat(e.getValue()).as("group %s row count", e.getKey()).isLessThanOrEqualTo(2);
  }

  @Test
  void sparseGroupByReturnsDistinctGroupsAtGroupSize1() {
    buildSchema();
    seed100Across10Files();

    final ResultSet rs = database.query("sql",
        "SELECT expand(`vector.sparseNeighbors`(?, ?, ?, ?, { groupBy: 'source_file', groupSize: 1 }))",
        SPARSE_IDX, new int[] { 1, 2, 3 }, new float[] { 1.0f, 1.0f, 1.0f }, 5);

    final Set<String> groups = new HashSet<>();
    int rowCount = 0;
    while (rs.hasNext()) {
      final Result row = rs.next();
      assertThat(groups.add((String) row.getProperty("source_file")))
          .as("each source_file appears at most once").isTrue();
      rowCount++;
    }
    assertThat(rowCount).isLessThanOrEqualTo(5);
    assertThat(groups.size()).isEqualTo(rowCount);
  }

  @Test
  void groupByWithoutOptionPreservesLegacyBehavior() {
    buildSchema();
    seed100Across10Files();

    final float[] queryVec = new float[8];
    queryVec[0] = 1.0f;

    // Without groupBy: no constraint on source_file diversity; expect exactly k results.
    final ResultSet rs = database.query("sql",
        "SELECT expand(`vector.neighbors`(?, ?, ?))",
        DENSE_IDX, queryVec, 10);

    int count = 0;
    while (rs.hasNext()) {
      rs.next();
      count++;
    }
    assertThat(count).isEqualTo(10);
  }

  @Test
  void overFetchCapRejectsPathologicalParams() {
    buildSchema();
    seed100Across10Files();

    final float[] queryVec = new float[8];
    queryVec[0] = 1.0f;

    // limit=10000, groupSize=10000 would request 500M candidates without the cap. Expect a
    // CommandSQLParsingException with a helpful message (caller should reduce limit or groupSize).
    org.assertj.core.api.Assertions.assertThatThrownBy(() -> {
      try (ResultSet rs = database.query("sql",
          "SELECT expand(`vector.neighbors`(?, ?, ?, { groupBy: 'source_file', groupSize: 10000 }))",
          DENSE_IDX, queryVec, 10000)) {
        while (rs.hasNext()) rs.next();
      }
    }).hasMessageContaining("over-fetch budget exceeded");

    org.assertj.core.api.Assertions.assertThatThrownBy(() -> {
      try (ResultSet rs = database.query("sql",
          "SELECT expand(`vector.sparseNeighbors`(?, ?, ?, ?, { groupBy: 'source_file', groupSize: 10000 }))",
          SPARSE_IDX, new int[] { 1, 2, 3 }, new float[] { 1.0f, 1.0f, 1.0f }, 10000)) {
        while (rs.hasNext()) rs.next();
      }
    }).hasMessageContaining("over-fetch budget exceeded");
  }

  /**
   * The integrated path replaces the MVP post-filter with a per-group min-heap inside the BMW
   * DAAT loop. Two-group corpus where the doc with the lowest RID per group has the lowest score
   * against the query: a "first per group" implementation (which the post-filter would degenerate
   * to without the global score sort) returns the low-scoring docs. The integrated path's per
   * group min-heap evicts the lower score when a higher one arrives later in the BMW loop, so the
   * returned per-group winner is the highest-scoring member.
   */
  @Test
  void sparseGroupByReturnsHighestScorePerGroup() {
    buildSchema();
    database.transaction(() -> {
      for (int g = 0; g < 2; g++) {
        for (int i = 0; i < 3; i++) {
          final MutableDocument d = database.newDocument(TYPE_NAME);
          d.set("source_file", g == 0 ? "best_per_group_A" : "best_per_group_B");
          d.set("embedding", new float[8]);
          d.set("tokens", new int[] { 1 });
          d.set("weights", new float[] { 0.1f * (i + 1) });
          d.save();
        }
      }
    });

    final ResultSet rs = database.query("sql",
        "SELECT expand(`vector.sparseNeighbors`(?, ?, ?, ?, { groupBy: 'source_file', groupSize: 1 }))",
        SPARSE_IDX, new int[] { 1 }, new float[] { 1.0f }, 2);

    final HashMap<String, Float> bestPerGroup = new HashMap<>();
    while (rs.hasNext()) {
      final Result row = rs.next();
      bestPerGroup.put(row.getProperty("source_file"), ((Number) row.getProperty("score")).floatValue());
    }

    assertThat(bestPerGroup).hasSize(2);
    assertThat(bestPerGroup.get("best_per_group_A")).isNotNull();
    assertThat(bestPerGroup.get("best_per_group_B")).isNotNull();
    // Top per group must be the highest weight (0.3f * 1.0f = 0.3f), not the first encountered.
    assertThat(bestPerGroup.get("best_per_group_A")).isCloseTo(0.3f, org.assertj.core.data.Offset.offset(1e-3f));
    assertThat(bestPerGroup.get("best_per_group_B")).isCloseTo(0.3f, org.assertj.core.data.Offset.offset(1e-3f));
  }

  @Test
  void sparseGroupByGroupSize2KeepsTopTwoByScore() {
    buildSchema();
    database.transaction(() -> {
      // 5 docs in one group with weights 0.1, 0.2, 0.3, 0.4, 0.5. groupSize=2, k=1 should return
      // docs with weights 0.5 and 0.4 (the top two), not the first two encountered.
      for (int i = 0; i < 5; i++) {
        final MutableDocument d = database.newDocument(TYPE_NAME);
        d.set("source_file", "single_group");
        d.set("embedding", new float[8]);
        d.set("tokens", new int[] { 1 });
        d.set("weights", new float[] { 0.1f * (i + 1) });
        d.save();
      }
    });

    final ResultSet rs = database.query("sql",
        "SELECT expand(`vector.sparseNeighbors`(?, ?, ?, ?, { groupBy: 'source_file', groupSize: 2 }))",
        SPARSE_IDX, new int[] { 1 }, new float[] { 1.0f }, 1);

    final java.util.ArrayList<Float> scores = new java.util.ArrayList<>();
    while (rs.hasNext()) {
      final Result row = rs.next();
      scores.add(((Number) row.getProperty("score")).floatValue());
    }
    assertThat(scores).hasSize(2);
    scores.sort((a, b) -> Float.compare(b, a));
    assertThat(scores.get(0)).isCloseTo(0.5f, org.assertj.core.data.Offset.offset(1e-3f));
    assertThat(scores.get(1)).isCloseTo(0.4f, org.assertj.core.data.Offset.offset(1e-3f));
  }

  @Test
  void groupByComposesWithFilter() {
    buildSchema();
    seed100Across10Files();

    // Build a filter list that whitelists only the first half of file_0..file_4 (half the corpus).
    final List<RID> whitelist = new java.util.ArrayList<>();
    try (ResultSet rs = database.query("sql",
        "SELECT @rid AS rid FROM " + TYPE_NAME + " WHERE source_file IN ['file_0', 'file_1', 'file_2', 'file_3', 'file_4']")) {
      while (rs.hasNext())
        whitelist.add((RID) rs.next().getProperty("rid"));
    }
    assertThat(whitelist).hasSize(50);

    final float[] queryVec = new float[8];
    queryVec[0] = 1.0f;

    final ResultSet rs = database.query("sql",
        "SELECT expand(`vector.neighbors`(?, ?, ?, { filter: ?, groupBy: 'source_file', groupSize: 1 }))",
        DENSE_IDX, queryVec, 10, whitelist);

    final Set<String> groups = new HashSet<>();
    while (rs.hasNext()) {
      final Result row = rs.next();
      final String sourceFile = row.getProperty("source_file");
      assertThat(sourceFile).as("filter excludes file_5..file_9")
          .isIn("file_0", "file_1", "file_2", "file_3", "file_4");
      groups.add(sourceFile);
    }
    // 5 files survive the filter, groupSize=1, so we expect at most 5 groups.
    assertThat(groups.size()).isLessThanOrEqualTo(5);
  }
}
