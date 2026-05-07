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
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tier 4 follow-up: range-search gates on both vector functions. {@code vector.neighbors}
 * accepts {@code maxDistance} (drop neighbors whose distance exceeds the threshold);
 * {@code vector.sparseNeighbors} accepts {@code minScore} (drop neighbors below the threshold).
 * Both are post-filters on top of the regular top-K (JVector's HNSW does not expose a native
 * range mode, BMW DAAT pruning already truncates by relevance) but they make the radius-search
 * pattern ergonomic without the {@code SELECT ... WHERE distance < D} workaround.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class VectorRangeSearchTest extends TestHelper {

  /**
   * Dense path. 4 docs along {@code +X}, increasingly farther from the query at {@code (1,0,0)}.
   * A {@code maxDistance=0.05} drops the two distant ones, leaving the two close clones.
   */
  @Test
  void denseNeighborsHonorsMaxDistance() {
    final Map<String, RID> rids = new HashMap<>();
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY Doc.name IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY Doc.embedding IF NOT EXISTS ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX IF NOT EXISTS ON Doc (name) UNIQUE");
      database.command("sql", """
          CREATE INDEX IF NOT EXISTS ON Doc (embedding) LSM_VECTOR
          METADATA { dimensions: 3, similarity: 'COSINE', idPropertyName: 'name' }""");
    });
    database.transaction(() -> {
      rids.put("near1", database.newVertex("Doc").set("name", "near1").set("embedding", new float[] { 1.0f, 0.0f, 0.0f }).save().getIdentity());
      rids.put("near2", database.newVertex("Doc").set("name", "near2").set("embedding", new float[] { 0.99f, 0.01f, 0.0f }).save().getIdentity());
      rids.put("mid",   database.newVertex("Doc").set("name", "mid").set("embedding", new float[] { 0.5f, 0.5f, 0.0f }).save().getIdentity());
      rids.put("far",   database.newVertex("Doc").set("name", "far").set("embedding", new float[] { 0.0f, 0.0f, 1.0f }).save().getIdentity());
    });

    // Without the filter: top-K returns all 4.
    final List<String> unfiltered = denseQuery(new float[] { 1.0f, 0.0f, 0.0f }, 4, null);
    assertThat(unfiltered).hasSize(4);

    // With maxDistance=0.05: only near1 and near2 (distance ~0 / 0.0001) qualify; mid (~0.29) and
    // far (~1.0) are dropped. Result MAY be smaller than k - that's the contract of range search.
    final List<String> filtered = denseQuery(new float[] { 1.0f, 0.0f, 0.0f }, 4, 0.05);
    assertThat(filtered).containsExactlyInAnyOrder("near1", "near2");
  }

  /**
   * Sparse path. Same fixture pattern but using BM25-style sparse weights. {@code minScore=0.5}
   * keeps only the candidates whose dot-product with the query exceeds the threshold.
   */
  @Test
  void sparseNeighborsHonorsMinScore() {
    final String type = "SparseRangeDoc";
    final String idx = type + "[tokens,weights]";
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE " + type);
      database.command("sql", "CREATE PROPERTY " + type + ".tokens ARRAY_OF_INTEGERS");
      database.command("sql", "CREATE PROPERTY " + type + ".weights ARRAY_OF_FLOATS");
      database.getSchema()
          .buildTypeIndex(type, new String[] { "tokens", "weights" })
          .withSparseVectorType()
          .withDimensions(50)
          .create();
    });
    database.transaction(() -> {
      // Each doc has a single (dim=1, weight) posting; query is also (dim=1, weight=1.0). The
      // dot-product score is exactly the doc's weight: 1.0, 0.7, 0.4, 0.1.
      database.newDocument(type).set("tokens", new int[] { 1 }).set("weights", new float[] { 1.0f }).save();
      database.newDocument(type).set("tokens", new int[] { 1 }).set("weights", new float[] { 0.7f }).save();
      database.newDocument(type).set("tokens", new int[] { 1 }).set("weights", new float[] { 0.4f }).save();
      database.newDocument(type).set("tokens", new int[] { 1 }).set("weights", new float[] { 0.1f }).save();
    });

    final ResultSet unfiltered = database.query("sql",
        "SELECT score FROM (SELECT expand(`vector.sparseNeighbors`(?, ?, ?, ?))) ORDER BY score DESC",
        idx, new int[] { 1 }, new float[] { 1.0f }, 4);
    final List<Float> scoresUnfiltered = readScores(unfiltered);
    assertThat(scoresUnfiltered).hasSize(4);

    final ResultSet filtered = database.query("sql",
        "SELECT score FROM (SELECT expand(`vector.sparseNeighbors`(?, ?, ?, ?, { minScore: 0.5 }))) ORDER BY score DESC",
        idx, new int[] { 1 }, new float[] { 1.0f }, 4);
    final List<Float> scoresFiltered = readScores(filtered);
    // Only docs with score >= 0.5 are kept (1.0 and 0.7).
    assertThat(scoresFiltered).hasSize(2);
    assertThat(scoresFiltered.get(0)).isCloseTo(1.0f, org.assertj.core.data.Offset.offset(0.01f));
    assertThat(scoresFiltered.get(1)).isCloseTo(0.7f, org.assertj.core.data.Offset.offset(0.01f));
  }

  // --- helpers ---

  private List<String> denseQuery(final float[] q, final int k, final Double maxDistance) {
    final ResultSet rs;
    if (maxDistance == null) {
      rs = database.query("sql",
          "SELECT name FROM (SELECT expand(`vector.neighbors`(?, ?, ?))) ORDER BY name",
          "Doc[embedding]", q, k);
    } else {
      rs = database.query("sql",
          "SELECT name FROM (SELECT expand(`vector.neighbors`(?, ?, ?, { maxDistance: ? }))) ORDER BY name",
          "Doc[embedding]", q, k, maxDistance.floatValue());
    }
    final List<String> names = new ArrayList<>();
    while (rs.hasNext())
      names.add(rs.next().getProperty("name"));
    rs.close();
    return names;
  }

  private static List<Float> readScores(final ResultSet rs) {
    final List<Float> out = new ArrayList<>();
    while (rs.hasNext()) {
      final Result r = rs.next();
      final Object s = r.getProperty("score");
      if (s instanceof Number n)
        out.add(n.floatValue());
    }
    rs.close();
    return out;
  }
}
