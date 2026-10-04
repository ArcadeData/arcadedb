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
package com.arcadedb.gremlin;

import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * https://github.com/ArcadeData/arcadedb/issues/9143
 * <p>
 * {@code g.call('arcadedb#vectorNeighbors', ...)} searched only the first bucket of a multi-bucket type and accepted only a
 * {@code float[]} vector and an {@code Integer} limit. It now answers what the SQL {@code vectorNeighbors()} answers.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9143VectorNeighborsServiceTest {
  private ArcadeGraph graph;
  private Database    db;
  private float[][]   vectors;

  @BeforeEach
  void setup() {
    graph = ArcadeGraph.open("./target/test-issue9143");
    db = (Database) graph.getDatabase();
    db.command("sql", "CREATE VERTEX TYPE Doc BUCKETS 4").close();
    db.command("sql", "CREATE PROPERTY Doc.emb LIST OF FLOAT").close();
    db.command("sql", "CREATE INDEX ON Doc (emb) LSM_VECTOR METADATA {\"dimensions\": 8, \"similarity\": \"EUCLIDEAN\"}").close();
    final Random random = new Random(42);
    vectors = new float[400][8];
    db.transaction(() -> {
      for (int i = 0; i < vectors.length; i++) {
        final List<Float> list = new ArrayList<>();
        for (int d = 0; d < 8; d++) {
          vectors[i][d] = random.nextFloat();
          list.add(vectors[i][d]);
        }
        db.newVertex("Doc").set("emb", list).save();
      }
    });
  }

  @AfterEach
  void teardown() {
    graph.drop();
  }

  private List<RID> sql(final Object vector, final int k) {
    final List<RID> out = new ArrayList<>();
    try (final ResultSet rs = db.query("sql", "select vectorNeighbors('Doc[emb]', ?, ?) as neighbors", vector, k)) {
      while (rs.hasNext())
        for (final Map<String, Object> n : rs.next().<List<Map<String, Object>>>getProperty("neighbors"))
          out.add(((Document) n.get("record")).getIdentity());
    }
    return out;
  }

  private List<RID> gremlin(final Map<String, Object> params) {
    final List<RID> out = new ArrayList<>();
    try (final ResultSet rs = db.query("gremlin", "g.call('arcadedb#vectorNeighbors', params)", "params", params)) {
      while (rs.hasNext())
        for (final Map<String, Object> n : rs.next().<List<Map<String, Object>>>getProperty("result"))
          out.add(((ArcadeVertex) n.get("record")).getIdentity());
    }
    return out;
  }

  @Test
  void everyBucketIsSearched() {
    for (int q = 0; q < 20; q++) {
      final float[] vector = vectors[q * 7];
      assertThat(gremlin(Map.of("indexName", "Doc[emb]", "vector", vector, "limit", 10))).hasSize(10)
          .containsExactlyElementsOf(sql(vector, 10));
    }
  }

  @Test
  void vectorAndLimitShapesAreAcceptedLikeSql() {
    final float[] f = vectors[3];
    final List<Float> asFloats = new ArrayList<>();
    final List<Double> asDoubles = new ArrayList<>();
    final double[] doubles = new double[f.length];
    for (int i = 0; i < f.length; i++) {
      asFloats.add(f[i]);
      asDoubles.add((double) f[i]);
      doubles[i] = f[i];
    }
    final Set<RID> expected = new HashSet<>(sql(f, 5));
    assertThat(expected).hasSize(5);
    assertThat(gremlin(Map.of("indexName", "Doc[emb]", "vector", asFloats, "limit", 5))).hasSize(5);
    assertThat(new HashSet<>(gremlin(Map.of("indexName", "Doc[emb]", "vector", asDoubles, "limit", 5)))).isEqualTo(expected);
    assertThat(new HashSet<>(gremlin(Map.of("indexName", "Doc[emb]", "vector", doubles, "limit", 5)))).isEqualTo(expected);
    assertThat(new HashSet<>(gremlin(Map.of("indexName", "Doc[emb]", "vector", f, "limit", 5L)))).isEqualTo(expected);
  }

  @Test
  void aLimitOutsideTheIntRangeIsRefused() {
    assertThatThrownBy(() -> gremlin(Map.of("indexName", "Doc[emb]", "vector", vectors[0], "limit", 4_294_967_297L)))
        .hasStackTraceContaining("must be between 0 and");
    assertThatThrownBy(() -> gremlin(Map.of("indexName", "Doc[emb]", "vector", vectors[0], "limit", -1)))
        .hasStackTraceContaining("must be between 0 and");
  }

  @Test
  void theBoundariesOfTheLimitAreAccepted() {
    assertThat(gremlin(Map.of("indexName", "Doc[emb]", "vector", vectors[0], "limit", 0))).isEmpty();
    assertThat(gremlin(Map.of("indexName", "Doc[emb]", "vector", vectors[0], "limit", Integer.MAX_VALUE))).hasSize(vectors.length);
  }

  @Test
  void missingLimitIsAClearError() {
    assertThatThrownBy(() -> gremlin(Map.of("indexName", "Doc[emb]", "vector", vectors[0])))
        .hasStackTraceContaining("'limit' is required");
  }
}
