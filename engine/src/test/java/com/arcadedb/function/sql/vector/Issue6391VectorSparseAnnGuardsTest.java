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
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #6391: three {@code vector.*} sparse/ANN functions threw raw
 * exceptions (or imposed an unnecessary restriction) on valid input, inconsistent with their own
 * siblings.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6391VectorSparseAnnGuardsTest extends TestHelper {

  @Override
  public void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY Doc.name IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY Doc.embedding IF NOT EXISTS ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX IF NOT EXISTS ON Doc (name) UNIQUE");
      database.command("sql", """
          CREATE INDEX IF NOT EXISTS ON Doc (embedding) LSM_VECTOR
          METADATA {
            dimensions: 3,
            similarity: 'COSINE',
            idPropertyName: 'name'
          }""");
    });

    database.transaction(() -> {
      database.newVertex("Doc").set("name", "docA").set("embedding", new float[] { 1.0f, 0.0f, 0.0f }).save();
      database.newVertex("Doc").set("name", "docB").set("embedding", new float[] { 0.9f, 0.1f, 0.0f }).save();
    });
  }

  // 1. sparseDot must not require equal inferred dimensions: it is implicitly 0 over non-overlapping indices.
  @Test
  void sparseDotOnDisjointIndicesReturnsZeroInsteadOfThrowing() {
    try (final ResultSet rs = database.query("sql",
        "SELECT `vector.sparseDot`(`vector.sparseCreate`([0,2],[0.5,0.3]), `vector.sparseCreate`([1,5],[0.2,0.8])) as score")) {
      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      assertThat(result.<Float>getProperty("score")).isEqualTo(0.0f);
    }
  }

  @Test
  void sparseDotOnDifferentDimensionsWithOverlapStillComputesCorrectly() {
    try (final ResultSet rs = database.query("sql",
        "SELECT `vector.sparseDot`(`vector.sparseCreate`([0,2],[0.5,0.3]), `vector.sparseCreate`([0,1,5],[0.6,0.2,0.8])) as score")) {
      final Result result = rs.next();
      // only index 0 overlaps: 0.5 * 0.6 = 0.30
      assertThat(result.<Float>getProperty("score")).isEqualTo(0.30f, org.assertj.core.data.Offset.offset(0.0001f));
    }
  }

  // 2. sparseCreate must accept long[] indices (how integer JSON arrays arrive from HTTP), not just int[]/Object[].
  @Test
  void sparseCreateAcceptsLongArrayIndices() {
    final long[] indices = { 0L, 2L, 5L };
    final float[] values = { 0.5f, 0.3f, 0.8f };

    try (final ResultSet rs = database.query("sql", "SELECT `vector.sparseCreate`(?, ?) as sparse_emb",
        (Object) indices, (Object) values)) {
      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      assertThat((Object) result.getProperty("sparse_emb")).isNotNull();
    }
  }

  // 3. vector.neighbors(k <= 0) must return empty, matching rerank/recommend/discover/sparseNeighbors,
  //    instead of reaching a negative-capacity ArrayList allocation deep in LSMVectorIndex.
  @Test
  void neighborsWithNonPositiveKReturnsEmptyInsteadOfThrowing() {
    final SQLFunctionVectorNeighbors function = new SQLFunctionVectorNeighbors();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    @SuppressWarnings("unchecked")
    final List<Map<String, Object>> negative = (List<Map<String, Object>>) function.execute(null, null, null,
        new Object[] { "Doc[embedding]", new float[] { 1.0f, 0.0f, 0.0f }, -1 }, context);
    assertThat(negative).isNotNull().isEmpty();

    @SuppressWarnings("unchecked")
    final List<Map<String, Object>> zero = (List<Map<String, Object>>) function.execute(null, null, null,
        new Object[] { "Doc[embedding]", new float[] { 1.0f, 0.0f, 0.0f }, 0 }, context);
    assertThat(zero).isNotNull().isEmpty();
  }
}
