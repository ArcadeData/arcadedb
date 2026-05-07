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
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins {@code vector.rerank} (Tier 4 follow-up). The function takes a stage-1 candidate set and
 * re-scores each row against a fresh query vector by reading a (potentially higher-precision)
 * embedding property off each record. The rerank output ranking should match what a direct
 * cosine search against the rerank vector would produce - regardless of how stage 1 ordered
 * the candidates.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SQLFunctionVectorRerankTest extends TestHelper {

  private static final String TYPE = "RerankDoc";

  /**
   * Stage 1 returns candidates in an arbitrary order (we synthesize a candidate list directly).
   * After rerank against a query at {@code (1, 0, 0)}, the order must reflect cosine similarity
   * to that query - regardless of stage-1's order.
   */
  @Test
  void rerankReplacesStage1OrderingWithFreshSimilarity() {
    final List<RID> rids = new ArrayList<>();
    database.transaction(() -> {
      database.getSchema().getOrCreateDocumentType(TYPE).createProperty("name", Type.STRING);
      database.getSchema().getType(TYPE).createProperty("embedding", Type.ARRAY_OF_FLOATS);
      rids.add(insert("near", new float[] { 1.0f, 0.0f, 0.0f }));
      rids.add(insert("far",  new float[] { 0.0f, 0.0f, 1.0f }));
      rids.add(insert("mid",  new float[] { 0.7f, 0.7f, 0.0f }));
    });

    // Stage-1 candidates in INTENTIONALLY-WRONG order (far first, near last). The rerank stage
    // must still put 'near' at rank 0 because it's the actually-closest to (1,0,0).
    final List<Map<String, Object>> stage1 = new ArrayList<>();
    for (final RID r : List.of(rids.get(1), rids.get(2), rids.get(0))) {
      final LinkedHashMap<String, Object> row = new LinkedHashMap<>();
      row.put("@rid", r);
      row.put("score", 0.5f);  // bogus uniform score from stage 1
      stage1.add(row);
    }

    final ResultSet rs = database.query("sql",
        "SELECT name, score FROM (SELECT expand(`vector.rerank`(?, ?, 'embedding', 3)))",
        stage1, new float[] { 1.0f, 0.0f, 0.0f });
    final List<String> names = new ArrayList<>();
    while (rs.hasNext()) names.add(rs.next().getProperty("name"));
    rs.close();
    assertThat(names).containsExactly("near", "mid", "far");
  }

  @Test
  void kCapsResultSize() {
    final List<RID> rids = new ArrayList<>();
    database.transaction(() -> {
      database.getSchema().getOrCreateDocumentType(TYPE).createProperty("name", Type.STRING);
      database.getSchema().getType(TYPE).createProperty("embedding", Type.ARRAY_OF_FLOATS);
      rids.add(insert("a", new float[] { 1.0f, 0.0f, 0.0f }));
      rids.add(insert("b", new float[] { 0.9f, 0.1f, 0.0f }));
      rids.add(insert("c", new float[] { 0.0f, 0.0f, 1.0f }));
    });

    final List<Map<String, Object>> stage1 = new ArrayList<>();
    for (final RID r : rids) {
      final LinkedHashMap<String, Object> row = new LinkedHashMap<>();
      row.put("@rid", r);
      stage1.add(row);
    }

    final ResultSet rs = database.query("sql",
        "SELECT name FROM (SELECT expand(`vector.rerank`(?, ?, 'embedding', 1)))",
        stage1, new float[] { 1.0f, 0.0f, 0.0f });
    final List<String> names = new ArrayList<>();
    while (rs.hasNext()) names.add(rs.next().getProperty("name"));
    rs.close();
    assertThat(names).containsExactly("a");
  }

  @Test
  void deletedCandidateIsSilentlyDropped() {
    final List<RID> rids = new ArrayList<>();
    database.transaction(() -> {
      database.getSchema().getOrCreateDocumentType(TYPE).createProperty("name", Type.STRING);
      database.getSchema().getType(TYPE).createProperty("embedding", Type.ARRAY_OF_FLOATS);
      rids.add(insert("alive", new float[] { 1.0f, 0.0f, 0.0f }));
    });

    // Inject a phantom RID alongside the live one. The function must skip the phantom rather
    // than failing the query.
    final RID phantom = new RID(rids.get(0).getBucketId(), 999_999L);
    final List<Map<String, Object>> stage1 = new ArrayList<>();
    for (final RID r : List.of(rids.get(0), phantom)) {
      final LinkedHashMap<String, Object> row = new LinkedHashMap<>();
      row.put("@rid", r);
      stage1.add(row);
    }

    final ResultSet rs = database.query("sql",
        "SELECT name FROM (SELECT expand(`vector.rerank`(?, ?, 'embedding', 5)))",
        stage1, new float[] { 1.0f, 0.0f, 0.0f });
    final List<String> names = new ArrayList<>();
    while (rs.hasNext()) names.add(rs.next().getProperty("name"));
    rs.close();
    assertThat(names).containsExactly("alive");
  }

  @Test
  void mismatchedDimensionThrows() {
    final List<RID> rids = new ArrayList<>();
    database.transaction(() -> {
      database.getSchema().getOrCreateDocumentType(TYPE).createProperty("name", Type.STRING);
      database.getSchema().getType(TYPE).createProperty("embedding", Type.ARRAY_OF_FLOATS);
      rids.add(insert("only", new float[] { 1.0f, 0.0f, 0.0f }));
    });

    final List<Map<String, Object>> stage1 = new ArrayList<>();
    final LinkedHashMap<String, Object> row = new LinkedHashMap<>();
    row.put("@rid", rids.get(0));
    stage1.add(row);

    // Query is 4-dim but candidate embeddings are 3-dim. Must throw rather than silently scoring 0.
    assertThatThrownBy(() -> database.query("sql",
        "SELECT expand(`vector.rerank`(?, ?, 'embedding', 5))",
        stage1, new float[] { 1.0f, 0.0f, 0.0f, 0.0f }))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("dimension");
  }

  // --- helpers ---

  private RID insert(final String name, final float[] embedding) {
    final MutableDocument d = database.newDocument(TYPE);
    d.set("name", name);
    d.set("embedding", embedding);
    d.save();
    return d.getIdentity();
  }
}
