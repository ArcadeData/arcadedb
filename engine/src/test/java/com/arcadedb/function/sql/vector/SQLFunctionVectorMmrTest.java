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
 * {@code vector.mmr} Maximal Marginal Relevance reranker (Tier 4 follow-up). Pins:
 * <ol>
 *   <li>{@code lambda=1.0} collapses to top-K by score (no diversity term).</li>
 *   <li>{@code lambda=0.5} forces diversity: a near-duplicate candidate is rejected in favor
 *       of a less-similar one, even when the duplicate has a slightly higher relevance score.</li>
 *   <li>The {@code k} option caps result size.</li>
 *   <li>Composes with the standard {@code @rid} / {@code score} row shape that
 *       {@code vector.neighbors} and {@code vector.fuse} produce, so it slots into the same
 *       sub-pipeline patterns.</li>
 * </ol>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SQLFunctionVectorMmrTest extends TestHelper {

  private static final String TYPE = "MmrDoc";

  /**
   * Three docs: A (cluster 1, top score), B (cluster 1, near-duplicate of A, slightly lower
   * score), C (cluster 2, far from both, lower score). lambda=1 must return [A, B, C] in score
   * order; lambda=0.5 should still pick A first (highest score) but then prefer C over B because
   * C is far from A while B is near A.
   */
  @Test
  void diversityRanksOverNearDuplicatesWhenLambdaBelowOne() {
    final List<RID> rids = setupClusterFixture();

    // Synthesised candidate set: A=0.9, B=0.85, C=0.7 (descending score).
    final List<Map<String, Object>> candidates = candidates(rids, new float[] { 0.9f, 0.85f, 0.7f });

    // lambda=1: pure relevance. Order is exactly score-descending: A, B, C.
    final List<RID> pure = runMmr(candidates, "embedding", 1.0f, 3);
    assertThat(pure).containsExactly(rids.get(0), rids.get(1), rids.get(2));

    // lambda=0.5: diversity wins on rank 2. A is picked first (highest score). Then B and C
    // compete: B has score 0.85 but is highly similar to A; C has score 0.7 but is dissimilar.
    // The MMR formula is lambda * score - (1 - lambda) * max_cos_to_selected:
    //   B: 0.5 * 0.85 - 0.5 * cos(B, A) ≈ 0.425 - 0.5 * 0.95 = -0.05
    //   C: 0.5 * 0.7  - 0.5 * cos(C, A) ≈ 0.35  - 0.5 * 0.0  =  0.35
    // C wins. Final: [A, C, B].
    final List<RID> diversified = runMmr(candidates, "embedding", 0.5f, 3);
    assertThat(diversified).containsExactly(rids.get(0), rids.get(2), rids.get(1));
  }

  @Test
  void kOptionCapsResultSize() {
    final List<RID> rids = setupClusterFixture();
    final List<Map<String, Object>> candidates = candidates(rids, new float[] { 0.9f, 0.85f, 0.7f });

    final List<RID> top1 = runMmr(candidates, "embedding", 0.5f, 1);
    assertThat(top1).hasSize(1).containsExactly(rids.get(0));
  }

  @Test
  void emptyCandidateSetReturnsEmptyList() {
    setupClusterFixture();
    final ResultSet rs = database.query("sql",
        "SELECT expand(`vector.mmr`([], 'embedding', { lambda: 0.5, k: 3 }))");
    assertThat(rs.hasNext()).isFalse();
    rs.close();
  }

  @Test
  void rejectsLambdaOutOfRange() {
    setupClusterFixture();
    assertThatThrownBy(() -> database.query("sql",
        "SELECT expand(`vector.mmr`([], 'embedding', { lambda: 1.5, k: 3 }))"))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("lambda");
  }

  /**
   * Composition test: pipe {@code vector.neighbors} (which emits a {@code distance} field, not
   * {@code score}) directly into {@code vector.mmr}. The shared
   * {@code SQLFunctionVectorAbstract.extractScoreFromRow} auto-flips distance to similarity, so
   * the composition must work without manual sign-flipping at the SQL boundary. Without the
   * auto-flip, every row's score would be NaN at materialize time and silently dropped. This
   * test pins the composition contract that prevents that silent-data-loss regression.
   */
  @Test
  void composesWithVectorNeighborsDistanceField() {
    // Set up a vector index and the same A/B/C cluster fixture used by setupClusterFixture.
    final List<RID> rids = setupClusterFixture();
    database.transaction(() -> {
      database.command("sql", """
          CREATE INDEX IF NOT EXISTS ON %s (embedding) LSM_VECTOR
          METADATA { dimensions: 3, similarity: 'COSINE' }""".formatted(TYPE));
    });

    // Query at A's location. vector.neighbors returns rows with {@code distance}, not
    // {@code score}. mmr must accept this shape.
    final ResultSet rs = database.query("sql",
        "SELECT expand(`vector.mmr`("
            + "`vector.neighbors`('" + TYPE + "[embedding]', [1.0, 0.1, 0.0], 3), "
            + "'embedding', { lambda: 0.5, k: 3 }))");
    final List<RID> got = new ArrayList<>();
    while (rs.hasNext()) got.add((RID) rs.next().getProperty("@rid"));
    rs.close();
    // All three docs should appear (k=3 candidates, all valid). The exact order depends on the
    // diversity tradeoff; the assertion is that mmr produced any results at all - a NaN-dropped
    // run would return empty.
    assertThat(got).hasSize(3).containsExactlyInAnyOrder(rids.get(0), rids.get(1), rids.get(2));
  }

  @Test
  void candidatesWithoutEmbeddingPropertyAreSkipped() {
    final List<RID> rids = setupClusterFixture();

    // Add a 4th doc without an embedding (deliberately missing the property).
    final RID[] rid4Holder = new RID[1];
    database.transaction(() -> {
      final MutableDocument d = database.newDocument(TYPE);
      d.set("name", "D");
      // deliberately no embedding set
      d.save();
      rid4Holder[0] = d.getIdentity();
    });
    final RID rid4 = rid4Holder[0];

    final List<Map<String, Object>> candidates = new ArrayList<>(candidates(rids, new float[] { 0.9f, 0.85f, 0.7f }));
    final LinkedHashMap<String, Object> missing = new LinkedHashMap<>();
    missing.put("@rid", rid4);
    missing.put("score", 0.95f);
    candidates.add(0, missing);

    final List<RID> result = runMmr(candidates, "embedding", 1.0f, 4);
    // The 4th candidate (no embedding) is silently dropped; remaining order is by score desc.
    assertThat(result).containsExactly(rids.get(0), rids.get(1), rids.get(2));
  }

  // --- helpers ---

  private List<RID> setupClusterFixture() {
    final List<RID> ids = new ArrayList<>(3);
    database.transaction(() -> {
      database.getSchema().getOrCreateDocumentType(TYPE).createProperty("name", Type.STRING);
      database.getSchema().getType(TYPE).createProperty("embedding", Type.ARRAY_OF_FLOATS);
      // A and B are nearly identical (cluster 1). C is orthogonal (cluster 2).
      ids.add(insert("A", new float[] { 1.0f,  0.1f, 0.0f }));
      ids.add(insert("B", new float[] { 0.99f, 0.05f, 0.0f }));
      ids.add(insert("C", new float[] { 0.0f,  0.0f, 1.0f }));
    });
    return ids;
  }

  private RID insert(final String name, final float[] embedding) {
    final MutableDocument d = database.newDocument(TYPE);
    d.set("name", name);
    d.set("embedding", embedding);
    d.save();
    return d.getIdentity();
  }

  private static List<Map<String, Object>> candidates(final List<RID> rids, final float[] scores) {
    final List<Map<String, Object>> out = new ArrayList<>(rids.size());
    for (int i = 0; i < rids.size(); i++) {
      final LinkedHashMap<String, Object> row = new LinkedHashMap<>();
      row.put("@rid", rids.get(i));
      row.put("score", scores[i]);
      out.add(row);
    }
    return out;
  }

  private List<RID> runMmr(final List<Map<String, Object>> candidates, final String prop, final float lambda, final int k) {
    final ResultSet rs = database.query("sql",
        "SELECT expand(`vector.mmr`(?, ?, { lambda: ?, k: ? }))",
        candidates, prop, lambda, k);
    final List<RID> rids = new ArrayList<>();
    while (rs.hasNext()) {
      final Result row = rs.next();
      rids.add((RID) row.getProperty("@rid"));
    }
    rs.close();
    return rids;
  }
}
