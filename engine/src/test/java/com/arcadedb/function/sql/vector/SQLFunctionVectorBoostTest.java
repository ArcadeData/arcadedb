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
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins {@code vector.boost} (Tier 4 follow-up). The function blends a vector relevance score
 * with one or more scalar business signals via a linear formula
 * {@code base_similarity + Σ(weight_i * field_i)}, then re-sorts. The base similarity is read
 * from {@code score} / {@code $score} or auto-flipped from {@code distance}, so all upstream
 * shapes compose with a single "higher is better" output.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SQLFunctionVectorBoostTest extends TestHelper {

  @Test
  void boostFlipsRankingWhenWeightOverwhelmsBaseScore() {
    // 3 candidates with similar base scores; only their popularity differs sharply.
    final List<Map<String, Object>> candidates = List.of(
        row("A", 0.90f, 1.0f),    // high score, low popularity
        row("B", 0.85f, 100.0f),  // mid score, very high popularity
        row("C", 0.80f, 5.0f));   // low score, mid popularity

    // Weight 0.0 = relevance-only ranking. Order by base score: A, B, C.
    final List<String> baseline = boost(candidates, "popularity", 0.0f);
    assertThat(baseline).containsExactly("A", "B", "C");

    // Weight 0.01 = popularity dominates. B's popularity (100) gives it +1.0 = 1.85 total,
    // beating A (0.90 + 0.01) and C (0.80 + 0.05). Order: B, A, C.
    final List<String> boostedHeavy = boost(candidates, "popularity", 0.01f);
    assertThat(boostedHeavy).containsExactly("B", "A", "C");
  }

  @Test
  void emitsHigherIsBetterScoreWhenSourceUsesDistance() {
    // Upstream rows use {@code distance} (lower is better), as {@code vector.neighbors} emits.
    // The boost step must auto-flip the sign so the output score is consistently higher-is-better.
    final List<Map<String, Object>> distanceRows = List.of(
        rowWithDistance("near", 0.1f, 0.0f),  // very close
        rowWithDistance("far",  0.9f, 0.0f)); // far

    // No boost weight: the order should still be [near, far] - i.e. lower distance ranked higher.
    final ResultSet rs = database.query("sql",
        "SELECT @rid AS r, name, score FROM (SELECT expand(`vector.boost`(?, "
            + "{ boosts: [{ field: 'popularity', weight: 0 }] })))",
        distanceRows);
    final List<String> names = new ArrayList<>();
    final List<Float> scores = new ArrayList<>();
    while (rs.hasNext()) {
      final Result row = rs.next();
      names.add(row.getProperty("name"));
      scores.add(((Number) row.getProperty("score")).floatValue());
    }
    rs.close();

    assertThat(names).containsExactly("near", "far");
    // The boosted score is the negated distance: -0.1 vs -0.9. Higher is closer.
    assertThat(scores.get(0)).isCloseTo(-0.1f, org.assertj.core.data.Offset.offset(0.01f));
    assertThat(scores.get(1)).isCloseTo(-0.9f, org.assertj.core.data.Offset.offset(0.01f));
  }

  @Test
  void multipleBoostsAreSummed() {
    final List<Map<String, Object>> candidates = List.of(
        rowMulti("A", 1.0f, 10.0f, 1.0f),
        rowMulti("B", 1.0f, 0.0f, 100.0f));

    // weights: popularity 1.0, recency 0.1.
    // A: 1.0 + 1.0*10 + 0.1*1 = 11.1
    // B: 1.0 + 1.0*0 + 0.1*100 = 11.0
    // A wins narrowly.
    final ResultSet rs = database.query("sql",
        "SELECT name FROM (SELECT expand(`vector.boost`(?, "
            + "{ boosts: [{ field: 'popularity', weight: 1.0 }, { field: 'recency', weight: 0.1 }] })))",
        candidates);
    final List<String> names = new ArrayList<>();
    while (rs.hasNext()) names.add(rs.next().getProperty("name"));
    rs.close();
    assertThat(names).containsExactly("A", "B");
  }

  @Test
  void limitOptionCapsResultSize() {
    final List<Map<String, Object>> candidates = List.of(
        row("A", 0.9f, 1.0f), row("B", 0.85f, 1.0f), row("C", 0.8f, 1.0f));
    final ResultSet rs = database.query("sql",
        "SELECT name FROM (SELECT expand(`vector.boost`(?, "
            + "{ boosts: [{ field: 'popularity', weight: 0 }], limit: 2 })))",
        candidates);
    final List<String> names = new ArrayList<>();
    while (rs.hasNext()) names.add(rs.next().getProperty("name"));
    rs.close();
    assertThat(names).hasSize(2).containsExactly("A", "B");
  }

  @Test
  void rejectsEmptyBoostsList() {
    assertThatThrownBy(() -> database.query("sql",
        "SELECT expand(`vector.boost`([], { boosts: [] }))"))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("boosts");
  }

  /**
   * Contract pin for the materialize-time score gate: rows the function does not recognise as a
   * scored shape (anything other than Map / Result with a numeric {@code score}/{@code distance}
   * field) are silently dropped. This is intentional - upstream pipelines hand out
   * {@code @rid + score} tuples, and a row that lacks both is missing the data the boost loop
   * needs. Without coverage, a future refactor that admits NaN-base rows could let unscored
   * shapes propagate through and surface as corrupt output.
   */
  @Test
  void unrecognisedRowShapeIsSilentlyDropped() {
    final SQLFunctionVectorBoost function = new SQLFunctionVectorBoost();
    final com.arcadedb.query.sql.executor.BasicCommandContext ctx =
        new com.arcadedb.query.sql.executor.BasicCommandContext();
    ctx.setDatabase(database);

    final Map<String, Object> mapRow = row("A", 0.9f, 1.0f);
    final List<Object> mixed = new ArrayList<>();
    mixed.add(mapRow);
    mixed.add("opaque-string-no-score-field");

    @SuppressWarnings("unchecked")
    final List<Object> result = (List<Object>) function.execute(null, null, null,
        new Object[] { mixed, Map.of("boosts", List.of(Map.of("field", "popularity", "weight", 0.0))) },
        ctx);

    // Only the Map-shaped row survives - the bare String had no score, was rejected at the
    // materialize-time NaN gate.
    assertThat(result).hasSize(1);
    @SuppressWarnings("unchecked")
    final Map<String, Object> only = (Map<String, Object>) result.get(0);
    assertThat(only.get("name")).isEqualTo("A");
  }

  // --- helpers ---

  private static Map<String, Object> row(final String name, final float score, final float popularity) {
    final LinkedHashMap<String, Object> m = new LinkedHashMap<>();
    m.put("name", name);
    m.put("score", score);
    m.put("popularity", popularity);
    return m;
  }

  private static Map<String, Object> rowWithDistance(final String name, final float distance, final float popularity) {
    final LinkedHashMap<String, Object> m = new LinkedHashMap<>();
    m.put("name", name);
    m.put("distance", distance);
    m.put("popularity", popularity);
    return m;
  }

  private static Map<String, Object> rowMulti(final String name, final float score, final float popularity, final float recency) {
    final LinkedHashMap<String, Object> m = new LinkedHashMap<>();
    m.put("name", name);
    m.put("score", score);
    m.put("popularity", popularity);
    m.put("recency", recency);
    return m;
  }

  private List<String> boost(final List<Map<String, Object>> candidates, final String field, final float weight) {
    final ResultSet rs = database.query("sql",
        "SELECT name FROM (SELECT expand(`vector.boost`(?, "
            + "{ boosts: [{ field: ?, weight: ? }] })))",
        candidates, field, weight);
    final List<String> names = new ArrayList<>();
    while (rs.hasNext()) names.add(rs.next().getProperty("name"));
    rs.close();
    return names;
  }
}
