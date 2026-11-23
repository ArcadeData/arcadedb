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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * Oracle test of {@code vector.fuse}: the fused scores of RRF, LINEAR and DBSF are compared with a straight double-precision
 * model of the formulas the function documents, over sources given as rows with an explicit {@code score}.
 */
class SQLFunctionVectorFuseOracleTest extends TestHelper {
  private static final int DOCS = 12;

  private final List<RID> pool = new ArrayList<>();

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("FuseOracleDoc");
    database.transaction(() -> {
      for (int i = 0; i < DOCS; i++) {
        final MutableDocument d = database.newDocument("FuseOracleDoc");
        d.set("n", i);
        d.save();
        pool.add(d.getIdentity());
      }
    });
  }

  private static List<Map<String, Object>> source(final RID[] order, final double[] scores) {
    final List<Map<String, Object>> rows = new ArrayList<>();
    for (int i = 0; i < order.length; i++) {
      final Map<String, Object> row = new LinkedHashMap<>();
      row.put("@rid", order[i]);
      row.put("score", scores == null ? null : (float) scores[i]);
      if (scores == null)
        row.remove("score");
      rows.add(row);
    }
    return rows;
  }

  private Map<RID, Double> fuse(final List<List<Map<String, Object>>> sources, final String options) {
    final StringBuilder sql = new StringBuilder("SELECT expand(`vector.fuse`(");
    final Map<String, Object> params = new HashMap<>();
    for (int i = 0; i < sources.size(); i++) {
      sql.append(":s").append(i).append(", ");
      params.put("s" + i, sources.get(i));
    }
    sql.append(options).append("))");
    final Map<RID, Double> out = new LinkedHashMap<>();
    try (final ResultSet rs = database.query("sql", sql.toString(), params)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        out.put(r.getIdentity().get(), ((Number) r.getProperty("score")).doubleValue());
      }
    }
    return out;
  }

  /** The documented RRF: score = sum over sources of weight / (k + rank), rank counted from 1 in each source. */
  private static Map<RID, Double> modelRrf(final List<RID[]> orders, final double[] weights, final long k) {
    final Map<RID, Double> out = new HashMap<>();
    for (int s = 0; s < orders.size(); s++)
      for (int rank = 1; rank <= orders.get(s).length; rank++)
        out.merge(orders.get(s)[rank - 1], weights[s] / (double) (k + rank), Double::sum);
    return out;
  }

  /** The documented LINEAR: per-source min-max normalization (a source with all equal scores gives 1), weighted sum. */
  private static Map<RID, Double> modelLinear(final List<RID[]> orders, final List<double[]> scores, final double[] weights) {
    final Map<RID, Double> out = new HashMap<>();
    for (int s = 0; s < orders.size(); s++) {
      double min = Double.POSITIVE_INFINITY;
      double max = Double.NEGATIVE_INFINITY;
      for (final double v : scores.get(s)) {
        min = Math.min(min, v);
        max = Math.max(max, v);
      }
      for (int i = 0; i < orders.get(s).length; i++) {
        final double normalized = max == min ? 1.0 : (scores.get(s)[i] - min) / (max - min);
        out.merge(orders.get(s)[i], weights[s] * normalized, Double::sum);
      }
    }
    return out;
  }

  /** The documented DBSF: clip to [mean - 3 sigma, mean + 3 sigma] (population sigma), normalize that band to [0, 1], weighted sum. */
  private static Map<RID, Double> modelDbsf(final List<RID[]> orders, final List<double[]> scores, final double[] weights) {
    final Map<RID, Double> out = new HashMap<>();
    for (int s = 0; s < orders.size(); s++) {
      final double[] v = scores.get(s);
      double sum = 0;
      for (final double x : v)
        sum += x;
      final double mean = sum / v.length;
      double sq = 0;
      for (final double x : v)
        sq += (x - mean) * (x - mean);
      final double std = Math.sqrt(sq / v.length);
      final double lo = mean - 3 * std;
      final double hi = mean + 3 * std;
      for (int i = 0; i < v.length; i++) {
        final double clipped = Math.max(lo, Math.min(hi, v[i]));
        final double normalized = hi == lo ? 1.0 : (clipped - lo) / (hi - lo);
        out.merge(orders.get(s)[i], weights[s] * normalized, Double::sum);
      }
    }
    return out;
  }

  private void assertMatches(final Map<RID, Double> actual, final Map<RID, Double> model, final String ctx) {
    assertThat(actual.keySet()).as(ctx).containsExactlyInAnyOrderElementsOf(model.keySet());
    for (final Map.Entry<RID, Double> e : model.entrySet())
      assertThat(actual.get(e.getKey())).as(ctx + " " + e.getKey()).isCloseTo(e.getValue(), within(1e-5 + 1e-5 * Math.abs(e.getValue())));
    // the answer is ranked best first: a pair the model separates clearly must come in the model's order
    final List<RID> order = new ArrayList<>(actual.keySet());
    for (int i = 0; i + 1 < order.size(); i++)
      assertThat(actual.get(order.get(i))).as(ctx + " rank " + i).isGreaterThanOrEqualTo(actual.get(order.get(i + 1)));
  }

  private RID[] randomOrder(final Random rnd, final int size) {
    final List<RID> copy = new ArrayList<>(pool);
    java.util.Collections.shuffle(copy, rnd);
    return copy.subList(0, size).toArray(new RID[0]);
  }

  private static double[] randomScores(final Random rnd, final int size) {
    // distinct, descending: the order of a source is its ranking
    final double[] s = new double[size];
    double v = 5.0 + rnd.nextDouble() * 5;
    for (int i = 0; i < size; i++) {
      s[i] = v;
      v -= 0.01 + rnd.nextDouble() * 2.0;
    }
    return s;
  }

  @Test
  void rrfMatchesTheFormula() {
    final Random rnd = new Random(7);
    for (int round = 0; round < 60; round++) {
      final int sourceCount = 2 + rnd.nextInt(3);
      final List<RID[]> orders = new ArrayList<>();
      final List<List<Map<String, Object>>> sources = new ArrayList<>();
      final double[] weights = new double[sourceCount];
      final long k = round % 5 == 0 ? 0 : 1 + rnd.nextInt(100);
      for (int s = 0; s < sourceCount; s++) {
        final RID[] order = randomOrder(rnd, rnd.nextInt(DOCS + 1));
        orders.add(order);
        sources.add(source(order, null));
        weights[s] = round % 3 == 0 ? (rnd.nextInt(3) - 1) * rnd.nextDouble() : 0.1 + rnd.nextDouble() * 3;
      }
      final StringBuilder w = new StringBuilder("[");
      for (int s = 0; s < sourceCount; s++)
        w.append(s > 0 ? "," : "").append(weights[s]);
      w.append("]");
      final String ctx = "round " + round + " k=" + k + " weights=" + w;
      assertMatches(fuse(sources, "{fusion: 'RRF', k: " + k + ", weights: " + w + "}"), modelRrf(orders, weights, k), ctx);
    }
  }

  @Test
  void linearMatchesTheFormula() {
    final Random rnd = new Random(11);
    for (int round = 0; round < 60; round++) {
      final int sourceCount = 2 + rnd.nextInt(3);
      final List<RID[]> orders = new ArrayList<>();
      final List<double[]> scores = new ArrayList<>();
      final List<List<Map<String, Object>>> sources = new ArrayList<>();
      final double[] weights = new double[sourceCount];
      for (int s = 0; s < sourceCount; s++) {
        final RID[] order = randomOrder(rnd, 1 + rnd.nextInt(DOCS));
        final double[] sc = randomScores(rnd, order.length);
        if (round % 7 == 0)
          java.util.Arrays.fill(sc, 0.5);
        orders.add(order);
        scores.add(sc);
        sources.add(source(order, sc));
        weights[s] = 0.1 + rnd.nextDouble() * 3;
      }
      final StringBuilder w = new StringBuilder("[");
      for (int s = 0; s < sourceCount; s++)
        w.append(s > 0 ? "," : "").append(weights[s]);
      w.append("]");
      // the model works on the float scores the rows carry
      final List<double[]> floatScores = new ArrayList<>();
      for (final double[] sc : scores) {
        final double[] f = new double[sc.length];
        for (int i = 0; i < sc.length; i++)
          f[i] = (float) sc[i];
        floatScores.add(f);
      }
      assertMatches(fuse(sources, "{fusion: 'LINEAR', weights: " + w + "}"), modelLinear(orders, floatScores, weights), "round " + round);
    }
  }

  @Test
  void dbsfMatchesTheFormula() {
    final Random rnd = new Random(13);
    for (int round = 0; round < 60; round++) {
      final int sourceCount = 2 + rnd.nextInt(3);
      final List<RID[]> orders = new ArrayList<>();
      final List<double[]> scores = new ArrayList<>();
      final List<List<Map<String, Object>>> sources = new ArrayList<>();
      final double[] weights = new double[sourceCount];
      for (int s = 0; s < sourceCount; s++) {
        final RID[] order = randomOrder(rnd, 1 + rnd.nextInt(DOCS));
        final double[] sc = randomScores(rnd, order.length);
        // one extreme outlier so that the 3 sigma clip is reached
        if (round % 2 == 0 && sc.length >= 8)
          sc[0] = sc[1] + 400;
        final double[] f = new double[sc.length];
        for (int i = 0; i < sc.length; i++)
          f[i] = (float) sc[i];
        orders.add(order);
        scores.add(f);
        sources.add(source(order, sc));
        weights[s] = 0.1 + rnd.nextDouble() * 3;
      }
      final StringBuilder w = new StringBuilder("[");
      for (int s = 0; s < sourceCount; s++)
        w.append(s > 0 ? "," : "").append(weights[s]);
      w.append("]");
      assertMatches(fuse(sources, "{fusion: 'DBSF', weights: " + w + "}"), modelDbsf(orders, scores, weights), "round " + round);
    }
  }

  @Test
  void aStrategyThatNeedsScoresRefusesASourceWithoutThem() {
    final List<Map<String, Object>> scored = source(new RID[] { pool.get(0), pool.get(1) }, new double[] { 2.0, 1.0 });
    final List<Map<String, Object>> scoreless = source(new RID[] { pool.get(1), pool.get(2) }, null);
    for (final String strategy : new String[] { "LINEAR", "DBSF" }) {
      assertThatThrownBy(() -> fuse(List.of(scored, scoreless), "{fusion: '" + strategy + "'}")).as(strategy)
          .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining(strategy).hasMessageContaining("source[1]");
      // rank only RRF does not need them
      assertThat(fuse(List.of(scored, scoreless), "{fusion: 'RRF'}")).hasSize(3);
    }
  }

  @Test
  void anEmptySourceContributesNothingToAScoreStrategy() {
    final List<Map<String, Object>> scored = source(new RID[] { pool.get(0), pool.get(1) }, new double[] { 2.0, 1.0 });
    final List<Map<String, Object>> empty = List.of();
    // LINEAR: min-max of {2, 1} -> doc0 = 1, doc1 = 0; DBSF: mean 1.5, sigma 0.5, band [0, 3] -> doc0 = 2/3, doc1 = 1/3
    final Map<RID, Double> linear = fuse(List.of(scored, empty), "{fusion: 'LINEAR'}");
    assertThat(linear.get(pool.get(0))).isCloseTo(1.0, within(1e-6));
    assertThat(linear.get(pool.get(1))).isCloseTo(0.0, within(1e-6));
    final Map<RID, Double> dbsf = fuse(List.of(scored, empty), "{fusion: 'DBSF'}");
    assertThat(dbsf.get(pool.get(0))).isCloseTo(2.0 / 3.0, within(1e-6));
    assertThat(dbsf.get(pool.get(1))).isCloseTo(1.0 / 3.0, within(1e-6));
  }

  @Test
  void theWeightsAndTheGroupSizeAreChecked() {
    final List<List<Map<String, Object>>> two = List.of(source(new RID[] { pool.get(0) }, null), source(new RID[] { pool.get(1) }, null));
    assertThatThrownBy(() -> fuse(two, "{weights: [1.0]}")).isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("weights");
    assertThatThrownBy(() -> fuse(two, "{weights: [1.0, 2.0, 3.0]}")).isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("weights");
    assertThatThrownBy(() -> fuse(two, "{weights: [1.0, 'x']}")).isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("weights[1]");
    assertThatThrownBy(() -> fuse(two, "{groupBy: 'n', groupSize: 0}")).isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("groupSize");
    assertThatThrownBy(() -> fuse(two, "{fusion: 'NOPE'}")).isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("NOPE");
    assertThatThrownBy(() -> fuse(List.of(source(new RID[] { pool.get(0) }, null)), "{fusion: 'RRF'}"))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("at least 2");
  }
}
