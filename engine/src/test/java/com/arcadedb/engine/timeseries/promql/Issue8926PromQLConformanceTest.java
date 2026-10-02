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
package com.arcadedb.engine.timeseries.promql;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.timeseries.promql.PromQLResult.InstantVector;
import com.arcadedb.engine.timeseries.promql.PromQLResult.ScalarResult;
import com.arcadedb.engine.timeseries.promql.PromQLResult.VectorSample;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issues #8926 (vector matching must ignore {@code __name__}), #8927 (topk/bottomk must rank the
 * ABSENT marker below every real sample), #8928 (round() must stay in double) and #8929 (unary minus vs {@code ^},
 * IEEE-754 division by zero).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8926PromQLConformanceTest extends TestHelper {

  private static final long EVAL_TIME_MS = 6_000L;

  // ---- #8926

  @Test
  void arithmeticBetweenTwoDifferentMetricsMatchesOnLabelsAndDropsTheName() throws Exception {
    createSeries("m8926_errors", new String[] { "api" }, new double[] { 3.0 });
    createSeries("m8926_total", new String[] { "api" }, new double[] { 12.0 });

    final List<VectorSample> samples = vectorOf("m8926_errors / m8926_total");
    assertThat(samples).hasSize(1);
    assertThat(samples.getFirst().value()).isEqualTo(0.25);
    assertThat(samples.getFirst().labels()).doesNotContainKey("__name__").containsEntry("host", "api");

    assertThat(vectorOf("m8926_errors + m8926_total").getFirst().value()).isEqualTo(15.0);
    assertThat(vectorOf("rate(m8926_errors[5m]) / rate(m8926_total[5m])")).hasSize(1);
  }

  @Test
  void comparisonAndSetOperatorsIgnoreTheMetricName() throws Exception {
    createSeries("m8926_a", new String[] { "api" }, new double[] { 3.0 });
    createSeries("m8926_b", new String[] { "api" }, new double[] { 12.0 });

    assertThat(vectorOf("m8926_a < m8926_b")).hasSize(1);
    assertThat(vectorOf("m8926_a and m8926_b")).hasSize(1);
    // the matching series must be EXCLUDED
    assertThat(vectorOf("m8926_a unless m8926_b")).isEmpty();
  }

  @Test
  void sumWithoutDropsTheMetricName() throws Exception {
    createSeries("m8926_w1", new String[] { "api" }, new double[] { 3.0 });
    createSeries("m8926_w2", new String[] { "api" }, new double[] { 12.0 });

    final List<VectorSample> one = vectorOf("sum without (instance) (m8926_w1)");
    assertThat(one.getFirst().labels()).doesNotContainKey("__name__");
    final List<VectorSample> ratio = vectorOf("sum without (instance) (m8926_w1) / sum without (instance) (m8926_w2)");
    assertThat(ratio).hasSize(1);
    assertThat(ratio.getFirst().value()).isEqualTo(0.25);
  }

  @Test
  void orAcrossDifferentlyNamedMetricsMatchesOnLabels() throws Exception {
    createSeries("m8926_o1", new String[] { "api" }, new double[] { 3.0 });
    createSeries("m8926_o2", new String[] { "api", "web" }, new double[] { 12.0, 7.0 });

    // api is matched (left wins), web is the unmatched right-hand sample
    final List<VectorSample> samples = vectorOf("m8926_o1 or m8926_o2");
    assertThat(samples).hasSize(2);
    assertThat(samples.getFirst().value()).isEqualTo(3.0);
    assertThat(samples.get(1).labels()).containsEntry("host", "web");
  }

  @Test
  void delimiterCharactersInATagValueNeverMakeDistinctLabelSetsMatch() throws Exception {
    database.command("sql",
        "CREATE TIMESERIES TYPE m8926_x1 TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 1");
    database.command("sql",
        "CREATE TIMESERIES TYPE m8926_x2 TIMESTAMP ts TAGS (host STRING, zone STRING) FIELDS (value DOUBLE) SHARDS 1");
    database.begin();
    ((LocalTimeSeriesType) database.getSchema().getType("m8926_x1")).getEngine()
        .appendSamples(new long[] { 1_000L }, new Object[] { "a,zone=b" }, new Object[] { 1.0 });
    ((LocalTimeSeriesType) database.getSchema().getType("m8926_x2")).getEngine()
        .appendSamples(new long[] { 1_000L }, new Object[] { "a" }, new Object[] { "b" }, new Object[] { 2.0 });
    database.commit();

    // {host="a,zone=b"} and {host="a",zone="b"} are different label sets: nothing matches, nothing is excluded
    assertThat(vectorOf("m8926_x1 and m8926_x2")).isEmpty();
    assertThat(vectorOf("m8926_x1 unless m8926_x2")).hasSize(1);
  }

  // ---- #8927

  @Test
  void topkAndBottomkHonourGroupingAndAnInvalidKIsRefusedOnAnEmptyVector() throws Exception {
    createSeries("m8927_g", new String[] { "a", "b" }, new double[] { 1.0, 2.0 });

    final List<VectorSample> byHost = vectorOf("topk by (host) (1, m8927_g)");
    assertThat(byHost).hasSize(2);
    assertThat(vectorOf("bottomk without (host) (1, m8927_g)")).hasSize(1);
    assertThat(vectorOf("bottomk without (host) (1, m8927_g)").getFirst().value()).isEqualTo(1.0);
    assertThatThrownBy(() -> vectorOf("topk(0/0, m8927_g{host=\"nobody\"})")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void topkAndBottomkRankAnAbsentSampleBelowEveryRealOne() throws Exception {
    createSeries("m8927_k", new String[] { "a", "b", "c" }, new double[] { 10.0, Double.NaN, 5.0 });

    final List<VectorSample> top = vectorOf("topk(1, m8927_k)");
    assertThat(top).hasSize(1);
    assertThat(top.getFirst().labels()).containsEntry("host", "a");

    final List<VectorSample> bottom = vectorOf("bottomk(1, m8927_k)");
    assertThat(bottom).hasSize(1);
    assertThat(bottom.getFirst().labels()).containsEntry("host", "c");

    // the absent sample only fills a slot when there is nothing else, as Prometheus does
    final List<VectorSample> top3 = vectorOf("topk(3, m8927_k)");
    assertThat(top3).hasSize(3);
    assertThat(top3.get(0).value()).isEqualTo(10.0);
    assertThat(top3.get(1).value()).isEqualTo(5.0);
    assertThat(top3.get(2).value()).isNaN();
  }

  @Test
  void topkAcceptsAnyScalarExpressionAndRefusesAnInvalidK() throws Exception {
    createSeries("m8927_e", new String[] { "a", "b", "c" }, new double[] { 1.0, 2.0, 3.0 });

    assertThat(vectorOf("topk(1+1, m8927_e)")).hasSize(2);
    assertThat(vectorOf("topk(0, m8927_e)")).isEmpty();
    assertThat(vectorOf("bottomk(-1, m8927_e)")).isEmpty();
    assertThatThrownBy(() -> vectorOf("topk(0/0, m8927_e)")).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> vectorOf("topk(m8927_e, m8927_e)")).isInstanceOf(IllegalArgumentException.class);
  }

  // ---- #8928

  @Test
  void roundStaysInDouble() {
    assertThat(PromQLFunctions.round(2.5, 1)).isEqualTo(3.0);
    assertThat(PromQLFunctions.round(-2.5, 1)).isEqualTo(-2.0);
    assertThat(PromQLFunctions.round(0.49999999999999994, 1)).isEqualTo(1.0);
    assertThat(PromQLFunctions.round(1e30, 1)).isEqualTo(1e30);
    assertThat(PromQLFunctions.round(-1e30, 1)).isEqualTo(-1e30);
    assertThat(PromQLFunctions.round(Double.NaN, 1)).isNaN();
    assertThat(PromQLFunctions.round(Double.POSITIVE_INFINITY, 1)).isEqualTo(Double.POSITIVE_INFINITY);
    assertThat(PromQLFunctions.round(Double.NEGATIVE_INFINITY, 1)).isEqualTo(Double.NEGATIVE_INFINITY);
    assertThat(PromQLFunctions.round(9.3e18, 1)).isEqualTo(9.3e18);
    assertThat(PromQLFunctions.round(12, 5)).isEqualTo(10.0);
    assertThat(PromQLFunctions.round(7.26, 0.1)).isEqualTo(7.3);
    assertThat(PromQLFunctions.round(7.3, 0.5)).isEqualTo(7.5);
    assertThat(PromQLFunctions.round(7.2, 0.5)).isEqualTo(7.0);
    assertThat(PromQLFunctions.round(12, -5)).isEqualTo(10.0);
  }

  @Test
  void roundKeepsAnAbsentSampleAbsent() throws Exception {
    createSeries("m8928_r", new String[] { "a" }, new double[] { Double.NaN });
    assertThat(vectorOf("round(m8928_r)").getFirst().value()).isNaN();
  }

  // ---- #8929

  @Test
  void unaryMinusAppliesToTheWholePowerExpression() {
    assertThat(scalarOf("-2^2")).isEqualTo(-4.0);
    assertThat(scalarOf("2^-2")).isEqualTo(0.25);
    assertThat(scalarOf("-2^-2")).isEqualTo(-0.25);
    assertThat(scalarOf("2^3^2")).isEqualTo(512.0);
    assertThat(scalarOf("--2^2")).isEqualTo(4.0);
    assertThat(scalarOf("3*-2^2")).isEqualTo(-12.0);
    assertThat(scalarOf("(-2)^2")).isEqualTo(4.0);
    assertThat(scalarOf("1 - -2")).isEqualTo(3.0);
  }

  @Test
  void divisionByZeroIsIeee754() {
    assertThat(scalarOf("1/0")).isEqualTo(Double.POSITIVE_INFINITY);
    assertThat(scalarOf("-1/0")).isEqualTo(Double.NEGATIVE_INFINITY);
    assertThat(scalarOf("0/0")).isNaN();
    assertThat(scalarOf("1%0")).isNaN();
  }

  // ---- helpers

  private double scalarOf(final String promql) {
    final PromQLExpr expr = new PromQLParser(promql).parse();
    final PromQLResult result = new PromQLEvaluator((DatabaseInternal) database).evaluateInstant(expr, EVAL_TIME_MS);
    assertThat(result).isInstanceOf(ScalarResult.class);
    return ((ScalarResult) result).value();
  }

  private List<VectorSample> vectorOf(final String promql) {
    final PromQLExpr expr = new PromQLParser(promql).parse();
    final PromQLResult result = new PromQLEvaluator((DatabaseInternal) database).evaluateInstant(expr, EVAL_TIME_MS);
    assertThat(result).isInstanceOf(InstantVector.class);
    return ((InstantVector) result).samples();
  }

  /** One sample per host at t=1000 (NaN has no SQL literal, so the samples go through the engine). */
  private void createSeries(final String typeName, final String[] hosts, final double[] values) throws Exception {
    database.command("sql",
        "CREATE TIMESERIES TYPE " + typeName + " TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 1");
    final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType(typeName);
    final long[] ts = new long[hosts.length];
    final Object[] tags = new Object[hosts.length];
    final Object[] vals = new Object[hosts.length];
    for (int i = 0; i < hosts.length; i++) {
      ts[i] = 1_000L;
      tags[i] = hosts[i];
      vals[i] = values[i];
    }
    database.begin();
    tsType.getEngine().appendSamples(ts, tags, vals);
    database.commit();
  }
}
