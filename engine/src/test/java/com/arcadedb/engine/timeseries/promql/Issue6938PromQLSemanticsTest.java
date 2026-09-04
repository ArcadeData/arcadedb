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
import com.arcadedb.engine.timeseries.promql.PromQLResult.MatrixResult;
import com.arcadedb.engine.timeseries.promql.PromQLResult.MatrixSeries;
import com.arcadedb.engine.timeseries.promql.PromQLResult.VectorSample;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #6938 - three PromQL semantics/presentation defects in {@link PromQLEvaluator}:
 *
 * <ol>
 *   <li>{@code or} produced {@code NaN} whenever the two sides shared a label set, because the matched branch of
 *       {@code applyVectorVector()} fell through to {@code applyBinaryOp()}, which maps AND/OR/UNLESS to
 *       {@code Double.NaN}. The canonical {@code sum(a) or sum(b)} fallback was the worst case: aggregation
 *       collapses labels to the empty set, so both sides always matched.</li>
 *   <li>A label matcher naming a column the type does not declare matched backwards in every direction:
 *       {@code =} silently matched every series (a typo widened the query instead of narrowing it) and
 *       {@code !=}/{@code !~} matched none.</li>
 *   <li>{@code evaluateRange()} stamped each matrix point with the raw sample timestamp instead of the
 *       evaluation step, so a series sparser than the step repeated timestamps rather than producing the
 *       regular {@code start + n*step} grid Prometheus guarantees.</li>
 * </ol>
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue6938PromQLSemanticsTest extends TestHelper {

  private static final long EVAL_TIME_MS = 6_000L;

  // --- 1. `or` on a matched label set -------------------------------------------------------------------

  @Test
  void orKeepsTheLeftHandSampleWhenBothSidesCollapseToTheEmptyLabelSet() {
    createTaggedType("promql6938_or_a", 10.0, 20.0);
    createTaggedType("promql6938_or_b", 100.0, 300.0);

    // sum() collapses labels to {} on both sides, so the two samples always match: the matched branch used to
    // fall through to applyBinaryOp(OR, ...) = NaN.
    final InstantVector iv = evaluateInstant("sum(promql6938_or_a) or sum(promql6938_or_b)");

    assertThat(iv.samples()).hasSize(1);
    final VectorSample sample = iv.samples().get(0);
    assertThat(sample.value()).isNotNaN();
    assertThat(sample.value()).isEqualTo(30.0); // 10 + 20 from the left side, never the right side's 400
    assertThat(sample.labels()).isEmpty();
  }

  @Test
  void orKeepsTheLeftHandSampleForEveryGroupSharedByBothSides() {
    // left  -> host=h1:10, host=h2:20
    // right -> host=h1:100, host=h3:300
    createTaggedType("promql6938_orby_a", 10.0, 20.0);
    createTaggedTypeWithHosts("promql6938_orby_b", "h1", 100.0, "h3", 300.0);

    final InstantVector iv = evaluateInstant(
        "sum by (host) (promql6938_orby_a) or sum by (host) (promql6938_orby_b)");

    final Map<String, Double> byHost = byHost(iv);
    // h1 is on both sides: OR must yield the LEFT value, not NaN and not the right one.
    assertThat(byHost).containsEntry("h1", 10.0).containsEntry("h2", 20.0).containsEntry("h3", 300.0);
    assertThat(byHost).hasSize(3);
    for (final VectorSample s : iv.samples())
      assertThat(s.value()).isNotNaN();
  }

  @Test
  void andAndUnlessKeepTheirEstablishedSemanticsOnAMatchedLabelSet() {
    createTaggedType("promql6938_and_a", 10.0, 20.0);
    createTaggedTypeWithHosts("promql6938_and_b", "h1", 100.0, "h3", 300.0);

    // Guard against the OR fix leaking into the sibling set operators.
    final Map<String, Double> and = byHost(
        evaluateInstant("sum by (host) (promql6938_and_a) and sum by (host) (promql6938_and_b)"));
    assertThat(and).containsExactlyEntriesOf(Map.of("h1", 10.0));

    final Map<String, Double> unless = byHost(
        evaluateInstant("sum by (host) (promql6938_and_a) unless sum by (host) (promql6938_and_b)"));
    assertThat(unless).containsExactlyEntriesOf(Map.of("h2", 20.0));
  }

  // --- 2. label matchers on a column absent from the schema ---------------------------------------------

  @Test
  void anEqualityMatcherOnAnAbsentColumnMatchesNoSeries() {
    createTaggedType("promql6938_absent_eq", 10.0, 20.0);

    // "jobb" is a typo for a label this type does not declare: Prometheus matches nothing, the old code
    // dropped the matcher entirely and returned every series.
    assertThat(evaluateInstant("promql6938_absent_eq{jobb=\"api\"}").samples()).isEmpty();
  }

  @Test
  void anInequalityMatcherOnAnAbsentColumnMatchesEverySeries() {
    createTaggedType("promql6938_absent_neq", 10.0, 20.0);

    // An absent label reads as "" for every series, so != with a non-empty value matches all of them.
    assertThat(evaluateInstant("promql6938_absent_neq{jobb!=\"api\"}").samples()).hasSize(2);
  }

  @Test
  void anEmptyValueMatcherOnAnAbsentColumnFollowsPrometheusAbsentLabelRules() {
    createTaggedType("promql6938_absent_empty", 10.0, 20.0);

    // ="" selects series that do not have the label -> all of them.
    assertThat(evaluateInstant("promql6938_absent_empty{jobb=\"\"}").samples()).hasSize(2);
    // !="" selects series that DO have the label -> none of them.
    assertThat(evaluateInstant("promql6938_absent_empty{jobb!=\"\"}").samples()).isEmpty();
  }

  @Test
  void regexMatchersOnAnAbsentColumnAreEvaluatedAgainstTheEmptyString() {
    createTaggedType("promql6938_absent_re", 10.0, 20.0);

    // =~".*" matches "" -> every series; =~"api" does not -> none.
    assertThat(evaluateInstant("promql6938_absent_re{jobb=~\".*\"}").samples()).hasSize(2);
    assertThat(evaluateInstant("promql6938_absent_re{jobb=~\"api\"}").samples()).isEmpty();
    // !~ is the exact negation of the two above.
    assertThat(evaluateInstant("promql6938_absent_re{jobb!~\".*\"}").samples()).isEmpty();
    assertThat(evaluateInstant("promql6938_absent_re{jobb!~\"api\"}").samples()).hasSize(2);
  }

  @Test
  void matchersOnAColumnTheTypeDoesDeclareAreUnaffected() {
    createTaggedType("promql6938_present", 10.0, 20.0);

    assertThat(byHost(evaluateInstant("promql6938_present{host=\"h1\"}"))).containsOnlyKeys("h1");
    assertThat(byHost(evaluateInstant("promql6938_present{host!=\"h1\"}"))).containsOnlyKeys("h2");
    assertThat(byHost(evaluateInstant("promql6938_present{host=~\"h.*\"}"))).containsOnlyKeys("h1", "h2");
    assertThat(evaluateInstant("promql6938_present{host!~\"h.*\"}").samples()).isEmpty();
  }

  @Test
  void anAbsentColumnMatcherAlsoGovernsARangeSelector() {
    createTaggedType("promql6938_absent_matrix", 10.0, 20.0);

    // Same rule on the matrix-selector path (rate() over a range vector), not just the instant one.
    assertThat(evaluateInstant("count_over_time(promql6938_absent_matrix{jobb=\"api\"}[5m])").samples()).isEmpty();
    assertThat(evaluateInstant("count_over_time(promql6938_absent_matrix{jobb!=\"api\"}[5m])").samples()).hasSize(2);
  }

  @Test
  void aRegexMatcherOnAnAbsentColumnIsStillValidatedRatherThanSilentlySkipped() {
    // Deliberate change of error behaviour, called out in review: the absent-column path now compiles the
    // pattern, so a malformed or ReDoS-shaped regex is rejected exactly as it is against a label the type does
    // declare. It used to be dropped before compilePattern() was ever reached, handing the author a plausible
    // empty result for a query that is simply invalid - and a typo'd label name is the case where being told
    // matters most.
    createTaggedType("promql6938_absent_badre", 10.0, 20.0);

    assertThatThrownBy(() -> evaluateInstant("promql6938_absent_badre{jobb=~\"(a+)+\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("ReDoS");
    assertThatThrownBy(() -> evaluateInstant("promql6938_absent_badre{jobb!~\"[\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid regex pattern");
  }

  // --- 3. step-aligned matrix points --------------------------------------------------------------------

  @Test
  void rangePointsAreStampedWithTheEvaluationStepNotTheRawSampleTimestamp() {
    // Two samples 4 seconds apart, queried on a 1-second step: every step between them resolves to the same
    // ts=1000 sample through the lookback window, so the raw sample timestamp repeated four times instead of
    // producing the regular start + n*step grid.
    database.command("sql", "CREATE TIMESERIES TYPE promql6938_grid TIMESTAMP ts FIELDS (value DOUBLE)");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO promql6938_grid SET ts = 1000, value = 1.0");
      database.command("sql", "INSERT INTO promql6938_grid SET ts = 5000, value = 5.0");
    });

    final PromQLResult result = new PromQLEvaluator(getDatabaseInternal())
        .evaluateRange(new PromQLParser("promql6938_grid").parse(), 1000L, 5000L, 1000L);

    assertThat(result).isInstanceOf(MatrixResult.class);
    final List<MatrixSeries> series = ((MatrixResult) result).series();
    assertThat(series).hasSize(1);

    final List<double[]> points = series.get(0).values();
    assertThat(points).hasSize(5);
    for (int i = 0; i < points.size(); i++)
      assertThat(points.get(i)[0]).as("point %d must sit on the step grid", i).isEqualTo(1000.0 + i * 1000.0);

    // The values still come from the lookback resolution: 1.0 carried forward until ts=5000 lands.
    assertThat(points.get(0)[1]).isEqualTo(1.0);
    assertThat(points.get(3)[1]).isEqualTo(1.0);
    assertThat(points.get(4)[1]).isEqualTo(5.0);
  }

  @Test
  void rangePointsOfAnAggregationAreAlsoStepAligned() {
    createTaggedType("promql6938_grid_agg", 10.0, 20.0);

    final PromQLResult result = new PromQLEvaluator(getDatabaseInternal())
        .evaluateRange(new PromQLParser("sum(promql6938_grid_agg)").parse(), 2000L, 6000L, 2000L);

    final List<MatrixSeries> series = ((MatrixResult) result).series();
    assertThat(series).hasSize(1);
    final List<double[]> points = series.get(0).values();
    assertThat(points).hasSize(3);
    assertThat(points.get(0)[0]).isEqualTo(2000.0);
    assertThat(points.get(1)[0]).isEqualTo(4000.0);
    assertThat(points.get(2)[0]).isEqualTo(6000.0);
  }

  // --- helpers ------------------------------------------------------------------------------------------

  private DatabaseInternal getDatabaseInternal() {
    return (DatabaseInternal) database;
  }

  private InstantVector evaluateInstant(final String promql) {
    final PromQLExpr expr = new PromQLParser(promql).parse();
    final PromQLResult result = new PromQLEvaluator(getDatabaseInternal()).evaluateInstant(expr, EVAL_TIME_MS);
    assertThat(result).isInstanceOf(InstantVector.class);
    return (InstantVector) result;
  }

  private static Map<String, Double> byHost(final InstantVector iv) {
    final Map<String, Double> result = new LinkedHashMap<>();
    for (final VectorSample s : iv.samples())
      result.put(s.labels().get("host"), s.value());
    return result;
  }

  private void createTaggedType(final String typeName, final double firstValue, final double secondValue) {
    createTaggedTypeWithHosts(typeName, "h1", firstValue, "h2", secondValue);
  }

  private void createTaggedTypeWithHosts(final String typeName, final String firstHost, final double firstValue,
      final String secondHost, final double secondValue) {
    database.command("sql",
        "CREATE TIMESERIES TYPE " + typeName + " TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE)");
    database.transaction(() -> {
      database.command("sql",
          "INSERT INTO " + typeName + " SET ts = 1000, host = '" + firstHost + "', value = " + firstValue);
      database.command("sql",
          "INSERT INTO " + typeName + " SET ts = 1000, host = '" + secondHost + "', value = " + secondValue);
    });
  }
}
