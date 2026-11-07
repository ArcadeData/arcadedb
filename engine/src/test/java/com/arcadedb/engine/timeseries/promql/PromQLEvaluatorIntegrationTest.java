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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.timeseries.promql.PromQLResult.InstantVector;
import com.arcadedb.engine.timeseries.promql.PromQLResult.MatrixResult;
import com.arcadedb.engine.timeseries.promql.PromQLResult.MatrixSeries;
import com.arcadedb.engine.timeseries.promql.PromQLResult.RangeSeries;
import com.arcadedb.engine.timeseries.promql.PromQLResult.RangeVector;
import com.arcadedb.engine.timeseries.promql.PromQLResult.ScalarResult;
import com.arcadedb.engine.timeseries.promql.PromQLResult.VectorSample;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr.BinaryExpr;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.TimeSeriesTypeBuilder;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.offset;
import static org.assertj.core.api.Assertions.within;

/**
 * End-to-end PromQL integration tests: parse → evaluate against live TimeSeries data.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PromQLEvaluatorIntegrationTest extends TestHelper {

  @Test
  void instantVectorSelector() {
    createTypeAndInsertData("cpu_usage");

    final PromQLEvaluator evaluator = new PromQLEvaluator(getDatabaseInternal());
    final PromQLExpr expr = new PromQLParser("cpu_usage").parse();
    final PromQLResult result = evaluator.evaluateInstant(expr, 6000L);

    assertThat(result).isInstanceOf(PromQLResult.InstantVector.class);
    final PromQLResult.InstantVector iv = (PromQLResult.InstantVector) result;
    // Should find at least the latest sample within the 5-minute lookback window
    assertThat(iv.samples()).isNotEmpty();
  }

  @Test
  void rateFunction() {
    createTypeAndInsertData("http_requests_total");

    final PromQLEvaluator evaluator = new PromQLEvaluator(getDatabaseInternal());
    final PromQLExpr expr = new PromQLParser("rate(http_requests_total[5m])").parse();
    final PromQLResult result = evaluator.evaluateInstant(expr, 6000L);

    assertThat(result).isInstanceOf(PromQLResult.InstantVector.class);
    final PromQLResult.InstantVector iv = (PromQLResult.InstantVector) result;
    // rate() over a counter-like series should produce non-negative values
    for (final PromQLResult.VectorSample sample : iv.samples())
      assertThat(sample.value()).isGreaterThanOrEqualTo(0.0);
  }

  @Test
  void rateExtrapolatesToRangeWindow() {
    // Regression for #4598: rate()/increase() must extrapolate to the full matrix window (Prometheus
    // semantics), not divide by the actual span between samples. Two sparse counter samples 60s apart,
    // centered inside a 5-minute window: first=1000 @120s, last=1100 @180s.
    database.command("sql", "CREATE TIMESERIES TYPE sparse_counter TIMESTAMP ts FIELDS (value DOUBLE)");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO sparse_counter SET ts = 120000, value = 1000.0");
      database.command("sql", "INSERT INTO sparse_counter SET ts = 180000, value = 1100.0");
    });

    final PromQLEvaluator evaluator = new PromQLEvaluator(getDatabaseInternal());

    // Prometheus reference: resultValue=100, sampledInterval=60s, durationToStart=durationToEnd=120s,
    // both >= 1.1*avg(60s) so each contributes avg/2=30s -> extrapolateToInterval=120s,
    // factor=120/60=2.0 -> rate = 100 * 2.0 / 300 = 0.6667/s (the old code returned 100/60 = 1.667/s).
    final PromQLExpr rateExpr = new PromQLParser("rate(sparse_counter[5m])").parse();
    final PromQLResult.InstantVector rateIv = (PromQLResult.InstantVector) evaluator.evaluateInstant(rateExpr, 300000L);
    assertThat(rateIv.samples()).hasSize(1);
    assertThat(rateIv.samples().getFirst().value()).isCloseTo(0.6667, within(0.001));

    // increase = rate * range = 200 (the old code returned the raw 100).
    final PromQLExpr incExpr = new PromQLParser("increase(sparse_counter[5m])").parse();
    final PromQLResult.InstantVector incIv = (PromQLResult.InstantVector) evaluator.evaluateInstant(incExpr, 300000L);
    assertThat(incIv.samples()).hasSize(1);
    assertThat(incIv.samples().getFirst().value()).isCloseTo(200.0, within(0.1));
  }

  @Test
  void binaryExpressionWithScalar() {
    createTypeAndInsertData("metric_a");

    final PromQLEvaluator evaluator = new PromQLEvaluator(getDatabaseInternal());
    final PromQLExpr expr = new PromQLParser("metric_a * 2").parse();
    final PromQLResult result = evaluator.evaluateInstant(expr, 6000L);

    assertThat(result).isInstanceOf(PromQLResult.InstantVector.class);
    final PromQLResult.InstantVector iv = (PromQLResult.InstantVector) result;
    for (final PromQLResult.VectorSample sample : iv.samples())
      assertThat(sample.value()).isNotNaN();
  }

  @Test
  void sumAggregation() {
    createTypeWithTags("tagged_metric");

    final PromQLEvaluator evaluator = new PromQLEvaluator(getDatabaseInternal());
    final PromQLExpr expr = new PromQLParser("sum(tagged_metric)").parse();
    final PromQLResult result = evaluator.evaluateInstant(expr, 6000L);

    assertThat(result).isInstanceOf(PromQLResult.InstantVector.class);
    final PromQLResult.InstantVector iv = (PromQLResult.InstantVector) result;
    assertThat(iv.samples()).hasSize(1);
    // Sum of latest samples per label combination
    assertThat(iv.samples().getFirst().value()).isGreaterThan(0.0);
  }

  @Test
  void sumByAggregation() {
    createTypeWithTags("group_metric");

    final PromQLEvaluator evaluator = new PromQLEvaluator(getDatabaseInternal());
    final PromQLExpr expr = new PromQLParser("sum by (host) (group_metric)").parse();
    final PromQLResult result = evaluator.evaluateInstant(expr, 6000L);

    assertThat(result).isInstanceOf(PromQLResult.InstantVector.class);
    final PromQLResult.InstantVector iv = (PromQLResult.InstantVector) result;
    // Should have one result per distinct host
    assertThat(iv.samples()).hasSizeGreaterThanOrEqualTo(1);
  }

  @Test
  void rangeQueryWithStep() {
    createTypeAndInsertData("range_metric");

    final PromQLEvaluator evaluator = new PromQLEvaluator(getDatabaseInternal());
    final PromQLExpr expr = new PromQLParser("range_metric").parse();
    final PromQLResult result = evaluator.evaluateRange(expr, 1000L, 5000L, 1000L);

    assertThat(result).isInstanceOf(PromQLResult.MatrixResult.class);
    final PromQLResult.MatrixResult mr = (PromQLResult.MatrixResult) result;
    assertThat(mr.series()).isNotEmpty();
  }

  @Test
  void emptyResultForNonExistentType() {
    final PromQLEvaluator evaluator = new PromQLEvaluator(getDatabaseInternal());
    final PromQLExpr expr = new PromQLParser("nonexistent_metric").parse();
    final PromQLResult result = evaluator.evaluateInstant(expr, 1000L);

    assertThat(result).isInstanceOf(PromQLResult.InstantVector.class);
    final PromQLResult.InstantVector iv = (PromQLResult.InstantVector) result;
    assertThat(iv.samples()).isEmpty();
  }

  @Test
  void evaluateRangeStepZero() {
    final PromQLEvaluator evaluator = new PromQLEvaluator(getDatabaseInternal());
    final PromQLExpr expr = new PromQLParser("42").parse();

    assertThatThrownBy(() -> evaluator.evaluateRange(expr, 1000L, 5000L, 0L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("stepMs must be positive");
  }

  @Test
  void evaluateRangeInvertedBounds() {
    // Regression: endMs < startMs previously returned empty results silently
    final PromQLEvaluator evaluator = new PromQLEvaluator(getDatabaseInternal());
    final PromQLExpr expr = new PromQLParser("42").parse();

    assertThatThrownBy(() -> evaluator.evaluateRange(expr, 5000L, 1000L, 1000L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("endMs")
        .hasMessageContaining("startMs");
  }

  @Test
  void reDoSPatternRejected() {
    // Security: regex patterns with nested quantifiers must be rejected to prevent ReDoS attacks
    createTypeWithTags("redos_metric");

    final PromQLEvaluator evaluator = new PromQLEvaluator(getDatabaseInternal());
    // (a+)+ is the classic ReDoS pattern
    final PromQLExpr expr = new PromQLParser("redos_metric{host=~\"(a+)+\"}").parse();

    assertThatThrownBy(() -> evaluator.evaluateInstant(expr, 6000L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("ReDoS");
  }

  @Test
  void unparenthesizedCatastrophicPatternIsAbortedByRegexTimeout() {
    // Issue #5886 follow-up: REDOS_CHECK only flags parenthesized nested-quantifier/alternation shapes
    // ((a+)+, (a|aa)+, ...), so a sequence of unparenthesized quantified segments - the same
    // "sequential-.*-without-nesting" shape that turned out to break SQL LIKE and full-text wildcard
    // queries - reaches Pattern.matcher(...).matches() uncaught by that static pre-filter. Bounded now by
    // routing =~/!~ through TimeBoundRegex, same as every other entry point this issue covers.
    database.getConfiguration().setValue(GlobalConfiguration.COMMAND_REGEX_TIMEOUT, 200L);

    final String typeName = "promql_redos_unparenthesized";
    database.command("sql", "CREATE TIMESERIES TYPE " + typeName + " TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE)");
    final String pathologicalHost = "a".repeat(40);
    database.transaction(() -> database.command("sql",
        "INSERT INTO " + typeName + " SET ts = 1000, host = '" + pathologicalHost + "', value = 1.0"));

    final PromQLEvaluator evaluator = new PromQLEvaluator(getDatabaseInternal());
    // 20 sequential "a*" quantifiers then a literal 'c' that never appears in the all-'a' host value: forces
    // the same exhaustive backtrack-then-fail every other reproducer in this issue relies on, with no '(' in
    // sight for REDOS_CHECK to catch.
    final String wildcardPattern = "a*".repeat(20) + "c";
    final PromQLExpr expr = new PromQLParser(typeName + "{host=~\"" + wildcardPattern + "\"}").parse();

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    assertThatThrownBy(() -> evaluator.evaluateInstant(expr, 6000L)).isInstanceOf(TimeoutException.class);

    // Generous upper bound: proves the scan was aborted near the configured deadline rather than merely
    // being slow (the unbounded match takes tens of seconds).
    stopwatch.assertGaveUpWithin(5000, "the configured 200ms deadline from an unbounded match");
  }

  @Test
  void rangeQuerySharesOneTimeoutBudgetAcrossAllSteps() {
    // Issue #5886, 12th review pass: evaluateVectorSelector()/evaluateMatrixSelector() already share one
    // deadline across every row within a single step's scan, but evaluateRange() calls evaluate() once per
    // step (up to MAX_RANGE_STEPS = 1,000,000), and each of those calls used to compute its own fresh deadline -
    // the same N-times-timeout shape closed at the row level, reopened here at the step level. The deadline is
    // now cached on the PromQLEvaluator instance itself (safe here since a fresh evaluator is created per
    // top-level query - see SQLFunctionPromQL - never reused across executions the way RegexExpression's AST
    // nodes are).
    database.getConfiguration().setValue(GlobalConfiguration.COMMAND_REGEX_TIMEOUT, 200L);

    final String typeName = "promql_range_multistep";
    database.command("sql", "CREATE TIMESERIES TYPE " + typeName + " TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE)");
    final String pathologicalHost = "a".repeat(40); // no trailing 'c': forces the same exhaustive backtrack as
    // unparenthesizedCatastrophicPatternIsAbortedByRegexTimeout above. A single data point at ts=0, visible
    // from every step's lookback window below (evalTime - lookbackMs <= 0 for every evalTime in the range), so
    // every one of the 10 steps evaluates the same catastrophic label matcher against it.
    database.transaction(() -> database.command("sql",
        "INSERT INTO " + typeName + " SET ts = 0, host = '" + pathologicalHost + "', value = 1.0"));

    final PromQLEvaluator evaluator = new PromQLEvaluator(getDatabaseInternal(), 100_000L);
    // Unparenthesized shape (see unparenthesizedCatastrophicPatternIsAbortedByRegexTimeout above): REDOS_CHECK
    // rejects parenthesized nested-quantifier patterns like (.*a){20}$ at parse time, before this ever reaches
    // TimeBoundRegex, so the reproducer here has to be the sequential-a*-without-nesting shape instead.
    final PromQLExpr expr = new PromQLParser(typeName + "{host=~\"" + "a*".repeat(20) + "c\"}").parse();

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    // startMs=1000, endMs=10000, stepMs=1000 -> 10 steps, every one of which sees the ts=0 data point.
    assertThatThrownBy(() -> evaluator.evaluateRange(expr, 1000L, 10_000L, 1000L)).isInstanceOf(TimeoutException.class);

    // 10 independent 200ms-per-step budgets would take >= 2000ms; a shared deadline keeps the whole range
    // query close to the single configured 200ms bound instead.
    stopwatch.assertStayedUnder(1000, "one deadline shared by the whole range query, not one 200ms budget per step");
  }

  @Test
  void scalarArithmetic() {
    final PromQLEvaluator evaluator = new PromQLEvaluator(getDatabaseInternal());
    final PromQLExpr expr = new PromQLParser("2 + 3 * 4").parse();
    final PromQLResult result = evaluator.evaluateInstant(expr, 1000L);

    assertThat(result).isInstanceOf(PromQLResult.ScalarResult.class);
    assertThat(((PromQLResult.ScalarResult) result).value()).isEqualTo(14.0);
  }

  @Test
  void extractLabelsWithTagBeforeTimestamp() {
    // Regression: extractLabels / extractValue must work correctly even when the
    // TIMESTAMP column is not at schema position 0.
    // Schema: TAG(host) at index 0, TIMESTAMP(ts) at index 1, FIELD(value) at index 2.
    // Row format from engine: [ts, host, value] — TIMESTAMP is always row[0].
    // Previously the code used row[schemaIndex] directly, so host would read the timestamp.
    final String typeName = "promql_tag_first";
    // Use the builder API to create a type with TAG before TIMESTAMP
    new TimeSeriesTypeBuilder(getDatabaseInternal())
        .withName(typeName)
        .withTag("host", Type.STRING)
        .withTimestamp("ts")
        .withField("value", Type.DOUBLE)
        .withShards(1)
        .create();

    database.transaction(() -> {
      database.command("sql", "INSERT INTO " + typeName + " SET ts = 1000, host = 'srv1', value = 42.0");
      database.command("sql", "INSERT INTO " + typeName + " SET ts = 2000, host = 'srv2', value = 84.0");
    });

    final PromQLEvaluator evaluator = new PromQLEvaluator(getDatabaseInternal());
    final PromQLExpr expr = new PromQLParser(typeName + "{host=\"srv1\"}").parse();
    // eval at 6000ms so the 5-minute lookback window covers ts=1000 and ts=2000
    final PromQLResult result = evaluator.evaluateInstant(expr, 6000L);

    assertThat(result).isInstanceOf(PromQLResult.InstantVector.class);
    final PromQLResult.InstantVector iv = (PromQLResult.InstantVector) result;
    assertThat(iv.samples()).isNotEmpty();
    // The label "host" must resolve to "srv1", not to a timestamp number
    assertThat(iv.samples().getFirst().labels()).containsEntry("host", "srv1");
    // The value must be a numeric double, not NaN
    assertThat(iv.samples().getFirst().value()).isEqualTo(42.0);
  }

  @Test
  void queryUsesTheForEachRowFoldingPath() {
    // evaluateVectorSelector() folds rows into its answer through TimeSeriesEngine#forEachRow rather than
    // collecting them via iterateQuery()/query() first (issue #7696) - the latter materialise every matching
    // row before the caller sees the first one. We verify this indirectly by confirming a dataset is still
    // evaluated correctly end to end.
    createTypeAndInsertData("promql_iter_test");

    final PromQLEvaluator evaluator = new PromQLEvaluator(getDatabaseInternal());
    // eval at 6000ms so the 5-minute lookback window covers the inserted data
    final PromQLResult result = evaluator.evaluateInstant(
        new PromQLParser("promql_iter_test").parse(), 6000L);

    assertThat(result).isInstanceOf(PromQLResult.InstantVector.class);
    assertThat(((PromQLResult.InstantVector) result).samples()).isNotEmpty();
  }

  @Test
  void promQLSqlFunction() {
    createTypeAndInsertData("promql_sql_test");

    // RETURN with a List<Map> unwraps each map entry into a separate result row.
    // Each row has __value__ and any label properties as direct row properties.
    try (final ResultSet rs = database.command("sql", "RETURN promql('promql_sql_test', 6000)")) {
      assertThat(rs.hasNext()).isTrue();
      // First row should have a numeric __value__
      final Object sampleValue = rs.next().getProperty("__value__");
      assertThat(sampleValue).isNotNull().isInstanceOf(Double.class);
    }
  }

  @Test
  void setRegexDeadlineOverridesTheLazilyComputedDefault() {
    // Issue #5886, 17th review pass: SQLFunctionPromQL creates a fresh PromQLEvaluator per promql() call (never
    // reused across executions the way rangeQuerySharesOneTimeoutBudgetAcrossAllSteps above relies on), so
    // without a way to inject an externally-resolved deadline it would always fall back to its own
    // GlobalConfiguration-derived budget rather than the CommandContext-cached one every other per-row SQL
    // function in this issue shares. Verify setRegexDeadline() is actually honored - not silently ignored - by
    // forcing an already-elapsed deadline and confirming it aborts an otherwise-trivial, non-catastrophic match
    // that would normally complete instantly. The host value must be long enough to force more than
    // TimeBoundRegex's CHECK_INTERVAL (256) charAt() calls - too short an input completes before the first
    // deadline checkpoint is ever reached, regardless of whether the deadline is already expired.
    final String typeName = "promql_setregexdeadline_metric";
    database.command("sql", "CREATE TIMESERIES TYPE " + typeName + " TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE)");
    final String longHost = "a".repeat(256); // tag dictionary caps values at 256 bytes
    database.transaction(() -> database.command("sql",
        "INSERT INTO " + typeName + " SET ts = 1000, host = '" + longHost + "', value = 1.0"));

    final PromQLEvaluator evaluator = new PromQLEvaluator(getDatabaseInternal());
    evaluator.setRegexDeadline(System.nanoTime() - 1_000_000_000L); // 1s in the past: already expired

    final PromQLExpr expr = new PromQLParser(typeName + "{host=~\"a*\"}").parse();

    assertThatThrownBy(() -> evaluator.evaluateInstant(expr, 6000L)).isInstanceOf(TimeoutException.class);
  }

  @Test
  void promQLSqlFunctionAbortsOnCatastrophicPattern() {
    // End-to-end coverage for the promql() SQL function path specifically (as opposed to calling
    // PromQLEvaluator.evaluateInstant directly, as unparenthesizedCatastrophicPatternIsAbortedByRegexTimeout
    // above does) - proves the context.getRegexDeadline() -> evaluator.setRegexDeadline() wiring in
    // SQLFunctionPromQL actually bounds a catastrophic =~ label matcher reached through SELECT promql(...).
    database.getConfiguration().setValue(GlobalConfiguration.COMMAND_REGEX_TIMEOUT, 200L);

    final String typeName = "promql_function_redos";
    database.command("sql", "CREATE TIMESERIES TYPE " + typeName + " TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE)");
    final String pathologicalHost = "a".repeat(40); // no trailing 'c': forces exhaustive backtrack-then-fail
    database.transaction(() -> database.command("sql",
        "INSERT INTO " + typeName + " SET ts = 0, host = '" + pathologicalHost + "', value = 1.0"));

    final String wildcardPattern = "a*".repeat(20) + "c";
    final String promqlExpr = typeName + "{host=~\"" + wildcardPattern + "\"}";

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    assertThatThrownBy(() -> database.command("sql", "RETURN promql('" + promqlExpr + "', 6000)").close())
        .isInstanceOf(TimeoutException.class);

    // Generous upper bound: proves the call was aborted near the configured deadline rather than merely being
    // slow (the unbounded match takes tens of seconds).
    stopwatch.assertGaveUpWithin(5000, "the configured 200ms deadline from an unbounded match");
  }

  @Test
  void promQLSqlFunctionUsesCurrentTimeWhenNoArgument() {
    createTypeAndInsertData("promql_sql_notime_test");

    // Called without evalTimeMs — uses System.currentTimeMillis() internally.
    // The data was inserted with timestamps 1000-5000 ms which are in the far past
    // relative to current time, so results will be empty (lookback window is 5 minutes).
    // RETURN of an empty list produces no rows — verify the query executes without error.
    database.command("sql", "RETURN promql('promql_sql_notime_test')").close();
  }

  // --- Helper methods ---

  private DatabaseInternal getDatabaseInternal() {
    return (DatabaseInternal) database;
  }

  private void createTypeAndInsertData(final String typeName) {
    database.command("sql",
        "CREATE TIMESERIES TYPE " + typeName + " TIMESTAMP ts FIELDS (value DOUBLE)");

    database.transaction(() -> {
      for (int i = 1; i <= 5; i++)
        database.command("sql",
            "INSERT INTO " + typeName + " SET ts = " + (i * 1000) + ", value = " + (i * 10.0));
    });
  }

  private void createTypeWithTags(final String typeName) {
    database.command("sql",
        "CREATE TIMESERIES TYPE " + typeName + " TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE)");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO " + typeName + " SET ts = 1000, host = 'a', value = 10.0");
      database.command("sql", "INSERT INTO " + typeName + " SET ts = 2000, host = 'b', value = 20.0");
      database.command("sql", "INSERT INTO " + typeName + " SET ts = 3000, host = 'a', value = 30.0");
      database.command("sql", "INSERT INTO " + typeName + " SET ts = 4000, host = 'b', value = 40.0");
      database.command("sql", "INSERT INTO " + typeName + " SET ts = 5000, host = 'a', value = 50.0");
    });
  }

  @Nested
  /**
   * Regression tests for issues #8926 (vector matching must ignore {@code __name__}), #8927 (topk/bottomk must rank the
   * ABSENT marker below every real sample), #8928 (round() must stay in double) and #8929 (unary minus vs {@code ^},
   * IEEE-754 division by zero).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  class Issue8926Conformance {

    private static final long EVAL_TIME_MS = 6_000L;

    // ---- #8926

    // #8926
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

    // #8926
    @Test
    void comparisonAndSetOperatorsIgnoreTheMetricName() throws Exception {
      createSeries("m8926_a", new String[] { "api" }, new double[] { 3.0 });
      createSeries("m8926_b", new String[] { "api" }, new double[] { 12.0 });

      assertThat(vectorOf("m8926_a < m8926_b")).hasSize(1);
      assertThat(vectorOf("m8926_a and m8926_b")).hasSize(1);
      // the matching series must be EXCLUDED
      assertThat(vectorOf("m8926_a unless m8926_b")).isEmpty();
    }

    // #8926
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

    // #8926
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

    // #8926
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

    // #8926
    @Test
    void topkAndBottomkHonourGroupingAndAnInvalidKIsRefusedOnAnEmptyVector() throws Exception {
      createSeries("m8927_g", new String[] { "a", "b" }, new double[] { 1.0, 2.0 });

      final List<VectorSample> byHost = vectorOf("topk by (host) (1, m8927_g)");
      assertThat(byHost).hasSize(2);
      assertThat(vectorOf("bottomk without (host) (1, m8927_g)")).hasSize(1);
      assertThat(vectorOf("bottomk without (host) (1, m8927_g)").getFirst().value()).isEqualTo(1.0);
      assertThatThrownBy(() -> vectorOf("topk(0/0, m8927_g{host=\"nobody\"})")).isInstanceOf(IllegalArgumentException.class);
    }

    // #8926
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

    // #8926
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

    // #8926
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

    // #8926
    @Test
    void roundKeepsAnAbsentSampleAbsent() throws Exception {
      createSeries("m8928_r", new String[] { "a" }, new double[] { Double.NaN });
      assertThat(vectorOf("round(m8928_r)").getFirst().value()).isNaN();
    }

    // ---- #8929

    // #8926
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

    // #8926
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

  @Nested
  /**
   * Issue #9644: the PromQL {@code bool} modifier. A comparison with {@code bool} returns {@code 0}/{@code 1} for every
   * sample instead of filtering, and deletes {@code __name__} from the result, while the same comparison without the
   * modifier keeps filtering and keeps the name. {@code bool} on a non-comparison operator is a parse error that names
   * the modifier.
   */
  class Issue9644BoolModifier {

    private static final long EVAL_TIME_MS = 6_000L;

    @BeforeEach
    void createData() {
      database.command("sql", "CREATE TIMESERIES TYPE m9644 TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE)");
      database.transaction(() -> {
        database.command("sql", "INSERT INTO m9644 SET ts = 1000, host = 'h1', value = 1.5");
        database.command("sql", "INSERT INTO m9644 SET ts = 3000, host = 'h1', value = 3.5");
        database.command("sql", "INSERT INTO m9644 SET ts = 5000, host = 'h1', value = 5.5");
      });
    }


    // #9644
    @Test
    void boolFollowedByAParenthesisIsTheModifier() {
      // as in Prometheus, `bool` is the modifier and the parenthesised expression after it is the right operand
      final BinaryExpr paren = (BinaryExpr) new PromQLParser("m9644 > bool(1 + 1)").parse();
      assertThat(paren.returnBool()).isTrue();
      assertThat(paren.right()).isInstanceOf(BinaryExpr.class);
      assertThat(only("m9644 > bool(1 + 1)").value()).isEqualTo(1.0);
    }

    // --- evaluator ---

    // #9644
    @Test
    void chainedAndParenthesisedBoolComparisonsEvaluate() {
      assertThat(only("m9644 > bool 1 > bool 0").value()).isEqualTo(1.0); // (5.5 > 1 = 1) > 0 = 1
      assertThat(only("m9644 > bool 10 > bool 0").value()).isEqualTo(0.0); // (5.5 > 10 = 0) > 0 = 0
      final VectorSample s = only("(m9644 > bool 1) + 1");
      assertThat(s.value()).isEqualTo(2.0);
      assertThat(s.labels()).doesNotContainKey("__name__");
    }

    // #9644
    @Test
    void aBoolComparisonInsideAnAggregationCountsTheTrueSamples() {
      database.command("sql", "CREATE TIMESERIES TYPE a9644 TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE)");
      database.transaction(() -> {
        database.command("sql", "INSERT INTO a9644 SET ts = 5000, host = 'h1', value = 5.0");
        database.command("sql", "INSERT INTO a9644 SET ts = 5000, host = 'h2', value = 0.5");
        database.command("sql", "INSERT INTO a9644 SET ts = 5000, host = 'h3', value = 2.0");
      });
      // sum over 0/1 counts the hosts above the threshold; the filter form would sum their values instead
      final VectorSample sum = only("sum(a9644 > bool 1)");
      assertThat(sum.value()).isEqualTo(2.0);
      assertThat(sum.labels()).isEmpty();
      assertThat(only("count(a9644 > bool 1)").value()).isEqualTo(3.0);

      final InstantVector byHost = evaluate("sum by (host) (a9644 > bool 1)");
      assertThat(byHost.samples()).hasSize(3);
      for (final VectorSample s : byHost.samples()) {
        assertThat(s.labels()).doesNotContainKey("__name__");
        assertThat(s.value()).isEqualTo("h2".equals(s.labels().get("host")) ? 0.0 : 1.0);
      }
    }

    // #9644
    @Test
    void aNaNSampleComparesAsPrometheusDoes() {
      // 5.5 * 0 / 0 = NaN on both sides: NaN == NaN is false, NaN != NaN is true, as in Go
      assertThat(only("(m9644 * 0 / 0) == bool (m9644 * 0 / 0)").value()).isEqualTo(0.0);
      assertThat(only("(m9644 * 0 / 0) != bool (m9644 * 0 / 0)").value()).isEqualTo(1.0);
      assertThat(only("(m9644 * 0 / 0) > bool 1").value()).isEqualTo(0.0);
    }

    // #9644
    @Test
    void vectorVectorWithBoolDropsUnmatchedSeriesRatherThanAnsweringZero() {
      database.command("sql", "CREATE TIMESERIES TYPE n9644 TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE)");
      database.transaction(() -> {
        database.command("sql", "INSERT INTO n9644 SET ts = 5000, host = 'h1', value = 1.0");
        database.command("sql", "INSERT INTO n9644 SET ts = 5000, host = 'h2', value = 1.0");
      });
      // only h1 exists on both sides: h2 of n9644 has no partner in m9644, so it is not in the result at all
      final InstantVector iv = evaluate("n9644 < bool m9644");
      assertThat(iv.samples()).hasSize(1);
      assertThat(iv.samples().getFirst().labels()).containsEntry("host", "h1").doesNotContainKey("__name__");
      assertThat(iv.samples().getFirst().value()).isEqualTo(1.0);
    }

    // #9644
    @Test
    void vectorScalarWithBoolAnswersOneAndDropsTheName() {
      final VectorSample s = only("m9644 > bool 1");
      assertThat(s.value()).isEqualTo(1.0);
      assertThat(s.labels()).doesNotContainKey("__name__").containsEntry("host", "h1");
    }

    // #9644
    @Test
    void vectorScalarWithBoolKeepsAFalseSampleAsZero() {
      final VectorSample s = only("m9644 > bool 10");
      assertThat(s.value()).isEqualTo(0.0);
      assertThat(s.labels()).doesNotContainKey("__name__").containsEntry("host", "h1");
    }

    // #9644
    @Test
    void scalarVectorWithBoolAnswersZeroOrOne() {
      assertThat(only("1 < bool m9644").value()).isEqualTo(1.0);
      final VectorSample s = only("10 < bool m9644");
      assertThat(s.value()).isEqualTo(0.0);
      assertThat(s.labels()).doesNotContainKey("__name__").containsEntry("host", "h1");
    }

    // #9644
    @Test
    void vectorVectorWithBoolAnswersZeroOrOneAndDropsTheName() {
      final VectorSample eq = only("m9644 == bool m9644");
      assertThat(eq.value()).isEqualTo(1.0);
      assertThat(eq.labels()).doesNotContainKey("__name__").containsEntry("host", "h1");

      final VectorSample gt = only("m9644 > bool m9644");
      assertThat(gt.value()).isEqualTo(0.0);
      assertThat(gt.labels()).doesNotContainKey("__name__").containsEntry("host", "h1");
    }

    // #9644
    @Test
    void theComparisonWithoutTheModifierStillFiltersAndKeepsTheName() {
      final VectorSample s = only("m9644 > 1");
      assertThat(s.value()).isEqualTo(5.5);
      assertThat(s.labels()).containsEntry("__name__", "m9644").containsEntry("host", "h1");

      assertThat(evaluate("m9644 > 10").samples()).isEmpty();
      assertThat(evaluate("m9644 > m9644").samples()).isEmpty();
      assertThat(only("m9644 == m9644").labels()).containsEntry("__name__", "m9644");
    }

    // #9644
    @Test
    void aRangeQueryWithBoolAnswersZeroOrOneAtEveryStep() {
      final PromQLResult result = new PromQLEvaluator(getDatabaseInternal()).evaluateRange(new PromQLParser("m9644 > bool 2").parse(),
          1000L, 5000L, 2000L);
      assertThat(result).isInstanceOf(MatrixResult.class);
      final List<MatrixSeries> series = ((MatrixResult) result).series();
      assertThat(series).hasSize(1);
      assertThat(series.getFirst().labels()).doesNotContainKey("__name__").containsEntry("host", "h1");
      // steps at 1000 (1.5 -> 0), 3000 (3.5 -> 1), 5000 (5.5 -> 1): the false step is reported, not filtered
      final List<double[]> values = series.getFirst().values();
      assertThat(values).hasSize(3);
      assertThat(values.get(0)[1]).isEqualTo(0.0);
      assertThat(values.get(1)[1]).isEqualTo(1.0);
      assertThat(values.get(2)[1]).isEqualTo(1.0);
    }

    private DatabaseInternal getDatabaseInternal() {
      return (DatabaseInternal) database;
    }

    private VectorSample only(final String promql) {
      final InstantVector iv = evaluate(promql);
      assertThat(iv.samples()).as(promql).hasSize(1);
      return iv.samples().getFirst();
    }

    private double scalar(final String promql) {
      final PromQLResult result = new PromQLEvaluator(getDatabaseInternal()).evaluateInstant(new PromQLParser(promql).parse(),
          EVAL_TIME_MS);
      assertThat(result).isInstanceOf(ScalarResult.class);
      return ((ScalarResult) result).value();
    }

    private InstantVector evaluate(final String promql) {
      final PromQLResult result = new PromQLEvaluator(getDatabaseInternal()).evaluateInstant(new PromQLParser(promql).parse(),
          EVAL_TIME_MS);
      assertThat(result).isInstanceOf(InstantVector.class);
      return (InstantVector) result;
    }
  }

  @Nested
  /**
   * Issue #9723: Prometheus refuses a comparison between two scalars that does not carry the {@code bool} modifier
   * ("comparisons between scalars must use BOOL modifier"). The evaluator used to answer it with a scalar 0/1, as if
   * {@code bool} had been written. Whether an operand is a scalar is not always syntactic, so the check is made on the
   * evaluated operands and must hold for every expression shape that yields a scalar, on every entry point.
   */
  class Issue9723ScalarComparisonWithoutBool {

    private static final long   EVAL_TIME_MS = 6_000L;
    private static final String MESSAGE      = "comparisons between scalars must use BOOL modifier";

    @BeforeEach
    void createData() {
      database.command("sql", "CREATE TIMESERIES TYPE m9723 TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE)");
      database.transaction(() -> database.command("sql", "INSERT INTO m9723 SET ts = 5000, host = 'h1', value = 5.5"));
    }

    // #9723
    @Test
    void everyComparisonOperatorBetweenTwoScalarsWithoutBoolIsRefused() {
      for (final String op : new String[] { "==", "!=", "<", ">", "<=", ">=" })
        assertThatThrownBy(() -> instant("1 " + op + " 0")).as(op).isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining(MESSAGE);
    }

    // #9723
    @Test
    void aScalarThatIsNotALiteralIsRefusedToo() {
      for (final String promql : new String[] { "(1) > 0", "-1 < 0", "-(1) > 0", "1 + 1 == 2", "(1 > bool 0) == 1",
          "(2 * 0.7) >= (0.2 + 0)" })
        assertThatThrownBy(() -> instant(promql)).as(promql).isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining(MESSAGE);
    }

    // #9723
    @Test
    void aNestedRefusalIsNotMaskedByAnOuterVectorOperand() {
      assertThatThrownBy(() -> instant("m9723 + (1 > 0)")).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining(MESSAGE);
    }

    // #9723
    @Test
    void aRangeQueryIsRefusedToo() {
      assertThatThrownBy(() -> new PromQLEvaluator(db()).evaluateRange(new PromQLParser("1 > 0").parse(), 1000L, 5000L, 2000L))
          .isInstanceOf(IllegalArgumentException.class).hasMessageContaining(MESSAGE);
    }

    // #9723
    @Test
    void theSqlFunctionIsRefusedToo() {
      assertThatThrownBy(() -> {
        try (final ResultSet rs = database.command("sql", "RETURN promql('1 > 0', 6000)")) {
          rs.hasNext();
        }
      }).isInstanceOf(IllegalArgumentException.class).hasMessageContaining(MESSAGE);
    }

    // #9723
    @Test
    void theSameComparisonWithBoolStillAnswersZeroOrOne() {
      assertThat(scalar("2 > bool 1")).isEqualTo(1.0);
      assertThat(scalar("1 > bool 2")).isEqualTo(0.0);
      assertThat(scalar("-(-1) == bool 1")).isEqualTo(1.0);
    }

    // #9723
    @Test
    void arithmeticBetweenScalarsIsUnaffected() {
      assertThat(scalar("1 + 2")).isEqualTo(3.0);
      assertThat(scalar("2 ^ 3")).isEqualTo(8.0);
    }

    // #9723
    @Test
    void aComparisonWithAVectorOperandStillFiltersWithoutBool() {
      final InstantVector left = (InstantVector) instant("m9723 > 1");
      assertThat(left.samples()).hasSize(1);
      final VectorSample s = left.samples().getFirst();
      assertThat(s.value()).isEqualTo(5.5);
      assertThat(s.labels()).containsEntry("__name__", "m9723");

      assertThat(((InstantVector) instant("1 < m9723")).samples()).hasSize(1);
      assertThat(((InstantVector) instant("m9723 > 10")).samples()).isEmpty();
    }

    private DatabaseInternal db() {
      return (DatabaseInternal) database;
    }

    private PromQLResult instant(final String promql) {
      return new PromQLEvaluator(db()).evaluateInstant(new PromQLParser(promql).parse(), EVAL_TIME_MS);
    }

    private double scalar(final String promql) {
      final PromQLResult result = instant(promql);
      assertThat(result).isInstanceOf(ScalarResult.class);
      return ((ScalarResult) result).value();
    }
  }

  @Nested
  /**
   * Issue #9743: Prometheus refuses a set operator ({@code and}, {@code or}, {@code unless}) with a scalar operand
   * ("set operator \"and\" not allowed in binary scalar expression"). The evaluator used to route it through the arithmetic
   * branches, where the set operators map to NaN: {@code 1 and 2} answered a scalar NaN and {@code m and 1} answered every
   * series of {@code m} with value NaN. Whether an operand is a scalar is not always syntactic, so the check is made on the
   * evaluated operands and must hold for every operand shape and every entry point.
   */
  class Issue9743SetOperatorWithScalar {

    private static final long     EVAL_TIME_MS = 6_000L;
    private static final String[] SET_OPS      = { "and", "or", "unless" };

    @BeforeEach
    void createData() {
      database.command("sql", "CREATE TIMESERIES TYPE m9743 TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE)");
      database.transaction(() -> database.command("sql", "INSERT INTO m9743 SET ts = 5000, host = 'h1', value = 5.5"));
    }

    // #9743
    @Test
    void aSetOperatorBetweenTwoScalarsIsRefused() {
      for (final String op : SET_OPS)
        assertRefused("1 " + op + " 2", op);
    }

    // #9743
    @Test
    void aSetOperatorWithAScalarOnTheRightIsRefused() {
      for (final String op : SET_OPS)
        assertRefused("m9743 " + op + " 1", op);
    }

    // #9743
    @Test
    void aSetOperatorWithAScalarOnTheLeftIsRefused() {
      for (final String op : SET_OPS)
        assertRefused("1 " + op + " m9743", op);
    }

    // #9743
    @Test
    void aScalarThatIsNotALiteralIsRefusedToo() {
      assertRefused("m9743 and -(1)", "and");
      assertRefused("(1 + 1) or m9743", "or");
      assertRefused("m9743 unless (1 > bool 0)", "unless");
    }

    // #9743
    @Test
    void aVectorWithNoSamplesIsStillAVector() {
      // the refusal is about the operand type, not about there being samples to combine
      assertRefused("missing_metric_9743 and 1", "and");
    }

    // #9743
    @Test
    void aNestedRefusalIsNotMaskedByAnOuterOperator() {
      assertRefused("m9743 + (m9743 and 1)", "and");
    }

    // #9743
    @Test
    void aRangeQueryIsRefusedToo() {
      assertThatThrownBy(() -> new PromQLEvaluator(db()).evaluateRange(new PromQLParser("m9743 or 1").parse(), 1000L, 5000L, 2000L))
          .isInstanceOf(IllegalArgumentException.class).hasMessageContaining(message("or"));
    }

    // #9743
    @Test
    void theSqlFunctionIsRefusedToo() {
      assertThatThrownBy(() -> {
        try (final ResultSet rs = database.command("sql", "RETURN promql('1 unless 2', 6000)")) {
          rs.hasNext();
        }
      }).isInstanceOf(IllegalArgumentException.class).hasMessageContaining(message("unless"));
    }

    // #9743
    @Test
    void aSetOperatorBetweenTwoVectorsStillWorks() {
      // a set operator passes the left-hand sample through unchanged: value and metric name both survive
      for (final String promql : new String[] { "m9743 and m9743", "m9743 or m9743" }) {
        final List<VectorSample> samples = ((InstantVector) instant(promql)).samples();
        assertThat(samples).as(promql).hasSize(1);
        assertThat(samples.getFirst().value()).as(promql).isEqualTo(5.5);
        assertThat(samples.getFirst().labels()).as(promql).containsEntry("__name__", "m9743").containsEntry("host", "h1");
      }
      assertThat(((InstantVector) instant("m9743 unless m9743")).samples()).isEmpty();
    }

    private void assertRefused(final String promql, final String op) {
      assertThatThrownBy(() -> instant(promql)).as(promql).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining(message(op));
    }

    private static String message(final String op) {
      return "set operator \"" + op + "\" not allowed in binary scalar expression";
    }

    private DatabaseInternal db() {
      return (DatabaseInternal) database;
    }

    private PromQLResult instant(final String promql) {
      return new PromQLEvaluator(db()).evaluateInstant(new PromQLParser(promql).parse(), EVAL_TIME_MS);
    }
  }

  @Nested
  /**
   * Issue #9788: Prometheus refuses a binary operator whose operand is neither a scalar nor an instant vector ("binary
   * expression must contain only scalar and instant vector types"). The evaluator used to fall through to its last line and
   * answer a scalar NaN for a range-vector operand ({@code m[5m] + 1}, {@code m[5m] and m}), and to treat a string literal
   * operand as a NaN scalar ({@code "a" + 1}). The unary minus had the same gap: {@code -m[5m]} returned the range vector
   * unchanged, silently dropping the sign.
   */
  class Issue9788BinaryOperandType {

    private static final long   EVAL_TIME_MS   = 6_000L;
    private static final String BINARY_MESSAGE = "binary expression must contain only scalar and instant vector types";
    private static final String UNARY_MESSAGE  = "unary expression only allowed on expressions of type scalar or instant vector";

    @BeforeEach
    void createData() {
      database.command("sql", "CREATE TIMESERIES TYPE m9788 TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE)");
      database.transaction(() -> database.command("sql", "INSERT INTO m9788 SET ts = 5000, host = 'h1', value = 5.5"));
    }

    // #9788
    @Test
    void anArithmeticOperatorWithARangeVectorOperandIsRefused() {
      for (final String op : new String[] { "+", "-", "*", "/", "%", "^" }) {
        assertRefused("m9788[5m] " + op + " 1", BINARY_MESSAGE);
        assertRefused("1 " + op + " m9788[5m]", BINARY_MESSAGE);
        assertRefused("m9788[5m] " + op + " m9788", BINARY_MESSAGE);
        assertRefused("m9788 " + op + " m9788[5m]", BINARY_MESSAGE);
        assertRefused("m9788[5m] " + op + " m9788[5m]", BINARY_MESSAGE);
      }
    }

    // #9788
    @Test
    void aComparisonWithARangeVectorOperandIsRefused() {
      assertRefused("m9788[5m] > 1", BINARY_MESSAGE);
      assertRefused("m9788[5m] > bool 1", BINARY_MESSAGE);
      assertRefused("m9788 == m9788[5m]", BINARY_MESSAGE);
    }

    // #9788
    @Test
    void aSetOperatorWithARangeVectorOperandIsRefused() {
      for (final String op : new String[] { "and", "or", "unless" }) {
        assertRefused("m9788[5m] " + op + " m9788", BINARY_MESSAGE);
        assertRefused("m9788 " + op + " m9788[5m]", BINARY_MESSAGE);
        // the operand type is checked before the scalar set-operator rule, like Prometheus
        assertRefused("m9788[5m] " + op + " 1", BINARY_MESSAGE);
      }
    }

    // #9788
    @Test
    void aRangeVectorWithNoSeriesIsStillARangeVector() {
      // the refusal is about the operand type, not about there being samples to combine
      assertRefused("missing_metric_9788[5m] + 1", BINARY_MESSAGE);
      assertRefused("missing_metric_9788[5m] and missing_metric_9788", BINARY_MESSAGE);
    }

    // #9788
    @Test
    void aParenthesizedOrOffsetRangeVectorIsRefusedToo() {
      assertRefused("(m9788[5m]) + 1", BINARY_MESSAGE);
      assertRefused("m9788[5m] offset 1m * 2", BINARY_MESSAGE);
    }

    // #9788
    @Test
    void aStringLiteralOperandIsRefused() {
      assertRefused("\"a\" + 1", BINARY_MESSAGE);
      assertRefused("1 + \"a\"", BINARY_MESSAGE);
      assertRefused("m9788 * 'a'", BINARY_MESSAGE);
      assertRefused("(\"a\") + 1", BINARY_MESSAGE);
      assertRefused("m9788 and \"a\"", BINARY_MESSAGE);
    }

    // #9788
    @Test
    void aNestedRefusalIsNotMaskedByAnOuterOperator() {
      assertRefused("m9788 + (m9788[5m] + 1)", BINARY_MESSAGE);
      assertRefused("sum(m9788[5m] * 2)", BINARY_MESSAGE);
    }

    // #9788
    @Test
    void aUnaryMinusOnARangeVectorOrAStringIsRefused() {
      assertRefused("-m9788[5m]", UNARY_MESSAGE);
      assertRefused("-missing_metric_9788[5m]", UNARY_MESSAGE);
      assertRefused("-\"a\"", UNARY_MESSAGE);
    }

    // #9788
    @Test
    void aRangeQueryIsRefusedToo() {
      assertThatThrownBy(
          () -> new PromQLEvaluator(db()).evaluateRange(new PromQLParser("m9788[5m] + 1").parse(), 1000L, 5000L, 2000L))
          .isInstanceOf(IllegalArgumentException.class).hasMessageContaining(BINARY_MESSAGE);
    }

    // #9788
    @Test
    void theSqlFunctionIsRefusedToo() {
      assertThatThrownBy(() -> {
        try (final ResultSet rs = database.command("sql", "RETURN promql('m9788[5m] + 1', 6000)")) {
          rs.hasNext();
        }
      }).isInstanceOf(IllegalArgumentException.class).hasMessageContaining(BINARY_MESSAGE);
    }

    // #9788
    @Test
    void validOperandsStillWork() {
      assertThat(((ScalarResult) instant("1 + 2")).value()).isEqualTo(3.0);
      assertThat(((ScalarResult) instant("-2")).value()).isEqualTo(-2.0);

      final List<VectorSample> plusOne = ((InstantVector) instant("m9788 + 1")).samples();
      assertThat(plusOne).hasSize(1);
      assertThat(plusOne.getFirst().value()).isEqualTo(6.5);

      final List<VectorSample> negated = ((InstantVector) instant("-m9788")).samples();
      assertThat(negated).hasSize(1);
      assertThat(negated.getFirst().value()).isEqualTo(-5.5);

      // a range vector consumed by a range function is an instant vector, a legal operand
      final List<VectorSample> summed = ((InstantVector) instant("sum_over_time(m9788[5m]) * 2")).samples();
      assertThat(summed).hasSize(1);
      assertThat(summed.getFirst().value()).isEqualTo(11.0);
    }

    private void assertRefused(final String promql, final String message) {
      assertThatThrownBy(() -> instant(promql)).as(promql).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining(message);
    }

    private DatabaseInternal db() {
      return (DatabaseInternal) database;
    }

    private PromQLResult instant(final String promql) {
      return new PromQLEvaluator(db()).evaluateInstant(new PromQLParser(promql).parse(), EVAL_TIME_MS);
    }
  }

  @Nested
  /**
   * Issue #9883: Prometheus refuses an aggregation whose inner expression is not an instant vector ("expected type instant
   * vector in aggregation expression, got range vector") and a math function whose argument is a range vector or a string
   * ("expected type instant vector in call to function \"abs\", got range vector"). The evaluator answered an empty vector
   * for {@code sum(m[5m])} and {@code sum(1)}, returned the range vector unchanged for {@code abs(m[5m])}, treated
   * {@code abs("a")} as a NaN scalar and silently replaced a non-scalar {@code round} precision with 1.
   */
  class Issue9883FunctionArgumentType {

    private static final long     EVAL_TIME_MS = 6_000L;
    private static final String[] AGGREGATIONS = { "sum", "avg", "min", "max", "count", "topk(1, %s)", "bottomk(1, %s)" };
    private static final String[] MATH_FNS     = { "abs", "ceil", "floor", "round" };

    @BeforeEach
    void createData() {
      database.command("sql", "CREATE TIMESERIES TYPE m9883 TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE)");
      database.transaction(() -> {
        database.command("sql", "INSERT INTO m9883 SET ts = 5000, host = 'h1', value = 5.4");
        database.command("sql", "INSERT INTO m9883 SET ts = 5000, host = 'h2', value = -2.6");
      });
    }

    // #9883
    @Test
    void anAggregationOverARangeVectorIsRefused() {
      for (final String agg : AGGREGATIONS) {
        assertRefused(aggregate(agg, "m9883[5m]"), "expected type instant vector in aggregation expression, got range vector");
        // no series at all is still a range vector
        assertRefused(aggregate(agg, "missing_9883[5m]"), "expected type instant vector in aggregation expression, got range vector");
      }
      assertRefused("sum by (host) (m9883[5m])", "expected type instant vector in aggregation expression, got range vector");
      assertRefused("sum without (host) (m9883[5m] offset 1m)", "expected type instant vector in aggregation expression, got range vector");
    }

    // #9883
    @Test
    void anAggregationOverAScalarOrAStringIsRefused() {
      for (final String agg : AGGREGATIONS) {
        assertRefused(aggregate(agg, "1"), "expected type instant vector in aggregation expression, got scalar");
        assertRefused(aggregate(agg, "-(1)"), "expected type instant vector in aggregation expression, got scalar");
        assertRefused(aggregate(agg, "\"a\""), "expected type instant vector in aggregation expression, got string");
        assertRefused(aggregate(agg, "(\"a\")"), "expected type instant vector in aggregation expression, got string");
      }
    }

    // #9883
    @Test
    void theInnerTypeIsCheckedBeforeTheRankingParameter() {
      // Prometheus' type checker looks at the inner expression before the parameter
      assertRefused("topk(m9883, m9883[5m])", "expected type instant vector in aggregation expression, got range vector");
    }

    // #9883
    @Test
    void aMathFunctionOverARangeVectorIsRefused() {
      for (final String fn : MATH_FNS) {
        assertRefused(fn + "(m9883[5m])", "expected type instant vector in call to function \"" + fn + "\", got range vector");
        assertRefused(fn + "(missing_9883[5m])", "expected type instant vector in call to function \"" + fn + "\", got range vector");
      }
      assertRefused("round(m9883[5m], 0.5)", "expected type instant vector in call to function \"round\", got range vector");
    }

    // #9883
    @Test
    void aMathFunctionOverAStringIsRefused() {
      for (final String fn : MATH_FNS) {
        assertRefused(fn + "(\"a\")", "expected type instant vector in call to function \"" + fn + "\", got string");
        assertRefused(fn + "('a')", "expected type instant vector in call to function \"" + fn + "\", got string");
        assertRefused(fn + "((\"a\"))", "expected type instant vector in call to function \"" + fn + "\", got string");
      }
    }

    // #9883
    @Test
    void aNonScalarRoundPrecisionIsRefused() {
      assertRefused("round(m9883, m9883)", "expected type scalar in call to function \"round\", got instant vector");
      assertRefused("round(m9883, m9883[5m])", "expected type scalar in call to function \"round\", got range vector");
      assertRefused("round(m9883, \"a\")", "expected type scalar in call to function \"round\", got string");
      // both arguments wrong: the first one is reported, like Prometheus
      assertRefused("round(m9883[5m], \"a\")", "expected type instant vector in call to function \"round\", got range vector");
    }

    // #9883
    @Test
    void aWrongArgumentCountIsRefused() {
      for (final String fn : new String[] { "abs", "ceil", "floor" }) {
        assertRefused(fn + "()", "expected 1 argument(s) in call to \"" + fn + "\", got 0");
        assertRefused(fn + "(m9883, 1)", "expected 1 argument(s) in call to \"" + fn + "\", got 2");
      }
      assertRefused("round()", "expected at least 1 argument(s) in call to \"round\", got 0");
      assertRefused("round(m9883, 1, 2)", "expected at most 2 argument(s) in call to \"round\", got 3");
      assertRefused("rate()", "expected 1 argument(s) in call to \"rate\", got 0");
      assertRefused("rate(m9883[5m], 1)", "expected 1 argument(s) in call to \"rate\", got 2");
    }

    // #9883
    @Test
    void aRangeFunctionOverANonRangeVectorUsesPrometheusWording() {
      assertRefused("rate(m9883)", "expected type range vector in call to function \"rate\", got instant vector");
      assertRefused("sum_over_time(1)", "expected type range vector in call to function \"sum_over_time\", got scalar");
      assertRefused("increase(\"a\")", "expected type range vector in call to function \"increase\", got string");
    }

    // #9883
    @Test
    void aNestedRefusalIsNotMaskedByAnOuterExpression() {
      assertRefused("sum(abs(m9883[5m]))", "expected type instant vector in call to function \"abs\", got range vector");
      assertRefused("abs(sum(m9883[5m]))", "expected type instant vector in aggregation expression, got range vector");
      assertRefused("m9883 + sum(m9883[5m])", "expected type instant vector in aggregation expression, got range vector");
    }

    // #9883
    @Test
    void aRangeQueryIsRefusedToo() {
      assertThatThrownBy(() -> range("sum(m9883[5m])"))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("expected type instant vector in aggregation expression, got range vector");
      assertThatThrownBy(() -> range("abs(m9883[5m])"))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("expected type instant vector in call to function \"abs\", got range vector");
    }

    // #9883
    @Test
    void theSqlFunctionIsRefusedToo() {
      assertThatThrownBy(() -> {
        try (final ResultSet rs = database.command("sql", "RETURN promql('abs(m9883[5m])', 6000)")) {
          rs.hasNext();
        }
      }).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("expected type instant vector in call to function \"abs\", got range vector");
    }

    // #9883
    @Test
    void validArgumentsStillWork() {
      assertThat(((InstantVector) instant("sum(m9883)")).samples().getFirst().value()).isCloseTo(2.8, offset(1e-9));
      assertThat(((InstantVector) instant("sum(sum_over_time(m9883[5m]))")).samples()).hasSize(1);
      assertThat(((InstantVector) instant("topk(1, m9883)")).samples().getFirst().value()).isEqualTo(5.4);
      assertThat(((InstantVector) instant("sum(missing_9883)")).samples()).isEmpty();

      final List<VectorSample> abs = ((InstantVector) instant("abs(m9883)")).samples();
      assertThat(abs).extracting(VectorSample::value).containsExactlyInAnyOrder(5.4, 2.6);
      assertThat(((InstantVector) instant("round(m9883, 0.5)")).samples()).extracting(VectorSample::value)
          .containsExactlyInAnyOrder(5.5, -2.5);
      assertThat(((InstantVector) instant("round(m9883, 2 * 0.25)")).samples()).extracting(VectorSample::value)
          .containsExactlyInAnyOrder(5.5, -2.5);
    }

    /**
     * Wraps {@code inner} in an aggregation: a plain operator name ({@code sum}) becomes {@code sum(inner)}, a template with a
     * {@code %s} placeholder ({@code topk(1, %s)}) gets {@code inner} substituted for it.
     */
    private static String aggregate(final String agg, final String inner) {
      return agg.contains("%s") ? String.format(agg, inner) : agg + "(" + inner + ")";
    }

    private void assertRefused(final String promql, final String message) {
      assertThatThrownBy(() -> instant(promql)).as(promql).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining(message);
    }

    private DatabaseInternal db() {
      return (DatabaseInternal) database;
    }

    private PromQLResult instant(final String promql) {
      return new PromQLEvaluator(db()).evaluateInstant(new PromQLParser(promql).parse(), EVAL_TIME_MS);
    }

    private PromQLResult range(final String promql) {
      return new PromQLEvaluator(db()).evaluateRange(new PromQLParser(promql).parse(), 1000L, 5000L, 2000L);
    }
  }

  @Nested
  /**
   * Issue #9924: Prometheus refuses a scalar argument to {@code abs}, {@code ceil}, {@code floor} and {@code round}
   * ("expected type instant vector in call to function \"abs\", got scalar"). The evaluator answered {@code abs(-1)} with
   * the scalar 1. Whether an argument is a scalar is not always syntactic, so the check is made on the evaluated argument
   * and must hold for every expression shape that yields a scalar, on every entry point.
   */
  class Issue9924MathFunctionScalarArgument {

    private static final long     EVAL_TIME_MS = 6_000L;
    private static final String[] MATH_FNS     = { "abs", "ceil", "floor", "round" };
    private static final String[] SCALARS      = { "-1", "1.4", "(1)", "-(2)", "1 + 1", "2 * 0.25", "(1 > bool 0)" };

    @BeforeEach
    void createData() {
      database.command("sql", "CREATE TIMESERIES TYPE m9924 TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE)");
      database.transaction(() -> {
        database.command("sql", "INSERT INTO m9924 SET ts = 5000, host = 'h1', value = 5.4");
        database.command("sql", "INSERT INTO m9924 SET ts = 5000, host = 'h2', value = -2.6");
      });
    }

    // #9924
    @Test
    void everyScalarShapeIsRefusedByEveryMathFunction() {
      for (final String fn : MATH_FNS)
        for (final String scalar : SCALARS)
          assertRefused(fn + "(" + scalar + ")", message(fn));
    }

    // #9924
    @Test
    void roundWithAScalarFirstArgumentIsRefusedOnTheFirstArgument() {
      // the precision is valid, the first argument is not
      assertRefused("round(7.3, 0.5)", message("round"));
      // both are wrong: the first one is reported, like Prometheus
      assertRefused("round(1, m9924)", message("round"));
    }

    // #9924
    @Test
    void aNestedRefusalIsNotMaskedByAnOuterExpression() {
      assertRefused("sum(abs(-1))", message("abs"));
      assertRefused("m9924 + abs(-1)", message("abs"));
      assertRefused("abs(-1) > bool 0", message("abs"));
      assertRefused("abs(ceil(-1))", message("ceil"));
      assertRefused("topk(floor(1.5), m9924)", message("floor"));
      assertRefused("round(m9924, abs(-1))", message("abs"));
    }

    // #9924
    @Test
    void aRangeQueryIsRefusedToo() {
      assertThatThrownBy(() -> new PromQLEvaluator(db()).evaluateRange(new PromQLParser("abs(-1)").parse(), 1000L, 5000L, 2000L))
          .isInstanceOf(IllegalArgumentException.class).hasMessageContaining(message("abs"));
    }

    // #9924
    @Test
    void theSqlFunctionIsRefusedToo() {
      assertThatThrownBy(() -> {
        try (final ResultSet rs = database.command("sql", "RETURN promql('ceil(0.2)', 6000)")) {
          rs.hasNext();
        }
      }).isInstanceOf(IllegalArgumentException.class).hasMessageContaining(message("ceil"));
    }

    // #9924
    @Test
    void anInstantVectorArgumentStillWorks() {
      assertThat(((InstantVector) instant("abs(m9924)")).samples()).extracting(VectorSample::value)
          .containsExactlyInAnyOrder(5.4, 2.6);
      assertThat(((InstantVector) instant("ceil(m9924)")).samples()).extracting(VectorSample::value)
          .containsExactlyInAnyOrder(6.0, -2.0);
      assertThat(((InstantVector) instant("floor(m9924)")).samples()).extracting(VectorSample::value)
          .containsExactlyInAnyOrder(5.0, -3.0);
      assertThat(((InstantVector) instant("round(m9924, 0.5)")).samples()).extracting(VectorSample::value)
          .containsExactlyInAnyOrder(5.5, -2.5);
      // a vector built from arithmetic with a scalar is still a vector
      assertThat(((InstantVector) instant("abs(m9924 * -1)")).samples()).extracting(VectorSample::value)
          .containsExactlyInAnyOrder(5.4, 2.6);
      // a metric with no series is an empty instant vector, not a scalar
      assertThat(((InstantVector) instant("abs(missing_9924)")).samples()).isEmpty();
    }

    private static String message(final String fn) {
      return "expected type instant vector in call to function \"" + fn + "\", got scalar";
    }

    private void assertRefused(final String promql, final String message) {
      assertThatThrownBy(() -> instant(promql)).as(promql).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining(message);
    }

    private DatabaseInternal db() {
      return (DatabaseInternal) database;
    }

    private PromQLResult instant(final String promql) {
      return new PromQLEvaluator(db()).evaluateInstant(new PromQLParser(promql).parse(), EVAL_TIME_MS);
    }
  }

  @Nested
  /**
   * Issue #9922: a range query keeps only the instant vectors and scalars its steps evaluate to, so a top-level range vector
   * ({@code m[5m]}) or string ({@code "a"}) fell through and answered an empty matrix. Prometheus refuses both with
   * {@code invalid expression type "range vector" for range query, must be Scalar or instant Vector}.
   */
  class Issue9922RangeQueryExpressionType {

    private static final String RANGE_VECTOR = "invalid expression type \"range vector\" for range query, must be Scalar or instant Vector";
    private static final String STRING       = "invalid expression type \"string\" for range query, must be Scalar or instant Vector";

    @BeforeEach
    void createData() {
      database.command("sql", "CREATE TIMESERIES TYPE m9922 TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE)");
      database.transaction(() -> {
        database.command("sql", "INSERT INTO m9922 SET ts = 1000, host = 'h1', value = 1.0");
        database.command("sql", "INSERT INTO m9922 SET ts = 3000, host = 'h1', value = 3.0");
      });
    }

    // #9922
    @Test
    void aTopLevelRangeVectorIsRefused() {
      assertRefused("m9922[5m]", RANGE_VECTOR);
      assertRefused("m9922[5m] offset 1m", RANGE_VECTOR);
      assertRefused("(m9922[5m])", RANGE_VECTOR);
      // no series at all is still a range vector, and must not answer an empty matrix either
      assertRefused("missing_9922[5m]", RANGE_VECTOR);
    }

    // #9922
    @Test
    void aTopLevelStringIsRefused() {
      assertRefused("\"a\"", STRING);
      assertRefused("'a'", STRING);
      assertRefused("(\"a\")", STRING);
    }

    // #9922
    @Test
    void anInstantQueryStillAcceptsBoth() {
      // Prometheus answers an instant query over a range vector or a string, only the range query refuses them
      final PromQLEvaluator evaluator = new PromQLEvaluator(db());
      assertThat(evaluator.evaluateInstant(new PromQLParser("m9922[5m]").parse(), 5000L)).isInstanceOf(RangeVector.class);
      assertThat(evaluator.evaluateInstant(new PromQLParser("\"a\"").parse(), 5000L)).isInstanceOf(ScalarResult.class);
      // and a missing metric is an (empty) instant vector, which is why its range query above answers an empty matrix
      assertThat(evaluator.evaluateInstant(new PromQLParser("missing_9922").parse(), 5000L)).isInstanceOf(InstantVector.class);
    }

    // #9922
    @Test
    void validRangeQueriesStillWork() {
      final MatrixResult vector = range("m9922");
      assertThat(vector.series()).hasSize(1);
      assertThat(vector.series().getFirst().values()).isNotEmpty();

      final MatrixResult scalar = range("1 + 1");
      assertThat(scalar.series()).hasSize(1);
      assertThat(scalar.series().getFirst().values()).hasSize(3);
      assertThat(scalar.series().getFirst().values().getFirst()[1]).isEqualTo(2.0);

      assertThat(range("sum_over_time(m9922[5m])").series()).hasSize(1);
      // an instant vector with no series is still an instant vector: an empty matrix, not a refusal
      assertThat(range("missing_9922").series()).isEmpty();
    }

    private void assertRefused(final String promql, final String message) {
      assertThatThrownBy(() -> range(promql)).as(promql).isInstanceOf(IllegalArgumentException.class).hasMessage(message);
    }

    private DatabaseInternal db() {
      return (DatabaseInternal) database;
    }

    private MatrixResult range(final String promql) {
      return (MatrixResult) new PromQLEvaluator(db()).evaluateRange(new PromQLParser(promql).parse(), 1000L, 5000L, 2000L);
    }
  }

  @Nested
  /**
   * Regression test for issue #7039 (follow-up on #4716): {@code min_over_time}/{@code max_over_time} and the
   * instant-vector MIN/MAX aggregation arms kept the {@code ±Infinity} seed the rest of the time-series stack had
   * already dropped, so an all-NaN window returned the sentinel as data.
   * <p>
   * {@code <} and {@code >} skip NaN implicitly, so an all-NaN input never displaced the seed. Prometheus - the
   * protocol being emulated - answers NaN here, and so does {@code TimeSeriesVectorOps.min} over the same samples:
   * the two paths disagreed with each other on identical data, which is what made this more than cosmetic.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  class Issue7039MinMaxNaN {

    private static final long EVAL_TIME_MS = 6_000L;

    // #7039
    @Test
    void minAndMaxOverTimeReportAnAllNaNWindowAsAbsent() throws Exception {
      createSeriesWithValues("promql7039_allnan", Double.NaN, Double.NaN);

      // Was +Infinity / -Infinity.
      assertThat(singleValueOf("min_over_time(promql7039_allnan[5m])")).as("min_over_time over an all-NaN window")
          .isNaN();
      assertThat(singleValueOf("max_over_time(promql7039_allnan[5m])")).as("max_over_time over an all-NaN window")
          .isNaN();
    }

    // #7039
    @Test
    void theMinAndMaxAggregationArmsReportAnAllNaNGroupAsAbsent() throws Exception {
      createSeriesWithValues("promql7039_agg", Double.NaN, Double.NaN);

      assertThat(singleValueOf("min(promql7039_agg)")).as("min() over an all-NaN group").isNaN();
      assertThat(singleValueOf("max(promql7039_agg)")).as("max() over an all-NaN group").isNaN();
    }

    // #7039
    @Test
    void aRealValueAmongTheNaNSamplesIsStillTheAnswer() throws Exception {
      createSeriesWithValues("promql7039_mixed", Double.NaN, 7.5);

      assertThat(singleValueOf("min_over_time(promql7039_mixed[5m])")).isEqualTo(7.5);
      assertThat(singleValueOf("max_over_time(promql7039_mixed[5m])")).isEqualTo(7.5);
    }

    /**
     * A genuine ±Infinity sample is a measurement, not the absent marker: it must survive the fold.
     */
    // #7039
    @Test
    void aRealInfinitySampleIsPreserved() throws Exception {
      createSeriesWithValues("promql7039_inf", Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);

      assertThat(singleValueOf("min_over_time(promql7039_inf[5m])")).isEqualTo(Double.NEGATIVE_INFINITY);
      assertThat(singleValueOf("max_over_time(promql7039_inf[5m])")).isEqualTo(Double.POSITIVE_INFINITY);
    }

    private void createSeriesWithValues(final String typeName, final double first, final double second)
        throws Exception {
      database.command("sql",
          "CREATE TIMESERIES TYPE " + typeName + " TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 1");

      // A NaN value has no SQL literal, so the samples go in through the engine - the same write path the SQL
      // INSERT reaches, and the one the issue's repro uses.
      final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType(typeName);
      database.begin();
      tsType.getEngine().appendSamples(new long[] { 1_000L, 2_000L },
          new Object[] { "h1", "h1" }, new Object[] { first, second });
      database.commit();
    }

    private double singleValueOf(final String promql) {
      final PromQLExpr expr = new PromQLParser(promql).parse();
      final PromQLResult result = new PromQLEvaluator((DatabaseInternal) database).evaluateInstant(expr, EVAL_TIME_MS);
      assertThat(result).isInstanceOf(InstantVector.class);

      final InstantVector iv = (InstantVector) result;
      assertThat(iv.samples()).as("the query must produce exactly one sample: %s", promql).hasSize(1);
      final VectorSample sample = iv.samples().getFirst();
      return sample.value();
    }
  }

  @Nested
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
  class Issue6938Semantics {

    private static final long EVAL_TIME_MS = 6_000L;

    // --- 1. `or` on a matched label set -------------------------------------------------------------------

    // #6938
    @Test
    void orKeepsTheLeftHandSampleWhenBothSidesCollapseToTheEmptyLabelSet() {
      createTaggedType("promql6938_or_a", 10.0, 20.0);
      createTaggedType("promql6938_or_b", 100.0, 300.0);

      // sum() collapses labels to {} on both sides, so the two samples always match: the matched branch used to
      // fall through to applyBinaryOp(OR, ...) = NaN.
      final InstantVector iv = evaluateInstant("sum(promql6938_or_a) or sum(promql6938_or_b)");

      assertThat(iv.samples()).hasSize(1);
      final VectorSample sample = iv.samples().getFirst();
      assertThat(sample.value()).isNotNaN();
      assertThat(sample.value()).isEqualTo(30.0); // 10 + 20 from the left side, never the right side's 400
      assertThat(sample.labels()).isEmpty();
    }

    // #6938
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

    // #6938
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

    // #6938
    @Test
    void anEqualityMatcherOnAnAbsentColumnMatchesNoSeries() {
      createTaggedType("promql6938_absent_eq", 10.0, 20.0);

      // "jobb" is a typo for a label this type does not declare: Prometheus matches nothing, the old code
      // dropped the matcher entirely and returned every series.
      assertThat(evaluateInstant("promql6938_absent_eq{jobb=\"api\"}").samples()).isEmpty();
    }

    // #6938
    @Test
    void anInequalityMatcherOnAnAbsentColumnMatchesEverySeries() {
      createTaggedType("promql6938_absent_neq", 10.0, 20.0);

      // An absent label reads as "" for every series, so != with a non-empty value matches all of them.
      assertThat(evaluateInstant("promql6938_absent_neq{jobb!=\"api\"}").samples()).hasSize(2);
    }

    // #6938
    @Test
    void anEmptyValueMatcherOnAnAbsentColumnFollowsPrometheusAbsentLabelRules() {
      createTaggedType("promql6938_absent_empty", 10.0, 20.0);

      // ="" selects series that do not have the label -> all of them.
      assertThat(evaluateInstant("promql6938_absent_empty{jobb=\"\"}").samples()).hasSize(2);
      // !="" selects series that DO have the label -> none of them.
      assertThat(evaluateInstant("promql6938_absent_empty{jobb!=\"\"}").samples()).isEmpty();
    }

    // #6938
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

    // #6938
    @Test
    void matchersOnAColumnTheTypeDoesDeclareAreUnaffected() {
      createTaggedType("promql6938_present", 10.0, 20.0);

      assertThat(byHost(evaluateInstant("promql6938_present{host=\"h1\"}"))).containsOnlyKeys("h1");
      assertThat(byHost(evaluateInstant("promql6938_present{host!=\"h1\"}"))).containsOnlyKeys("h2");
      assertThat(byHost(evaluateInstant("promql6938_present{host=~\"h.*\"}"))).containsOnlyKeys("h1", "h2");
      assertThat(evaluateInstant("promql6938_present{host!~\"h.*\"}").samples()).isEmpty();
    }

    // #6938
    @Test
    void anAbsentColumnMatcherAlsoGovernsARangeSelector() {
      createTaggedType("promql6938_absent_matrix", 10.0, 20.0);

      // Same rule on the matrix-selector path (rate() over a range vector), not just the instant one.
      assertThat(evaluateInstant("count_over_time(promql6938_absent_matrix{jobb=\"api\"}[5m])").samples()).isEmpty();
      assertThat(evaluateInstant("count_over_time(promql6938_absent_matrix{jobb!=\"api\"}[5m])").samples()).hasSize(2);
    }

    // #6938
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

    // #6938
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

      final List<double[]> points = series.getFirst().values();
      assertThat(points).hasSize(5);
      for (int i = 0; i < points.size(); i++)
        assertThat(points.get(i)[0]).as("point %d must sit on the step grid", i).isEqualTo(1000.0 + i * 1000.0);

      // The values still come from the lookback resolution: 1.0 carried forward until ts=5000 lands.
      assertThat(points.get(0)[1]).isEqualTo(1.0);
      assertThat(points.get(3)[1]).isEqualTo(1.0);
      assertThat(points.get(4)[1]).isEqualTo(5.0);
    }

    // #6938
    @Test
    void rangePointsOfAnAggregationAreAlsoStepAligned() {
      createTaggedType("promql6938_grid_agg", 10.0, 20.0);

      final PromQLResult result = new PromQLEvaluator(getDatabaseInternal())
          .evaluateRange(new PromQLParser("sum(promql6938_grid_agg)").parse(), 2000L, 6000L, 2000L);

      final List<MatrixSeries> series = ((MatrixResult) result).series();
      assertThat(series).hasSize(1);
      final List<double[]> points = series.getFirst().values();
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

  @Nested
  /**
   * Issue #9833: only the {@code =} arm of a PromQL label matcher read the matcher value in the tag's declared type. The other three
   * compared the stored value's {@code toString()} with the matcher text, so on any tag whose canonical text differs from what the
   * user wrote, {@code {t="X"}} and {@code {t!="X"}} both selected the series and {@code {t=~"X"}} selected none.
   * <p>
   * The tests pin the claim rather than the cells: for every declared tag type, {@code =} and {@code !=} on the same value select
   * complementary sets, {@code =~} with the value as a literal pattern agrees with {@code =} and {@code !~} with {@code !=}. Each
   * case runs against the instant-vector selector and the range-vector selector, the two scans of the evaluator, and against the
   * mutable bucket and the sealed store, the two layers a row can come from.
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  class Issue9833TypedTagMatchers {

    private static final long EVAL_TIME_MS = 6_000L;

    private static int typeCounter;

    static Stream<Arguments> typedTags() {
      return Stream.of(
          Arguments.of("LONG", "7", "007"),
          Arguments.of("LONG", "7", "+7"),
          Arguments.of("LONG", "7", "7"),
          Arguments.of("INTEGER", "7", "007"),
          Arguments.of("SHORT", "7", "007"),
          Arguments.of("BYTE", "7", "007"),
          Arguments.of("DOUBLE", "5", "5"),
          Arguments.of("DOUBLE", "5", "5.00"),
          Arguments.of("FLOAT", "5", "5"),
          Arguments.of("DATETIME", "'2024-01-01 00:00:00'", "2024-01-01 00:00:00"),
          Arguments.of("DATE", "'2024-01-01'", "2024-01-01"),
          Arguments.of("STRING", "'007'", "007"),
          Arguments.of("BOOLEAN", "true", "true"));
    }

    // #9833
    @ParameterizedTest(name = "{0} {1} matched by \"{2}\"")
    @MethodSource("typedTags")
    void theFourOperatorsAgreeOnAValueOfTheDeclaredType(final String declaredType, final String stored, final String matcherValue) {
      final String type = createType(declaredType, stored);
      assertOperatorsAgree(type, matcherValue, true);

      compact(type);
      assertOperatorsAgree(type, matcherValue, true);
    }

    // #9833
    @ParameterizedTest(name = "{0} {1} matched by \"{2}\"")
    @MethodSource("typedTags")
    void theFourOperatorsAgreeOnADifferentValue(final String declaredType, final String stored, final String matcherValue) {
      // a second series whose tag is NOT the matcher's value: = must not select it, != must, the patterns likewise
      final String type = createType(declaredType, stored);
      final String other = switch (declaredType) {
        case "DATETIME" -> "2025-06-01 00:00:00";
        case "DATE" -> "2025-06-01";
        case "BOOLEAN" -> "false";
        case "STRING" -> "7";
        default -> "8";
      };
      assertOperatorsAgree(type, other, false);

      compact(type);
      assertOperatorsAgree(type, other, false);
    }

    // #9833
    @Test
    void aRealPatternStillMatchesTheTagAsItIsReported() {
      // the label a typed tag is reported under is its canonical text: a pattern is matched against that text, as before
      final String type = createType("LONG", "7");
      assertThat(count(type + "{code=~\"7|8\"}")).isEqualTo(1);
      assertThat(count(type + "{code=~\"[0-9]\"}")).isEqualTo(1);
      assertThat(count(type + "{code!~\"7|8\"}")).isZero();
      assertThat(count(type + "{code=~\"8|9\"}")).isZero();
      assertThat(count(type + "{code!~\"8|9\"}")).isEqualTo(1);
      // a real pattern is not read as a value: "0.7" matches neither the text "7" nor names the LONG 7
      assertThat(count(type + "{code=~\"0.7\"}")).isZero();
      assertThat(count(type + "{code!~\"0.7\"}")).isEqualTo(1);
      assertThat(count(type + "{code=~\"0+7\"}")).isZero();

      // an unescaped pattern that reads as a value of the declared type names that value too, as = does
      final String doubleType = createType("DOUBLE", "5");
      assertThat(count(doubleType + "{code=~\"5.00\"}")).isEqualTo(1);
      assertThat(count(doubleType + "{code!~\"5.00\"}")).isZero();
    }

    // #9833
    @Test
    void textTheDeclaredTypeCannotReadIsNoValueOfTheTag() {
      final String type = createType("BOOLEAN", "false");
      // "yes" is no boolean: it must not be read as false, on any arm
      assertThat(count(type + "{code=\"yes\"}")).isZero();
      assertThat(count(type + "{code!=\"yes\"}")).isEqualTo(1);
      assertThat(count(type + "{code=~\"yes\"}")).isZero();
      assertThat(count(type + "{code!~\"yes\"}")).isEqualTo(1);
    }

    // #9833
    @Test
    void aDateTimeMatcherIsReadAsTheInsertReadItWhateverTheJvmTimeZone() {
      // The INSERT reads the text through the database; the matcher used to be read with no database, in the JVM's default
      // zone, and so named another instant whenever that zone is not the database's. CI runs in UTC, where the two coincide:
      // a zone with an offset on the stored date is forced for the duration of the test so the difference shows anywhere.
      final TimeZone jvmZone = TimeZone.getDefault();
      try {
        TimeZone.setDefault(TimeZone.getTimeZone(jvmZone.getRawOffset() == 9 * 3_600_000 ? "America/New_York" : "Asia/Tokyo"));
        final String type = createType("DATETIME", "'2024-01-01 00:00:00'");
        assertOperatorsAgree(type, "2024-01-01 00:00:00", true);
      } finally {
        TimeZone.setDefault(jvmZone);
      }
    }

    // #9833
    @Test
    void aPatternIsReadAsALiteralOnlyWhenItHasNoUnescapedMetacharacter() {
      assertThat(PromQLEvaluator.literalOfPattern("007")).isEqualTo("007");
      assertThat(PromQLEvaluator.literalOfPattern("5\\.00")).isEqualTo("5.00");
      assertThat(PromQLEvaluator.literalOfPattern("\\+7")).isEqualTo("+7");
      assertThat(PromQLEvaluator.literalOfPattern("a\\\\b")).as("an escaped backslash").isEqualTo("a\\b");
      assertThat(PromQLEvaluator.literalOfPattern("2024-01-01 00:00:00")).as("- is no metacharacter").isEqualTo("2024-01-01 00:00:00");
      assertThat(PromQLEvaluator.literalOfPattern("")).isEmpty();

      assertThat(PromQLEvaluator.literalOfPattern("5.00")).as("an unescaped dot").isNull();
      assertThat(PromQLEvaluator.literalOfPattern("7|8")).isNull();
      assertThat(PromQLEvaluator.literalOfPattern("0+7")).isNull();
      assertThat(PromQLEvaluator.literalOfPattern("\\d")).as("an escaped letter is a character class").isNull();
      assertThat(PromQLEvaluator.literalOfPattern("\\1")).as("an escaped digit is a back reference").isNull();
      assertThat(PromQLEvaluator.literalOfPattern("7\\")).as("a trailing backslash").isNull();
    }

    private void assertOperatorsAgree(final String type, final String value, final boolean expectedEquality) {
      for (final boolean range : new boolean[] { false, true }) {
        final int eq = count(selector(type, "=", value), range);
        final int neq = count(selector(type, "!=", value), range);
        final int re = count(selector(type, "=~", value), range);
        final int nre = count(selector(type, "!~", value), range);
        final String scan = range ? "range selector" : "instant selector";

        assertThat(eq).as("%s: = on %s", scan, value).isEqualTo(expectedEquality ? 1 : 0);
        assertThat(eq + neq).as("%s: = and != on %s must be complements", scan, value).isEqualTo(1);
        assertThat(re).as("%s: =~ with the literal %s must agree with =", scan, value).isEqualTo(eq);
        assertThat(nre).as("%s: !~ with the literal %s must agree with !=", scan, value).isEqualTo(neq);
      }
    }

    private static String selector(final String type, final String op, final String value) {
      // a literal pattern: escape what PromQL's string syntax and the regex engine would read as syntax
      final String pattern = op.contains("~") ? value.replace(".", "\\\\.").replace("+", "\\\\+") : value;
      return type + "{code" + op + "\"" + pattern + "\"}";
    }

    private String createType(final String declaredType, final String stored) {
      final String type = "promql9833_" + declaredType.toLowerCase() + "_" + (++typeCounter);
      database.command("sql", "CREATE TIMESERIES TYPE " + type + " TIMESTAMP ts TAGS (code " + declaredType + ") FIELDS (value DOUBLE)");
      database.transaction(() -> database.command("sql", "INSERT INTO " + type + " SET ts = 1000, code = " + stored + ", value = 1.0"));
      return type;
    }

    private void compact(final String type) {
      try {
        ((LocalTimeSeriesType) database.getSchema().getType(type)).getEngine().compactAll();
      } catch (final Exception e) {
        throw new AssertionError("compaction of " + type + " failed", e);
      }
    }

    private int count(final String selector) {
      return count(selector, false);
    }

    /** The series {@code selector} selects: as an instant vector, or through {@code count_over_time} over a range vector. */
    private int count(final String selector, final boolean range) {
      final PromQLExpr expr = new PromQLParser(range ? "count_over_time(" + selector + "[10s])" : selector).parse();
      final PromQLResult result = new PromQLEvaluator((DatabaseInternal) database).evaluateInstant(expr, EVAL_TIME_MS);
      assertThat(result).isInstanceOf(InstantVector.class);
      int samples = 0;
      for (final VectorSample ignored : ((InstantVector) result).samples())
        samples++;
      return samples;
    }
  }

  @Nested
  /**
   * Issue #9801: Prometheus selects a window LEFT-OPEN, {@code (t - window, t]}. A sample exactly one lookback old is
   * stale for an instant selector, and {@code [5m]} excludes the sample at {@code t-5m}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  class Issue9801LeftOpenWindow {
    private static final long T        = 1_700_000_000_000L;
    private static final long LOOKBACK = 300_000L;

    @BeforeEach
    void createData() {
      database.command("sql", "CREATE TIMESERIES TYPE m1 TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE)");
      database.transaction(() -> database.command("sql", "INSERT INTO m1 SET ts = ?, host = 'h1', value = 42.0", T));
    }

    // #9801
    @Test
    void aSampleExactlyOneLookbackOldIsStale() {
      assertThat(instant("m1", T + LOOKBACK)).isEmpty();
      assertThat(instant("m1", T + LOOKBACK - 1)).hasSize(1);
      assertThat(instant("m1", T + LOOKBACK + 1)).isEmpty();
      assertThat(instant("m1", T)).hasSize(1);
    }

    // #9801
    @Test
    void theOffsetShiftsBothEndsOfTheWindow() {
      assertThat(instant("m1 offset 1m", T + LOOKBACK + 60_000L)).isEmpty();
      assertThat(instant("m1 offset 1m", T + LOOKBACK + 59_999L)).hasSize(1);
    }

    // #9801
    @Test
    void aRangeSelectorExcludesTheSampleOnItsLowerBound() {
      assertThat(instant("count_over_time(m1[5m])", T + LOOKBACK)).isEmpty();
      assertThat(instant("count_over_time(m1[5m])", T + LOOKBACK - 1)).hasSize(1);

      final RangeVector range = (RangeVector) new PromQLEvaluator((DatabaseInternal) database).evaluateInstant(
          new PromQLParser("m1[5m]").parse(), T + LOOKBACK - 1);
      final RangeSeries series = range.series().getFirst();
      // The window itself still starts at t-5m: only which samples fall in it changed
      assertThat(series.rangeStartMs()).isEqualTo(T - 1);
      assertThat(series.rangeEndMs()).isEqualTo(T + LOOKBACK - 1);
    }

    // #9801
    @Test
    void aScrapeIntervalThatDividesTheRangeCountsItsSamplesOnce() {
      // A 1m feed over [5m]: 6 samples from T to T+5m, evaluated at T+5m, Prometheus 3.x counts the last 5
      database.transaction(() -> {
        for (int i = 1; i <= 5; i++)
          database.command("sql", "INSERT INTO m1 SET ts = ?, host = 'h1', value = 42.0", T + i * 60_000L);
      });
      final List<VectorSample> samples = instant("count_over_time(m1[5m])", T + LOOKBACK);
      assertThat(samples).hasSize(1);
      assertThat(samples.getFirst().value()).isEqualTo(5.0);
    }

    // #9801
    @Test
    void theSqlFunctionAgrees() {
      try (final ResultSet rs = database.query("sql", "SELECT promql('m1', ?) AS r", T + LOOKBACK)) {
        assertThat((List<Map<String, Object>>) rs.next().getProperty("r")).isEmpty();
      }
      try (final ResultSet rs = database.query("sql", "SELECT promql('m1', ?) AS r", T + LOOKBACK - 1)) {
        assertThat((List<Map<String, Object>>) rs.next().getProperty("r")).hasSize(1);
      }
    }

    private List<VectorSample> instant(final String promql, final long evalTimeMs) {
      final PromQLResult result = new PromQLEvaluator((DatabaseInternal) database).evaluateInstant(
          new PromQLParser(promql).parse(), evalTimeMs);
      return ((InstantVector) result).samples();
    }
  }

  @Nested
  /**
   * Issue #7696: {@code PromQLEvaluator} read its selectors through {@code TimeSeriesEngine#iterateQuery}, whose
   * own javadoc says the sealed layer materialises every matching row of the range before the caller sees the
   * first one - so an instant or range selector over a wide lookback held the whole window in heap to produce an
   * answer that is only O(series).
   * <p>
   * The fix folds rows straight from {@link com.arcadedb.engine.timeseries.TimeSeriesEngine#forEachRow} instead,
   * which is the same swap {@code TagFilterTest} pins for the label-discovery endpoints. That swap
   * is not a drop-in replacement here the way it is there, because {@code forEachRow} gives up something
   * {@code iterateQuery} used to guarantee for free: rows arrive shard by shard, NOT merged by timestamp, and
   * {@code TimeSeriesEngine#appendBatch} round-robins samples across shards regardless of their tags - so a
   * SINGLE label combination's samples are scattered across every shard, and the order they reach the visitor is
   * no longer chronological. Two things in {@code PromQLEvaluator} silently depended on that chronological order:
   * <ul>
   *   <li>{@code evaluateVectorSelector}'s "keep the latest sample" fold used to just overwrite on every row,
   *   which only picks out the true latest because {@code iterateQuery} visited rows oldest-to-newest. Folding
   *   the same way over {@code forEachRow} would keep whichever row happened to be visited LAST across all
   *   shards, not the one with the greatest timestamp.</li>
   *   <li>{@code evaluateMatrixSelector}'s per-series point list is read by {@code PromQLFunctions.rate}/
   *   {@code irate}/{@code increase}, which index its first, last and consecutive elements assuming ascending
   *   time order. A list built by appending in scan order would hand them the wrong pair.</li>
   * </ul>
   * These tests are built so the naive swap - fold in scan order, keep overwriting, don't re-sort - gives an
   * observably WRONG answer, not just a differently-shaped correct one: every sample below shares one label
   * combination, spread over 2 shards by a single {@code appendBatch} call, with the true latest / most recent
   * pair placed so the shard-by-shard scan visits it before its own last row.
   * <p>
   * A third case pins a design decision the swap also had to get right: {@code excludesEverySeries}'s contract -
   * that a malformed or ReDoS-shaped regex matcher must propagate rather than read as an empty result - has to
   * keep holding once the row-level post-filter that raises it runs INSIDE the {@code forEachRow} visitor, on the
   * same call stack as the {@code IOException} the scan itself can throw. A catch that is too broad there would
   * silently turn a rejected query into an empty one.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  class Issue7696ForEachRowOrdering {

    // #7696
    @Test
    void theInstantVectorPicksTheTrueLatestSampleNotTheLastOneVisited() throws IOException {
      // index -> shard (round robin, base 0): 0,2,4 -> shard 0; 1,3 -> shard 1. The true latest sample (ts=4000)
      // lands on shard 0, which forEachRow visits BEFORE shard 1 - so the last row the scan visits overall is
      // ts=3000 on shard 1, not the true latest. A fold that just overwrites on every row would report THAT one.
      database.command("sql",
          "CREATE TIMESERIES TYPE Latest7696 TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 2");
      final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType("Latest7696");

      database.begin();
      tsType.getEngine().appendBatch(new long[] { 0L, 1_000L, 2_000L, 3_000L, 4_000L },
          new Object[][] {
              { "h1", "h1", "h1", "h1", "h1" },
              { 100.0, 101.0, 102.0, 103.0, 104.0 }
          });
      database.commit();

      final PromQLExpr expr = new PromQLParser("Latest7696{host=\"h1\"}").parse();
      final PromQLResult result = new PromQLEvaluator(getDatabaseInternal()).evaluateInstant(expr, 5_000L);

      assertThat(result).isInstanceOf(InstantVector.class);
      final InstantVector iv = (InstantVector) result;
      assertThat(iv.samples()).hasSize(1);
      final VectorSample sample = iv.samples().getFirst();
      assertThat(sample.timestampMs()).as("the true latest timestamp").isEqualTo(4_000L);
      assertThat(sample.value()).as("the value paired with the true latest sample, not the last one scanned")
          .isEqualTo(104.0);
    }

    // #7696
    @Test
    void theMatrixSelectorRestoresTimestampOrderAcrossShardsForIrate() throws IOException {
      // Same round-robin placement as above. irate() reads the LAST TWO points of the series in chronological
      // order. In scan order (shard 0 fully, then shard 1) the last two rows visited are ts=3000/v=0 and
      // ts=5000/v=100 - a 2-second gap. In true chronological order they are ts=4000/v=0 and ts=5000/v=100 - a
      // 1-second gap. Both give a diff of 100, so the two orderings produce two different, unambiguous rates
      // (50 vs 100) rather than coincidentally agreeing.
      database.command("sql",
          "CREATE TIMESERIES TYPE Irate7696 TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 2");
      final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType("Irate7696");

      database.begin();
      tsType.getEngine().appendBatch(new long[] { 0L, 1_000L, 2_000L, 3_000L, 4_000L, 5_000L },
          new Object[][] {
              { "h1", "h1", "h1", "h1", "h1", "h1" },
              { 0.0, 0.0, 0.0, 0.0, 0.0, 100.0 }
          });
      database.commit();

      final PromQLExpr expr = new PromQLParser("irate(Irate7696{host=\"h1\"}[6s])").parse();
      final PromQLResult result = new PromQLEvaluator(getDatabaseInternal()).evaluateInstant(expr, 5_000L);

      assertThat(result).isInstanceOf(InstantVector.class);
      final InstantVector iv = (InstantVector) result;
      assertThat(iv.samples()).hasSize(1);
      assertThat(iv.samples().getFirst().value())
          .as("the 1-second, chronologically-correct gap, not the 2-second scan-order gap")
          .isEqualTo(100.0);
    }

    // #7696
    @Test
    void aMalformedRegexOnAColumnTheTypeDoesDeclareStillPropagatesRatherThanReadingAsEmpty() {
      // matchesPostFilters() - which validates =~/!~ patterns via compilePattern() - now runs INSIDE the
      // forEachRow visitor, on the same call stack as the scan's own IOException. The catch around that scan
      // must stay narrow enough that this still throws instead of being read as an empty vector.
      database.command("sql",
          "CREATE TIMESERIES TYPE BadRegex7696 TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE)");
      database.command("sql", "INSERT INTO BadRegex7696 SET ts = 1000, host = 'h1', value = 1.0");

      assertThatThrownBy(() -> new PromQLEvaluator(getDatabaseInternal())
          .evaluateInstant(new PromQLParser("BadRegex7696{host=~\"(a+)+\"}").parse(), 5_000L))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("ReDoS");
    }

    private DatabaseInternal getDatabaseInternal() {
      return (DatabaseInternal) database;
    }
  }

  @Nested
  /**
   * Issues #9330 and #9363, on the PromQL evaluator:
   * <ul>
   * <li>#9330: a unary minus and every value-transforming function must delete {@code __name__} from the result labels,
   * as Prometheus does, so a derived series is not labelled with the name of the raw one.</li>
   * <li>#9363: a label whose value is the empty string is the same as an absent label, so it is not reported and
   * {@code {host=""}} selects it.</li>
   * </ul>
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  class Issue9330DropMetricName {

    private static final long EVAL_TIME_MS = 6_000L;

    @BeforeEach
    void createData() {
      database.command("sql", "CREATE TIMESERIES TYPE m9330 TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE)");
      database.transaction(() -> {
        database.command("sql", "INSERT INTO m9330 SET ts = 1000, host = 'h1', value = 1.5");
        database.command("sql", "INSERT INTO m9330 SET ts = 3000, host = 'h1', value = 3.5");
        database.command("sql", "INSERT INTO m9330 SET ts = 5000, host = 'h1', value = 5.5");
      });
    }

    // #9330
    @Test
    void aSelectorKeepsTheName() {
      assertThat(only("m9330").labels()).containsEntry("__name__", "m9330").containsEntry("host", "h1");
    }

    // #9330
    @Test
    void unaryMinusDropsTheName() {
      final VectorSample s = only("-m9330");
      assertThat(s.labels()).doesNotContainKey("__name__").containsEntry("host", "h1");
      assertThat(s.value()).isEqualTo(-5.5);
    }

    // #9330
    @Test
    void transformingFunctionsDropTheName() {
      for (final String fn : new String[] { "abs", "ceil", "floor", "round" })
        assertThat(only(fn + "(m9330)").labels()).as(fn).doesNotContainKey("__name__").containsEntry("host", "h1");

      for (final String fn : new String[] { "rate", "irate", "increase", "sum_over_time", "avg_over_time", "min_over_time",
          "max_over_time", "count_over_time" })
        assertThat(only(fn + "(m9330[10s])").labels()).as(fn).doesNotContainKey("__name__").containsEntry("host", "h1");
    }

    // #9330
    @Test
    void aRangeQueryDropsTheNameToo() {
      final PromQLResult result = new PromQLEvaluator(getDatabaseInternal()).evaluateRange(new PromQLParser("-m9330").parse(), 1000L, 5000L,
          2000L);
      assertThat(result).isInstanceOf(MatrixResult.class);
      final List<MatrixSeries> series = ((MatrixResult) result).series();
      assertThat(series).isNotEmpty();
      for (final MatrixSeries s : series)
        assertThat(s.labels()).doesNotContainKey("__name__").containsEntry("host", "h1");
    }

    // #9330
    @Test
    void anEmptyLabelValueIsAnAbsentLabel() {
      database.command("sql", "CREATE TIMESERIES TYPE e9363 TIMESTAMP ts TAGS (host STRING, zone STRING) FIELDS (value DOUBLE)");
      database.transaction(() -> {
        database.command("sql", "INSERT INTO e9363 SET ts = 1000, host = '', zone = 'z1', value = 1.0");
        database.command("sql", "INSERT INTO e9363 SET ts = 1000, host = 'h2', zone = 'z1', value = 2.0");
      });

      final InstantVector all = evaluate("e9363");
      assertThat(all.samples()).hasSize(2);
      for (final VectorSample s : all.samples())
        if (s.value() == 1.0)
          assertThat(s.labels()).as("an empty label is not reported").doesNotContainKey("host");

      final InstantVector empty = evaluate("e9363{host=\"\"}");
      assertThat(empty.samples()).hasSize(1);
      assertThat(empty.samples().getFirst().value()).isEqualTo(1.0);

      // an absent label is "" for the regex matchers too
      assertThat(evaluate("e9363{host=~\".*\"}").samples()).hasSize(2);
      assertThat(evaluate("e9363{host=~\"\"}").samples()).hasSize(1);
      assertThat(evaluate("e9363{host!~\"h.*\"}").samples()).hasSize(1);
      assertThat(evaluate("e9363{host!~\".+\"}").samples()).hasSize(1);

      final InstantVector present = evaluate("e9363{host!=\"\"}");
      assertThat(present.samples()).hasSize(1);
      assertThat(present.samples().getFirst().value()).isEqualTo(2.0);
    }

    private DatabaseInternal getDatabaseInternal() {
      return (DatabaseInternal) database;
    }

    private VectorSample only(final String promql) {
      final InstantVector iv = evaluate(promql);
      assertThat(iv.samples()).hasSize(1);
      return iv.samples().getFirst();
    }

    private InstantVector evaluate(final String promql) {
      final PromQLResult result = new PromQLEvaluator(getDatabaseInternal()).evaluateInstant(new PromQLParser(promql).parse(),
          EVAL_TIME_MS);
      assertThat(result).isInstanceOf(InstantVector.class);
      return (InstantVector) result;
    }
  }
}
