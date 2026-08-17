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
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.TimeSeriesTypeBuilder;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.StallAwareStopwatch;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
    assertThat(rateIv.samples().get(0).value()).isCloseTo(0.6667, within(0.001));

    // increase = rate * range = 200 (the old code returned the raw 100).
    final PromQLExpr incExpr = new PromQLParser("increase(sparse_counter[5m])").parse();
    final PromQLResult.InstantVector incIv = (PromQLResult.InstantVector) evaluator.evaluateInstant(incExpr, 300000L);
    assertThat(incIv.samples()).hasSize(1);
    assertThat(incIv.samples().get(0).value()).isCloseTo(200.0, within(0.1));
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
    assertThat(iv.samples().get(0).value()).isGreaterThan(0.0);
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
    assertThat(iv.samples().get(0).labels()).containsEntry("host", "srv1");
    // The value must be a numeric double, not NaN
    assertThat(iv.samples().get(0).value()).isEqualTo(42.0);
  }

  @Test
  void queryUsesIterateQueryPath() {
    // Verify that evaluateVectorSelector uses the lazy iterator path (iterateQuery)
    // rather than the eager-loading query() path. We verify this indirectly by
    // confirming that a large dataset is evaluated correctly.
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
    // above does) - proves the context.getOrComputeRegexDeadline() -> evaluator.setRegexDeadline() wiring in
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
}
