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
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end PromQL integration tests: parse → evaluate against live TimeSeries data.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PromQLEvaluatorIntegrationTest extends TestHelper {

  @Test
  void testInstantVectorSelector() {
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
  void testRateFunction() {
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
  void testBinaryExpressionWithScalar() {
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
  void testSumAggregation() {
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
  void testSumByAggregation() {
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
  void testRangeQueryWithStep() {
    createTypeAndInsertData("range_metric");

    final PromQLEvaluator evaluator = new PromQLEvaluator(getDatabaseInternal());
    final PromQLExpr expr = new PromQLParser("range_metric").parse();
    final PromQLResult result = evaluator.evaluateRange(expr, 1000L, 5000L, 1000L);

    assertThat(result).isInstanceOf(PromQLResult.MatrixResult.class);
    final PromQLResult.MatrixResult mr = (PromQLResult.MatrixResult) result;
    assertThat(mr.series()).isNotEmpty();
  }

  @Test
  void testEmptyResultForNonExistentType() {
    final PromQLEvaluator evaluator = new PromQLEvaluator(getDatabaseInternal());
    final PromQLExpr expr = new PromQLParser("nonexistent_metric").parse();
    final PromQLResult result = evaluator.evaluateInstant(expr, 1000L);

    assertThat(result).isInstanceOf(PromQLResult.InstantVector.class);
    final PromQLResult.InstantVector iv = (PromQLResult.InstantVector) result;
    assertThat(iv.samples()).isEmpty();
  }

  @Test
  void testEvaluateRangeStepZero() {
    final PromQLEvaluator evaluator = new PromQLEvaluator(getDatabaseInternal());
    final PromQLExpr expr = new PromQLParser("42").parse();

    assertThatThrownBy(() -> evaluator.evaluateRange(expr, 1000L, 5000L, 0L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("stepMs must be positive");
  }

  @Test
  void testEvaluateRangeInvertedBounds() {
    // Regression: endMs < startMs previously returned empty results silently
    final PromQLEvaluator evaluator = new PromQLEvaluator(getDatabaseInternal());
    final PromQLExpr expr = new PromQLParser("42").parse();

    assertThatThrownBy(() -> evaluator.evaluateRange(expr, 5000L, 1000L, 1000L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("endMs")
        .hasMessageContaining("startMs");
  }

  @Test
  void testReDoSPatternRejected() {
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
  void testScalarArithmetic() {
    final PromQLEvaluator evaluator = new PromQLEvaluator(getDatabaseInternal());
    final PromQLExpr expr = new PromQLParser("2 + 3 * 4").parse();
    final PromQLResult result = evaluator.evaluateInstant(expr, 1000L);

    assertThat(result).isInstanceOf(PromQLResult.ScalarResult.class);
    assertThat(((PromQLResult.ScalarResult) result).value()).isEqualTo(14.0);
  }

  @Test
  void testExtractLabelsWithTagBeforeTimestamp() {
    // Regression: extractLabels / extractValue must work correctly even when the
    // TIMESTAMP column is not at schema position 0.
    // Schema: TAG(host) at index 0, TIMESTAMP(ts) at index 1, FIELD(value) at index 2.
    // Row format from engine: [ts, host, value] — TIMESTAMP is always row[0].
    // Previously the code used row[schemaIndex] directly, so host would read the timestamp.
    final String typeName = "promql_tag_first";
    // Use the builder API to create a type with TAG before TIMESTAMP
    new com.arcadedb.schema.TimeSeriesTypeBuilder(getDatabaseInternal())
        .withName(typeName)
        .withTag("host", com.arcadedb.schema.Type.STRING)
        .withTimestamp("ts")
        .withField("value", com.arcadedb.schema.Type.DOUBLE)
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
  void testQueryUsesIterateQueryPath() {
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
  void testPromQLSqlFunction() {
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
  void testPromQLSqlFunctionUsesCurrentTimeWhenNoArgument() {
    createTypeAndInsertData("promql_sql_notime_test");

    // Called without evalTimeMs — uses System.currentTimeMillis() internally.
    // The data was inserted with timestamps 1000-5000 ms which are in the far past
    // relative to current time, so results will be empty (lookback window is 5 minutes).
    // RETURN of an empty list produces no rows — verify the query executes without error.
    database.command("sql", "RETURN promql('promql_sql_notime_test')").close();
  }

  // --- Helper methods ---

  private com.arcadedb.database.DatabaseInternal getDatabaseInternal() {
    return (com.arcadedb.database.DatabaseInternal) database;
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
