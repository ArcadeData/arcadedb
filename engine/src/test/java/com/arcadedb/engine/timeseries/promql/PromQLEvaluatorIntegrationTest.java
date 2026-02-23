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
