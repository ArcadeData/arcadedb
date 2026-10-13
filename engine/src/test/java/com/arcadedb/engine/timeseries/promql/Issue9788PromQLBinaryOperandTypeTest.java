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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9788: Prometheus refuses a binary operator whose operand is neither a scalar nor an instant vector ("binary
 * expression must contain only scalar and instant vector types"). The evaluator used to fall through to its last line and
 * answer a scalar NaN for a range-vector operand ({@code m[5m] + 1}, {@code m[5m] and m}), and to treat a string literal
 * operand as a NaN scalar ({@code "a" + 1}). The unary minus had the same gap: {@code -m[5m]} returned the range vector
 * unchanged, silently dropping the sign.
 */
class Issue9788PromQLBinaryOperandTypeTest extends TestHelper {

  private static final long   EVAL_TIME_MS   = 6_000L;
  private static final String BINARY_MESSAGE = "binary expression must contain only scalar and instant vector types";
  private static final String UNARY_MESSAGE  = "unary expression only allowed on expressions of type scalar or instant vector";

  @BeforeEach
  void createData() {
    database.command("sql", "CREATE TIMESERIES TYPE m9788 TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE)");
    database.transaction(() -> database.command("sql", "INSERT INTO m9788 SET ts = 5000, host = 'h1', value = 5.5"));
  }

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

  @Test
  void aComparisonWithARangeVectorOperandIsRefused() {
    assertRefused("m9788[5m] > 1", BINARY_MESSAGE);
    assertRefused("m9788[5m] > bool 1", BINARY_MESSAGE);
    assertRefused("m9788 == m9788[5m]", BINARY_MESSAGE);
  }

  @Test
  void aSetOperatorWithARangeVectorOperandIsRefused() {
    for (final String op : new String[] { "and", "or", "unless" }) {
      assertRefused("m9788[5m] " + op + " m9788", BINARY_MESSAGE);
      assertRefused("m9788 " + op + " m9788[5m]", BINARY_MESSAGE);
      // the operand type is checked before the scalar set-operator rule, like Prometheus
      assertRefused("m9788[5m] " + op + " 1", BINARY_MESSAGE);
    }
  }

  @Test
  void aRangeVectorWithNoSeriesIsStillARangeVector() {
    // the refusal is about the operand type, not about there being samples to combine
    assertRefused("missing_metric_9788[5m] + 1", BINARY_MESSAGE);
    assertRefused("missing_metric_9788[5m] and missing_metric_9788", BINARY_MESSAGE);
  }

  @Test
  void aParenthesizedOrOffsetRangeVectorIsRefusedToo() {
    assertRefused("(m9788[5m]) + 1", BINARY_MESSAGE);
    assertRefused("m9788[5m] offset 1m * 2", BINARY_MESSAGE);
  }

  @Test
  void aStringLiteralOperandIsRefused() {
    assertRefused("\"a\" + 1", BINARY_MESSAGE);
    assertRefused("1 + \"a\"", BINARY_MESSAGE);
    assertRefused("m9788 * 'a'", BINARY_MESSAGE);
    assertRefused("(\"a\") + 1", BINARY_MESSAGE);
    assertRefused("m9788 and \"a\"", BINARY_MESSAGE);
  }

  @Test
  void aNestedRefusalIsNotMaskedByAnOuterOperator() {
    assertRefused("m9788 + (m9788[5m] + 1)", BINARY_MESSAGE);
    assertRefused("sum(m9788[5m] * 2)", BINARY_MESSAGE);
  }

  @Test
  void aUnaryMinusOnARangeVectorOrAStringIsRefused() {
    assertRefused("-m9788[5m]", UNARY_MESSAGE);
    assertRefused("-missing_metric_9788[5m]", UNARY_MESSAGE);
    assertRefused("-\"a\"", UNARY_MESSAGE);
  }

  @Test
  void aRangeQueryIsRefusedToo() {
    assertThatThrownBy(
        () -> new PromQLEvaluator(db()).evaluateRange(new PromQLParser("m9788[5m] + 1").parse(), 1000L, 5000L, 2000L))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining(BINARY_MESSAGE);
  }

  @Test
  void theSqlFunctionIsRefusedToo() {
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.command("sql", "RETURN promql('m9788[5m] + 1', 6000)")) {
        rs.hasNext();
      }
    }).isInstanceOf(IllegalArgumentException.class).hasMessageContaining(BINARY_MESSAGE);
  }

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
