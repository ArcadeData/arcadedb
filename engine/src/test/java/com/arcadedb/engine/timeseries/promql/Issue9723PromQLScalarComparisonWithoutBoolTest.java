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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9723: Prometheus refuses a comparison between two scalars that does not carry the {@code bool} modifier
 * ("comparisons between scalars must use BOOL modifier"). The evaluator used to answer it with a scalar 0/1, as if
 * {@code bool} had been written. Whether an operand is a scalar is not always syntactic, so the check is made on the
 * evaluated operands and must hold for every expression shape that yields a scalar, on every entry point.
 */
class Issue9723PromQLScalarComparisonWithoutBoolTest extends TestHelper {

  private static final long   EVAL_TIME_MS = 6_000L;
  private static final String MESSAGE      = "comparisons between scalars must use BOOL modifier";

  @BeforeEach
  void createData() {
    database.command("sql", "CREATE TIMESERIES TYPE m9723 TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE)");
    database.transaction(() -> database.command("sql", "INSERT INTO m9723 SET ts = 5000, host = 'h1', value = 5.5"));
  }

  @Test
  void everyComparisonOperatorBetweenTwoScalarsWithoutBoolIsRefused() {
    for (final String op : new String[] { "==", "!=", "<", ">", "<=", ">=" })
      assertThatThrownBy(() -> instant("1 " + op + " 0")).as(op).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining(MESSAGE);
  }

  @Test
  void aScalarThatIsNotALiteralIsRefusedToo() {
    for (final String promql : new String[] { "(1) > 0", "-1 < 0", "-(1) > 0", "1 + 1 == 2", "(1 > bool 0) == 1",
        "(2 * 0.7) >= (0.2 + 0)" })
      assertThatThrownBy(() -> instant(promql)).as(promql).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining(MESSAGE);
  }

  @Test
  void aNestedRefusalIsNotMaskedByAnOuterVectorOperand() {
    assertThatThrownBy(() -> instant("m9723 + (1 > 0)")).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(MESSAGE);
  }

  @Test
  void aRangeQueryIsRefusedToo() {
    assertThatThrownBy(() -> new PromQLEvaluator(db()).evaluateRange(new PromQLParser("1 > 0").parse(), 1000L, 5000L, 2000L))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining(MESSAGE);
  }

  @Test
  void theSqlFunctionIsRefusedToo() {
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.command("sql", "RETURN promql('1 > 0', 6000)")) {
        rs.hasNext();
      }
    }).isInstanceOf(IllegalArgumentException.class).hasMessageContaining(MESSAGE);
  }

  @Test
  void theSameComparisonWithBoolStillAnswersZeroOrOne() {
    assertThat(scalar("2 > bool 1")).isEqualTo(1.0);
    assertThat(scalar("1 > bool 2")).isEqualTo(0.0);
    assertThat(scalar("-(-1) == bool 1")).isEqualTo(1.0);
  }

  @Test
  void arithmeticBetweenScalarsIsUnaffected() {
    assertThat(scalar("1 + 2")).isEqualTo(3.0);
    assertThat(scalar("2 ^ 3")).isEqualTo(8.0);
  }

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
