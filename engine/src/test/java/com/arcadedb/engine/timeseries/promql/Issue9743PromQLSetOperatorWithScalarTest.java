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
import com.arcadedb.engine.timeseries.promql.PromQLResult.VectorSample;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9743: Prometheus refuses a set operator ({@code and}, {@code or}, {@code unless}) with a scalar operand
 * ("set operator \"and\" not allowed in binary scalar expression"). The evaluator used to route it through the arithmetic
 * branches, where the set operators map to NaN: {@code 1 and 2} answered a scalar NaN and {@code m and 1} answered every
 * series of {@code m} with value NaN. Whether an operand is a scalar is not always syntactic, so the check is made on the
 * evaluated operands and must hold for every operand shape and every entry point.
 */
class Issue9743PromQLSetOperatorWithScalarTest extends TestHelper {

  private static final long     EVAL_TIME_MS = 6_000L;
  private static final String[] SET_OPS      = { "and", "or", "unless" };

  @BeforeEach
  void createData() {
    database.command("sql", "CREATE TIMESERIES TYPE m9743 TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE)");
    database.transaction(() -> database.command("sql", "INSERT INTO m9743 SET ts = 5000, host = 'h1', value = 5.5"));
  }

  @Test
  void aSetOperatorBetweenTwoScalarsIsRefused() {
    for (final String op : SET_OPS)
      assertRefused("1 " + op + " 2", op);
  }

  @Test
  void aSetOperatorWithAScalarOnTheRightIsRefused() {
    for (final String op : SET_OPS)
      assertRefused("m9743 " + op + " 1", op);
  }

  @Test
  void aSetOperatorWithAScalarOnTheLeftIsRefused() {
    for (final String op : SET_OPS)
      assertRefused("1 " + op + " m9743", op);
  }

  @Test
  void aScalarThatIsNotALiteralIsRefusedToo() {
    assertRefused("m9743 and -(1)", "and");
    assertRefused("(1 + 1) or m9743", "or");
    assertRefused("m9743 unless (1 > bool 0)", "unless");
  }

  @Test
  void aVectorWithNoSamplesIsStillAVector() {
    // the refusal is about the operand type, not about there being samples to combine
    assertRefused("missing_metric_9743 and 1", "and");
  }

  @Test
  void aNestedRefusalIsNotMaskedByAnOuterOperator() {
    assertRefused("m9743 + (m9743 and 1)", "and");
  }

  @Test
  void aRangeQueryIsRefusedToo() {
    assertThatThrownBy(() -> new PromQLEvaluator(db()).evaluateRange(new PromQLParser("m9743 or 1").parse(), 1000L, 5000L, 2000L))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining(message("or"));
  }

  @Test
  void theSqlFunctionIsRefusedToo() {
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.command("sql", "RETURN promql('1 unless 2', 6000)")) {
        rs.hasNext();
      }
    }).isInstanceOf(IllegalArgumentException.class).hasMessageContaining(message("unless"));
  }

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
