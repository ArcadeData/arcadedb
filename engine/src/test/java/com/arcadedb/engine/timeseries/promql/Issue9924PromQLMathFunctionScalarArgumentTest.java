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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9924: Prometheus refuses a scalar argument to {@code abs}, {@code ceil}, {@code floor} and {@code round}
 * ("expected type instant vector in call to function \"abs\", got scalar"). The evaluator answered {@code abs(-1)} with
 * the scalar 1. Whether an argument is a scalar is not always syntactic, so the check is made on the evaluated argument
 * and must hold for every expression shape that yields a scalar, on every entry point.
 */
class Issue9924PromQLMathFunctionScalarArgumentTest extends TestHelper {

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

  @Test
  void everyScalarShapeIsRefusedByEveryMathFunction() {
    for (final String fn : MATH_FNS)
      for (final String scalar : SCALARS)
        assertRefused(fn + "(" + scalar + ")", message(fn));
  }

  @Test
  void roundWithAScalarFirstArgumentIsRefusedOnTheFirstArgument() {
    // the precision is valid, the first argument is not
    assertRefused("round(7.3, 0.5)", message("round"));
    // both are wrong: the first one is reported, like Prometheus
    assertRefused("round(1, m9924)", message("round"));
  }

  @Test
  void aNestedRefusalIsNotMaskedByAnOuterExpression() {
    assertRefused("sum(abs(-1))", message("abs"));
    assertRefused("m9924 + abs(-1)", message("abs"));
    assertRefused("abs(-1) > bool 0", message("abs"));
    assertRefused("abs(ceil(-1))", message("ceil"));
    assertRefused("topk(floor(1.5), m9924)", message("floor"));
    assertRefused("round(m9924, abs(-1))", message("abs"));
  }

  @Test
  void aRangeQueryIsRefusedToo() {
    assertThatThrownBy(() -> new PromQLEvaluator(db()).evaluateRange(new PromQLParser("abs(-1)").parse(), 1000L, 5000L, 2000L))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining(message("abs"));
  }

  @Test
  void theSqlFunctionIsRefusedToo() {
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.command("sql", "RETURN promql('ceil(0.2)', 6000)")) {
        rs.hasNext();
      }
    }).isInstanceOf(IllegalArgumentException.class).hasMessageContaining(message("ceil"));
  }

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
