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
import static org.assertj.core.api.Assertions.offset;

/**
 * Issue #9883: Prometheus refuses an aggregation whose inner expression is not an instant vector ("expected type instant
 * vector in aggregation expression, got range vector") and a math function whose argument is a range vector or a string
 * ("expected type instant vector in call to function \"abs\", got range vector"). The evaluator answered an empty vector
 * for {@code sum(m[5m])} and {@code sum(1)}, returned the range vector unchanged for {@code abs(m[5m])}, treated
 * {@code abs("a")} as a NaN scalar and silently replaced a non-scalar {@code round} precision with 1.
 */
class Issue9883PromQLFunctionArgumentTypeTest extends TestHelper {

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

  @Test
  void anAggregationOverAScalarOrAStringIsRefused() {
    for (final String agg : AGGREGATIONS) {
      assertRefused(aggregate(agg, "1"), "expected type instant vector in aggregation expression, got scalar");
      assertRefused(aggregate(agg, "-(1)"), "expected type instant vector in aggregation expression, got scalar");
      assertRefused(aggregate(agg, "\"a\""), "expected type instant vector in aggregation expression, got string");
      assertRefused(aggregate(agg, "(\"a\")"), "expected type instant vector in aggregation expression, got string");
    }
  }

  @Test
  void theInnerTypeIsCheckedBeforeTheRankingParameter() {
    // Prometheus' type checker looks at the inner expression before the parameter
    assertRefused("topk(m9883, m9883[5m])", "expected type instant vector in aggregation expression, got range vector");
  }

  @Test
  void aMathFunctionOverARangeVectorIsRefused() {
    for (final String fn : MATH_FNS) {
      assertRefused(fn + "(m9883[5m])", "expected type instant vector in call to function \"" + fn + "\", got range vector");
      assertRefused(fn + "(missing_9883[5m])", "expected type instant vector in call to function \"" + fn + "\", got range vector");
    }
    assertRefused("round(m9883[5m], 0.5)", "expected type instant vector in call to function \"round\", got range vector");
  }

  @Test
  void aMathFunctionOverAStringIsRefused() {
    for (final String fn : MATH_FNS) {
      assertRefused(fn + "(\"a\")", "expected type instant vector in call to function \"" + fn + "\", got string");
      assertRefused(fn + "('a')", "expected type instant vector in call to function \"" + fn + "\", got string");
      assertRefused(fn + "((\"a\"))", "expected type instant vector in call to function \"" + fn + "\", got string");
    }
  }

  @Test
  void aNonScalarRoundPrecisionIsRefused() {
    assertRefused("round(m9883, m9883)", "expected type scalar in call to function \"round\", got instant vector");
    assertRefused("round(m9883, m9883[5m])", "expected type scalar in call to function \"round\", got range vector");
    assertRefused("round(m9883, \"a\")", "expected type scalar in call to function \"round\", got string");
    // both arguments wrong: the first one is reported, like Prometheus
    assertRefused("round(m9883[5m], \"a\")", "expected type instant vector in call to function \"round\", got range vector");
  }

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

  @Test
  void aRangeFunctionOverANonRangeVectorUsesPrometheusWording() {
    assertRefused("rate(m9883)", "expected type range vector in call to function \"rate\", got instant vector");
    assertRefused("sum_over_time(1)", "expected type range vector in call to function \"sum_over_time\", got scalar");
    assertRefused("increase(\"a\")", "expected type range vector in call to function \"increase\", got string");
  }

  @Test
  void aNestedRefusalIsNotMaskedByAnOuterExpression() {
    assertRefused("sum(abs(m9883[5m]))", "expected type instant vector in call to function \"abs\", got range vector");
    assertRefused("abs(sum(m9883[5m]))", "expected type instant vector in aggregation expression, got range vector");
    assertRefused("m9883 + sum(m9883[5m])", "expected type instant vector in aggregation expression, got range vector");
  }

  @Test
  void aRangeQueryIsRefusedToo() {
    assertThatThrownBy(() -> range("sum(m9883[5m])"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("expected type instant vector in aggregation expression, got range vector");
    assertThatThrownBy(() -> range("abs(m9883[5m])"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("expected type instant vector in call to function \"abs\", got range vector");
  }

  @Test
  void theSqlFunctionIsRefusedToo() {
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.command("sql", "RETURN promql('abs(m9883[5m])', 6000)")) {
        rs.hasNext();
      }
    }).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("expected type instant vector in call to function \"abs\", got range vector");
  }

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
