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
import com.arcadedb.engine.timeseries.promql.PromQLResult.ScalarResult;
import com.arcadedb.engine.timeseries.promql.PromQLResult.VectorSample;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr.BinaryExpr;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr.BinaryOp;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr.VectorSelector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9644: the PromQL {@code bool} modifier. A comparison with {@code bool} returns {@code 0}/{@code 1} for every
 * sample instead of filtering, and deletes {@code __name__} from the result, while the same comparison without the
 * modifier keeps filtering and keeps the name. {@code bool} on a non-comparison operator is a parse error that names
 * the modifier.
 */
class Issue9644PromQLBoolModifierTest extends TestHelper {

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

  // --- parser ---

  @Test
  void theParserRecordsTheModifier() {
    final BinaryExpr withBool = (BinaryExpr) new PromQLParser("m9644 > bool 1").parse();
    assertThat(withBool.op()).isEqualTo(BinaryOp.GT);
    assertThat(withBool.returnBool()).isTrue();
    assertThat(withBool.left()).isInstanceOf(VectorSelector.class);

    final BinaryExpr upperCase = (BinaryExpr) new PromQLParser("m9644 >= BOOL 1").parse();
    assertThat(upperCase.returnBool()).isTrue();

    final BinaryExpr without = (BinaryExpr) new PromQLParser("m9644 > 1").parse();
    assertThat(without.returnBool()).isFalse();
  }

  @Test
  void everyComparisonOperatorAcceptsTheModifier() {
    for (final String op : new String[] { "==", "!=", "<", ">", "<=", ">=" }) {
      final PromQLExpr expr = new PromQLParser("m9644 " + op + " bool 1").parse();
      assertThat(expr).as(op).isInstanceOf(BinaryExpr.class);
      assertThat(((BinaryExpr) expr).returnBool()).as(op).isTrue();
    }
  }

  @Test
  void aMetricWhoseNameStartsWithBoolIsNotTheModifier() {
    final BinaryExpr bin = (BinaryExpr) new PromQLParser("m9644 > boolean_metric").parse();
    assertThat(bin.returnBool()).isFalse();
    assertThat(((VectorSelector) bin.right()).metricName()).isEqualTo("boolean_metric");
  }

  @Test
  void theModifierOnANonComparisonOperatorIsAnErrorThatNamesIt() {
    for (final String op : new String[] { "+", "-", "*", "/", "%", "^", "and", "or", "unless" })
      assertThatThrownBy(() -> new PromQLParser("m9644 " + op + " bool 1").parse()).as(op)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("bool modifier can only be used on comparison operators");
  }

  @Test
  void theErrorPointsAtTheModifier() {
    // "m9644 + bool 1": the modifier starts at position 8
    assertThatThrownBy(() -> new PromQLParser("m9644 + bool 1").parse()).hasMessageEndingWith("at position 8");
  }

  @Test
  void boolIsAKeywordRightAfterAnOperatorAsInPrometheus() {
    // Prometheus lexes `bool` as a keyword, so a metric literally named `bool` cannot be the right operand of a binary
    // operator: `a + bool` and `a > bool` are parse errors there too
    assertThatThrownBy(() -> new PromQLParser("m9644 + bool").parse()).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("bool modifier can only be used on comparison operators");
    assertThatThrownBy(() -> new PromQLParser("m9644 > bool").parse()).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void boolFollowedByAParenthesisIsTheModifier() {
    // as in Prometheus, `bool` is the modifier and the parenthesised expression after it is the right operand
    final BinaryExpr paren = (BinaryExpr) new PromQLParser("m9644 > bool(1 + 1)").parse();
    assertThat(paren.returnBool()).isTrue();
    assertThat(paren.right()).isInstanceOf(BinaryExpr.class);
    assertThat(only("m9644 > bool(1 + 1)").value()).isEqualTo(1.0);
  }

  @Test
  void theAstRefusesTheModifierOnANonComparisonOperator() {
    assertThatThrownBy(() -> new BinaryExpr(new VectorSelector("m9644", List.of(), 0), BinaryOp.ADD,
        new VectorSelector("m9644", List.of(), 0), true)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("bool modifier can only be used on comparison operators");
  }

  @Test
  void chainedAndParenthesisedBoolComparisonsParse() {
    // left-associative: (m9644 > bool 1) > bool 0
    final BinaryExpr chained = (BinaryExpr) new PromQLParser("m9644 > bool 1 > bool 0").parse();
    assertThat(chained.returnBool()).isTrue();
    assertThat(((BinaryExpr) chained.left()).returnBool()).isTrue();

    final BinaryExpr plusOne = (BinaryExpr) new PromQLParser("(m9644 > bool 1) + 1").parse();
    assertThat(plusOne.op()).isEqualTo(BinaryOp.ADD);
    assertThat(plusOne.returnBool()).isFalse();
    assertThat(((BinaryExpr) plusOne.left()).returnBool()).isTrue();
  }

  // --- evaluator ---

  @Test
  void chainedAndParenthesisedBoolComparisonsEvaluate() {
    assertThat(only("m9644 > bool 1 > bool 0").value()).isEqualTo(1.0); // (5.5 > 1 = 1) > 0 = 1
    assertThat(only("m9644 > bool 10 > bool 0").value()).isEqualTo(0.0); // (5.5 > 10 = 0) > 0 = 0
    final VectorSample s = only("(m9644 > bool 1) + 1");
    assertThat(s.value()).isEqualTo(2.0);
    assertThat(s.labels()).doesNotContainKey("__name__");
  }

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

  @Test
  void aNaNSampleComparesAsPrometheusDoes() {
    // 5.5 * 0 / 0 = NaN on both sides: NaN == NaN is false, NaN != NaN is true, as in Go
    assertThat(only("(m9644 * 0 / 0) == bool (m9644 * 0 / 0)").value()).isEqualTo(0.0);
    assertThat(only("(m9644 * 0 / 0) != bool (m9644 * 0 / 0)").value()).isEqualTo(1.0);
    assertThat(only("(m9644 * 0 / 0) > bool 1").value()).isEqualTo(0.0);
  }

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

  @Test
  void vectorScalarWithBoolAnswersOneAndDropsTheName() {
    final VectorSample s = only("m9644 > bool 1");
    assertThat(s.value()).isEqualTo(1.0);
    assertThat(s.labels()).doesNotContainKey("__name__").containsEntry("host", "h1");
  }

  @Test
  void vectorScalarWithBoolKeepsAFalseSampleAsZero() {
    final VectorSample s = only("m9644 > bool 10");
    assertThat(s.value()).isEqualTo(0.0);
    assertThat(s.labels()).doesNotContainKey("__name__").containsEntry("host", "h1");
  }

  @Test
  void scalarVectorWithBoolAnswersZeroOrOne() {
    assertThat(only("1 < bool m9644").value()).isEqualTo(1.0);
    final VectorSample s = only("10 < bool m9644");
    assertThat(s.value()).isEqualTo(0.0);
    assertThat(s.labels()).doesNotContainKey("__name__").containsEntry("host", "h1");
  }

  @Test
  void vectorVectorWithBoolAnswersZeroOrOneAndDropsTheName() {
    final VectorSample eq = only("m9644 == bool m9644");
    assertThat(eq.value()).isEqualTo(1.0);
    assertThat(eq.labels()).doesNotContainKey("__name__").containsEntry("host", "h1");

    final VectorSample gt = only("m9644 > bool m9644");
    assertThat(gt.value()).isEqualTo(0.0);
    assertThat(gt.labels()).doesNotContainKey("__name__").containsEntry("host", "h1");
  }

  @Test
  void scalarScalarWithBoolAnswersZeroOrOne() {
    assertThat(scalar("2 > bool 1")).isEqualTo(1.0);
    assertThat(scalar("1 > bool 2")).isEqualTo(0.0);
  }

  @Test
  void theComparisonWithoutTheModifierStillFiltersAndKeepsTheName() {
    final VectorSample s = only("m9644 > 1");
    assertThat(s.value()).isEqualTo(5.5);
    assertThat(s.labels()).containsEntry("__name__", "m9644").containsEntry("host", "h1");

    assertThat(evaluate("m9644 > 10").samples()).isEmpty();
    assertThat(evaluate("m9644 > m9644").samples()).isEmpty();
    assertThat(only("m9644 == m9644").labels()).containsEntry("__name__", "m9644");
  }

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
