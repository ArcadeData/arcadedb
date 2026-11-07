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

import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr.AggOp;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr.AggregationExpr;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr.BinaryExpr;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr.BinaryOp;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr.FunctionCallExpr;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr.LabelMatcher;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr.MatchOp;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr.MatrixSelector;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr.NumberLiteral;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr.StringLiteral;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr.UnaryExpr;
import com.arcadedb.engine.timeseries.promql.ast.PromQLExpr.VectorSelector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PromQLParserTest {

  @Test
  void simpleSelector() {
    final PromQLExpr expr = new PromQLParser("cpu_usage").parse();
    assertThat(expr).isInstanceOf(VectorSelector.class);
    final VectorSelector vs = (VectorSelector) expr;
    assertThat(vs.metricName()).isEqualTo("cpu_usage");
    assertThat(vs.matchers()).isEmpty();
    assertThat(vs.offsetMs()).isZero();
  }

  @Test
  void selectorWithMatchers() {
    final PromQLExpr expr = new PromQLParser("http_requests{job=\"api\",status!=\"500\"}").parse();
    assertThat(expr).isInstanceOf(VectorSelector.class);
    final VectorSelector vs = (VectorSelector) expr;
    assertThat(vs.metricName()).isEqualTo("http_requests");
    assertThat(vs.matchers()).hasSize(2);
    assertThat(vs.matchers().get(0)).isEqualTo(new LabelMatcher("job", MatchOp.EQ, "api"));
    assertThat(vs.matchers().get(1)).isEqualTo(new LabelMatcher("status", MatchOp.NEQ, "500"));
  }

  @Test
  void regexMatcher() {
    final PromQLExpr expr = new PromQLParser("http_requests{job=~\"api.*\"}").parse();
    assertThat(expr).isInstanceOf(VectorSelector.class);
    final VectorSelector vs = (VectorSelector) expr;
    assertThat(vs.matchers()).hasSize(1);
    assertThat(vs.matchers().getFirst().op()).isEqualTo(MatchOp.RE);
    assertThat(vs.matchers().getFirst().value()).isEqualTo("api.*");
  }

  @Test
  void negativeRegexMatcher() {
    final PromQLExpr expr = new PromQLParser("http_requests{job!~\"test.*\"}").parse();
    final VectorSelector vs = (VectorSelector) expr;
    assertThat(vs.matchers().getFirst().op()).isEqualTo(MatchOp.NRE);
  }

  @Test
  void rangeVector() {
    final PromQLExpr expr = new PromQLParser("http_requests[5m]").parse();
    assertThat(expr).isInstanceOf(MatrixSelector.class);
    final MatrixSelector ms = (MatrixSelector) expr;
    assertThat(ms.selector().metricName()).isEqualTo("http_requests");
    assertThat(ms.rangeMs()).isEqualTo(300_000);
  }

  @Test
  void aggregationByBefore() {
    final PromQLExpr expr = new PromQLParser("sum by (job) (http_requests)").parse();
    assertThat(expr).isInstanceOf(AggregationExpr.class);
    final AggregationExpr agg = (AggregationExpr) expr;
    assertThat(agg.op()).isEqualTo(AggOp.SUM);
    assertThat(agg.groupLabels()).containsExactly("job");
    assertThat(agg.without()).isFalse();
    assertThat(agg.expr()).isInstanceOf(VectorSelector.class);
  }

  @Test
  void aggregationByAfter() {
    final PromQLExpr expr = new PromQLParser("sum(http_requests) by (job)").parse();
    assertThat(expr).isInstanceOf(AggregationExpr.class);
    final AggregationExpr agg = (AggregationExpr) expr;
    assertThat(agg.op()).isEqualTo(AggOp.SUM);
    assertThat(agg.groupLabels()).containsExactly("job");
  }

  @Test
  void aggregationWithout() {
    final PromQLExpr expr = new PromQLParser("avg without (instance) (cpu_usage)").parse();
    final AggregationExpr agg = (AggregationExpr) expr;
    assertThat(agg.op()).isEqualTo(AggOp.AVG);
    assertThat(agg.without()).isTrue();
    assertThat(agg.groupLabels()).containsExactly("instance");
  }

  @Test
  void functionCall() {
    final PromQLExpr expr = new PromQLParser("rate(http_requests[5m])").parse();
    assertThat(expr).isInstanceOf(FunctionCallExpr.class);
    final FunctionCallExpr fn = (FunctionCallExpr) expr;
    assertThat(fn.name()).isEqualTo("rate");
    assertThat(fn.args()).hasSize(1);
    assertThat(fn.args().getFirst()).isInstanceOf(MatrixSelector.class);
  }

  @Test
  void binaryExpression() {
    final PromQLExpr expr = new PromQLParser("cpu_usage * 100").parse();
    assertThat(expr).isInstanceOf(BinaryExpr.class);
    final BinaryExpr bin = (BinaryExpr) expr;
    assertThat(bin.op()).isEqualTo(BinaryOp.MUL);
    assertThat(bin.left()).isInstanceOf(VectorSelector.class);
    assertThat(bin.right()).isInstanceOf(NumberLiteral.class);
    assertThat(((NumberLiteral) bin.right()).value()).isEqualTo(100.0);
  }

  @Test
  void nestedAggregationAndFunction() {
    final PromQLExpr expr = new PromQLParser("sum(rate(http_requests_total[5m])) by (job)").parse();
    assertThat(expr).isInstanceOf(AggregationExpr.class);
    final AggregationExpr agg = (AggregationExpr) expr;
    assertThat(agg.op()).isEqualTo(AggOp.SUM);
    assertThat(agg.groupLabels()).containsExactly("job");
    assertThat(agg.expr()).isInstanceOf(FunctionCallExpr.class);
  }

  @Test
  void offset() {
    final PromQLExpr expr = new PromQLParser("http_requests offset 5m").parse();
    assertThat(expr).isInstanceOf(VectorSelector.class);
    final VectorSelector vs = (VectorSelector) expr;
    assertThat(vs.offsetMs()).isEqualTo(300_000);
  }

  @Test
  void rangeWithOffset() {
    final PromQLExpr expr = new PromQLParser("http_requests[5m] offset 1h").parse();
    assertThat(expr).isInstanceOf(MatrixSelector.class);
    final MatrixSelector ms = (MatrixSelector) expr;
    assertThat(ms.rangeMs()).isEqualTo(300_000);
    assertThat(ms.selector().offsetMs()).isEqualTo(3_600_000);
  }

  @Test
  void durationParsing() {
    assertThat(PromQLParser.parseDuration("5m")).isEqualTo(300_000);
    assertThat(PromQLParser.parseDuration("1h30m")).isEqualTo(5_400_000);
    assertThat(PromQLParser.parseDuration("2d")).isEqualTo(172_800_000);
    assertThat(PromQLParser.parseDuration("1w")).isEqualTo(604_800_000);
    assertThat(PromQLParser.parseDuration("30s")).isEqualTo(30_000);
  }

  @Test
  void durationParsingMilliseconds() {
    // Regression: '500ms' was previously mis-parsed as 500 minutes (30,000,000 ms) instead of 500 ms
    assertThat(PromQLParser.parseDuration("500ms")).isEqualTo(500);
    assertThat(PromQLParser.parseDuration("1ms")).isEqualTo(1);
    assertThat(PromQLParser.parseDuration("100ms")).isEqualTo(100);
    // Combined: 1s + 500ms
    assertThat(PromQLParser.parseDuration("1s500ms")).isEqualTo(1_500);
    // 'm' followed by something other than 's' should still be minutes
    assertThat(PromQLParser.parseDuration("2m")).isEqualTo(120_000);
    assertThat(PromQLParser.parseDuration("2m30s")).isEqualTo(150_000);
  }

  @Test
  void durationParsingOverflow() {
    // Values that overflow long when multiplied by the unit multiplier should throw.
    // 300,000,000 years overflows: 300_000_000 * 31_536_000_000 > Long.MAX_VALUE
    assertThatThrownBy(() -> PromQLParser.parseDuration("300000000y"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("too large");
  }

  @Test
  void rangeVectorWithMilliseconds() {
    // Regression: metric[500ms] should have rangeMs = 500, not 30,000,000
    final PromQLExpr expr = new PromQLParser("http_requests[500ms]").parse();
    assertThat(expr).isInstanceOf(MatrixSelector.class);
    assertThat(((MatrixSelector) expr).rangeMs()).isEqualTo(500);
  }

  @Test
  void operatorPrecedence() {
    // 1 + 2 * 3 should be 1 + (2 * 3)
    final PromQLExpr expr = new PromQLParser("1 + 2 * 3").parse();
    assertThat(expr).isInstanceOf(BinaryExpr.class);
    final BinaryExpr bin = (BinaryExpr) expr;
    assertThat(bin.op()).isEqualTo(BinaryOp.ADD);
    assertThat(bin.left()).isInstanceOf(NumberLiteral.class);
    assertThat(bin.right()).isInstanceOf(BinaryExpr.class);
    assertThat(((BinaryExpr) bin.right()).op()).isEqualTo(BinaryOp.MUL);
  }

  @Test
  void comparisonOperator() {
    final PromQLExpr expr = new PromQLParser("cpu_usage > 80").parse();
    assertThat(expr).isInstanceOf(BinaryExpr.class);
    final BinaryExpr bin = (BinaryExpr) expr;
    assertThat(bin.op()).isEqualTo(BinaryOp.GT);
  }

  @Test
  void unaryNegation() {
    final PromQLExpr expr = new PromQLParser("-cpu_usage").parse();
    assertThat(expr).isInstanceOf(UnaryExpr.class);
    final UnaryExpr un = (UnaryExpr) expr;
    assertThat(un.op()).isEqualTo('-');
    assertThat(un.expr()).isInstanceOf(VectorSelector.class);
  }

  @Test
  void topk() {
    final PromQLExpr expr = new PromQLParser("topk(5, http_requests)").parse();
    assertThat(expr).isInstanceOf(AggregationExpr.class);
    final AggregationExpr agg = (AggregationExpr) expr;
    assertThat(agg.op()).isEqualTo(AggOp.TOPK);
    assertThat(agg.param()).isInstanceOf(NumberLiteral.class);
    assertThat(((NumberLiteral) agg.param()).value()).isEqualTo(5.0);
  }

  @Test
  void stringLiteral() {
    final PromQLExpr expr = new PromQLParser("\"hello world\"").parse();
    assertThat(expr).isInstanceOf(StringLiteral.class);
    assertThat(((StringLiteral) expr).value()).isEqualTo("hello world");
  }

  @Test
  void numberLiteral() {
    final PromQLExpr expr = new PromQLParser("42.5").parse();
    assertThat(expr).isInstanceOf(NumberLiteral.class);
    assertThat(((NumberLiteral) expr).value()).isEqualTo(42.5);
  }

  @Test
  void parenthesizedExpression() {
    final PromQLExpr expr = new PromQLParser("(cpu_usage + mem_usage) / 2").parse();
    assertThat(expr).isInstanceOf(BinaryExpr.class);
    final BinaryExpr bin = (BinaryExpr) expr;
    assertThat(bin.op()).isEqualTo(BinaryOp.DIV);
    assertThat(bin.left()).isInstanceOf(BinaryExpr.class);
  }

  @Test
  void malformedExpression() {
    assertThatThrownBy(() -> new PromQLParser("sum(").parse())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void emptyExpression() {
    assertThatThrownBy(() -> new PromQLParser("").parse())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void invalidDuration() {
    assertThatThrownBy(() -> PromQLParser.parseDuration("5"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void multipleFunctionArgs() {
    final PromQLExpr expr = new PromQLParser("round(cpu_usage, 0.5)").parse();
    assertThat(expr).isInstanceOf(FunctionCallExpr.class);
    final FunctionCallExpr fn = (FunctionCallExpr) expr;
    assertThat(fn.name()).isEqualTo("round");
    assertThat(fn.args()).hasSize(2);
  }

  @Test
  void selectorWithMatchersAndRange() {
    final PromQLExpr expr = new PromQLParser("http_requests{job=\"api\"}[5m]").parse();
    assertThat(expr).isInstanceOf(MatrixSelector.class);
    final MatrixSelector ms = (MatrixSelector) expr;
    assertThat(ms.selector().metricName()).isEqualTo("http_requests");
    assertThat(ms.selector().matchers()).hasSize(1);
    assertThat(ms.rangeMs()).isEqualTo(300_000);
  }

  // #6807: the query_range step-count guard was computed in signed 64-bit arithmetic, so a span wider than
  // Long.MAX_VALUE milliseconds wrapped negative and the per-step loop ran unbounded. The evaluator rejects every one
  // of these before touching the database, so a null database is enough. The @Timeout is a hang detector, not a latency
  // bound.

  private static final PromQLExpr UP = new PromQLParser("up").parse();

  // #6807
  @Test
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  void spanWiderThanLongIsRejectedInsteadOfLoopingForever() {
    // The exact repro from the issue: start=-9e15s, end=9e15s, step=60s. Both operands are in order, so the
    // endMs < startMs test does not fire, and (endMs - startMs) overflows to a negative number.
    assertThatThrownBy(() -> new PromQLEvaluator(null).evaluateRange(UP, -9_000_000_000_000_000_000L,
        9_000_000_000_000_000_000L, 60_000L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("wider than the representable time span");
  }

  // #6807
  @Test
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  void spanOfLongMaxWithStepOneIsRejectedInsteadOfSilentlyEmpty() {
    // spanMs / stepMs is Long.MAX_VALUE here, so the historical "+ 1" would itself have overflowed to
    // Long.MIN_VALUE and turned the rejection into an empty result. The guard is tested before the "+ 1".
    assertThatThrownBy(() -> new PromQLEvaluator(null).evaluateRange(UP, 0L, Long.MAX_VALUE, 1L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("maximum of");
  }

  // #6807
  @Test
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  void representableButTooManyStepsIsStillRejected() {
    // The pre-existing guard must keep working unchanged for a range that does not overflow.
    assertThatThrownBy(() -> new PromQLEvaluator(null).evaluateRange(UP, 0L, 2_000_000_000L, 1_000L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("maximum of 1000000");
  }

  // #6807
  @Test
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  void endBeforeStartIsStillRejected() {
    assertThatThrownBy(() -> new PromQLEvaluator(null).evaluateRange(UP, 1_000L, 0L, 1_000L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must be >=");
  }

  // #6807
  @Test
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  void nonPositiveStepIsStillRejected() {
    assertThatThrownBy(() -> new PromQLEvaluator(null).evaluateRange(UP, 0L, 1_000L, 0L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("stepMs must be positive");
  }

  // #9644: the bool modifier, parser side
  // #9644
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

  // #9644
  @Test
  void everyComparisonOperatorAcceptsTheModifier() {
    for (final String op : new String[] { "==", "!=", "<", ">", "<=", ">=" }) {
      final PromQLExpr expr = new PromQLParser("m9644 " + op + " bool 1").parse();
      assertThat(expr).as(op).isInstanceOf(BinaryExpr.class);
      assertThat(((BinaryExpr) expr).returnBool()).as(op).isTrue();
    }
  }

  // #9644
  @Test
  void aMetricWhoseNameStartsWithBoolIsNotTheModifier() {
    final BinaryExpr bin = (BinaryExpr) new PromQLParser("m9644 > boolean_metric").parse();
    assertThat(bin.returnBool()).isFalse();
    assertThat(((VectorSelector) bin.right()).metricName()).isEqualTo("boolean_metric");
  }

  // #9644
  @Test
  void theModifierOnANonComparisonOperatorIsAnErrorThatNamesIt() {
    for (final String op : new String[] { "+", "-", "*", "/", "%", "^", "and", "or", "unless" })
      assertThatThrownBy(() -> new PromQLParser("m9644 " + op + " bool 1").parse()).as(op)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("bool modifier can only be used on comparison operators");
  }

  // #9644
  @Test
  void theErrorPointsAtTheModifier() {
    // "m9644 + bool 1": the modifier starts at position 8
    assertThatThrownBy(() -> new PromQLParser("m9644 + bool 1").parse()).hasMessageEndingWith("at position 8");
  }

  // #9644
  @Test
  void boolIsAKeywordRightAfterAnOperatorAsInPrometheus() {
    // Prometheus lexes `bool` as a keyword, so a metric literally named `bool` cannot be the right operand of a binary
    // operator: `a + bool` and `a > bool` are parse errors there too
    assertThatThrownBy(() -> new PromQLParser("m9644 + bool").parse()).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("bool modifier can only be used on comparison operators");
    assertThatThrownBy(() -> new PromQLParser("m9644 > bool").parse()).isInstanceOf(IllegalArgumentException.class);
  }

  // #9644
  @Test
  void theAstRefusesTheModifierOnANonComparisonOperator() {
    assertThatThrownBy(() -> new BinaryExpr(new VectorSelector("m9644", List.of(), 0), BinaryOp.ADD,
        new VectorSelector("m9644", List.of(), 0), true)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("bool modifier can only be used on comparison operators");
  }

  // #9644
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
}
