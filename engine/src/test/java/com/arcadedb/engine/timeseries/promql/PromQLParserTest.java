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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PromQLParserTest {

  @Test
  void testSimpleSelector() {
    final PromQLExpr expr = new PromQLParser("cpu_usage").parse();
    assertThat(expr).isInstanceOf(VectorSelector.class);
    final VectorSelector vs = (VectorSelector) expr;
    assertThat(vs.metricName()).isEqualTo("cpu_usage");
    assertThat(vs.matchers()).isEmpty();
    assertThat(vs.offsetMs()).isZero();
  }

  @Test
  void testSelectorWithMatchers() {
    final PromQLExpr expr = new PromQLParser("http_requests{job=\"api\",status!=\"500\"}").parse();
    assertThat(expr).isInstanceOf(VectorSelector.class);
    final VectorSelector vs = (VectorSelector) expr;
    assertThat(vs.metricName()).isEqualTo("http_requests");
    assertThat(vs.matchers()).hasSize(2);
    assertThat(vs.matchers().get(0)).isEqualTo(new LabelMatcher("job", MatchOp.EQ, "api"));
    assertThat(vs.matchers().get(1)).isEqualTo(new LabelMatcher("status", MatchOp.NEQ, "500"));
  }

  @Test
  void testRegexMatcher() {
    final PromQLExpr expr = new PromQLParser("http_requests{job=~\"api.*\"}").parse();
    assertThat(expr).isInstanceOf(VectorSelector.class);
    final VectorSelector vs = (VectorSelector) expr;
    assertThat(vs.matchers()).hasSize(1);
    assertThat(vs.matchers().getFirst().op()).isEqualTo(MatchOp.RE);
    assertThat(vs.matchers().getFirst().value()).isEqualTo("api.*");
  }

  @Test
  void testNegativeRegexMatcher() {
    final PromQLExpr expr = new PromQLParser("http_requests{job!~\"test.*\"}").parse();
    final VectorSelector vs = (VectorSelector) expr;
    assertThat(vs.matchers().getFirst().op()).isEqualTo(MatchOp.NRE);
  }

  @Test
  void testRangeVector() {
    final PromQLExpr expr = new PromQLParser("http_requests[5m]").parse();
    assertThat(expr).isInstanceOf(MatrixSelector.class);
    final MatrixSelector ms = (MatrixSelector) expr;
    assertThat(ms.selector().metricName()).isEqualTo("http_requests");
    assertThat(ms.rangeMs()).isEqualTo(300_000);
  }

  @Test
  void testAggregationByBefore() {
    final PromQLExpr expr = new PromQLParser("sum by (job) (http_requests)").parse();
    assertThat(expr).isInstanceOf(AggregationExpr.class);
    final AggregationExpr agg = (AggregationExpr) expr;
    assertThat(agg.op()).isEqualTo(AggOp.SUM);
    assertThat(agg.groupLabels()).containsExactly("job");
    assertThat(agg.without()).isFalse();
    assertThat(agg.expr()).isInstanceOf(VectorSelector.class);
  }

  @Test
  void testAggregationByAfter() {
    final PromQLExpr expr = new PromQLParser("sum(http_requests) by (job)").parse();
    assertThat(expr).isInstanceOf(AggregationExpr.class);
    final AggregationExpr agg = (AggregationExpr) expr;
    assertThat(agg.op()).isEqualTo(AggOp.SUM);
    assertThat(agg.groupLabels()).containsExactly("job");
  }

  @Test
  void testAggregationWithout() {
    final PromQLExpr expr = new PromQLParser("avg without (instance) (cpu_usage)").parse();
    final AggregationExpr agg = (AggregationExpr) expr;
    assertThat(agg.op()).isEqualTo(AggOp.AVG);
    assertThat(agg.without()).isTrue();
    assertThat(agg.groupLabels()).containsExactly("instance");
  }

  @Test
  void testFunctionCall() {
    final PromQLExpr expr = new PromQLParser("rate(http_requests[5m])").parse();
    assertThat(expr).isInstanceOf(FunctionCallExpr.class);
    final FunctionCallExpr fn = (FunctionCallExpr) expr;
    assertThat(fn.name()).isEqualTo("rate");
    assertThat(fn.args()).hasSize(1);
    assertThat(fn.args().getFirst()).isInstanceOf(MatrixSelector.class);
  }

  @Test
  void testBinaryExpression() {
    final PromQLExpr expr = new PromQLParser("cpu_usage * 100").parse();
    assertThat(expr).isInstanceOf(BinaryExpr.class);
    final BinaryExpr bin = (BinaryExpr) expr;
    assertThat(bin.op()).isEqualTo(BinaryOp.MUL);
    assertThat(bin.left()).isInstanceOf(VectorSelector.class);
    assertThat(bin.right()).isInstanceOf(NumberLiteral.class);
    assertThat(((NumberLiteral) bin.right()).value()).isEqualTo(100.0);
  }

  @Test
  void testNestedAggregationAndFunction() {
    final PromQLExpr expr = new PromQLParser("sum(rate(http_requests_total[5m])) by (job)").parse();
    assertThat(expr).isInstanceOf(AggregationExpr.class);
    final AggregationExpr agg = (AggregationExpr) expr;
    assertThat(agg.op()).isEqualTo(AggOp.SUM);
    assertThat(agg.groupLabels()).containsExactly("job");
    assertThat(agg.expr()).isInstanceOf(FunctionCallExpr.class);
  }

  @Test
  void testOffset() {
    final PromQLExpr expr = new PromQLParser("http_requests offset 5m").parse();
    assertThat(expr).isInstanceOf(VectorSelector.class);
    final VectorSelector vs = (VectorSelector) expr;
    assertThat(vs.offsetMs()).isEqualTo(300_000);
  }

  @Test
  void testRangeWithOffset() {
    final PromQLExpr expr = new PromQLParser("http_requests[5m] offset 1h").parse();
    assertThat(expr).isInstanceOf(MatrixSelector.class);
    final MatrixSelector ms = (MatrixSelector) expr;
    assertThat(ms.rangeMs()).isEqualTo(300_000);
    assertThat(ms.selector().offsetMs()).isEqualTo(3_600_000);
  }

  @Test
  void testDurationParsing() {
    assertThat(PromQLParser.parseDuration("5m")).isEqualTo(300_000);
    assertThat(PromQLParser.parseDuration("1h30m")).isEqualTo(5_400_000);
    assertThat(PromQLParser.parseDuration("2d")).isEqualTo(172_800_000);
    assertThat(PromQLParser.parseDuration("1w")).isEqualTo(604_800_000);
    assertThat(PromQLParser.parseDuration("30s")).isEqualTo(30_000);
  }

  @Test
  void testDurationParsingMilliseconds() {
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
  void testDurationParsingOverflow() {
    // Values that overflow long when multiplied by the unit multiplier should throw.
    // 300,000,000 years overflows: 300_000_000 * 31_536_000_000 > Long.MAX_VALUE
    assertThatThrownBy(() -> PromQLParser.parseDuration("300000000y"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("too large");
  }

  @Test
  void testRangeVectorWithMilliseconds() {
    // Regression: metric[500ms] should have rangeMs = 500, not 30,000,000
    final PromQLExpr expr = new PromQLParser("http_requests[500ms]").parse();
    assertThat(expr).isInstanceOf(MatrixSelector.class);
    assertThat(((MatrixSelector) expr).rangeMs()).isEqualTo(500);
  }

  @Test
  void testOperatorPrecedence() {
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
  void testComparisonOperator() {
    final PromQLExpr expr = new PromQLParser("cpu_usage > 80").parse();
    assertThat(expr).isInstanceOf(BinaryExpr.class);
    final BinaryExpr bin = (BinaryExpr) expr;
    assertThat(bin.op()).isEqualTo(BinaryOp.GT);
  }

  @Test
  void testUnaryNegation() {
    final PromQLExpr expr = new PromQLParser("-cpu_usage").parse();
    assertThat(expr).isInstanceOf(UnaryExpr.class);
    final UnaryExpr un = (UnaryExpr) expr;
    assertThat(un.op()).isEqualTo('-');
    assertThat(un.expr()).isInstanceOf(VectorSelector.class);
  }

  @Test
  void testTopk() {
    final PromQLExpr expr = new PromQLParser("topk(5, http_requests)").parse();
    assertThat(expr).isInstanceOf(AggregationExpr.class);
    final AggregationExpr agg = (AggregationExpr) expr;
    assertThat(agg.op()).isEqualTo(AggOp.TOPK);
    assertThat(agg.param()).isInstanceOf(NumberLiteral.class);
    assertThat(((NumberLiteral) agg.param()).value()).isEqualTo(5.0);
  }

  @Test
  void testStringLiteral() {
    final PromQLExpr expr = new PromQLParser("\"hello world\"").parse();
    assertThat(expr).isInstanceOf(StringLiteral.class);
    assertThat(((StringLiteral) expr).value()).isEqualTo("hello world");
  }

  @Test
  void testNumberLiteral() {
    final PromQLExpr expr = new PromQLParser("42.5").parse();
    assertThat(expr).isInstanceOf(NumberLiteral.class);
    assertThat(((NumberLiteral) expr).value()).isEqualTo(42.5);
  }

  @Test
  void testParenthesizedExpression() {
    final PromQLExpr expr = new PromQLParser("(cpu_usage + mem_usage) / 2").parse();
    assertThat(expr).isInstanceOf(BinaryExpr.class);
    final BinaryExpr bin = (BinaryExpr) expr;
    assertThat(bin.op()).isEqualTo(BinaryOp.DIV);
    assertThat(bin.left()).isInstanceOf(BinaryExpr.class);
  }

  @Test
  void testMalformedExpression() {
    assertThatThrownBy(() -> new PromQLParser("sum(").parse())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void testEmptyExpression() {
    assertThatThrownBy(() -> new PromQLParser("").parse())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void testInvalidDuration() {
    assertThatThrownBy(() -> PromQLParser.parseDuration("5"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void testMultipleFunctionArgs() {
    final PromQLExpr expr = new PromQLParser("round(cpu_usage, 0.5)").parse();
    assertThat(expr).isInstanceOf(FunctionCallExpr.class);
    final FunctionCallExpr fn = (FunctionCallExpr) expr;
    assertThat(fn.name()).isEqualTo("round");
    assertThat(fn.args()).hasSize(2);
  }

  @Test
  void testSelectorWithMatchersAndRange() {
    final PromQLExpr expr = new PromQLParser("http_requests{job=\"api\"}[5m]").parse();
    assertThat(expr).isInstanceOf(MatrixSelector.class);
    final MatrixSelector ms = (MatrixSelector) expr;
    assertThat(ms.selector().metricName()).isEqualTo("http_requests");
    assertThat(ms.selector().matchers()).hasSize(1);
    assertThat(ms.rangeMs()).isEqualTo(300_000);
  }
}
