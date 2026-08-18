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
package com.arcadedb.query.sql.parser;

import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6190: before this, nothing on {@link com.arcadedb.query.sql.executor.SQLFunction} told a pure function
 * ({@code abs}) from one with a side effect or hidden input ({@code uuid}, {@code sysdate}), so the planner could
 * not cache a plan containing a call, and constant folding refused to reach into one at all - both restrictions are
 * pinned as the pre-existing behaviour in {@code Issue6179LiteralExpressionTest} and
 * {@code ConstantFalseFilterFoldingTest#aFilterCallingAFunctionIsNeverFolded}.
 * <p>
 * These pin the two consumers that need no live database: {@link FunctionCall#isCacheable()} (plan caching) and
 * {@link Expression#isFoldable()} (constant folding). The third site the issue names,
 * {@code BinaryCondition#isIndexAware}'s plan-time probe of an indexed equality, is left unchanged - it cannot use
 * this same marker without also losing the probe for a bound parameter (also "early calculated" but never a purity
 * concern), which needs its own predicate; see the PR description.
 */
class Issue6190FunctionDeterminismTest extends AbstractParserTest {

  private final CommandContext context = new BasicCommandContext();

  private Expression rightOperandOf(final String query) {
    final SelectStatement statement = (SelectStatement) checkRightSyntax(query);
    final BooleanExpression term = statement.whereClause.flatten().getFirst().subBlocks.getFirst();
    return ((BinaryCondition) term).right;
  }

  private FunctionCall functionCallIn(final String query) {
    final Expression expression = rightOperandOf(query);
    final BaseExpression baseExpression = (BaseExpression) expression.mathExpression;
    return baseExpression.identifier.levelZero.functionCall;
  }

  @Test
  void builtinsMarkedDeterministicAreExactlyTheVerifiedPureOnes() {
    for (final String call : new String[] { "abs(-1)", "pow(2, 3)", "sqrt(4)", "coalesce(null, 1)", "ifnull(null, 1)",
        "ifempty('', 'x')", "if(true, 1, 2)", "strcmpci('a', 'A')", "decode('YQ==', 'base64')" }) {
      assertThat(functionCallIn("select from Foo where a = " + call).isCacheable()).as(call).isTrue();
    }
  }

  /**
   * {@code uuid()}/{@code sysdate()} read a random source / the clock; {@code encode()} can dereference a RID and
   * read the record store; {@code format()} resolves locale-sensitive conversions against process-wide mutable
   * state. None of the four may ever be marked deterministic.
   */
  @Test
  void functionsWithHiddenInputsOrSideEffectsAreNeverCacheableOrFoldable() {
    for (final String call : new String[] { "uuid()", "sysdate()", "encode('a', 'base64')", "format('%d', 1)" }) {
      final String query = "select from Foo where a = " + call;
      assertThat(functionCallIn(query).isCacheable()).as(call).isFalse();
      assertThat(rightOperandOf(query).isFoldable()).as(call).isFalse();
    }
  }

  @Test
  void aUserDefinedOrUnknownFunctionIsConservativelyNotCacheable() {
    assertThat(functionCallIn("select from Foo where a = thisFunctionDoesNotExist(1)").isCacheable()).isFalse();
  }

  @Test
  void aDeterministicCallIsCacheableOnlyWhenItsArgumentsAre() {
    assertThat(functionCallIn("select from Foo where a = abs(-1)").isCacheable())
        .as("literal argument").isTrue();
    assertThat(functionCallIn("select from Foo where a = abs(b)").isCacheable())
        .as("a property read does not vary the plan shape, so it is cacheable too - like SuffixIdentifier.isCacheable()")
        .isTrue();
    assertThat(functionCallIn("select from Foo where a = abs(uuid())").isCacheable())
        .as("a non-deterministic argument taints the whole call").isFalse();
  }

  @Test
  void aGraphTraversalStaysCacheableRegardlessOfDeterminism() {
    // isCacheable() must not regress the pre-existing graph-traversal exemption (out()/in()/both()/... are not
    // SQLFunction.isDeterministic()-marked and never will be, since their result depends on the graph, not just
    // their literal arguments)
    assertThat(functionCallIn("select from Foo where a = out('E')").isCacheable()).isTrue();
  }

  @Test
  void aLiteralIsFoldableAndSoIsANestedDeterministicCall() {
    for (final String literal : new String[] { "1", "'x'", "true", "1 + 2 * 3" }) {
      assertThat(rightOperandOf("select from Foo where a = " + literal).isFoldable()).as(literal).isTrue();
    }
    assertThat(rightOperandOf("select from Foo where a = abs(-1)").isFoldable())
        .as("a bare deterministic call over a literal").isTrue();
    assertThat(rightOperandOf("select from Foo where a = abs(pow(-2, 3))").isFoldable())
        .as("a deterministic call nested inside another").isTrue();
  }

  @Test
  void aPropertyOrInputParameterArgumentIsNeverFoldable() {
    // isFoldable() feeds a value baked into a CACHED plan, so - like isLiteral() - it must refuse a bound parameter
    // even though the parameter is resolved before the plan is built
    assertThat(rightOperandOf("select from Foo where a = abs(b)").isFoldable()).isFalse();
    assertThat(rightOperandOf("select from Foo where a = abs(:p)").isFoldable()).isFalse();
  }

  @Test
  void aMethodCallOnADeterministicFunctionResultIsNotFoldable() {
    // a modifier applied to the call's result is no longer a bare call; isFoldable() must not walk past it
    assertThat(rightOperandOf("select from Foo where a = abs(-1).asString()").isFoldable()).isFalse();
  }

  /**
   * isFoldable() is a strictly narrower, additive question - it must never replace
   * {@link Expression#isEarlyCalculated(CommandContext)} (issue #6179), which still answers "computable without a
   * record" for every function, pure or not. Pinning that for a function call needs a live database (see
   * {@code Issue6179EarlyCalculatedModifierTest}); the literal case needs none.
   */
  @Test
  void theEarlyCalculatedPredicateAdmitsMoreThanFoldableDoes() {
    assertThat(rightOperandOf("select from Foo where a = :p").isEarlyCalculated(context))
        .as("an input parameter is early calculated ...").isTrue();
    assertThat(rightOperandOf("select from Foo where a = :p").isFoldable())
        .as("... but never foldable, unlike isLiteral(true)").isFalse();
  }
}
