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

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.literal.LiteralParameterizer.Lookup;
import com.arcadedb.query.literal.LiteralParameterizer;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Expression classification, node identity, positional parameters and per-row identifier work.
 * <p>
 * A container for the regression suites of the feature; each nested class keeps its own fixture.
 */
class ExpressionEvaluationTest {
  /**
   * Regression for #6179: BaseExpression.isEarlyCalculated() classified an expression on its leftmost term alone and never looked at...
   * <p>
   * Issue #6179: {@code BaseExpression.isEarlyCalculated()} classified an expression on its leftmost
   * term alone and never looked at the modifier chain, so {@code 'Mr.'.append(surname)} was reported
   * as computable without a record. The planner takes that as licence to use an index and to evaluate
   * the expression against a {@code null} record, so the record-dependent part silently resolved to
   * null and the query answered differently depending on whether an index existed.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class EarlyCalculatedModifier extends TestHelper {

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        final VertexType person = database.getSchema().createVertexType("Person");
        person.createProperty("fullName", Type.STRING);
        person.createProperty("surname", Type.STRING);
        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Person", "fullName");

        final VertexType noIndex = database.getSchema().createVertexType("PersonNoIndex");
        noIndex.createProperty("fullName", Type.STRING);
        noIndex.createProperty("surname", Type.STRING);
      });
    }

    private void createFixture() {
      database.transaction(() -> {
        database.command("sql", "INSERT INTO Person SET fullName = 'Mr.Smith', surname = 'Smith'");
        database.command("sql", "INSERT INTO Person SET fullName = 'Mr.Jones', surname = 'Jones'");
        database.command("sql", "INSERT INTO PersonNoIndex SET fullName = 'Mr.Smith', surname = 'Smith'");
        database.command("sql", "INSERT INTO PersonNoIndex SET fullName = 'Mr.Jones', surname = 'Jones'");
      });
    }

    @Test
    void aMethodCallOnARecordFieldIsNotEarlyCalculated() {
      createFixture();

      database.transaction(() -> {
        // the indexed and the non-indexed type hold the same rows, so both queries must answer the same
        try (final ResultSet indexed = database.query("sql", "SELECT fullName FROM Person WHERE fullName = 'Mr.'.append(surname)")) {
          assertThat(indexed.stream().map(r -> r.<String>getProperty("fullName")).toList()).containsExactlyInAnyOrder("Mr.Smith",
              "Mr.Jones");
        }

        try (final ResultSet scanned = database.query("sql",
            "SELECT fullName FROM PersonNoIndex WHERE fullName = 'Mr.'.append(surname)")) {
          assertThat(scanned.stream().map(r -> r.<String>getProperty("fullName")).toList()).containsExactlyInAnyOrder("Mr.Smith",
              "Mr.Jones");
        }
      });
    }

    @Test
    void anIndexIsNotUsedWhenTheRightSideDependsOnTheRecord() {
      createFixture();

      database.transaction(() -> {
        try (final ResultSet explain = database.query("sql",
            "EXPLAIN SELECT fullName FROM Person WHERE fullName = 'Mr.'.append(surname)")) {
          final String plan = explain.next().getProperty("executionPlanAsString");
          assertThat(plan).doesNotContain("FETCH FROM INDEX");
        }
      });
    }

    @Test
    void aCollectionSelectorOnARecordFieldIsNotEarlyCalculated() {
      database.transaction(() -> {
        database.command("sql", "INSERT INTO Person SET fullName = 'b', surname = 'x', idx = 1");
        database.command("sql", "INSERT INTO PersonNoIndex SET fullName = 'b', surname = 'x', idx = 1");
      });

      database.transaction(() -> {
        try (final ResultSet indexed = database.query("sql", "SELECT fullName FROM Person WHERE fullName = ['a','b'][idx]")) {
          assertThat(indexed.stream().map(r -> r.<String>getProperty("fullName")).toList()).containsExactly("b");
        }

        try (final ResultSet scanned = database.query("sql", "SELECT fullName FROM PersonNoIndex WHERE fullName = ['a','b'][idx]")) {
          assertThat(scanned.stream().map(r -> r.<String>getProperty("fullName")).toList()).containsExactly("b");
        }
      });
    }

    @Test
    void aCaseExpressionOverARecordFieldIsNotEarlyCalculated() {
      createFixture();

      database.transaction(() -> {
        final String filter = " WHERE fullName = CASE surname WHEN 'Smith' THEN 'Mr.Smith' ELSE 'nomatch' END";

        try (final ResultSet indexed = database.query("sql", "SELECT fullName FROM Person" + filter)) {
          assertThat(indexed.stream().map(r -> r.<String>getProperty("fullName")).toList()).containsExactly("Mr.Smith");
        }

        try (final ResultSet scanned = database.query("sql", "SELECT fullName FROM PersonNoIndex" + filter)) {
          assertThat(scanned.stream().map(r -> r.<String>getProperty("fullName")).toList()).containsExactly("Mr.Smith");
        }
      });
    }

    /**
     * The other half of the contract: a function whose arguments need no record stays "early calculated", so an
     * equality against it is still resolved through the index. Only expressions that reach the record are excluded.
     */
    @Test
    void aFunctionOverLiteralsIsStillEarlyCalculated() {
      createFixture();

      database.transaction(() -> {
        try (final ResultSet explain = database.query("sql", "EXPLAIN SELECT fullName FROM Person WHERE fullName = uuid()")) {
          final String plan = explain.next().getProperty("executionPlanAsString");
          assertThat(plan).contains("FETCH FROM INDEX");
        }
      });
    }

    @Test
    void aModifierOverALiteralIsStillEarlyCalculated() {
      createFixture();

      database.transaction(() -> {
        try (final ResultSet explain = database.query("sql",
            "EXPLAIN SELECT fullName FROM Person WHERE fullName = 'Mr.'.append('Smith')")) {
          final String plan = explain.next().getProperty("executionPlanAsString");
          assertThat(plan).contains("FETCH FROM INDEX");
        }

        try (final ResultSet indexed = database.query("sql", "SELECT fullName FROM Person WHERE fullName = 'Mr.'.append('Smith')")) {
          assertThat(indexed.stream().map(r -> r.<String>getProperty("fullName")).toList()).containsExactly("Mr.Smith");
        }
      });
    }
  }

  /**
   * Regression for #6179: isEarlyCalculated() answers "can this be computed without a record", which is not the same question as "is...
   * <p>
   * Issue #6179: {@code isEarlyCalculated()} answers "can this be computed without a record", which is not the same
   * question as "is this a constant". These pin both predicates, and above all the cases where they disagree.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class LiteralExpression extends AbstractParserTest {

    private final CommandContext context = new BasicCommandContext();

    private Expression rightOperandOf(final String query) {
      final SelectStatement statement = (SelectStatement) checkRightSyntax(query);
      final BooleanExpression term = statement.whereClause.flatten().getFirst().subBlocks.getFirst();
      return ((BinaryCondition) term).right;
    }

    @Test
    void aLiteralIsBothEarlyCalculatedAndLiteral() {
      for (final String literal : new String[] { "1", "'x'", "true", "1 + 2 * 3", "(1 + 2)", "['a', 'b']", "[]", "#12:0" }) {
        final Expression expression = rightOperandOf("select from Foo where a = " + literal);
        assertThat(expression.isEarlyCalculated(context)).as(literal).isTrue();
        assertThat(expression.isLiteral()).as(literal).isTrue();
      }
    }

    /**
     * The one shape where the narrower predicate says yes and the wider one says no: {@code null} is a constant, but
     * {@code BaseExpression.isEarlyCalculated} has never admitted it, and it must not start to - an index lookup on
     * a null key is not the same question as the per-record comparison the filter would make.
     */
    @Test
    void theNullLiteralIsALiteralButNotEarlyCalculated() {
      final Expression expression = rightOperandOf("select from Foo where a = null");
      assertThat(expression.isLiteral()).isTrue();
      assertThat(expression.isEarlyCalculated(context)).isFalse();
    }

    @Test
    void aPropertyIsNeither() {
      final Expression expression = rightOperandOf("select from Foo where a = b");
      assertThat(expression.isEarlyCalculated(context)).isFalse();
      assertThat(expression.isLiteral()).isFalse();
    }

    /**
     * A function call is never a literal, whatever its arguments are. That it stays "early calculated" - the point
     * of the issue - is pinned where a database is at hand, in {@code ExpressionEvaluationTest.EarlyCalculatedModifier}.
     */
    @Test
    void aFunctionOverLiteralsIsNotALiteral() {
      for (final String call : new String[] { "uuid()", "sysdate()", "concat('a', 'b')" }) {
        final Expression expression = rightOperandOf("select from Foo where a = " + call);
        assertThat(expression.isLiteral()).as(call).isFalse();
        assertThat(expression.isLiteral(true)).as(call).isFalse();
      }
    }

    @Test
    void anInputParameterIsALiteralOnlyWhenTheCallerSaysSo() {
      final Expression expression = rightOperandOf("select from Foo where a = :p");
      assertThat(expression.isEarlyCalculated(context)).isTrue();
      assertThat(expression.isLiteral()).isFalse();
      assertThat(expression.isLiteral(true)).isTrue();
    }

    @Test
    void aCollectionIsLiteralOnlyWhenEveryItemIs() {
      assertThat(rightOperandOf("select from Foo where a = ['x', 1]").isLiteral()).isTrue();
      assertThat(rightOperandOf("select from Foo where a = ['x', b]").isLiteral()).isFalse();
      assertThat(rightOperandOf("select from Foo where a = ['x', :p]").isLiteral()).isFalse();
      assertThat(rightOperandOf("select from Foo where a = ['x', :p]").isLiteral(true)).isTrue();
    }

    @Test
    void aModifierOverALiteralIsEarlyCalculatedButNotLiteral() {
      for (final String modified : new String[] { "'x'.append('y')", "[1, 2][0]", "'abc'.substring(1)" }) {
        final Expression expression = rightOperandOf("select from Foo where a = " + modified);
        assertThat(expression.isEarlyCalculated(context)).as(modified).isTrue();
        assertThat(expression.isLiteral()).as(modified).isFalse();
      }
    }

    @Test
    void aModifierReachingTheRecordIsNotEarlyCalculated() {
      for (final String modified : new String[] { "'x'.append(b)", "[1, 2][b]", "[1, 2][b - 1]", "'x'.append('y').append(b)",
          "'x'.append(b).append('y')" }) {
        final Expression expression = rightOperandOf("select from Foo where a = " + modified);
        assertThat(expression.isEarlyCalculated(context)).as(modified).isFalse();
        assertThat(expression.isLiteral()).as(modified).isFalse();
      }
    }

    @Test
    void aCaseIsALiteralOnlyWhenEveryBranchIs() {
      assertThat(rightOperandOf("select from Foo where a = CASE 1 WHEN 1 THEN 'x' ELSE 'y' END").isLiteral()).isTrue();
      assertThat(rightOperandOf("select from Foo where a = CASE b WHEN 1 THEN 'x' ELSE 'y' END").isLiteral()).isFalse();
      assertThat(rightOperandOf("select from Foo where a = CASE 1 WHEN 1 THEN b ELSE 'y' END").isLiteral()).isFalse();
      assertThat(rightOperandOf("select from Foo where a = CASE 1 WHEN 1 THEN 'x' ELSE b END").isLiteral()).isFalse();
      // the simple form's WHEN is a boolean expression, which carries no constant-ness of its own
      assertThat(rightOperandOf("select from Foo where a = CASE WHEN 1 = 1 THEN 'x' ELSE 'y' END").isLiteral()).isFalse();
    }

    @Test
    void aCaseIsEarlyCalculatedOnlyWhenNoBranchReadsTheRecord() {
      assertThat(rightOperandOf("select from Foo where a = CASE 1 WHEN 1 THEN 'x' ELSE 'y' END").isEarlyCalculated(
          context)).isTrue();
      assertThat(rightOperandOf("select from Foo where a = CASE b WHEN 1 THEN 'x' ELSE 'y' END").isEarlyCalculated(
          context)).isFalse();
      assertThat(rightOperandOf("select from Foo where a = CASE 1 WHEN 1 THEN b ELSE 'y' END").isEarlyCalculated(
          context)).isFalse();
      assertThat(rightOperandOf("select from Foo where a = CASE WHEN 1 = 1 THEN 'x' ELSE 'y' END").isEarlyCalculated(
          context)).isFalse();
    }

    @Test
    void aTraversalMethodIsNotEarlyCalculated() {
      final Expression expression = rightOperandOf("select from Foo where a = 'x'.out('E')");
      assertThat(expression.isEarlyCalculated(context)).isFalse();
      assertThat(expression.isLiteral()).isFalse();
    }
  }

  /**
   * Regression for #6190: before this, nothing on com.arcadedb.query.sql.executor.SQLFunction told a pure function (abs) from one with...
   * <p>
   * Issue #6190: before this, nothing on {@link com.arcadedb.query.sql.executor.SQLFunction} told a pure function
   * ({@code abs}) from one with a side effect or hidden input ({@code uuid}, {@code sysdate}), so the planner could
   * not cache a plan containing a call, and constant folding refused to reach into one at all - both restrictions are
   * pinned as the pre-existing behaviour in {@code ExpressionEvaluationTest.LiteralExpression} and
   * {@code ConstantFalseFilterFoldingTest#aFilterCallingAFunctionIsNeverFolded}.
   * <p>
   * These pin the two consumers that need no live database: {@link FunctionCall#isCacheable()} (plan caching) and
   * {@link Expression#isFoldable()} (constant folding). The third site the issue names,
   * {@code BinaryCondition#isIndexAware}'s plan-time probe of an indexed equality, is left unchanged - it cannot use
   * this same marker without also losing the probe for a bound parameter (also "early calculated" but never a purity
   * concern), which needs its own predicate; see the PR description.
   */
  @Nested
  class FunctionDeterminism extends AbstractParserTest {

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
     * {@code ExpressionEvaluationTest.EarlyCalculatedModifier}); the literal case needs none.
     */
    @Test
    void theEarlyCalculatedPredicateAdmitsMoreThanFoldableDoes() {
      assertThat(rightOperandOf("select from Foo where a = :p").isEarlyCalculated(context))
          .as("an input parameter is early calculated ...").isTrue();
      assertThat(rightOperandOf("select from Foo where a = :p").isFoldable())
          .as("... but never foldable, unlike isLiteral(true)").isFalse();
    }
  }

  /**
   * Regression for #6401: the identity of a parsed node.
   * <p>
   * Issue #6401, items 3 and 4: the identity of a parsed node.
   * <p>
   * Two independent defects met in the same place. {@link SimpleNode#hashCode()} hashed the ARRAY that
   * {@code getIdentityElements()} returns rather than its contents, and that array is allocated fresh on every call - so
   * one node answered a different hash on every call, and two nodes {@code equals()} to each other answered different
   * hashes, which is the {@link Object#hashCode()} contract broken outright. And the identity of an expression left out
   * fields that carry its VALUE - {@code number} and {@code expression} on {@link BaseExpression}, {@code whereCondition}
   * and the inherited {@code value} slot on {@link Expression} - so the parsed forms of {@code SELECT 1} and
   * {@code SELECT 2} were {@code equals()} to each other, and so was every setting key of a {@code WITH} clause.
   * <p>
   * The two are each other's cover: {@code equals()} said yes where it should have said no, and the hash codes said no
   * to everything, so a {@code HashMap} keyed on a node behaved almost correctly by accident. Fixing either one alone
   * makes a map worse rather than better, which is why both are here.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class NodeIdentity extends AbstractParserTest {

    private Expression projectionOf(final String query) {
      return ((SelectStatement) checkRightSyntax(query)).getProjection().getItems().getFirst().expression;
    }

    /**
     * The contract, on one object: a hash code is a property of the value, so asking twice has to answer twice the same.
     * It did not, because {@code Objects.hashCode(Object)} on an {@code Object[]} is the array's IDENTITY hash and the
     * array is a new one on every call.
     */
    @Test
    void oneNodeAnswersTheSameHashCodeOnEveryCall() {
      final Expression expression = projectionOf("SELECT 1 FROM V");
      assertThat(expression.hashCode()).isEqualTo(expression.hashCode());

      final SimpleNode statement = checkRightSyntax("SELECT a, b FROM V WHERE c > 3");
      assertThat(statement.hashCode()).isEqualTo(statement.hashCode());
    }

    /**
     * And the other half of the contract: two nodes that are equal answer the same hash code. This is the corpus the
     * issue #6401 javadoc above suggested widening to sweep for nodes whose identity does not work (issue #6409, item
     * 3). Widening it surfaced three: {@code foo[1..5]} exercises {@link ArrayRangeSelector}, confirmed by that issue
     * to fall back to reference identity and UNDER-match (two equal parses compared unequal); {@code ORDER BY} and
     * {@code INSERT ... SET} exercise {@link OrderByItem} and {@link InsertSetExpression}, which had the same gap; and
     * a bare {@code CaseExpression} had the opposite failure - no {@code getIdentityElements()} override of its own
     * meant it inherited {@link MathExpression}'s, which reads fields ({@code childExpressions}, {@code operators})
     * {@code CaseExpression} never populates, so it OVER-matched: two structurally different {@code CASE WHEN}
     * expressions compared equal to each other. The rest is a representative spread of statement and expression
     * shapes rather than an exhaustive walk of the ~67 {@code getIdentityElements()} overrides.
     * <p>
     * Issue #6442, item 2 flagged this corpus as still not exhaustive and asked for exactly this kind of widening
     * pass the next time it comes up - not a full audit, one batch of previously-unexercised shapes. This batch
     * added {@code MATCH}, {@code CREATE EDGE}, {@code CREATE INDEX}, {@code LET}, {@code CONTAINS}, {@code
     * INSTANCEOF} and {@code IF}; none of the seven surfaced a defect. That is a real (if less exciting) outcome
     * of the recipe, not a reason to stop following it - the next widening pass is still the same one query at a
     * time, on whichever shapes remain unexercised (window functions and less-common SQL extensions per the issue).
     */
    @Test
    void equalNodesAnswerEqualHashCodes() {
      for (final String query : new String[] { //
          "SELECT 1 FROM V", //
          "SELECT a FROM V", //
          "SELECT a.b[1] FROM V WHERE c = 'x'", //
          "SELECT (1 + 2) * 3 FROM V", //
          "SELECT max(a) FROM V GROUP BY b", //
          "SELECT FROM V WHERE a IN [1, 2, 3]", //
          "SELECT foo[1..5] FROM V", //
          "SELECT foo[1...5] FROM V", //
          "SELECT foo[?..?] FROM V", //
          "SELECT foo[:a..:b] FROM V", //
          "SELECT FROM V WHERE a = ?", //
          "SELECT FROM V WHERE a = :name", //
          "SELECT FROM V WHERE a BETWEEN 1 AND 10", //
          "SELECT FROM V WHERE a LIKE 'foo%'", //
          "SELECT FROM V ORDER BY a DESC LIMIT 10 SKIP 5", //
          "SELECT CASE WHEN a = 1 THEN 'x' ELSE 'y' END FROM V", //
          "SELECT CASE a WHEN 1 THEN 'x' ELSE 'y' END FROM V", //
          "UPDATE V SET a = 1 WHERE b = 2", //
          "DELETE FROM V WHERE a = 1", //
          "INSERT INTO V SET a = 1", //
          "REBUILD INDEX idx WITH batchSize = 100", //
          "REBUILD TYPE Foo WITH batchSize = 100, repartition = true", //
          "IMPORT DATABASE http://foo.bar WITH forceDatabaseCreate = true", //
          "EXPORT DATABASE file://foo.bar WITH format = 'graphml'", //
          "BACKUP DATABASE file://foo.bar WITH compressionLevel = 5", //
          // Widened per issue #6442, item 2: MATCH patterns, CREATE EDGE/INDEX, LET, CONTAINS, INSTANCEOF and IF
          // were not exercised by any query above. None of these five turned up a defect, so the corpus grew but
          // no fix was needed for this batch - see the class javadoc for the recipe if a future widening finds one.
          "MATCH { type: 'V', as: foo}-E->{ type: 'V', as: bar} RETURN foo, bar", //
          "CREATE EDGE Friend FROM #1:1 TO #1:2 SET since = 2020", //
          "CREATE INDEX ON Foo (bar, baz) UNIQUE", //
          "SELECT FROM V LET $temp = (SELECT FROM E)", //
          "SELECT FROM V WHERE tags CONTAINS 'x'", //
          "SELECT FROM V WHERE a INSTANCEOF 'V'", //
          "IF (1 = 1) { SELECT FROM V; }" }) {
        final SimpleNode one = checkRightSyntax(query);
        final SimpleNode other = checkRightSyntax(query);

        assertThat(one).as(query).isEqualTo(other);
        assertThat(one.hashCode()).as(query).isEqualTo(other.hashCode());
      }
    }

    /**
     * The inverse of {@link #equalNodesAnswerEqualHashCodes()}: two DIFFERENT {@code CASE WHEN} expressions must
     * compare unequal. This is the one the corpus test above cannot express - it only ever compares a query against
     * itself - and it is the one that actually caught the {@link CaseExpression} defect (issue #6409, item 3): before
     * the fix, every {@code CASE} expression's identity was the two EMPTY lists it inherited from
     * {@link MathExpression}, so any two of them were equal regardless of their WHEN/THEN content.
     */
    @Test
    void differentCaseExpressionsAreNotTheSameExpression() {
      final Expression one = projectionOf("SELECT CASE WHEN a = 1 THEN 'x' ELSE 'y' END FROM V");
      final Expression two = projectionOf("SELECT CASE WHEN a = 2 THEN 'p' ELSE 'q' END FROM V");

      assertThat(one).as("different WHEN/THEN branches are different expressions").isNotEqualTo(two);
      assertThat(projectionOf("SELECT CASE WHEN a = 1 THEN 'x' ELSE 'y' END FROM V")).as("but the same one is the same").isEqualTo(one);
    }

    /** A bare numeric literal: five of {@code BaseExpression}'s fields are null, and the value lived in the sixth. */
    @Test
    void twoDifferentNumericLiteralsAreNotTheSameExpression() {
      final Expression one = projectionOf("SELECT 1 FROM V");
      final Expression two = projectionOf("SELECT 2 FROM V");

      assertThat(one).as("1 is not 2").isNotEqualTo(two);
      assertThat(projectionOf("SELECT 1 FROM V")).as("but 1 is 1").isEqualTo(one);
    }

    /** The same for the other omitted field: a parenthesised sub-expression parked in {@code expression}. */
    @Test
    void twoDifferentWrappedSubExpressionsAreNotTheSameExpression() {
      final Expression one = projectionOf("SELECT (1 + 2) FROM V");
      final Expression two = projectionOf("SELECT (3 + 4) FROM V");

      assertThat(one).as("(1 + 2) is not (3 + 4)").isNotEqualTo(two);
      assertThat(projectionOf("SELECT (1 + 2) FROM V")).as("but (1 + 2) is (1 + 2)").isEqualTo(one);
    }

    /** Map literals and collections are parked in the same field, so they get the same treatment. */
    @Test
    void twoDifferentCollectionLiteralsAreNotTheSameExpression() {
      assertThat(projectionOf("SELECT [1, 2] FROM V")).isNotEqualTo(projectionOf("SELECT [3, 4] FROM V"));
      assertThat(projectionOf("SELECT {'a': 1} FROM V")).isNotEqualTo(projectionOf("SELECT {'a': 2} FROM V"));
    }

    /**
     * What the contract buys, and the shape the four {@code Map<Expression, Expression> settings} maps in the DDL
     * statements are built out of: a key put in is a key found back, and putting an equal key twice overwrites rather
     * than growing a second entry that {@code get()} can never reach.
     */
    @Test
    void anExpressionUsedAsAHashKeyBehavesLikeAKey() {
      final Map<Expression, Expression> settings = new HashMap<>();
      settings.put(projectionOf("SELECT batchSize FROM V"), projectionOf("SELECT 1 FROM V"));
      settings.put(projectionOf("SELECT batchSize FROM V"), projectionOf("SELECT 2 FROM V"));

      assertThat(settings).as("the same setting named twice is ONE setting").hasSize(1);
      assertThat(settings.get(projectionOf("SELECT batchSize FROM V")))
          .as("and the last spelling of it wins").isEqualTo(projectionOf("SELECT 2 FROM V"));

      final Set<Expression> distinct = new HashSet<>();
      distinct.add(projectionOf("SELECT 1 FROM V"));
      distinct.add(projectionOf("SELECT 1 FROM V"));
      distinct.add(projectionOf("SELECT 2 FROM V"));
      assertThat(distinct).as("two different literals are two elements, and two equal ones are one").hasSize(2);
    }

    /**
     * {@code PNumber} is the node the literal's value lives in, and it had no identity of its own - so listing
     * {@code number} among {@code BaseExpression}'s identity elements would have compared two literals by object
     * identity, which says "not equal" for two parses of the very same statement.
     */
    @Test
    void aNumberNodeComparesByItsValue() {
      final PNumber one = new PNumber();
      one.value = 42;
      final PNumber same = new PNumber();
      same.value = 42;
      final PNumber other = new PNumber();
      other.value = 43;

      assertThat(one).isEqualTo(same);
      assertThat(one.hashCode()).isEqualTo(same.hashCode());
      assertThat(one).isNotEqualTo(other);
      assertThat(one.copy()).as("a copy keeps the identity of what it copied").isEqualTo(one);
    }

    /**
     * The shape that turned item 3 from a trap into a live defect once the hash codes started agreeing with
     * {@code equals()}: before issue #6409 item 1, {@code IMPORT}, {@code EXPORT} and {@code BACKUP DATABASE} parked a
     * setting NAME in the node's inherited {@code value} slot rather than building an identifier expression out of it,
     * and {@code value} was not among {@link Expression}'s identity elements - so every key of a {@code WITH} clause
     * was a node whose every other field is {@code null}/{@code false}, which is to say equal to every other key.
     * <p>
     * The key is now built the same way {@code REBUILD INDEX} and {@code REBUILD TYPE} already built it - {@code new
     * Expression(Identifier)} - so it is read back with {@code toString()}, not the raw {@code value} slot.
     */
    @Test
    void everySettingKeyOfAWithClauseIsItsOwnKey() {
      final ImportDatabaseStatement statement = (ImportDatabaseStatement) checkRightSyntax(
          "IMPORT DATABASE http://www.foo.bar WITH forceDatabaseCreate = true, commitEvery = 10000");

      assertThat(statement.settings).as("two settings are two entries").hasSize(2);
      assertThat(statement.settings.keySet().stream().map(Expression::toString))
          .containsExactlyInAnyOrder("forceDatabaseCreate", "commitEvery");
    }

    /**
     * {@code whereCondition} is the other field {@link Expression#execute} reads that identity used to ignore: a
     * parenthesised boolean condition lives there, and two different ones were the same node.
     */
    @Test
    void twoDifferentInlineConditionsAreNotTheSameExpression() {
      final Expression one = projectionOf("SELECT (a = 1 AND b = 2) FROM V");
      final Expression two = projectionOf("SELECT (a = 1 AND b = 3) FROM V");

      assertThat(one.whereCondition).as("the shape under test parks the condition in whereCondition").isNotNull();
      assertThat(one).isNotEqualTo(two);
      assertThat(projectionOf("SELECT (a = 1 AND b = 2) FROM V")).isEqualTo(one);
    }

    /** A node with no identity of its own keeps the default: itself, and stably so. */
    @Test
    void aNodeWithoutIdentityElementsStillHonoursTheContract() {
      final Timeout timeout = new Timeout();
      assertThat(timeout.hashCode()).isEqualTo(timeout.hashCode());
      assertThat(timeout).isEqualTo(timeout);
    }
  }

  /**
   * Regression for #6409: three follow-ups from #6401 that were not that issue's to fix.
   * <p>
   * Issue #6409, items 1, 2 and 4: three follow-ups from #6401 that were not that issue's to fix.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Followups extends AbstractParserTest {

    /**
     * Item 1: {@code IMPORT}, {@code EXPORT} and {@code BACKUP DATABASE} used to park a setting NAME in the node's
     * inherited {@code value} slot rather than building an identifier expression out of it, the way {@code REBUILD
     * INDEX} and {@code REBUILD TYPE} already did. All five now build the key the same way, so all five read it back
     * the same way too: {@code toString()}, not the raw {@code value} slot.
     */
    @Test
    void allFiveWithSettingsStatementsBuildTheSameKeyShape() throws Exception {
      final ImportDatabaseStatement imp = (ImportDatabaseStatement) new SQLAntlrParser(null).parse(
          "IMPORT DATABASE http://foo.bar WITH forceDatabaseCreate = true");
      final ExportDatabaseStatement exp = (ExportDatabaseStatement) new SQLAntlrParser(null).parse(
          "EXPORT DATABASE file://foo.bar WITH format = 'graphml'");
      final BackupDatabaseStatement bak = (BackupDatabaseStatement) new SQLAntlrParser(null).parse(
          "BACKUP DATABASE file://foo.bar WITH compressionLevel = 5");
      final RebuildIndexStatement idx = (RebuildIndexStatement) new SQLAntlrParser(null).parse(
          "REBUILD INDEX Foo WITH batchSize = 1000");
      final RebuildTypeStatement typ = (RebuildTypeStatement) new SQLAntlrParser(null).parse(
          "REBUILD TYPE Foo WITH batchSize = 1000");

      assertThat(imp.settings.keySet()).as("IMPORT").allMatch(k -> k.isBaseIdentifier());
      assertThat(exp.settings.keySet()).as("EXPORT").allMatch(k -> k.isBaseIdentifier());
      assertThat(bak.settings.keySet()).as("BACKUP").allMatch(k -> k.isBaseIdentifier());
      assertThat(idx.settings.keySet()).as("REBUILD INDEX").allMatch(k -> k.isBaseIdentifier());
      assertThat(typ.settings.keySet()).as("REBUILD TYPE").allMatch(k -> k.isBaseIdentifier());

      assertThat(imp.settings.keySet().iterator().next().value).as("no raw value slot any more").isNull();
      assertThat(imp.settings.keySet().stream().map(Expression::toString)).containsExactly("forceDatabaseCreate");
      assertThat(exp.settings.keySet().stream().map(Expression::toString)).containsExactly("format");
      assertThat(bak.settings.keySet().stream().map(Expression::toString)).containsExactly("compressionLevel");
    }

    /**
     * The historical defect the old shape produced (issue #6080 / #6359, item 2): a backup asked to be encrypted was
     * silently written in clear because the setting name could not be read back. Guards against a regression on the
     * executable path, not just the parsed shape.
     */
    @Test
    void importExportBackupSettingsRoundTripThroughToString() {
      checkRightSyntax("IMPORT DATABASE http://foo.bar WITH forceDatabaseCreate = true, commitEvery = 10000");
      checkRightSyntax("EXPORT DATABASE file://foo.bar WITH format = 'graphml'");
      checkRightSyntax("BACKUP DATABASE file://foo.bar WITH compressionLevel = 5, encryptionKey = 'secret'");
    }

    /**
     * Item 2: before this, only {@code REBUILD TYPE} refused a repeated setting; the other four silently accepted it,
     * last one wins. All five now agree, and the message names the actual statement.
     */
    @Test
    void allFiveStatementsRefuseADuplicateSetting() {
      assertThatThrownBy(() -> new SQLAntlrParser(null).parse("REBUILD INDEX Foo WITH batchSize = 1, batchSize = 2"))
          .isInstanceOf(CommandSQLParsingException.class)
          .hasMessageContaining("REBUILD INDEX").hasMessageContaining("duplicate setting");

      assertThatThrownBy(() -> new SQLAntlrParser(null).parse("REBUILD TYPE Foo WITH batchSize = 1, batchSize = 2"))
          .isInstanceOf(CommandSQLParsingException.class)
          .hasMessageContaining("REBUILD TYPE").hasMessageContaining("duplicate setting");

      assertThatThrownBy(() -> new SQLAntlrParser(null).parse("IMPORT DATABASE http://foo.bar WITH batchSize = 1, batchSize = 2"))
          .isInstanceOf(CommandSQLParsingException.class)
          .hasMessageContaining("IMPORT DATABASE").hasMessageContaining("duplicate setting");

      assertThatThrownBy(() -> new SQLAntlrParser(null).parse("EXPORT DATABASE file://foo.bar WITH batchSize = 1, batchSize = 2"))
          .isInstanceOf(CommandSQLParsingException.class)
          .hasMessageContaining("EXPORT DATABASE").hasMessageContaining("duplicate setting");

      assertThatThrownBy(() -> new SQLAntlrParser(null).parse("BACKUP DATABASE file://foo.bar WITH batchSize = 1, batchSize = 2"))
          .isInstanceOf(CommandSQLParsingException.class)
          .hasMessageContaining("BACKUP DATABASE").hasMessageContaining("duplicate setting");
    }

    /**
     * The dedup key is the LOWERCASED name - matching the {@code equalsIgnoreCase} every reader already uses to
     * recognise a setting - not {@link Expression} identity, which is case-sensitive since #6401. A case-only repeat
     * ({@code batchSize} / {@code BATCHSIZE}) has to be caught too, in every one of the five statements.
     */
    @Test
    void aCaseOnlyRepeatIsStillADuplicateInEveryStatement() {
      assertThatThrownBy(() -> new SQLAntlrParser(null).parse("REBUILD INDEX Foo WITH batchSize = 1, BATCHSIZE = 2"))
          .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("duplicate setting");

      assertThatThrownBy(() -> new SQLAntlrParser(null).parse("IMPORT DATABASE http://foo.bar WITH batchSize = 1, BATCHSIZE = 2"))
          .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("duplicate setting");

      assertThatThrownBy(() -> new SQLAntlrParser(null).parse("EXPORT DATABASE file://foo.bar WITH batchSize = 1, BATCHSIZE = 2"))
          .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("duplicate setting");

      assertThatThrownBy(() -> new SQLAntlrParser(null).parse("BACKUP DATABASE file://foo.bar WITH batchSize = 1, BATCHSIZE = 2"))
          .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("duplicate setting");
    }

    /** Two DIFFERENT settings, or the same setting spelled once, must keep working - only an actual repeat is refused. */
    @Test
    void distinctSettingsAreNotFlaggedAsDuplicates() throws Exception {
      final RebuildIndexStatement idx = (RebuildIndexStatement) new SQLAntlrParser(null).parse(
          "REBUILD INDEX Foo WITH batchSize = 1000, maxAttempts = 5, statsOnly = true");
      assertThat(idx.settings).hasSize(3);

      final ImportDatabaseStatement imp = (ImportDatabaseStatement) new SQLAntlrParser(null).parse(
          "IMPORT DATABASE http://foo.bar WITH forceDatabaseCreate = true, commitEvery = 10000");
      assertThat(imp.settings).hasSize(2);
    }

    /**
     * Found while addressing code review on this very PR, in the same sweep as item 3 but on a statement node rather
     * than an expression node: {@code ImportDatabaseStatement#equals()} only compared {@code url}, and
     * {@code ExportDatabaseStatement#getIdentityElements()} only listed {@code url} - so two statements with the same
     * URL but DIFFERENT {@code WITH} settings compared equal. For {@code IMPORT DATABASE} this was the sharper of the
     * two: it has no URL at all for a CSV import, so the settings ARE the statement.
     * {@link BackupDatabaseStatement} already included {@code settings} in its identity; the other two now match it.
     */
    @Test
    void importAndExportStatementsWithDifferentSettingsAreNotEqual() throws Exception {
      final ImportDatabaseStatement importOne = (ImportDatabaseStatement) new SQLAntlrParser(null).parse(
          "IMPORT DATABASE http://foo.bar WITH forceDatabaseCreate = true");
      final ImportDatabaseStatement importTwo = (ImportDatabaseStatement) new SQLAntlrParser(null).parse(
          "IMPORT DATABASE http://foo.bar WITH forceDatabaseCreate = false");
      assertThat(importOne).as("same URL, different settings").isNotEqualTo(importTwo);
      assertThat((ImportDatabaseStatement) new SQLAntlrParser(null).parse("IMPORT DATABASE http://foo.bar WITH forceDatabaseCreate = true"))
          .as("but the same statement is still the same").isEqualTo(importOne);

      final ExportDatabaseStatement exportOne = (ExportDatabaseStatement) new SQLAntlrParser(null).parse(
          "EXPORT DATABASE file://foo.bar WITH format = 'graphml'");
      final ExportDatabaseStatement exportTwo = (ExportDatabaseStatement) new SQLAntlrParser(null).parse(
          "EXPORT DATABASE file://foo.bar WITH format = 'jsonl'");
      assertThat(exportOne).as("same URL, different settings").isNotEqualTo(exportTwo);
      assertThat(exportOne.hashCode()).isNotEqualTo(exportTwo.hashCode());
    }

    /**
     * Item 4: {@link SimpleNode#equals(Object)} indexed the OTHER node's identity array by THIS node's length, with no
     * guard. Safe today because {@code getClass() == other.getClass()} means both arrays come from the same override,
     * and every override returns a fixed-length array literal - but nothing enforces that invariant. This subclass
     * builds its array conditionally, the shape the issue calls out as the one that would otherwise throw
     * {@link ArrayIndexOutOfBoundsException} out of {@code equals()}.
     */
    private static final class VariableArityNode extends SimpleNode {
      private final Object[] elements;

      private VariableArityNode(final Object... elements) {
        this.elements = elements;
      }

      @Override
      protected Object[] getIdentityElements() {
        return elements;
      }
    }

    @Test
    void equalsGuardsAgainstAVariableLengthIdentityArray() {
      final VariableArityNode shortOne = new VariableArityNode("a");
      final VariableArityNode longOne = new VariableArityNode("a", "b");

      assertThat(shortOne).as("different arity is never equal, and must not throw").isNotEqualTo(longOne);
      assertThat(longOne).isNotEqualTo(shortOne);
      assertThat(shortOne.hashCode()).as("hashCode still has to be computable").isEqualTo(Objects.hash("a"));
    }
  }

  /**
   * Regression for #10655: PositionalParameter looks its value up under the key String.valueOf(paramNumber);
   * <p>
   * Issue #10655 (item 1): {@link PositionalParameter} looks its value up under the key {@code String.valueOf(paramNumber)};
   * the key for small numbers now comes from a table instead of being built on every evaluation. Lookup semantics must not
   * change, for numbers inside the table, at its edges and above it.
   */
  @Nested
  class PositionalParameterKey extends TestHelper {

    private static final int[] NUMBERS = { 0, 1, 9, 10, 31, 62, 63, 64, 65, 99, 100, 1_000, 123_456, Integer.MAX_VALUE, -1, -64 };

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE DOCUMENT TYPE V");
      database.transaction(() -> {
        for (int i = 0; i < 10; i++)
          database.newDocument("V").set("a", (long) i).save();
      });
    }

    @Test
    void getValueAndBindResolveTheKeyOfEveryNumber() {
      for (final int number : NUMBERS) {
        final PositionalParameter parameter = new PositionalParameter();
        parameter.paramNumber = number;

        final Map<String, Object> params = new HashMap<>();
        params.put(String.valueOf(number), "v" + number);
        // a neighbour under another key must never be picked
        params.put(String.valueOf(number + 1), "other");

        assertThat(parameter.getValue(params)).as("getValue %d", number).isEqualTo("v" + number);
        // bind returns the parsed tree of the value under this number's key, never the neighbour's
        assertThat(String.valueOf(parameter.bindFromInputParams(params))).as("bind %d", number)
            .isEqualTo(String.valueOf(parameter.toParsedTree("v" + number)))
            .doesNotContain("other");
        assertThat(parameter.getValue(new HashMap<>())).as("absent %d", number).isNull();
        assertThat(parameter.getValue(null)).isNull();
        assertThat(parameter.bindFromInputParams(null)).isSameAs(parameter);
      }
    }

    @Test
    void theKeyFollowsAChangeOfTheNumber() {
      final PositionalParameter parameter = new PositionalParameter();
      final Map<String, Object> params = new HashMap<>();
      params.put("3", "three");
      params.put("70", "seventy");

      parameter.paramNumber = 3;
      assertThat(parameter.getValue(params)).isEqualTo("three");
      parameter.paramNumber = 70;
      assertThat(parameter.getValue(params)).isEqualTo("seventy");
      parameter.paramNumber = 3;
      assertThat(parameter.getValue(params)).isEqualTo("three");
    }

    @Test
    void aStatementWithMoreParametersThanTheTableStillBindsThemAll() {
      final int count = 100;
      final StringBuilder sql = new StringBuilder("SELECT FROM V WHERE a IN [");
      final Object[] args = new Object[count];
      for (int i = 0; i < count; i++) {
        sql.append(i == 0 ? "?" : ", ?");
        args[i] = 1_000L + i; // matches nothing...
      }
      sql.append("]");
      args[count - 1] = 7L; // ...except the last one, parameter number 99, which lies above the table
      try (final ResultSet rs = database.query("sql", sql.toString(), args)) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        assertThat(row.<Long>getProperty("a")).isEqualTo(7L);
        assertThat(rs.hasNext()).isFalse();
      }

      // a filtered scan evaluates the same parameter for every row
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM V WHERE a >= ? AND a < ?", 3L, 8L)) {
        assertThat(rs.next().<Long>getProperty("c")).isEqualTo(5L);
      }
    }
  }

  /**
   * Regression for #9925: reading a property of a scanned record repeated, for every row, string work that depends only on the query...
   * <p>
   * Issue #9925: reading a property of a scanned record repeated, for every row, string work that depends only on the query
   * text - unescaping the identifier, testing it for the Postgres wire's {@code $N} shape, and probing the parameter map for a
   * colon-prefixed spelling of a generated literal parameter. These tests pin that the work is now done once per identifier, and
   * that every answer it feeds stays what it was. The last ones pin two bugs found on the way: a caller could replace a literal of
   * the query text by binding {@code ":__lit_..."}, and a {@code $N} too large for an int failed the query.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class PerRowIdentifierWork extends TestHelper {

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE DOCUMENT TYPE V");
      database.command("sql", "CREATE PROPERTY V.a LONG");
      database.command("sql", "CREATE PROPERTY V.name STRING");
      database.transaction(() -> {
        for (int i = 0; i < 10; i++)
          database.newDocument("V").set("a", (long) i).set("name", "n" + i).set("a`b\\c", "escaped" + i).save();
      });
    }

    @Test
    void anIdentifierUnescapesItsNameOnce() {
      final Identifier plain = new Identifier("l_quantity");
      assertThat(plain.getStringValue()).isEqualTo("l_quantity");
      assertThat(plain.getStringValue()).isSameAs(plain.getValue());

      // a name that needs escaping: unescaping it builds a new String, so a second read returning the same instance proves the
      // plain name is kept rather than rebuilt per call
      final Identifier escaped = new Identifier("a`b\\c");
      assertThat(escaped.getValue()).isEqualTo("a\\`b\\\\c");
      assertThat(escaped.getStringValue()).isEqualTo("a`b\\c");
      assertThat(escaped.getStringValue()).isSameAs(escaped.getStringValue());

      final Identifier quoted = Identifier.quoted("`x\\`y`");
      assertThat(quoted.getValue()).isEqualTo("x\\`y");
      assertThat(quoted.getStringValue()).isEqualTo("x`y");
      assertThat(quoted.getStringValue()).isSameAs(quoted.getStringValue());
    }

    @Test
    void anIdentifierFollowsEveryWriteOfItsName() {
      final Identifier identifier = new Identifier("first");
      assertThat(identifier.getStringValue()).isEqualTo("first");

      identifier.setStringValue("se`cond");
      assertThat(identifier.getStringValue()).isEqualTo("se`cond");
      assertThat(identifier.positionalParameterKey()).isNull();

      identifier.setQuotedStringValue("`$3`");
      assertThat(identifier.getStringValue()).isEqualTo("$3");
      assertThat(identifier.positionalParameterKey()).isEqualTo("2");

      identifier.setStringValue(null);
      assertThat(identifier.getStringValue()).isNull();
      assertThat(identifier.positionalParameterKey()).isNull();

      final Identifier copy = new Identifier(new Identifier("a`b"), true);
      assertThat(copy.getStringValue()).isEqualTo("a`b");
      assertThat(new Identifier(new Identifier("$7"), false).positionalParameterKey()).isEqualTo("6");
    }

    @Test
    void anIdentifierKnowsWhetherItIsAPostgresPositionalParameter() {
      assertThat(new Identifier("$1").positionalParameterKey()).isEqualTo("0");
      assertThat(new Identifier("$12").positionalParameterKey()).isEqualTo("11");
      assertThat(new Identifier("$" + Integer.MAX_VALUE).positionalParameterKey()).isEqualTo(String.valueOf(Integer.MAX_VALUE - 1));

      assertThat(new Identifier("$").positionalParameterKey()).isNull();
      assertThat(new Identifier("$0").positionalParameterKey()).as("Postgres positions start at 1").isNull();
      assertThat(new Identifier("$1a").positionalParameterKey()).isNull();
      assertThat(new Identifier("$parent").positionalParameterKey()).isNull();
      assertThat(new Identifier("name").positionalParameterKey()).isNull();
      assertThat(new Identifier("1").positionalParameterKey()).isNull();
      assertThat(new Identifier("$99999999999").positionalParameterKey()).as("beyond any position a caller can bind").isNull();
    }

    @Test
    void aPostgresPositionalParameterStillReadsItsBoundValue() {
      final Map<String, Object> params = new HashMap<>();
      params.put("0", 4L);
      try (final ResultSet rs = database.query("sql", "SELECT name FROM V WHERE a = $1", params)) {
        assertThat(rs.next().<String>getProperty("name")).isEqualTo("n4");
        assertThat(rs.hasNext()).isFalse();
      }

      // with nothing bound at its position, $2 is not a parameter: it reads as an (unset) variable
      try (final ResultSet rs = database.query("sql", "SELECT $2 AS v, name FROM V WHERE a = $1", params)) {
        assertThat(rs.next().<Object>getProperty("v")).isNull();
      }
    }

    @Test
    void aPositionalShapeTooLargeForAnIntIsNotAParameter() {
      // used to fail with a NumberFormatException as soon as any parameter was bound
      final Map<String, Object> params = new HashMap<>();
      params.put("0", 4L);
      try (final ResultSet rs = database.query("sql", "SELECT $99999999999 AS v FROM V WHERE a = $1", params)) {
        assertThat(rs.next().<Object>getProperty("v")).isNull();
        assertThat(rs.hasNext()).isFalse();
      }
    }

    @Test
    void dollarNamesStillResolveAfterTheFirstCharacterGate() {
      // $parent, reached through the Result overload of a sub-query's WHERE
      try (final ResultSet rs = database.query("sql",
          "SELECT a, (SELECT count(*) AS c FROM V WHERE a = $parent.$current.a)[0].c AS same FROM V WHERE a = 2")) {
        assertThat(rs.next().<Long>getProperty("same")).isEqualTo(1L);
      }

      // a LET variable carried as record metadata
      try (final ResultSet rs = database.query("sql", "SELECT $x AS v FROM V LET $x = a * 2 WHERE a = 3")) {
        assertThat(rs.next().<Number>getProperty("v").longValue()).isEqualTo(6L);
      }
    }

    @Test
    void aPlainPropertyIsNotShadowedByAVariableOfTheSameName() {
      ((DatabaseInternal) database).setGlobalVariable("name", "shadow");
      try {
        try (final ResultSet rs = database.query("sql", "SELECT name, $name AS g FROM V WHERE a = 1")) {
          final Result row = rs.next();
          assertThat(row.<String>getProperty("name")).isEqualTo("n1");
          assertThat(row.<String>getProperty("g")).isEqualTo("shadow");
        }
      } finally {
        ((DatabaseInternal) database).setGlobalVariable("name", null);
      }
    }

    @Test
    void anIdentifierWithoutANameEvaluatesToNull() {
      final SuffixIdentifier suffix = new SuffixIdentifier(new Identifier((String) null));
      assertThat(suffix.execute((Result) null, null)).isNull();
    }

    @Test
    void anEscapedPropertyNameStillReadsItsValue() {
      try (final ResultSet rs = database.query("sql", "SELECT `a\\`b\\\\c` AS v FROM V WHERE a = 3")) {
        assertThat(rs.next().<String>getProperty("v")).isEqualTo("escaped3");
      }
    }

    @Test
    void aCallerCannotReplaceALiteralOfTheQueryText() {
      // the same shape with other values, so the literal is extracted into a generated parameter
      for (int i = 0; i < 3; i++)
        try (final ResultSet rs = database.query("sql", "SELECT a FROM V WHERE name = 'n" + i + "'")) {
          assertThat(rs.next().<Long>getProperty("a")).isEqualTo((long) i);
        }

      final String text = "SELECT a FROM V WHERE name = 'n5'";
      final Lookup<Statement> lookup = ((DatabaseInternal) database).getStatementCache().getParameterized(text);
      assertThat(lookup.parameters()).as("the literal is extracted").isNotNull().isNotEmpty();

      // binding the colon-prefixed spelling of every generated name must not change what the text says
      final Map<String, Object> params = new HashMap<>();
      for (final String name : lookup.parameters().keySet()) {
        assertThat(name).startsWith(LiteralParameterizer.PARAMETER_PREFIX);
        params.put(":" + name, "n7");
      }
      try (final ResultSet rs = database.query("sql", text, params)) {
        assertThat(rs.next().<Long>getProperty("a")).isEqualTo(5L);
        assertThat(rs.hasNext()).isFalse();
      }
    }

    @Test
    void aGeneratedParameterReadsOnlyItsPlainName() {
      final NamedParameter generated = new NamedParameter();
      generated.paramName = LiteralParameterizer.PARAMETER_PREFIX + "s0";
      generated.paramNumber = 2;

      final Map<String, Object> params = new HashMap<>();
      params.put(":" + generated.paramName, "caller");
      params.put(generated.paramName, "literal");
      assertThat(generated.getValue(params)).isEqualTo("literal");
      assertThat(generated.copy().getValue(params)).isEqualTo("literal");

      // a name renamed into or out of the generated namespace follows the rule of its current name
      generated.paramName = "p";
      params.put(":p", "prefixed");
      params.put("p", "plain");
      assertThat(generated.getValue(params)).isEqualTo("prefixed");
      generated.paramName = LiteralParameterizer.PARAMETER_PREFIX + "s0";
      assertThat(generated.getValue(params)).isEqualTo("literal");
    }

    @Test
    void aUserNamedParameterStillPrefersItsColonPrefixedKey() {
      final Map<String, Object> params = new HashMap<>();
      params.put(":name", "n2");
      params.put("name", "n3");
      try (final ResultSet rs = database.query("sql", "SELECT a FROM V WHERE name = :name", params)) {
        assertThat(rs.next().<Long>getProperty("a")).isEqualTo(2L);
      }
    }
  }
}
