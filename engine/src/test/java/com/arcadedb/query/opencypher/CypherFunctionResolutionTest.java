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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.exception.ErrorCategory;
import com.arcadedb.exception.QueryNotIdempotentException;
import com.arcadedb.function.CypherBuiltinFunctions;
import com.arcadedb.function.CypherFunctionRegistry;
import com.arcadedb.function.FunctionDefinition;
import com.arcadedb.function.FunctionLibraryDefinition;
import com.arcadedb.function.FunctionRegistry;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.function.sql.DefaultSQLFunctionFactory;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.query.OperationType;
import com.arcadedb.query.opencypher.executor.CypherFunctionFactory;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.SQLFunction;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Function resolution: unknown and namespaced functions, apoc.util.validate and dotted aggregate names.
 * <p>
 * Each nested class holds the regression tests of the GitHub issue its name carries.
 */
class CypherFunctionResolutionTest {
  /**
   * Regression tests for issue #6418: {@code SimpleCypherStatement.isReadOnly()} only inspected top-level
   * {@code CALL} clauses resolving to a registered {@code CypherProcedure}. A call into a user-defined
   * {@code DEFINE FUNCTION} - in {@code CALL} position or, as here, in plain expression position
   * ({@code WITH library.fn() AS x}, {@code RETURN library.fn()}) - was invisible to it:
   * {@code SQLFunctionDefinition.execute} runs the function body as a full {@code sqlscript} command, which can
   * itself {@code CREATE}/{@code UPDATE}/{@code DELETE}, yet the statement was classified read-only.
   * <p>
   * Mirrors {@code CypherProcedureCallTest.Issue6094WriteProcedureCallClassification}, the direct precedent for this same flag and the
   * same three consequences (HA leader routing, {@code Database.query()}'s idempotency gate, and
   * {@code analyze().getOperationTypes()}), for the shape #6094 already covers - a registered write
   * {@code CypherProcedure} in {@code CALL} position - but which does not reach a {@code DEFINE FUNCTION} call
   * outside a {@code CALL} clause at all.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue6418UnresolvedFunctionCallClassification {
    private Database database;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6418");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.getSchema().createVertexType("Issue6418Person");
      database.command("sql",
          "DEFINE FUNCTION test6418.createPerson \"INSERT INTO Issue6418Person SET name = :n RETURN @rid\" PARAMETERS [n] LANGUAGE sql");
      database.command("sql", "DEFINE FUNCTION test6418.echo \"SELECT :a AS result\" PARAMETERS [a] LANGUAGE sql");
    }

    @AfterEach
    void teardown() {
      if (database != null)
        database.drop();
    }

    private boolean isIdempotent(final String query) {
      return database.getQueryEngine("opencypher").analyze(query).isIdempotent();
    }

    @Test
    void writeFunctionCallInWithClauseIsNotIdempotent() {
      assertThat(isIdempotent("WITH test6418.createPerson('Alice') AS r RETURN r")).isFalse();
    }

    @Test
    void writeFunctionCallInReturnClauseIsNotIdempotent() {
      assertThat(isIdempotent("RETURN test6418.createPerson('Bob')")).isFalse();
    }

    @Test
    void writeFunctionCallInsideCallSubqueryIsNotIdempotent() {
      assertThat(isIdempotent(
          "CALL { WITH test6418.createPerson('Carol') AS r RETURN r } RETURN r")).isFalse();
    }

    /**
     * A read-only user-defined SQL function (a plain projection, no write inside its body) is a name this
     * classifier cannot confirm pure either - it has no database access at parse time - so it is conservatively
     * treated the same as a writing one. Costs the caller its idempotent-query fast path, never correctness: the
     * opposite misclassification is the one #6418 is about.
     */
    @Test
    void readOnlyCustomFunctionIsConservativelyNotIdempotentEither() {
      assertThat(isIdempotent("RETURN test6418.echo('x')")).isFalse();
    }

    @Test
    void builtinFunctionCallsStayIdempotent() {
      assertThat(isIdempotent("RETURN toUpper('x')")).isTrue();
      assertThat(isIdempotent("WITH abs(-1) AS a RETURN a")).isTrue();
      assertThat(isIdempotent("RETURN text.indexOf('abc', 'b')")).isTrue();
      assertThat(isIdempotent("RETURN sql.abs(-1)")).isTrue();
      assertThat(isIdempotent("MATCH (n:Issue6418Person) RETURN n")).isTrue();
    }

    /**
     * The trickiest branch of {@code isConfirmedPureFunctionName}: the parser rewrites the underscored
     * {@code vector_norm}/{@code vector_distance} spellings into a dotted SQL bridge name ({@code vector.magnitude},
     * {@code vector.l2Distance}, ...) before a {@code FunctionCallExpression} is ever built, so these land in the
     * classifier as dotted names resolved via {@code DefaultSQLFunctionFactory}'s direct (unprefixed, unmapped)
     * lookup rather than via {@link com.arcadedb.function.CypherFunctionRegistry} or
     * {@link com.arcadedb.function.CypherBuiltinFunctions}. Pinned here directly rather than only transitively
     * through execution, since a regression in this branch specifically would misclassify every dotted built-in
     * that isn't in the other two sources.
     */
    @Test
    void dottedBuiltinsResolvedOnlyThroughTheSqlFunctionFactoryStayIdempotent() {
      assertThat(isIdempotent(
          "RETURN vector_norm(vector([1.0, 5.0, 3.0, 6.7], 4, FLOAT32), EUCLIDEAN) AS norm")).isTrue();
      assertThat(isIdempotent(
          "RETURN vector_distance(vector([1.0, 5.0, 3.0, 6.7], 4, FLOAT32), vector([5.0, 2.5, 3.1, 9.0], 4, FLOAT32), EUCLIDEAN) AS distance"))
          .isTrue();
    }

    @Test
    void queryRejectsWriteFunctionCallInExpressionPosition() {
      assertThatThrownBy(() -> database.query("opencypher", "RETURN test6418.createPerson('Dave')"))
          .isInstanceOf(QueryNotIdempotentException.class);
    }

    /**
     * {@code CallStep} auto-commits around a {@code CALL} to a write procedure (#6073), but a write reached only
     * through a function evaluated during projection has no step of its own to hang that convenience off - it is
     * not something {@code isReadOnly()} controls either way, on this path or before this fix. An explicit
     * transaction is the general contract for an embedded write {@code command()} outside that one convenience, and
     * is what proves the write behind the function call actually executes.
     */
    @Test
    void commandStillExecutesWriteFunctionCallInExpressionPosition() {
      database.begin();
      try (ResultSet rs = database.command("opencypher", "RETURN test6418.createPerson('Eve')")) {
        assertThat(rs.hasNext()).isTrue();
      }
      database.commit();
      try (ResultSet check = database.query("sql", "SELECT FROM Issue6418Person WHERE name = 'Eve'")) {
        assertThat(check.hasNext()).isTrue();
      }
    }

    @Test
    void operationTypesOfAWriteFunctionCallInExpressionPositionAreNotReadOnly() {
      assertThat(database.getQueryEngine("opencypher")
          .analyze("RETURN test6418.createPerson('Frank')")
          .getOperationTypes())
          .doesNotContain(OperationType.READ);
    }
  }

  /**
   * Issue #9768, the openCypher half: {@code vector.sum}, {@code vector.avg}, {@code vector.min} and {@code vector.max} were
   * called once per row and answered a NULL for each, because {@code FunctionCallExpression} knew which calls aggregate from a
   * list of names kept beside the planner, and the namespaced aggregates of the function registry were not on it. In Cypher these
   * are the ONLY spelling of them: the camelCase aliases of the SQL side are not Cypher names.
   * <p>
   * Per the openCypher reference an aggregate is a function that folds the rows of its group into one value, whatever its name;
   * the registry is what knows that.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue9768CypherDottedAggregate extends TestHelper {

    private void load() {
      final var type = database.getSchema().createVertexType("D");
      type.createProperty("g", Type.INTEGER);
      type.createProperty("n", Type.INTEGER);
      type.createProperty("v", Type.ARRAY_OF_FLOATS);
      database.transaction(() -> {
        database.newVertex("D").set("g", 0).set("n", 1).set("v", new float[] { 1f, 2f }).save();
        database.newVertex("D").set("g", 1).set("n", 2).set("v", new float[] { 2f, 1f }).save();
        database.newVertex("D").set("g", 0).set("n", 3).set("v", new float[] { 1f, 2f }).save();
        database.newVertex("D").set("g", 1).set("n", 4).set("v", new float[] { 2f, 3f }).save();
      });
    }

    private List<Result> run(final String cypher) {
      final List<Result> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", cypher)) {
        while (rs.hasNext())
          rows.add(rs.next());
      }
      return rows;
    }

    private static float[] vector(final Result row, final String property) {
      final Object value = row.getProperty(property);
      assertThat(value).as(property).isNotNull();
      if (value instanceof float[] floats)
        return floats;
      final List<?> list = (List<?>) value;
      final float[] result = new float[list.size()];
      for (int i = 0; i < result.length; i++)
        result[i] = ((Number) list.get(i)).floatValue();
      return result;
    }

    @Test
    void dottedAggregatesAnswerOneRowWithTheFoldedValue() {
      load();
      final List<Result> sum = run("MATCH (d:D) RETURN vector.sum(d.v) AS s");
      assertThat(sum).hasSize(1);
      assertThat(vector(sum.getFirst(), "s")).containsExactly(6f, 8f);

      assertThat(vector(run("MATCH (d:D) RETURN vector.avg(d.v) AS s").getFirst(), "s")).containsExactly(1.5f, 2f);
      assertThat(vector(run("MATCH (d:D) RETURN vector.min(d.v) AS s").getFirst(), "s")).containsExactly(1f, 1f);
      assertThat(vector(run("MATCH (d:D) RETURN vector.max(d.v) AS s").getFirst(), "s")).containsExactly(2f, 3f);
    }

    @Test
    void dottedAggregateNextToAnotherAggregateAndGroupedByAKey() {
      load();
      final List<Result> together = run("MATCH (d:D) RETURN vector.sum(d.v) AS s, count(d) AS c");
      assertThat(together).hasSize(1);
      assertThat(together.getFirst().<Long>getProperty("c")).isEqualTo(4L);
      assertThat(vector(together.getFirst(), "s")).containsExactly(6f, 8f);

      final List<Result> grouped = run("MATCH (d:D) RETURN d.g AS g, vector.sum(d.v) AS s ORDER BY g");
      assertThat(grouped).hasSize(2);
      assertThat(vector(grouped.get(0), "s")).containsExactly(2f, 4f);
      assertThat(vector(grouped.get(1), "s")).containsExactly(4f, 4f);
    }

    @Test
    void theExplicitSqlPrefixAggregatesToo() {
      load();
      final List<Result> rows = run("MATCH (d:D) RETURN sql.vector.sum(d.v) AS s");
      assertThat(rows).hasSize(1);
      assertThat(vector(rows.getFirst(), "s")).containsExactly(6f, 8f);

      final List<Result> sum = run("MATCH (d:D) RETURN sql.sum(d.n) AS s");
      assertThat(sum).hasSize(1);
      assertThat(((Number) sum.getFirst().getProperty("s")).longValue()).isEqualTo(10L);
    }

    @Test
    void aNamespacedScalarStaysPerRow() {
      load();
      assertThat(run("MATCH (d:D) RETURN vector.magnitude(d.v) AS m")).hasSize(4);
    }

    @Test
    void aCypherScalarThatSharesANameWithASqlAggregateStaysPerRow() {
      load();
      // last() folds the rows in SQL; in Cypher it is the last element of a list
      final List<Result> rows = run("MATCH (d:D) RETURN last([1, 2, 3]) AS l");
      assertThat(rows).hasSize(4);
      assertThat(((Number) rows.getFirst().getProperty("l")).intValue()).isEqualTo(3);
    }

    /** A function that validates its arguments when it is configured, which the planner's probe configuration must survive. */
    public static class StrictConfig extends SQLFunctionAbstract {
      public StrictConfig() {
        super("acme9768.strict");
      }

      @Override
      public SQLFunction config(final Object[] configuredParameters) {
        throw new IllegalArgumentException("acme9768.strict wants a number");
      }

      @Override
      public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
          final CommandContext context) {
        return null;
      }

      @Override
      public String getSyntax() {
        return "acme9768.strict(<n>)";
      }
    }

    @Test
    void aFunctionThatRefusesTheProbeConfigurationIsNotAnAggregationAndDoesNotFailPlanning() {
      final DefaultSQLFunctionFactory factory = DefaultSQLFunctionFactory.getInstance();
      factory.register("acme9768.strict", StrictConfig.class);
      try {
        assertThat(CypherFunctionFactory.isNamespacedAggregation("acme9768.strict", 1)).isFalse();
      } finally {
        factory.unregister("acme9768.strict");
      }
    }

    /**
     * The planner can only decide on what the probe configuration lets it see, so a registered function that refuses it would be
     * planned as a scalar again - one NULL per row, no error, which is the bug. This fails by name for such a function, and for any
     * registered dotted function the probe and the function itself disagree about.
     */
    @Test
    void everyRegisteredDottedSqlFunctionAcceptsTheProbeAndIsPlannedAsItsOwnFlagSays() {
      final DefaultSQLFunctionFactory factory = DefaultSQLFunctionFactory.getInstance();
      final List<String> refused = new ArrayList<>();
      final List<String> aggregates = new ArrayList<>();
      for (final String name : new TreeSet<>(factory.getFunctionNames())) {
        if (name.indexOf('.') < 1 || CypherBuiltinFunctions.isBuiltin(name) || CypherFunctionRegistry.get(name) != null)
          continue;

        final int arguments = Math.max(1, factory.getFunctionInstance(name).getMinArgs());
        final Object[] placeholders = new Object[arguments];
        Arrays.fill(placeholders, "dummy");
        final boolean aggregatesByFlag;
        try {
          aggregatesByFlag = factory.getFunctionInstance(name).config(placeholders).aggregateResults();
        } catch (final RuntimeException e) {
          refused.add(name + " (" + e.getMessage() + ")");
          continue;
        }
        if (aggregatesByFlag)
          aggregates.add(name);
        assertThat(CypherFunctionFactory.isNamespacedAggregation(name, arguments)).as(name).isEqualTo(aggregatesByFlag);
      }

      assertThat(refused).as("registered dotted functions that refuse the planner's probe configuration").isEmpty();
      assertThat(aggregates).as("the dotted aggregates of the registry").contains("vector.sum", "vector.avg", "vector.min", "vector.max");
    }

    /** The functions only the Cypher registry holds say whether they aggregate themselves, with no configuration to refuse. */
    @Test
    void everyDottedFunctionOfTheCypherRegistryIsPlannedAsItsOwnFlagSays() {
      final List<String> aggregates = new ArrayList<>();
      for (final String name : new TreeSet<>(FunctionRegistry.getFunctionNames())) {
        final StatelessFunction function = CypherFunctionRegistry.get(name);
        if (name.indexOf('.') < 1 || function == null || CypherBuiltinFunctions.isBuiltin(name))
          continue;
        if (function.aggregateResults())
          aggregates.add(name);
        assertThat(CypherFunctionFactory.isNamespacedAggregation(name, 1)).as(name).isEqualTo(function.aggregateResults());
      }
      assertThat(aggregates).as("dotted aggregates that only the Cypher registry holds").doesNotContain("vector.sum");
    }

    @Test
    void aFunctionRegisteredAsAClassIsFreshAndOneRegisteredAsAnInstanceIsShared() {
      final DefaultSQLFunctionFactory factory = DefaultSQLFunctionFactory.getInstance();
      assertThat(factory.isSharedInstance("vector.sum")).as("stateful aggregate").isFalse();
      assertThat(factory.getFunctionInstance("vector.sum")).isNotSameAs(factory.getFunctionInstance("vector.sum"));
      assertThat(factory.isSharedInstance("geo.point")).as("stateless function").isTrue();
      assertThat(factory.getFunctionInstance("geo.point")).isSameAs(factory.getFunctionInstance("geo.point"));
      assertThat(factory.isSharedInstance("no.such_function")).isFalse();
    }

    @Test
    void theRegistryAnswersWhichNamespacedCallsAggregate() {
      assertThat(CypherFunctionFactory.isNamespacedAggregation("vector.sum", 1)).isTrue();
      assertThat(CypherFunctionFactory.isNamespacedAggregation("vector.avg", 1)).isTrue();
      assertThat(CypherFunctionFactory.isNamespacedAggregation("vector.min", 1)).isTrue();
      assertThat(CypherFunctionFactory.isNamespacedAggregation("vector.max", 1)).isTrue();
      assertThat(CypherFunctionFactory.isNamespacedAggregation("sql.vector.sum", 1)).isTrue();
      assertThat(CypherFunctionFactory.isNamespacedAggregation("vector.magnitude", 1)).isFalse();
      assertThat(CypherFunctionFactory.isNamespacedAggregation("text.indexof", 2)).isFalse();
      assertThat(CypherFunctionFactory.isNamespacedAggregation("no.such_function", 1)).isFalse();
      assertThat(CypherFunctionFactory.isNamespacedAggregation("last", 1)).as("undotted names are never asked").isFalse();
    }
  }

  /**
   * Regression tests for #11114: {@code apoc.util.validate} took two arguments where APOC defines three
   * ({@code predicate, message, params}), and it raised when the predicate was FALSE, where APOC raises when it is TRUE
   * (<a href="https://neo4j.com/docs/apoc/current/overview/apoc.util/apoc.util.validate/">APOC reference</a>), so a guard
   * ported from Neo4j aborted exactly the writes it was written to let through.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue11114ApocUtilValidate extends TestHelper {

    @Test
    void aFalsePredicateLetsTheQueryThrough() {
      assertThat(rows("CALL apoc.util.validate(false, 'bad %s', ['x']) RETURN 1 AS ok")).hasSize(1);
      assertThat(rows("CALL apoc.util.validate(false, 'bad') RETURN 1 AS ok")).hasSize(1);
      assertThat(rows("CALL util.validate(false, 'bad %s', ['x']) RETURN 1 AS ok")).hasSize(1);
    }

    @Test
    void aTruePredicateRaisesTheFormattedMessage() {
      final Throwable error = catchThrowable(() -> rows("CALL apoc.util.validate(true, 'bad %s', ['x']) RETURN 1 AS ok"));
      assertThat(error).isNotNull();
      assertThat(rootMessage(error)).isEqualTo("bad x");
      // a driver shows the outermost message only, so the text the query aborted with must be in it
      assertThat(error.getMessage()).contains("bad x");
      // the caller's condition, not a server fault
      assertThat(ErrorCategory.of(error)).isEqualTo(ErrorCategory.VALIDATION);
    }

    @Test
    void severalPlaceholdersAndNonStringParameters() {
      final Throwable error = catchThrowable(
          () -> rows("CALL apoc.util.validate(1 < 2, 'node %s not found in %s (%s)', [42, 'Person', true]) RETURN 1 AS ok"));
      assertThat(rootMessage(error)).isEqualTo("node 42 not found in Person (true)");
    }

    @Test
    void withoutParametersTheMessageIsTakenAsItIs() {
      // APOC formats only when params are given, so a literal % in a plain message stays as written
      assertThat(rootMessage(catchThrowable(() -> rows("CALL apoc.util.validate(true, '100% wrong') RETURN 1 AS ok"))))
          .isEqualTo("100% wrong");
      assertThat(rootMessage(catchThrowable(() -> rows("CALL apoc.util.validate(true, '100% wrong', []) RETURN 1 AS ok"))))
          .isEqualTo("100% wrong");
    }

    @Test
    void aMessageTheParamsDoNotFitStillAbortsWithTheMessage() {
      assertThat(rootMessage(catchThrowable(() -> rows("CALL apoc.util.validate(true, 'id %d missing', ['abc']) RETURN 1 AS ok"))))
          .isEqualTo("id %d missing [abc]");
      assertThat(rootMessage(catchThrowable(() -> rows("CALL apoc.util.validate(true, '%s and %s', ['one']) RETURN 1 AS ok"))))
          .isEqualTo("%s and %s [one]");
    }

    @Test
    void anOversizedFormatWidthIsNotFormatted() {
      assertThat(rootMessage(catchThrowable(() -> rows("CALL apoc.util.validate(true, 'x%999999999s', ['y']) RETURN 1 AS ok"))))
          .isEqualTo("x%999999999s [y]");
      // '<' reuses the previous argument: its width is measured too
      assertThat(rootMessage(catchThrowable(() -> rows("CALL apoc.util.validate(true, '%s%<999999999s', ['y']) RETURN 1 AS ok"))))
          .isEqualTo("%s%<999999999s [y]");
      // an argument index does not hide the width after it
      assertThat(rootMessage(catchThrowable(() -> rows("CALL apoc.util.validate(true, '%1$1001s', ['y']) RETURN 1 AS ok"))))
          .isEqualTo("%1$1001s [y]");
      // '%%' is a literal percent, not the start of a specifier
      assertThat(rootMessage(catchThrowable(() -> rows("CALL apoc.util.validate(true, 'usage 100%%<1001: %s', ['limit']) RETURN 1 AS ok"))))
          .isEqualTo("usage 100%<1001: limit");
      // a width a real message uses still formats
      assertThat(rootMessage(catchThrowable(() -> rows("CALL apoc.util.validate(true, '[%5s]', ['y']) RETURN 1 AS ok"))))
          .isEqualTo("[    y]");
    }

    @Test
    void numberAndStringPredicates() {
      assertThat(rows("CALL apoc.util.validate(0, 'bad') RETURN 1 AS ok")).hasSize(1);
      assertThat(rows("CALL apoc.util.validate('false', 'bad') RETURN 1 AS ok")).hasSize(1);
      assertThat(rows("CALL apoc.util.validate('', 'bad') RETURN 1 AS ok")).hasSize(1);
      assertThat(rootMessage(catchThrowable(() -> rows("CALL apoc.util.validate(1, 'one') RETURN 1 AS ok")))).isEqualTo("one");
      assertThat(rootMessage(catchThrowable(() -> rows("CALL apoc.util.validate('yes', 'yes') RETURN 1 AS ok")))).isEqualTo("yes");
      // a decimal too small for a double is still not zero
      assertThat(rootMessage(catchThrowable(() -> rows("CALL apoc.util.validate($p, 'tiny') RETURN 1 AS ok",
          Map.of("p", new BigDecimal("1E-400")))))).isEqualTo("tiny");
      assertThat(rows("CALL apoc.util.validate($p, 'zero') RETURN 1 AS ok", Map.of("p", BigDecimal.ZERO))).hasSize(1);
    }

    @Test
    void aNullPredicateDoesNotRaise() {
      assertThat(rows("CALL apoc.util.validate(null, 'bad', []) RETURN 1 AS ok")).hasSize(1);
    }

    @Test
    void theGuardAbortsOnlyTheWriteItProtects() {
      database.transaction(() -> database.command("opencypher", "CREATE (:Account {id: 1, balance: 10})"));

      // the usual "not found" guard: the account exists, so the write goes through
      database.transaction(() -> database.command("opencypher", """
          MATCH (a:Account {id: $id})
          CALL apoc.util.validate(a.balance < $amount, 'insufficient balance on %s', [$id])
          SET a.balance = a.balance - $amount""", Map.of("id", 1, "amount", 4)));
      assertThat(rows("MATCH (a:Account {id: 1}) RETURN a.balance AS b").getFirst().<Number>getProperty("b").intValue())
          .isEqualTo(6);

      final Throwable error = catchThrowable(() -> database.transaction(() -> database.command("opencypher", """
          MATCH (a:Account {id: $id})
          CALL apoc.util.validate(a.balance < $amount, 'insufficient balance on %s', [$id])
          SET a.balance = a.balance - $amount""", Map.of("id", 1, "amount", 100))));
      assertThat(rootMessage(error)).isEqualTo("insufficient balance on 1");
      assertThat(rows("MATCH (a:Account {id: 1}) RETURN a.balance AS b").getFirst().<Number>getProperty("b").intValue())
          .isEqualTo(6);
    }

    @Test
    void validatePredicateIsTheFunctionForm() {
      assertThat(rows("RETURN apoc.util.validatePredicate(false, 'bad %s', ['x']) AS ok").getFirst().<Boolean>getProperty("ok"))
          .isTrue();
      assertThat(rootMessage(catchThrowable(() -> rows("RETURN apoc.util.validatePredicate(true, 'bad %s', ['x']) AS ok"))))
          .isEqualTo("bad x");
    }

    @Test
    void aWrongArgumentCountIsStillRefused() {
      assertThat(catchThrowable(() -> rows("CALL apoc.util.validate(true) RETURN 1 AS ok"))).isNotNull();
      assertThat(catchThrowable(() -> rows("CALL apoc.util.validate(true, 'a', [], 'extra') RETURN 1 AS ok"))).isNotNull();
    }

    private List<Result> rows(final String query) {
      return rows(query, Map.of());
    }

    private List<Result> rows(final String query, final Map<String, Object> parameters) {
      try (final ResultSet rs = database.query("opencypher", query, parameters)) {
        final List<Result> rows = new ArrayList<>();
        while (rs.hasNext())
          rows.add(rs.next());
        return rows;
      }
    }

    private static String rootMessage(final Throwable error) {
      assertThat(error).isNotNull();
      Throwable t = error;
      while (t.getCause() != null && t.getCause() != t)
        t = t.getCause();
      return t.getMessage();
    }
  }

  /**
   * Regression tests for #11115: a call of an unknown NAMESPACED function ({@code apoc.coll.sortMulti}, an APOC function
   * ArcadeDB does not ship) failed while running, with a server error ({@code CommandExecutionException}, Bolt's
   * {@code Neo.DatabaseError.General.UnknownError}) naming the function lower-cased, where an unknown plain function is refused
   * before the query runs as a syntax error naming it as written - which is what Neo4j answers for both.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue11115UnknownNamespacedFunction extends TestHelper {

    @Test
    void anUnknownApocFunctionIsASyntaxErrorNamedAsWritten() {
      assertUnknownFunction("RETURN apoc.coll.sortMulti([{a: 1}], ['a']) AS s", "apoc.coll.sortMulti");
    }

    @Test
    void anUnknownPlainFunctionStillIs() {
      assertUnknownFunction("RETURN noSuchFunction(1) AS s", "noSuchFunction");
    }

    @Test
    void itIsRefusedEvenWhenNoRowWouldEverCallIt() {
      // Neo4j refuses the statement at compile time, so whether a row reaches the call does not matter
      assertUnknownFunction("MATCH (n:NoSuchType) RETURN my.lib.missing(n) AS s", "my.lib.missing");
      assertUnknownFunction("UNWIND [] AS x RETURN apoc.text.noSuch(x) AS s", "apoc.text.noSuch");
    }

    @Test
    void anywhereInTheStatement() {
      assertUnknownFunction("UNWIND [1] AS x WITH x WHERE apoc.nope.f(x) > 0 RETURN x", "apoc.nope.f");
      assertUnknownFunction("UNWIND [1] AS x RETURN x ORDER BY Apoc.Coll.SortMulti(x)", "Apoc.Coll.SortMulti");
      assertUnknownFunction("UNWIND [1] AS x CALL { WITH x RETURN apoc.nope.g(x) AS y } RETURN y", "apoc.nope.g");
      assertUnknownFunction("RETURN [x IN [1, 2] | apoc.nope.h(x)] AS s", "apoc.nope.h");
      assertUnknownFunction("RETURN { a: apoc.nope.i(1) } AS s", "apoc.nope.i");
    }

    @Test
    void inAnyBranchOrConditionOfAConditionalQuery() {
      assertUnknownFunction("WHEN true THEN RETURN 1 AS r ELSE MATCH (n:NoSuchType) RETURN apoc.nope.k(n) AS r", "apoc.nope.k");
      assertUnknownFunction("WHEN false THEN RETURN 1 AS r WHEN true THEN RETURN apoc.nope.l(1) AS r", "apoc.nope.l");
      assertUnknownFunction("WHEN apoc.nope.m(1) THEN RETURN 1 AS r ELSE RETURN 2 AS r", "apoc.nope.m");
    }

    @Test
    void aCallAPredicateSimplificationDropsIsStillRefused() {
      assertUnknownFunction("UNWIND [1] AS x WITH x WHERE true OR apoc.nope.n(x) RETURN x", "apoc.nope.n");
      assertUnknownFunction("MATCH (n) WHERE false AND apoc.nope.o(n) RETURN n", "apoc.nope.o");
    }

    @Test
    void anUnknownFunctionInAWriteIsRefusedBeforeAnythingIsWritten() {
      database.getSchema().getOrCreateVertexType("W");
      assertUnknownFunction("CREATE (:W {v: 1}) WITH 1 AS one RETURN apoc.nope.j(one) AS s", "apoc.nope.j");
      assertThat(rows("MATCH (w:W) RETURN count(w) AS c").getFirst().<Number>getProperty("c").longValue()).isZero();
    }

    @Test
    void knownNamespacedFunctionsStillResolve() {
      final Result row = rows("""
          RETURN apoc.text.capitalize('abc') AS a, text.capitalize('abc') AS b, apoc.util.validatePredicate(false, 'x', []) AS c,
                 sql.abs(-1) AS d, date.truncate('month', date('2026-11-15')) AS e, point.distance(point({x: 0, y: 0}),
                 point({x: 3, y: 4})) AS f, vector.similarity.cosine([1.0, 0.0], [1.0, 0.0]) AS g""").getFirst();
      assertThat(row.<String>getProperty("a")).isEqualTo("Abc");
      assertThat(row.<String>getProperty("b")).isEqualTo("Abc");
      assertThat(row.<Boolean>getProperty("c")).isTrue();
      assertThat(row.<Number>getProperty("d").intValue()).isEqualTo(1);
      assertThat(row.<Object>getProperty("e")).isNotNull();
      assertThat(row.<Number>getProperty("f").doubleValue()).isEqualTo(5.0);
      assertThat(row.<Number>getProperty("g").doubleValue()).isEqualTo(1.0);
    }

    @Test
    void aUserDefinedFunctionResolvesInAnyCaseAndOnlyOnceDefined() {
      assertUnknownFunction("RETURN mylib.twice(2) AS s", "mylib.twice");

      // defined with upper-case letters, which Cypher's lower-cased lookup used to miss
      database.getSchema().registerFunctionLibrary(new TestLibrary("MyLib", new TestFunction("Twice")));
      try {
        assertThat(rows("RETURN mylib.twice(2) AS s").getFirst().<Number>getProperty("s").intValue()).isEqualTo(4);
        assertThat(rows("RETURN MyLib.Twice(3) AS s").getFirst().<Number>getProperty("s").intValue()).isEqualTo(6);
        // a library that exists does not make every name in it a function
        assertUnknownFunction("RETURN mylib.thrice(2) AS s", "mylib.thrice");
      } finally {
        database.getSchema().unregisterFunctionLibrary("MyLib");
      }

      // the statement parsed and planned while the library existed must not outlive it
      final Throwable error = catchThrowable(() -> rows("RETURN mylib.twice(2) AS s"));
      assertThat(error).isInstanceOf(CommandParsingException.class).isNotInstanceOf(CommandSemanticException.class);
      assertThat(error.getMessage()).contains("Unknown function 'mylib.twice'");
    }

    private void assertUnknownFunction(final String query, final String writtenName) {
      final Throwable error = catchThrowable(() -> database.transaction(() -> rows(query)));
      assertThat(error).as(query).isInstanceOf(CommandParsingException.class);
      // a SemanticError over Bolt otherwise: Neo4j's title for an unknown function is SyntaxError
      assertThat(error).as(query).isNotInstanceOf(CommandSemanticException.class);
      assertThat(ErrorCategory.of(error)).as(query).isEqualTo(ErrorCategory.PARSING);
      assertThat(error.getMessage()).as(query).contains("Unknown function '" + writtenName + "'");
    }

    private List<Result> rows(final String query) {
      try (final ResultSet rs = database.command("opencypher", query)) {
        final List<Result> rows = new ArrayList<>();
        while (rs.hasNext())
          rows.add(rs.next());
        return rows;
      }
    }

    private record TestFunction(String name) implements FunctionDefinition {
      @Override
      public String getName() {
        return name;
      }

      @Override
      public Object execute(final Object... parameters) {
        return ((Number) parameters[0]).intValue() * 2;
      }
    }

    private record TestLibrary(String name, TestFunction function) implements FunctionLibraryDefinition<TestFunction> {
      @Override
      public String getName() {
        return name;
      }

      @Override
      public Iterable<TestFunction> getFunctions() {
        return List.of(function);
      }

      @Override
      public boolean hasFunction(final String functionName) {
        return function.name.equals(functionName);
      }

      @Override
      public TestFunction getFunction(final String functionName) throws IllegalArgumentException {
        if (!hasFunction(functionName))
          throw new IllegalArgumentException("Function '" + functionName + "' not defined");
        return function;
      }

      @Override
      public FunctionLibraryDefinition<TestFunction> registerFunction(final TestFunction registerFunction) {
        throw new UnsupportedOperationException();
      }

      @Override
      public FunctionLibraryDefinition<TestFunction> unregisterFunction(final String functionName) {
        throw new UnsupportedOperationException();
      }
    }
  }
}
