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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.QueryNotIdempotentException;
import com.arcadedb.query.OperationType;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #6418: {@code SimpleCypherStatement.isReadOnly()} only inspected top-level
 * {@code CALL} clauses resolving to a registered {@code CypherProcedure}. A call into a user-defined
 * {@code DEFINE FUNCTION} - in {@code CALL} position or, as here, in plain expression position
 * ({@code WITH library.fn() AS x}, {@code RETURN library.fn()}) - was invisible to it:
 * {@code SQLFunctionDefinition.execute} runs the function body as a full {@code sqlscript} command, which can
 * itself {@code CREATE}/{@code UPDATE}/{@code DELETE}, yet the statement was classified read-only.
 * <p>
 * Mirrors {@code Issue6094WriteProcedureCallClassificationTest}, the direct precedent for this same flag and the
 * same three consequences (HA leader routing, {@code Database.query()}'s idempotency gate, and
 * {@code analyze().getOperationTypes()}), for the shape #6094 already covers - a registered write
 * {@code CypherProcedure} in {@code CALL} position - but which does not reach a {@code DEFINE FUNCTION} call
 * outside a {@code CALL} clause at all.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6418UnresolvedFunctionCallClassificationTest {
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
