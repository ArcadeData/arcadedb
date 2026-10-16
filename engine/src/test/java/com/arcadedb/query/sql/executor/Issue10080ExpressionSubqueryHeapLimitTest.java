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
package com.arcadedb.query.sql.executor;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.exception.HeapLimitExceededException;
import com.arcadedb.function.sql.SQLFunctionDefinition;
import com.arcadedb.function.sql.SQLFunctionLibraryDefinition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10080: {@code OperationHeapLimit.materialize()} is "the one way a step materialises a sub-plan, so none can hold one on the
 * heap without the limits seeing it", and was applied to every STEP that materialises one and to none of the EXPRESSIONS that do.
 * A subquery written inline - in a projection, a function argument, a SET value, a {@code CREATE EDGE ... FROM} clause - ran the
 * hand-rolled drain loop {@code materialize()} was created to retire, so the identical subquery over the identical data was refused
 * as a LET and accepted inline, and a single modifier after a LET subquery moved the planner from the covered step to the
 * uncovered one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10080ExpressionSubqueryHeapLimitTest extends TestHelper {
  private static final int  OUTER_ROWS = 3_000;
  private static final int  INNER_ROWS = 4_000;
  private static final long CAP        = 100L;

  /** Every statement holds the whole of InnerT (or V) in one sub-plan, in a position that is not hoisted to a LET. */
  private static final List<String> OVER_CAP = List.of(//
      // the controls: the LET twin that was already covered, and a hoisted position
      "SELECT $x.size() AS s FROM OuterT LET $x = (SELECT k, pad FROM InnerT) WHERE k = 0",//
      "SELECT FROM OuterT WHERE k IN (SELECT k FROM InnerT)",//
      // the expression positions
      "SELECT (SELECT k, pad FROM InnerT).size() AS s FROM OuterT WHERE k = 0",//
      "SELECT coalesce((SELECT k, pad FROM InnerT), 0) AS s FROM OuterT WHERE k = 0",//
      "UPDATE OuterT SET n = (SELECT k, pad FROM InnerT).size() WHERE k = 0",//
      // one modifier after the subquery moves the LET from LetQueryStep to LetExpressionStep
      "SELECT $x AS s FROM OuterT LET $x = (SELECT k, pad FROM InnerT).size() WHERE k = 0",//
      "SELECT $x AS s FROM OuterT LET $x = (SELECT k, pad FROM InnerT)[0] WHERE k = 0",//
      // the endpoints of CREATE EDGE
      "CREATE EDGE E FROM (SELECT FROM V) TO (SELECT FROM V WHERE k = 0)");

  @AfterEach
  void resetSettings() {
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.reset();
  }

  @Test
  void everySubqueryPositionFailsPastTheElementCap() {
    createData();
    setCap(CAP);

    // every statement that slipped past the cap, all of them at once rather than the first one
    final List<String> notCapped = new ArrayList<>();
    for (final String sql : OVER_CAP)
      if (!failsOnTheCap(sql))
        notCapped.add(sql);
    assertThat(notCapped).isEmpty();
  }

  /** The same statements, with room to spare: what failed above was the cap, not the statement. */
  @Test
  void theSameStatementsRunBelowTheCap() {
    createData();
    setCap(100_000L);

    for (final String sql : OVER_CAP)
      assertThat(drain(sql)).as(sql).isGreaterThan(0);

    assertThat(single("SELECT (SELECT k, pad FROM InnerT).size() AS s FROM OuterT WHERE k = 0").<Integer>getProperty("s")).isEqualTo(
        INNER_ROWS);
    assertThat(single("SELECT $x AS s FROM OuterT LET $x = (SELECT k, pad FROM InnerT).size() WHERE k = 0").<Integer>getProperty(
        "s")).isEqualTo(INNER_ROWS);
  }

  @Test
  void theFailureNamesTheConstructAndTheSetting() {
    createData();
    setCap(CAP);

    assertThatThrownBy(() -> drain("SELECT (SELECT k, pad FROM InnerT).size() AS s FROM OuterT WHERE k = 0")).isInstanceOf(
        HeapLimitExceededException.class).hasMessageContaining("subquery").hasMessageContaining(
        GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey());
  }

  /** The bytes of an inline subquery reach the budget all queries share, and go back to it when the query ends. */
  @Test
  void anInlineSubqueryIsChargedToTheSharedBudgetAndReleased() {
    createData();
    setCap(-1L);
    final long baseline = QueryHeapBudget.getReservedBytes();

    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(1L);
    for (final String sql : List.of("SELECT (SELECT k, pad FROM InnerT).size() AS s FROM OuterT WHERE k = 0",
        "SELECT $x AS s FROM OuterT LET $x = (SELECT k, pad FROM InnerT).size() WHERE k = 0"))
      assertThatThrownBy(() -> drain(sql)).as(sql).hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_RAM.getKey());
    assertThat(QueryHeapBudget.getReservedBytes()).isLessThanOrEqualTo(baseline);

    // with room, the same statement runs, and gives everything back
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(256L);
    assertThat(single("SELECT (SELECT k, pad FROM InnerT).size() AS s FROM OuterT WHERE k = 0").<Integer>getProperty("s")).isEqualTo(
        INNER_ROWS);
    assertThat(QueryHeapBudget.getReservedBytes()).isLessThanOrEqualTo(baseline);
  }

  /**
   * A subquery re-evaluated for every outer row must give back what it took before the next row: 3,000 evaluations of ~500 bytes
   * are 1.5MB through a budget of 1MB, and only a charge that accumulated would fail it.
   */
  @Test
  void aSubqueryEvaluatedPerRowDoesNotAccumulateItsCharge() {
    createData();
    setCap(-1L);
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(1L);

    assertThat(drain("SELECT (SELECT pad FROM InnerT WHERE k = 1).size() AS s FROM OuterT")).isEqualTo(OUTER_ROWS);
  }

  @Test
  void aUserDefinedSqlFunctionIsUnderTheCapToo() {
    createData();
    final SQLFunctionLibraryDefinition library = new SQLFunctionLibraryDefinition(database, "lib10080");
    library.registerFunction(new SQLFunctionDefinition(database, "all", "SELECT k FROM InnerT"));
    database.getSchema().registerFunctionLibrary(library);

    setCap(CAP);
    assertThatThrownBy(() -> database.getSchema().getFunction("lib10080", "all").execute()).isInstanceOf(
        HeapLimitExceededException.class).hasMessageContaining("SQL function 'all'").hasMessageContaining(
        GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey());

    setCap(100_000L);
    assertThat((List<?>) database.getSchema().getFunction("lib10080", "all").execute()).hasSize(INNER_ROWS);
  }

  /** What the function answers is unchanged: nothing for no row, the bare value for one, the list of values for many. */
  @Test
  @SuppressWarnings("unchecked")
  void aUserDefinedSqlFunctionAnswersWhatItAlwaysDid() {
    createData();
    final SQLFunctionLibraryDefinition library = new SQLFunctionLibraryDefinition(database, "lib10080b");
    library.registerFunction(new SQLFunctionDefinition(database, "none", "SELECT k FROM InnerT WHERE k < 0"));
    library.registerFunction(new SQLFunctionDefinition(database, "one", "SELECT k FROM InnerT WHERE k = 7"));
    library.registerFunction(new SQLFunctionDefinition(database, "many", "SELECT k FROM InnerT WHERE k < 3 ORDER BY k"));
    database.getSchema().registerFunctionLibrary(library);

    assertThat(database.getSchema().getFunction("lib10080b", "none").execute()).isNull();
    assertThat(database.getSchema().getFunction("lib10080b", "one").execute()).isEqualTo(7);
    assertThat((List<Object>) database.getSchema().getFunction("lib10080b", "many").execute()).containsExactly(0, 1, 2);
  }

  /**
   * The claim {@code OperationHeapLimit.materialize()} makes is checkable: no other code of the SQL engine, nor of the openCypher
   * expressions and procedures (issue #10160), drains a sub-plan into a list.
   */
  @Test
  void onlyOperationHeapLimitDrainsASubPlanIntoAList() throws IOException {
    // The loops below read a result set without holding it - or hold something other than rows - and say why. A new entry is a
    // decision that the result set it drains is bounded some other way, not a way to silence this test
    final Map<String, String> allowed = Map.of(//
        "InCondition.java", "tests membership row by row and keeps none",//
        "ProfileStatement.java", "runs the profiled statement to completion and keeps no row",//
        "TraverseProjectionItem.java", "reads the elements of a value a record already holds",//
        "FindReferencesStatement.java", "keeps the RID of each row of an administrative statement, not the row",//
        "CountExpression.java", "counts the rows of a COUNT { ... } subquery and keeps none");

    final Pattern loop = Pattern.compile("while\\s*\\(\\s*(\\w+)\\.hasNext\\(\\)\\s*\\)");
    final List<String> offenders = new ArrayList<>();
    // The expressions, conditions and functions: where a sub-plan is evaluated for its value. The steps of query/sql/executor pull
    // their input in batches of nRecords, and the ones that materialise a sub-plan go through OperationHeapLimit.materialize().
    // The openCypher expressions, procedures and user functions are the same positions in the other language (issue #10160)
    for (final String dir : List.of("src/main/java/com/arcadedb/query/sql/parser", "src/main/java/com/arcadedb/query/sql/antlr",
        "src/main/java/com/arcadedb/function/sql", "src/main/java/com/arcadedb/query/opencypher/ast",
        "src/main/java/com/arcadedb/query/opencypher/procedures", "src/main/java/com/arcadedb/function/cypher")) {
      final Path root = Path.of(dir);
      if (!Files.isDirectory(root))
        continue;
      try (final Stream<Path> files = Files.walk(root)) {
        for (final Path file : files.filter(f -> f.toString().endsWith(".java")).toList()) {
          if (allowed.containsKey(file.getFileName().toString()))
            continue;
          final String source = Files.readString(file);
          final Matcher matcher = loop.matcher(source);
          while (matcher.find()) {
            final String variable = matcher.group(1);
            // a loop over a ResultSet, as opposed to an Iterator: the variable is declared as one in the same file
            if (Pattern.compile("\\b(?:Local)?ResultSet\\s+" + variable + "\\b").matcher(source).find())
              offenders.add(file.getFileName() + " drains `" + variable + "`");
          }
        }
      }
    }
    assertThat(offenders).as("a ResultSet drained by hand, past both heap limits: use OperationHeapLimit.materialize()").isEmpty();
  }

  private void createData() {
    database.command("sql", "CREATE VERTEX TYPE OuterT");
    database.command("sql", "CREATE DOCUMENT TYPE InnerT");
    database.command("sql", "CREATE VERTEX TYPE V");
    database.command("sql", "CREATE EDGE TYPE E");
    final String pad = "x".repeat(500);
    database.transaction(() -> {
      for (int i = 0; i < OUTER_ROWS; i++)
        database.newVertex("OuterT").set("k", i).save();
      for (int i = 0; i < INNER_ROWS; i++) {
        database.newDocument("InnerT").set("k", i).set("pad", pad).save();
        database.newVertex("V").set("k", i).save();
      }
    });
  }

  private void setCap(final long cap) {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, cap);
  }

  private boolean failsOnTheCap(final String sql) {
    try {
      drain(sql);
      return false;
    } catch (final HeapLimitExceededException e) {
      assertThat(e.getMessage()).as(sql).contains(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey());
      return true;
    }
  }

  private int drain(final String sql) {
    final int[] rows = new int[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", sql)) {
        while (rs.hasNext()) {
          rs.next();
          ++rows[0];
        }
      }
    });
    return rows[0];
  }

  private Result single(final String sql) {
    final Result[] result = new Result[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", sql)) {
        assertThat(rs.hasNext()).as(sql).isTrue();
        result[0] = rs.next();
      }
    });
    return result[0];
  }
}
