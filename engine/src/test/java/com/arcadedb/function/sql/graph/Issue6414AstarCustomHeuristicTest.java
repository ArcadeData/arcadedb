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
package com.arcadedb.function.sql.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Identifiable;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code astar}'s {@code customHeuristicFormula} option was documented, listed among the accepted options and read into
 * a field that nothing ever consulted: a query supplying one silently got MANHATTAN, with no error and no warning
 * (issue #6414, item 1). It now names a SQL function that computes h(n), and every way of getting that wrong is an
 * error the caller sees.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6414AstarCustomHeuristicTest extends TestHelper {
  private static final String            CUSTOM_FUNCTION = "distanceOnX6414";
  private static final String            NOT_A_NUMBER    = "notANumber6414";
  private static final AtomicInteger     CALLS           = new AtomicInteger();
  private static final List<List<Object>> ARGUMENTS      = new ArrayList<>();

  @BeforeEach
  void registerFunctionsAndGraph() {
    CALLS.set(0);
    ARGUMENTS.clear();

    final SQLQueryEngine engine = (SQLQueryEngine) database.getQueryEngine("sql");
    engine.getFunctionFactory().register(new SQLFunctionAbstract(CUSTOM_FUNCTION) {
      @Override
      public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult,
          final Object[] params, final CommandContext context) {
        CALLS.incrementAndGet();
        ARGUMENTS.add(Arrays.asList(params));
        final Vertex current = (Vertex) params[0];
        final Vertex target = (Vertex) params[2];
        return Math.abs(target.getDouble("x") - current.getDouble("x"));
      }

      @Override
      public String getSyntax() {
        return CUSTOM_FUNCTION + "(current, parent, target, source, depth, dFactor)";
      }
    });
    engine.getFunctionFactory().register(new SQLFunctionAbstract(NOT_A_NUMBER) {
      @Override
      public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult,
          final Object[] params, final CommandContext context) {
        return "not a number";
      }

      @Override
      public String getSyntax() {
        return NOT_A_NUMBER + "()";
      }
    });

    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Node6414");
      database.command("sql", "CREATE PROPERTY Node6414.name STRING");
      database.command("sql", "CREATE PROPERTY Node6414.x DOUBLE");
      database.command("sql", "CREATE EDGE TYPE Road6414");
      database.command("sql", "CREATE PROPERTY Road6414.weight DOUBLE");

      for (int i = 0; i < 4; i++)
        database.command("sql", "INSERT INTO Node6414 SET name = ?, x = ?", "n" + i, (double) i);

      // A cheap three-hop chain and one expensive shortcut, so the answer is only right if the search really runs.
      link("n0", "n1", 1);
      link("n1", "n2", 1);
      link("n2", "n3", 1);
      link("n0", "n3", 10);
    });
  }

  @AfterEach
  void unregisterFunctions() {
    final SQLQueryEngine engine = (SQLQueryEngine) database.getQueryEngine("sql");
    engine.getFunctionFactory().unregister(CUSTOM_FUNCTION);
    engine.getFunctionFactory().unregister(NOT_A_NUMBER);
  }

  @Test
  void customHeuristicFormulaIsInvokedAndDrivesTheSearch() {
    final List<String> names = pathNames(
        "{'direction':'OUT','customHeuristicFormula':'" + CUSTOM_FUNCTION + "'}");

    assertThat(names).containsExactly("n0", "n1", "n2", "n3");
    // The point of the whole item: the option is applied, not read and dropped.
    assertThat(CALLS.get()).isGreaterThan(0);
  }

  /**
   * A custom formula owns h(n) outright, so it applies with no {@code vertexAxisNames} declared at all - the branch
   * every built-in formula lives in is never reached. The arguments it receives are the contract the syntax states.
   */
  @Test
  void customHeuristicReceivesTheDocumentedArguments() {
    pathNames("{'direction':'OUT','dFactor':2.5,'customHeuristicFormula':'" + CUSTOM_FUNCTION + "'}");

    assertThat(ARGUMENTS).isNotEmpty();
    for (final List<Object> args : ARGUMENTS) {
      assertThat(args).hasSize(6);
      assertThat(args.get(0)).isInstanceOf(Vertex.class);   // current
      assertThat(args.get(2)).isInstanceOf(Vertex.class);   // target
      assertThat(args.get(3)).isInstanceOf(Vertex.class);   // source
      assertThat(args.get(4)).isInstanceOf(Long.class);     // depth
      assertThat(args.get(5)).isEqualTo(2.5);               // dFactor
    }
    // The first call is the source's own estimate, taken before any node has a parent.
    assertThat(ARGUMENTS.get(0).get(1)).isNull();
  }

  @Test
  void heuristicFormulaCustomWithoutAFunctionNameIsRejected() {
    assertThatThrownBy(() -> pathNames("{'direction':'OUT','heuristicFormula':'CUSTOM'}"))
        .hasMessageContaining("customHeuristicFormula");
  }

  @Test
  void anUnknownFunctionNameIsRejected() {
    assertThatThrownBy(() -> pathNames("{'direction':'OUT','customHeuristicFormula':'noSuchFunction6414'}"))
        .hasMessageContaining("noSuchFunction6414");
  }

  @Test
  void aBuiltInFormulaAndACustomOneTogetherAreAContradiction() {
    assertThatThrownBy(() -> pathNames(
        "{'direction':'OUT','heuristicFormula':'EUCLIDEAN','customHeuristicFormula':'" + CUSTOM_FUNCTION + "'}"))
        .hasMessageContaining("conflict");
  }

  @Test
  void aCustomFormulaThatDoesNotReturnANumberIsRejected() {
    assertThatThrownBy(() -> pathNames("{'direction':'OUT','customHeuristicFormula':'" + NOT_A_NUMBER + "'}"))
        .hasMessageContaining("must return a number");
  }

  /**
   * {@code dijkstra} does not accept the option at all - it never had a heuristic - and says so rather than ignoring it.
   */
  @Test
  void dijkstraStillRejectsTheOptionOutright() {
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.query("sql",
          "SELECT dijkstra((SELECT FROM Node6414 WHERE name = 'n0'), (SELECT FROM Node6414 WHERE name = 'n3'), 'weight', "
              + "{'direction':'OUT','customHeuristicFormula':'" + CUSTOM_FUNCTION + "'}) AS path")) {
        while (rs.hasNext())
          rs.next();
      }
    }).hasMessageContaining("customHeuristicFormula");
  }

  private List<String> pathNames(final String options) {
    final List<String> names = new ArrayList<>();
    try (final ResultSet rs = database.query("sql",
        "SELECT expand(astar((SELECT FROM Node6414 WHERE name = 'n0'), (SELECT FROM Node6414 WHERE name = 'n3'), 'weight', "
            + options + ")) AS path")) {
      while (rs.hasNext())
        names.add(rs.next().toElement().getString("name"));
    }
    return names;
  }

  private void link(final String from, final String to, final double weight) {
    database.command("sql",
        "CREATE EDGE Road6414 FROM (SELECT FROM Node6414 WHERE name = ?) TO (SELECT FROM Node6414 WHERE name = ?) SET weight = ?",
        from, to, weight);
  }
}
