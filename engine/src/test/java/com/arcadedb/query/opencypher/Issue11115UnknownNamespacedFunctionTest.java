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
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.exception.ErrorCategory;
import com.arcadedb.function.FunctionDefinition;
import com.arcadedb.function.FunctionLibraryDefinition;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Regression tests for #11115: a call of an unknown NAMESPACED function ({@code apoc.coll.sortMulti}, an APOC function
 * ArcadeDB does not ship) failed while running, with a server error ({@code CommandExecutionException}, Bolt's
 * {@code Neo.DatabaseError.General.UnknownError}) naming the function lower-cased, where an unknown plain function is refused
 * before the query runs as a syntax error naming it as written - which is what Neo4j answers for both.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11115UnknownNamespacedFunctionTest extends TestHelper {

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
    assertThat(rows("MATCH (w:W) RETURN count(w) AS c").get(0).<Number>getProperty("c").longValue()).isZero();
  }

  @Test
  void knownNamespacedFunctionsStillResolve() {
    final Result row = rows("""
        RETURN apoc.text.capitalize('abc') AS a, text.capitalize('abc') AS b, apoc.util.validatePredicate(false, 'x', []) AS c,
               sql.abs(-1) AS d, date.truncate('month', date('2026-11-15')) AS e, point.distance(point({x: 0, y: 0}),
               point({x: 3, y: 4})) AS f, vector.similarity.cosine([1.0, 0.0], [1.0, 0.0]) AS g""").get(0);
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
      assertThat(rows("RETURN mylib.twice(2) AS s").get(0).<Number>getProperty("s").intValue()).isEqualTo(4);
      assertThat(rows("RETURN MyLib.Twice(3) AS s").get(0).<Number>getProperty("s").intValue()).isEqualTo(6);
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
