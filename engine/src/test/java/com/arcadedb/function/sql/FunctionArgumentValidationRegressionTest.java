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
package com.arcadedb.function.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.SQLFunction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.offset;

/**
 * Regression test for #5884: calling one of the 43 listed SQL functions with too few arguments used to surface a raw JDK
 * exception (ArrayIndexOutOfBoundsException, NullPointerException, ClassCastException, NegativeArraySizeException) instead
 * of a validation error, because {@link com.arcadedb.query.sql.parser.FunctionCall} never called
 * {@link com.arcadedb.function.Function#checkArity} before invoking the function - even though every one of these
 * functions now declares {@code getMinArgs()}/{@code getMaxArgs()} correctly.
 * <p>
 * Driven off the declared bounds themselves (not a hand-picked argument count per function), so a function whose
 * declaration regresses back to the unenforced default (0, MAX_VALUE) is caught here rather than only by a person
 * re-running the reporter's original sweep.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class FunctionArgumentValidationRegressionTest extends TestHelper {

  /**
   * The 43 functions #5884 found throwing a raw JDK exception on too few arguments.
   */
  private static final String[] AFFECTED_FUNCTIONS = { //
      "abs", "astar", "bothE", "bothV", "concat", "decode", "difference", "dijkstra", "encode", "first", //
      "format", "geo.lineString", "geo.polygon", "ifempty", "ifnull", "inE", "inV", "intersect", "last", //
      "median", "mode", "outE", "outV", "percentile", "shortestPath", "sqrt", "stddev", "stddevp", "strcmpci", //
      "symmetricDifference", "ts.correlate", "ts.delta", "ts.first", "ts.interpolate", "ts.lag", "ts.last", //
      "ts.lead", "ts.movingAvg", "ts.percentile", "ts.rank", "ts.rate", "variance", "variancep" };

  /**
   * Graph-traversal functions that legitimately accept zero arguments (the labels are optional varargs): the bug for
   * these was not a missing arity check but {@link com.arcadedb.function.sql.graph.SQLFunctionMove#v2e} /
   * {@link com.arcadedb.function.sql.graph.SQLFunctionMove#e2v} dereferencing a null receiver. Calling them with no
   * arguments outside a graph context must return gracefully (null), not throw.
   */
  private static final String[] ZERO_ARG_GRAPH_FUNCTIONS = { "bothE", "bothV", "inE", "inV", "outE", "outV" };

  @ParameterizedTest
  @ValueSource(strings = { //
      "abs", "astar", "concat", "decode", "difference", "dijkstra", "encode", "first", //
      "format", "geo.lineString", "geo.polygon", "ifempty", "ifnull", "intersect", "last", //
      "median", "mode", "percentile", "shortestPath", "sqrt", "stddev", "stddevp", "strcmpci", //
      "symmetricDifference", "ts.correlate", "ts.delta", "ts.first", "ts.interpolate", "ts.lag", "ts.last", //
      "ts.lead", "ts.movingAvg", "ts.percentile", "ts.rank", "ts.rate", "variance", "variancep" })
  void tooFewArgumentsIsAValidationErrorNotARawException(final String functionName) {
    final int minArgs = functionInstance(functionName).getMinArgs();
    assertThat(minArgs).as("%s should require at least one argument", functionName).isGreaterThan(0);

    final String query = "SELECT " + functionName + "(" + placeholders(minArgs - 1) + ") AS r";
    assertThatThrownBy(() -> consume(query)) //
        .as("%s called with %d (< %d required) arguments", functionName, minArgs - 1, minArgs) //
        .isInstanceOf(CommandSemanticException.class) //
        .hasMessageContaining(functionName);
  }

  @ParameterizedTest
  @ValueSource(strings = { "bothE", "bothV", "inE", "inV", "outE", "outV" })
  void graphTraversalFunctionsToleratesZeroArgumentsOutsideAGraphContext(final String functionName) {
    assertThatCode(() -> consume("SELECT " + functionName + "() AS r")) //
        .as("%s() with no current vertex must return gracefully, not throw", functionName) //
        .doesNotThrowAnyException();
  }

  @Test
  void aggregateProjectionDispatchPathStillWorksWithCorrectArguments() {
    // FunctionAggregationContext (used for real GROUP BY aggregation, not just a no-FROM synthetic call) got its
    // own checkArity() call alongside FunctionCall's. A call with the right argument count must still work through
    // that path, not just through the no-FROM cases the other tests above exercise.
    database.getSchema().createDocumentType("AggregateArityFixture");
    database.transaction(() -> {
      for (final int value : new int[] { 1, 2, 3, 4, 5 })
        database.newDocument("AggregateArityFixture").set("value", value).save();
    });

    try (final ResultSet rs = database.query("sql", "SELECT stddev(value) AS r FROM AggregateArityFixture")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat((Double) rs.next().getProperty("r")).isCloseTo(Math.sqrt(2.5), offset(0.0001));
    }

    try (final ResultSet rs = database.query("sql",
        "SELECT (value % 2) AS parity, count(*) AS c FROM AggregateArityFixture GROUP BY parity")) {
      int rows = 0;
      while (rs.hasNext()) {
        rs.next();
        rows++;
      }
      assertThat(rows).isEqualTo(2);
    }
  }

  @Test
  void graphFunctionAsMethodCallSyntaxStillWorksWithCorrectArguments() {
    // MethodCall's isGraphFunction() branch (used for the <expr>.out()/.in()/.both()/... method-call syntax,
    // as opposed to the out()/in()/both() function-call syntax the other tests exercise) also calls
    // checkArity() now. Confirm a normal traversal through that branch still works.
    database.getSchema().createVertexType("ArityMethodSyntaxVertex");
    database.transaction(() -> database.newVertex("ArityMethodSyntaxVertex").save());

    assertThatCode(() -> consume("SELECT @this.bothE() AS r FROM ArityMethodSyntaxVertex")) //
        .as("bothE() via method-call syntax with correct (zero) arguments must not throw") //
        .doesNotThrowAnyException();
  }

  @Test
  void everyAffectedFunctionIsAccountedFor() {
    // Every name in AFFECTED_FUNCTIONS is exercised by exactly one of the two parameterized tests above.
    for (final String name : AFFECTED_FUNCTIONS) {
      final boolean isZeroArgGraphFunction = Arrays.asList(ZERO_ARG_GRAPH_FUNCTIONS).contains(name);
      if (!isZeroArgGraphFunction)
        assertThat(functionInstance(name).getMinArgs()).as("%s", name).isGreaterThan(0);
    }
  }

  private static SQLFunction functionInstance(final String name) {
    return DefaultSQLFunctionFactory.getInstance().getFunctionInstance(name);
  }

  private static String placeholders(final int count) {
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < count; i++) {
      if (i > 0)
        sb.append(", ");
      sb.append("'x'");
    }
    return sb.toString();
  }

  private void consume(final String query) {
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext())
        rs.next();
    }
  }
}
