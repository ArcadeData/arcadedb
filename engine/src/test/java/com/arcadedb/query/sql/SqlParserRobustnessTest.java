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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.exception.ArcadeDBException;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.exception.InvalidArgumentException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.function.Function;
import com.arcadedb.function.FunctionArity;
import com.arcadedb.function.FunctionRegistry;
import com.arcadedb.function.sql.DefaultSQLFunctionFactory;
import com.arcadedb.function.sql.graph.SQLFunctionCypherRID;
import com.arcadedb.function.sql.math.SQLFunctionEval;
import com.arcadedb.function.sql.text.SQLFunctionSearchFieldsMore;
import com.arcadedb.function.sql.text.SQLFunctionSearchIndexMore;
import com.arcadedb.function.sql.vector.SQLFunctionVectorDequantizeInt8;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests, one nested class per reported case.
 */
class SqlParserRobustnessTest {
  /**
   * Regression for #9045 / #9052.
   * <p>
   * Regression tests for issue #9045 (comment between ORDER and BY / GROUP and BY) and #9052 (hexadecimal integer literals).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SqlLexer extends TestHelper {
    @Test
    void commentsBetweenOrderAndBy() {
      database.getSchema().createDocumentType("T9045");
      database.transaction(() -> {
        for (int i = 3; i >= 1; i--)
          database.command("sql", "INSERT INTO T9045 SET id = " + i + ", g = " + (i % 2)).close();
      });

      for (final String order : new String[] { "ORDER BY", "ORDER /* c */ BY", "ORDER/**/BY", "ORDER -- c\nBY", "ORDER -- c\r\nBY", "order \n\t BY" }) {
        final List<Integer> ids = new ArrayList<>();
        try (final ResultSet rs = database.query("sql", "SELECT id FROM T9045 " + order + " id")) {
          while (rs.hasNext())
            ids.add(rs.next().<Integer>getProperty("id"));
        }
        assertThat(ids).as(order).containsExactly(1, 2, 3);
      }

      for (final String group : new String[] { "GROUP BY", "GROUP /* c */ BY", "GROUP/**/BY", "GROUP -- c\nBY" }) {
        try (final ResultSet rs = database.query("sql", "SELECT g, count(*) AS c FROM T9045 " + group + " g")) {
          int n = 0;
          while (rs.hasNext()) {
            rs.next();
            n++;
          }
          assertThat(n).as(group).isEqualTo(2);
        }
      }
    }

    @Test
    void commentContainingTokensBetweenOrderAndByDoesNotSwallowTheQuery() {
      database.getSchema().createDocumentType("T9045b");
      database.transaction(() -> database.command("sql", "INSERT INTO T9045b SET id = 1").close());

      try (final ResultSet rs = database.query("sql", "SELECT id FROM T9045b ORDER /* a */ /* b */ BY id")) {
        assertThat(rs.next().<Integer>getProperty("id")).isEqualTo(1);
      }
      // a token between two comments is not part of the keyword
      assertThatThrownBy(() -> database.query("sql", "SELECT id FROM T9045b ORDER /* a */ x /* b */ BY id").close())
          .isInstanceOf(CommandSQLParsingException.class);
    }

    @Test
    void aLineCommentMentioningByAfterATypeNamedOrderIsStillAComment() {
      database.getSchema().createDocumentType("Order");
      database.transaction(() -> database.command("sql", "INSERT INTO `Order` SET x = 1").close());
      for (final String query : new String[] { "SELECT FROM Order -- by customer\nWHERE x = 1", "SELECT FROM Order -- by\nWHERE x = 1",
          "SELECT FROM Order /* by */ WHERE x = 1", "SELECT FROM Group -- by customer\nWHERE x = 1" }) {
        if (query.contains("Group"))
          continue;
        try (final ResultSet rs = database.query("sql", query)) {
          assertThat(rs.hasNext()).as(query).isTrue();
        }
      }
    }

    @Test
    void hexadecimalBoundaries() {
      assertThat(scalar("SELECT 0x7FFFFFFF AS r")).isEqualTo(Integer.MAX_VALUE);
      assertThat(scalar("SELECT 0x80000000 AS r")).isEqualTo(2147483648L);
      assertThat(scalar("SELECT 0xFFFFFFFFL AS r")).isEqualTo(4294967295L);
      assertThat(scalar("SELECT 0x7FFFFFFFFFFFFFFFL AS r")).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void hexadecimalOverflowIsAParseError() {
      assertThatThrownBy(() -> database.query("sql", "SELECT 0xFFFFFFFFFFFFFFFFL AS r").close()).isInstanceOf(CommandSQLParsingException.class);
    }

    @Test
    void hexadecimalVariantsAndPositions() {
      assertThat(scalar("SELECT 0xffl AS r")).isEqualTo(255L);
      assertThat(scalar("SELECT -0x10 AS r")).isEqualTo(-16);
      database.getSchema().createDocumentType("T9052");
      database.transaction(() -> {
        for (int i = 0; i < 20; i++)
          database.command("sql", "INSERT INTO T9052 SET id = " + i).close();
      });
      try (final ResultSet rs = database.query("sql", "SELECT FROM T9052 ORDER BY id LIMIT 0x10 SKIP 0xA")) {
        int n = 0;
        while (rs.hasNext()) {
          rs.next();
          n++;
        }
        assertThat(n).isEqualTo(10);
      }
      for (final String bad : new String[] { "SELECT 0x AS r", "SELECT 0xG AS r" })
        assertThatThrownBy(() -> database.query("sql", bad).close()).as(bad).isInstanceOf(CommandSQLParsingException.class);
    }

    @Test
    void hexadecimalLiterals() {
      assertThat(scalar("SELECT 0xFF AS r")).isEqualTo(255);
      assertThat(scalar("SELECT 0XFFL AS r")).isEqualTo(255L);
      assertThat(scalar("SELECT 0x10 + 1 AS r")).isEqualTo(17);
      assertThat(scalar("SELECT 0xFFFFFFFF AS r")).isEqualTo(4294967295L);
      assertThat(scalar("SELECT 0 AS r")).isEqualTo(0);
      assertThat(scalar("SELECT 255 AS r")).isEqualTo(255);
    }

    private Object scalar(final String sql) {
      try (final ResultSet rs = database.query("sql", sql)) {
        return rs.next().getProperty("r");
      }
    }
  }

  /**
   * Regression for #9047.
   * <p>
   * Regression test for issue #9047: registry functions (text.*, date.*, agg.* ...) called with too few arguments ended in a raw
   * ArrayIndexOutOfBoundsException instead of a CommandSemanticException, in SQL and in openCypher.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class RegistryFunctionArity extends TestHelper {
    @Test
    void tooFewArgumentsIsASemanticError() {
      for (final String call : new String[] { "text.regexReplace('abc', 'y')", "text.replace('abc')", "date.parse()", "convert.toInteger()",
          "map.merge()", "agg.first()", "util.md5()" }) {
        assertThatThrownBy(() -> drain("sql", "SELECT " + call + " AS r")).as("sql " + call).isInstanceOf(CommandSemanticException.class);
        assertThatThrownBy(() -> drain("opencypher", "RETURN " + call + " AS r")).as("cypher " + call)
            .isInstanceOf(CommandSemanticException.class);
      }
    }

    @Test
    void theMessageNamesTheFunctionAndTheCounts() {
      assertThatThrownBy(() -> drain("sql", "SELECT text.replace('abc') AS r")).isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("text.replace").hasMessageContaining("got 1");
    }

    @Test
    void tooManyArgumentsIsASemanticError() {
      assertThatThrownBy(() -> drain("sql", "SELECT text.replace('a', 'b', 'c', 'd') AS r")).isInstanceOf(CommandSemanticException.class);
      assertThatThrownBy(() -> drain("opencypher", "RETURN text.replace('a', 'b', 'c', 'd') AS r")).isInstanceOf(CommandSemanticException.class);
    }

    @Test
    void aggregatorsWithWrongArityAreRejected() {
      assertThatThrownBy(() -> drain("opencypher", "UNWIND [1,2] AS x RETURN agg.first() AS r")).isInstanceOf(CommandSemanticException.class);
      assertThatThrownBy(() -> drain("opencypher", "UNWIND [1,2] AS x RETURN x, agg.first() AS r")).isInstanceOf(CommandSemanticException.class);
    }

    /** Positive control: calls inside the declared bounds, optional arguments included, still succeed in both languages. */
    @Test
    void validCallsStillWork() {
      for (final String language : new String[] { "sql", "opencypher" }) {
        final String prefix = language.equals("sql") ? "SELECT " : "RETURN ";
        drain(language, prefix + "text.lpad('x', 3, '*') AS r");
        drain(language, prefix + "text.replace('abc', 'b', 'x') AS r");
        drain(language, prefix + "text.regexReplace('abc', 'b', 'x') AS r");
        drain(language, prefix + "convert.toInteger('7') AS r");
      }
    }

    /** The whole registry: one argument fewer than the declared minimum is a semantic error, never a raw JDK exception. */
    @Test
    void everyRegistryFunctionRejectsTooFewArgumentsCleanly() {
      int tried = 0;
      final List<String> offenders = new ArrayList<>();
      for (final Map.Entry<String, Function> entry : declaredFunctions().entrySet()) {
        final String name = entry.getKey();
        final Function function = entry.getValue();
        final String lower = name.toLowerCase();
        // names with two dots (node.degree.in) parse in SQL as a method call on a property (node.degree).in(), not as a registry call
        if (name.indexOf('.') != name.lastIndexOf('.') || function.getMinArgs() < 1 || lower.contains("sleep") || lower.contains("load") || lower.contains("cypher.run"))
          continue;
        final String args = IntStream.range(0, function.getMinArgs() - 1).mapToObj(i -> "1").collect(Collectors.joining(", "));
        tried++;
        try {
          drain("sql", "SELECT " + name + "(" + args + ") AS r");
          offenders.add(name + " (no error)");
        } catch (final CommandSemanticException | CommandSQLParsingException e) {
          // expected
        } catch (final Throwable t) {
          offenders.add(name + " (" + t.getClass().getSimpleName() + ")");
        }
      }
      assertThat(offenders).isEmpty();
      assertThat(tried).isGreaterThan(50);
    }

    /** The contract is not narrower than what the functions accept: a call at the declared minimum and maximum is never an arity error. */
    @Test
    void callsAtTheDeclaredBoundsAreNeverArityErrors() {
      final List<String> offenders = new ArrayList<>();
      for (final Map.Entry<String, Function> entry : declaredFunctions().entrySet()) {
        final String name = entry.getKey();
        final Function function = entry.getValue();
        final String lower = name.toLowerCase();
        if (name.indexOf('.') != name.lastIndexOf('.') || lower.contains("sleep") || lower.contains("load") || lower.contains("cypher.run"))
          continue;
        final int min = function.getMinArgs();
        final int max = function.getMaxArgs();
        for (final int count : max > min && max < 8 && max > 0 ? new int[] { min, max } : new int[] { min }) {
          final String args = IntStream.range(0, count).mapToObj(i -> "1").collect(Collectors.joining(", "));
          try {
            drain("sql", "SELECT " + name + "(" + args + ") AS r");
          } catch (final CommandSemanticException e) {
            // a type mismatch on the junk argument is the function's own business; an arity message is the contract being too narrow
            if (!String.valueOf(e.getMessage()).startsWith("Type mismatch") && String.valueOf(e.getMessage()).contains("expects"))
              offenders.add(name + "(" + count + "): " + e.getMessage());
          } catch (final Throwable t) {
            // junk arguments: any other failure is the function's own business
          }
        }
      }
      assertThat(offenders).isEmpty();
    }

    /**
     * The census must not depend on what other tests left in the shared {@link FunctionRegistry} (issue #11927): it scans the
     * built-in SQL function factory, the authority every SQL function is registered from, including the functions registered
     * as a class (stateful, never placed in the registry), together with whatever the registry holds.
     */
    @Test
    void theCensusReachesTheSqlFunctionFactory() {
      final Map<String, Function> declared = declaredFunctions();
      assertThat(declared).as("the census reaches DefaultSQLFunctionFactory's instance-registered functions")
          .containsKeys(SQLFunctionCypherRID.NAME, SQLFunctionSearchFieldsMore.NAME.toLowerCase(Locale.ROOT),
              SQLFunctionSearchIndexMore.NAME.toLowerCase(Locale.ROOT));
      assertThat(declared).as("the census reaches DefaultSQLFunctionFactory's class-registered (stateful) functions")
          .containsKey(SQLFunctionEval.NAME);
      assertThat(declared.keySet()).as("the census reaches every name DefaultSQLFunctionFactory registers")
          .containsAll(DefaultSQLFunctionFactory.getInstance().getFunctionNames());
    }

    /** Issue #11927: the arity these functions enforce is the arity they declare, so the parse-time gate and the registry agree. */
    @Test
    void functionsDeclareTheArityTheyEnforce() {
      assertThat(new SQLFunctionCypherRID().getMinArgs()).isEqualTo(1);
      assertThat(new SQLFunctionCypherRID().getMaxArgs()).isEqualTo(1);
      assertThat(new SQLFunctionSearchFieldsMore().getMinArgs()).isEqualTo(2);
      assertThat(new SQLFunctionSearchFieldsMore().getMaxArgs()).isEqualTo(3);
      assertThat(new SQLFunctionSearchIndexMore().getMinArgs()).isEqualTo(2);
      assertThat(new SQLFunctionSearchIndexMore().getMaxArgs()).isEqualTo(3);
      assertThat(new SQLFunctionEval().getMinArgs()).isEqualTo(1);
      assertThat(new SQLFunctionEval().getMaxArgs()).isEqualTo(1);

      for (final String call : new String[] { "cypherRID()", "cypherRID(1, 2)", "fulltext.searchFieldsMore('a')",
          "fulltext.searchFieldsMore('a', '#1:0', {}, 'x')", "fulltext.searchIndexMore('a')", "fulltext.searchIndexMore('a', '#1:0', {}, 'x')",
          "eval()", "eval('1 = 1', 'x')" })
        assertThatThrownBy(() -> drain("sql", "SELECT " + call + " AS r")).as("sql " + call).isInstanceOf(CommandSemanticException.class)
            .hasMessageContaining("expects");
    }

    /**
     * Issue #11944: the functions that declared 0..MAX but counted their arguments by hand inside execute(). Each row is
     * {name, min, max}; the declared bounds are what the gate enforces, and one argument outside them is the uniform
     * "expects" semantic error.
     */
    @Test
    void issue11944FunctionsDeclareTheArityTheyEnforce() {
      final int unbounded = Integer.MAX_VALUE;
      final Object[][] arities = { { "circle", 3, 3 }, { "rectangle", 4, 4 }, { "bellmanFord", 3, 4 }, { "randomInt", 1, 1 },
          { "duration", 1, 2 }, { "ts.timeBucket", 2, 3 }, { "vector.add", 2, 2 }, { "vector.subtract", 2, 2 },
          { "vector.approxDistance", 2, 3 }, { "vector.avg", 1, 1 }, { "vector.boost", 2, 2 }, { "vector.clip", 3, 3 },
          { "vector.cosineSimilarity", 2, 2 }, { "vector.denseToSparse", 1, 2 }, { "vector.dequantizeBinary", 1, 3 },
          { "vector.dequantizeInt8", 1, 3 }, { "vector.dimension", 1, 1 }, { "vector.discover", 3, 4 }, { "vector.dotProduct", 2, 2 },
          { "vector.fuse", 1, unbounded }, { "vector.hasInf", 1, 1 }, { "vector.hasNaN", 1, 1 }, { "vector.hasNull", 1, 1 },
          { "vector.hybridScore", 3, 3 }, { "vector.isNormalized", 1, 2 }, { "vector.l1Distance", 2, 2 }, { "vector.l1Norm", 1, 1 },
          { "vector.l2Distance", 2, 2 }, { "vector.lInfNorm", 1, 1 }, { "vector.magnitude", 1, 1 }, { "vector.max", 1, 1 },
          { "vector.min", 1, 1 }, { "vector.mmr", 2, 3 }, { "vector.multiply", 2, 2 }, { "vector.multiScore", 2, 3 },
          { "vector.neighbors", 3, 4 }, { "vector.normalize", 1, 1 }, { "vector.normalizeScores", 1, 1 }, { "vector.quantizeBinary", 1, 1 },
          { "vector.quantizeInt8", 1, 1 }, { "vector.recommend", 4, 5 }, { "vector.rerank", 4, 4 }, { "vector.rrfScore", 1, unbounded },
          { "vector.scale", 2, 2 }, { "vector.scoreTransform", 2, 2 }, { "vector.sparseCreate", 2, 3 }, { "vector.sparseDot", 2, 2 },
          { "vector.sparseNeighbors", 4, 5 }, { "vector.sparseToDense", 1, 1 }, { "vector.sparsity", 1, 3 }, { "vector.stdDev", 1, 1 },
          { "vector.sum", 1, 1 }, { "vector.toString", 1, 2 }, { "vector.variance", 1, 1 } };

      final Map<String, Function> declared = declaredFunctions();
      for (final Object[] row : arities) {
        final String name = (String) row[0];
        final int min = (Integer) row[1];
        final int max = (Integer) row[2];
        final Function function = declared.get(name.toLowerCase(Locale.ROOT));
        assertThat(function).as("the census reaches " + name).isNotNull();
        assertThat(function.getMinArgs()).as(name + " declared minimum").isEqualTo(min);
        assertThat(function.getMaxArgs()).as(name + " declared maximum").isEqualTo(max);

        final List<Integer> outside = new ArrayList<>();
        if (min > 0)
          outside.add(min - 1);
        if (max != unbounded)
          outside.add(max + 1);
        for (final int count : outside) {
          final String call = name + "(" + IntStream.range(0, count).mapToObj(i -> "1").collect(Collectors.joining(", ")) + ")";
          assertThatThrownBy(() -> drain("sql", "SELECT " + call + " AS r")).as("sql " + call).isInstanceOf(CommandSemanticException.class)
              .hasMessageContaining("expects");
        }
      }
    }

    /**
     * Issue #11944: vector.dequantizeInt8 accepts one argument (a quantized result) or three (bytes, min, max), never two. The
     * declared range is 1..3, so the gap is still rejected inside execute(), with the same "expects" wording as the gate.
     */
    @Test
    void dequantizeInt8RejectsTwoArgumentsAsAnArityError() {
      assertThatThrownBy(() -> drain("sql", "SELECT vector.dequantizeInt8([1, 2], 0.5) AS r")).isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("expects 1 or 3 arguments").hasMessageContaining("got 2");
      // the counts outside the declared 1..3 read the same: the gate must not advertise "1-3 arguments", which includes 2
      for (final String call : new String[] { "vector.dequantizeInt8()", "vector.dequantizeInt8([1, 2], 0.5, 1.0, 2.0)" })
        assertThatThrownBy(() -> drain("sql", "SELECT " + call + " AS r")).as(call).isInstanceOf(CommandSemanticException.class)
            .hasMessageContaining("expects 1 or 3 arguments").hasMessageNotContaining("1-3");
    }

    /**
     * Issue #11944 (adversarial pass): ensureLabelTypes declared 0..MAX, refused no arguments inside execute() with an
     * IllegalArgumentException and silently ignored every argument after the first. It now declares exactly one.
     */
    @Test
    void ensureLabelTypesDeclaresTheOneArgumentItReads() {
      final Function function = declaredFunctions().get("ensurelabeltypes");
      assertThat(function).as("the census reaches ensureLabelTypes").isNotNull();
      assertThat(function.getMinArgs()).isEqualTo(1);
      assertThat(function.getMaxArgs()).isEqualTo(1);
      for (final String call : new String[] { "ensureLabelTypes()", "ensureLabelTypes(['A11944'], ['B11944'])" })
        assertThatThrownBy(() -> drain("sql", "SELECT " + call + " AS r")).as(call).isInstanceOf(CommandSemanticException.class)
            .hasMessageContaining("expects 1 argument");
      // an empty list is still refused, as a value error rather than a count error
      assertThatThrownBy(() -> drain("sql", "SELECT ensureLabelTypes([]) AS r")).hasStackTraceContaining("non-empty list of labels");
    }

    /**
     * Issue #11944: a count inside the declared range is never rejected FOR ITS COUNT, whatever the exception type. The bounds
     * census above only recognises the gate's own "expects" message, so a function declaring 0..MAX and counting by hand inside
     * execute() (an IllegalArgumentException, a CommandSQLParsingException carrying the syntax, an index past the end of the
     * array) was invisible to it. This one probes every count from the declared minimum up to three above it.
     */
    @Test
    void countsInsideTheDeclaredRangeAreNeverRejectedForTheirCount() {
      final List<String> offenders = new ArrayList<>();
      final List<String> probed = new ArrayList<>();
      for (final Map.Entry<String, Function> entry : declaredFunctions().entrySet()) {
        final String name = entry.getKey();
        final Function function = entry.getValue();
        final String lower = name.toLowerCase();
        if (name.indexOf('.') != name.lastIndexOf('.') || lower.contains("sleep") || lower.contains("load") || lower.contains("cypher.run"))
          continue;
        final int min = function.getMinArgs();
        final int max = FunctionArity.effectiveMax(function.getMaxArgs());
        final int upper = (int) Math.min(max, (long) min + 3);
        for (int count = min; count <= upper; count++) {
          // the one non-contiguous arity: 1 or 3, never 2 (see dequantizeInt8RejectsTwoArgumentsAsAnArityError)
          if (function instanceof SQLFunctionVectorDequantizeInt8 && count == 2)
            continue;
          final String args = IntStream.range(0, count).mapToObj(i -> "1").collect(Collectors.joining(", "));
          probed.add(name);
          try {
            drain("sql", "SELECT " + name + "(" + args + ") AS r");
          } catch (final Throwable t) {
            if (isArityShaped(t, function))
              offenders.add(name + "(" + count + " args, declared " + FunctionArity.describe(min, max) + "): " + t);
          }
        }
      }
      assertThat(offenders).as("functions rejecting a count inside their declared arity").isEmpty();
      assertThat(probed).as("the census reaches DefaultSQLFunctionFactory's functions, the ones issue #11944 named among them")
          .contains("circle", "vector.dotproduct", "ts.timebucket", "duration", "randomint", "bellmanford");
    }

    /** The control for the census above: every shape issue #11944 found is recognised as an arity rejection, and a plain value error is not. */
    @Test
    void theArityShapeDetectorRecognisesTheShapesTheIssueNamed() {
      final Function circle = declaredFunctions().get("circle");
      assertThat(isArityShaped(new CommandSQLParsingException(circle.getSyntax()), circle)).isTrue();
      assertThat(isArityShaped(new IllegalArgumentException("circle() requires 3 parameters: circle(<center-x>, <center-y>, <distance>)"),
          circle)).isTrue();
      assertThat(isArityShaped(new IllegalArgumentException("bellmanFord() requires at least 3 parameters: source, destination"), circle))
          .isTrue();
      assertThat(isArityShaped(new InvalidArgumentException("duration() expects an ISO-8601 duration, but received 3 arguments"), circle))
          .isTrue();
      assertThat(isArityShaped(new CommandSQLParsingException("Expected maximum value in function"), circle)).isTrue();
      assertThat(isArityShaped(FunctionArity.mismatch("circle", "3 arguments", 2), circle)).isTrue();
      assertThat(isArityShaped(new CommandExecutionException("wrapped", new ArrayIndexOutOfBoundsException("Index 1 out of bounds for length 1")),
          circle)).isTrue();
      assertThat(isArityShaped(new IllegalArgumentException("Unsupported timestamp for time_bucket: '1'"), circle)).isFalse();
      assertThat(isArityShaped(new CommandSemanticException("Type mismatch: expects a vector"), circle)).isFalse();
    }

    /** Whether a failure, anywhere in its cause chain, is a rejection of the argument COUNT rather than of an argument's value. */
    private boolean isArityShaped(final Throwable failure, final Function function) {
      final String syntax = function.getSyntax();
      for (Throwable t = failure; t != null; t = t.getCause() == t ? null : t.getCause()) {
        if (t instanceof ArrayIndexOutOfBoundsException)
          return true;
        final String message = t.getMessage();
        if (message == null)
          continue;
        if (message.startsWith("Type mismatch"))
          return false;
        if (syntax != null && message.equals(syntax))
          return true;
        if (ARITY_MESSAGE.matcher(message).find())
          return true;
      }
      return false;
    }

    private static final Pattern ARITY_MESSAGE = Pattern.compile(
        "(?i)(\\b(requires|expects|expected)\\b[^.]*\\b\\d+\\s+(parameters?|arguments?)\\b)|(\\breceived \\d+ arguments?\\b)"
            + "|(\\bexpects .*\\bbut got \\d+)|(^Expected maximum value in function$)");

    /**
     * Every function SQL can call, keyed by the lower-case name it answers to: the shared {@link FunctionRegistry} plus every name
     * of {@link DefaultSQLFunctionFactory}, so a test that cleared the registry earlier in the same JVM cannot shrink the census.
     */
    private Map<String, Function> declaredFunctions() {
      final Map<String, Function> declared = new TreeMap<>();
      for (final String name : FunctionRegistry.getFunctionNames()) {
        final Function function = FunctionRegistry.get(name);
        if (function != null)
          declared.put(name, function);
      }
      final DefaultSQLFunctionFactory factory = DefaultSQLFunctionFactory.getInstance();
      for (final String name : factory.getFunctionNames())
        declared.putIfAbsent(name.toLowerCase(Locale.ROOT), factory.getFunctionInstance(name));
      return declared;
    }

    private void drain(final String language, final String query) {
      try (final ResultSet rs = database.query(language, query)) {
        while (rs.hasNext())
          rs.next();
      }
    }
  }

  /**
   * Regression for #9148 / #9053 / #9050 / #9049.
   * <p>
   * Regression tests for #9148 (MAXDEPTH as an unquoted map key), #9053 (quadratic multi-value array selector),
   * #9050 (nesting and chains that overflow the stack) and #9049 (malformed statements ending in raw JDK exceptions
   * out of the SELECT planner).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class MalformedAndDeeplyNestedStatements extends TestHelper {

    private static String rep(final String s, final int n) {
      final StringBuilder sb = new StringBuilder(s.length() * n);
      for (int i = 0; i < n; i++)
        sb.append(s);
      return sb.toString();
    }

    // ---- #9148

    private RID[] chain() {
      database.getSchema().createVertexType("Person");
      database.getSchema().createEdgeType("KNOWS");
      final RID[] r = new RID[4];
      database.transaction(() -> {
        final MutableVertex[] v = new MutableVertex[4];
        for (int i = 0; i < 4; i++) {
          v[i] = database.newVertex("Person").set("name", "p" + i).save();
          r[i] = v[i].getIdentity();
        }
        for (int i = 0; i < 3; i++)
          v[i].newEdge("KNOWS", v[i + 1], "weight", 1.0).save();
      });
      return r;
    }

    @Test
    void maxDepthIsUsableAsAnUnquotedMapKey() {
      final RID[] r = chain();
      try (final ResultSet rs = database.query("sql", "SELECT { maxDepth: 1 } AS m")) {
        assertThat(rs.next().<Map<String, Object>>getProperty("m")).containsEntry("maxDepth", 1);
      }
      try (final ResultSet rs = database.query("sql",
          "SELECT dijkstra(" + r[0] + ", " + r[3] + ", 'weight', { direction: 'OUT', maxDepth: 20 }) AS path")) {
        assertThat(rs.next().<List<?>>getProperty("path")).hasSize(4);
      }
      try (final ResultSet rs = database.query("sql",
          "SELECT shortestPath(" + r[0] + ", " + r[3] + ", { direction: 'BOTH', edgeTypeNames: ['KNOWS'], maxDepth: 6 }) AS path")) {
        assertThat(rs.next().<List<?>>getProperty("path")).hasSize(4);
      }
      try (final ResultSet rs = database.query("sql",
          "SELECT shortestPath(" + r[0] + ", " + r[3] + ", 'BOTH', 'KNOWS', { maxDepth: 6 }) AS path")) {
        assertThat(rs.next().<List<?>>getProperty("path")).hasSize(4);
      }
      try (final ResultSet rs = database.query("sql", "SELECT shortestPath(" + r[0] + ", " + r[3] + ", { maxDepth: 1 }) AS path")) {
        assertThat(rs.next().<List<?>>getProperty("path")).isEmpty();
      }
      try (final ResultSet rs = database.query("sql", "SELECT shortestPath(" + r[0] + ", " + r[3] + ", { MAXDEPTH: 1 }) AS path")) {
        assertThat(rs.next().<List<?>>getProperty("path")).isEmpty();
      }
    }

    @Test
    void traverseMaxDepthStillParses() {
      final RID[] r = chain();
      try (final ResultSet rs = database.query("sql", "TRAVERSE out() FROM " + r[0] + " MAXDEPTH 1")) {
        int n = 0;
        while (rs.hasNext()) {
          rs.next();
          n++;
        }
        assertThat(n).isEqualTo(2);
      }
    }

    // ---- #9053

    @Test
    void multiValueArraySelectorParsesInLinearTime() {
      database.getSchema().createDocumentType("T");
      final StringBuilder sb = new StringBuilder();
      for (int i = 0; i < 20000; i++) {
        if (i > 0)
          sb.append(',');
        sb.append(i);
      }
      final StallAwareStopwatch watch = StallAwareStopwatch.start();
      database.getQueryEngine("sql").analyze("SELECT tags[" + sb + "] AS x FROM T");
      // quadratic parse took ~14 s at this size, linear takes tens of ms
      watch.assertGaveUpWithin(5000, "linear multi-value selector build against a quadratic one");
    }

    @Test
    void multiValueArraySelectorKeepsItsValues() {
      database.getSchema().createDocumentType("T");
      database.transaction(() -> database.command("sql", "INSERT INTO T SET tags = ['a','b','c','d']").close());
      try (final ResultSet rs = database.query("sql", "SELECT tags[0,2,3] AS x FROM T")) {
        assertThat(rs.next().<List<Object>>getProperty("x")).containsExactly("a", "c", "d");
      }
    }

    // ---- #9050

    private void assertRefusedCleanly(final String sql) {
      assertThatThrownBy(() -> {
        try (final ResultSet rs = database.query("sql", sql)) {
          while (rs.hasNext())
            rs.next();
        }
      }).isInstanceOf(CommandSQLParsingException.class).satisfies(e -> assertThat(e.getMessage()).isNotNull().contains("nest"));
    }

    @Test
    void deeplyNestedBracketsAreRefusedWithAMessage() {
      assertRefusedCleanly("SELECT " + rep("[", 10000) + "1" + rep("]", 10000) + " AS x");
    }

    @Test
    void deeplyNestedMapsAreRefusedWithAMessage() {
      assertRefusedCleanly("SELECT " + rep("{\"a\":", 1000) + "1" + rep("}", 1000) + " AS x");
    }

    @Test
    void deeplyNestedCaseIsRefusedWithAMessage() {
      assertRefusedCleanly("SELECT " + rep("CASE WHEN true THEN ", 1000) + "1" + rep(" END", 1000) + " AS x");
    }

    @Test
    void longChainsNeverLeakAStackOverflowError() {
      database.getSchema().createDocumentType("T");
      database.transaction(() -> database.command("sql", "INSERT INTO T SET name = 'x', tags = ['a'], m = {'k': 1}").close());
      final String[] statements = { //
          "SELECT " + rep("1 + ", 10000) + "1 AS x", //
          "SELECT " + rep("- ", 10000) + "1 AS x", //
          "SELECT m" + rep(".k", 20000) + " AS x FROM T", //
          "SELECT tags" + rep("[0]", 20000) + " AS x FROM T", //
          "SELECT name" + rep(".toLowerCase()", 20000) + " AS x FROM T" };
      final AtomicInteger refused = new AtomicInteger();
      final List<Throwable> unexpected = new CopyOnWriteArrayList<>();
      final Thread t = new Thread(null, () -> {
        for (final String sql : statements) {
          try (final ResultSet rs = database.query("sql", sql)) {
            while (rs.hasNext())
              rs.next();
          } catch (final CommandSQLParsingException e) {
            assertThat(e.getMessage()).isNotNull();
            refused.incrementAndGet();
          } catch (final Throwable e) {
            // anything but a parsing exception (a raw Error included) is a failure
            unexpected.add(e);
          }
        }
      }, "small-stack", 256 * 1024);
      final Throwable[] failure = new Throwable[1];
      t.setUncaughtExceptionHandler((th, e) -> failure[0] = e);
      t.start();
      try {
        t.join();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      assertThat(failure[0]).isNull();
      assertThat(unexpected).isEmpty();
      assertThat(refused.get()).as("the 20000 element chains cannot be executed on a default stack").isPositive();
    }

    // ---- #9049

    @Test
    void unknownSchemaMetadataIsAnArcadeDBException() {
      assertThatThrownBy(() -> database.query("sql", "SELECT FROM schema:material").hasNext())
          .isInstanceOf(SchemaException.class).hasMessageContaining("Invalid metadata: material");
    }

    @Test
    void emptyDistinctIsAParsingException() {
      database.getSchema().createVertexType("Person");
      assertThatThrownBy(() -> database.query("sql", "SELECT distinct() FROM Person")).isInstanceOf(CommandSQLParsingException.class)
          .hasMessageContaining("distinct() requires one argument");
    }

    @Test
    void parentAsTargetReturnsNoRows() {
      database.getSchema().createVertexType("Person");
      database.transaction(() -> database.command("sql", "INSERT INTO Person SET name = 'a'").close());
      try (final ResultSet rs = database.query("sql", "SELECT @rid FROM $parent WHERE name = 'a'")) {
        assertThat(rs.hasNext()).isFalse();
      }
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS cnt FROM $parent Person")) {
        assertThat(rs.next().<Number>getProperty("cnt").intValue()).isZero();
      }
    }

    @Test
    void nestedLetDoesNotThrowRawUnsupportedOperation() {
      try (final ResultSet rs = database.query("sql", "SELECT $x LET $x = {\"a\": 1}")) {
        assertThat(rs.hasNext()).isTrue();
      }
      // used to be the raw UnsupportedOperationException of Statement.refersToParent()
      try (final ResultSet rs = database.query("sql", "SELECT $x LET $x = LET $x = {\"a\": 1}")) {
        assertThat(rs.hasNext()).isTrue();
      }
    }

    // ---- the guard must not refuse realistic nesting

    @Test
    void realisticNestingStaysUnderTheGuard() {
      database.getSchema().createDocumentType("Doc");
      database.transaction(() -> {
        database.command("sql", "INSERT INTO Doc CONTENT " + rep("{\"a\":", 20) + "1" + rep("}", 20)).close();
        database.command("sqlscript", "LET $i = 0;\nWHILE ($i < 1) {\n  IF ($i = 0) {\n    INSERT INTO Doc SET n = 1;\n  }\n  LET $i = $i + 1;\n}").close();
      });
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM Doc")) {
        assertThat(rs.next().<Number>getProperty("c").intValue()).isEqualTo(2);
      }
    }

    @Test
    void multiValueSelectorWithRids() {
      database.getSchema().createDocumentType("T");
      database.transaction(() -> database.command("sql", "INSERT INTO T SET tags = ['a','b']").close());
      try (final ResultSet rs = database.query("sql", "SELECT tags[#1:0, #1:1] AS x FROM T")) {
        // only pins that it runs and keeps its selectors
        assertThat(rs.next().<List<Object>>getProperty("x")).isNotEmpty();
      }
    }
  }
}
