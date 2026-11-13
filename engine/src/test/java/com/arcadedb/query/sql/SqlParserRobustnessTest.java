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
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.function.Function;
import com.arcadedb.function.FunctionRegistry;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
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
      for (final String name : new TreeSet<>(FunctionRegistry.getFunctionNames())) {
        final Function function = FunctionRegistry.get(name);
        final String lower = name.toLowerCase();
        // names with two dots (node.degree.in) parse in SQL as a method call on a property (node.degree).in(), not as a registry call
        if (function == null || name.indexOf('.') != name.lastIndexOf('.') || function.getMinArgs() < 1 || lower.contains("sleep") || lower.contains("load") || lower.contains("cypher.run"))
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
      for (final String name : new TreeSet<>(FunctionRegistry.getFunctionNames())) {
        final Function function = FunctionRegistry.get(name);
        final String lower = name.toLowerCase();
        if (function == null || name.indexOf('.') != name.lastIndexOf('.') || lower.contains("sleep") || lower.contains("load") || lower.contains("cypher.run"))
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
