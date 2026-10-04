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
import com.arcadedb.exception.ArcadeDBException;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for #9148 (MAXDEPTH as an unquoted map key), #9053 (quadratic multi-value array selector),
 * #9050 (nesting and chains that overflow the stack) and #9049 (malformed statements ending in raw JDK exceptions
 * out of the SELECT planner).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9148_9053_9050_9049Test extends TestHelper {

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
    final Thread t = new Thread(null, () -> {
      for (final String sql : statements) {
        try (final ResultSet rs = database.query("sql", sql)) {
          while (rs.hasNext())
            rs.next();
        } catch (final CommandSQLParsingException e) {
          assertThat(e.getMessage()).isNotNull();
          refused.incrementAndGet();
        } catch (final ArcadeDBException e) {
          // any ArcadeDB exception is acceptable, a raw Error is not
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
    assertThat(refused.get()).as("the 20000 element chains cannot be executed on a default stack").isPositive();
  }

  // ---- #9049

  @Test
  void unknownSchemaMetadataIsAnArcadeDBException() {
    assertThatThrownBy(() -> database.query("sql", "SELECT FROM schema:material").hasNext())
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("Invalid metadata: material");
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
    // pre-existing behaviour pinned: parses and runs without a raw exception
    try (final ResultSet rs = database.query("sql", "SELECT tags[#1:0, #1:1] AS x FROM T")) {
      // the RIDs parse as plain expressions, so they are real selectors, not dropped ones (the dead rid branches)
      assertThat(rs.next().<List<Object>>getProperty("x")).isNotEmpty().containsOnlyNulls();
    }
  }
}
