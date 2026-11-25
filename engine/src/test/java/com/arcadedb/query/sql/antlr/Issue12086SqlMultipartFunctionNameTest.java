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
package com.arcadedb.query.sql.antlr;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.function.sql.DefaultSQLFunctionFactory;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.SelectStatement;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12086: a function name with two dots ({@code apoc.text.indexOf}, {@code node.relationship.exists}) was parsed as a
 * property chain plus a method call, because the AST builder rewrote {@code <ns>.<fn>(args)} into a function call only when a
 * single identifier preceded the call. {@code SELECT apoc.text.indexOf('hello', 'l')} answered NULL (property {@code apoc}
 * missing, then the SQL method {@code indexOf} on null) and {@code apoc.map.merge(...)} failed with "Unknown method name".
 */
class Issue12086SqlMultipartFunctionNameTest extends TestHelper {

  private List<Result> run(final String sql) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    return rows;
  }

  private Object single(final String sql) {
    final List<Result> rows = run(sql);
    assertThat(rows).as(sql).hasSize(1);
    return rows.getFirst().getProperty("v");
  }

  @Test
  void apocPrefixedFunctionInAnyCase() {
    assertThat(single("SELECT text.indexOf('hello', 'l') AS v")).isEqualTo(2L);
    assertThat(single("SELECT apoc.text.indexOf('hello', 'l') AS v")).isEqualTo(2L);
    assertThat(single("SELECT APOC.TEXT.INDEXOF('hello', 'l') AS v")).isEqualTo(2L);
    assertThat(single("SELECT Apoc.Text.IndexOf('hello', 'l') AS v")).isEqualTo(2L);
    assertThat(single("SELECT `apoc.text.indexOf`('hello', 'l') AS v")).isEqualTo(2L);
  }

  @Test
  void apocMapMergeAndTextJoin() {
    assertThat(single("SELECT apoc.map.merge({'a': 1}, {'b': 2}) AS v")).isEqualTo(Map.of("a", 1, "b", 2));
    assertThat(single("SELECT apoc.text.join(['a', 'b'], ',') AS v")).isEqualTo("a,b");
    // agg.* are list functions, so the apoc form takes a list
    assertThat(((Number) single("SELECT apoc.agg.median([1, 2, 3]) AS v")).doubleValue()).isEqualTo(2.0);
  }

  @Test
  void methodsAfterTheFunctionAreChained() {
    assertThat(single("SELECT apoc.text.indexOf('hello', 'l').asString() AS v")).isEqualTo("2");
    assertThat(single("SELECT apoc.text.split('a,b,c', ',').size() AS v")).isEqualTo(3);
    assertThat(single("SELECT apoc.text.split('a,b,c', ',')[1] AS v")).isEqualTo("b");
  }

  @Test
  void threePartFunctionNamesOnAVertex() {
    database.getSchema().createVertexType("P");
    database.getSchema().createEdgeType("K");
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("P").set("name", "a").save();
      final MutableVertex b = database.newVertex("P").set("name", "b").save();
      a.newEdge("K", b).save();
    });

    assertThat(single("SELECT node.relationship.exists(@this, 'K') AS v FROM P WHERE name = 'a'")).isEqualTo(true);
    assertThat(single("SELECT NODE.RELATIONSHIP.EXISTS(@this, 'K') AS v FROM P WHERE name = 'a'")).isEqualTo(true);
    assertThat(single("SELECT apoc.node.relationship.exists(@this, 'K') AS v FROM P WHERE name = 'a'")).isEqualTo(true);
    assertThat(single("SELECT node.relationship.exists(@this, 'Missing') AS v FROM P WHERE name = 'a'")).isEqualTo(false);
    assertThat(single("SELECT node.relationship.types(@this) AS v FROM P WHERE name = 'a'")).asInstanceOf(
        InstanceOfAssertFactories.COLLECTION).containsExactly("K");
  }

  @Test
  void unknownApocFunctionIsAnUnknownFunction() {
    assertThatThrownBy(() -> run("SELECT apoc.text.nosuch('x') AS v")).hasMessageContaining("Unknown function")
        .hasMessageContaining("apoc.text.nosuch");
    assertThatThrownBy(() -> run("SELECT APOC.NOSUCH.fn('x') AS v")).hasMessageContaining("Unknown function");
  }

  @Test
  void propertyChainsWithMethodsAreUnchanged() {
    database.getSchema().createDocumentType("T");
    database.transaction(() -> {
      final MutableDocument friend = database.newDocument("T").set("name", "bob").save();
      database.newDocument("T").set("name", "alice")//
          .set("address", Map.of("city", "Rome"))//
          .set("a", Map.of("b", Map.of("c", List.of(1, 2, 3))))//
          .set("node", Map.of("relationship", "abc"))//
          .set("friend", friend.getIdentity()).save();
    });

    assertThat(single("SELECT address.city.toUpperCase() AS v FROM T WHERE name = 'alice'")).isEqualTo("ROME");
    assertThat(single("SELECT a.b.c.size() AS v FROM T WHERE name = 'alice'")).isEqualTo(3);
    assertThat(single("SELECT friend.name.toUpperCase() AS v FROM T WHERE name = 'alice'")).isEqualTo("BOB");
    // the first segment is a function namespace but node.relationship.toUpperCase is no function: a property chain
    assertThat(single("SELECT node.relationship.toUpperCase() AS v FROM T WHERE name = 'alice'")).isEqualTo("ABC");
    assertThat(single("SELECT `node`.relationship.toUpperCase() AS v FROM T WHERE name = 'alice'")).isEqualTo("ABC");

    assertThatThrownBy(() -> run("SELECT a.b.nosuchmethod() AS v FROM T")).hasMessageContaining("Unknown method name: nosuchmethod");
    assertThatThrownBy(() -> run("SELECT node.relationship.nosuch() AS v FROM T")).hasMessageContaining(
        "Unknown method name: nosuch");
  }

  @Test
  void backTickedPropertyNamedApocIsAProperty() {
    database.getSchema().createDocumentType("Q");
    database.transaction(() -> database.newDocument("Q").set("apoc", Map.of("text", "hello")).save());

    assertThat(single("SELECT `apoc`.text.indexOf('l') AS v FROM Q")).isEqualTo(2);
  }

  /**
   * The price of reading every {@code apoc.<a>.<b>(...)} as a function: a target alias called {@code apoc} is a function
   * prefix too, as an alias named after one of the SQL function namespaces already was. Back-ticks address the record.
   */
  @Test
  void targetAliasNamedApocIsAddressedWithBackTicks() {
    database.getSchema().createDocumentType("R");
    database.transaction(() -> database.newDocument("R").set("name", "x").save());

    assertThat(single("SELECT name AS v FROM R `apoc` WHERE `apoc`.name.toUpperCase() = 'X'")).isEqualTo("x");
    assertThat(single("SELECT name AS v FROM R r WHERE r.name.toUpperCase() = 'X'")).isEqualTo("x");
    assertThatThrownBy(() -> run("SELECT name AS v FROM R apoc WHERE apoc.name.toUpperCase() = 'X'")).hasMessageContaining(
        "Unknown function").hasMessageContaining("apoc.name.toUpperCase");
  }

  /** An aggregate an application registers under a three-part name. */
  public static class Total extends SQLFunctionAbstract {
    private long total;

    public Total() {
      super("acme12086.stats.total");
    }

    @Override
    public boolean aggregateResults() {
      return true;
    }

    @Override
    public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
        final CommandContext context) {
      total += ((Number) params[0]).longValue();
      return null;
    }

    @Override
    public Object getResult() {
      return total;
    }

    @Override
    public String getSyntax() {
      return "acme12086.stats.total(<n>)";
    }
  }

  @Test
  void aThreePartAggregateIsAggregatedNotPerRow() {
    database.getSchema().createDocumentType("N");
    database.transaction(() -> {
      for (int i = 1; i <= 3; i++)
        database.newDocument("N").set("n", i).save();
    });

    final DefaultSQLFunctionFactory factory = DefaultSQLFunctionFactory.getInstance();
    factory.register("acme12086.stats.total", Total.class);
    try {
      final DatabaseInternal db = (DatabaseInternal) database;
      final SelectStatement statement = (SelectStatement) db.getStatementCache().get("SELECT acme12086.stats.total(n) AS v FROM N");
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(db);
      assertThat(statement.projection.items.getFirst().isAggregate(context)).isTrue();

      assertThat(single("SELECT acme12086.stats.total(n) AS v FROM N")).isEqualTo(6L);
    } finally {
      factory.unregister("acme12086.stats.total");
    }
  }
}
